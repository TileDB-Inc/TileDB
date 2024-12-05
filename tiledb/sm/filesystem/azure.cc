/**
 * @file   azure.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements the Azure class.
 */

#ifdef HAVE_AZURE

#include <sstream>

#include <azure/core/diagnostics/logger.hpp>
#include <azure/identity.hpp>
#include <azure/storage/blobs.hpp>

#include "tiledb/common/common.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/platform/cert_file.h"
#include "tiledb/sm/filesystem/azure.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"

static std::shared_ptr<::Azure::Core::Http::HttpTransport> create_transport(
    const tiledb::sm::SSLConfig& ssl_cfg);

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace {
/**
 * The maximum number of committed parts a block blob can have.
 *
 * This value was obtained from
 * https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets
 */
constexpr int max_committed_block_num = 50000;
}  // namespace

namespace tiledb::sm {

/** Converts an Azure nullable value to an STL optional. */
template <class T>
std::optional<T> from_azure_nullable(const ::Azure::Nullable<T>& value) {
  return value ? std::optional(*value) : nullopt;
}

/** Converts an STL optional value to an Azure nullable. */
template <class T>
::Azure::Nullable<T> to_azure_nullable(const std::optional<T>& value) {
  return value ? *value : ::Azure::Nullable<T>{};
}

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

AzureParameters::AzureParameters(const Config& config)
    : max_parallel_ops_(
          config.get<uint64_t>("vfs.azure.max_parallel_ops", Config::must_find))
    , block_list_block_size_(config.get<uint64_t>(
          "vfs.azure.block_list_block_size", Config::must_find))
    , write_cache_max_size_(max_parallel_ops_ * block_list_block_size_)
    , max_retries_(
          config.get<uint64_t>("vfs.azure.max_retries", Config::must_find))
    , retry_delay_(std::chrono::milliseconds(
          config.get<uint64_t>("vfs.azure.retry_delay_ms", Config::must_find)))
    , max_retry_delay_(std::chrono::milliseconds(config.get<uint64_t>(
          "vfs.azure.max_retry_delay_ms", Config::must_find)))
    , use_block_list_upload_(config.get<bool>(
          "vfs.azure.use_block_list_upload", Config::must_find))
    , account_name_(get_config_with_env_fallback(
          config, "vfs.azure.storage_account_name", "AZURE_STORAGE_ACCOUNT"))
    , account_key_(get_config_with_env_fallback(
          config, "vfs.azure.storage_account_key", "AZURE_STORAGE_KEY"))
    , blob_endpoint_(get_blob_endpoint(config, account_name_))
    , ssl_cfg_(config)
    , has_sas_token_(
          !get_config_with_env_fallback(
               config, "vfs.azure.storage_sas_token", "AZURE_STORAGE_SAS_TOKEN")
               .empty()) {
  if (blob_endpoint_.empty()) {
    throw AzureException(
        "Azure VFS is not configured. Please set the "
        "'vfs.azure.storage_account_name' and/or "
        "'vfs.azure.blob_endpoint' configuration options.");
  }
}

Azure::Azure(ThreadPool* const thread_pool, const AzureParameters& config)
    : azure_params_(config)
    , thread_pool_(thread_pool) {
  assert(thread_pool);
}

Azure::~Azure() {
}

std::string get_config_with_env_fallback(
    const Config& config, const std::string& key, const char* env_name) {
  std::string result = config.get<std::string>(key, Config::must_find);
  if (result.empty()) {
    char* env = getenv(env_name);
    if (env) {
      result = getenv(env_name);
    }
  }
  return result;
}

std::string get_blob_endpoint(
    const Config& config, const std::string& account_name) {
  std::string sas_token = get_config_with_env_fallback(
      config, "vfs.azure.storage_sas_token", "AZURE_STORAGE_SAS_TOKEN");

  std::string result = get_config_with_env_fallback(
      config, "vfs.azure.blob_endpoint", "AZURE_BLOB_ENDPOINT");
  if (result.empty()) {
    if (!account_name.empty()) {
      result = "https://" + account_name + ".blob.core.windows.net";
    } else {
      return "";
    }
  } else if (!(utils::parse::starts_with(result, "http://") ||
               utils::parse::starts_with(result, "https://"))) {
    LOG_WARN(
        "The 'vfs.azure.blob_endpoint' option should include the scheme (HTTP "
        "or HTTPS).");
  }
  if (!result.empty() && !sas_token.empty()) {
    // The question mark is not strictly part of the SAS token
    // (https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview#sas-token),
    // but in the Azure Portal the SAS token starts with one. If it does not, we
    // add the question mark ourselves.
    if (!utils::parse::starts_with(sas_token, "?")) {
      result += '?';
    }
    result += sas_token;
  }
  return result;
}

/* ********************************* */
/*                 API               */
/* ********************************* */

const ::Azure::Storage::Blobs::BlobServiceClient&
Azure::AzureClientSingleton::get(const AzureParameters& params) {
  // Initialize logging from the Azure SDK.
  static std::once_flag azure_log_sentinel;
  std::call_once(azure_log_sentinel, []() {
    ::Azure::Core::Diagnostics::Logger::SetListener(
        [](auto level, const std::string& message) {
          switch (level) {
            case ::Azure::Core::Diagnostics::Logger::Level::Error:
              LOG_ERROR(message);
              break;
            case ::Azure::Core::Diagnostics::Logger::Level::Warning:
              LOG_WARN(message);
              break;
            case ::Azure::Core::Diagnostics::Logger::Level::Informational:
              LOG_INFO(message);
              break;
            case ::Azure::Core::Diagnostics::Logger::Level::Verbose:
              LOG_DEBUG(message);
              break;
          }
        });
  });

  std::lock_guard<std::mutex> lck(client_init_mtx_);

  if (client_) {
    return *client_;
  }

  ::Azure::Storage::Blobs::BlobClientOptions options;
  options.Retry.MaxRetries = params.max_retries_;
  options.Retry.RetryDelay = params.retry_delay_;
  options.Retry.MaxRetryDelay = params.max_retry_delay_;
  options.Transport.Transport = create_transport(params.ssl_cfg_);

  // Construct the Azure SDK blob service client.
  // We pass a shared key if it was specified.
  if (!params.account_key_.empty()) {
    // If we don't have an account name, warn and try other authentication
    // methods.
    if (params.account_name_.empty()) {
      LOG_WARN(
          "Azure storage account name must be set when specifying account key. "
          "Account key will be ignored.");
    } else {
      client_ =
          tdb_unique_ptr<::Azure::Storage::Blobs::BlobServiceClient>(tdb_new(
              ::Azure::Storage::Blobs::BlobServiceClient,
              params.blob_endpoint_,
              make_shared<::Azure::Storage::StorageSharedKeyCredential>(
                  HERE(), params.account_name_, params.account_key_),
              options));
      return *client_;
    }
  }

  // Otherwise, if we did not specify an SAS token
  // and we are connecting to an HTTPS endpoint,
  // use ChainedTokenCredential to authenticate using Microsoft Entra ID.
  if (!params.has_sas_token_ &&
      utils::parse::starts_with(params.blob_endpoint_, "https://")) {
    try {
      ::Azure::Core::Credentials::TokenCredentialOptions cred_options;
      cred_options.Retry = options.Retry;
      cred_options.Transport = options.Transport;
      auto credential = make_shared<::Azure::Identity::ChainedTokenCredential>(
          HERE(),
          std::vector<
              std::shared_ptr<::Azure::Core::Credentials::TokenCredential>>{
              make_shared<::Azure::Identity::EnvironmentCredential>(
                  HERE(), cred_options),
              make_shared<::Azure::Identity::AzureCliCredential>(
                  HERE(), cred_options),
              make_shared<::Azure::Identity::ManagedIdentityCredential>(
                  HERE(), cred_options),
              make_shared<::Azure::Identity::WorkloadIdentityCredential>(
                  HERE(), cred_options)});
      // If a token is not available we wouldn't know it until we make a
      // request and it would be too late. Try getting a token, and if it
      // fails fall back to anonymous authentication.
      ::Azure::Core::Credentials::TokenRequestContext tokenContext;

      // https://github.com/Azure/azure-sdk-for-cpp/blob/azure-storage-blobs_12.7.0/sdk/storage/azure-storage-blobs/src/blob_service_client.cpp#L84
      tokenContext.Scopes.emplace_back("https://storage.azure.com/.default");
      std::ignore = credential->GetToken(tokenContext, {});
      client_ =
          tdb_unique_ptr<::Azure::Storage::Blobs::BlobServiceClient>(tdb_new(
              ::Azure::Storage::Blobs::BlobServiceClient,
              params.blob_endpoint_,
              credential,
              options));
      return *client_;
    } catch (...) {
      LOG_INFO(
          "Failed to get Microsoft Entra ID token, falling back to anonymous "
          "authentication");
    }
  }

  client_ = tdb_unique_ptr<::Azure::Storage::Blobs::BlobServiceClient>(tdb_new(
      ::Azure::Storage::Blobs::BlobServiceClient,
      params.blob_endpoint_,
      options));

  return *client_;
}

Status Azure::create_container(const URI& uri) const {
  const auto& c = client();
  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  bool created;
  std::string error_message = "";
  try {
    created = c.GetBlobContainerClient(container_name).Create().Value.Created;
  } catch (const ::Azure::Storage::StorageException& e) {
    created = false;
    error_message = "; " + e.Message;
  }

  if (!created) {
    throw AzureException(
        "Create container failed on: " + uri.to_string() + error_message);
  }

  return Status::Ok();
}

Status Azure::empty_container(const URI& container) const {
  return remove_dir(container);
}

Status Azure::flush_blob(const URI& uri) {
  const auto& c = client();

  if (!azure_params_.use_block_list_upload_) {
    return flush_blob_direct(uri);
  }

  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  const Status flush_write_cache_st =
      flush_write_cache(uri, write_cache_buffer, true);

  BlockListUploadState* state;
  {
    std::unique_lock<std::mutex> states_lock(block_list_upload_states_lock_);

    if (block_list_upload_states_.count(uri.to_string()) == 0) {
      return flush_write_cache_st;
    }

    state = &block_list_upload_states_.at(uri.to_string());
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  if (!state->st().ok()) {
    // Save the return status because 'state' will be freed before we return.
    const Status st = state->st();

    // Unlike S3 that can abort a chunked upload to immediately release
    // uncommited chunks and leave the original object unmodified, the
    // only way to do this on Azure is by some form of a write. We must
    // either:
    // 1. Delete the blob
    // 2. Overwrite the blob with a zero-length buffer.
    //
    // Alternatively, we could do nothing and let Azure release the
    // uncommited blocks ~7 days later. We chose to delete the blob
    // as a best-effort operation. We intentionally are ignoring the
    // returned Status from 'remove_blob'.
    throw_if_not_ok(remove_blob(uri));

    // Release all instance state associated with this block list
    // transactions.
    finish_block_list_upload(uri);

    return st;
  }

  // Build the block list to commit.
  const std::list<std::string> block_ids = state->get_block_ids();

  // Release all instance state associated with this block list
  // transactions so that we can safely return if the following
  // request failed.
  finish_block_list_upload(uri);

  try {
    c.GetBlobContainerClient(container_name)
        .GetBlockBlobClient(blob_path)
        .CommitBlockList(std::vector(block_ids.begin(), block_ids.end()));
  } catch (const ::Azure::Storage::StorageException& e) {
    std::string msg_extra;
    // Unlike S3 where each part has its number and uploading a part with an out
    // of bounds number fails, Azure blocks do not have a sequenced number. We
    // could fail ourselves as soon as we hit the limit, but if Azure ever
    // increases it, it would need updating our own code. Therefore delay the
    // check until we finish the upload.
    // We could also add an explanation if we hit the limit of uncommitted
    // blocks (100,000 at the time of writing this), but it would require more
    // refactorings, and it's much harder to hit with reasonably sized blocks.
    if (block_ids.size() > max_committed_block_num) {
      msg_extra +=
          " This error might be resolved by increasing the value of the "
          "'vfs.azure.block_list_block_size' config option";
    }
    throw AzureException(
        "Flush blob failed on: " + uri.to_string() + "; " + e.Message +
        msg_extra);
  }

  return Status::Ok();
}

void Azure::finish_block_list_upload(const URI& uri) {
  // Protect 'block_list_upload_states_' from multiple writers.
  {
    std::unique_lock<std::mutex> states_lock(block_list_upload_states_lock_);
    block_list_upload_states_.erase(uri.to_string());
  }

  // Protect 'write_cache_map_' from multiple writers.
  {
    std::unique_lock<std::mutex> cache_lock(write_cache_map_lock_);
    write_cache_map_.erase(uri.to_string());
  }
}

Status Azure::flush_blob_direct(const URI& uri) {
  auto& c = client();
  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  if (write_cache_buffer->size() == 0) {
    return Status::Ok();
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  try {
    c.GetBlobContainerClient(container_name)
        .GetBlockBlobClient(blob_path)
        .UploadFrom(
            static_cast<uint8_t*>(write_cache_buffer->data()),
            static_cast<size_t>(write_cache_buffer->size()));
  } catch (const ::Azure::Storage::StorageException& e) {
    throw AzureException(
        "Flush blob failed on: " + uri.to_string() + "; " + e.Message);
  }

  // Protect 'write_cache_map_' from multiple writers.
  {
    std::unique_lock<std::mutex> cache_lock(write_cache_map_lock_);
    write_cache_map_.erase(uri.to_string());
  }

  return Status::Ok();
}

Status Azure::is_empty_container(const URI& uri, bool* is_empty) const {
  const auto& c = client();
  assert(is_empty);

  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  ::Azure::Storage::Blobs::ListBlobsOptions options;
  options.PageSizeHint = 1;
  try {
    *is_empty = c.GetBlobContainerClient(container_name)
                    .ListBlobs(options)
                    .Blobs.empty();
  } catch (const ::Azure::Storage::StorageException& e) {
    throw AzureException(
        "List blobs failed on: " + uri.to_string() + "; " + e.Message);
  }

  return Status::Ok();
}

Status Azure::is_container(const URI& uri, bool* const is_container) const {
  assert(is_container);

  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  return this->is_container(container_name, is_container);
}

Status Azure::is_container(
    const std::string& container_name, bool* const is_container) const {
  const auto& c = client();
  assert(is_container);

  try {
    c.GetBlobContainerClient(container_name).GetProperties();
  } catch (const ::Azure::Storage::StorageException& e) {
    if (e.StatusCode == ::Azure::Core::Http::HttpStatusCode::NotFound) {
      *is_container = false;
      return Status_Ok();
    }
    throw AzureException(
        "Get container properties failed on: " + container_name + "; " +
        e.Message);
  }

  *is_container = true;
  return Status_Ok();
}

Status Azure::is_dir(const URI& uri, bool* const exists) const {
  assert(exists);

  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri, &paths, "/", 1));
  *exists = (bool)paths.size();
  return Status_Ok();
}

Status Azure::is_blob(const URI& uri, bool* const is_blob) const {
  assert(is_blob);

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  return this->is_blob(container_name, blob_path, is_blob);
}

Status Azure::is_blob(
    const std::string& container_name,
    const std::string& blob_path,
    bool* const is_blob) const {
  const auto& c = client();
  assert(is_blob);

  try {
    c.GetBlobContainerClient(container_name)
        .GetBlobClient(blob_path)
        .GetProperties();
  } catch (const ::Azure::Storage::StorageException& e) {
    if (e.StatusCode == ::Azure::Core::Http::HttpStatusCode::NotFound) {
      *is_blob = false;
      return Status_Ok();
    }
    throw AzureException(
        "Get blob properties failed on: " + blob_path + "; " + e.Message);
  }

  *is_blob = true;
  return Status_Ok();
}

std::string Azure::remove_front_slash(const std::string& path) {
  if (path.front() == '/') {
    return path.substr(1, path.length());
  }

  return path;
}

std::string Azure::add_trailing_slash(const std::string& path) {
  if (path.back() != '/') {
    return path + "/";
  }

  return path;
}

std::string Azure::remove_trailing_slash(const std::string& path) {
  if (path.back() == '/') {
    return path.substr(0, path.length() - 1);
  }

  return path;
}

Status Azure::ls(
    const URI& uri,
    std::vector<std::string>* paths,
    const std::string& delimiter,
    const int max_paths) const {
  assert(paths);

  for (auto& fs : ls_with_sizes(uri, delimiter, max_paths)) {
    paths->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

std::vector<directory_entry> Azure::ls_with_sizes(
    const URI& uri, const std::string& delimiter, int max_paths) const {
  const auto& c = client();

  const URI uri_dir = uri.add_trailing_slash();

  if (!uri_dir.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri_dir.to_string());
  }

  std::string container_name;
  std::string blob_path;
  throw_if_not_ok(parse_azure_uri(uri_dir, &container_name, &blob_path));

  auto container_client = c.GetBlobContainerClient(container_name);

  std::vector<directory_entry> entries;
  ::Azure::Storage::Blobs::ListBlobsOptions options;
  options.ContinuationToken = "";
  options.PageSizeHint = max_paths > 0 ? max_paths : 5000;
  options.Prefix = blob_path;
  do {
    ::Azure::Storage::Blobs::ListBlobsByHierarchyPagedResponse response;
    try {
      response = container_client.ListBlobsByHierarchy(delimiter, options);
    } catch (const ::Azure::Storage::StorageException& e) {
      throw AzureException(
          "List blobs failed on: " + uri_dir.to_string() + "; " + e.Message);
    }

    for (const auto& blob : response.Blobs) {
      entries.emplace_back(
          "azure://" + container_name + "/" +
              remove_front_slash(remove_trailing_slash(blob.Name)),
          blob.BlobSize,
          false);
    }

    for (const auto& prefix : response.BlobPrefixes) {
      entries.emplace_back(
          "azure://" + container_name + "/" +
              remove_front_slash(remove_trailing_slash(prefix)),
          0,
          true);
    }

    options.ContinuationToken = response.NextPageToken;
  } while (options.ContinuationToken.HasValue());

  return entries;
}

Status Azure::move_object(const URI& old_uri, const URI& new_uri) {
  RETURN_NOT_OK(copy_blob(old_uri, new_uri));
  RETURN_NOT_OK(remove_blob(old_uri));
  return Status::Ok();
}

Status Azure::copy_blob(const URI& old_uri, const URI& new_uri) {
  auto& c = client();
  if (!old_uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + old_uri.to_string());
  }
  if (!new_uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + new_uri.to_string());
  }

  std::string old_container_name;
  std::string old_blob_path;
  RETURN_NOT_OK(parse_azure_uri(old_uri, &old_container_name, &old_blob_path));
  std::string source_uri = c.GetBlobContainerClient(old_container_name)
                               .GetBlobClient(old_blob_path)
                               .GetUrl();

  std::string new_container_name;
  std::string new_blob_path;
  RETURN_NOT_OK(parse_azure_uri(new_uri, &new_container_name, &new_blob_path));

  try {
    c.GetBlobContainerClient(new_container_name)
        .GetBlobClient(new_blob_path)
        .StartCopyFromUri(source_uri)
        .PollUntilDone(azure_params_.retry_delay_);
  } catch (const ::Azure::Storage::StorageException& e) {
    throw AzureException(
        "Copy blob failed on: " + old_uri.to_string() + "; " + e.Message);
  }

  return Status::Ok();
}

Status Azure::move_dir(const URI& old_uri, const URI& new_uri) {
  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(old_uri, &paths, ""));
  for (const auto& path : paths) {
    const std::string suffix = path.substr(old_uri.to_string().size());
    const URI new_path = new_uri.join_path(suffix);
    RETURN_NOT_OK(move_object(URI(path), new_path));
  }
  return Status::Ok();
}

Status Azure::blob_size(const URI& uri, uint64_t* const nbytes) const {
  auto& c = client();
  assert(nbytes);

  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  std::optional<std::string> error_message = nullopt;

  try {
    ::Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = blob_path;
    options.PageSizeHint = 1;

    auto response = c.GetBlobContainerClient(container_name).ListBlobs(options);

    if (response.Blobs.empty()) {
      error_message = "Blob does not exist.";
    } else {
      *nbytes = static_cast<uint64_t>(response.Blobs[0].BlobSize);
    }
  } catch (const ::Azure::Storage::StorageException& e) {
    error_message = e.Message;
  }

  if (error_message.has_value()) {
    throw AzureException(
        "Get blob size failed on: " + uri.to_string() + error_message.value());
  }

  return Status::Ok();
}

Status Azure::read(
    const URI& uri,
    const off_t offset,
    void* const buffer,
    const uint64_t length,
    const uint64_t read_ahead_length,
    uint64_t* const length_returned) const {
  const auto& c = client();

  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  size_t total_length = length + read_ahead_length;

  ::Azure::Storage::Blobs::DownloadBlobOptions options;
  options.Range = ::Azure::Core::Http::HttpRange();
  options.Range.Value().Length = static_cast<int64_t>(total_length);
  options.Range.Value().Offset = static_cast<int64_t>(offset);

  ::Azure::Storage::Blobs::Models::DownloadBlobResult result;
  try {
    result = c.GetBlobContainerClient(container_name)
                 .GetBlobClient(blob_path)
                 .Download(options)
                 .Value;
  } catch (const ::Azure::Storage::StorageException& e) {
    throw AzureException(
        "Read blob failed on: " + uri.to_string() + "; " + e.Message);
  }

  *length_returned = result.BodyStream->ReadToCount(
      static_cast<uint8_t*>(buffer), total_length);

  if (*length_returned < length) {
    throw AzureException("Read operation read unexpected number of bytes.");
  }

  return Status::Ok();
}

Status Azure::remove_container(const URI& uri) const {
  auto& c = client();

  // Empty container
  RETURN_NOT_OK(empty_container(uri));

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  bool deleted;
  std::string error_message = "";
  try {
    deleted = c.DeleteBlobContainer(container_name).Value.Deleted;
  } catch (const ::Azure::Storage::StorageException& e) {
    deleted = false;
    error_message = "; " + e.Message;
  }

  if (!deleted) {
    throw AzureException(
        "Remove container failed on: " + uri.to_string() + error_message);
  }

  return Status::Ok();
}

Status Azure::remove_blob(const URI& uri) const {
  auto& c = client();

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  bool deleted;
  std::string error_message = "";
  try {
    deleted = c.GetBlobContainerClient(container_name)
                  .DeleteBlob(blob_path)
                  .Value.Deleted;
  } catch (const ::Azure::Storage::StorageException& e) {
    deleted = false;
    error_message = "; " + e.Message;
  }

  if (!deleted) {
    throw AzureException(
        "Remove blob failed on: " + uri.to_string() + error_message);
  }

  return Status::Ok();
}

Status Azure::remove_dir(const URI& uri) const {
  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri, &paths, ""));
  auto status = parallel_for(thread_pool_, 0, paths.size(), [&](size_t i) {
    throw_if_not_ok(remove_blob(URI(paths[i])));
    return Status::Ok();
  });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Azure::touch(const URI& uri) const {
  auto& c = client();

  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  if (uri.to_string().back() == '/') {
    throw AzureException(
        "Cannot create file; URI is a directory: " + uri.to_string());
  }

  bool is_blob;
  RETURN_NOT_OK(this->is_blob(uri, &is_blob));
  if (is_blob) {
    return Status::Ok();
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  try {
    c.GetBlobContainerClient(container_name)
        .GetBlockBlobClient(blob_path)
        .UploadFrom(nullptr, 0);
  } catch (const ::Azure::Storage::StorageException& e) {
    throw AzureException(
        "Touch blob failed on: " + uri.to_string() + "; " + e.Message);
  }

  return Status::Ok();
}

Status Azure::write(
    const URI& uri, const void* const buffer, const uint64_t length) {
  auto write_cache_max_size = azure_params_.write_cache_max_size_;
  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  uint64_t nbytes_filled;
  RETURN_NOT_OK(
      fill_write_cache(write_cache_buffer, buffer, length, &nbytes_filled));

  if (!azure_params_.use_block_list_upload_) {
    if (nbytes_filled != length) {
      std::stringstream errmsg;
      errmsg << "Direct write failed! " << nbytes_filled
             << " bytes written to buffer, " << length << " bytes requested.";
      throw AzureException(errmsg.str());
    } else {
      return Status::Ok();
    }
  }

  if (write_cache_buffer->size() == write_cache_max_size) {
    RETURN_NOT_OK(flush_write_cache(uri, write_cache_buffer, false));
  }

  uint64_t new_length = length - nbytes_filled;
  uint64_t offset = nbytes_filled;
  while (new_length > 0) {
    if (new_length >= write_cache_max_size) {
      RETURN_NOT_OK(write_blocks(
          uri,
          static_cast<const char*>(buffer) + offset,
          write_cache_max_size,
          false));
      offset += write_cache_max_size;
      new_length -= write_cache_max_size;
    } else {
      RETURN_NOT_OK(fill_write_cache(
          write_cache_buffer,
          static_cast<const char*>(buffer) + offset,
          new_length,
          &nbytes_filled));
      offset += nbytes_filled;
      new_length -= nbytes_filled;
    }
  }

  assert(offset == length);

  return Status::Ok();
}

Buffer* Azure::get_write_cache_buffer(const std::string& uri) {
  std::unique_lock<std::mutex> map_lock(write_cache_map_lock_);
  if (write_cache_map_.count(uri) > 0) {
    return &write_cache_map_.at(uri);
  } else {
    return &write_cache_map_[uri];
  }
}

Status Azure::fill_write_cache(
    Buffer* const write_cache_buffer,
    const void* const buffer,
    const uint64_t length,
    uint64_t* const nbytes_filled) {
  assert(write_cache_buffer);
  assert(buffer);
  assert(nbytes_filled);

  *nbytes_filled = std::min(
      azure_params_.write_cache_max_size_ - write_cache_buffer->size(), length);

  if (*nbytes_filled > 0) {
    RETURN_NOT_OK(write_cache_buffer->write(buffer, *nbytes_filled));
  }

  return Status::Ok();
}

Status Azure::flush_write_cache(
    const URI& uri, Buffer* const write_cache_buffer, const bool last_block) {
  assert(write_cache_buffer);

  if (write_cache_buffer->size() > 0) {
    const Status st = write_blocks(
        uri,
        write_cache_buffer->data(),
        write_cache_buffer->size(),
        last_block);
    write_cache_buffer->reset_size();
    RETURN_NOT_OK(st);
  }

  return Status::Ok();
}

Status Azure::write_blocks(
    const URI& uri,
    const void* const buffer,
    const uint64_t length,
    const bool last_block) {
  auto block_list_block_size = azure_params_.block_list_block_size_;
  if (!uri.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + uri.to_string());
  }

  // Ensure that each thread is responsible for exactly block_list_block_size_
  // bytes (except if this is the last block, in which case the final
  // thread should write less). Cap the number of parallel operations at the
  // configured max number. Length must be evenly divisible by
  // block_list_block_size_ unless this is the last block.
  uint64_t num_ops = last_block ?
                         utils::math::ceil(length, block_list_block_size) :
                         (length / block_list_block_size);
  num_ops =
      std::min(std::max(num_ops, uint64_t(1)), azure_params_.max_parallel_ops_);

  if (!last_block && length % block_list_block_size != 0) {
    throw AzureException("Length not evenly divisible by block size");
  }

  BlockListUploadState* state;
  // Protect 'block_list_upload_states_' from concurrent read and writes.
  {
    std::unique_lock<std::mutex> states_lock(block_list_upload_states_lock_);

    auto state_iter = block_list_upload_states_.find(uri.to_string());
    if (state_iter == block_list_upload_states_.end()) {
      // Delete file if it exists (overwrite).
      bool exists;
      RETURN_NOT_OK(is_blob(uri, &exists));
      if (exists) {
        RETURN_NOT_OK(remove_blob(uri));
      }

      // Instantiate the new state.
      BlockListUploadState state;

      // Store the new state.
      const std::pair<
          std::unordered_map<std::string, BlockListUploadState>::iterator,
          bool>
          emplaced = block_list_upload_states_.emplace(
              uri.to_string(), std::move(state));
      assert(emplaced.second);
      state_iter = emplaced.first;
    }

    state = &state_iter->second;
    // We're done reading and writing from 'block_list_upload_states_'.
    // Mutating the 'state' element does not affect the thread-safety of
    // 'block_list_upload_states_'.
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  if (num_ops == 1) {
    const std::string block_id = state->next_block_id();

    const Status st =
        upload_block(container_name, blob_path, buffer, length, block_id);
    state->update_st(st);
    return st;
  } else {
    std::vector<ThreadPool::Task> tasks;
    tasks.reserve(num_ops);
    for (uint64_t i = 0; i < num_ops; i++) {
      const uint64_t begin = i * block_list_block_size;
      const uint64_t end =
          std::min((i + 1) * block_list_block_size - 1, length - 1);
      const char* const thread_buffer =
          reinterpret_cast<const char*>(buffer) + begin;
      const uint64_t thread_buffer_len = end - begin + 1;
      const std::string block_id = state->next_block_id();

      std::function<Status()> upload_block_fn = std::bind(
          &Azure::upload_block,
          this,
          container_name,
          blob_path,
          thread_buffer,
          thread_buffer_len,
          block_id);
      ThreadPool::Task task = thread_pool_->execute(std::move(upload_block_fn));
      tasks.emplace_back(std::move(task));
    }

    const Status st = thread_pool_->wait_all(tasks);
    state->update_st(st);
    return st;
  }

  return Status::Ok();
}

LsObjects Azure::list_blobs_impl(
    const std::string& container_name,
    const std::string& blob_path,
    bool recursive,
    int max_keys,
    optional<std::string>& continuation_token) const {
  try {
    ::Azure::Storage::Blobs::ListBlobsOptions options{
        .Prefix = blob_path,
        .ContinuationToken = to_azure_nullable(continuation_token),
        .PageSizeHint = max_keys};
    auto container_client = client().GetBlobContainerClient(container_name);
    auto to_directory_entry =
        [&container_name](const ::Azure::Storage::Blobs::Models::BlobItem& item)
        -> LsObjects::value_type {
      return {
          "azure://" + container_name + "/" +
              remove_front_slash(remove_trailing_slash(item.Name)),
          item.BlobSize >= 0 ? static_cast<uint64_t>(item.BlobSize) : 0};
    };

    LsObjects result;
    if (recursive) {
      auto response = container_client.ListBlobs(options);

      continuation_token = from_azure_nullable(response.NextPageToken);

      result.reserve(response.Blobs.size());
      std::transform(
          response.Blobs.begin(),
          response.Blobs.end(),
          std::back_inserter(result),
          to_directory_entry);
    } else {
      auto response = container_client.ListBlobsByHierarchy("/", options);

      continuation_token = from_azure_nullable(response.NextPageToken);

      result.reserve(response.Blobs.size() + response.BlobPrefixes.size());
      std::transform(
          response.Blobs.begin(),
          response.Blobs.end(),
          std::back_inserter(result),
          to_directory_entry);
      std::transform(
          response.BlobPrefixes.begin(),
          response.BlobPrefixes.end(),
          std::back_inserter(result),
          [&container_name](std::string& name) -> LsObjects::value_type {
            return {
                "azure://" + container_name + "/" +
                    remove_front_slash(add_trailing_slash(name)),
                0};
          });
    }

    return result;
  } catch (const ::Azure::Storage::StorageException& e) {
    throw AzureException(
        "List blobs failed on: " + blob_path + "; " + e.Message);
  }
}

Status Azure::upload_block(
    const std::string& container_name,
    const std::string& blob_path,
    const void* const buffer,
    const uint64_t length,
    const std::string& block_id) {
  const auto& c = client();
  ::Azure::Core::IO::MemoryBodyStream stream(
      static_cast<const uint8_t*>(buffer), static_cast<size_t>(length));
  try {
    c.GetBlobContainerClient(container_name)
        .GetBlockBlobClient(blob_path)
        .StageBlock(block_id, stream);
  } catch (const ::Azure::Storage::StorageException& e) {
    throw AzureException(
        "Upload block failed on: " + blob_path + "; " + e.Message);
  }

  return Status::Ok();
}

Status Azure::parse_azure_uri(
    const URI& uri,
    std::string* const container_name,
    std::string* const blob_path) {
  assert(uri.is_azure());
  const std::string uri_str = uri.to_string();

  const static std::string azure_prefix = "azure://";
  assert(uri_str.rfind(azure_prefix, 0) == 0);

  if (uri_str.size() == azure_prefix.size()) {
    if (container_name)
      *container_name = "";
    if (blob_path)
      *blob_path = "";
    return Status::Ok();
  }

  // Find the '/' after the container name.
  const size_t separator = uri_str.find('/', azure_prefix.size() + 1);

  // There is only a container name if there isn't a separating slash.
  if (separator == std::string::npos) {
    const size_t c_pos_start = azure_prefix.size();
    const size_t c_pos_end = uri_str.size();
    if (container_name)
      *container_name = uri_str.substr(c_pos_start, c_pos_end - c_pos_start);
    if (blob_path)
      *blob_path = "";
    return Status::Ok();
  }

  // There is only a container name if there aren't any characters past the
  // separating slash.
  if (uri_str.size() == separator) {
    const size_t c_pos_start = azure_prefix.size();
    const size_t c_pos_end = separator;
    if (container_name)
      *container_name = uri_str.substr(c_pos_start, c_pos_end - c_pos_start);
    if (blob_path)
      *blob_path = "";
    return Status::Ok();
  }

  const size_t c_pos_start = azure_prefix.size();
  const size_t c_pos_end = separator;
  const size_t b_pos_start = separator + 1;
  const size_t b_pos_end = uri_str.size();

  if (container_name)
    *container_name = uri_str.substr(c_pos_start, c_pos_end - c_pos_start);
  if (blob_path)
    *blob_path = uri_str.substr(b_pos_start, b_pos_end - b_pos_start);

  return Status::Ok();
}

/* Generates the next base64-encoded block id. */
std::string Azure::BlockListUploadState::next_block_id() {
  const uint64_t block_id = next_block_id_++;
  const std::string block_id_str = std::to_string(block_id);

  // Pad the block id string with enough leading zeros to support
  // the maximum number of blocks (50,000). All block ids must be
  // of equal length among a single blob.
  const int block_id_chars = 5;
  std::vector<uint8_t> padded_block_id(
      block_id_chars - block_id_str.length(), '0');
  std::copy(
      block_id_str.begin(),
      block_id_str.end(),
      std::back_inserter(padded_block_id));

  const std::string b64_block_id_str =
      ::Azure::Core::Convert::Base64Encode(padded_block_id);

  block_ids_.emplace_back(b64_block_id_str);

  return b64_block_id_str;
}

}  // namespace tiledb::sm

#if defined(_WIN32)
#include <azure/core/http/win_http_transport.hpp>
std::shared_ptr<::Azure::Core::Http::HttpTransport> create_transport(
    const tiledb::sm::SSLConfig& ssl_cfg) {
  ::Azure::Core::Http::WinHttpTransportOptions transport_opts;

  if (!ssl_cfg.ca_file().empty()) {
    LOG_WARN("Azure ignores the `ssl.ca_file` configuration key on Windows.");
  }

  if (!ssl_cfg.ca_path().empty()) {
    LOG_WARN("Azure ignores the `ssl.ca_path` configuration key on Windows.");
  }

  if (ssl_cfg.verify() == false) {
    transport_opts.IgnoreUnknownCertificateAuthority = true;
  }

  return make_shared<::Azure::Core::Http::WinHttpTransport>(
      HERE(), transport_opts);
}
#else
#include <azure/core/http/curl_transport.hpp>
std::shared_ptr<::Azure::Core::Http::HttpTransport> create_transport(
    const tiledb::sm::SSLConfig& ssl_cfg) {
  ::Azure::Core::Http::CurlTransportOptions transport_opts;

  if (!ssl_cfg.ca_file().empty()) {
    transport_opts.CAInfo = ssl_cfg.ca_file();
  }

  if (!ssl_cfg.ca_path().empty()) {
    transport_opts.CAPath = ssl_cfg.ca_path();
  }

  if (ssl_cfg.verify() == false) {
    transport_opts.SslVerifyPeer = false;
  }

  return make_shared<::Azure::Core::Http::CurlTransport>(
      HERE(), transport_opts);
}
#endif

#endif
