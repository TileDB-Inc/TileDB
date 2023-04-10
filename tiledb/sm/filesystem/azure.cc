/**
 * @file   azure.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#if !defined(NOMINMAX)
#define NOMINMAX  // avoid min/max macros from windows headers
#endif

#include <future>

#include <azure/core/diagnostics/logger.hpp>
#include <azure/storage/blobs.hpp>

#include "tiledb/common/common.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/platform/cert_file.h"
#include "tiledb/sm/filesystem/azure.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Azure::Azure()
    : write_cache_max_size_(0)
    , max_parallel_ops_(1)
    , block_list_block_size_(0)
    , use_block_list_upload_(false) {
}

Azure::~Azure() {
}

/* ********************************* */
/*                 API               */
/* ********************************* */

Status Azure::init(const Config& config, ThreadPool* const thread_pool) {
  if (thread_pool == nullptr) {
    return LOG_STATUS(
        Status_AzureError("Can't initialize with null thread pool."));
  }

  thread_pool_ = thread_pool;

  bool found;
  char* tmp = nullptr;

  std::string account_name =
      config.get("vfs.azure.storage_account_name", &found);
  assert(found);
  if (account_name.empty() &&
      ((tmp = getenv("AZURE_STORAGE_ACCOUNT")) != nullptr)) {
    account_name = std::string(tmp);
  }

  std::string account_key = config.get("vfs.azure.storage_account_key", &found);
  assert(found);
  if (account_key.empty() && ((tmp = getenv("AZURE_STORAGE_KEY")) != nullptr)) {
    account_key = std::string(tmp);
  }

  if (!config.get("vfs.azure.storage_sas_token", &found).empty() ||
      getenv("AZURE_STORAGTE_SAS_TOKEN") != nullptr) {
    LOG_WARN(
        "The 'vfs.azure.storage_sas_token' option is deprecated and unused. "
        "Make sure the 'vfs.azure.blob_endpoint' property has the SAS token "
        "instead.");
  }

  std::string blob_endpoint = config.get("vfs.azure.blob_endpoint", &found);
  assert(found);
  if (blob_endpoint.empty() &&
      ((tmp = getenv("AZURE_BLOB_ENDPOINT")) != nullptr)) {
    blob_endpoint = std::string(tmp);
  }
  if (blob_endpoint.empty()) {
    LOG_WARN("The 'vfs.azure.blob_endpoint' option is not specified.");
  }
  if (!blob_endpoint.empty() &&
      !(utils::parse::starts_with(blob_endpoint, "http://") ||
        utils::parse::starts_with(blob_endpoint, "https://"))) {
    LOG_WARN(
        "The 'vfs.azure.blob_endpoint' option should include the scheme (HTTP "
        "or HTTPS).");
  }

  RETURN_NOT_OK(config.get<uint64_t>(
      "vfs.azure.max_parallel_ops", &max_parallel_ops_, &found));
  assert(found);
  RETURN_NOT_OK(config.get<uint64_t>(
      "vfs.azure.block_list_block_size", &block_list_block_size_, &found));
  assert(found);
  RETURN_NOT_OK(config.get<bool>(
      "vfs.azure.use_block_list_upload", &use_block_list_upload_, &found));
  assert(found);

  write_cache_max_size_ = max_parallel_ops_ * block_list_block_size_;

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

  // Construct the Azure SDK blob service client.
  // We pass a shared kay if it was specified.
  if (!account_key.empty()) {
    client_ =
        tdb_unique_ptr<::Azure::Storage::Blobs::BlobServiceClient>(tdb_new(
            ::Azure::Storage::Blobs::BlobServiceClient,
            blob_endpoint,
            make_shared<::Azure::Storage::StorageSharedKeyCredential>(
                HERE(), account_name, account_key)));
  } else {
    client_ = tdb_unique_ptr<::Azure::Storage::Blobs::BlobServiceClient>(
        tdb_new(::Azure::Storage::Blobs::BlobServiceClient, blob_endpoint));
  }

  return Status::Ok();
}

Status Azure::create_container(const URI& uri) const {
  assert(client_);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  bool created;
  std::string error_message = "";
  try {
    created =
        client_->GetBlobContainerClient(container_name).Create().Value.Created;
  } catch (const ::Azure::Storage::StorageException& e) {
    created = false;
    error_message = "; " + e.Message;
  }

  if (!created) {
    return LOG_STATUS(Status_AzureError(std::string(
        "Create container failed on: " + uri.to_string() + error_message)));
  }

  return Status::Ok();
}

Status Azure::empty_container(const URI& container) const {
  assert(client_);

  return remove_dir(container);
}

Status Azure::flush_blob(const URI& uri) {
  assert(client_);

  if (!use_block_list_upload_) {
    return flush_blob_direct(uri);
  }

  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
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
    client_->GetBlobContainerClient(container_name)
        .GetBlockBlobClient(blob_path)
        .CommitBlockList(std::vector(block_ids.begin(), block_ids.end()));
  } catch (const ::Azure::Storage::StorageException& e) {
    return LOG_STATUS(Status_AzureError(
        "Flush blob failed on: " + uri.to_string() + "; " + e.Message));
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
  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  if (write_cache_buffer->size() == 0) {
    return Status::Ok();
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  try {
    client_->GetBlobContainerClient(container_name)
        .GetBlockBlobClient(blob_path)
        .UploadFrom(
            static_cast<uint8_t*>(write_cache_buffer->data()),
            static_cast<size_t>(write_cache_buffer->size()));
  } catch (const ::Azure::Storage::StorageException& e) {
    return LOG_STATUS(Status_AzureError(
        "Flush blob failed on: " + uri.to_string() + "; " + e.Message));
  }

  // Protect 'write_cache_map_' from multiple writers.
  {
    std::unique_lock<std::mutex> cache_lock(write_cache_map_lock_);
    write_cache_map_.erase(uri.to_string());
  }

  return Status::Ok();
}

Status Azure::is_empty_container(const URI& uri, bool* is_empty) const {
  assert(client_);
  assert(is_empty);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  ::Azure::Storage::Blobs::ListBlobsOptions options;
  options.PageSizeHint = 1;
  try {
    *is_empty = client_->GetBlobContainerClient(container_name)
                    .ListBlobs(options)
                    .Blobs.empty();
  } catch (const ::Azure::Storage::StorageException& e) {
    return LOG_STATUS(Status_AzureError(
        "List blobs failed on: " + uri.to_string() + "; " + e.Message));
  }

  return Status::Ok();
}

Status Azure::is_container(const URI& uri, bool* const is_container) const {
  assert(is_container);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  return this->is_container(container_name, is_container);
}

Status Azure::is_container(
    const std::string& container_name, bool* const is_container) const {
  assert(client_);
  assert(is_container);

  try {
    client_->GetBlobContainerClient(container_name).GetProperties();
  } catch (const ::Azure::Storage::StorageException& e) {
    if (e.StatusCode == ::Azure::Core::Http::HttpStatusCode::NotFound) {
      *is_container = false;
      return Status_Ok();
    }
    return LOG_STATUS(Status_AzureError(
        "Get blob properties failed on: " + container_name + "; " + e.Message));
  }

  *is_container = true;
  return Status_Ok();
}

Status Azure::is_dir(const URI& uri, bool* const exists) const {
  assert(client_);
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
  assert(client_);
  assert(is_blob);

  try {
    client_->GetBlobContainerClient(container_name)
        .GetBlobClient(blob_path)
        .GetProperties();
  } catch (const ::Azure::Storage::StorageException& e) {
    if (e.StatusCode == ::Azure::Core::Http::HttpStatusCode::NotFound) {
      *is_blob = false;
      return Status_Ok();
    }
    return LOG_STATUS(Status_AzureError(
        "Get blob properties failed on: " + blob_path + "; " + e.Message));
  }

  *is_blob = true;
  return Status_Ok();
}

std::string Azure::remove_front_slash(const std::string& path) const {
  if (path.front() == '/') {
    return path.substr(1, path.length());
  }

  return path;
}

std::string Azure::add_trailing_slash(const std::string& path) const {
  if (path.back() != '/') {
    return path + "/";
  }

  return path;
}

std::string Azure::remove_trailing_slash(const std::string& path) const {
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
  assert(client_);
  assert(paths);

  auto&& [st, entries] = ls_with_sizes(uri, delimiter, max_paths);
  RETURN_NOT_OK(st);

  for (auto& fs : *entries) {
    paths->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

tuple<Status, optional<std::vector<directory_entry>>> Azure::ls_with_sizes(
    const URI& uri, const std::string& delimiter, int max_paths) const {
  assert(client_);

  const URI uri_dir = uri.add_trailing_slash();

  if (!uri_dir.is_azure()) {
    auto st = LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri_dir.to_string())));
    return {st, nullopt};
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK_TUPLE(
      parse_azure_uri(uri_dir, &container_name, &blob_path), nullopt);

  auto container_client = client_->GetBlobContainerClient(container_name);

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
      auto st = LOG_STATUS(Status_AzureError(
          "List blobs failed on: " + uri_dir.to_string() + "; " + e.Message));
      return {st, nullopt};
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

  return {Status::Ok(), entries};
}

Status Azure::move_object(const URI& old_uri, const URI& new_uri) {
  assert(client_);
  RETURN_NOT_OK(copy_blob(old_uri, new_uri));
  RETURN_NOT_OK(remove_blob(old_uri));
  return Status::Ok();
}

Status Azure::copy_blob(const URI& old_uri, const URI& new_uri) {
  assert(client_);

  if (!old_uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + old_uri.to_string())));
  }

  if (!new_uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + new_uri.to_string())));
  }

  std::string old_container_name;
  std::string old_blob_path;
  RETURN_NOT_OK(parse_azure_uri(old_uri, &old_container_name, &old_blob_path));
  std::string source_uri = client_->GetBlobContainerClient(old_container_name)
                               .GetBlobClient(old_blob_path)
                               .GetUrl();

  std::string new_container_name;
  std::string new_blob_path;
  RETURN_NOT_OK(parse_azure_uri(new_uri, &new_container_name, &new_blob_path));

  try {
    client_->GetBlobContainerClient(new_container_name)
        .GetBlobClient(new_blob_path)
        .StartCopyFromUri(source_uri)
        .PollUntilDone(
            std::chrono::milliseconds(constants::azure_attempt_sleep_ms));
  } catch (const ::Azure::Storage::StorageException& e) {
    return LOG_STATUS(Status_AzureError(
        "Copy blob failed on: " + old_uri.to_string() + "; " + e.Message));
  }

  return Status::Ok();
}

Status Azure::move_dir(const URI& old_uri, const URI& new_uri) {
  assert(client_);

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
  assert(client_);
  assert(nbytes);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  std::optional<std::string> error_message = nullopt;

  try {
    ::Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = blob_path;
    options.PageSizeHint = 1;

    auto response =
        client_->GetBlobContainerClient(container_name).ListBlobs(options);

    if (response.Blobs.empty()) {
      error_message = "Blob does not exist.";
    }

    *nbytes = static_cast<uint64_t>(response.Blobs[0].BlobSize);
  } catch (const ::Azure::Storage::StorageException& e) {
    error_message = e.Message;
  }

  if (error_message.has_value()) {
    return LOG_STATUS(Status_AzureError(
        "Get blob size failed on: " + uri.to_string() + error_message.value()));
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
  assert(client_);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
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
    result = client_->GetBlobContainerClient(container_name)
                 .GetBlobClient(blob_path)
                 .Download(options)
                 .Value;
  } catch (const ::Azure::Storage::StorageException& e) {
    return LOG_STATUS(Status_AzureError(
        "Read blob failed on: " + uri.to_string() + "; " + e.Message));
  }

  *length_returned = result.BodyStream->ReadToCount(
      static_cast<uint8_t*>(buffer), total_length);

  if (*length_returned < length) {
    return LOG_STATUS(Status_AzureError(
        std::string("Read operation read unexpected number of bytes.")));
  }

  return Status::Ok();
}

Status Azure::remove_container(const URI& uri) const {
  assert(client_);

  // Empty container
  RETURN_NOT_OK(empty_container(uri));

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  bool deleted;
  std::string error_message = "";
  try {
    deleted = client_->DeleteBlobContainer(container_name).Value.Deleted;
  } catch (const ::Azure::Storage::StorageException& e) {
    deleted = false;
    error_message = "; " + e.Message;
  }

  if (!deleted) {
    return LOG_STATUS(Status_AzureError(std::string(
        "Remove container failed on: " + uri.to_string() + error_message)));
  }

  return Status::Ok();
}

Status Azure::remove_blob(const URI& uri) const {
  assert(client_);

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  bool deleted;
  std::string error_message = "";
  try {
    deleted = client_->GetBlobContainerClient(container_name)
                  .DeleteBlob(blob_path)
                  .Value.Deleted;
  } catch (const ::Azure::Storage::StorageException& e) {
    deleted = false;
    error_message = "; " + e.Message;
  }

  if (!deleted) {
    return LOG_STATUS(Status_AzureError(std::string(
        "Remove blob failed on: " + uri.to_string() + error_message)));
  }

  return Status::Ok();
}

Status Azure::remove_dir(const URI& uri) const {
  assert(client_);

  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri, &paths, ""));
  auto status = parallel_for(thread_pool_, 0, paths.size(), [&](size_t i) {
    RETURN_NOT_OK(remove_blob(URI(paths[i])));
    return Status::Ok();
  });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Azure::touch(const URI& uri) const {
  assert(client_);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  if (uri.to_string().back() == '/') {
    return LOG_STATUS(Status_AzureError(std::string(
        "Cannot create file; URI is a directory: " + uri.to_string())));
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
    client_->GetBlobContainerClient(container_name)
        .GetBlockBlobClient(blob_path)
        .UploadFrom(nullptr, 0);
  } catch (const ::Azure::Storage::StorageException& e) {
    return LOG_STATUS(Status_AzureError(
        "Touch blob failed on: " + uri.to_string() + "; " + e.Message));
  }

  return Status::Ok();
}

Status Azure::write(
    const URI& uri, const void* const buffer, const uint64_t length) {
  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  uint64_t nbytes_filled;
  RETURN_NOT_OK(
      fill_write_cache(write_cache_buffer, buffer, length, &nbytes_filled));

  if (!use_block_list_upload_) {
    if (nbytes_filled != length) {
      std::stringstream errmsg;
      errmsg << "Direct write failed! " << nbytes_filled
             << " bytes written to buffer, " << length << " bytes requested.";
      return LOG_STATUS(Status_AzureError(errmsg.str()));
    } else {
      return Status::Ok();
    }
  }

  if (write_cache_buffer->size() == write_cache_max_size_) {
    RETURN_NOT_OK(flush_write_cache(uri, write_cache_buffer, false));
  }

  uint64_t new_length = length - nbytes_filled;
  uint64_t offset = nbytes_filled;
  while (new_length > 0) {
    if (new_length >= write_cache_max_size_) {
      RETURN_NOT_OK(write_blocks(
          uri,
          static_cast<const char*>(buffer) + offset,
          write_cache_max_size_,
          false));
      offset += write_cache_max_size_;
      new_length -= write_cache_max_size_;
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

  *nbytes_filled =
      std::min(write_cache_max_size_ - write_cache_buffer->size(), length);

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
  if (!uri.is_azure()) {
    return LOG_STATUS(Status_AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  // Ensure that each thread is responsible for exactly block_list_block_size_
  // bytes (except if this is the last block, in which case the final
  // thread should write less). Cap the number of parallel operations at the
  // configured max number. Length must be evenly divisible by
  // block_list_block_size_ unless this is the last block.
  uint64_t num_ops = last_block ?
                         utils::math::ceil(length, block_list_block_size_) :
                         (length / block_list_block_size_);
  num_ops = std::min(std::max(num_ops, uint64_t(1)), max_parallel_ops_);

  if (!last_block && length % block_list_block_size_ != 0) {
    return LOG_STATUS(
        Status_AzureError("Length not evenly divisible by block size"));
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
    // We're done reading and writing from 'block_list_upload_states_'. Mutating
    // the 'state' element does not affect the thread-safety of
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
      const uint64_t begin = i * block_list_block_size_;
      const uint64_t end =
          std::min((i + 1) * block_list_block_size_ - 1, length - 1);
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

Status Azure::upload_block(
    const std::string& container_name,
    const std::string& blob_path,
    const void* const buffer,
    const uint64_t length,
    const std::string& block_id) {
  ::Azure::Core::IO::MemoryBodyStream stream(
      static_cast<const uint8_t*>(buffer), static_cast<size_t>(length));
  try {
    client_->GetBlobContainerClient(container_name)
        .GetBlockBlobClient(blob_path)
        .StageBlock(block_id, stream);
  } catch (const ::Azure::Storage::StorageException& e) {
    return LOG_STATUS(Status_AzureError(
        "Upload block failed on: " + blob_path + "; " + e.Message));
  }

  return Status::Ok();
}

Status Azure::parse_azure_uri(
    const URI& uri,
    std::string* const container_name,
    std::string* const blob_path) const {
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

}  // namespace sm
}  // namespace tiledb

#endif
