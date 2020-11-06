/**
 * @file   azure.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include <put_block_list_request_base.h>
#include <future>

#include "tiledb/common/logger.h"
#include "tiledb/sm/filesystem/azure.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb::common;

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
        Status::AzureError("Can't initialize with null thread pool."));
  }

  thread_pool_ = thread_pool;

  bool found;
  char* tmp = NULL;

  std::string account_name =
      config.get("vfs.azure.storage_account_name", &found);
  assert(found);
  if (account_name.empty() &&
      ((tmp = getenv("AZURE_STORAGE_ACCOUNT")) != NULL)) {
    account_name = std::string(tmp);
  }

  std::string account_key = config.get("vfs.azure.storage_account_key", &found);
  assert(found);
  if (account_key.empty() && ((tmp = getenv("AZURE_STORAGE_KEY")) != NULL)) {
    account_key = std::string(getenv("AZURE_STORAGE_KEY"));
  }

  std::string blob_endpoint = config.get("vfs.azure.blob_endpoint", &found);
  assert(found);
  if (blob_endpoint.empty() &&
      ((tmp = getenv("AZURE_BLOB_ENDPOINT")) != NULL)) {
    blob_endpoint = std::string(getenv("AZURE_BLOB_ENDPOINT"));
  }

  bool use_https;
  RETURN_NOT_OK(config.get<bool>("vfs.azure.use_https", &use_https, &found));
  assert(found);
  // note that the default here is use_https=true, so the logic below is
  // inverted
  if (use_https && ((tmp = getenv("AZURE_USE_HTTPS")) != NULL)) {
    std::string use_https_env(tmp);
    use_https = (!use_https_env.empty());
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

  // The Azurite default test account name is 'devstoreaccount1'. If this
  // is the account name, we must flag the credential constructor that
  // it must authenticate with a storage emulator instead of a production
  // instance of Azure.
  const bool under_test = account_name == "devstoreaccount1" ? true : false;

  std::shared_ptr<azure::storage_lite::shared_key_credential> credential =
      std::make_shared<azure::storage_lite::shared_key_credential>(
          account_name, account_key, under_test);

  std::shared_ptr<azure::storage_lite::storage_account> account =
      std::make_shared<azure::storage_lite::storage_account>(
          account_name, credential, use_https, blob_endpoint);

  // Construct the Azure SDK blob client with a concurrency level
  // equal to 'thread_pool_->concurrency_level'. Internally, the client
  // will allocate an equal number of libcurl sessions with
  // 'curl_easy_init'. This ensures that our 'thread_pool_' threads
  // will not block on the blob client's internal request queue
  // unless the user is performing concurrent I/O on this instance.
#ifdef __linux__
  // Get CA Cert bundle file from global state. This is initialized and cached
  // if detected. We have only had issues with finding the certificate path on
  // Linux.
  const std::string cert_file =
      global_state::GlobalState::GetGlobalState().cert_file();
  client_ = std::make_shared<azure::storage_lite::blob_client>(
      account, thread_pool_->concurrency_level(), cert_file);
#else
  client_ = std::make_shared<azure::storage_lite::blob_client>(
      account, thread_pool_->concurrency_level());
#endif

  // The Azure SDK does not provide a way to configure the retry
  // policy or construct a client with our own retry policy. This
  // re-assigns the context with our own retry policy.
  *client_->context() = azure::storage_lite::executor_context(
      std::make_shared<azure::storage_lite::tinyxml2_parser>(),
      std::make_shared<AzureRetryPolicy>());

  return Status::Ok();
}

Status Azure::create_container(const URI& uri) const {
  assert(client_);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->create_container(container_name);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Create container failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Create container failed on: " + uri.to_string())));
  }

  return wait_for_container_to_propagate(container_name);
}

Status Azure::wait_for_container_to_propagate(
    const std::string& container_name) const {
  unsigned attempts = 0;
  while (attempts++ < constants::azure_max_attempts) {
    bool is_container;
    RETURN_NOT_OK(this->is_container(container_name, &is_container));

    if (is_container) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::azure_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::AzureError(std::string(
      "Timed out waiting on container to propogate: " + container_name)));
}

Status Azure::wait_for_container_to_be_deleted(
    const std::string& container_name) const {
  assert(client_);

  unsigned attempts = 0;
  while (attempts++ < constants::azure_max_attempts) {
    bool is_container;
    RETURN_NOT_OK(this->is_container(container_name, &is_container));
    if (!is_container) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::azure_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::AzureError(std::string(
      "Timed out waiting on container to be deleted: " + container_name)));
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
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  const Status flush_write_cache_st =
      flush_write_cache(uri, write_cache_buffer, true);

  std::unique_lock<std::mutex> states_lock(block_list_upload_states_lock_);

  if (block_list_upload_states_.count(uri.to_string()) == 0) {
    return flush_write_cache_st;
  }

  BlockListUploadState* const state =
      &block_list_upload_states_.at(uri.to_string());

  states_lock.unlock();

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
    remove_blob(uri);

    // Release all instance state associated with this block list
    // transactions.
    finish_block_list_upload(uri);

    return st;
  }

  // Build the block list to commit.
  const std::list<std::string> block_ids = state->get_block_ids();
  std::vector<azure::storage_lite::put_block_list_request_base::block_item>
      block_list;
  block_list.reserve(block_ids.size());
  for (const auto& block_id : state->get_block_ids()) {
    azure::storage_lite::put_block_list_request_base::block_item block;
    block.id = block_id;
    block.type = azure::storage_lite::put_block_list_request_base::block_type::
        uncommitted;
    block_list.emplace_back(block);
  }

  // We do not store any custom metadata with the blob.
  std::vector<std::pair<std::string, std::string>> empty_metadata;

  // Release all instance state associated with this block list
  // transactions so that we can safely return if the following
  // request failed.
  finish_block_list_upload(uri);

  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->put_block_list(
          container_name, blob_path, block_list, empty_metadata);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Flush blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Flush blob failed on: " + uri.to_string())));
  }

  return wait_for_blob_to_propagate(container_name, blob_path);
}

void Azure::finish_block_list_upload(const URI& uri) {
  // Protect 'block_list_upload_states_' from multiple writers.
  std::unique_lock<std::mutex> states_lock(block_list_upload_states_lock_);
  block_list_upload_states_.erase(uri.to_string());
  states_lock.unlock();

  // Protect 'write_cache_map_' from multiple writers.
  std::unique_lock<std::mutex> cache_lock(write_cache_map_lock_);
  write_cache_map_.erase(uri.to_string());
  cache_lock.unlock();
}

Status Azure::flush_blob_direct(const URI& uri) {
  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  if (write_cache_buffer->size() == 0) {
    return Status::Ok();
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  // We do not store any custom metadata with the blob.
  std::vector<std::pair<std::string, std::string>> empty_metadata;

  // Unlike the 'upload_block_from_buffer' interface used in
  // the block list upload path, there is not an interface to
  // upload a single blob with a buffer. There is only
  // 'upload_block_blob_from_stream'. Here, we construct a
  // zero-copy stream buffer.
  ZeroCopyStreamBuffer zc_stream_buffer(
      static_cast<char*>(write_cache_buffer->data()),
      write_cache_buffer->size());
  std::istream zc_istream(&zc_stream_buffer);

  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->upload_block_blob_from_stream(
          container_name,
          blob_path,
          zc_istream,
          empty_metadata,
          write_cache_buffer->size());
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Flush blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Flush blob failed on: " + uri.to_string())));
  }

  // Protect 'write_cache_map_' from multiple writers.
  std::unique_lock<std::mutex> cache_lock(write_cache_map_lock_);
  write_cache_map_.erase(uri.to_string());
  cache_lock.unlock();

  return wait_for_blob_to_propagate(container_name, blob_path);
}

Status Azure::is_empty_container(const URI& uri, bool* is_empty) const {
  assert(client_);
  assert(is_empty);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  std::future<azure::storage_lite::storage_outcome<
      azure::storage_lite::list_blobs_segmented_response>>
      result = client_->list_blobs_segmented(container_name, "", "", "", 1);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("List blobs failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<
      azure::storage_lite::list_blobs_segmented_response>
      outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("List blobs failed on: " + uri.to_string())));
  }

  azure::storage_lite::list_blobs_segmented_response response =
      outcome.response();

  *is_empty = response.blobs.empty();

  return Status::Ok();
}

Status Azure::is_container(const URI& uri, bool* const is_container) const {
  assert(is_container);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
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

  std::future<azure::storage_lite::storage_outcome<
      azure::storage_lite::container_property>>
      result = client_->get_container_properties(container_name);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Get container properties failed on: " + container_name)));
  }

  azure::storage_lite::storage_outcome<azure::storage_lite::container_property>
      outcome = result.get();
  if (!outcome.success()) {
    *is_container = false;
    return Status::Ok();
  }

  azure::storage_lite::container_property response = outcome.response();

  *is_container = response.valid();
  return Status::Ok();
}

Status Azure::is_dir(const URI& uri, bool* const exists) const {
  assert(client_);
  assert(exists);

  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri, &paths, "/", 1));
  *exists = (bool)paths.size();
  return Status::Ok();
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

  std::future<
      azure::storage_lite::storage_outcome<azure::storage_lite::blob_property>>
      result = client_->get_blob_properties(container_name, blob_path);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Get blob properties failed on: " + blob_path)));
  }

  azure::storage_lite::storage_outcome<azure::storage_lite::blob_property>
      outcome = result.get();
  if (!outcome.success()) {
    *is_blob = false;
    return Status::Ok();
  }

  azure::storage_lite::blob_property response = outcome.response();

  *is_blob = response.valid();
  return Status::Ok();
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

  const URI uri_dir = uri.add_trailing_slash();

  if (!uri_dir.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri_dir.to_string())));
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri_dir, &container_name, &blob_path));

  std::string continuation_token = "";
  do {
    std::future<azure::storage_lite::storage_outcome<
        azure::storage_lite::list_blobs_segmented_response>>
        result = client_->list_blobs_segmented(
            container_name,
            delimiter,
            continuation_token,
            blob_path,
            max_paths > 0 ? max_paths : 5000);
    if (!result.valid()) {
      return LOG_STATUS(Status::AzureError(
          std::string("List blobs failed on: " + uri_dir.to_string())));
    }

    azure::storage_lite::storage_outcome<
        azure::storage_lite::list_blobs_segmented_response>
        outcome = result.get();
    if (!outcome.success()) {
      return LOG_STATUS(Status::AzureError(
          std::string("List blobs failed on: " + uri_dir.to_string())));
    }

    azure::storage_lite::list_blobs_segmented_response response =
        outcome.response();

    for (const auto& blob : response.blobs) {
      paths->emplace_back(
          "azure://" + container_name + "/" +
          remove_front_slash(remove_trailing_slash(blob.name)));
    }

    continuation_token = response.next_marker;
  } while (!continuation_token.empty());

  return Status::Ok();
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
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + old_uri.to_string())));
  }

  if (!new_uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + new_uri.to_string())));
  }

  std::string old_container_name;
  std::string old_blob_path;
  RETURN_NOT_OK(parse_azure_uri(old_uri, &old_container_name, &old_blob_path));

  std::string new_container_name;
  std::string new_blob_path;
  RETURN_NOT_OK(parse_azure_uri(new_uri, &new_container_name, &new_blob_path));

  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->start_copy(
          old_container_name, old_blob_path, new_container_name, new_blob_path);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Copy blob failed on: " + old_uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Copy blob failed on: " + old_uri.to_string())));
  }

  return wait_for_blob_to_propagate(new_container_name, new_blob_path);
}

Status Azure::wait_for_blob_to_propagate(
    const std::string& container_name, const std::string& blob_path) const {
  assert(client_);

  unsigned attempts = 0;
  while (attempts++ < constants::azure_max_attempts) {
    bool is_blob;
    RETURN_NOT_OK(this->is_blob(container_name, blob_path, &is_blob));
    if (is_blob) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::azure_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::AzureError(
      std::string("Timed out waiting on blob to propogate: " + blob_path)));
}

Status Azure::wait_for_blob_to_be_deleted(
    const std::string& container_name, const std::string& blob_path) const {
  assert(client_);

  unsigned attempts = 0;
  while (attempts++ < constants::azure_max_attempts) {
    bool is_blob;
    RETURN_NOT_OK(this->is_blob(container_name, blob_path, &is_blob));
    if (!is_blob) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::azure_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::AzureError(
      std::string("Timed out waiting on blob to be deleted: " + blob_path)));
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
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  std::future<azure::storage_lite::storage_outcome<
      azure::storage_lite::list_blobs_segmented_response>>
      result =
          client_->list_blobs_segmented(container_name, "", "", blob_path, 1);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Get blob size failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<
      azure::storage_lite::list_blobs_segmented_response>
      outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Get blob size failed on: " + uri.to_string())));
  }

  azure::storage_lite::list_blobs_segmented_response response =
      outcome.response();

  if (response.blobs.empty()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Get blob size failed on: " + uri.to_string())));
  }

  const azure::storage_lite::list_blobs_segmented_item& blob =
      response.blobs[0];

  *nbytes = blob.content_length;

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
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  std::stringstream ss;
  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->download_blob_to_stream(
          container_name, blob_path, offset, length + read_ahead_length, ss);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Read blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Read blob failed on: " + uri.to_string())));
  }

  ss.read(static_cast<char*>(buffer), length + read_ahead_length);
  *length_returned = ss.gcount();

  if (*length_returned < length) {
    return LOG_STATUS(Status::AzureError(
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

  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->delete_container(container_name);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Remove container failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Remove container failed on: " + uri.to_string())));
  }

  return wait_for_container_to_be_deleted(container_name);
}

Status Azure::remove_blob(const URI& uri) const {
  assert(client_);

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->delete_blob(
          container_name, blob_path, false /* delete_snapshots */);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Remove blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Remove blob failed on: " + uri.to_string())));
  }

  return wait_for_blob_to_be_deleted(container_name, blob_path);
}

Status Azure::remove_dir(const URI& uri) const {
  assert(client_);

  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri, &paths, ""));
  for (const auto& path : paths) {
    RETURN_NOT_OK(remove_blob(URI(path)));
  }

  return Status::Ok();
}

Status Azure::touch(const URI& uri) const {
  assert(client_);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  if (uri.to_string().back() == '/') {
    return LOG_STATUS(Status::AzureError(std::string(
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

  std::stringstream empty_ss;
  std::vector<std::pair<std::string, std::string>> empty_metadata;
  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->upload_block_blob_from_stream(
          container_name, blob_path, empty_ss, empty_metadata);
  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Touch blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Touch blob failed on: " + uri.to_string())));
  }

  return Status::Ok();
}

Status Azure::write(
    const URI& uri, const void* const buffer, const uint64_t length) {
  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
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
      return LOG_STATUS(Status::AzureError(errmsg.str()));
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
    return LOG_STATUS(Status::AzureError(
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
        Status::AzureError("Length not evenly divisible by block size"));
  }

  // Protect 'block_list_upload_states_' from concurrent read and writes.
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

  BlockListUploadState* const state = &state_iter->second;

  // We're done reading and writing from 'block_list_upload_states_'. Mutating
  // the 'state' element does not affect the thread-safety of
  // 'block_list_upload_states_'.
  states_lock.unlock();

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
  // The 'const_cast' is necessary because the SDK API requires a
  // non-const 'buffer'. However, this is safe because the SDK does
  // not actually mutate 'buffer'.
  //
  // This may removed once the following PR is merged and released:
  // https://github.com/Azure/azure-storage-cpplite/pull/64
  std::future<azure::storage_lite::storage_outcome<void>> result =
      client_->upload_block_from_buffer(
          container_name,
          blob_path,
          block_id,
          const_cast<char*>(static_cast<const char*>(buffer)),
          length);

  if (!result.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Upload block failed on: " + blob_path)));
  }

  azure::storage_lite::storage_outcome<void> outcome = result.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Upload block failed on: " + blob_path)));
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
