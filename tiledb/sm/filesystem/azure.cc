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

#include "tiledb/sm/filesystem/azure.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Azure::Azure() {
}

Azure::~Azure() {
}

/* ********************************* */
/*                 API               */
/* ********************************* */

Status Azure::init(const Config& /*config*/, ThreadPool* const thread_pool) {
  if (thread_pool == nullptr) {
    return LOG_STATUS(
        Status::AzureError("Can't initialize with null thread pool."));
  }
  // TODO: make use of 'thread_pool'

  // TODO: pull account name, accont key, and blob endpoint from 'config'.
  const std::string account_name = "";
  const std::string account_key = "";
  const std::string blob_endpoint = "";
  const bool use_https = true;
  const int connection_count = 1;

  std::shared_ptr<azure::storage_lite::shared_key_credential> credential =
      std::make_shared<azure::storage_lite::shared_key_credential>(
          account_name, account_key);

  std::shared_ptr<azure::storage_lite::storage_account> account =
      std::make_shared<azure::storage_lite::storage_account>(
          account_name, credential, use_https, blob_endpoint);

  client_ = std::make_shared<azure::storage_lite::blob_client>(
      account, connection_count);

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

  std::future<azure::storage_lite::storage_outcome<void>> request =
      client_->create_container(container_name);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Create container failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = request.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Create container failed on: " + uri.to_string())));
  }

  return wait_for_container_to_propagate(container_name);
}

Status Azure::wait_for_container_to_propagate(
    const std::string& container_name) const {
  unsigned attempts = 0;
  while (attempts++ < constants::s3_max_attempts) {
    bool is_container;
    RETURN_NOT_OK(this->is_container(container_name, &is_container));

    if (is_container) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::AzureError(std::string(
      "Timed out waiting on container to propogate: " + container_name)));
}

Status Azure::wait_for_container_to_be_deleted(
    const std::string& container_name) const {
  assert(client_);

  unsigned attempts = 0;
  while (attempts++ < constants::s3_max_attempts) {
    bool is_container;
    RETURN_NOT_OK(this->is_container(container_name, &is_container));
    if (!is_container) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::AzureError(std::string(
      "Timed out waiting on container to be deleted: " + container_name)));
}

Status Azure::empty_container(const URI& container) const {
  assert(client_);

  const URI uri_dir = container.add_trailing_slash();
  return remove_dir(uri_dir);
}

Status Azure::flush_blob(const URI& uri) {
  assert(client_);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::stringstream* const write_ss = &write_cache_[uri.to_string()];
  if (write_ss->eof()) {
    return Status::Ok();
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  std::vector<std::pair<std::string, std::string>> empty_metadata;
  std::future<azure::storage_lite::storage_outcome<void>> request =
      client_->upload_block_blob_from_stream(
          container_name, blob_path, *write_ss, empty_metadata);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Flush blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = request.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Flush blob failed on: " + uri.to_string())));
  }

  // Clear 'write_ss'.
  std::stringstream().swap(*write_ss);

  return Status::Ok();
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
      request = client_->list_blobs_segmented(container_name, "", "", "", 1);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("List blobs failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<
      azure::storage_lite::list_blobs_segmented_response>
      outcome = request.get();
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
      request = client_->get_container_properties(container_name);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Get container properties failed on: " + container_name)));
  }

  azure::storage_lite::storage_outcome<azure::storage_lite::container_property>
      outcome = request.get();
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

  // Potentially add `/` to the end of `uri`
  const URI uri_dir = uri.add_trailing_slash();
  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri_dir, &paths, "/", 1));
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
      request = client_->get_blob_properties(container_name, blob_path);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Get blob properties failed on: " + blob_path)));
  }

  azure::storage_lite::storage_outcome<azure::storage_lite::blob_property>
      outcome = request.get();
  if (!outcome.success()) {
    *is_blob = false;
    return Status::Ok();
  }

  azure::storage_lite::blob_property response = outcome.response();

  *is_blob = response.valid();
  return Status::Ok();
}

Status Azure::ls(
    const URI& uri,
    std::vector<std::string>* paths,
    const std::string& delimiter,
    const int max_paths) const {
  assert(client_);
  assert(paths);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  std::string continuation_token = "";
  do {
    std::future<azure::storage_lite::storage_outcome<
        azure::storage_lite::list_blobs_segmented_response>>
        request = client_->list_blobs_segmented(
            container_name,
            delimiter,
            continuation_token,
            blob_path,
            max_paths > 0 ? max_paths : 5000);
    if (!request.valid()) {
      return LOG_STATUS(Status::AzureError(
          std::string("List blobs failed on: " + uri.to_string())));
    }

    azure::storage_lite::storage_outcome<
        azure::storage_lite::list_blobs_segmented_response>
        outcome = request.get();
    if (!outcome.success()) {
      return LOG_STATUS(Status::AzureError(
          std::string("List blobs failed on: " + uri.to_string())));
    }

    azure::storage_lite::list_blobs_segmented_response response =
        outcome.response();

    for (const auto& blob : response.blobs) {
      paths->emplace_back("azure://" + container_name + "/" + blob.name);
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

  std::string old_container_name;
  std::string old_blob_path;
  RETURN_NOT_OK(parse_azure_uri(old_uri, &old_container_name, &old_blob_path));

  std::string new_container_name;
  std::string new_blob_path;
  RETURN_NOT_OK(parse_azure_uri(new_uri, &new_container_name, &new_blob_path));

  std::future<azure::storage_lite::storage_outcome<void>> request =
      client_->start_copy(
          old_container_name, old_blob_path, new_container_name, new_blob_path);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Copy blob failed on: " + old_uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = request.get();
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
  while (attempts++ < constants::s3_max_attempts) {
    bool is_blob;
    RETURN_NOT_OK(this->is_blob(container_name, blob_path, &is_blob));
    if (is_blob) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::AzureError(
      std::string("Timed out waiting on blob to propogate: " + blob_path)));
}

Status Azure::wait_for_blob_to_be_deleted(
    const std::string& container_name, const std::string& blob_path) const {
  assert(client_);

  unsigned attempts = 0;
  while (attempts++ < constants::s3_max_attempts) {
    bool is_blob;
    RETURN_NOT_OK(this->is_blob(container_name, blob_path, &is_blob));
    if (!is_blob) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
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
      request =
          client_->list_blobs_segmented(container_name, "", "", blob_path, 1);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Get blob size failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<
      azure::storage_lite::list_blobs_segmented_response>
      outcome = request.get();
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
    const uint64_t length) const {
  assert(client_);

  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::string container_name;
  std::string blob_path;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, &blob_path));

  std::stringstream ss;
  // TODO: download in parallel
  std::future<azure::storage_lite::storage_outcome<void>> request =
      client_->download_blob_to_stream(
          container_name, blob_path, offset, length, ss);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Read blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = request.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Read blob failed on: " + uri.to_string())));
  }

  ss >> static_cast<char*>(buffer);

  return Status::Ok();
}

Status Azure::remove_container(const URI& uri) const {
  assert(client_);

  // Empty container
  RETURN_NOT_OK(empty_container(uri));

  std::string container_name;
  RETURN_NOT_OK(parse_azure_uri(uri, &container_name, nullptr));

  std::future<azure::storage_lite::storage_outcome<void>> request =
      client_->delete_container(container_name);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Remove container failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = request.get();
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

  std::future<azure::storage_lite::storage_outcome<void>> request =
      client_->delete_blob(
          container_name, blob_path, false /* delete_snapshots */);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Remove blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = request.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Remove blob failed on: " + uri.to_string())));
  }

  return wait_for_blob_to_be_deleted(container_name, blob_path);
}

Status Azure::remove_dir(const URI& uri) const {
  assert(client_);

  std::vector<std::string> paths;
  const URI uri_dir = uri.add_trailing_slash();
  RETURN_NOT_OK(ls(uri_dir, &paths, ""));
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
  std::future<azure::storage_lite::storage_outcome<void>> request =
      client_->upload_block_blob_from_stream(
          container_name, blob_path, empty_ss, empty_metadata);
  if (!request.valid()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Touch blob failed on: " + uri.to_string())));
  }

  azure::storage_lite::storage_outcome<void> outcome = request.get();
  if (!outcome.success()) {
    return LOG_STATUS(Status::AzureError(
        std::string("Touch blob failed on: " + uri.to_string())));
  }

  return Status::Ok();
}

Status Azure::write(const URI& uri, const void* buffer, uint64_t length) {
  if (!uri.is_azure()) {
    return LOG_STATUS(Status::AzureError(
        std::string("URI is not an Azure URI: " + uri.to_string())));
  }

  std::stringstream* const write_ss = &write_cache_[uri.to_string()];
  write_ss->write(reinterpret_cast<const char*>(buffer), length);
  if (write_ss->fail()) {
    // Clear 'write_ss'.
    std::stringstream().swap(*write_ss);

    return LOG_STATUS(Status::AzureError(
        std::string("Write blob failed: " + uri.to_string())));
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
      *container_name == "";
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
      *container_name == uri_str.substr(c_pos_start, c_pos_end - c_pos_start);
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
      *container_name == uri_str.substr(c_pos_start, c_pos_end - c_pos_start);
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