/**
 * @file   gcs.cc
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
 * This file implements the GCS class.
 */

#ifdef HAVE_GCS

#include <google/cloud/status.h>
#include <google/cloud/storage/client_options.h>
#include "google/cloud/storage/oauth2/credentials.h"
#include "google/cloud/storage/oauth2/google_credentials.h"

#include <sstream>
#include <unordered_set>

#include "tiledb/common/logger.h"
#include "tiledb/common/unique_rwlock.h"
#include "tiledb/sm/filesystem/gcs.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

GCS::GCS()
    : state_(State::UNINITIALIZED)
    , write_cache_max_size_(0)
    , max_parallel_ops_(1)
    , multi_part_part_size_(0)
    , use_multi_part_upload_(true)
    , request_timeout_ms_(0) {
}

GCS::~GCS() {
}

/* ********************************* */
/*                 API               */
/* ********************************* */

Status GCS::init(const Config& config, ThreadPool* const thread_pool) {
  if (thread_pool == nullptr) {
    return LOG_STATUS(
        Status::GCSError("Can't initialize with null thread pool."));
  }

  assert(state_ == State::UNINITIALIZED);

  thread_pool_ = thread_pool;

  bool found;
  project_id_ = config.get("vfs.gcs.project_id", &found);
  assert(found);
  RETURN_NOT_OK(config.get<uint64_t>(
      "vfs.gcs.max_parallel_ops", &max_parallel_ops_, &found));
  assert(found);
  RETURN_NOT_OK(config.get<bool>(
      "vfs.gcs.use_multi_part_upload", &use_multi_part_upload_, &found));
  assert(found);
  RETURN_NOT_OK(config.get<uint64_t>(
      "vfs.gcs.multi_part_size", &multi_part_part_size_, &found));
  assert(found);
  RETURN_NOT_OK(config.get<uint64_t>(
      "vfs.gcs.request_timeout_ms", &request_timeout_ms_, &found));
  assert(found);

  write_cache_max_size_ = max_parallel_ops_ * multi_part_part_size_;

  state_ = State::INITIALIZED;
  return Status::Ok();
}

Status GCS::init_client() const {
  assert(state_ == State::INITIALIZED);

  std::lock_guard<std::mutex> lck(client_init_mtx_);

  // Client is a google::cloud::storage::StatusOr which compares (in)valid as
  // bool
  if (client_) {
    return Status::Ok();
  }

  google::cloud::storage::ChannelOptions channel_options;

#ifdef __linux__
  const std::string cert_file =
      global_state::GlobalState::GetGlobalState().cert_file();
  if (!cert_file.empty()) {
    channel_options.set_ssl_root_path(cert_file);
  }
#endif

  // Note that the order here is *extremely important*
  // We must call ::GoogleDefaultCredentials *with* a channel_options
  // argument, or else the Curl handle pool will be default-initialized
  // with no root dir (CURLOPT_CAINFO), defaulting to build host path.
  // Later initializations of ClientOptions/Client with the channel_options
  // do not appear to sufficiently reset the internal option, leading to
  // CA verification failures when using lib from systemA on systemB.
  // Ideally we could use CreateDefaultClientOptions(channel_options)
  // signature, but that function is header-only/unimplemented
  // (as of GCS 1.15).

  // Creates the client using the credentials file pointed to by the
  // env variable GOOGLE_APPLICATION_CREDENTIALS
  try {
    std::shared_ptr<google::cloud::storage::oauth2::Credentials> creds =
        nullptr;
    if (getenv("CLOUD_STORAGE_EMULATOR_ENDPOINT")) {
      creds = google::cloud::storage::oauth2::CreateAnonymousCredentials();
    } else {
      auto status_or_creds =
          google::cloud::storage::oauth2::GoogleDefaultCredentials(
              channel_options);
      if (!status_or_creds) {
        return LOG_STATUS(Status::GCSError(
            "Failed to initialize GCS credentials: " +
            status_or_creds.status().message()));
      }
      creds = *status_or_creds;
    }
    google::cloud::storage::ClientOptions client_options(
        creds, channel_options);
    auto client = google::cloud::storage::Client(
        client_options,
        google::cloud::storage::LimitedTimeRetryPolicy(
            std::chrono::milliseconds(request_timeout_ms_)));
    client_ = google::cloud::StatusOr<google::cloud::storage::Client>(client);
    if (!client_) {
      return LOG_STATUS(Status::GCSError(
          "Failed to initialize GCS Client; " + client_.status().message()));
    }
  } catch (const std::exception& e) {
    return LOG_STATUS(
        Status::GCSError("Failed to initialize GCS: " + std::string(e.what())));
  }

  return Status::Ok();
}

Status GCS::create_bucket(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::string bucket_name;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, nullptr));

  google::cloud::StatusOr<google::cloud::storage::BucketMetadata>
      bucket_metadata = client_->CreateBucketForProject(
          bucket_name, project_id_, google::cloud::storage::BucketMetadata());

  if (!bucket_metadata.ok()) {
    return LOG_STATUS(Status::GCSError(std::string(
        "Create bucket failed on: " + uri.to_string() + " (" +
        bucket_metadata.status().message() + ")")));
  }

  return wait_for_bucket_to_propagate(bucket_name);
}

Status GCS::empty_bucket(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  return remove_dir(uri);
}

Status GCS::is_empty_bucket(const URI& uri, bool* is_empty) const {
  RETURN_NOT_OK(init_client());
  assert(is_empty);

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::string bucket_name;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, nullptr));

  google::cloud::storage::ListObjectsReader objects_reader =
      client_->ListObjects(bucket_name);

  for (const google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>&
           object_metadata : objects_reader) {
    if (!object_metadata) {
      return LOG_STATUS(Status::GCSError(std::string(
          "List bucket objects failed on: " + uri.to_string() + " (" +
          object_metadata.status().message() + ")")));
    }

    *is_empty = false;
    return Status::Ok();
  }

  *is_empty = true;
  return Status::Ok();
}

Status GCS::is_bucket(const URI& uri, bool* const is_bucket) const {
  RETURN_NOT_OK(init_client());
  assert(is_bucket);

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::string bucket_name;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, nullptr));

  return this->is_bucket(bucket_name, is_bucket);
}

Status GCS::is_bucket(
    const std::string& bucket_name, bool* const is_bucket) const {
  google::cloud::StatusOr<google::cloud::storage::BucketMetadata>
      bucket_metadata = client_->GetBucketMetadata(bucket_name);

  if (!bucket_metadata.ok()) {
    const google::cloud::Status status = bucket_metadata.status();
    const google::cloud::StatusCode code = status.code();

    if (code == google::cloud::StatusCode::kNotFound) {
      *is_bucket = false;
      return Status::Ok();
    } else {
      return LOG_STATUS(Status::GCSError(std::string(
          "Get bucket failed on: " + bucket_name + " (" + status.message() +
          ")")));
    }
  }

  *is_bucket = true;
  return Status::Ok();
}

Status GCS::is_dir(const URI& uri, bool* const exists) const {
  RETURN_NOT_OK(init_client());
  assert(exists);

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri, &paths, "/", 1));
  *exists = (bool)paths.size();
  return Status::Ok();
}

Status GCS::remove_bucket(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  // Empty bucket
  RETURN_NOT_OK(empty_bucket(uri));

  std::string bucket_name;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, nullptr));

  const google::cloud::Status status = client_->DeleteBucket(bucket_name);
  if (!status.ok()) {
    return LOG_STATUS(Status::GCSError(std::string(
        "Delete bucket failed on: " + uri.to_string() + " (" +
        status.message() + ")")));
  }

  return wait_for_bucket_to_be_deleted(bucket_name);
}

Status GCS::remove_object(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, &object_path));

  const google::cloud::Status status =
      client_->DeleteObject(bucket_name, object_path);
  if (!status.ok()) {
    return LOG_STATUS(Status::GCSError(std::string(
        "Delete object failed on: " + uri.to_string() + " (" +
        status.message() + ")")));
  }

  return wait_for_object_to_be_deleted(bucket_name, object_path);
}

Status GCS::remove_dir(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri, &paths, ""));
  for (const auto& path : paths) {
    RETURN_NOT_OK(remove_object(URI(path)));
  }

  return Status::Ok();
}

std::string GCS::remove_front_slash(const std::string& path) {
  if (path.front() == '/') {
    return path.substr(1, path.length());
  }

  return path;
}

std::string GCS::add_trailing_slash(const std::string& path) {
  if (path.back() != '/') {
    return path + "/";
  }

  return path;
}

std::string GCS::remove_trailing_slash(const std::string& path) {
  if (path.back() == '/') {
    return path.substr(0, path.length() - 1);
  }

  return path;
}

Status GCS::ls(
    const URI& uri,
    std::vector<std::string>* paths,
    const std::string& delimiter,
    const int max_paths) const {
  RETURN_NOT_OK(init_client());
  assert(paths);

  const URI uri_dir = uri.add_trailing_slash();

  if (!uri_dir.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri_dir.to_string())));
  }

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri_dir, &bucket_name, &object_path));

  google::cloud::storage::Prefix prefix_option(object_path);
  google::cloud::storage::Delimiter delimiter_option(delimiter);

  google::cloud::storage::ListObjectsAndPrefixesReader objects_reader =
      client_->ListObjectsAndPrefixes(
          bucket_name, std::move(prefix_option), std::move(delimiter_option));
  for (const auto& object_metadata : objects_reader) {
    if (!object_metadata.ok()) {
      const google::cloud::Status status = object_metadata.status();

      return LOG_STATUS(Status::GCSError(std::string(
          "List objects failed on: " + uri.to_string() + " (" +
          status.message() + ")")));
    }

    if (paths->size() >= static_cast<size_t>(max_paths)) {
      break;
    }

    auto& results = object_metadata.value();

    if (absl::holds_alternative<google::cloud::storage::ObjectMetadata>(
            results)) {
      paths->emplace_back(
          "gcs://" + bucket_name + "/" +
          remove_front_slash(remove_trailing_slash(
              absl::get<google::cloud::storage::ObjectMetadata>(results)
                  .name())));
    } else if (absl::holds_alternative<std::string>(results)) {
      paths->emplace_back(
          "gcs://" + bucket_name + "/" +
          remove_front_slash(
              remove_trailing_slash(absl::get<std::string>(results))));
    }
  }

  return Status::Ok();
}

Status GCS::move_object(const URI& old_uri, const URI& new_uri) {
  RETURN_NOT_OK(init_client());

  RETURN_NOT_OK(copy_object(old_uri, new_uri));
  RETURN_NOT_OK(remove_object(old_uri));
  return Status::Ok();
}

Status GCS::copy_object(const URI& old_uri, const URI& new_uri) {
  RETURN_NOT_OK(init_client());

  if (!old_uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + old_uri.to_string())));
  }

  if (!new_uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + new_uri.to_string())));
  }

  std::string old_bucket_name;
  std::string old_object_path;
  RETURN_NOT_OK(parse_gcs_uri(old_uri, &old_bucket_name, &old_object_path));

  std::string new_bucket_name;
  std::string new_object_path;
  RETURN_NOT_OK(parse_gcs_uri(new_uri, &new_bucket_name, &new_object_path));

  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
      object_metadata = client_->CopyObject(
          old_bucket_name, old_object_path, new_bucket_name, new_object_path);

  if (!object_metadata.ok()) {
    const google::cloud::Status status = object_metadata.status();

    return LOG_STATUS(Status::GCSError(std::string(
        "Copy object failed on: " + old_uri.to_string() + " (" +
        status.message() + ")")));
  }

  return wait_for_object_to_propagate(new_bucket_name, new_object_path);
}

Status GCS::wait_for_object_to_propagate(
    const std::string& bucket_name, const std::string& object_path) const {
  RETURN_NOT_OK(init_client());

  unsigned attempts = 0;
  while (attempts++ < constants::gcs_max_attempts) {
    bool is_object;
    RETURN_NOT_OK(this->is_object(bucket_name, object_path, &is_object));
    if (is_object) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::gcs_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::GCSError(
      std::string("Timed out waiting on object to propogate: " + object_path)));
}

Status GCS::wait_for_object_to_be_deleted(
    const std::string& bucket_name, const std::string& object_path) const {
  RETURN_NOT_OK(init_client());

  unsigned attempts = 0;
  while (attempts++ < constants::gcs_max_attempts) {
    bool is_object;
    RETURN_NOT_OK(this->is_object(bucket_name, object_path, &is_object));
    if (!is_object) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::gcs_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::GCSError(std::string(
      "Timed out waiting on object to be deleted: " + object_path)));
}

Status GCS::wait_for_bucket_to_propagate(const std::string& bucket_name) const {
  unsigned attempts = 0;
  while (attempts++ < constants::gcs_max_attempts) {
    bool is_bucket;
    RETURN_NOT_OK(this->is_bucket(bucket_name, &is_bucket));

    if (is_bucket) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::gcs_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::GCSError(
      std::string("Timed out waiting on bucket to propogate: " + bucket_name)));
}

Status GCS::wait_for_bucket_to_be_deleted(
    const std::string& bucket_name) const {
  RETURN_NOT_OK(init_client());

  unsigned attempts = 0;
  while (attempts++ < constants::gcs_max_attempts) {
    bool is_bucket;
    RETURN_NOT_OK(this->is_bucket(bucket_name, &is_bucket));
    if (!is_bucket) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::gcs_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::GCSError(std::string(
      "Timed out waiting on bucket to be deleted: " + bucket_name)));
}

Status GCS::move_dir(const URI& old_uri, const URI& new_uri) {
  RETURN_NOT_OK(init_client());

  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(old_uri, &paths, ""));
  for (const auto& path : paths) {
    const std::string suffix = path.substr(old_uri.to_string().size());
    const URI new_path = new_uri.join_path(suffix);
    RETURN_NOT_OK(move_object(URI(path), new_path));
  }
  return Status::Ok();
}

Status GCS::touch(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, &object_path));

  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
      object_metadata = client_->InsertObject(bucket_name, object_path, "");

  if (!object_metadata.ok()) {
    const google::cloud::Status status = object_metadata.status();

    return LOG_STATUS(Status::GCSError(std::string(
        "Touch object failed on: " + uri.to_string() + " (" + status.message() +
        ")")));
  }

  return Status::Ok();
}

Status GCS::is_object(const URI& uri, bool* const is_object) const {
  RETURN_NOT_OK(init_client());
  assert(is_object);

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, &object_path));

  return this->is_object(bucket_name, object_path, is_object);
}

Status GCS::is_object(
    const std::string& bucket_name,
    const std::string& object_path,
    bool* const is_object) const {
  assert(is_object);

  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
      object_metadata = client_->GetObjectMetadata(bucket_name, object_path);

  if (!object_metadata.ok()) {
    const google::cloud::Status status = object_metadata.status();
    const google::cloud::StatusCode code = status.code();

    if (code == google::cloud::StatusCode::kNotFound) {
      *is_object = false;
      return Status::Ok();
    } else {
      return LOG_STATUS(Status::GCSError(std::string(
          "Get object failed on: " + object_path + " (" + status.message() +
          ")")));
    }
  }

  *is_object = true;

  return Status::Ok();
}

Status GCS::write(
    const URI& uri, const void* const buffer, const uint64_t length) {
  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  uint64_t nbytes_filled;
  RETURN_NOT_OK(
      fill_write_cache(write_cache_buffer, buffer, length, &nbytes_filled));

  if (!use_multi_part_upload_) {
    if (nbytes_filled != length) {
      std::stringstream errmsg;
      errmsg << "Direct write failed! " << nbytes_filled
             << " bytes written to buffer, " << length << " bytes requested.";
      return LOG_STATUS(Status::GCSError(errmsg.str()));
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
      RETURN_NOT_OK(write_parts(
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

Status GCS::object_size(const URI& uri, uint64_t* const nbytes) const {
  RETURN_NOT_OK(init_client());
  assert(nbytes);

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, &object_path));

  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
      object_metadata = client_->GetObjectMetadata(bucket_name, object_path);

  if (!object_metadata.ok()) {
    const google::cloud::Status status = object_metadata.status();

    return LOG_STATUS(Status::GCSError(std::string(
        "Get object size failed on: " + object_path + " (" + status.message() +
        ")")));
  }

  *nbytes = object_metadata->size();

  return Status::Ok();
}

Buffer* GCS::get_write_cache_buffer(const std::string& uri) {
  std::unique_lock<std::mutex> map_lock(write_cache_map_lock_);
  if (write_cache_map_.count(uri) > 0) {
    return &write_cache_map_.at(uri);
  } else {
    return &write_cache_map_[uri];
  }
}

Status GCS::fill_write_cache(
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

Status GCS::flush_write_cache(
    const URI& uri, Buffer* const write_cache_buffer, const bool last_part) {
  assert(write_cache_buffer);

  if (write_cache_buffer->size() > 0) {
    const Status st = write_parts(
        uri, write_cache_buffer->data(), write_cache_buffer->size(), last_part);
    write_cache_buffer->reset_size();
    RETURN_NOT_OK(st);
  }

  return Status::Ok();
}

Status GCS::write_parts(
    const URI& uri,
    const void* const buffer,
    const uint64_t length,
    const bool last_part) {
  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not an GCS URI: " + uri.to_string())));
  }

  // Ensure that each thread is responsible for exactly multi_part_part_size_
  // bytes (except if this is the last part, in which case the final
  // thread should write less). Cap the number of parallel operations at the
  // configured max number. Length must be evenly divisible by
  // multi_part_part_size_ unless this is the last part.
  uint64_t num_ops = last_part ?
                         utils::math::ceil(length, multi_part_part_size_) :
                         (length / multi_part_part_size_);
  num_ops = std::min(std::max(num_ops, uint64_t(1)), max_parallel_ops_);

  if (!last_part && length % multi_part_part_size_ != 0) {
    return LOG_STATUS(
        Status::S3Error("Length not evenly divisible by part size"));
  }

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, &object_path));

  MultiPartUploadState* state;
  std::unique_lock<std::mutex> state_lck;

  // Take a lock protecting the shared multipart data structures
  // Read lock to see if it exists
  UniqueReadLock unique_rl(&multipart_upload_rwlock_);

  auto state_iter = multi_part_upload_states_.find(uri.to_string());
  if (state_iter == multi_part_upload_states_.end()) {
    // If the state is new, we must grab write lock, so unlock from read and
    // grab write
    unique_rl.unlock();
    UniqueWriteLock unique_wl(&multipart_upload_rwlock_);
    // Since we switched locks we need to once again check to make sure another
    // thread didn't create the state
    state_iter = multi_part_upload_states_.find(uri.to_string());
    if (state_iter == multi_part_upload_states_.end()) {
      // Instantiate the new state.
      MultiPartUploadState new_state(object_path);

      // Store the new state.
      multi_part_upload_states_.emplace(uri.to_string(), std::move(new_state));
      state = &multi_part_upload_states_.at(uri.to_string());
      state_lck = std::unique_lock<std::mutex>(state->mtx_);

      // Downgrade to read lock, expected below outside the create
      unique_wl.unlock();

      // Delete file if it exists (overwrite).
      bool exists;
      RETURN_NOT_OK(is_object(uri, &exists));
      if (exists) {
        RETURN_NOT_OK(remove_object(uri));
      }
    } else {
      // If another thread switched state, switch back to a read lock
      state = &multi_part_upload_states_.at(uri.to_string());

      // Lock multipart state
      state_lck = std::unique_lock<std::mutex>(state->mtx_);
    }
  } else {
    state = &multi_part_upload_states_.at(uri.to_string());

    // Lock multipart state
    state_lck = std::unique_lock<std::mutex>(state->mtx_);

    // Unlock, as make_upload_part_req will reaquire as necessary.
    unique_rl.unlock();
  }

  if (num_ops == 1) {
    const std::string object_part_path = state->next_part_path();

    const Status st =
        upload_part(bucket_name, object_part_path, buffer, length);
    state->update_st(st);
    state_lck.unlock();
    return st;
  } else {
    std::vector<ThreadPool::Task> tasks;
    tasks.reserve(num_ops);
    for (uint64_t i = 0; i < num_ops; i++) {
      const uint64_t begin = i * multi_part_part_size_;
      const uint64_t end =
          std::min((i + 1) * multi_part_part_size_ - 1, length - 1);
      const char* const thread_buffer =
          reinterpret_cast<const char*>(buffer) + begin;
      const uint64_t thread_buffer_len = end - begin + 1;
      const std::string object_part_path = state->next_part_path();

      std::function<Status()> upload_part_fn = std::bind(
          &GCS::upload_part,
          this,
          bucket_name,
          object_part_path,
          thread_buffer,
          thread_buffer_len);
      ThreadPool::Task task = thread_pool_->execute(std::move(upload_part_fn));
      tasks.emplace_back(std::move(task));
    }

    const Status st = thread_pool_->wait_all(tasks);
    state->update_st(st);
    state_lck.unlock();
    return st;
  }

  return Status::Ok();
}

Status GCS::upload_part(
    const std::string& bucket_name,
    const std::string& object_part_path,
    const void* const buffer,
    const uint64_t length) {
  std::string write_buffer(
      static_cast<const char*>(buffer), static_cast<size_t>(length));

  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
      object_metadata = client_->InsertObject(
          bucket_name, object_part_path, std::move(write_buffer));

  if (!object_metadata.ok()) {
    const google::cloud::Status status = object_metadata.status();

    return LOG_STATUS(Status::GCSError(std::string(
        "Upload part failed on: " + object_part_path + " (" + status.message() +
        ")")));
  }

  return Status::Ok();
}

Status GCS::flush_object(const URI& uri) {
  RETURN_NOT_OK(init_client());

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not a GCS URI: " + uri.to_string())));
  }

  if (!use_multi_part_upload_) {
    return flush_object_direct(uri);
  }

  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  const Status flush_write_cache_st =
      flush_write_cache(uri, write_cache_buffer, true);

  // Take a lock protecting 'multipart_upload_states_'.
  UniqueReadLock unique_rl(&multipart_upload_rwlock_);

  if (multi_part_upload_states_.count(uri.to_string()) == 0) {
    return flush_write_cache_st;
  }

  MultiPartUploadState* const state =
      &multi_part_upload_states_.at(uri.to_string());

  // Lock multipart state
  std::unique_lock<std::mutex> state_lck(state->mtx_);
  unique_rl.unlock();

  const std::vector<std::string> part_paths = state->get_part_paths();

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, &object_path));

  // Wait for the last written part to propogate to ensure all parts
  // are available for composition into a single object.
  std::string last_part_path = part_paths.back();
  const Status st = wait_for_object_to_propagate(bucket_name, last_part_path);
  state->update_st(st);
  state_lck.unlock();

  if (!st.ok()) {
    // Delete all outstanding part objects.
    delete_parts(bucket_name, part_paths);

    // Release all instance state associated with this part list
    // transactions.
    finish_multi_part_upload(uri);

    return Status::Ok();
  }

  // Build a list of objects to compose.
  std::vector<google::cloud::storage::ComposeSourceObject> source_objects;
  source_objects.reserve(part_paths.size());
  for (const auto& part_path : part_paths) {
    google::cloud::storage::ComposeSourceObject source_object;
    source_object.object_name = part_path;
    source_objects.emplace_back(std::move(source_object));
  }

  // Provide a best-effort attempt to delete any stale, intermediate
  // composed objects.
  const std::string prefix = object_path + "__compose";
  google::cloud::storage::DeleteByPrefix(*client_, bucket_name, prefix);

  // Compose all parts into a single object.
  const bool ignore_cleanup_failures = true;
  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
      object_metadata = google::cloud::storage::ComposeMany(
          *client_,
          bucket_name,
          source_objects,
          prefix,
          object_path,
          ignore_cleanup_failures);

  // Delete all outstanding part objects.
  delete_parts(bucket_name, part_paths);

  // Release all instance state associated with this multi part
  // transactions so that we can safely return if the following
  // request failed.
  finish_multi_part_upload(uri);

  if (!object_metadata.ok()) {
    const google::cloud::Status status = object_metadata.status();

    return LOG_STATUS(Status::GCSError(std::string(
        "Compse object failed on: " + uri.to_string() + " (" +
        status.message() + ")")));
  }

  return wait_for_object_to_propagate(bucket_name, object_path);
}

void GCS::delete_parts(
    const std::string& bucket_name,
    const std::vector<std::string>& part_paths) {
  std::vector<ThreadPool::Task> tasks;
  tasks.reserve(part_paths.size());
  for (const auto& part_path : part_paths) {
    std::function<Status()> delete_part_fn =
        std::bind(&GCS::delete_part, this, bucket_name, part_path);
    ThreadPool::Task task = thread_pool_->execute(std::move(delete_part_fn));
    tasks.emplace_back(std::move(task));
  }

  const Status st = thread_pool_->wait_all(tasks);
  if (!st.ok()) {
    LOG_STATUS(st);
  }
}

Status GCS::delete_part(
    const std::string& bucket_name, const std::string& part_path) {
  const google::cloud::Status status =
      client_->DeleteObject(bucket_name, part_path);
  if (!status.ok()) {
    return Status::GCSError(std::string(
        "Delete part failed on: " + part_path + " (" + status.message() + ")"));
  }

  return Status::Ok();
}

void GCS::finish_multi_part_upload(const URI& uri) {
  // Protect 'multi_part_upload_states_' from multiple writers.
  UniqueWriteLock unique_wl(&multipart_upload_rwlock_);
  multi_part_upload_states_.erase(uri.to_string());
  unique_wl.unlock();

  // Protect 'write_cache_map_' from multiple writers.
  std::unique_lock<std::mutex> cache_lock(write_cache_map_lock_);
  write_cache_map_.erase(uri.to_string());
  cache_lock.unlock();
}

Status GCS::flush_object_direct(const URI& uri) {
  Buffer* const write_cache_buffer = get_write_cache_buffer(uri.to_string());

  if (write_cache_buffer->size() == 0) {
    return Status::Ok();
  }

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, &object_path));

  std::string write_buffer(
      static_cast<const char*>(write_cache_buffer->data()),
      write_cache_buffer->size());

  // Protect 'write_cache_map_' from multiple writers.
  std::unique_lock<std::mutex> cache_lock(write_cache_map_lock_);
  write_cache_map_.erase(uri.to_string());
  cache_lock.unlock();

  google::cloud::StatusOr<google::cloud::storage::ObjectMetadata>
      object_metadata = client_->InsertObject(
          bucket_name, object_path, std::move(write_buffer));

  if (!object_metadata.ok()) {
    const google::cloud::Status status = object_metadata.status();

    return LOG_STATUS(Status::GCSError(std::string(
        "Write object failed on: " + uri.to_string() + " (" + status.message() +
        ")")));
  }

  return wait_for_object_to_propagate(bucket_name, object_path);
}

Status GCS::read(
    const URI& uri,
    const off_t offset,
    void* const buffer,
    const uint64_t length,
    const uint64_t read_ahead_length,
    uint64_t* const length_returned) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_gcs()) {
    return LOG_STATUS(Status::GCSError(
        std::string("URI is not an GCS URI: " + uri.to_string())));
  }

  std::string bucket_name;
  std::string object_path;
  RETURN_NOT_OK(parse_gcs_uri(uri, &bucket_name, &object_path));

  google::cloud::storage::ObjectReadStream stream = client_->ReadObject(
      bucket_name, object_path, google::cloud::storage::ReadFromOffset(offset));

  if (!stream.status().ok()) {
    return LOG_STATUS(Status::GCSError(std::string(
        "Read object failed on: " + uri.to_string() + " (" +
        stream.status().message() + ")")));
  }

  stream.read(static_cast<char*>(buffer), length + read_ahead_length);
  *length_returned = stream.gcount();

  stream.Close();

  if (*length_returned < length) {
    return LOG_STATUS(Status::GCSError(
        std::string("Read operation read unexpected number of bytes.")));
  }

  return Status::Ok();
}

Status GCS::parse_gcs_uri(
    const URI& uri,
    std::string* const bucket_name,
    std::string* const object_path) const {
  assert(uri.is_gcs());
  const std::string uri_str = uri.to_string();

  const static std::string gcs_prefix = "gcs://";
  assert(uri_str.rfind(gcs_prefix, 0) == 0);

  if (uri_str.size() == gcs_prefix.size()) {
    if (bucket_name)
      *bucket_name = "";
    if (object_path)
      *object_path = "";
    return Status::Ok();
  }

  // Find the '/' after the bucket name.
  const size_t separator = uri_str.find('/', gcs_prefix.size() + 1);

  // There is only a bucket name if there isn't a separating slash.
  if (separator == std::string::npos) {
    const size_t c_pos_start = gcs_prefix.size();
    const size_t c_pos_end = uri_str.size();
    if (bucket_name)
      *bucket_name = uri_str.substr(c_pos_start, c_pos_end - c_pos_start);
    if (object_path)
      *object_path = "";
    return Status::Ok();
  }

  // There is only a bucket name if there aren't any characters past the
  // separating slash.
  if (uri_str.size() == separator) {
    const size_t c_pos_start = gcs_prefix.size();
    const size_t c_pos_end = separator;
    if (bucket_name)
      *bucket_name = uri_str.substr(c_pos_start, c_pos_end - c_pos_start);
    if (object_path)
      *object_path = "";
    return Status::Ok();
  }

  const size_t c_pos_start = gcs_prefix.size();
  const size_t c_pos_end = separator;
  const size_t b_pos_start = separator + 1;
  const size_t b_pos_end = uri_str.size();

  if (bucket_name)
    *bucket_name = uri_str.substr(c_pos_start, c_pos_end - c_pos_start);
  if (object_path)
    *object_path = uri_str.substr(b_pos_start, b_pos_end - b_pos_start);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif
