/**
 * @file   s3.cc
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
 * This file implements the S3 class.
 */

#ifdef HAVE_S3

#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <boost/interprocess/streams/bufferstream.hpp>
#include <fstream>
#include <iostream>
#include "tiledb/sm/global_state/global_state.h"

#include "tiledb/common/logger.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/utils.h"

#ifdef _WIN32
#include <Windows.h>
#undef GetMessage  // workaround for
                   // https://github.com/aws/aws-sdk-cpp/issues/402
#endif

#include "tiledb/sm/filesystem/s3.h"

namespace {

Aws::Utils::Logging::LogLevel aws_log_name_to_level(std::string loglevel) {
  std::transform(loglevel.begin(), loglevel.end(), loglevel.begin(), ::tolower);
  if (loglevel == "fatal")
    return Aws::Utils::Logging::LogLevel::Fatal;
  else if (loglevel == "error")
    return Aws::Utils::Logging::LogLevel::Error;
  else if (loglevel == "warn")
    return Aws::Utils::Logging::LogLevel::Warn;
  else if (loglevel == "info")
    return Aws::Utils::Logging::LogLevel::Info;
  else if (loglevel == "debug")
    return Aws::Utils::Logging::LogLevel::Debug;
  else if (loglevel == "trace")
    return Aws::Utils::Logging::LogLevel::Trace;
  else
    return Aws::Utils::Logging::LogLevel::Off;
}
}  // namespace

using namespace tiledb::common;

namespace tiledb {
namespace sm {

namespace {

/**
 * Return the exception name and error message from the given outcome object.
 *
 * @tparam R AWS result type
 * @tparam E AWS error type
 * @param outcome Outcome to retrieve error message from
 * @return Error message string
 */
template <typename R, typename E>
std::string outcome_error_message(const Aws::Utils::Outcome<R, E>& outcome) {
  return std::string("\nException:  ") +
         outcome.GetError().GetExceptionName().c_str() +
         std::string("\nError message:  ") +
         outcome.GetError().GetMessage().c_str();
}

}  // namespace

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Ensures that the AWS library is only initialized once per process. */
static std::once_flag aws_lib_initialized;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

S3::S3()
    : state_(State::UNINITIALIZED)
    , file_buffer_size_(0)
    , max_parallel_ops_(1)
    , multipart_part_size_(0)
    , vfs_thread_pool_(nullptr)
    , use_virtual_addressing_(false)
    , use_multipart_upload_(true) {
}

S3::~S3() {
  assert(state_ == State::DISCONNECTED);
  for (auto& buff : file_buffers_)
    delete buff.second;
}

/* ********************************* */
/*                 API               */
/* ********************************* */

Status S3::init(const Config& config, ThreadPool* const thread_pool) {
  // already initialized
  if (state_ == State::DISCONNECTED)
    return Status::Ok();

  assert(state_ == State::UNINITIALIZED);

  if (thread_pool == nullptr) {
    return LOG_STATUS(
        Status::S3Error("Can't initialize with null thread pool."));
  }

  bool found = false;
  auto logging_level = config.get("vfs.s3.logging_level", &found);
  assert(found);

  options_.loggingOptions.logLevel = aws_log_name_to_level(logging_level);

  // Initialize the library once per process.
  std::call_once(aws_lib_initialized, [this]() { Aws::InitAPI(options_); });

  if (options_.loggingOptions.logLevel != Aws::Utils::Logging::LogLevel::Off) {
    Aws::Utils::Logging::InitializeAWSLogging(
        Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
            "TileDB", Aws::Utils::Logging::LogLevel::Trace, "tiledb_s3_"));
  }

  vfs_thread_pool_ = thread_pool;
  RETURN_NOT_OK(config.get<uint64_t>(
      "vfs.s3.max_parallel_ops", &max_parallel_ops_, &found));
  assert(found);
  RETURN_NOT_OK(config.get<uint64_t>(
      "vfs.s3.multipart_part_size", &multipart_part_size_, &found));
  assert(found);
  file_buffer_size_ = multipart_part_size_ * max_parallel_ops_;
  region_ = config.get("vfs.s3.region", &found);
  assert(found);
  RETURN_NOT_OK(config.get<bool>(
      "vfs.s3.use_virtual_addressing", &use_virtual_addressing_, &found));
  assert(found);
  RETURN_NOT_OK(config.get<bool>(
      "vfs.s3.use_multipart_upload", &use_multipart_upload_, &found));
  assert(found);

  config_ = config;

  state_ = State::INITIALIZED;
  return Status::Ok();
}

Status S3::create_bucket(const URI& bucket) const {
  RETURN_NOT_OK(init_client());

  if (!bucket.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + bucket.to_string())));
  }

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::CreateBucketRequest create_bucket_request;
  create_bucket_request.SetBucket(aws_uri.GetAuthority());

  // Set the bucket location constraint equal to the S3 region.
  // Note: empty string and 'us-east-1' are parsing errors in the SDK.
  if (!region_.empty() && region_ != "us-east-1") {
    Aws::S3::Model::CreateBucketConfiguration cfg;
    Aws::String region_str(region_.c_str());
    auto location_constraint = Aws::S3::Model::BucketLocationConstraintMapper::
        GetBucketLocationConstraintForName(region_str);
    cfg.SetLocationConstraint(location_constraint);
    create_bucket_request.SetCreateBucketConfiguration(cfg);
  }

  auto create_bucket_outcome = client_->CreateBucket(create_bucket_request);
  if (!create_bucket_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to create S3 bucket ") + bucket.to_string() +
        outcome_error_message(create_bucket_outcome)));
  }

  RETURN_NOT_OK(wait_for_bucket_to_be_created(bucket));

  return Status::Ok();
}

Status S3::remove_bucket(const URI& bucket) const {
  RETURN_NOT_OK(init_client());

  // Empty bucket
  RETURN_NOT_OK(empty_bucket(bucket));

  // Delete bucket
  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::DeleteBucketRequest delete_bucket_request;
  delete_bucket_request.SetBucket(aws_uri.GetAuthority());
  auto delete_bucket_outcome = client_->DeleteBucket(delete_bucket_request);
  if (!delete_bucket_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to remove S3 bucket ") + bucket.to_string() +
        outcome_error_message(delete_bucket_outcome)));
  }
  return Status::Ok();
}

Status S3::disconnect() {
  Status ret_st = Status::Ok();

  if (state_ == State::UNINITIALIZED) {
    return ret_st;
  }

  // Take a lock protecting 'multipart_upload_states_'.
  std::unique_lock<std::mutex> multipart_lck(multipart_upload_mtx_);

  if (multipart_upload_states_.size() > 0) {
    RETURN_NOT_OK(init_client());

    for (auto& kv : multipart_upload_states_) {
      const MultiPartUploadState& state = kv.second;

      if (state.st.ok()) {
        Aws::S3::Model::CompleteMultipartUploadRequest complete_request =
            make_multipart_complete_request(state);
        auto outcome = client_->CompleteMultipartUpload(complete_request);
        if (!outcome.IsSuccess()) {
          const Status st = LOG_STATUS(Status::S3Error(
              std::string("Failed to disconnect and flush S3 objects. ") +
              outcome_error_message(outcome)));
          if (!st.ok()) {
            ret_st = st;
          }
        }
      } else {
        Aws::S3::Model::AbortMultipartUploadRequest abort_request =
            make_multipart_abort_request(state);
        auto outcome = client_->AbortMultipartUpload(abort_request);
        if (!outcome.IsSuccess()) {
          const Status st = LOG_STATUS(Status::S3Error(
              std::string("Failed to disconnect and flush S3 objects. ") +
              outcome_error_message(outcome)));
          if (!st.ok()) {
            ret_st = st;
          }
        }
      }
    }
  }

  multipart_lck.unlock();

  if (options_.loggingOptions.logLevel != Aws::Utils::Logging::LogLevel::Off) {
    Aws::Utils::Logging::ShutdownAWSLogging();
  }

  if (s3_tp_executor_) {
    const Status st = s3_tp_executor_->Stop();
    if (!st.ok()) {
      ret_st = st;
    }
  }

  state_ = State::DISCONNECTED;
  return ret_st;
}

Status S3::empty_bucket(const URI& bucket) const {
  RETURN_NOT_OK(init_client());

  auto uri_dir = bucket.add_trailing_slash();
  return remove_dir(uri_dir);
}

Status S3::flush_object(const URI& uri) {
  RETURN_NOT_OK(init_client());
  if (!use_multipart_upload_) {
    return flush_direct(uri);
  }
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  // Flush and delete file buffer. For multipart requests, we must
  // continue even if 'flush_file_buffer' fails. In that scenario,
  // we will send an abort request.
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));
  const Status flush_st = flush_file_buffer(uri, buff, true);

  Aws::Http::URI aws_uri = uri.c_str();
  std::string path_c_str = aws_uri.GetPath().c_str();

  // Take a lock protecting 'multipart_upload_states_'.
  std::unique_lock<std::mutex> multipart_lck(multipart_upload_mtx_);

  // Do nothing - empty object
  auto state_iter = multipart_upload_states_.find(path_c_str);
  if (state_iter == multipart_upload_states_.end()) {
    RETURN_NOT_OK(flush_st);
    return Status::Ok();
  }

  const MultiPartUploadState& state = state_iter->second;

  if (state.st.ok()) {
    Aws::S3::Model::CompleteMultipartUploadRequest complete_request =
        make_multipart_complete_request(state);
    auto outcome = client_->CompleteMultipartUpload(complete_request);
    if (!outcome.IsSuccess()) {
      return LOG_STATUS(Status::S3Error(
          std::string("Failed to flush S3 object ") + uri.c_str() +
          outcome_error_message(outcome)));
    }

    auto bucket = state.bucket;
    auto key = state.key;
    multipart_lck.unlock();

    wait_for_object_to_propagate(move(bucket), move(key));

    return finish_flush_object(std::move(outcome), uri, buff);
  } else {
    Aws::S3::Model::AbortMultipartUploadRequest abort_request =
        make_multipart_abort_request(state);
    auto outcome = client_->AbortMultipartUpload(abort_request);

    multipart_lck.unlock();

    return finish_flush_object(std::move(outcome), uri, buff);
  }
}

Aws::S3::Model::CompleteMultipartUploadRequest
S3::make_multipart_complete_request(const MultiPartUploadState& state) {
  // Add all the completed parts (sorted by part number) to the upload object.
  Aws::S3::Model::CompletedMultipartUpload completed_upload;
  for (auto& tup : state.completed_parts) {
    const Aws::S3::Model::CompletedPart& part = std::get<1>(tup);
    completed_upload.AddParts(part);
  }

  Aws::S3::Model::CompleteMultipartUploadRequest complete_request;
  complete_request.SetBucket(state.bucket);
  complete_request.SetKey(state.key);
  complete_request.SetUploadId(state.upload_id);
  return complete_request.WithMultipartUpload(std::move(completed_upload));
}

Aws::S3::Model::AbortMultipartUploadRequest S3::make_multipart_abort_request(
    const MultiPartUploadState& state) {
  Aws::S3::Model::AbortMultipartUploadRequest abort_request;
  abort_request.SetBucket(state.bucket);
  abort_request.SetKey(state.key);
  abort_request.SetUploadId(state.upload_id);
  return abort_request;
}

template <typename R, typename E>
Status S3::finish_flush_object(
    const Aws::Utils::Outcome<R, E>& outcome,
    const URI& uri,
    Buffer* const buff) {
  Aws::Http::URI aws_uri = uri.c_str();

  std::unique_lock<std::mutex> multipart_lck(multipart_upload_mtx_);
  multipart_upload_states_.erase(aws_uri.GetPath().c_str());
  multipart_lck.unlock();

  file_buffers_.erase(uri.to_string());
  delete buff;

  if (!outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to flush S3 object ") + uri.c_str() +
        outcome_error_message(outcome)));
  }

  return Status::Ok();
}

Status S3::is_empty_bucket(const URI& bucket, bool* is_empty) const {
  RETURN_NOT_OK(init_client());

  bool exists;
  RETURN_NOT_OK(is_bucket(bucket, &exists));
  if (!exists)
    return LOG_STATUS(Status::S3Error(
        "Cannot check if bucket is empty; Bucket does not exist"));

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::ListObjectsRequest list_objects_request;
  list_objects_request.SetBucket(aws_uri.GetAuthority());
  list_objects_request.SetPrefix("");
  list_objects_request.SetDelimiter("/");
  auto list_objects_outcome = client_->ListObjects(list_objects_request);

  if (!list_objects_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to list s3 objects in bucket ") + bucket.c_str() +
        outcome_error_message(list_objects_outcome)));
  }

  *is_empty = list_objects_outcome.GetResult().GetContents().empty() &&
              list_objects_outcome.GetResult().GetCommonPrefixes().empty();

  return Status::Ok();
}

Status S3::is_bucket(const URI& uri, bool* const exists) const {
  init_client();

  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::HeadBucketRequest head_bucket_request;
  head_bucket_request.SetBucket(aws_uri.GetAuthority());
  auto head_bucket_outcome = client_->HeadBucket(head_bucket_request);
  *exists = head_bucket_outcome.IsSuccess();

  return Status::Ok();
}

Status S3::is_object(const URI& uri, bool* const exists) const {
  init_client();

  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.c_str();

  return is_object(aws_uri.GetAuthority(), aws_uri.GetPath(), exists);
}

Status S3::is_object(
    const Aws::String& bucket_name,
    const Aws::String& object_key,
    bool* const exists) const {
  init_client();

  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.SetBucket(bucket_name);
  head_object_request.SetKey(object_key);
  auto head_object_outcome = client_->HeadObject(head_object_request);
  *exists = head_object_outcome.IsSuccess();

  return Status::Ok();
}

Status S3::is_dir(const URI& uri, bool* exists) const {
  RETURN_NOT_OK(init_client());

  // Potentially add `/` to the end of `uri`
  auto uri_dir = uri.add_trailing_slash();
  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri_dir, &paths, "/", 1));
  *exists = (bool)paths.size();
  return Status::Ok();
}

Status S3::ls(
    const URI& prefix,
    std::vector<std::string>* paths,
    const std::string& delimiter,
    int max_paths) const {
  RETURN_NOT_OK(init_client());

  const auto prefix_dir = prefix.add_trailing_slash();

  auto prefix_str = prefix_dir.to_string();
  if (!prefix_dir.is_s3()) {
    return LOG_STATUS(
        Status::S3Error(std::string("URI is not an S3 URI: " + prefix_str)));
  }

  Aws::Http::URI aws_uri = prefix_str.c_str();
  auto aws_prefix = remove_front_slash(aws_uri.GetPath().c_str());
  std::string aws_auth = aws_uri.GetAuthority().c_str();
  Aws::S3::Model::ListObjectsRequest list_objects_request;
  list_objects_request.SetBucket(aws_uri.GetAuthority());
  list_objects_request.SetPrefix(aws_prefix.c_str());
  list_objects_request.SetDelimiter(delimiter.c_str());
  if (max_paths != -1)
    list_objects_request.SetMaxKeys(max_paths);

  bool is_done = false;
  while (!is_done) {
    auto list_objects_outcome = client_->ListObjects(list_objects_request);

    if (!list_objects_outcome.IsSuccess())
      return LOG_STATUS(Status::S3Error(
          std::string("Error while listing with prefix '") + prefix_str +
          "' and delimiter '" + delimiter + "'" +
          outcome_error_message(list_objects_outcome)));

    for (const auto& object : list_objects_outcome.GetResult().GetContents()) {
      std::string file(object.GetKey().c_str());
      paths->push_back("s3://" + aws_auth + add_front_slash(file));
    }

    for (const auto& object :
         list_objects_outcome.GetResult().GetCommonPrefixes()) {
      std::string file(object.GetPrefix().c_str());
      paths->push_back(
          "s3://" + aws_auth + add_front_slash(remove_trailing_slash(file)));
    }

    is_done = !list_objects_outcome.GetResult().GetIsTruncated();
    if (!is_done) {
      // The documentation states that "GetNextMarker" will be non-empty only
      // when the delimiter in the request is non-empty. When the delimiter is
      // non-empty, we must used the last returned key as the next marker.
      assert(
          !delimiter.empty() ||
          !list_objects_outcome.GetResult().GetContents().empty());
      Aws::String next_marker =
          !delimiter.empty() ?
              list_objects_outcome.GetResult().GetNextMarker() :
              list_objects_outcome.GetResult().GetContents().back().GetKey();
      assert(!next_marker.empty());

      list_objects_request.SetMarker(std::move(next_marker));
    }
  }

  return Status::Ok();
}

Status S3::move_object(const URI& old_uri, const URI& new_uri) {
  RETURN_NOT_OK(init_client());

  RETURN_NOT_OK(copy_object(old_uri, new_uri));
  RETURN_NOT_OK(remove_object(old_uri));
  return Status::Ok();
}

Status S3::move_dir(const URI& old_uri, const URI& new_uri) {
  RETURN_NOT_OK(init_client());

  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(old_uri, &paths, ""));
  for (const auto& path : paths) {
    auto suffix = path.substr(old_uri.to_string().size());
    auto new_path = new_uri.join_path(suffix);
    RETURN_NOT_OK(move_object(URI(path), URI(new_path)));
  }

  return Status::Ok();
}

Status S3::object_size(const URI& uri, uint64_t* nbytes) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  auto aws_path = remove_front_slash(aws_uri.GetPath().c_str());

  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.SetBucket(aws_uri.GetAuthority());
  head_object_request.SetKey(aws_path.c_str());
  auto head_object_outcome = client_->HeadObject(head_object_request);

  if (!head_object_outcome.IsSuccess())
    return LOG_STATUS(Status::S3Error(
        "Cannot retrieve S3 object size; Error while listing file " +
        uri.to_string() + outcome_error_message(head_object_outcome)));
  *nbytes =
      static_cast<uint64_t>(head_object_outcome.GetResult().GetContentLength());

  return Status::Ok();
}

Status S3::read(
    const URI& uri,
    const off_t offset,
    void* const buffer,
    const uint64_t length,
    const uint64_t read_ahead_length,
    uint64_t* const length_returned) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::GetObjectRequest get_object_request;
  get_object_request.WithBucket(aws_uri.GetAuthority())
      .WithKey(aws_uri.GetPath());
  get_object_request.SetRange(
      ("bytes=" + std::to_string(offset) + "-" +
       std::to_string(offset + length + read_ahead_length - 1))
          .c_str());
  get_object_request.SetResponseStreamFactory(
      [buffer, length, read_ahead_length]() {
        auto streamBuf = new boost::interprocess::bufferbuf(
            (char*)buffer, length + read_ahead_length);
        return Aws::New<Aws::IOStream>(
            constants::s3_allocation_tag.c_str(), streamBuf);
      });

  auto get_object_outcome = client_->GetObject(get_object_request);
  if (!get_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to read S3 object ") + uri.c_str() +
        outcome_error_message(get_object_outcome)));
  }

  *length_returned =
      static_cast<uint64_t>(get_object_outcome.GetResult().GetContentLength());
  if (*length_returned < length) {
    return LOG_STATUS(Status::S3Error(
        std::string("Read operation returned different size of bytes.")));
  }

  return Status::Ok();
}

Status S3::remove_object(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  Aws::S3::Model::DeleteObjectRequest delete_object_request;
  delete_object_request.SetBucket(aws_uri.GetAuthority());
  delete_object_request.SetKey(aws_uri.GetPath());

  auto delete_object_outcome = client_->DeleteObject(delete_object_request);
  if (!delete_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to delete S3 object '") + uri.c_str() +
        outcome_error_message(delete_object_outcome)));
  }

  wait_for_object_to_be_deleted(
      delete_object_request.GetBucket(), delete_object_request.GetKey());
  return Status::Ok();
}

Status S3::remove_dir(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  std::vector<std::string> paths;
  auto uri_dir = uri.add_trailing_slash();
  RETURN_NOT_OK(ls(uri_dir, &paths, ""));
  for (const auto& p : paths)
    RETURN_NOT_OK(remove_object(URI(p)));
  return Status::Ok();
}

Status S3::touch(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(std::string(
        "Cannot create file; URI is not an S3 URI: " + uri.to_string())));
  }

  if (uri.to_string().back() == '/') {
    return LOG_STATUS(Status::S3Error(std::string(
        "Cannot create file; URI is a directory: " + uri.to_string())));
  }

  bool exists;
  RETURN_NOT_OK(is_object(uri, &exists));
  if (exists) {
    return Status::Ok();
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::PutObjectRequest put_object_request;
  put_object_request.WithKey(aws_uri.GetPath())
      .WithBucket(aws_uri.GetAuthority());

  auto request_stream =
      Aws::MakeShared<Aws::StringStream>(constants::s3_allocation_tag.c_str());
  put_object_request.SetBody(request_stream);

  auto put_object_outcome = client_->PutObject(put_object_request);
  if (!put_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Cannot touch object '") + uri.c_str() +
        outcome_error_message(put_object_outcome)));
  }

  wait_for_object_to_propagate(
      put_object_request.GetBucket(), put_object_request.GetKey());

  return Status::Ok();
}

Status S3::write(const URI& uri, const void* buffer, uint64_t length) {
  RETURN_NOT_OK(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  // This write is never considered the last part of an object. The last part is
  // only uploaded with flush_object().
  const bool is_last_part = false;

  // Get file buffer
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));

  // Fill file buffer
  uint64_t nbytes_filled;
  RETURN_NOT_OK(fill_file_buffer(buff, buffer, length, &nbytes_filled));

  if ((!use_multipart_upload_) && (nbytes_filled != length)) {
    std::stringstream errmsg;
    errmsg << "Direct write failed! " << nbytes_filled
           << " bytes written to buffer, " << length << " bytes requested.";
    return LOG_STATUS(Status::S3Error(errmsg.str()));
  }

  // Flush file buffer
  // multipart objects will flush whenever the writes exceed file_buffer_size_
  // write_direct should just append to buffer and upload later
  if (use_multipart_upload_) {
    if (buff->size() == file_buffer_size_)
      RETURN_NOT_OK(flush_file_buffer(uri, buff, is_last_part));

    uint64_t new_length = length - nbytes_filled;
    uint64_t offset = nbytes_filled;
    // Write chunks
    while (new_length > 0) {
      if (new_length >= file_buffer_size_) {
        RETURN_NOT_OK(write_multipart(
            uri, (char*)buffer + offset, file_buffer_size_, is_last_part));
        offset += file_buffer_size_;
        new_length -= file_buffer_size_;
      } else {
        RETURN_NOT_OK(fill_file_buffer(
            buff, (char*)buffer + offset, new_length, &nbytes_filled));
        offset += nbytes_filled;
        new_length -= nbytes_filled;
      }
    }
    assert(offset == length);
  }

  return Status::Ok();
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

Status S3::init_client() const {
  assert(state_ == State::INITIALIZED);

  std::lock_guard<std::mutex> lck(client_init_mtx_);

  if (client_ != nullptr)
    return Status::Ok();

  bool found;
  auto s3_endpoint_override = config_.get("vfs.s3.endpoint_override", &found);
  assert(found);

  // ClientConfiguration should be lazily init'ed here in init_client to avoid
  // potential slowdowns for non s3 users as the ClientConfig now attempts to
  // check for client configuration on create, which can be slow if aws is not
  // configured on a users systems due to ec2 metadata check
  client_config_ = std::unique_ptr<Aws::Client::ClientConfiguration>(
      new Aws::Client::ClientConfiguration);
  s3_tp_executor_ = std::make_shared<S3ThreadPoolExecutor>(vfs_thread_pool_);
  client_config_->executor = s3_tp_executor_;
  auto& client_config = *client_config_.get();
  if (!region_.empty())
    client_config.region = region_.c_str();
  if (!s3_endpoint_override.empty())
    client_config.endpointOverride = s3_endpoint_override.c_str();

  auto proxy_host = config_.get("vfs.s3.proxy_host", &found);
  assert(found);
  uint32_t proxy_port = 0;
  RETURN_NOT_OK(
      config_.get<uint32_t>("vfs.s3.proxy_port", &proxy_port, &found));
  assert(found);
  auto proxy_username = config_.get("vfs.s3.proxy_username", &found);
  assert(found);
  auto proxy_password = config_.get("vfs.s3.proxy_password", &found);
  assert(found);
  auto proxy_scheme = config_.get("vfs.s3.proxy_scheme", &found);
  assert(found);
  if (!proxy_host.empty()) {
    client_config.proxyHost = proxy_host.c_str();
    client_config.proxyPort = proxy_port;
    client_config.proxyScheme = proxy_scheme == "https" ?
                                    Aws::Http::Scheme::HTTPS :
                                    Aws::Http::Scheme::HTTP;
    client_config.proxyUserName = proxy_username.c_str();
    client_config.proxyPassword = proxy_password.c_str();
  }

  auto s3_scheme = config_.get("vfs.s3.scheme", &found);
  assert(found);
  int64_t connect_timeout_ms = 0;
  RETURN_NOT_OK(config_.get<int64_t>(
      "vfs.s3.connect_timeout_ms", &connect_timeout_ms, &found));
  assert(found);
  int64_t request_timeout_ms = 0;
  RETURN_NOT_OK(config_.get<int64_t>(
      "vfs.s3.request_timeout_ms", &request_timeout_ms, &found));
  assert(found);
  auto ca_file = config_.get("vfs.s3.ca_file", &found);
  assert(found);
  auto ca_path = config_.get("vfs.s3.ca_path", &found);
  assert(found);
  bool verify_ssl = false;
  RETURN_NOT_OK(config_.get<bool>("vfs.s3.verify_ssl", &verify_ssl, &found));
  assert(found);
  auto aws_access_key_id = config_.get("vfs.s3.aws_access_key_id", &found);
  assert(found);
  auto aws_secret_access_key =
      config_.get("vfs.s3.aws_secret_access_key", &found);
  assert(found);
  auto aws_session_token = config_.get("vfs.s3.aws_session_token", &found);
  assert(found);
  int64_t connect_max_tries = 0;
  RETURN_NOT_OK(config_.get<int64_t>(
      "vfs.s3.connect_max_tries", &connect_max_tries, &found));
  assert(found);
  int64_t connect_scale_factor = 0;
  RETURN_NOT_OK(config_.get<int64_t>(
      "vfs.s3.connect_scale_factor", &connect_scale_factor, &found));
  assert(found);

  client_config.scheme = (s3_scheme == "http") ? Aws::Http::Scheme::HTTP :
                                                 Aws::Http::Scheme::HTTPS;
  client_config.connectTimeoutMs = (long)connect_timeout_ms;
  client_config.requestTimeoutMs = (long)request_timeout_ms;
  client_config.caFile = ca_file.c_str();
  client_config.caPath = ca_path.c_str();
  client_config.verifySSL = verify_ssl;

  client_config.retryStrategy = Aws::MakeShared<S3RetryStrategy>(
      constants::s3_allocation_tag.c_str(),
      connect_max_tries,
      connect_scale_factor);

#ifdef __linux__
  // If the user has not set a s3 ca file or ca path then let's attempt to set
  // the cert file if we've autodetected it
  if (ca_file.empty() && ca_path.empty()) {
    const std::string cert_file =
        global_state::GlobalState::GetGlobalState().cert_file();
    if (!cert_file.empty()) {
      client_config.caFile = cert_file.c_str();
    }
  }
#endif

  // If the user set config variables for AWS keys use them.
  if (!aws_access_key_id.empty() && !aws_secret_access_key.empty()) {
    Aws::String access_key_id(aws_access_key_id.c_str());
    Aws::String secret_access_key(aws_secret_access_key.c_str());
    client_creds_ = std::unique_ptr<Aws::Auth::AWSCredentials>(
        new Aws::Auth::AWSCredentials(access_key_id, secret_access_key));

    // If the user has set a session token (for AWS Security Token Service)
    // then use it:
    //     - https://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html
    // For testing run: `aws sts get-session-token --duration-seconds 900`. See:
    //     -
    //     https://docs.aws.amazon.com/cli/latest/reference/sts/get-session-token.html
    if (!aws_session_token.empty()) {
      Aws::String session_token(aws_session_token.c_str());
      client_creds_->SetSessionToken(session_token);
    }
  }

  if (client_creds_ == nullptr) {
    client_ = Aws::MakeShared<Aws::S3::S3Client>(
        constants::s3_allocation_tag.c_str(),
        *client_config_,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        use_virtual_addressing_);
  } else {
    client_ = Aws::MakeShared<Aws::S3::S3Client>(
        constants::s3_allocation_tag.c_str(),
        *client_creds_,
        *client_config_,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        use_virtual_addressing_);
  }

  return Status::Ok();
}

Status S3::copy_object(const URI& old_uri, const URI& new_uri) {
  RETURN_NOT_OK(init_client());

  Aws::Http::URI src_uri = old_uri.c_str();
  Aws::Http::URI dst_uri = new_uri.c_str();
  Aws::S3::Model::CopyObjectRequest copy_object_request;
  copy_object_request.SetCopySource(
      join_authority_and_path(
          src_uri.GetAuthority().c_str(), src_uri.GetPath().c_str())
          .c_str());
  copy_object_request.SetBucket(dst_uri.GetAuthority());
  copy_object_request.SetKey(dst_uri.GetPath());

  auto copy_object_outcome = client_->CopyObject(copy_object_request);
  if (!copy_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to copy S3 object ") + old_uri.c_str() + " to " +
        new_uri.c_str() + outcome_error_message(copy_object_outcome)));
  }

  wait_for_object_to_propagate(
      copy_object_request.GetBucket(), copy_object_request.GetKey());

  return Status::Ok();
}

Status S3::fill_file_buffer(
    Buffer* buff,
    const void* buffer,
    uint64_t length,
    uint64_t* nbytes_filled) {
  *nbytes_filled = std::min(file_buffer_size_ - buff->size(), length);
  if (*nbytes_filled > 0)
    RETURN_NOT_OK(buff->write(buffer, *nbytes_filled));

  return Status::Ok();
}

std::string S3::add_front_slash(const std::string& path) const {
  return (path.front() != '/') ? (std::string("/") + path) : path;
}

std::string S3::remove_front_slash(const std::string& path) const {
  if (path.front() == '/')
    return path.substr(1, path.length());
  return path;
}

std::string S3::remove_trailing_slash(const std::string& path) const {
  if (path.back() == '/') {
    return path.substr(0, path.length() - 1);
  }

  return path;
}

Status S3::flush_file_buffer(const URI& uri, Buffer* buff, bool last_part) {
  RETURN_NOT_OK(init_client());
  if (buff->size() > 0) {
    const Status st =
        write_multipart(uri, buff->data(), buff->size(), last_part);
    buff->reset_size();
    RETURN_NOT_OK(st);
  }

  return Status::Ok();
}

Status S3::get_file_buffer(const URI& uri, Buffer** buff) {
  std::unique_lock<std::mutex> lck(multipart_upload_mtx_);

  auto uri_str = uri.to_string();
  auto it = file_buffers_.find(uri_str);
  if (it == file_buffers_.end()) {
    auto new_buff = new Buffer();
    file_buffers_[uri_str] = new_buff;
    *buff = new_buff;
  } else {
    *buff = it->second;
  }

  return Status::Ok();
}

Status S3::initiate_multipart_request(Aws::Http::URI aws_uri) {
  RETURN_NOT_OK(init_client());

  auto& path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  Aws::S3::Model::CreateMultipartUploadRequest multipart_upload_request;
  multipart_upload_request.SetBucket(aws_uri.GetAuthority());
  multipart_upload_request.SetKey(path);
  multipart_upload_request.SetContentType("application/octet-stream");

  auto multipart_upload_outcome =
      client_->CreateMultipartUpload(multipart_upload_request);
  if (!multipart_upload_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to create multipart request for object '") +
        path_c_str + outcome_error_message(multipart_upload_outcome)));
  }

  MultiPartUploadState state(
      1,
      Aws::String(aws_uri.GetAuthority()),
      Aws::String(path),
      Aws::String(multipart_upload_outcome.GetResult().GetUploadId()),
      std::map<int, Aws::S3::Model::CompletedPart>());

  assert(multipart_upload_states_.count(path_c_str) == 0);
  multipart_upload_states_.emplace(std::move(path_c_str), std::move(state));

  return Status::Ok();
}

std::string S3::join_authority_and_path(
    const std::string& authority, const std::string& path) const {
  bool path_has_slash = !path.empty() && path.front() == '/';
  bool authority_has_slash = !authority.empty() && authority.back() == '/';
  bool need_slash = !(path_has_slash || authority_has_slash);
  return authority + (need_slash ? "/" : "") + path;
}

Status S3::wait_for_object_to_propagate(
    const Aws::String& bucket_name, const Aws::String& object_key) const {
  init_client();

  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    bool exists;
    RETURN_NOT_OK(is_object(bucket_name, object_key, &exists));
    if (exists) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::S3Error(
      "Failed waiting for object " +
      std::string(object_key.c_str(), object_key.size()) + " to be created."));
}

Status S3::wait_for_object_to_be_deleted(
    const Aws::String& bucket_name, const Aws::String& object_key) const {
  init_client();

  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    bool exists;
    RETURN_NOT_OK(is_object(bucket_name, object_key, &exists));
    if (!exists) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::S3Error(
      "Failed waiting for object " +
      std::string(object_key.c_str(), object_key.size()) + " to be deleted."));
}

Status S3::wait_for_bucket_to_be_created(const URI& bucket_uri) const {
  init_client();

  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    bool exists;
    RETURN_NOT_OK(is_bucket(bucket_uri, &exists));
    if (exists) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return LOG_STATUS(Status::S3Error(
      "Failed waiting for bucket " + bucket_uri.to_string() +
      " to be created."));
}

Status S3::flush_direct(const URI& uri) {
  RETURN_NOT_OK(init_client());

  // Get file buffer
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));

  const Aws::Http::URI aws_uri(uri.c_str());
  const std::string uri_path(aws_uri.GetPath().c_str());

  Aws::S3::Model::PutObjectRequest put_object_request;

  auto stream = std::shared_ptr<Aws::IOStream>(
      new boost::interprocess::bufferstream((char*)buff->data(), buff->size()));

  put_object_request.SetBody(stream);
  put_object_request.SetContentLength(buff->size());

  // we only want to hash once, and must do it after setting the body
  auto md5_hash =
      Aws::Utils::HashingUtils::CalculateMD5(*put_object_request.GetBody());

  put_object_request.SetContentMD5(
      Aws::Utils::HashingUtils::Base64Encode(md5_hash));
  put_object_request.SetContentType("application/octet-stream");
  put_object_request.SetBucket(aws_uri.GetAuthority());
  put_object_request.SetKey(aws_uri.GetPath());

  auto put_object_outcome = client_->PutObject(put_object_request);
  if (!put_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Cannot write object '") + uri.c_str() +
        outcome_error_message(put_object_outcome)));
  }

  // verify the MD5 hash of the result
  // note the etag is hex-encoded not base64
  Aws::StringStream md5_hex;
  md5_hex << "\"" << Aws::Utils::HashingUtils::HexEncode(md5_hash) << "\"";
  if (md5_hex.str() != put_object_outcome.GetResult().GetETag()) {
    return LOG_STATUS(
        Status::S3Error("Object uploaded successfully, but MD5 hash does not "
                        "match result from server!' "));
  }

  wait_for_object_to_propagate(
      put_object_request.GetBucket(), put_object_request.GetKey());

  return Status::Ok();
}

Status S3::write_multipart(
    const URI& uri, const void* buffer, uint64_t length, bool last_part) {
  RETURN_NOT_OK(init_client());

  // Ensure that each thread is responsible for exactly multipart_part_size_
  // bytes (except if this is the last write_multipart, in which case the final
  // thread should write less), and cap the number of parallel operations at the
  // configured max number. Length must be evenly divisible by
  // multipart_part_size_ unless this is the last part.
  uint64_t num_ops = last_part ?
                         utils::math::ceil(length, multipart_part_size_) :
                         (length / multipart_part_size_);
  num_ops = std::min(std::max(num_ops, uint64_t(1)), max_parallel_ops_);

  if (!last_part && length % multipart_part_size_ != 0) {
    return LOG_STATUS(
        Status::S3Error("Length not evenly divisible by part length"));
  }

  const Aws::Http::URI aws_uri(uri.c_str());
  const std::string uri_path(aws_uri.GetPath().c_str());

  // Take a lock protecting the shared multipart data structures
  std::unique_lock<std::mutex> multipart_lck(multipart_upload_mtx_);

  auto state_iter = multipart_upload_states_.find(uri_path);
  if (state_iter == multipart_upload_states_.end()) {
    // Delete file if it exists (overwrite) and initiate multipart request
    bool exists;
    RETURN_NOT_OK(is_object(uri, &exists));
    if (exists) {
      RETURN_NOT_OK(remove_object(uri));
    }

    const Status st = initiate_multipart_request(aws_uri);
    if (!st.ok()) {
      return st;
    }

    state_iter = multipart_upload_states_.find(uri_path);
    assert(state_iter != multipart_upload_states_.end());
  }

  MultiPartUploadState* const state = &state_iter->second;

  // Get the upload ID
  const auto upload_id = state->upload_id;

  // Assign the part number(s), and make the write request.
  if (num_ops == 1) {
    const int part_num = state->part_number++;

    // Unlock, as make_upload_part_req will reaquire as necessary.
    multipart_lck.unlock();

    auto ctx =
        make_upload_part_req(aws_uri, buffer, length, upload_id, part_num);
    return get_make_upload_part_req(uri, uri_path, ctx);
  } else {
    std::vector<MakeUploadPartCtx> ctx_vec;
    ctx_vec.reserve(num_ops);
    const uint64_t bytes_per_op = multipart_part_size_;
    const int part_num_base = state->part_number;
    for (uint64_t i = 0; i < num_ops; i++) {
      uint64_t begin = i * bytes_per_op,
               end = std::min((i + 1) * bytes_per_op - 1, length - 1);
      uint64_t thread_nbytes = end - begin + 1;
      auto thread_buffer = reinterpret_cast<const char*>(buffer) + begin;
      int part_num = static_cast<int>(part_num_base + i);
      auto ctx = make_upload_part_req(
          aws_uri, thread_buffer, thread_nbytes, upload_id, part_num);
      ctx_vec.emplace_back(std::move(ctx));
    }
    state->part_number += num_ops;

    // Unlock, so the threads can take the lock as necessary.
    multipart_lck.unlock();

    Status aggregate_st = Status::Ok();
    for (auto& ctx : ctx_vec) {
      const Status st = get_make_upload_part_req(uri, uri_path, ctx);
      if (!st.ok()) {
        aggregate_st = st;
      }
    }

    if (!aggregate_st.ok()) {
      std::stringstream errmsg;
      errmsg << "S3 parallel write multipart error; " << aggregate_st.message();
      LOG_STATUS(Status::S3Error(errmsg.str()));
    }
    return aggregate_st;
  }
}

S3::MakeUploadPartCtx S3::make_upload_part_req(
    const Aws::Http::URI& aws_uri,
    const void* const buffer,
    const uint64_t length,
    const Aws::String& upload_id,
    const int upload_part_num) {
  auto stream = std::shared_ptr<Aws::IOStream>(
      new boost::interprocess::bufferstream((char*)buffer, length));

  Aws::S3::Model::UploadPartRequest upload_part_request;
  upload_part_request.SetBucket(aws_uri.GetAuthority());
  upload_part_request.SetKey(aws_uri.GetPath());
  upload_part_request.SetPartNumber(upload_part_num);
  upload_part_request.SetUploadId(upload_id);
  upload_part_request.SetBody(stream);
  upload_part_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(
      Aws::Utils::HashingUtils::CalculateMD5(*stream)));
  upload_part_request.SetContentLength(length);

  auto upload_part_outcome_callable =
      client_->UploadPartCallable(upload_part_request);

  MakeUploadPartCtx ctx(
      std::move(upload_part_outcome_callable), upload_part_num);
  return ctx;
}

Status S3::get_make_upload_part_req(
    const URI& uri, const std::string& uri_path, MakeUploadPartCtx& ctx) {
  RETURN_NOT_OK(init_client());

  auto upload_part_outcome = ctx.upload_part_outcome_callable.get();
  bool success = upload_part_outcome.IsSuccess();

  static const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
  if (unit_test_cfg.s3_fail_every_nth_upload_request.is_set() &&
      ctx.upload_part_num %
              unit_test_cfg.s3_fail_every_nth_upload_request.get() ==
          0) {
    success = false;
  }

  if (!success) {
    std::unique_lock<std::mutex> lck(multipart_upload_mtx_);
    auto state_iter = multipart_upload_states_.find(uri_path);
    state_iter->second.st = Status::S3Error(
        std::string("Failed to upload part of S3 object '") + uri.c_str() +
        outcome_error_message(upload_part_outcome));
    return LOG_STATUS(state_iter->second.st);
  }

  Aws::S3::Model::CompletedPart completed_part;
  completed_part.SetETag(upload_part_outcome.GetResult().GetETag());
  completed_part.SetPartNumber(ctx.upload_part_num);

  {
    std::unique_lock<std::mutex> lck(multipart_upload_mtx_);
    auto state_iter = multipart_upload_states_.find(uri_path);
    state_iter->second.completed_parts.emplace(
        ctx.upload_part_num, std::move(completed_part));
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif
