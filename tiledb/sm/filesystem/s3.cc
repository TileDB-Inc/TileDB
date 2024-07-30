/**
 * @file   s3.cc
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
 * This file implements the S3 class.
 *
 * ============================================================================
 * Notes on implementation limitations:
 *
 * When using MinIO object storage, files may mask Array directories
 * if they are of the same name. As such, the ArrayDirectory may filter out
 * non-empty directories. Users should be aware of this limitation and attempt
 * to manually maintain their working directory to avoid test failures.
 * ============================================================================
 */

#ifdef HAVE_S3

#include "tiledb/common/common.h"
#include "tiledb/common/filesystem/directory_entry.h"

#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/sts/STSClient.h>
#include <boost/interprocess/streams/bufferstream.hpp>
#include <fstream>
#include <iostream>

#include "tiledb/common/logger.h"
#include "tiledb/common/unique_rwlock.h"
#include "tiledb/platform/platform.h"
#include "tiledb/sm/config/config_iter.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/tdb_math.h"

#ifdef _WIN32
#if !defined(NOMINMAX)
#define NOMINMAX
#endif
#include <Windows.h>
#undef GetMessage  // workaround for
                   // https://github.com/aws/aws-sdk-cpp/issues/402
#endif

#include "tiledb/sm/filesystem/s3.h"
#include "tiledb/sm/filesystem/s3/STSProfileWithWebIdentityCredentialsProvider.h"
#include "tiledb/sm/misc/parallel_functions.h"

using tiledb::common::filesystem::directory_entry;

namespace {

/*
 * Functions to convert strings to AWS enums.
 *
 * The AWS SDK provides some enum conversion functions, but they must not be
 * used, because they have non-deterministic behavior in certain scenarios.
 */

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

/**
 * Return a S3 enum value for any recognized string or NOT_SET if
 * B) the string is not recognized to match any of the enum values
 *
 * @param canned_acl_str A textual string naming one of the
 *        Aws::S3::Model::ObjectCannedACL enum members.
 */
Aws::S3::Model::ObjectCannedACL S3_ObjectCannedACL_from_str(
    const std::string& canned_acl_str) {
  if (canned_acl_str.empty())
    return Aws::S3::Model::ObjectCannedACL::NOT_SET;

  if (canned_acl_str == "NOT_SET")
    return Aws::S3::Model::ObjectCannedACL::NOT_SET;
  else if (canned_acl_str == "private_")
    return Aws::S3::Model::ObjectCannedACL::private_;
  else if (canned_acl_str == "public_read")
    return Aws::S3::Model::ObjectCannedACL::public_read;
  else if (canned_acl_str == "public_read_write")
    return Aws::S3::Model::ObjectCannedACL::public_read_write;
  else if (canned_acl_str == "authenticated_read")
    return Aws::S3::Model::ObjectCannedACL::authenticated_read;
  else if (canned_acl_str == "aws_exec_read")
    return Aws::S3::Model::ObjectCannedACL::aws_exec_read;
  else if (canned_acl_str == "bucket_owner_read")
    return Aws::S3::Model::ObjectCannedACL::bucket_owner_read;
  else if (canned_acl_str == "bucket_owner_full_control")
    return Aws::S3::Model::ObjectCannedACL::bucket_owner_full_control;
  else
    return Aws::S3::Model::ObjectCannedACL::NOT_SET;
}

/**
 * Return a S3 enum value for any recognized string or NOT_SET if
 * B) the string is not recognized to match any of the enum values
 *
 * @param canned_acl_str A textual string naming one of the
 *        Aws::S3::Model::BucketCannedACL enum members.
 */
Aws::S3::Model::BucketCannedACL S3_BucketCannedACL_from_str(
    const std::string& canned_acl_str) {
  if (canned_acl_str.empty())
    return Aws::S3::Model::BucketCannedACL::NOT_SET;

  if (canned_acl_str == "NOT_SET")
    return Aws::S3::Model::BucketCannedACL::NOT_SET;
  else if (canned_acl_str == "private_")
    return Aws::S3::Model::BucketCannedACL::private_;
  else if (canned_acl_str == "public_read")
    return Aws::S3::Model::BucketCannedACL::public_read;
  else if (canned_acl_str == "public_read_write")
    return Aws::S3::Model::BucketCannedACL::public_read_write;
  else if (canned_acl_str == "authenticated_read")
    return Aws::S3::Model::BucketCannedACL::authenticated_read;
  else
    return Aws::S3::Model::BucketCannedACL::NOT_SET;
}

/**
 * Return a S3 enum value for any recognized string or NOT_SET if
 * B) the string is not recognized to match any of the enum values
 *
 * @param storage_class_str A textual string naming one of the
 *        Aws::S3::Model::StorageClass enum members.
 */
Aws::S3::Model::StorageClass S3_StorageClass_from_str(
    const std::string& storage_class_str) {
  using Aws::S3::Model::StorageClass;
  if (storage_class_str.empty())
    return StorageClass::NOT_SET;

  if (storage_class_str == "NOT_SET")
    return StorageClass::NOT_SET;
  else if (storage_class_str == "STANDARD")
    return StorageClass::STANDARD;
  else if (storage_class_str == "REDUCED_REDUNDANCY")
    return StorageClass::REDUCED_REDUNDANCY;
  else if (storage_class_str == "STANDARD_IA")
    return StorageClass::STANDARD_IA;
  else if (storage_class_str == "ONEZONE_IA")
    return StorageClass::ONEZONE_IA;
  else if (storage_class_str == "INTELLIGENT_TIERING")
    return StorageClass::INTELLIGENT_TIERING;
  else if (storage_class_str == "GLACIER")
    return StorageClass::GLACIER;
  else if (storage_class_str == "DEEP_ARCHIVE")
    return StorageClass::DEEP_ARCHIVE;
  else if (storage_class_str == "OUTPOSTS")
    return StorageClass::OUTPOSTS;
  else if (storage_class_str == "GLACIER_IR")
    return StorageClass::GLACIER_IR;
  else if (storage_class_str == "SNOW")
    return StorageClass::SNOW;
  else if (storage_class_str == "EXPRESS_ONEZONE")
    return StorageClass::EXPRESS_ONEZONE;
  else
    return StorageClass::NOT_SET;
}

}  // namespace

using namespace tiledb::common;

namespace tiledb::sm {

S3Parameters::Headers S3Parameters::load_headers(const Config& cfg) {
  Headers ret;
  auto iter = ConfigIter(cfg, constants::s3_header_prefix);
  for (; !iter.end(); iter.next()) {
    auto key = iter.param();
    if (key.size() == 0) {
      continue;
    }
    ret[key] = iter.value();
  }
  return ret;
}

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Ensures that the AWS library is only initialized once per process. */
static std::once_flag aws_lib_initialized;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

S3::S3(
    stats::Stats* parent_stats, ThreadPool* thread_pool, const Config& config)
    : s3_params_(S3Parameters(config))
    , stats_(parent_stats->create_child("S3"))
    , state_(State::UNINITIALIZED)
    , credentials_provider_(nullptr)
    , file_buffer_size_(
          s3_params_.multipart_part_size_ * s3_params_.max_parallel_ops_)
    , vfs_thread_pool_(thread_pool)
    , request_payer_(
          s3_params_.requester_pays_ ? Aws::S3::Model::RequestPayer::requester :
                                       Aws::S3::Model::RequestPayer::NOT_SET)
    , sse_(Aws::S3::Model::ServerSideEncryption::NOT_SET)
    , storage_class_(S3_StorageClass_from_str(s3_params_.storage_class_))
    , object_canned_acl_(
          S3_ObjectCannedACL_from_str(s3_params_.object_acl_str_))
    , bucket_canned_acl_(
          S3_BucketCannedACL_from_str(s3_params_.bucket_acl_str_))
    , ssl_config_(S3SSLConfig(config)) {
  assert(thread_pool);
  options_.loggingOptions.logLevel =
      aws_log_name_to_level(s3_params_.logging_level_);

  // By default, curl sets the signal handler for SIGPIPE to SIG_IGN while
  // executing. When curl is done executing, it restores the previous signal
  // handler. This is not thread safe, so the AWS SDK disables this behavior
  // in curl using the `CURLOPT_NOSIGNAL` option.
  // Here, we set the `installSigPipeHandler` AWS SDK option to `true` to allow
  // the AWS SDK to set its own signal handler to ignore SIGPIPE signals. A
  // SIGPIPE may be raised from the socket library when the peer disconnects
  // unexpectedly.
  options_.httpOptions.installSigPipeHandler =
      config.get<bool>("vfs.s3.install_sigpipe_handler", Config::must_find);

  // Initialize the library once per process.
  if (!s3_params_.skip_init_)
    std::call_once(aws_lib_initialized, [this]() { Aws::InitAPI(options_); });
  if (options_.loggingOptions.logLevel != Aws::Utils::Logging::LogLevel::Off) {
    Aws::Utils::Logging::InitializeAWSLogging(
        Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
            "TileDB", Aws::Utils::Logging::LogLevel::Trace, "tiledb_s3_"));
  }

  auto sse = s3_params_.sse_algorithm_;
  if (!sse.empty()) {
    if (sse == "aes256") {
      sse_ = Aws::S3::Model::ServerSideEncryption::AES256;
    } else if (sse == "kms") {
      sse_ = Aws::S3::Model::ServerSideEncryption::aws_kms;
      if (s3_params_.sse_kms_key_id_.empty()) {
        throw S3Exception(
            "Config parameter 'vfs.s3.sse_kms_key_id' must be set "
            "for kms server-side encryption.");
      }
    } else {
      throw S3Exception(
          "Unknown 'vfs.s3.sse' config value " + sse +
          "; supported values are 'aes256' and 'kms'.");
    }
  }

  // Ensure `sse_kms_key_id` was only set for kms encryption.
  if (!s3_params_.sse_kms_key_id_.empty() &&
      sse_ != Aws::S3::Model::ServerSideEncryption::aws_kms) {
    throw S3Exception(
        "Config parameter 'vfs.s3.sse_kms_key_id' may only be "
        "set for 'vfs.s3.sse' == 'kms'.");
  }

  state_ = State::INITIALIZED;
}

S3::~S3() {
  /**
   * Note: if s3 fails to disconnect, the Status must be logged.
   * Right now, there aren't means to adjust s3 issues that may cause
   * disconnection failure.
   * In the future, the Governor class may be invoked here as a means of
   * handling s3 connection issues.
   */
  Status st = disconnect();
  if (!st.ok()) {
    LOG_STATUS_NO_RETURN_VALUE(st);
  } else {
    assert(state_ == State::DISCONNECTED);
    for (auto& buff : file_buffers_)
      tdb_delete(buff.second);
  }
}

/* ********************************* */
/*                 API               */
/* ********************************* */

void S3::create_bucket(const URI& bucket) const {
  throw_if_not_ok(init_client());

  if (!bucket.is_s3()) {
    throw S3Exception(
        std::string("URI is not an S3 URI: " + bucket.to_string()));
  }

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::CreateBucketRequest create_bucket_request;
  create_bucket_request.SetBucket(aws_uri.GetAuthority());

  // Set the bucket location constraint equal to the S3 region.
  // Note: empty string and 'us-east-1' are parsing errors in the SDK.
  if (!s3_params_.region_.empty() && s3_params_.region_ != "us-east-1") {
    Aws::S3::Model::CreateBucketConfiguration cfg;
    Aws::String region_str(s3_params_.region_.c_str());
    auto location_constraint = Aws::S3::Model::BucketLocationConstraintMapper::
        GetBucketLocationConstraintForName(region_str);
    cfg.SetLocationConstraint(location_constraint);
    create_bucket_request.SetCreateBucketConfiguration(cfg);
  }

  if (bucket_canned_acl_ != Aws::S3::Model::BucketCannedACL::NOT_SET) {
    create_bucket_request.SetACL(bucket_canned_acl_);
  }

  auto create_bucket_outcome = client_->CreateBucket(create_bucket_request);
  if (!create_bucket_outcome.IsSuccess()) {
    throw S3Exception(
        std::string("Failed to create S3 bucket ") + bucket.to_string() +
        outcome_error_message(create_bucket_outcome));
  }

  throw_if_not_ok(wait_for_bucket_to_be_created(bucket));
}

void S3::empty_bucket(const URI& bucket) const {
  throw_if_not_ok(init_client());

  auto uri_dir = bucket.add_trailing_slash();
  remove_dir(uri_dir);
}

bool S3::is_empty_bucket(const URI& bucket) const {
  throw_if_not_ok(init_client());

  bool exists = is_bucket(bucket);
  if (!exists) {
    throw S3Exception("Cannot check if bucket is empty; Bucket does not exist");
  }

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::ListObjectsV2Request list_objects_request;
  list_objects_request.SetBucket(aws_uri.GetAuthority());
  list_objects_request.SetPrefix("");
  list_objects_request.SetDelimiter("/");
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET) {
    list_objects_request.SetRequestPayer(request_payer_);
  }
  auto list_objects_outcome = client_->ListObjectsV2(list_objects_request);

  if (!list_objects_outcome.IsSuccess()) {
    throw S3Exception(
        std::string("Failed to list s3 objects in bucket ") + bucket.c_str() +
        outcome_error_message(list_objects_outcome));
  }

  return list_objects_outcome.GetResult().GetContents().empty() &&
         list_objects_outcome.GetResult().GetCommonPrefixes().empty();
}

bool S3::is_bucket(const URI& uri) const {
  throw_if_not_ok(init_client());

  if (!uri.is_s3()) {
    throw S3Exception(std::string("URI is not an S3 URI: " + uri.to_string()));
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::HeadBucketRequest head_bucket_request;
  head_bucket_request.SetBucket(aws_uri.GetAuthority());
  auto head_bucket_outcome = client_->HeadBucket(head_bucket_request);
  return head_bucket_outcome.IsSuccess();
}

void S3::move_dir(const URI& old_uri, const URI& new_uri) const {
  throw_if_not_ok(init_client());

  std::vector<std::string> paths;
  throw_if_not_ok(ls(old_uri, &paths, ""));
  for (const auto& path : paths) {
    auto suffix = path.substr(old_uri.to_string().size());
    auto new_path = new_uri.join_path(suffix);
    throw_if_not_ok(move_object(URI(path), URI(new_path)));
  }
}

void S3::copy_file(const URI& old_uri, const URI& new_uri) const {
  throw_if_not_ok(init_client());

  throw_if_not_ok(copy_object(old_uri, new_uri));
}

void S3::copy_dir(const URI& old_uri, const URI& new_uri) const {
  throw_if_not_ok(init_client());

  std::string old_uri_string = old_uri.to_string();
  std::vector<std::string> paths;
  throw_if_not_ok(ls(old_uri, &paths));
  while (!paths.empty()) {
    std::string file_name_abs = paths.front();
    URI file_name_uri = URI(file_name_abs);
    std::string file_name = file_name_abs.substr(old_uri_string.length());
    paths.erase(paths.begin());

    bool dir_exists;
    throw_if_not_ok(is_dir(file_name_uri, &dir_exists));
    if (dir_exists) {
      std::vector<std::string> child_paths;
      throw_if_not_ok(ls(file_name_uri, &child_paths));
      paths.insert(paths.end(), child_paths.begin(), child_paths.end());
    } else {
      std::string new_path_string = new_uri.to_string() + file_name;
      URI new_path_uri = URI(new_path_string);
      throw_if_not_ok(copy_object(file_name_uri, new_path_uri));
    }
  }
}

void S3::read(
    const URI& uri,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes,
    bool) const {
  uint64_t nbytes_read = 0;
  throw_if_not_ok(read_impl(uri, offset, buffer, nbytes, 0, &nbytes_read));
}

void S3::remove_bucket(const URI& bucket) const {
  throw_if_not_ok(init_client());

  // Empty bucket
  empty_bucket(bucket);

  // Delete bucket
  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::DeleteBucketRequest delete_bucket_request;
  delete_bucket_request.SetBucket(aws_uri.GetAuthority());
  auto delete_bucket_outcome = client_->DeleteBucket(delete_bucket_request);
  if (!delete_bucket_outcome.IsSuccess()) {
    throw S3Exception(
        std::string("Failed to remove S3 bucket ") + bucket.to_string() +
        outcome_error_message(delete_bucket_outcome));
  }
}

void S3::remove_dir(const URI& uri) const {
  throw_if_not_ok(init_client());

  std::vector<std::string> paths;
  throw_if_not_ok(ls(uri, &paths, ""));

  // Bail early if we don't have anything to delete.
  if (paths.empty()) {
    return;
  }

  throw_if_not_ok(
      parallel_for(vfs_thread_pool_, 0, paths.size(), [&](size_t i) {
        throw_if_not_ok(remove_object(URI(paths[i])));
        return Status::Ok();
      }));

  // Minio changed their delete behavior when an object masks another object
  // with the same prefix. Previously, minio would delete any object with
  // a matching prefix. The new behavior is to only delete the object masking
  // the "directory" of objects below. To handle this we just run a second
  // ls to see if we still have paths to remove, and remove them if so.

  paths.clear();
  throw_if_not_ok(ls(uri, &paths, ""));

  // We got everything on the first pass.
  if (paths.empty()) {
    return;
  }

  // Delete the uncovered object prefixes.
  throw_if_not_ok(
      parallel_for(vfs_thread_pool_, 0, paths.size(), [&](size_t i) {
        throw_if_not_ok(remove_object(URI(paths[i])));
        return Status::Ok();
      }));
}

void S3::touch(const URI& uri) const {
  throw_if_not_ok(init_client());

  if (!uri.is_s3()) {
    throw S3Exception(std::string(
        "Cannot create file; URI is not an S3 URI: " + uri.to_string()));
  }

  if (uri.to_string().back() == '/') {
    throw S3Exception(std::string(
        "Cannot create file; URI is a directory: " + uri.to_string()));
  }

  bool exists;
  throw_if_not_ok(is_object(uri, &exists));
  if (exists) {
    return;
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::PutObjectRequest put_object_request;
  put_object_request.WithKey(aws_uri.GetPath())
      .WithBucket(aws_uri.GetAuthority());

  auto request_stream =
      Aws::MakeShared<Aws::StringStream>(constants::s3_allocation_tag.c_str());
  put_object_request.SetBody(request_stream);
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    put_object_request.SetRequestPayer(request_payer_);
  if (sse_ != Aws::S3::Model::ServerSideEncryption::NOT_SET)
    put_object_request.SetServerSideEncryption(sse_);
  if (!s3_params_.sse_kms_key_id_.empty())
    put_object_request.SetSSEKMSKeyId(
        Aws::String(s3_params_.sse_kms_key_id_.c_str()));
  // TODO: These checks are not needed since AWS SDK 1.11.275
  // https://github.com/aws/aws-sdk-cpp/pull/2875
  if (storage_class_ != Aws::S3::Model::StorageClass::NOT_SET)
    put_object_request.SetStorageClass(storage_class_);
  if (object_canned_acl_ != Aws::S3::Model::ObjectCannedACL::NOT_SET) {
    put_object_request.SetACL(object_canned_acl_);
  }

  auto put_object_outcome = client_->PutObject(put_object_request);
  if (!put_object_outcome.IsSuccess()) {
    throw S3Exception(
        std::string("Cannot touch object '") + uri.c_str() +
        outcome_error_message(put_object_outcome));
  }

  throw_if_not_ok(wait_for_object_to_propagate(
      put_object_request.GetBucket(), put_object_request.GetKey()));
}

void S3::write(
    const URI& uri,
    const void* buffer,
    uint64_t length,
    bool remote_global_order_write) {
  throw_if_not_ok(init_client());

  if (!uri.is_s3()) {
    throw S3Exception(std::string("URI is not an S3 URI: " + uri.to_string()));
  }

  if (remote_global_order_write) {
    global_order_write_buffered(uri, buffer, length);
    return;
  }

  // This write is never considered the last part of an object. The last part is
  // only uploaded with flush_object().
  const bool is_last_part = false;

  // Get file buffer
  auto buff = (Buffer*)nullptr;
  throw_if_not_ok(get_file_buffer(uri, &buff));

  // Fill file buffer
  uint64_t nbytes_filled;
  throw_if_not_ok(fill_file_buffer(buff, buffer, length, &nbytes_filled));

  if ((!s3_params_.use_multipart_upload_) && (nbytes_filled != length)) {
    std::stringstream errmsg;
    errmsg << "Direct write failed! " << nbytes_filled
           << " bytes written to buffer, " << length << " bytes requested.";
    throw S3Exception(errmsg.str());
  }

  // Flush file buffer
  // multipart objects will flush whenever the writes exceed file_buffer_size_
  // write_direct should just append to buffer and upload later
  if (s3_params_.use_multipart_upload_) {
    if (buff->size() == file_buffer_size_)
      throw_if_not_ok(flush_file_buffer(uri, buff, is_last_part));

    uint64_t new_length = length - nbytes_filled;
    uint64_t offset = nbytes_filled;
    // Write chunks
    while (new_length > 0) {
      if (new_length >= file_buffer_size_) {
        throw_if_not_ok(write_multipart(
            uri, (char*)buffer + offset, file_buffer_size_, is_last_part));
        offset += file_buffer_size_;
        new_length -= file_buffer_size_;
      } else {
        throw_if_not_ok(fill_file_buffer(
            buff, (char*)buffer + offset, new_length, &nbytes_filled));
        offset += nbytes_filled;
        new_length -= nbytes_filled;
      }
    }
    assert(offset == length);
  }
}

std::vector<directory_entry> S3::ls_with_sizes(const URI& parent) const {
  return ls_with_sizes(parent, "/", -1);
}

Status S3::disconnect() {
  Status ret_st = Status::Ok();

  if (state_ == State::UNINITIALIZED) {
    return ret_st;
  }

  // Read-lock 'multipart_upload_states_'.
  UniqueReadLock unique_rl(&multipart_upload_rwlock_);

  if (multipart_upload_states_.size() > 0) {
    RETURN_NOT_OK(init_client());

    std::vector<const MultiPartUploadState*> states;
    states.reserve(multipart_upload_states_.size());
    for (auto& kv : multipart_upload_states_)
      states.emplace_back(&kv.second);

    auto status =
        parallel_for(vfs_thread_pool_, 0, states.size(), [&](uint64_t i) {
          const MultiPartUploadState* state = states[i];
          // Lock multipart state
          std::unique_lock<std::mutex> state_lck(state->mtx);

          if (state->st.ok()) {
            Aws::S3::Model::CompleteMultipartUploadRequest complete_request =
                make_multipart_complete_request(*state);
            auto outcome = client_->CompleteMultipartUpload(complete_request);
            if (!outcome.IsSuccess()) {
              const Status st = LOG_STATUS(Status_S3Error(
                  std::string("Failed to disconnect and flush S3 objects. ") +
                  outcome_error_message(outcome)));
              if (!st.ok()) {
                ret_st = st;
              }
            }
          } else {
            Aws::S3::Model::AbortMultipartUploadRequest abort_request =
                make_multipart_abort_request(*state);
            auto outcome = client_->AbortMultipartUpload(abort_request);
            if (!outcome.IsSuccess()) {
              ret_st = LOG_STATUS(Status_S3Error(
                  std::string("Failed to disconnect and flush S3 objects. ") +
                  outcome_error_message(outcome)));
            } else {
              ret_st = LOG_STATUS(Status_S3Error(
                  std::string("Failed to disconnect and flush S3 objects. ")));
            }
          }
          return Status::Ok();
        });

    RETURN_NOT_OK(status);
  }

  unique_rl.unlock();

  if (options_.loggingOptions.logLevel != Aws::Utils::Logging::LogLevel::Off) {
    Aws::Utils::Logging::ShutdownAWSLogging();
  }

  if (s3_tp_executor_) {
    s3_tp_executor_->Stop();
  }

  state_ = State::DISCONNECTED;
  return ret_st;
}

Status S3::flush_object(const URI& uri) {
  RETURN_NOT_OK(init_client());
  if (!s3_params_.use_multipart_upload_) {
    return flush_direct(uri);
  }
  if (!uri.is_s3()) {
    return LOG_STATUS(Status_S3Error(
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
  UniqueReadLock unique_rl(&multipart_upload_rwlock_);

  // Do nothing - empty object
  auto state_iter = multipart_upload_states_.find(path_c_str);
  if (state_iter == multipart_upload_states_.end()) {
    RETURN_NOT_OK(flush_st);
    return Status::Ok();
  }

  const MultiPartUploadState* state = &multipart_upload_states_.at(path_c_str);
  // Lock multipart state
  std::unique_lock<std::mutex> state_lck(state->mtx);
  unique_rl.unlock();

  if (state->st.ok()) {
    Aws::S3::Model::CompleteMultipartUploadRequest complete_request =
        make_multipart_complete_request(*state);
    auto outcome = client_->CompleteMultipartUpload(complete_request);
    if (!outcome.IsSuccess()) {
      return LOG_STATUS(Status_S3Error(
          std::string("Failed to flush S3 object ") + uri.c_str() +
          outcome_error_message(outcome)));
    }

    auto bucket = state->bucket;
    auto key = state->key;
    // It is safe to unlock the state here
    state_lck.unlock();

    throw_if_not_ok(wait_for_object_to_propagate(move(bucket), move(key)));

    return finish_flush_object(std::move(outcome), uri, buff, false);
  } else {
    Aws::S3::Model::AbortMultipartUploadRequest abort_request =
        make_multipart_abort_request(*state);

    auto outcome = client_->AbortMultipartUpload(abort_request);

    state_lck.unlock();

    return finish_flush_object(std::move(outcome), uri, buff, true);
  }
}

void S3::finalize_and_flush_object(const URI& uri) {
  throw_if_not_ok(init_client());
  if (!s3_params_.use_multipart_upload_) {
    throw S3Exception(
        "Global order write failed! S3 multipart upload is disabled from "
        "config");
  }

  Aws::Http::URI aws_uri{uri.c_str()};
  std::string uri_path{aws_uri.GetPath().c_str()};

  // Take a lock protecting 'multipart_upload_states_'.
  UniqueReadLock unique_rl{&multipart_upload_rwlock_};

  auto state_iter = multipart_upload_states_.find(uri_path);
  if (state_iter == multipart_upload_states_.end()) {
    throw S3Exception(
        "Global order write failed! Couldn't find a multipart upload state "
        "associated with buffer: " +
        uri.last_path_part());
  }

  auto& state = state_iter->second;
  unique_rl.unlock();

  // If there are no intermediate chunks, it means they have all been uploaded
  // during the previous submit() call
  auto& intermediate_chunks = state.buffered_chunks;
  if (!intermediate_chunks.empty()) {
    uint64_t sum_sizes = 0;
    std::vector<uint64_t> offsets;
    for (auto& chunk : intermediate_chunks) {
      offsets.push_back(sum_sizes);
      sum_sizes += chunk.size;
    }

    std::vector<std::byte> merged(sum_sizes);
    throw_if_not_ok(parallel_for(
        vfs_thread_pool_, 0, intermediate_chunks.size(), [&](size_t i) {
          uint64_t length_returned;
          throw_if_not_ok(read_impl(
              URI(intermediate_chunks[i].uri),
              0,
              merged.data() + offsets[i],
              intermediate_chunks[i].size,
              0,
              &length_returned));
          return Status::Ok();
        }));

    const int part_num = state.part_number++;
    auto ctx = make_upload_part_req(
        aws_uri, merged.data(), merged.size(), state.upload_id, part_num);
    throw_if_not_ok(get_make_upload_part_req(uri, uri_path, ctx));
  }

  if (state.st.ok()) {
    Aws::S3::Model::CompleteMultipartUploadRequest complete_request =
        make_multipart_complete_request(state);
    auto outcome = client_->CompleteMultipartUpload(complete_request);
    if (!outcome.IsSuccess()) {
      throw S3Exception{
          std::string("Failed to flush S3 object ") + uri.c_str() +
          outcome_error_message(outcome)};
    }

    throw_if_not_ok(wait_for_object_to_propagate(state.bucket, state.key));
  } else {
    Aws::S3::Model::AbortMultipartUploadRequest abort_request =
        make_multipart_abort_request(state);

    auto outcome = client_->AbortMultipartUpload(abort_request);
    if (!outcome.IsSuccess()) {
      throw S3Exception{
          std::string("Failed to flush S3 object ") + uri.c_str() +
          outcome_error_message(outcome)};
    } else {
      throw S3Exception{
          std::string("Failed to flush S3 object ") + uri.c_str()};
    }
  }

  // Remove intermediate chunk files if any
  throw_if_not_ok(parallel_for(
      vfs_thread_pool_, 0, intermediate_chunks.size(), [&](size_t i) {
        throw_if_not_ok(remove_object(URI(intermediate_chunks[i].uri)));
        return Status::Ok();
      }));

  // Remove the multipart upload state entry
  UniqueWriteLock unique_wl(&multipart_upload_rwlock_);
  multipart_upload_states_.erase(uri_path);
  unique_wl.unlock();
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

Status S3::is_object(const URI& uri, bool* const exists) const {
  throw_if_not_ok(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status_S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.c_str();

  return is_object(aws_uri.GetAuthority(), aws_uri.GetPath(), exists);
}

Status S3::is_object(
    const Aws::String& bucket_name,
    const Aws::String& object_key,
    bool* const exists) const {
  throw_if_not_ok(init_client());

  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.SetBucket(bucket_name);
  head_object_request.SetKey(object_key);
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    head_object_request.SetRequestPayer(request_payer_);
  auto head_object_outcome = client_->HeadObject(head_object_request);
  *exists = head_object_outcome.IsSuccess();

  return Status::Ok();
}

Status S3::ls(
    const URI& prefix,
    std::vector<std::string>* paths,
    const std::string& delimiter,
    int max_paths) const {
  for (auto& fs : ls_with_sizes(prefix, delimiter, max_paths)) {
    paths->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

std::vector<directory_entry> S3::ls_with_sizes(
    const URI& prefix, const std::string& delimiter, int max_paths) const {
  throw_if_not_ok(init_client());

  const auto prefix_dir = prefix.add_trailing_slash();

  auto prefix_str = prefix_dir.to_string();
  if (!prefix_dir.is_s3()) {
    throw S3Exception("URI is not an S3 URI: " + prefix_str);
  }

  Aws::Http::URI aws_uri = prefix_str.c_str();
  auto aws_prefix = remove_front_slash(aws_uri.GetPath().c_str());
  std::string aws_auth = aws_uri.GetAuthority().c_str();
  Aws::S3::Model::ListObjectsV2Request list_objects_request;
  list_objects_request.SetBucket(aws_uri.GetAuthority());
  list_objects_request.SetPrefix(aws_prefix.c_str());
  list_objects_request.SetDelimiter(delimiter.c_str());
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET) {
    list_objects_request.SetRequestPayer(request_payer_);
  }

  std::vector<directory_entry> entries;

  bool is_done = false;
  while (!is_done) {
    // Not requesting more items than needed
    if (max_paths != -1) {
      list_objects_request.SetMaxKeys(
          max_paths - static_cast<int>(entries.size()));
    }
    auto list_objects_outcome = client_->ListObjectsV2(list_objects_request);

    if (!list_objects_outcome.IsSuccess()) {
      throw S3Exception(
          std::string("Error while listing with prefix '") + prefix_str +
          "' and delimiter '" + delimiter + "'" +
          outcome_error_message(list_objects_outcome));
    }

    for (const auto& object : list_objects_outcome.GetResult().GetContents()) {
      std::string file(object.GetKey().c_str());
      uint64_t size = object.GetSize();
      entries.emplace_back(
          "s3://" + aws_auth + add_front_slash(file), size, false);
    }

    for (const auto& object :
         list_objects_outcome.GetResult().GetCommonPrefixes()) {
      std::string file(object.GetPrefix().c_str());
      // For "directories" it doesn't seem possible to get a shallow size in
      // S3, so the size of such an entry will be 0 in S3.
      entries.emplace_back(
          "s3://" + aws_auth + add_front_slash(remove_trailing_slash(file)),
          0,
          true);
    }

    is_done =
        !list_objects_outcome.GetResult().GetIsTruncated() ||
        (max_paths != -1 && entries.size() >= static_cast<size_t>(max_paths));
    if (!is_done) {
      Aws::String next_marker =
          list_objects_outcome.GetResult().GetNextContinuationToken();
      if (next_marker.empty()) {
        throw S3Exception(
            "Failed to retrieve next continuation "
            "token for ListObjectsV2 request.");
      }
      list_objects_request.SetContinuationToken(std::move(next_marker));
    }
  }

  return entries;
}

Status S3::move_object(const URI& old_uri, const URI& new_uri) const {
  RETURN_NOT_OK(init_client());

  RETURN_NOT_OK(copy_object(old_uri, new_uri));
  RETURN_NOT_OK(remove_object(old_uri));
  return Status::Ok();
}

Status S3::object_size(const URI& uri, uint64_t* nbytes) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status_S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  auto aws_path = remove_front_slash(aws_uri.GetPath().c_str());

  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.SetBucket(aws_uri.GetAuthority());
  head_object_request.SetKey(aws_path.c_str());
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    head_object_request.SetRequestPayer(request_payer_);
  auto head_object_outcome = client_->HeadObject(head_object_request);

  if (!head_object_outcome.IsSuccess())
    return LOG_STATUS(Status_S3Error(
        "Cannot retrieve S3 object size; Error while listing file " +
        uri.to_string() + outcome_error_message(head_object_outcome)));
  *nbytes =
      static_cast<uint64_t>(head_object_outcome.GetResult().GetContentLength());

  return Status::Ok();
}

Status S3::read_impl(
    const URI& uri,
    const off_t offset,
    void* const buffer,
    const uint64_t length,
    const uint64_t read_ahead_length,
    uint64_t* const length_returned) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status_S3Error(
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
        return Aws::New<PreallocatedIOStream>(
            constants::s3_allocation_tag.c_str(),
            buffer,
            length + read_ahead_length);
      });

  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    get_object_request.SetRequestPayer(request_payer_);

  auto get_object_outcome = client_->GetObject(get_object_request);
  if (!get_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status_S3Error(
        std::string("Failed to read S3 object ") + uri.c_str() +
        outcome_error_message(get_object_outcome)));
  }

  *length_returned =
      static_cast<uint64_t>(get_object_outcome.GetResult().GetContentLength());
  if (*length_returned < length) {
    return LOG_STATUS(Status_S3Error(
        std::string("Read operation returned different size of bytes ") +
        std::to_string(*length_returned) + " vs " + std::to_string(length)));
  }

  return Status::Ok();
}

Status S3::remove_object(const URI& uri) const {
  RETURN_NOT_OK(init_client());

  if (!uri.is_s3()) {
    return LOG_STATUS(Status_S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  Aws::S3::Model::DeleteObjectRequest delete_object_request;
  delete_object_request.SetBucket(aws_uri.GetAuthority());
  delete_object_request.SetKey(aws_uri.GetPath());
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    delete_object_request.SetRequestPayer(request_payer_);

  auto delete_object_outcome = client_->DeleteObject(delete_object_request);
  if (!delete_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status_S3Error(
        std::string("Failed to delete S3 object '") + uri.c_str() +
        outcome_error_message(delete_object_outcome)));
  }

  throw_if_not_ok(wait_for_object_to_be_deleted(
      delete_object_request.GetBucket(), delete_object_request.GetKey()));
  return Status::Ok();
}

void S3::global_order_write_buffered(
    const URI& uri, const void* buffer, uint64_t length) {
  throw_if_not_ok(init_client());

  if (!s3_params_.use_multipart_upload_) {
    throw S3Exception(
        "Global order write failed! S3 multipart upload is disabled from "
        "config");
  }

  // Get file buffer
  Buffer* buff{nullptr};
  throw_if_not_ok(get_file_buffer(uri, &buff));

  // Fill file buffer
  uint64_t nbytes_filled;
  throw_if_not_ok(fill_file_buffer(buff, buffer, length, &nbytes_filled));

  // Multipart objects will flush whenever the writes exceed file_buffer_size_
  if (buff->size() == file_buffer_size_) {
    global_order_write(uri, buff->data(), file_buffer_size_);
    buff->reset_size();
  }

  uint64_t bytes_left = length - nbytes_filled;
  uint64_t offset = nbytes_filled;
  // Write chunks
  while (bytes_left > 0) {
    if (bytes_left >= file_buffer_size_) {
      global_order_write(uri, (char*)buffer + offset, file_buffer_size_);
      offset += file_buffer_size_;
      bytes_left -= file_buffer_size_;
    } else {
      throw_if_not_ok(fill_file_buffer(
          buff, (char*)buffer + offset, bytes_left, &nbytes_filled));
      offset += nbytes_filled;
      bytes_left -= nbytes_filled;
    }
  }
}

void S3::global_order_write(
    const URI& uri, const void* buffer, uint64_t length) {
  throw_if_not_ok(init_client());

  const Aws::Http::URI aws_uri(uri.c_str());
  const std::string uri_path(aws_uri.GetPath().c_str());

  UniqueReadLock unique_rl(&multipart_upload_rwlock_);
  auto state_iter = multipart_upload_states_.find(uri_path);
  if (state_iter == multipart_upload_states_.end()) {
    unique_rl.unlock();
    UniqueWriteLock unique_wl(&multipart_upload_rwlock_);

    state_iter =
        multipart_upload_states_.emplace(uri_path, MultiPartUploadState())
            .first;

    unique_wl.unlock();

    bool exists;
    throw_if_not_ok(is_object(uri, &exists));
    if (exists) {
      throw_if_not_ok(remove_object(uri));
    }

    throw_if_not_ok(initiate_multipart_request(aws_uri, &state_iter->second));
  } else {
    // Unlock, as make_upload_part_req will reaquire as necessary.
    unique_rl.unlock();
  }

  auto& state = state_iter->second;

  // Read the comments near BufferedChunk definition to get a better
  // understanding of what intermediate chunks are and how they function
  // within an s3 global order write
  auto& intermediate_chunks = state.buffered_chunks;
  uint64_t sum_sizes = 0;
  std::vector<uint64_t> offsets;
  offsets.reserve(intermediate_chunks.size() + 1);
  for (auto& chunk : intermediate_chunks) {
    offsets.push_back(sum_sizes);
    sum_sizes += chunk.size;
  }

  // Account for the current chunk too
  offsets.push_back(sum_sizes);
  sum_sizes += length;

  if (sum_sizes < tiledb::sm::constants::s3_min_multipart_part_size) {
    auto new_uri = generate_chunk_uri(uri, intermediate_chunks.size());
    write_direct(new_uri, buffer, length);
    intermediate_chunks.emplace_back(new_uri.to_string(), length);
    return;
  }

  // We have enough buffered for a multipart part upload.
  // Read all previously written chunks into one contiguous buffer and add the
  // current data at the end.
  std::vector<std::byte> merged(sum_sizes);
  throw_if_not_ok(parallel_for(
      vfs_thread_pool_, 0, intermediate_chunks.size(), [&](size_t i) {
        uint64_t length_returned;
        throw_if_not_ok(read_impl(
            URI(intermediate_chunks[i].uri),
            0,
            merged.data() + offsets[i],
            intermediate_chunks[i].size,
            0,
            &length_returned));
        return Status::Ok();
      }));
  std::memcpy(merged.data() + offsets.back(), buffer, length);

  // Issue one more multiple part uploads depending on the cumulative size of
  // the merged chunks. It's a valid case that one of the submit() calls brings
  // here a 10GB attribute write request, so we need to account for it and issue
  // parallel writes if necessary
  uint64_t num_ops = utils::math::ceil(
      merged.size(),
      std::max(
          s3_params_.multipart_part_size_,
          tiledb::sm::constants::s3_min_multipart_part_size));
  num_ops =
      std::min(std::max(num_ops, uint64_t(1)), s3_params_.max_parallel_ops_);

  auto upload_id = state.upload_id;

  // Assign the part number(s), and make the write request.
  if (num_ops == 1) {
    const int part_num = state.part_number++;
    auto ctx = make_upload_part_req(
        aws_uri, merged.data(), merged.size(), upload_id, part_num);
    throw_if_not_ok(get_make_upload_part_req(uri, uri_path, ctx));
  } else {
    std::vector<MakeUploadPartCtx> ctx_vec;
    ctx_vec.resize(num_ops);
    const uint64_t bytes_per_op = s3_params_.multipart_part_size_;
    const int part_num_base = state.part_number;
    throw_if_not_ok(
        parallel_for(vfs_thread_pool_, 0, num_ops, [&](uint64_t op_idx) {
          uint64_t begin = op_idx * bytes_per_op,
                   end = std::min(
                       (op_idx + 1) * bytes_per_op - 1,
                       uint64_t(merged.size() - 1));
          uint64_t thread_nbytes = end - begin + 1;
          auto thread_buffer = static_cast<const char*>(
                                   static_cast<const void*>(merged.data())) +
                               begin;
          int part_num = static_cast<int>(part_num_base + op_idx);
          ctx_vec[op_idx] = make_upload_part_req(
              aws_uri, thread_buffer, thread_nbytes, upload_id, part_num);

          return Status::Ok();
        }));
    state.part_number += num_ops;

    for (auto& ctx : ctx_vec) {
      auto st = get_make_upload_part_req(uri, uri_path, ctx);
      if (!st.ok()) {
        throw S3Exception{"S3 parallel write multipart error; " + st.message()};
      }
    }
  }

  throw_if_not_ok(parallel_for(
      vfs_thread_pool_, 0, intermediate_chunks.size(), [&](size_t i) {
        throw_if_not_ok(remove_object(URI(intermediate_chunks[i].uri)));
        return Status::Ok();
      }));

  intermediate_chunks.clear();
}

std::string S3::add_front_slash(const std::string& path) {
  return (path.front() != '/') ? (std::string("/") + path) : path;
}

std::string S3::remove_front_slash(const std::string& path) {
  if (path.front() == '/')
    return path.substr(1, path.length());
  return path;
}

std::string S3::remove_trailing_slash(const std::string& path) {
  if (path.back() == '/') {
    return path.substr(0, path.length() - 1);
  }

  return path;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

Status S3::init_client() const {
  assert(state_ == State::INITIALIZED);

  std::lock_guard<std::mutex> lck(client_init_mtx_);

  auto s3_config_source = s3_params_.config_source_;
  if (s3_config_source != "auto" && s3_config_source != "config_files" &&
      s3_config_source != "sts_profile_with_web_identity") {
    throw S3Exception(
        "Unknown 'vfs.s3.config_source' config value " + s3_config_source +
        "; supported values are 'auto', 'config_files' and "
        "'sts_profile_with_web_identity'");
  }

  if (client_ != nullptr) {
    // Check credentials. If expired, refresh it
    if (credentials_provider_) {
      Aws::Auth::AWSCredentials credentials =
          credentials_provider_->GetAWSCredentials();
      if (credentials.IsExpired()) {
        throw S3Exception("AWS credentials are expired.");
      } else if (!s3_params_.no_sign_request_ && credentials.IsEmpty()) {
        throw S3Exception(
            "AWS credentials were not provided. For public S3 data, consider "
            "setting the vfs.s3.no_sign_request config option.");
      }
    }
    return Status::Ok();
  }

  // ClientConfiguration should be lazily init'ed here in init_client to avoid
  // potential slowdowns for non s3 users as the ClientConfig now attempts to
  // check for client configuration on create, which can be slow if aws is not
  // configured on a users systems due to ec2 metadata check

  client_config_ = make_shared<Aws::S3::S3ClientConfiguration>(HERE());

  s3_tp_executor_ = make_shared<S3ThreadPoolExecutor>(HERE(), vfs_thread_pool_);

  client_config_->executor = s3_tp_executor_;

  auto& client_config = *client_config_.get();

  if (!s3_params_.region_.empty())
    client_config.region = s3_params_.region_.c_str();

  if (!s3_params_.endpoint_override_.empty()) {
    client_config.endpointOverride = s3_params_.endpoint_override_;
  }

  if (!s3_params_.proxy_host_.empty()) {
    client_config.proxyHost = s3_params_.proxy_host_;
    client_config.proxyPort = s3_params_.proxy_port_;
    client_config.proxyScheme = s3_params_.proxy_scheme_ == "https" ?
                                    Aws::Http::Scheme::HTTPS :
                                    Aws::Http::Scheme::HTTP;
    client_config.proxyUserName = s3_params_.proxy_username_;
    client_config.proxyPassword = s3_params_.proxy_password_;
  }

  client_config.scheme = (s3_params_.scheme_ == "http") ?
                             Aws::Http::Scheme::HTTP :
                             Aws::Http::Scheme::HTTPS;
  client_config.connectTimeoutMs = (long)s3_params_.connect_timeout_ms_;
  client_config.requestTimeoutMs = (long)s3_params_.request_timeout_ms_;
  client_config.caFile = ssl_config_.ca_file();
  client_config.caPath = ssl_config_.ca_path();
  client_config.verifySSL = ssl_config_.verify();

  client_config.retryStrategy = Aws::MakeShared<S3RetryStrategy>(
      constants::s3_allocation_tag.c_str(),
      stats_,
      s3_params_.connect_max_tries_,
      s3_params_.connect_scale_factor_);

  client_config.useVirtualAddressing = s3_params_.use_virtual_addressing_;
  client_config.payloadSigningPolicy =
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;

  // If the user says not to sign a request, use the
  // AnonymousAWSCredentialsProvider This is equivalent to --no-sign-request on
  // the aws cli
  if (s3_params_.no_sign_request_) {
    credentials_provider_ =
        make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(HERE());
  } else {  // Check other authentication methods
    switch ((!s3_params_.aws_access_key_id_.empty() ? 1 : 0) +
            (!s3_params_.aws_secret_access_key_.empty() ? 2 : 0) +
            (!s3_params_.aws_role_arn_.empty() ? 4 : 0) +
            (s3_config_source == "config_files" ? 8 : 0) +
            (s3_config_source == "sts_profile_with_web_identity" ? 16 : 0)) {
      case 0:
        break;
      case 1:
      case 2:
        throw S3Exception(
            "Insufficient authentication credentials; "
            "Both access key id and secret key are needed");
      case 3: {
        Aws::String access_key_id(s3_params_.aws_access_key_id_.c_str());
        Aws::String secret_access_key(
            s3_params_.aws_secret_access_key_.c_str());
        Aws::String session_token(
            !s3_params_.aws_session_token_.empty() ?
                s3_params_.aws_session_token_.c_str() :
                "");
        credentials_provider_ =
            make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
                HERE(), access_key_id, secret_access_key, session_token);
        break;
      }
      case 4: {
        // If AWS Role ARN provided instead of access_key and secret_key,
        // temporary credentials will be fetched by assuming this role.
        Aws::String role_arn(s3_params_.aws_role_arn_.c_str());
        Aws::String external_id(
            !s3_params_.aws_external_id_.empty() ?
                s3_params_.aws_external_id_.c_str() :
                "");
        int load_frequency(
            !s3_params_.aws_load_frequency_.empty() ?
                std::stoi(s3_params_.aws_load_frequency_) :
                Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS);
        Aws::String session_name(
            !s3_params_.aws_session_name_.empty() ?
                s3_params_.aws_session_name_.c_str() :
                "");
        credentials_provider_ =
            make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
                HERE(),
                role_arn,
                session_name,
                external_id,
                load_frequency,
                make_shared<Aws::STS::STSClient>(HERE(), client_config));
        break;
      }
      case 7: {
        s3_tp_executor_->Stop();

        throw S3Exception(
            "Ambiguous authentication credentials; both permanent and "
            "temporary authentication credentials are configured");
      }
      case 8: {
        credentials_provider_ =
            make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
                HERE());
        break;
      }
      case 16: {
        credentials_provider_ = make_shared<
            Aws::Auth::STSProfileWithWebIdentityCredentialsProvider>(
            HERE(),
            Aws::Auth::GetConfigProfileName(),
            std::chrono::minutes(60),
            [client_config](const auto& credentials) {
              return make_shared<Aws::STS::STSClient>(
                  HERE(), credentials, client_config);
            });
        break;
      }
      default: {
        s3_tp_executor_->Stop();

        throw S3Exception(
            "Ambiguous authentification options; Setting vfs.s3.config_source "
            "is "
            "mutually exclusive with providing either permanent or temporary "
            "credentials");
      }
    }
  }

  // The `Aws::S3::S3Client` constructor is not thread-safe. Although we
  // currently hold `client_init_mtx_` that protects this routine from threads
  // on this instance of `S3`, it is not sufficient protection from threads on
  // another instance of `S3`. Use an additional, static mutex for this
  // scenario.
  static std::mutex static_client_init_mtx;
  {
    std::lock_guard<std::mutex> static_lck(static_client_init_mtx);
    if (credentials_provider_ == nullptr) {
      client_ =
          make_shared<TileDBS3Client>(HERE(), s3_params_, *client_config_);
    } else {
      client_ = make_shared<TileDBS3Client>(
          HERE(), s3_params_, credentials_provider_, *client_config_);
    }
  }

  return Status::Ok();
}

Status S3::copy_object(const URI& old_uri, const URI& new_uri) const {
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
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    copy_object_request.SetRequestPayer(request_payer_);
  if (sse_ != Aws::S3::Model::ServerSideEncryption::NOT_SET)
    copy_object_request.SetServerSideEncryption(sse_);
  if (!s3_params_.sse_kms_key_id_.empty())
    copy_object_request.SetSSEKMSKeyId(
        Aws::String(s3_params_.sse_kms_key_id_.c_str()));
  if (object_canned_acl_ != Aws::S3::Model::ObjectCannedACL::NOT_SET) {
    copy_object_request.SetACL(object_canned_acl_);
  }

  auto copy_object_outcome = client_->CopyObject(copy_object_request);
  if (!copy_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status_S3Error(
        std::string("Failed to copy S3 object ") + old_uri.c_str() + " to " +
        new_uri.c_str() + outcome_error_message(copy_object_outcome)));
  }

  throw_if_not_ok(wait_for_object_to_propagate(
      copy_object_request.GetBucket(), copy_object_request.GetKey()));

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
  // TODO: remove this?
  std::unique_lock<std::mutex> lck(file_buffers_mtx_);

  auto uri_str = uri.to_string();
  auto it = file_buffers_.find(uri_str);
  if (it == file_buffers_.end()) {
    auto new_buff = tdb_new(Buffer);
    file_buffers_[uri_str] = new_buff;
    *buff = new_buff;
  } else {
    *buff = it->second;
  }

  return Status::Ok();
}

Status S3::initiate_multipart_request(
    Aws::Http::URI aws_uri, MultiPartUploadState* state) {
  RETURN_NOT_OK(init_client());

  auto path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  Aws::S3::Model::CreateMultipartUploadRequest multipart_upload_request;
  multipart_upload_request.SetBucket(aws_uri.GetAuthority());
  multipart_upload_request.SetKey(path);
  multipart_upload_request.SetContentType("application/octet-stream");
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    multipart_upload_request.SetRequestPayer(request_payer_);
  if (sse_ != Aws::S3::Model::ServerSideEncryption::NOT_SET)
    multipart_upload_request.SetServerSideEncryption(sse_);
  if (!s3_params_.sse_kms_key_id_.empty())
    multipart_upload_request.SetSSEKMSKeyId(
        Aws::String(s3_params_.sse_kms_key_id_.c_str()));
  if (storage_class_ != Aws::S3::Model::StorageClass::NOT_SET)
    multipart_upload_request.SetStorageClass(storage_class_);
  if (object_canned_acl_ != Aws::S3::Model::ObjectCannedACL::NOT_SET) {
    multipart_upload_request.SetACL(object_canned_acl_);
  }

  auto multipart_upload_outcome =
      client_->CreateMultipartUpload(multipart_upload_request);
  if (!multipart_upload_outcome.IsSuccess()) {
    return LOG_STATUS(Status_S3Error(
        std::string("Failed to create multipart request for object '") +
        path_c_str + outcome_error_message(multipart_upload_outcome)));
  }

  state->part_number = 1;
  state->bucket = aws_uri.GetAuthority();
  state->key = path;
  state->upload_id = multipart_upload_outcome.GetResult().GetUploadId();
  state->completed_parts = std::map<int, Aws::S3::Model::CompletedPart>();

  *state = MultiPartUploadState(
      1,
      Aws::String(aws_uri.GetAuthority()),
      Aws::String(path),
      Aws::String(multipart_upload_outcome.GetResult().GetUploadId()),
      std::map<int, Aws::S3::Model::CompletedPart>());

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
  throw_if_not_ok(init_client());

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

  return LOG_STATUS(Status_S3Error(
      "Failed waiting for object " +
      std::string(object_key.c_str(), object_key.size()) + " to be created."));
}

Status S3::wait_for_object_to_be_deleted(
    const Aws::String& bucket_name, const Aws::String& object_key) const {
  throw_if_not_ok(init_client());

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

  return LOG_STATUS(Status_S3Error(
      "Failed waiting for object " +
      std::string(object_key.c_str(), object_key.size()) + " to be deleted."));
}

Status S3::wait_for_bucket_to_be_created(const URI& bucket_uri) const {
  throw_if_not_ok(init_client());

  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    bool exists = is_bucket(bucket_uri);
    if (exists) {
      return Status::Ok();
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return LOG_STATUS(Status_S3Error(
      "Failed waiting for bucket " + bucket_uri.to_string() +
      " to be created."));
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
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    complete_request.SetRequestPayer(request_payer_);
  return complete_request.WithMultipartUpload(std::move(completed_upload));
}

Aws::S3::Model::AbortMultipartUploadRequest S3::make_multipart_abort_request(
    const MultiPartUploadState& state) {
  Aws::S3::Model::AbortMultipartUploadRequest abort_request;
  abort_request.SetBucket(state.bucket);
  abort_request.SetKey(state.key);
  abort_request.SetUploadId(state.upload_id);
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    abort_request.SetRequestPayer(request_payer_);
  return abort_request;
}

template <typename R, typename E>
Status S3::finish_flush_object(
    const Aws::Utils::Outcome<R, E>& outcome,
    const URI& uri,
    Buffer* const buff,
    bool is_abort) {
  Aws::Http::URI aws_uri = uri.c_str();

  UniqueWriteLock unique_wl(&multipart_upload_rwlock_);
  multipart_upload_states_.erase(aws_uri.GetPath().c_str());
  unique_wl.unlock();

  std::unique_lock<std::mutex> file_buffers_lck(file_buffers_mtx_);
  file_buffers_.erase(uri.to_string());
  file_buffers_lck.unlock();
  tdb_delete(buff);

  if (!outcome.IsSuccess() || is_abort) {
    std::string error_message =
        std::string("Failed to flush S3 object ") + uri.c_str();
    if (!is_abort) {
      error_message += outcome_error_message(outcome);
    }
    return LOG_STATUS(Status_S3Error(error_message));
  }

  return Status::Ok();
}

Status S3::flush_direct(const URI& uri) {
  RETURN_NOT_OK(init_client());

  // Get file buffer
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));

  write_direct(uri, buff->data(), buff->size());
  return Status::Ok();
}

void S3::write_direct(const URI& uri, const void* buffer, uint64_t length) {
  const Aws::Http::URI aws_uri(uri.c_str());
  const std::string uri_path(aws_uri.GetPath().c_str());

  Aws::S3::Model::PutObjectRequest put_object_request;

  auto stream = shared_ptr<Aws::IOStream>(
      new boost::interprocess::bufferstream((char*)buffer, length));

  put_object_request.SetBody(stream);
  put_object_request.SetContentLength(length);

  // we only want to hash once, and must do it after setting the body
  auto md5_hash =
      Aws::Utils::HashingUtils::CalculateMD5(*put_object_request.GetBody());

  put_object_request.SetContentMD5(
      Aws::Utils::HashingUtils::Base64Encode(md5_hash));
  put_object_request.SetContentType("application/octet-stream");
  put_object_request.SetBucket(aws_uri.GetAuthority());
  put_object_request.SetKey(aws_uri.GetPath());
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    put_object_request.SetRequestPayer(request_payer_);
  if (sse_ != Aws::S3::Model::ServerSideEncryption::NOT_SET)
    put_object_request.SetServerSideEncryption(sse_);
  if (!s3_params_.sse_kms_key_id_.empty())
    put_object_request.SetSSEKMSKeyId(
        Aws::String(s3_params_.sse_kms_key_id_.c_str()));
  if (storage_class_ != Aws::S3::Model::StorageClass::NOT_SET)
    put_object_request.SetStorageClass(storage_class_);
  if (object_canned_acl_ != Aws::S3::Model::ObjectCannedACL::NOT_SET) {
    put_object_request.SetACL(object_canned_acl_);
  }

  auto put_object_outcome = client_->PutObject(put_object_request);
  if (!put_object_outcome.IsSuccess()) {
    throw S3Exception(
        std::string("Cannot write object '") + uri.c_str() +
        outcome_error_message(put_object_outcome));
  }

  // verify the MD5 hash of the result
  // note the etag is hex-encoded not base64
  Aws::StringStream md5_hex;
  md5_hex << "\"" << Aws::Utils::HashingUtils::HexEncode(md5_hash) << "\"";
  if (md5_hex.str() != put_object_outcome.GetResult().GetETag()) {
    throw S3Exception(
        "Object uploaded successfully, but MD5 hash does "
        "not match result from server!' ");
  }

  throw_if_not_ok(wait_for_object_to_propagate(
      put_object_request.GetBucket(), put_object_request.GetKey()));
}

Status S3::write_multipart(
    const URI& uri, const void* buffer, uint64_t length, bool last_part) {
  RETURN_NOT_OK(init_client());

  // Ensure that each thread is responsible for exactly multipart_part_size_
  // bytes (except if this is the last write_multipart, in which case the final
  // thread should write less), and cap the number of parallel operations at the
  // configured max number. Length must be evenly divisible by
  // multipart_part_size_ unless this is the last part.
  uint64_t num_ops =
      last_part ? utils::math::ceil(length, s3_params_.multipart_part_size_) :
                  (length / s3_params_.multipart_part_size_);
  num_ops =
      std::min(std::max(num_ops, uint64_t(1)), s3_params_.max_parallel_ops_);

  if (!last_part && length % s3_params_.multipart_part_size_ != 0) {
    return LOG_STATUS(
        Status_S3Error("Length not evenly divisible by part length"));
  }

  const Aws::Http::URI aws_uri(uri.c_str());
  const std::string uri_path(aws_uri.GetPath().c_str());

  MultiPartUploadState* state;
  std::unique_lock<std::mutex> state_lck;

  // Take a lock protecting the shared multipart data structures
  // Read lock to see if it exists
  UniqueReadLock unique_rl(&multipart_upload_rwlock_);

  auto state_iter = multipart_upload_states_.find(uri_path);
  if (state_iter == multipart_upload_states_.end()) {
    // If the state is new, we must grab write lock, so unlock from read and
    // grab write
    unique_rl.unlock();
    UniqueWriteLock unique_wl(&multipart_upload_rwlock_);

    // Since we switched locks we need to once again check to make sure another
    // thread didn't create the state
    state_iter = multipart_upload_states_.find(uri_path);
    if (state_iter == multipart_upload_states_.end()) {
      auto path = aws_uri.GetPath();
      std::string path_str = path.c_str();
      MultiPartUploadState new_state;

      assert(multipart_upload_states_.count(path_str) == 0);
      multipart_upload_states_.emplace(
          std::move(path_str), std::move(new_state));
      state = &multipart_upload_states_.at(uri_path);

      // Expected in initiate_multipart_request
      state_lck = std::unique_lock<std::mutex>(state->mtx);

      unique_wl.unlock();

      // Delete file if it exists (overwrite) and initiate multipart request
      bool exists;
      RETURN_NOT_OK(is_object(uri, &exists));
      if (exists) {
        RETURN_NOT_OK(remove_object(uri));
      }

      const Status st = initiate_multipart_request(aws_uri, state);
      if (!st.ok()) {
        return st;
      }
    } else {
      // If another thread switched state, switch back to a read lock
      state = &multipart_upload_states_.at(uri_path);

      // Lock multipart state
      state_lck = std::unique_lock<std::mutex>(state->mtx);
    }
  } else {
    state = &multipart_upload_states_.at(uri_path);

    // Lock multipart state
    state_lck = std::unique_lock<std::mutex>(state->mtx);

    // Unlock, as make_upload_part_req will reaquire as necessary.
    unique_rl.unlock();
  }

  // Get the upload ID
  const auto upload_id = state->upload_id;

  // Assign the part number(s), and make the write request.
  if (num_ops == 1) {
    const int part_num = state->part_number++;
    state_lck.unlock();

    auto ctx =
        make_upload_part_req(aws_uri, buffer, length, upload_id, part_num);
    return get_make_upload_part_req(uri, uri_path, ctx);
  } else {
    std::vector<MakeUploadPartCtx> ctx_vec;
    ctx_vec.resize(num_ops);
    const uint64_t bytes_per_op = s3_params_.multipart_part_size_;
    const int part_num_base = state->part_number;
    auto status = parallel_for(vfs_thread_pool_, 0, num_ops, [&](uint64_t i) {
      uint64_t begin = i * bytes_per_op,
               end = std::min((i + 1) * bytes_per_op - 1, length - 1);
      uint64_t thread_nbytes = end - begin + 1;
      auto thread_buffer = static_cast<const char*>(buffer) + begin;
      int part_num = static_cast<int>(part_num_base + i);
      ctx_vec[i] = make_upload_part_req(
          aws_uri, thread_buffer, thread_nbytes, upload_id, part_num);

      return Status::Ok();
    });
    state->part_number += num_ops;
    state_lck.unlock();

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
      LOG_STATUS_NO_RETURN_VALUE(Status_S3Error(errmsg.str()));
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
  auto stream = shared_ptr<Aws::IOStream>(
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
  if (request_payer_ != Aws::S3::Model::RequestPayer::NOT_SET)
    upload_part_request.SetRequestPayer(request_payer_);

  auto upload_part_outcome = client_->UploadPart(upload_part_request);

  MakeUploadPartCtx ctx(std::move(upload_part_outcome), upload_part_num);
  return ctx;
}

Status S3::get_make_upload_part_req(
    const URI& uri, const std::string& uri_path, MakeUploadPartCtx& ctx) {
  RETURN_NOT_OK(init_client());

  const auto& upload_part_outcome = ctx.upload_part_outcome;
  bool success = upload_part_outcome.IsSuccess();

  static const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
  if (unit_test_cfg.s3_fail_every_nth_upload_request.is_set() &&
      ctx.upload_part_num %
              unit_test_cfg.s3_fail_every_nth_upload_request.get() ==
          0) {
    success = false;
  }

  if (!success) {
    UniqueReadLock unique_rl(&multipart_upload_rwlock_);
    auto state = &multipart_upload_states_.at(uri_path);
    Status st = Status_S3Error(
        std::string("Failed to upload part of S3 object '") + uri.c_str() +
        outcome_error_message(upload_part_outcome));
    // Lock multipart state
    std::unique_lock<std::mutex> state_lck(state->mtx);
    unique_rl.unlock();
    state->st = st;
    return LOG_STATUS(st);
  }

  Aws::S3::Model::CompletedPart completed_part;
  completed_part.SetETag(upload_part_outcome.GetResult().GetETag());
  completed_part.SetPartNumber(ctx.upload_part_num);

  UniqueReadLock unique_rl(&multipart_upload_rwlock_);
  auto state = &multipart_upload_states_.at(uri_path);
  // Lock multipart state
  std::unique_lock<std::mutex> state_lck(state->mtx);
  unique_rl.unlock();
  state->completed_parts.emplace(
      ctx.upload_part_num, std::move(completed_part));
  state_lck.unlock();

  return Status::Ok();
}

Status S3::set_multipart_upload_state(
    const std::string& uri, MultiPartUploadState& state) {
  Aws::Http::URI aws_uri(uri.c_str());
  std::string uri_path(aws_uri.GetPath().c_str());

  state.bucket = aws_uri.GetAuthority();
  state.key = aws_uri.GetPath();

  UniqueWriteLock unique_wl(&multipart_upload_rwlock_);
  multipart_upload_states_[uri_path] = state;

  return Status::Ok();
}

std::optional<S3::MultiPartUploadState> S3::multipart_upload_state(
    const URI& uri) {
  const Aws::Http::URI aws_uri(uri.c_str());
  const std::string uri_path(aws_uri.GetPath().c_str());

  // Lock the multipart states map for reads
  UniqueReadLock unique_rl(&multipart_upload_rwlock_);
  auto state_iter = multipart_upload_states_.find(uri_path);
  if (state_iter == multipart_upload_states_.end()) {
    return nullopt;
  }

  // Delete the multipart state from the internal map to avoid
  // the upload being completed by S3::disconnect during Context
  // destruction when cloud executors die. The multipart request should be
  // completed only during query finalize for remote global order writes.
  std::unique_lock<std::mutex> state_lck(state_iter->second.mtx);
  MultiPartUploadState rv_state = std::move(state_iter->second);
  multipart_upload_states_.erase(state_iter);

  return rv_state;
}

URI S3::generate_chunk_uri(const URI& attribute_uri, uint64_t id) {
  auto attribute_name = attribute_uri.remove_trailing_slash().last_path_part();
  auto fragment_uri = attribute_uri.parent_path();
  auto buffering_dir = fragment_uri.join_path(
      tiledb::sm::constants::s3_multipart_buffering_dirname);
  auto chunk_name = attribute_name + "_" + std::to_string(id);
  return buffering_dir.join_path(chunk_name);
}

URI S3::generate_chunk_uri(
    const URI& attribute_uri, const std::string& chunk_name) {
  auto fragment_uri = attribute_uri.parent_path();
  auto buffering_dir = fragment_uri.join_path(
      tiledb::sm::constants::s3_multipart_buffering_dirname);
  return buffering_dir.join_path(chunk_name);
}

}  // namespace tiledb::sm

#endif
