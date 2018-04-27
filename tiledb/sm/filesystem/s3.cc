/**
 * @file   s3.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include <boost/interprocess/streams/bufferstream.hpp>
#include <fstream>
#include <iostream>

#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"

#ifdef _WIN32
#include <Windows.h>
#endif

#include "tiledb/sm/filesystem/s3.h"

namespace tiledb {
namespace sm {

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Ensures that the AWS library is only initialized once per process. */
static std::once_flag aws_lib_initialized;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

S3::S3() {
  client_ = nullptr;
  multipart_part_size_ = 0;
}

S3::~S3() {
  for (auto& buff : file_buffers_)
    delete buff.second;
}

/* ********************************* */
/*                 API               */
/* ********************************* */

Status S3::init(const S3Config& s3_config, ThreadPool* thread_pool) {
  if (thread_pool == nullptr) {
    return LOG_STATUS(
        Status::S3Error("Can't initialize with null thread pool."));
  }

  // Initialize the library once per process.
  std::call_once(aws_lib_initialized, [this]() { Aws::InitAPI(options_); });

  vfs_thread_pool_ = thread_pool;
  multipart_part_size_ = s3_config.multipart_part_size_;
  file_buffer_size_ = multipart_part_size_ * thread_pool->num_threads();

  Aws::Client::ClientConfiguration config;
  if (!s3_config.region_.empty())
    config.region = s3_config.region_.c_str();
  if (!s3_config.endpoint_override_.empty())
    config.endpointOverride = s3_config.endpoint_override_.c_str();

  config.scheme = (s3_config.scheme_ == "http") ? Aws::Http::Scheme::HTTP :
                                                  Aws::Http::Scheme::HTTPS;
  config.connectTimeoutMs = s3_config.connect_timeout_ms_;
  config.requestTimeoutMs = s3_config.request_timeout_ms_;

  config.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(
      constants::s3_allocation_tag,
      s3_config.connect_max_tries_,
      s3_config.connect_scale_factor_);

  // Connect S3 client
  client_ = Aws::MakeShared<Aws::S3::S3Client>(
      constants::s3_allocation_tag,
      config,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      s3_config.use_virtual_addressing_);

  return Status::Ok();
}

Status S3::create_bucket(const URI& bucket) const {
  if (!bucket.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + bucket.to_string())));
  }

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::CreateBucketRequest create_bucket_request;
  create_bucket_request.SetBucket(aws_uri.GetAuthority());
  auto create_bucket_outcome = client_->CreateBucket(create_bucket_request);
  if (!create_bucket_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to create S3 bucket ") + bucket.to_string() +
        std::string("\nException:  ") +
        create_bucket_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        create_bucket_outcome.GetError().GetMessage().c_str()));
  }

  if (!wait_for_bucket_to_be_created(bucket)) {
    return LOG_STATUS(Status::S3Error(
        "Failed waiting for bucket " + bucket.to_string() + " to be created."));
  }

  return Status::Ok();
}

Status S3::remove_bucket(const URI& bucket) const {
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
        std::string("\nException:  ") +
        delete_bucket_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        delete_bucket_outcome.GetError().GetMessage().c_str()));
  }
  return Status::Ok();
}

Status S3::disconnect() {
  for (const auto& record : multipart_upload_request_) {
    auto completed_multipart_upload = multipart_upload_[record.first];
    auto complete_multipart_upload_request = record.second;
    complete_multipart_upload_request.WithMultipartUpload(
        completed_multipart_upload);
    auto complete_multipart_upload_outcome =
        client_->CompleteMultipartUpload(complete_multipart_upload_request);
    if (!complete_multipart_upload_outcome.IsSuccess()) {
      return LOG_STATUS(Status::S3Error(
          std::string("Failed to disconnect and flush S3 objects. ") +
          std::string("\nException:  ") +
          complete_multipart_upload_outcome.GetError()
              .GetExceptionName()
              .c_str() +
          std::string("\nError message:  ") +
          complete_multipart_upload_outcome.GetError().GetMessage().c_str()));
    }
  }
  Aws::ShutdownAPI(options_);

  return Status::Ok();
}

Status S3::empty_bucket(const URI& bucket) const {
  auto uri_dir = bucket.add_trailing_slash();
  return remove_dir(uri_dir);
}

Status S3::flush_object(const URI& uri) {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  // Flush and delete file buffer
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));
  RETURN_NOT_OK(flush_file_buffer(uri, buff, true));

  Aws::Http::URI aws_uri = uri.c_str();
  std::string path_c_str = aws_uri.GetPath().c_str();

  // Take a lock protecting the shared multipart data structures
  std::unique_lock<std::mutex> multipart_lck(multipart_upload_mtx_);

  // Do nothing - empty object
  auto multipart_upload_it = multipart_upload_.find(path_c_str);
  if (multipart_upload_it == multipart_upload_.end())
    return Status::Ok();

  // Get the completed upload object
  auto completed_multipart_upload = multipart_upload_it->second;

  // Add all the completed parts (sorted by part number) to the upload object.
  auto completed_parts_it = multipart_upload_completed_parts_.find(path_c_str);
  if (completed_parts_it == multipart_upload_completed_parts_.end()) {
    return LOG_STATUS(Status::S3Error(
        "Unable to find completed parts list for S3 object " +
        uri.to_string()));
  }
  for (auto& tup : completed_parts_it->second) {
    Aws::S3::Model::CompletedPart& part = std::get<1>(tup);
    completed_multipart_upload.AddParts(part);
  }

  auto multipart_upload_request_it = multipart_upload_request_.find(path_c_str);
  auto complete_multipart_upload_request = multipart_upload_request_it->second;
  complete_multipart_upload_request.WithMultipartUpload(
      completed_multipart_upload);
  auto complete_multipart_upload_outcome =
      client_->CompleteMultipartUpload(complete_multipart_upload_request);

  // Release lock while we wait for the file to flush.
  multipart_lck.unlock();

  wait_for_object_to_propagate(
      complete_multipart_upload_request.GetBucket(),
      complete_multipart_upload_request.GetKey());

  multipart_lck.lock();

  multipart_upload_IDs_.erase(path_c_str);
  multipart_upload_part_number_.erase(path_c_str);
  multipart_upload_request_.erase(multipart_upload_request_it);
  multipart_upload_.erase(multipart_upload_it);
  multipart_upload_completed_parts_.erase(path_c_str);
  file_buffers_.erase(uri.to_string());
  delete buff;

  //  fails when flushing directories or removed files
  if (!complete_multipart_upload_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to flush S3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        complete_multipart_upload_outcome.GetError()
            .GetExceptionName()
            .c_str() +
        std::string("\nError message:  ") +
        complete_multipart_upload_outcome.GetError().GetMessage().c_str()));
  }

  return Status::Ok();
}

Status S3::is_empty_bucket(const URI& bucket, bool* is_empty) const {
  if (!is_bucket(bucket))
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
        std::string("\nException:  ") +
        list_objects_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        list_objects_outcome.GetError().GetMessage().c_str()));
  }

  *is_empty = list_objects_outcome.GetResult().GetContents().empty() &&
              list_objects_outcome.GetResult().GetCommonPrefixes().empty();

  return Status::Ok();
}

bool S3::is_bucket(const URI& bucket) const {
  if (!bucket.is_s3()) {
    return false;
  }

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::HeadBucketRequest head_bucket_request;
  head_bucket_request.SetBucket(aws_uri.GetAuthority());
  auto head_bucket_outcome = client_->HeadBucket(head_bucket_request);
  return head_bucket_outcome.IsSuccess();
}

bool S3::is_object(const URI& uri) const {
  if (!uri.is_s3())
    return false;

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.SetBucket(aws_uri.GetAuthority());
  head_object_request.SetKey(aws_uri.GetPath());
  auto head_object_outcome = client_->HeadObject(head_object_request);
  return head_object_outcome.IsSuccess();
}

Status S3::is_dir(const URI& uri, bool* exists) const {
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
  auto prefix_str = prefix.to_string();
  if (!prefix.is_s3()) {
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
  auto list_objects_outcome = client_->ListObjects(list_objects_request);

  if (!list_objects_outcome.IsSuccess())
    return LOG_STATUS(Status::S3Error(
        std::string("Error while listing with prefix '") + prefix_str +
        "' and delimiter '" + delimiter + "'"));

  for (const auto& object : list_objects_outcome.GetResult().GetContents()) {
    std::string file(object.GetKey().c_str());
    paths->push_back("s3://" + aws_auth + add_front_slash(file));
  }

  for (const auto& object :
       list_objects_outcome.GetResult().GetCommonPrefixes()) {
    std::string file(object.GetPrefix().c_str());
    paths->push_back("s3://" + aws_auth + add_front_slash(file));
  }

  return Status::Ok();
}

Status S3::move_object(const URI& old_uri, const URI& new_uri) {
  RETURN_NOT_OK(copy_object(old_uri, new_uri));
  RETURN_NOT_OK(remove_object(old_uri));
  return Status::Ok();
}

Status S3::move_dir(const URI& old_uri, const URI& new_uri) {
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
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  std::string aws_path = remove_front_slash(aws_uri.GetPath().c_str());
  Aws::S3::Model::ListObjectsRequest list_objects_request;
  list_objects_request.SetBucket(aws_uri.GetAuthority());
  list_objects_request.SetPrefix(aws_path.c_str());
  auto list_objects_outcome = client_->ListObjects(list_objects_request);

  if (!list_objects_outcome.IsSuccess())
    return LOG_STATUS(Status::S3Error(
        "Cannot retrieve S3 object size; Error while listing file " +
        uri.to_string()));
  if (list_objects_outcome.GetResult().GetContents().empty())
    return LOG_STATUS(Status::S3Error(
        std::string("Cannot retrieve S3 object size; Not a file ") +
        uri.to_string()));
  *nbytes = static_cast<uint64_t>(
      list_objects_outcome.GetResult().GetContents()[0].GetSize());

  return Status::Ok();
}

Status S3::read(
    const URI& uri, off_t offset, void* buffer, uint64_t length) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::GetObjectRequest get_object_request;
  get_object_request.WithBucket(aws_uri.GetAuthority())
      .WithKey(aws_uri.GetPath());
  get_object_request.SetRange(("bytes=" + std::to_string(offset) + "-" +
                               std::to_string(offset + length - 1))
                                  .c_str());
  get_object_request.SetResponseStreamFactory([buffer, length]() {
    auto streamBuf = new boost::interprocess::bufferbuf((char*)buffer, length);
    return Aws::New<Aws::IOStream>(constants::s3_allocation_tag, streamBuf);
  });

  auto get_object_outcome = client_->GetObject(get_object_request);
  if (!get_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to read S3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        get_object_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        get_object_outcome.GetError().GetMessage().c_str()));
  }
  if ((uint64_t)get_object_outcome.GetResult().GetContentLength() != length) {
    return LOG_STATUS(Status::S3Error(
        std::string("Read operation returned different size of bytes.")));
  }

  return Status::Ok();
}

Status S3::remove_object(const URI& uri) const {
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
        std::string("'\nException:  ") +
        delete_object_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        delete_object_outcome.GetError().GetMessage().c_str()));
  }

  wait_for_object_to_be_deleted(
      delete_object_request.GetBucket(), delete_object_request.GetKey());
  return Status::Ok();
}

Status S3::remove_dir(const URI& uri) const {
  std::vector<std::string> paths;
  auto uri_dir = uri.add_trailing_slash();
  RETURN_NOT_OK(ls(uri_dir, &paths, ""));
  for (const auto& p : paths)
    RETURN_NOT_OK(remove_object(URI(p)));
  return Status::Ok();
}

Status S3::touch(const URI& uri) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(std::string(
        "Cannot create file; URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::PutObjectRequest put_object_request;
  put_object_request.WithKey(aws_uri.GetPath())
      .WithBucket(aws_uri.GetAuthority());

  auto request_stream =
      Aws::MakeShared<Aws::StringStream>(constants::s3_allocation_tag);
  put_object_request.SetBody(request_stream);

  auto put_object_outcome = client_->PutObject(put_object_request);
  if (!put_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Cannot touch object '") + uri.c_str() +
        std::string("'\nException:  ") +
        put_object_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        put_object_outcome.GetError().GetMessage().c_str()));
  }

  wait_for_object_to_propagate(
      put_object_request.GetBucket(), put_object_request.GetKey());

  return Status::Ok();
}

Status S3::write(const URI& uri, const void* buffer, uint64_t length) {
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

  // Flush file buffer
  if (buff->size() == file_buffer_size_)
    RETURN_NOT_OK(flush_file_buffer(uri, buff, is_last_part));

  // Write chunks
  uint64_t new_length = length - nbytes_filled;
  uint64_t offset = nbytes_filled;
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

  return Status::Ok();
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

Status S3::copy_object(const URI& old_uri, const URI& new_uri) {
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
        new_uri.c_str() + std::string("\nException:  ") +
        copy_object_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        copy_object_outcome.GetError().GetMessage().c_str()));
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
  STATS_FUNC_IN(vfs_s3_fill_file_buffer);

  *nbytes_filled = std::min(file_buffer_size_ - buff->size(), length);
  if (*nbytes_filled > 0)
    RETURN_NOT_OK(buff->write(buffer, *nbytes_filled));

  return Status::Ok();

  STATS_FUNC_OUT(vfs_s3_fill_file_buffer);
}

std::string S3::add_front_slash(const std::string& path) const {
  return (path.front() != '/') ? (std::string("/") + path) : path;
}

std::string S3::remove_front_slash(const std::string& path) const {
  if (path.front() == '/')
    return path.substr(1, path.length());
  return path;
}

Status S3::flush_file_buffer(const URI& uri, Buffer* buff, bool last_part) {
  if (buff->size() > 0) {
    RETURN_NOT_OK(write_multipart(uri, buff->data(), buff->size(), last_part));
    buff->reset_size();
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
        path_c_str + std::string("'\nException:  ") +
        multipart_upload_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        multipart_upload_outcome.GetError().GetMessage().c_str()));
  }

  multipart_upload_IDs_[path_c_str] =
      multipart_upload_outcome.GetResult().GetUploadId();
  multipart_upload_part_number_[path_c_str] = 1;

  Aws::S3::Model::CompleteMultipartUploadRequest
      complete_multipart_upload_request;
  complete_multipart_upload_request.SetBucket(aws_uri.GetAuthority());
  complete_multipart_upload_request.SetKey(path);
  complete_multipart_upload_request.SetUploadId(
      multipart_upload_IDs_[path_c_str]);

  Aws::S3::Model::CompletedMultipartUpload completed_multipart_upload;
  multipart_upload_[path_c_str] = completed_multipart_upload;
  multipart_upload_request_[path_c_str] = complete_multipart_upload_request;
  multipart_upload_completed_parts_[path_c_str] =
      std::map<int, Aws::S3::Model::CompletedPart>();

  return Status::Ok();
}

std::string S3::join_authority_and_path(
    const std::string& authority, const std::string& path) const {
  bool path_has_slash = !path.empty() && path.front() == '/';
  bool authority_has_slash = !authority.empty() && authority.back() == '/';
  bool need_slash = !(path_has_slash || authority_has_slash);
  return authority + (need_slash ? "/" : "") + path;
}

bool S3::wait_for_object_to_propagate(
    const Aws::String& bucketName, const Aws::String& objectKey) const {
  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    Aws::S3::Model::HeadObjectRequest head_object_request;
    head_object_request.SetBucket(bucketName);
    head_object_request.SetKey(objectKey);
    auto headObjectOutcome = client_->HeadObject(head_object_request);
    if (headObjectOutcome.IsSuccess())
      return true;

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return false;
}

bool S3::wait_for_object_to_be_deleted(
    const Aws::String& bucketName, const Aws::String& objectKey) const {
  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    Aws::S3::Model::HeadObjectRequest head_object_request;
    head_object_request.SetBucket(bucketName);
    head_object_request.SetKey(objectKey);
    auto head_object_outcome = client_->HeadObject(head_object_request);
    if (!head_object_outcome.IsSuccess())
      return true;

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  return false;
}

bool S3::wait_for_bucket_to_be_created(const URI& bucket_uri) const {
  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    if (is_bucket(bucket_uri)) {
      return true;
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }
  return false;
}

Status S3::write_multipart(
    const URI& uri, const void* buffer, uint64_t length, bool last_part) {
  STATS_FUNC_IN(vfs_s3_write_multipart);

  // Ensure that each thread is responsible for exactly multipart_part_size_
  // bytes (except if this is the last write_multipart, in which case the final
  // thread should write less), and cap the number of parallel operations at the
  // thread pool size.
  uint64_t num_ops = last_part ? utils::ceil(length, multipart_part_size_) :
                                 (length / multipart_part_size_);
  num_ops =
      std::min(std::max(num_ops, uint64_t(1)), vfs_thread_pool_->num_threads());

  if (!last_part && length % multipart_part_size_ != 0) {
    return LOG_STATUS(
        Status::S3Error("Length not evenly divisible by part length"));
  }

  Aws::Http::URI aws_uri = uri.c_str();
  auto& path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();

  // Take a lock protecting the shared multipart data structures
  std::unique_lock<std::mutex> multipart_lck(multipart_upload_mtx_);

  if (multipart_upload_IDs_.find(path_c_str) == multipart_upload_IDs_.end()) {
    // Delete file if it exists (overwrite) and initiate multipart request
    if (is_object(uri))
      RETURN_NOT_OK(remove_object(uri));
    Status st = initiate_multipart_request(aws_uri);
    if (!st.ok()) {
      return st;
    }
  }

  // Get the upload ID
  auto upload_id = multipart_upload_IDs_[path_c_str];

  // Assign the part number(s), and make the write request.
  if (num_ops == 1) {
    int part_num = multipart_upload_part_number_[path_c_str]++;

    // Unlock, as make_upload_part_req will reaquire as necessary.
    multipart_lck.unlock();

    return make_upload_part_req(uri, buffer, length, upload_id, part_num);
  } else {
    STATS_COUNTER_ADD(vfs_s3_write_num_parallelized, 1);

    std::vector<std::future<Status>> results;
    uint64_t bytes_per_op = multipart_part_size_;
    int part_num_base = multipart_upload_part_number_[path_c_str];
    for (uint64_t i = 0; i < num_ops; i++) {
      uint64_t begin = i * bytes_per_op,
               end = std::min((i + 1) * bytes_per_op - 1, length - 1);
      uint64_t thread_nbytes = end - begin + 1;
      auto thread_buffer = reinterpret_cast<const char*>(buffer) + begin;
      int part_num = static_cast<int>(part_num_base + i);
      results.push_back(vfs_thread_pool_->enqueue(
          [this, &uri, thread_buffer, thread_nbytes, &upload_id, part_num]() {
            return make_upload_part_req(
                uri, thread_buffer, thread_nbytes, upload_id, part_num);
          }));
    }
    multipart_upload_part_number_[path_c_str] += num_ops;

    // Unlock, so the threads can take the lock as necessary.
    multipart_lck.unlock();

    bool all_ok = vfs_thread_pool_->wait_all(results);
    return all_ok ?
               Status::Ok() :
               LOG_STATUS(Status::S3Error("S3 parallel write_multipart error"));
  }

  STATS_FUNC_OUT(vfs_s3_write_multipart);
}

Status S3::make_upload_part_req(
    const URI& uri,
    const void* buffer,
    uint64_t length,
    const Aws::String& upload_id,
    int upload_part_num) {
  Aws::Http::URI aws_uri = uri.c_str();
  auto& path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();

  auto stream = std::shared_ptr<Aws::IOStream>(
      new boost::interprocess::bufferstream((char*)buffer, length));

  Aws::S3::Model::UploadPartRequest upload_part_request;
  upload_part_request.SetBucket(aws_uri.GetAuthority());
  upload_part_request.SetKey(path);
  upload_part_request.SetPartNumber(upload_part_num);
  upload_part_request.SetUploadId(upload_id);
  upload_part_request.SetBody(stream);
  upload_part_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(
      Aws::Utils::HashingUtils::CalculateMD5(*stream)));
  upload_part_request.SetContentLength(length);

  auto upload_part_outcome_callable =
      client_->UploadPartCallable(upload_part_request);
  auto upload_part_outcome = upload_part_outcome_callable.get();
  if (!upload_part_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to upload part of S3 object '") + uri.c_str() +
        std::string("'\nException:  ") +
        upload_part_outcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        upload_part_outcome.GetError().GetMessage().c_str()));
  }

  Aws::S3::Model::CompletedPart completed_part;
  completed_part.SetETag(upload_part_outcome.GetResult().GetETag());
  completed_part.SetPartNumber(upload_part_num);

  {
    std::unique_lock<std::mutex> lck(multipart_upload_mtx_);
    multipart_upload_completed_parts_[path_c_str][upload_part_num] =
        completed_part;
  }

  STATS_COUNTER_ADD(vfs_s3_num_parts_written, 1);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif
