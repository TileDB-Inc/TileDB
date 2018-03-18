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

#include <fstream>
#include <iostream>

#include "boost/bufferstream.h"
#include "tiledb/sm/misc/logger.h"

#ifdef _WIN32
#include <Windows.h>
#endif

#include "tiledb/sm/filesystem/s3.h"

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

S3::S3() {
  client_ = nullptr;
  file_buffer_size_ = 0;
}

S3::~S3() {
  for (auto& buff : file_buffers_)
    delete buff.second;
}

/* ********************************* */
/*                 API               */
/* ********************************* */

Status S3::connect(const S3Config& s3_config) {
  Aws::InitAPI(options_);
  file_buffer_size_ = s3_config.file_buffer_size_;

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
  Aws::S3::Model::CreateBucketRequest createBucketRequest;
  createBucketRequest.SetBucket(aws_uri.GetAuthority());
  auto createBucketOutcome = client_->CreateBucket(createBucketRequest);
  if (!createBucketOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to create s3 bucket ") + bucket.to_string() +
        std::string("\nException:  ") +
        createBucketOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        createBucketOutcome.GetError().GetMessage().c_str()));
  }
  return Status::Ok();
}

Status S3::create_dir(const URI& uri) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  if (is_dir(uri))
    return Status::Ok();

  std::string directory = uri.to_string();
  if (directory.back() != '/')
    directory += "/";
  Aws::Http::URI aws_uri = directory.c_str();
  Aws::S3::Model::PutObjectRequest putObjectRequest;
  putObjectRequest.WithKey(aws_uri.GetPath())
      .WithBucket(aws_uri.GetAuthority());
  auto requestStream =
      Aws::MakeShared<Aws::StringStream>(constants::s3_allocation_tag);
  putObjectRequest.SetBody(requestStream);
  putObjectRequest.SetContentType("application/directory");
  auto putObjectOutcome = client_->PutObject(putObjectRequest);
  if (!putObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Creating s3 directory failed ") + directory.c_str() +
        std::string("\nException:  ") +
        putObjectOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        putObjectOutcome.GetError().GetMessage().c_str()));
  }
  wait_for_object_to_propagate(
      putObjectRequest.GetBucket(), putObjectRequest.GetKey());
  return Status::Ok();
}

Status S3::create_file(const URI& uri) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(std::string(
        "Cannot create file; URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::PutObjectRequest putObjectRequest;
  putObjectRequest.WithKey(aws_uri.GetPath())
      .WithBucket(aws_uri.GetAuthority());

  auto requestStream =
      Aws::MakeShared<Aws::StringStream>(constants::s3_allocation_tag);
  putObjectRequest.SetBody(requestStream);

  auto putObjectOutcome = client_->PutObject(putObjectRequest);
  if (!putObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Cannot create file ") + uri.c_str() +
        std::string("\nException:  ") +
        putObjectOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        putObjectOutcome.GetError().GetMessage().c_str()));
  }
  wait_for_object_to_propagate(
      putObjectRequest.GetBucket(), putObjectRequest.GetKey());
  return Status::Ok();
}

Status S3::delete_bucket(const URI& bucket) const {
  if (!bucket.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + bucket.to_string())));
  }

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::HeadBucketRequest headBucketRequest;
  headBucketRequest.SetBucket(aws_uri.GetAuthority());
  auto bucketOutcome = client_->HeadBucket(headBucketRequest);

  if (!bucketOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to head s3 bucket ") + bucket.to_string() +
        std::string("\nException:  ") +
        bucketOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        bucketOutcome.GetError().GetMessage().c_str()));
  }

  RETURN_NOT_OK(empty_bucket(aws_uri.GetAuthority()));

  Aws::S3::Model::DeleteBucketRequest deleteBucketRequest;
  deleteBucketRequest.SetBucket(aws_uri.GetAuthority());
  auto deleteBucketOutcome = client_->DeleteBucket(deleteBucketRequest);
  if (!deleteBucketOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to delete s3 bucket ") + bucket.to_string() +
        std::string("\nException:  ") +
        deleteBucketOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        deleteBucketOutcome.GetError().GetMessage().c_str()));
  }
  return Status::Ok();
}

Status S3::empty_bucket(const URI& bucket) const {
  if (!is_bucket(bucket))
    return LOG_STATUS(
        Status::S3Error("Cannot empty bucket; Bucket does not exist"));

  Aws::Http::URI aws_uri = bucket.c_str();
  return empty_bucket(aws_uri.GetAuthority());
}

Status S3::is_empty_bucket(const URI& bucket, bool* is_empty) const {
  if (!is_bucket(bucket))
    return LOG_STATUS(Status::S3Error(
        "Cannot check if bucket is empty; Bucket does not exist"));

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix("");
  listObjectsRequest.SetDelimiter("/");
  auto listObjectsOutcome = client_->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to list s3 objects in bucket ") + bucket.c_str() +
        std::string("\nException:  ") +
        listObjectsOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        listObjectsOutcome.GetError().GetMessage().c_str()));
  }

  *is_empty = listObjectsOutcome.GetResult().GetContents().empty() &&
              listObjectsOutcome.GetResult().GetCommonPrefixes().empty();

  return Status::Ok();
}

Status S3::disconnect() {
  for (const auto& record : multipart_upload_request_) {
    auto completedMultipartUpload = multipart_upload_[record.first];
    auto completeMultipartUploadRequest = record.second;
    completeMultipartUploadRequest.WithMultipartUpload(
        completedMultipartUpload);
    auto completeMultipartUploadOutcome =
        client_->CompleteMultipartUpload(completeMultipartUploadRequest);
    if (!completeMultipartUploadOutcome.IsSuccess()) {
      return LOG_STATUS(Status::S3Error(
          std::string("Failed to disconnect and flush s3 objects. ") +
          std::string("\nException:  ") +
          completeMultipartUploadOutcome.GetError().GetExceptionName().c_str() +
          std::string("\nError message:  ") +
          completeMultipartUploadOutcome.GetError().GetMessage().c_str()));
    }
  }
  Aws::ShutdownAPI(options_);

  return Status::Ok();
}

Status S3::file_size(const URI& uri, uint64_t* nbytes) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  Aws::S3::Model::ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(fix_path(aws_uri.GetPath()));
  auto listObjectsOutcome = client_->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::S3Error("Error while listing file " + uri.to_string()));
  if (listObjectsOutcome.GetResult().GetContents().empty())
    return LOG_STATUS(
        Status::S3Error(std::string("Not a file ") + uri.to_string()));
  *nbytes = static_cast<uint64_t>(
      listObjectsOutcome.GetResult().GetContents()[0].GetSize());
  return Status::Ok();
}

Status S3::flush_file(const URI& uri) {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  if (is_dir(uri))
    return Status::Ok();

  // Flush and delete file buffer
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));
  RETURN_NOT_OK(flush_file_buffer(uri, buff));
  delete buff;
  file_buffers_.erase(uri.to_string());

  Aws::Http::URI aws_uri = uri.c_str();
  std::string path_c_str = aws_uri.GetPath().c_str();

  // Do nothing - empty object
  auto multipart_upload_it = multipart_upload_.find(path_c_str);
  if (multipart_upload_it == multipart_upload_.end())
    return Status::Ok();

  auto completedMultipartUpload = multipart_upload_it->second;
  auto multipart_upload_request_it = multipart_upload_request_.find(path_c_str);
  auto completeMultipartUploadRequest = multipart_upload_request_it->second;
  completeMultipartUploadRequest.WithMultipartUpload(completedMultipartUpload);
  auto completeMultipartUploadOutcome =
      client_->CompleteMultipartUpload(completeMultipartUploadRequest);

  wait_for_object_to_propagate(
      completeMultipartUploadRequest.GetBucket(),
      completeMultipartUploadRequest.GetKey());
  multipart_upload_IDs_.erase(path_c_str);
  multipart_upload_part_number_.erase(path_c_str);
  multipart_upload_request_.erase(multipart_upload_request_it);
  multipart_upload_.erase(multipart_upload_it);

  //  fails when flushing directories or removed files
  if (!completeMultipartUploadOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to flush s3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        completeMultipartUploadOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        completeMultipartUploadOutcome.GetError().GetMessage().c_str()));
  }

  return Status::Ok();
}

bool S3::is_bucket(const URI& bucket) const {
  if (!bucket.is_s3()) {
    return false;
  }

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::HeadBucketRequest headBucketRequest;
  headBucketRequest.SetBucket(aws_uri.GetAuthority());
  auto headBucketOutcome = client_->HeadBucket(headBucketRequest);
  return headBucketOutcome.IsSuccess();
}

bool S3::is_dir(const URI& uri) const {
  if (!uri.is_s3())
    return false;

  auto path = uri.to_string();
  if (path.back() != '/')
    path += "/";
  return is_file(URI(path));
}

bool S3::is_file(const URI& uri) const {
  if (!uri.is_s3())
    return false;

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.SetBucket(aws_uri.GetAuthority());
  head_object_request.SetKey(aws_uri.GetPath());
  auto headObjectOutcome = client_->HeadObject(head_object_request);
  return headObjectOutcome.IsSuccess();
}

Status S3::ls(const URI& uri, std::vector<std::string>* paths) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  auto uri_path = uri.to_string();
  Aws::Http::URI aws_uri =
      (uri_path + (uri_path.back() != '/' ? "/" : "")).c_str();
  Aws::S3::Model::ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(fix_path(aws_uri.GetPath()));
  listObjectsRequest.WithDelimiter("/");
  auto listObjectsOutcome = client_->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::S3Error("Error while listing directory " + uri.to_string()));

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    // Get key
    std::string file(object.GetKey().c_str());
    if (file.front() != '/')
      file = std::string("/") + file;

    // Skip the directory itself
    if (file == std::string(aws_uri.GetPath().c_str()))
      continue;

    // Push key in path list
    paths->push_back(
        "s3://" + std::string(aws_uri.GetAuthority().c_str()) + file);
  }

  for (const auto& object :
       listObjectsOutcome.GetResult().GetCommonPrefixes()) {
    std::string file(object.GetPrefix().c_str());
    if (file.front() != '/')
      file = std::string("/") + file;
    paths->push_back(
        "s3://" + std::string(aws_uri.GetAuthority().c_str()) + file);
  }

  return Status::Ok();
}

Status S3::move_path(const URI& old_uri, const URI& new_uri) {
  // Sanity checks
  if (!old_uri.is_s3())
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + old_uri.to_string())));
  if (!new_uri.is_s3())
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + new_uri.to_string())));
  if (is_dir(new_uri) || is_file(new_uri))
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to move s3 path: ") + old_uri.c_str() +
        std::string(" target path :  ") + new_uri.c_str() +
        std::string("already exists")));

  if (is_dir(old_uri)) {
    RETURN_NOT_OK(create_dir(new_uri));
    RETURN_NOT_OK(copy_dir(old_uri, new_uri));
    return remove_path(old_uri);
  } else if (is_file(old_uri)) {
    RETURN_NOT_OK(copy_file(old_uri, new_uri));
    return remove_file(old_uri);
  } else {
    return LOG_STATUS(
        Status::S3Error("Failed to move path; Path does not exist"));
  }
}

Status S3::read(
    const URI& uri, off_t offset, void* buffer, uint64_t length) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::GetObjectRequest getObjectRequest;
  getObjectRequest.WithBucket(aws_uri.GetAuthority())
      .WithKey(aws_uri.GetPath());
  getObjectRequest.SetRange(("bytes=" + std::to_string(offset) + "-" +
                             std::to_string(offset + length - 1))
                                .c_str());
  getObjectRequest.SetResponseStreamFactory([buffer, length]() {
    auto streamBuf = new boost::interprocess::bufferbuf((char*)buffer, length);
    return Aws::New<Aws::IOStream>(constants::s3_allocation_tag, streamBuf);
  });

  auto getObjectOutcome = client_->GetObject(getObjectRequest);
  if (!getObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to read s3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        getObjectOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        getObjectOutcome.GetError().GetMessage().c_str()));
  }
  if ((uint64_t)getObjectOutcome.GetResult().GetContentLength() != length) {
    return LOG_STATUS(
        Status::S3Error(std::string("Read returned different size of bytes.")));
  }

  return Status::Ok();
}

Status S3::remove_file(const URI& uri) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  if (!is_file(uri))
    return LOG_STATUS(Status::S3Error(
        std::string("Cannot remove file `") + uri.c_str() +
        "'; File does not exist"));

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  Aws::S3::Model::DeleteObjectRequest deleteObjectRequest;
  deleteObjectRequest.SetBucket(aws_uri.GetAuthority());
  deleteObjectRequest.SetKey(aws_uri.GetPath());

  auto deleteObjectOutcome = client_->DeleteObject(deleteObjectRequest);
  if (!deleteObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to delete s3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        deleteObjectOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        deleteObjectOutcome.GetError().GetMessage().c_str()));
  }

  wait_for_object_to_be_deleted(
      deleteObjectRequest.GetBucket(), deleteObjectRequest.GetKey());
  return Status::Ok();
}

Status S3::remove_path(const URI& uri) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  // Check if it is a file
  if (uri.to_string().back() != '/' && is_file(uri))
    return remove_file(uri);

  URI directory = uri.join_path("/");
  // Delete contents of directory
  {
    // List objects
    Aws::Http::URI aws_uri = directory.c_str();
    Aws::S3::Model::ListObjectsRequest listObjectsRequest;
    auto authority = aws_uri.GetAuthority();
    listObjectsRequest.SetBucket(authority);
    listObjectsRequest.SetPrefix(fix_path(aws_uri.GetPath()));
    listObjectsRequest.SetDelimiter("/");
    auto listObjectsOutcome = client_->ListObjects(listObjectsRequest);
    if (!listObjectsOutcome.IsSuccess())
      return LOG_STATUS(Status::S3Error(
          "Error while listing s3 directory " + uri.to_string()));

    // Delete "non-directory" objects with prefix the directory
    for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
      std::string key = object.GetKey().c_str();
      if (key.front() != '/')
        key = std::string("/") + key;
      auto object_uri = std::string("s3://") + authority.c_str() + key;
      RETURN_NOT_OK(remove_file(URI(object_uri)));
    }

    // Recursively delete "directory" objects
    for (const auto& object :
         listObjectsOutcome.GetResult().GetCommonPrefixes()) {
      std::string key = object.GetPrefix().c_str();
      if (key.front() != '/')
        key = std::string("/") + key;
      auto object_uri = std::string("s3://") + authority.c_str() + key;
      RETURN_NOT_OK(remove_path(URI(object_uri)));
    }
  }

  // Delete directory object
  if (is_file(directory))
    RETURN_NOT_OK(remove_file(directory));

  return Status::Ok();
}

Status S3::write(const URI& uri, const void* buffer, uint64_t length) {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  // Get file buffer
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));

  // Fill file buffer
  uint64_t nbytes_filled;
  RETURN_NOT_OK(fill_file_buffer(buff, buffer, length, &nbytes_filled));

  // Flush file buffer
  if (buff->size() == file_buffer_size_)
    RETURN_NOT_OK(flush_file_buffer(uri, buff));

  // Write chunks
  uint64_t new_length = length - nbytes_filled;
  uint64_t offset = nbytes_filled;
  while (new_length > 0) {
    if (new_length >= file_buffer_size_) {
      RETURN_NOT_OK(
          write_multipart(uri, (char*)buffer + offset, file_buffer_size_));
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

Status S3::copy_dir(const URI& old_uri, const URI& new_uri) {
  std::string src_dir = old_uri.to_string();
  if (src_dir.back() != '/')
    src_dir.push_back('/');
  std::string dst_dir = new_uri.to_string();
  if (dst_dir.back() != '/')
    dst_dir.push_back('/');

  // Create new directory from old directory
  RETURN_NOT_OK(create_dir(new_uri));

  Aws::Http::URI src_uri = src_dir.c_str();
  Aws::Http::URI dst_uri = dst_dir.c_str();
  Aws::S3::Model::ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(src_uri.GetAuthority());
  listObjectsRequest.SetPrefix(fix_path(src_uri.GetPath()));
  listObjectsRequest.WithDelimiter("/");
  auto listObjectsOutcome = client_->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(Status::S3Error(
        "Error while listing s3 directory " + old_uri.to_string()));

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    // Get key
    std::string key = object.GetKey().c_str();
    if (key.front() != '/')
      key = std::string("/") + key;

    // Skip the directory itself
    if (key == std::string(src_uri.GetPath().c_str()))
      continue;

    // Make full source file path
    auto src_file =
        std::string("s3://") +
        join_authority_and_path(src_uri.GetAuthority().c_str(), key).c_str();

    // Create new destination file name
    std::string new_key =
        ((key.front() != '/') ? std::string("/") : std::string("")) + key;
    replace(
        new_key,
        std::string(src_uri.GetPath().c_str()),
        std::string(dst_uri.GetPath().c_str()));

    // Create new file path
    auto dst_file =
        std::string("s3://") +
        join_authority_and_path(dst_uri.GetAuthority().c_str(), new_key)
            .c_str();

    if (is_dir(URI(src_file))) {
      RETURN_NOT_OK(copy_dir(URI(src_file), URI(dst_file)));
    } else {
      RETURN_NOT_OK(copy_file(URI(src_file), URI(dst_file)));
    }
  }

  for (const auto& object :
       listObjectsOutcome.GetResult().GetCommonPrefixes()) {
    std::string key = object.GetPrefix().c_str();
    auto src_file =
        std::string("s3://") +
        join_authority_and_path(src_uri.GetAuthority().c_str(), key).c_str();

    std::string new_key =
        ((key.front() != '/') ? std::string("/") : std::string("")) + key;
    replace(
        new_key,
        std::string(src_uri.GetPath().c_str()),
        std::string(dst_uri.GetPath().c_str()));

    auto dst_file =
        std::string("s3://") +
        join_authority_and_path(dst_uri.GetAuthority().c_str(), new_key)
            .c_str();

    if (is_dir(URI(src_file))) {
      RETURN_NOT_OK(copy_dir(URI(src_file), URI(dst_file)));
    } else {
      RETURN_NOT_OK(copy_file(URI(src_file), URI(dst_file)));
    }
  }

  return Status::Ok();
}

Status S3::copy_file(const URI& old_uri, const URI& new_uri) {
  Aws::Http::URI src_uri = old_uri.c_str();
  Aws::Http::URI dst_uri = new_uri.c_str();
  Aws::S3::Model::CopyObjectRequest copyObjectRequest;
  copyObjectRequest.SetCopySource(
      join_authority_and_path(
          src_uri.GetAuthority().c_str(), src_uri.GetPath().c_str())
          .c_str());
  copyObjectRequest.SetBucket(dst_uri.GetAuthority());
  copyObjectRequest.SetKey(dst_uri.GetPath());

  auto copyObjectOutcome = client_->CopyObject(copyObjectRequest);
  if (!copyObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to copy s3 object ") + old_uri.c_str() + " to " +
        new_uri.c_str() + std::string("\nException:  ") +
        copyObjectOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        copyObjectOutcome.GetError().GetMessage().c_str()));
  }

  wait_for_object_to_propagate(
      copyObjectRequest.GetBucket(), copyObjectRequest.GetKey());

  return Status::Ok();
}

Status S3::empty_bucket(const Aws::String& bucketName) const {
  Aws::S3::Model::ListObjectsRequest listObjectsRequest;
  listObjectsRequest.WithBucket(bucketName);
  listObjectsRequest.SetPrefix("");
  listObjectsRequest.WithDelimiter("/");
  auto listObjectsOutcome = client_->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to list s3 objects in bucket ") +
        bucketName.c_str() + std::string("\nException:  ") +
        listObjectsOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        listObjectsOutcome.GetError().GetMessage().c_str()));
  }

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    std::string key = object.GetKey().c_str();
    if (key.front() != '/')
      key = std::string("/") + key;
    auto object_uri = std::string("s3://") + bucketName.c_str() + key;
    RETURN_NOT_OK(remove_file(URI(object_uri)));
  }

  for (const auto& object :
       listObjectsOutcome.GetResult().GetCommonPrefixes()) {
    std::string key = object.GetPrefix().c_str();
    if (key.front() != '/')
      key = std::string("/") + key;
    auto object_uri = std::string("s3://") + bucketName.c_str() + key;
    RETURN_NOT_OK(remove_path(URI(object_uri)));
  }

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

Aws::String S3::fix_path(const Aws::String& object_key) const {
  if (object_key.front() == '/')
    return object_key.substr(1, object_key.length());
  return object_key;
}

Status S3::flush_file_buffer(const URI& uri, Buffer* buff) {
  if (buff->size() > 0) {
    RETURN_NOT_OK(write_multipart(uri, buff->data(), buff->size()));
    buff->reset_size();
  }

  return Status::Ok();
}

Status S3::get_file_buffer(const URI& uri, Buffer** buff) {
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
  Aws::S3::Model::CreateMultipartUploadRequest createMultipartUploadRequest;
  createMultipartUploadRequest.SetBucket(aws_uri.GetAuthority());
  createMultipartUploadRequest.SetKey(path);
  createMultipartUploadRequest.SetContentType("application/octet-stream");

  auto createMultipartUploadOutcome =
      client_->CreateMultipartUpload(createMultipartUploadRequest);
  if (!createMultipartUploadOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to create multipart request for object ") +
        path_c_str + std::string("\nException:  ") +
        createMultipartUploadOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        createMultipartUploadOutcome.GetError().GetMessage().c_str()));
  }

  multipart_upload_IDs_[path_c_str] =
      createMultipartUploadOutcome.GetResult().GetUploadId();
  multipart_upload_part_number_[path_c_str] = 0;
  Aws::S3::Model::CompleteMultipartUploadRequest completeMultipartUploadRequest;
  completeMultipartUploadRequest.SetBucket(aws_uri.GetAuthority());
  completeMultipartUploadRequest.SetKey(path);
  completeMultipartUploadRequest.SetUploadId(multipart_upload_IDs_[path_c_str]);

  Aws::S3::Model::CompletedMultipartUpload completedMultipartUpload;
  multipart_upload_[path_c_str] = completedMultipartUpload;
  multipart_upload_request_[path_c_str] = completeMultipartUploadRequest;

  return Status::Ok();
}

std::string S3::join_authority_and_path(
    const std::string& authority, const std::string& path) const {
  bool path_has_slash = !path.empty() && path.front() == '/';
  bool authority_has_slash = !authority.empty() && authority.back() == '/';
  bool need_slash = !(path_has_slash || authority_has_slash);
  return authority + (need_slash ? "/" : "") + path;
}

bool S3::replace(
    std::string& str, const std::string& from, const std::string& to) const {
  auto start_pos = str.find(from);
  if (start_pos == std::string::npos)
    return false;
  str.replace(start_pos, from.length(), to);
  return true;
}

bool S3::wait_for_object_to_propagate(
    const Aws::String& bucketName, const Aws::String& objectKey) const {
  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    Aws::S3::Model::HeadObjectRequest headObjectRequest;
    headObjectRequest.SetBucket(bucketName);
    headObjectRequest.SetKey(objectKey);
    auto headObjectOutcome = client_->HeadObject(headObjectRequest);
    if (headObjectOutcome.IsSuccess())
      return true;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return false;
}

bool S3::wait_for_object_to_be_deleted(
    const Aws::String& bucketName, const Aws::String& objectKey) const {
  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    Aws::S3::Model::HeadObjectRequest headObjectRequest;
    headObjectRequest.SetBucket(bucketName);
    headObjectRequest.SetKey(objectKey);
    auto headObjectOutcome = client_->HeadObject(headObjectRequest);
    if (!headObjectOutcome.IsSuccess())
      return true;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return false;
}

Status S3::write_multipart(
    const URI& uri, const void* buffer, uint64_t length) {
  // length should be larger than 5MB
  Aws::Http::URI aws_uri = uri.c_str();
  auto& path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  if (multipart_upload_IDs_.find(path_c_str) == multipart_upload_IDs_.end()) {
    // Delete file if it exists (overwrite) and initiate multipart request
    if (is_file(uri))
      RETURN_NOT_OK(remove_file(uri));
    Status st = initiate_multipart_request(aws_uri);
    if (!st.ok()) {
      return st;
    }
  }

  // upload a part of the file
  multipart_upload_part_number_[path_c_str]++;
  auto stream = std::shared_ptr<Aws::IOStream>(
      new boost::interprocess::bufferstream((char*)buffer, length));

  Aws::S3::Model::UploadPartRequest uploadPartRequest;
  uploadPartRequest.SetBucket(aws_uri.GetAuthority());
  uploadPartRequest.SetKey(path);
  uploadPartRequest.SetPartNumber(multipart_upload_part_number_[path_c_str]);
  uploadPartRequest.SetUploadId(multipart_upload_IDs_[path_c_str]);
  uploadPartRequest.SetBody(stream);
  uploadPartRequest.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(
      Aws::Utils::HashingUtils::CalculateMD5(*stream)));
  uploadPartRequest.SetContentLength(length);

  auto uploadPartOutcomeCallable =
      client_->UploadPartCallable(uploadPartRequest);
  auto uploadPartOutcome = uploadPartOutcomeCallable.get();
  if (!uploadPartOutcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to upload part of s3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        uploadPartOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        uploadPartOutcome.GetError().GetMessage().c_str()));
  }
  Aws::S3::Model::CompletedPart completedPart;
  completedPart.SetETag(uploadPartOutcome.GetResult().GetETag());
  completedPart.SetPartNumber(multipart_upload_part_number_[path_c_str]);
  multipart_upload_[path_c_str].AddParts(completedPart);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif
