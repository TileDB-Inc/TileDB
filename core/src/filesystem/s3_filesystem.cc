/**
 * @file   s3_filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file includes implementations of S3 filesystem functions.
 */

#ifdef HAVE_S3
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetBucketLocationRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Utils;
using namespace Aws;

static const char* DIR_SUFFIX = ".dir";
static const int TIMEOUT_MAX = 10;
static const char* ALLOCATION_TAG = "TileDB";
static const char* ENDPOINT = "localhost:9000";
static const int MUlTIPART_MINIMUM_SIZE = 5 * 1024 * 1024;

static std::shared_ptr<S3Client> client = nullptr;
static SDKOptions options;
static std::unordered_map<std::string, String> multipartUploadIDs;
static std::unordered_map<std::string, int> multipartUploadPartNumber;
static std::unordered_map<std::string, CompleteMultipartUploadRequest>
    multipartCompleteMultipartUploadRequest;
static std::unordered_map<std::string, CompletedMultipartUpload>
    multipartCompleteMultipartUpload;

#endif

#include <fstream>
#include <iostream>
#include "s3_filesystem.h"

#include "buffer_cache.h"
#include "constants.h"
#include "logger.h"
#include "utils.h"

static tiledb::BufferCache buffer_cache;

namespace tiledb {

namespace s3 {

#ifdef HAVE_S3

Status connect() {
  InitAPI(options);
  ClientConfiguration config;
  config.region = Aws::Region::US_EAST_1;
  config.scheme = Scheme::HTTP;
  //            config.connectTimeoutMs = 30000;
  //            config.requestTimeoutMs = 30000;
  //            config.readRateLimiter = Limiter;
  //            config.writeRateLimiter = Limiter;
  //            config.executor =
  //            Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG,
  //            4);
  config.endpointOverride = ENDPOINT;

  client = Aws::MakeShared<S3Client>(
      ALLOCATION_TAG,
      Aws::MakeShared<DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG),
      config,
      false /*signPayloads*/,
      false /*useVirtualAddressing*/);
  return Status::Ok();
}

Status disconnect() {
  for (const auto& record : multipartCompleteMultipartUploadRequest) {
    CompletedMultipartUpload completedMultipartUpload =
        multipartCompleteMultipartUpload[record.first];
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        record.second;
    completeMultipartUploadRequest.WithMultipartUpload(
        completedMultipartUpload);
    CompleteMultipartUploadOutcome completeMultipartUploadOutcome =
        client->CompleteMultipartUpload(completeMultipartUploadRequest);
    if (!completeMultipartUploadOutcome.IsSuccess()) {
      return LOG_STATUS(Status::IOError(
          std::string("Failed to disconnect and flush s3 objects. ") +
          std::string("\nException:  ") +
          completeMultipartUploadOutcome.GetError().GetExceptionName().c_str() +
          std::string("\nError message:  ") +
          completeMultipartUploadOutcome.GetError().GetMessage().c_str()));
    }
  }
  Aws::ShutdownAPI(options);
  return Status::Ok();
}

Status flush_file(const URI& uri) {
  buffer_cache.flush_file(uri);

  Aws::Http::URI aws_uri = uri.c_str();
  String path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  CompletedMultipartUpload completedMultipartUpload =
      multipartCompleteMultipartUpload[path_c_str];

  CompleteMultipartUploadRequest completeMultipartUploadRequest =
      multipartCompleteMultipartUploadRequest[path_c_str];
  completeMultipartUploadRequest.WithMultipartUpload(completedMultipartUpload);
  CompleteMultipartUploadOutcome completeMultipartUploadOutcome =
      client->CompleteMultipartUpload(completeMultipartUploadRequest);

  multipartUploadIDs.erase(path_c_str);
  multipartUploadPartNumber.erase(path_c_str);
  multipartCompleteMultipartUploadRequest.erase(path_c_str);
  multipartCompleteMultipartUpload.erase(path_c_str);
  //  Not working when flushing directories
  //  if (!completeMultipartUploadOutcome.IsSuccess()) {
  //    return LOG_STATUS(Status::IOError(
  //        std::string("Failed to flush s3 object ") + uri.c_str() +
  //        std::string("\nException:  ") +
  //        completeMultipartUploadOutcome.GetError().GetExceptionName().c_str()
  //        + std::string("\nError message:  ") +
  //        completeMultipartUploadOutcome.GetError().GetMessage().c_str()));
  //  }

  return Status::Ok();
}

bool bucket_exists(const char* bucket) {
  HeadBucketRequest headBucketRequest;
  headBucketRequest.SetBucket(bucket);
  HeadBucketOutcome headBucketOutcome = client->HeadBucket(headBucketRequest);
  return headBucketOutcome.IsSuccess();
}

Status create_bucket(const char* bucket) {
  CreateBucketRequest createBucketRequest;
  createBucketRequest.SetBucket(bucket);
  CreateBucketOutcome createBucketOutcome =
      client->CreateBucket(createBucketRequest);
  if (!createBucketOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to create s3 bucket ") + bucket +
        std::string("\nException:  ") +
        createBucketOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        createBucketOutcome.GetError().GetMessage().c_str()));
  }
  return Status::Ok();
}

Status emptyBucket(const Aws::String& bucketName) {
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(bucketName);

  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to list s3 objects in bucket ") +
        bucketName.c_str() + std::string("\nException:  ") +
        listObjectsOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        listObjectsOutcome.GetError().GetMessage().c_str()));
  }

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    DeleteObjectRequest deleteObjectRequest;
    deleteObjectRequest.SetBucket(bucketName);
    deleteObjectRequest.SetKey(object.GetKey());
    DeleteObjectOutcome deleteObjectOutcome =
        client->DeleteObject(deleteObjectRequest);
    if (!deleteObjectOutcome.IsSuccess()) {
      return LOG_STATUS(Status::IOError(
          std::string("Failed to delete s3 object ") + object.GetKey().c_str() +
          std::string("\nException:  ") +
          deleteObjectOutcome.GetError().GetExceptionName().c_str() +
          std::string("\nError message:  ") +
          deleteObjectOutcome.GetError().GetMessage().c_str()));
    }
  }
  return Status::Ok();
}

bool waitForObjectToPropagate(const Aws::String& bucketName, const Aws::String& objectKey)
{
  unsigned timeoutCount = 0;
  while (timeoutCount++ < TIMEOUT_MAX)
  {
    HeadObjectRequest headObjectRequest;
    headObjectRequest.SetBucket(bucketName);
    headObjectRequest.SetKey(objectKey);
    HeadObjectOutcome headObjectOutcome = client->HeadObject(headObjectRequest);
    if (headObjectOutcome.IsSuccess())
    {
        return true;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  return false;
}

Status waitForBucketToEmpty(const Aws::String& bucketName) {
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(bucketName);

  unsigned checkForObjectsCount = 0;
  while (checkForObjectsCount++ < TIMEOUT_MAX) {
    ListObjectsOutcome listObjectsOutcome =
        client->ListObjects(listObjectsRequest);

    if (listObjectsOutcome.GetResult().GetContents().size() > 0) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }
  return Status::Ok();
}

Status delete_bucket(const char* bucket) {
  HeadBucketRequest headBucketRequest;
  headBucketRequest.SetBucket(bucket);
  HeadBucketOutcome bucketOutcome = client->HeadBucket(headBucketRequest);

  if (!bucketOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to head s3 bucket ") + bucket +
        std::string("\nException:  ") +
        bucketOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        bucketOutcome.GetError().GetMessage().c_str()));
  }
  Status status = emptyBucket(bucket);
  if (!status.ok()) {
    return status;
  }
  waitForBucketToEmpty(bucket);

  DeleteBucketRequest deleteBucketRequest;
  deleteBucketRequest.SetBucket(bucket);
  DeleteBucketOutcome deleteBucketOutcome =
      client->DeleteBucket(deleteBucketRequest);
  if (!deleteBucketOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to delete s3 bucket ") + bucket +
        std::string("\nException:  ") +
        deleteBucketOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        deleteBucketOutcome.GetError().GetMessage().c_str()));
  }
  return Status::Ok();
}

Status create_dir(const URI& uri) {
  std::string directory = uri.to_string();
  if (directory.back() == '/')
    directory.pop_back();
  Aws::Http::URI aws_uri = directory.c_str();
  PutObjectRequest putObjectRequest;
  putObjectRequest.WithKey(aws_uri.GetPath() + DIR_SUFFIX)
      .WithBucket(aws_uri.GetAuthority());
  auto requestStream = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
  //  *requestStream << "Folder";
  putObjectRequest.SetBody(requestStream);
  auto putObjectOutcome = client->PutObject(putObjectRequest);
  if (!putObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Creating s3 directory failed ") + directory.c_str() +
        std::string("\nException:  ") +
        putObjectOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        putObjectOutcome.GetError().GetMessage().c_str()));
  }
  waitForObjectToPropagate(putObjectRequest.GetBucket(), putObjectRequest.GetKey());
  return Status::Ok();
}

bool is_dir(const URI& uri) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath() + DIR_SUFFIX);
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return false;
  if (listObjectsOutcome.GetResult().GetContents().size() == 0)
    return false;
  if (listObjectsOutcome.GetResult().GetContents()[0].GetKey() ==
      aws_uri.GetPath() + DIR_SUFFIX)
    return true;
  return false;
}

Status move_path(const URI& old_uri, const URI& new_uri) {
  if (is_dir(new_uri)) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to move s3 path: ") + old_uri.c_str() +
        std::string(" target path :  ") + new_uri.c_str() +
        std::string("already exists")));
  }
  copy_path(old_uri, new_uri);
  return remove_path(old_uri);
}

bool replace(std::string& str, const std::string& from, const std::string& to) {
  size_t start_pos = str.find(from);
  if (start_pos == std::string::npos)
    return false;
  str.replace(start_pos, from.length(), to);
  return true;
}

Status copy_path(const URI& old_uri, const URI& new_uri) {
  std::string src_dir = old_uri.to_string();
  if (src_dir.back() != '/')
    src_dir.push_back('/');
  std::string dst_dir = new_uri.to_string();
  if (dst_dir.back() != '/')
    dst_dir.push_back('/');
  Aws::Http::URI src_uri = src_dir.c_str();
  Aws::Http::URI dst_uri = dst_dir.c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(src_uri.GetAuthority());
  listObjectsRequest.SetPrefix(src_uri.GetPath());
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(Status::IOError(
        "Error while listing s3 directory " + old_uri.to_string()));

  if (listObjectsOutcome.GetResult().GetContents().size() > 0) {
    // create new directory
    create_dir(new_uri);
  }
  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    // delete objects in directory
    Aws::S3::Model::CopyObjectRequest copyObjectRequest;
    copyObjectRequest.SetBucket(src_uri.GetAuthority());
    copyObjectRequest.WithCopySource(src_uri.GetAuthority() + object.GetKey());
    std::string new_file(object.GetKey().c_str());
    replace(
        new_file,
        std::string(src_uri.GetPath().c_str()),
        std::string(dst_uri.GetPath().c_str()));
    copyObjectRequest.WithKey(new_file.c_str());
    CopyObjectOutcome copyObjectOutcome = client->CopyObject(copyObjectRequest);
    if (!copyObjectOutcome.IsSuccess()) {
      return LOG_STATUS(Status::IOError(
          std::string("Failed to copy s3 object ") + object.GetKey().c_str() +
          " to " + new_file + std::string("\nException:  ") +
          copyObjectOutcome.GetError().GetExceptionName().c_str() +
          std::string("\nError message:  ") +
          copyObjectOutcome.GetError().GetMessage().c_str()));
    }
    waitForObjectToPropagate(copyObjectRequest.GetBucket(), copyObjectRequest.GetKey());
  }
  return Status::Ok();
}

bool is_file(const URI& uri) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return false;
  if (listObjectsOutcome.GetResult().GetContents().size() == 0)
    return false;
  if (listObjectsOutcome.GetResult().GetContents()[0].GetKey() ==
      aws_uri.GetPath())
    return true;
  return false;
}

Status initiate_multipart_request(Aws::Http::URI aws_uri) {
  String path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  CreateMultipartUploadRequest createMultipartUploadRequest;
  createMultipartUploadRequest.SetBucket(aws_uri.GetAuthority());
  createMultipartUploadRequest.SetKey(path);
  createMultipartUploadRequest.SetContentType("application/octet-stream");

  CreateMultipartUploadOutcome createMultipartUploadOutcome =
      client->CreateMultipartUpload(createMultipartUploadRequest);
  if (!createMultipartUploadOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to create multipart request for object ") +
        path_c_str + std::string("\nException:  ") +
        createMultipartUploadOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        createMultipartUploadOutcome.GetError().GetMessage().c_str()));
  }

  multipartUploadIDs[path_c_str] =
      createMultipartUploadOutcome.GetResult().GetUploadId();
  multipartUploadPartNumber[path_c_str] = 0;
  CompleteMultipartUploadRequest completeMultipartUploadRequest;
  completeMultipartUploadRequest.SetBucket(aws_uri.GetAuthority());
  completeMultipartUploadRequest.SetKey(path);
  completeMultipartUploadRequest.SetUploadId(multipartUploadIDs[path_c_str]);

  CompletedMultipartUpload completedMultipartUpload;
  multipartCompleteMultipartUpload[path_c_str] = completedMultipartUpload;
  multipartCompleteMultipartUploadRequest[path_c_str] =
      completeMultipartUploadRequest;

  return Status::Ok();
}

Status create_file(const URI& uri) {
  Aws::Http::URI aws_uri = uri.c_str();
  PutObjectRequest putObjectRequest;
  putObjectRequest.WithKey(aws_uri.GetPath())
      .WithBucket(aws_uri.GetAuthority());

  auto requestStream = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
  putObjectRequest.SetBody(requestStream);

  auto putObjectOutcome = client->PutObject(putObjectRequest);
  if (!putObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("S3 object is already open for write ") + uri.c_str()));
  }
  waitForObjectToPropagate(putObjectRequest.GetBucket(), putObjectRequest.GetKey());
  return Status::Ok();
}

Status remove_file(const URI& uri) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  DeleteObjectRequest deleteObjectRequest;
  deleteObjectRequest.SetBucket(aws_uri.GetAuthority());
  deleteObjectRequest.SetKey(aws_uri.GetPath());
  DeleteObjectOutcome deleteObjectOutcome =
      client->DeleteObject(deleteObjectRequest);
  if (!deleteObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to delete s3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        deleteObjectOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        deleteObjectOutcome.GetError().GetMessage().c_str()));
  }
  waitForObjectToPropagate(deleteObjectRequest.GetBucket(), deleteObjectRequest.GetKey());
  return Status::Ok();
}

Status remove_path(const URI& uri) {
  std::string directory = uri.to_string();
  if (directory.back() != '/')
    directory.push_back('/');
  Aws::Http::URI aws_uri = directory.c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::IOError("Error while listing s3 directory " + uri.to_string()));

  if (listObjectsOutcome.GetResult().GetContents().size() > 0) {
    // delete directory object
    directory.pop_back();
    Aws::Http::URI dir_uri = (directory + std::string(DIR_SUFFIX)).c_str();
    DeleteObjectRequest deleteObjectRequest;
    deleteObjectRequest.SetBucket(dir_uri.GetAuthority());
    deleteObjectRequest.SetKey(dir_uri.GetPath());
    DeleteObjectOutcome deleteObjectOutcome =
        client->DeleteObject(deleteObjectRequest);
    if (!deleteObjectOutcome.IsSuccess()) {
      return LOG_STATUS(Status::IOError(
          std::string("Failed to delete s3 object ") +
          dir_uri.GetPath().c_str() + std::string("\nException:  ") +
          deleteObjectOutcome.GetError().GetExceptionName().c_str() +
          std::string("\nError message:  ") +
          deleteObjectOutcome.GetError().GetMessage().c_str()));
    }
  }
  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    // delete objects in directory
    DeleteObjectRequest deleteObjectRequest;
    deleteObjectRequest.SetBucket(aws_uri.GetAuthority());
    deleteObjectRequest.SetKey(object.GetKey());
    DeleteObjectOutcome deleteObjectOutcome =
        client->DeleteObject(deleteObjectRequest);
    if (!deleteObjectOutcome.IsSuccess()) {
      return LOG_STATUS(Status::IOError(
          std::string("Failed to delete s3 object ") + object.GetKey().c_str() +
          std::string("\nException:  ") +
          deleteObjectOutcome.GetError().GetExceptionName().c_str() +
          std::string("\nError message:  ") +
          deleteObjectOutcome.GetError().GetMessage().c_str()));
    }
  }
  return Status::Ok();
}

Status read_from_file(
    const URI& uri, off_t offset, void* buffer, uint64_t length) {
  Aws::Http::URI aws_uri = uri.c_str();
  GetObjectRequest getObjectRequest;
  getObjectRequest.WithBucket(aws_uri.GetAuthority())
      .WithKey(aws_uri.GetPath());
  getObjectRequest.SetRange(("bytes=" + std::to_string(offset) + "-" +
                             std::to_string(offset + length - 1))
                                .c_str());
  getObjectRequest.SetResponseStreamFactory([buffer, length]() {
    std::unique_ptr<Aws::StringStream> stream(
        Aws::New<Aws::StringStream>(ALLOCATION_TAG));
    stream->rdbuf()->pubsetbuf(static_cast<char*>(buffer), length);
    return stream.release();
  });
  auto getObjectOutcome = client->GetObject(getObjectRequest);
  if (!getObjectOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to read s3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        getObjectOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        getObjectOutcome.GetError().GetMessage().c_str()));
  }
  if(getObjectOutcome.GetResult().GetContentLength()!=length){
    std::cout<< "Requested: " << length << " Received: " << getObjectOutcome.GetResult().GetContentLength()<<std::endl;
    return LOG_STATUS(Status::IOError(std::string("Read returned different size of bytes.")));
  }
  return Status::Ok();
}

Status write_to_file(
    const URI& uri, const void* buffer, const uint64_t length) {
  buffer_cache.write_to_file(uri, buffer, length);
  return Status::Ok();
}

Status write_to_file_no_cache(
    const URI& uri, const void* buffer, const uint64_t length) {
  // length should be larger than 5MB
  Aws::Http::URI aws_uri = uri.c_str();
  String path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  if (multipartUploadIDs.find(path_c_str) == multipartUploadIDs.end()) {
    // If file not open initiate a multipart upload
    Status st = initiate_multipart_request(aws_uri);
    if (!st.ok()) {
      return st;
    }
  }
  // upload a part of the file
  multipartUploadPartNumber[path_c_str]++;
  std::shared_ptr<Aws::StringStream> stream =
      Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
  stream->rdbuf()->pubsetbuf(
      static_cast<char*>(const_cast<void*>(buffer)), length);
  stream->rdbuf()->pubseekpos(length);
  stream->seekg(0);
  UploadPartRequest uploadPartRequest;
  uploadPartRequest.SetBucket(aws_uri.GetAuthority());
  uploadPartRequest.SetKey(path);
  uploadPartRequest.SetPartNumber(multipartUploadPartNumber[path_c_str]);
  uploadPartRequest.SetUploadId(multipartUploadIDs[path_c_str]);
  uploadPartRequest.SetBody(stream);
  uploadPartRequest.SetContentMD5(
      HashingUtils::Base64Encode(HashingUtils::CalculateMD5(*stream)));
  uploadPartRequest.SetContentLength(length);

  UploadPartOutcomeCallable uploadPartOutcomeCallable =
      client->UploadPartCallable(uploadPartRequest);
  UploadPartOutcome uploadPartOutcome = uploadPartOutcomeCallable.get();
  if (!uploadPartOutcome.IsSuccess()) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to upload part of s3 object ") + uri.c_str() +
        std::string("\nException:  ") +
        uploadPartOutcome.GetError().GetExceptionName().c_str() +
        std::string("\nError message:  ") +
        uploadPartOutcome.GetError().GetMessage().c_str()));
  }
  CompletedPart completedPart;
  completedPart.SetETag(uploadPartOutcome.GetResult().GetETag());
  completedPart.SetPartNumber(multipartUploadPartNumber[path_c_str]);
  multipartCompleteMultipartUpload[path_c_str].AddParts(completedPart);

  waitForObjectToPropagate(uploadPartRequest.GetBucket(), uploadPartRequest.GetKey());
  return Status::Ok();
}

Status ls(const URI& uri, std::vector<std::string>* paths) {
  Aws::Http::URI aws_uri = (uri.to_path() + std::string("/")).c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  listObjectsRequest.WithDelimiter("/");
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::IOError("Error while listing directory " + uri.to_string()));

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    std::string file(object.GetKey().c_str());
    replace(file, std::string(DIR_SUFFIX), std::string());
    paths->push_back(
        "s3://" + std::string(aws_uri.GetAuthority().c_str()) + file);
  }
  return Status::Ok();
}

Status file_size(const URI& uri, uint64_t* nbytes) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::IOError("Error while listing file " + uri.to_string()));
  if (listObjectsOutcome.GetResult().GetContents().size() < 1)
    return LOG_STATUS(
        Status::IOError(std::string("Not a file ") + uri.to_string()));
  *nbytes = static_cast<uint64_t>(
      listObjectsOutcome.GetResult().GetContents()[0].GetSize());
  return Status::Ok();
}

#endif
}
}  // namespace tiledb
