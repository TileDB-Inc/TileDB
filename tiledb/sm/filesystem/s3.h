/**
 * @file   s3.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file defines the S3 class.
 */

#ifndef TILEDB_S3_H
#define TILEDB_S3_H

#ifdef HAVE_S3
#include "tiledb/common/common.h"
#include "tiledb/common/rwlock.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/curl/curl_init.h"
#include "tiledb/sm/filesystem/s3_thread_pool_executor.h"
#include "tiledb/sm/filesystem/ssl_config.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/stats/stats.h"
#include "uri.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetBucketLocationRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <sys/types.h>
#include <mutex>
#include <string>
#include <vector>

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace tiledb::sm {

struct S3Parameters {
  S3Parameters() = delete;

  S3Parameters(const Config& config)
      : logging_level_(config.get<std::string>("vfs.s3.logging_level").value())
      , skip_init_(config.get<bool>("vfs.s3.skip_init").value())
      , max_parallel_ops_(
            config.get<uint64_t>("vfs.s3.max_parallel_ops").value())
      , multipart_part_size_(
            config.get<uint64_t>("vfs.s3.multipart_part_size").value())
      , region_(config.get<std::string>("vfs.s3.region").value_or(""))
      , use_virtual_addressing_(
            config.get<bool>("vfs.s3.use_virtual_addressing").value())
      , use_multipart_upload_(
            config.get<bool>("vfs.s3.use_multipart_upload").value())
      , requester_pays_(config.get<bool>("vfs.s3.requester_pays").value())
      , object_acl_str_(
            config.get<std::string>("vfs.s3.object_canned_acl").value())
      , sse_str_(config.get<std::string>("vfs.s3.sse").value())
      , sse_kms_key_id_(
            sse_str_ == "kms" ?
                config.get<std::string>("vfs.s3.sse_kms_key_id").value() :
                "")
      , config_source_(config.get<std::string>("vfs.s3.config_source").value())
      , aws_no_sign_request_(config.get<bool>("vfs.s3.no_sign_request").value())
      , endpoint_override_(
            config.get<std::string>("vfs.s3.endpoint_override").value())
      , proxy_host_(config.get<std::string>("vfs.s3.proxy_host").value())
      , proxy_port_(config.get<uint32_t>("vfs.s3.proxy_port").value())
      , proxy_username_(
            config.get<std::string>("vfs.s3.proxy_username").value())
      , proxy_password_(
            config.get<std::string>("vfs.s3.proxy_password").value())
      , proxy_scheme_(config.get<std::string>("vfs.s3.proxy_scheme").value())
      , scheme_(config.get<std::string>("vfs.s3.scheme").value())
      , connect_timeout_ms_(
            config.get<int64_t>("vfs.s3.connect_timeout_ms").value())
      , request_timeout_ms_(
            config.get<int64_t>("vfs.s3.request_timeout_ms").value())
      , aws_access_key_id_(
            config.get<std::string>("vfs.s3.aws_access_key_id").value())
      , aws_secret_access_key_(
            config.get<std::string>("vfs.s3.aws_secret_access_key").value())
      , aws_session_token_(
            config.get<std::string>("vfs.s3.aws_session_token").value())
      , aws_role_arn_(config.get<std::string>("vfs.s3.aws_role_arn").value())
      , aws_external_id_(
            config.get<std::string>("vfs.s3.aws_external_id").value())
      , aws_load_frequency_(
            config.get<std::string>("vfs.s3.aws_load_frequency").value())
      , aws_session_name_(
            config.get<std::string>("vfs.s3.aws_session_name").value())
      , connect_max_tries_(
            config.get<int64_t>("vfs.s3.connect_max_tries").value())
      , connect_scale_factor_(
            config.get<int64_t>("vfs.s3.connect_scale_factor").value()){};

  ~S3Parameters() = default;

  /** Process-global AWS SDK logging level. */
  std::string logging_level_;

  /** Skip Aws::InitAPI for the S3 layer. */
  bool skip_init_;

  /** The maximum number of parallel operations issued. */
  uint64_t max_parallel_ops_;

  /** The length of a non-terminal multipart part. */
  uint64_t multipart_part_size_;

  /** The AWS region. */
  std::string region_;

  /** Whether or not to use virtual addressing. */
  bool use_virtual_addressing_;

  /** Whether or not to use multipart upload. */
  bool use_multipart_upload_;

  /** If true, the requester pays for S3 access charges. */
  bool requester_pays_;

  /** Names of values found in Aws::S3::Model::ObjectCannedACL enumeration. */
  std::string object_acl_str_;

  /** Names of values found in Aws::S3::Model::BucketCannedACL enumeration. */
  std::string bucket_acl_str_;

  /** The server-side encryption algorithm to use. */
  std::string sse_str_;

  /** The server-side encryption kms key. */
  std::string sse_kms_key_id_;

  /** Force S3 SDK to only load config options from a set source. */
  std::string config_source_;

  /** Make unauthenticated requests to s3. */
  bool aws_no_sign_request_;

  /** The S3 endpoint, if S3 is enabled. */
  std::string endpoint_override_;

  /** The S3 proxy host. */
  std::string proxy_host_;

  /** The S3 proxy port. */
  uint32_t proxy_port_;

  /** The S3 proxy username. */
  std::string proxy_username_;

  /** The S3 proxy password. */
  std::string proxy_password_;

  /** The S3 proxy scheme. */
  std::string proxy_scheme_;

  /** The S3 scheme (`http` or `https`), if S3 is enabled. */
  std::string scheme_;

  /** The connection timeout in ms. Any `long` value is acceptable. */
  int64_t connect_timeout_ms_;

  /** The request timeout in ms. Any `long` value is acceptable. */
  int64_t request_timeout_ms_;

  /** Set the AWS_ACCESS_KEY_ID. */
  std::string aws_access_key_id_;

  /** Set the AWS_SECRET_ACCESS_KEY. */
  std::string aws_secret_access_key_;

  /** Set the AWS_SESSION_TOKEN. */
  std::string aws_session_token_;

  /** Set the AWS_ROLE_ARN. Determines the role that we want to assume. */
  std::string aws_role_arn_;

  /**
   * Set the AWS_EXTERNAL_ID.
   * Third party access ID to your resources when assuming a role.
   */
  std::string aws_external_id_;

  /** Set the AWS_LOAD_FREQUENCY. Session time limit when assuming a role. */
  std::string aws_load_frequency_;

  /**
   * Set the AWS_SESSION_NAME. (Optional) session name when assuming a role.
   * Can be used for tracing and bookkeeping.
   */
  std::string aws_session_name_;

  /** The maximum tries for a connection. Any `long` value is acceptable. */
  int64_t connect_max_tries_;

  /**
   * The scale factor for exponential backoff when connecting to S3.
   * Any `long` value is acceptable.
   */
  int64_t connect_scale_factor_;
};

/**
 * This class implements the various S3 filesystem functions. It also
 * maintains buffer caches for writing into the various attribute files.
 */
class S3 {
 private:
  /** Forward declaration */
  struct MultiPartUploadState;

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param parent_stats The parent stats to inherit from.
   * @param thread_pool The parent VFS thread pool.
   * @param config Configuration parameters.
   */
  S3(stats::Stats* parent_stats, ThreadPool* thread_pool, const Config& config);

  /** Destructor. */
  ~S3();

  DISABLE_COPY_AND_COPY_ASSIGN(S3);
  DISABLE_MOVE_AND_MOVE_ASSIGN(S3);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Creates a bucket.
   *
   * @param bucket The name of the bucket to be created.
   * @return Status
   */
  Status create_bucket(const URI& bucket) const;

  /**
   * Disconnects a S3 client.
   *
   * @return Status
   */
  Status disconnect();

  /** Removes the contents of an S3 bucket. */
  Status empty_bucket(const URI& bucket) const;

  /**
   * Flushes an object to S3, finalizing the multipart upload.
   *
   * @param uri The URI of the object to be flushed.
   * @return Status
   */
  Status flush_object(const URI& uri);

  /**
   * Flushes an s3 object as a result of a remote global order write
   * finalize() call.
   *
   * @param uri The URI of the object to be flushed.
   */
  void finalize_and_flush_object(const URI& uri);

  /** Checks if a bucket is empty. */
  Status is_empty_bucket(const URI& bucket, bool* is_empty) const;

  /**
   * Check if a bucket exists.
   *
   * @param bucket The name of the bucket.
   * @param exists Mutates to `true` if `uri` is an existing bucket,
   *   and `false` otherwise.
   * @return Status
   */
  Status is_bucket(const URI& uri, bool* exists) const;

  /**
   * Checks if there is an object with prefix `uri/`. For instance, suppose
   * the following objects exist:
   *
   * `s3://some_bucket/foo/bar1`
   * `s3://some_bucket/foo2`
   *
   * `is_dir(`s3://some_bucket/foo`) and `is_dir(`s3://some_bucket/foo`) will
   * both return `true`, whereas `is_dir(`s3://some_bucket/foo2`) will return
   * `false`. This is because the function will first convert the input to
   * `s3://some_bucket/foo2/` (appending `/` in the end) and then check if
   * there exists any object with prefix `s3://some_bucket/foo2/` (in this
   * case there is not).
   *
   * @param uri The URI to check.
   * @param exists Sets it to `true` if the above mentioned condition holds.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* exists) const;

  /**
   * Checks if the given URI is an existing S3 object.
   *
   * @param uri The URI of the object to be checked.
   * @param exists Mutates to `true` if `uri` is an existing object,
   *   and `false` otherwise.
   * @return Status
   */
  Status is_object(const URI& uri, bool* exists) const;

  /**
   * Lists the objects that start with `prefix`. Full URI paths are
   * retrieved for the matched objects. If a delimiter is specified,
   * the URI paths will be truncated to the first delimiter character.
   * For instance, if there is a hierarchy:
   *
   * - `foo/bar/baz`
   * - `foo/bar/bash`
   * - `foo/bar/bang`
   * - `foo/boo`
   *
   * and the delimiter is `/`, the returned URIs will be
   *
   * - `foo/boo`
   * - `foo/bar`
   *
   * @param prefix The prefix URI.
   * @param paths Pointer of a vector of URIs to store the retrieved paths.
   * @param delimiter The delimiter that will
   * @param max_paths The maximum number of paths to be retrieved. The default
   *     `-1` indicates that no upper bound is specified.
   * @return Status
   */
  Status ls(
      const URI& prefix,
      std::vector<std::string>* paths,
      const std::string& delimiter = "/",
      int max_paths = -1) const;

  /**
   *
   * Lists objects and object information that start with `prefix`.
   *
   * @param prefix The parent path to list sub-paths.
   * @param delimiter The uri is truncated to the first delimiter
   * @param max_paths The maximum number of paths to be retrieved
   * @return A list of directory_entry objects
   */
  tuple<Status, optional<std::vector<filesystem::directory_entry>>>
  ls_with_sizes(
      const URI& prefix,
      const std::string& delimiter = "/",
      int max_paths = -1) const;

  /**
   * Renames an object.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   * @return Status
   */
  Status move_object(const URI& old_uri, const URI& new_uri);

  /**
   * Renames a directory. Note that this is an expensive operation.
   * The function will essentially copy all objects with directory
   * prefix `old_uri` to new objects with prefix `new_uri` and then
   * delete the old ones.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   * @return Status
   */
  Status move_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Copies a file.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   * @return Status
   */
  Status copy_file(const URI& old_uri, const URI& new_uri);

  /**
   * Copies a directory. All subdirectories and files are copied.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   * @return Status
   */
  Status copy_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Returns the size of the input object with a given URI in bytes.
   *
   * @param uri The URI of the object.
   * @param nbytes Pointer to `uint64_t` bytes to return.
   * @return Status
   */
  Status object_size(const URI& uri, uint64_t* nbytes) const;

  /**
   * Reads data from an object into a buffer.
   *
   * @param uri The URI of the object to be read.
   * @param offset The offset in the object from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param length The size of the data to be read from the object.
   * @param read_ahead_length The additional length to read ahead.
   * @param length_returned Returns the total length read into `buffer`.
   * @return Status
   */
  Status read(
      const URI& uri,
      off_t offset,
      void* buffer,
      uint64_t length,
      uint64_t read_ahead_length,
      uint64_t* length_returned) const;

  /**
   * Deletes a bucket.
   *
   * @param bucket The name of the bucket to be deleted.
   * @return Status
   */
  Status remove_bucket(const URI& bucket) const;

  /**
   * Deletes an object with a given URI.
   *
   * @param uri The URI of the object to be deleted.
   * @return Status
   */
  Status remove_object(const URI& uri) const;

  /**
   * Deletes all objects with prefix `prefix/` (if the ending `/` does not
   * exist in `prefix`, it is added by the function.
   *
   * For instance, suppose there exist the following objects:
   * - `s3://some_bucket/foo/bar1`
   * - `s3://some_bucket/foo/bar2/bar3
   * - `s3://some_bucket/foo/bar4
   * - `s3://some_bucket/foo2`
   *
   * `remove("s3://some_bucket/foo")` and `remove("s3://some_bucket/foo/")`
   * will delete objects:
   *
   * - `s3://some_bucket/foo/bar1`
   * - `s3://some_bucket/foo/bar2/bar3
   * - `s3://some_bucket/foo/bar4
   *
   * In contrast, `remove("s3://some_bucket/foo2")` will not delete anything,
   * the function internally appends `/` to the end of the URI, and therefore
   * there is not object with prefix "s3://some_bucket/foo2/" in this example.
   *
   * @param uri The prefix of the objects to be deleted.
   * @return Status
   */
  Status remove_dir(const URI& prefix) const;

  /**
   * Creates an empty object.
   *
   * @param uri The URI of the object to be created.
   * @return Status
   */
  Status touch(const URI& uri) const;

  /**
   * Writes the input buffer to an S3 object. Note that this is essentially
   * an append operation implemented via multipart uploads.
   *
   * @param uri The URI of the object to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @return Status
   */
  Status write(const URI& uri, const void* buffer, uint64_t length);

  /**
   * Writes the input buffer to an S3 object. This function buffers in memory
   * file_buffer_size_ bytes before calling global_order_write() to execute
   * the s3 global write logic.
   * This function should only be called for tiledb global order writes.
   *
   * @param uri The URI of the object to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   */

  void global_order_write_buffered(
      const URI& uri, const void* buffer, uint64_t length);

  /**
   * Writes the input buffer to an S3 object using the algorithm
   * described at the BufferedChunk struct definition.
   *
   * @param uri The URI of the object to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   */
  void global_order_write(const URI& uri, const void* buffer, uint64_t length);

 private:
  /* ********************************* */
  /*         PRIVATE DATATYPES         */
  /* ********************************* */

  /**
   * Identifies the current state of this class.
   */
  enum State { UNINITIALIZED, INITIALIZED, DISCONNECTED };

  /**
   * The retry strategy for S3 request failures.
   */
  class S3RetryStrategy : public Aws::Client::RetryStrategy {
   public:
    /** Constructor. */
    S3RetryStrategy(
        stats::Stats* const s3_stats,
        const uint64_t max_retries,
        const uint64_t scale_factor)
        : s3_stats_(s3_stats)
        , max_retries_(max_retries)
        , scale_factor_(scale_factor) {
    }

    /*
     * Returns true if the error can be retried given the error and
     * the number of times already tried.
     */
    bool ShouldRetry(
        const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
        long attempted_retries) const override {
      // Unconditionally retry on 'SLOW_DOWN' errors. The request
      // will eventually succeed.
      if (error.GetErrorType() == Aws::Client::CoreErrors::SLOW_DOWN) {
        // With an average retry interval of 1.5 seconds, 100 retries
        // would be a 2.5 minute hang, which is unreasonably long. Error
        // out in this scenario, as we probably have encountered a
        // programming error.
        if (attempted_retries == 100) {
          return false;
        }

        s3_stats_->add_counter("vfs_s3_slow_down_retries", 1);

        return true;
      }

      if (static_cast<uint64_t>(attempted_retries) >= max_retries_)
        return false;

      return error.ShouldRetry();
    }

    /**
     * Calculates the time in milliseconds the client should sleep
     * before attempting another request based on the error and attempted
     * retries.
     */
    long CalculateDelayBeforeNextRetry(
        const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
        long attempted_retries) const override {
      // Unconditionally retry 'SLOW_DOWN' errors between 1.25 and 1.75
      // seconds. The random 0.5 second range is to reduce the likelyhood
      // of starving a single client when multiple clients are performing
      // requests on the same prefix.
      if (error.GetErrorType() == Aws::Client::CoreErrors::SLOW_DOWN) {
        return 1250 + rand() % 500;
      }

      if (attempted_retries == 0)
        return 0;

      return (1L << attempted_retries) * static_cast<long>(scale_factor_);
    }

   private:
    /** The S3 `stats_`. */
    stats::Stats* s3_stats_;

    /** The maximum number of retries after an error. */
    uint64_t max_retries_;

    /** The scale of each exponential backoff delay. */
    uint64_t scale_factor_;
  };

  /**
   * This struct wraps the context state of a pending multipart upload request.
   */
  struct MakeUploadPartCtx {
    /** Constructor. */
    MakeUploadPartCtx()
        : upload_part_num(0){};
    MakeUploadPartCtx(
        Aws::S3::Model::UploadPartOutcome&& in_upload_part_outcome,
        const int in_upload_part_num)
        : upload_part_outcome(std::move(in_upload_part_outcome))
        , upload_part_num(in_upload_part_num) {
    }

    /** Move Constructor */
    MakeUploadPartCtx(MakeUploadPartCtx&& other) noexcept {
      this->upload_part_outcome = std::move(other.upload_part_outcome);
      this->upload_part_num = other.upload_part_num;
    }

    /** Move assignment operator */
    MakeUploadPartCtx& operator=(MakeUploadPartCtx&& other) {
      this->upload_part_outcome = std::move(other.upload_part_outcome);
      this->upload_part_num = other.upload_part_num;
      return *this;
    }

    /** Copy Constructor **/
    MakeUploadPartCtx(const MakeUploadPartCtx& other) = delete;

    /** The AWS future to wait on for a pending upload part request. */
    Aws::S3::Model::UploadPartOutcome upload_part_outcome;

    /** The part number of the pending upload part request. */
    int upload_part_num;
  };

  /**
   * This type holds the location and the size of an intermediate chunk on S3
   * These chunks are used with remote global writes according to the
   * algorithm below:
   *
   * Intermediate chunks are persisted across cloud executors via serialization
   * When a new write arrives via global_order_write(URI):
   * - Look if there is a multipart state for the URI, if not,
   *   create one and initiate multipart request.
   * - Look at the sizes of all intermediate chunks persisted before
   *   for this URI.
   * - If the sum of all intermediate chunks and the current chunk is less
   *   than 5MB.
   *   a) Persist the new chunk as an intermediate chunk on s3 under
   *      fragment_uri/__global_order_write__chunks/buffer_name_intID.
   * - Else
   *   a) Read all previous intermediate chunks.
   *   b) Merge them in memory including the current chunk.
   *   c) Upload the merged buffer as a new part using multipart upload.
   *   d) Delete all intermediate chunks under __global_order_write_chunks/
   *      for this URI.
   *   e) Clear the intermediate chunks from multipart_upload_states[uri].
   * - Done
   * When the global order write is done and the buffer file is finalized
   * via finalize_and_flush_object(uri):
   * - Read all intermediate chunks (not uploaded as >5mbs multipart parts)
   *   if any left from the last submit().
   * - Merge them with the current chunk and upload as the last multipart
   *   part (can be smaller than 5mb).
   * - The intermediate chunk list might just be empty if the last submit()
   *   also triggered a part upload.
   * - Send CompleteMultipartUploadRequest for S3 to merge the final buffer
   *   file object.
   */
  struct BufferedChunk {
    std::string uri;
    uint64_t size;

    BufferedChunk(std::string chunk_uri, uint64_t chunk_size)
        : uri(chunk_uri)
        , size(chunk_size) {
    }
  };

  /** Contains all state associated with a multipart upload transaction. */
  struct MultiPartUploadState {
    MultiPartUploadState()
        : part_number(0)
        , st(Status::Ok()){};
    /** Constructor. */
    MultiPartUploadState(
        const int in_part_number,
        Aws::String&& in_bucket,
        Aws::String&& in_key,
        Aws::String&& in_upload_id,
        std::map<int, Aws::S3::Model::CompletedPart>&& in_completed_parts)
        : part_number(in_part_number)
        , bucket(std::move(in_bucket))
        , key(std::move(in_key))
        , upload_id(std::move(in_upload_id))
        , completed_parts(std::move(in_completed_parts))
        , st(Status::Ok()){};

    MultiPartUploadState(MultiPartUploadState&& other) noexcept {
      this->part_number = other.part_number;
      this->bucket = std::move(other.bucket);
      this->key = std::move(other.key);
      this->upload_id = std::move(other.upload_id);
      this->completed_parts = std::move(other.completed_parts);
      this->st = other.st;
      this->buffered_chunks = std::move(other.buffered_chunks);
    }

    // Copy initialization
    MultiPartUploadState(const MultiPartUploadState& other) {
      this->part_number = other.part_number;
      this->bucket = other.bucket;
      this->key = other.key;
      this->upload_id = other.upload_id;
      this->completed_parts = other.completed_parts;
      this->st = other.st;
      this->buffered_chunks = other.buffered_chunks;
    }

    MultiPartUploadState& operator=(const MultiPartUploadState& other) {
      this->part_number = other.part_number;
      this->bucket = other.bucket;
      this->key = other.key;
      this->upload_id = other.upload_id;
      this->completed_parts = other.completed_parts;
      this->st = other.st;
      this->buffered_chunks = other.buffered_chunks;
      return *this;
    }

    /** The current part number. */
    uint64_t part_number;

    /** The AWS bucket. */
    Aws::String bucket;

    /** The AWS key. */
    Aws::String key;

    /** The AWS upload id. */
    Aws::String upload_id;

    /** Maps each part number to its completed part state. */
    std::map<int, Aws::S3::Model::CompletedPart> completed_parts;

    /** The overall status of the multipart request. */
    Status st;

    /**
     * Tracks all global order write intermediate chunks that make up a >5mb
     * multipart upload part
     */
    std::vector<BufferedChunk> buffered_chunks;

    /** Mutex for thread safety */
    mutable std::mutex mtx;
  };

  /**
   * Used to stream results from the GetObject request into
   * a pre-allocated buffer.
   */
  class PreallocatedIOStream : public Aws::IOStream {
   public:
    /**
     * Value constructor. Does not take ownership of the
     * input buffer.
     *
     * @param buffer The pre-allocated underlying buffer.
     * @param size The maximum size of the underlying buffer.
     */
    PreallocatedIOStream(void* const buffer, const size_t size)
        : Aws::IOStream(new Aws::Utils::Stream::PreallocatedStreamBuf(
              reinterpret_cast<unsigned char*>(buffer), size)) {
    }

    /** Destructor. */
    ~PreallocatedIOStream() {
      // Delete the unmanaged `Aws::Utils::Stream::PreallocatedStreamBuf`
      // that was allocated in the constructor.
      delete rdbuf();
    }
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /* The S3 configuration parameters. */
  S3Parameters s3_params_;

  /**
   * A libcurl initializer instance. This should remain
   * the first member variable to ensure that libcurl is
   * initialized before any calls that may require it.
   */
  tiledb::sm::curl::LibCurlInitializer curl_inited_;

  /** The class stats. */
  stats::Stats* stats_;

  /** The current state. */
  State state_;

  /**
   * The lazily-initialized S3 client. This is mutable so that nominally const
   * functions can call init_client().
   */
  mutable shared_ptr<Aws::S3::S3Client> client_;

  /** The AWS credetial provider. */
  mutable shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider_;

  /**
   * Mutex protecting client initialization. This is mutable so that nominally
   * const functions can call init_client().
   */
  mutable std::mutex client_init_mtx_;

  /** Configuration object used to initialize the client. */
  mutable tdb_unique_ptr<Aws::Client::ClientConfiguration> client_config_;

  /** The executor used by 'client_'. */
  mutable shared_ptr<S3ThreadPoolExecutor> s3_tp_executor_;

  /** The size of the file buffers used in multipart uploads. */
  uint64_t file_buffer_size_;

  /** AWS options. */
  Aws::SDKOptions options_;

  /** Maps a file name to its multipart upload state. */
  std::unordered_map<std::string, MultiPartUploadState>
      multipart_upload_states_;

  /** Protects 'multipart_upload_states_'. */
  mutable RWLock multipart_upload_rwlock_;

  /** File buffers used in the multi-part uploads. */
  std::unordered_map<std::string, Buffer*> file_buffers_;

  /** Pointer to thread pool owned by parent VFS instance. */
  ThreadPool* vfs_thread_pool_;

  /** Set the request payer for a s3 request. */
  Aws::S3::Model::RequestPayer request_payer_;

  /** The server-side encryption algorithm. */
  Aws::S3::Model::ServerSideEncryption sse_;

  /** Protects file_buffers map */
  std::mutex file_buffers_mtx_;

  /** If !NOT_SET assign to object requests supporting SetACL() */
  Aws::S3::Model::ObjectCannedACL object_canned_acl_;

  /** If !NOT_SET assign to bucket requests supporting SetACL() */
  Aws::S3::Model::BucketCannedACL bucket_canned_acl_;

  /** Support the old s3 configuration values if configured by user. */
  SSLConfig ssl_config_;

  friend class VFS;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Initializes the client, if it has not already been initialized.
   *
   * @return Status
   */
  Status init_client() const;

  /**
   * Copies an object.
   *
   * @param old_uri The object to be copied.
   * @param new_uri The newly created object.
   * @return Status
   */
  Status copy_object(const URI& old_uri, const URI& new_uri);

  /**
   * Fills the file buffer (given as an input `Buffer` object) from the
   * input binary `buffer`, up until the size of the file buffer becomes
   * `FILE_BUFFER_SIZE`. It also retrieves the number of bytes filled.
   *
   * @param buff The destination file buffer to fill in.
   * @param buffer The source binary buffer to fill the data from.
   * @param length The length of `buffer`.
   * @param nbytes_filled The number of bytes filled into `buff`.
   * @return Status
   */
  Status fill_file_buffer(
      Buffer* buff,
      const void* buffer,
      uint64_t length,
      uint64_t* nbytes_filled);

  /**
   * Returns the input `path` after adding a `/` character
   * at the front if it does not exist.
   */
  std::string add_front_slash(const std::string& path) const;

  /**
   * Returns the input `path` after removing a potential `/` character
   * from the front if it exists.
   */
  std::string remove_front_slash(const std::string& path) const;

  /**
   * Returns the input `path` after removing a potential `/` character
   * from the end if it exists.
   */
  std::string remove_trailing_slash(const std::string& path) const;

  /**
   * Writes the contents of the input buffer to the S3 object given by
   * the input `uri` as a new series of multipart uploads. It then
   * resets the buffer.
   *
   * @param uri The S3 object to write to.
   * @param buff The input buffer to flush.
   * @param last_part Should be true only when the flush corresponds to the last
   * part(s) of a multi-part upload.
   * @return Status
   */
  Status flush_file_buffer(const URI& uri, Buffer* buff, bool last_part);

  /**
   * Gets the local file buffer of an S3 object with a given URI.
   *
   * @param uri The URI of the S3 object whose file buffer is retrieved.
   * @param buff The local file buffer to be retrieved.
   * @return Status
   */
  Status get_file_buffer(const URI& uri, Buffer** buff);

  /**
   * Initiates a new multipart upload request for the input URI. Note: the
   * caller must hold the multipart data structure mutex.
   */
  Status initiate_multipart_request(
      Aws::Http::URI aws_uri, MultiPartUploadState* state);

  /**
   * Return the given authority and path strings joined with a '/'
   * character.
   */
  std::string join_authority_and_path(
      const std::string& authority, const std::string& path) const;

  /** Checks if the given object exists on S3. */
  Status is_object(
      const Aws::String& bucket_name,
      const Aws::String& object_key,
      bool* exists) const;

  /** Waits for the input object to be propagated. */
  Status wait_for_object_to_propagate(
      const Aws::String& bucketName, const Aws::String& objectKey) const;

  /** Waits for the input object to be deleted. */
  Status wait_for_object_to_be_deleted(
      const Aws::String& bucketName, const Aws::String& objectKey) const;

  /** Waits for the bucket to be created. */
  Status wait_for_bucket_to_be_created(const URI& bucket_uri) const;

  /**
   * Builds and returns a CompleteMultipartUploadRequest for completing an
   * in-progress multipart upload.
   *
   * @param state The state of an in-progress multipart upload transaction.
   * @return CompleteMultipartUploadRequest
   */
  Aws::S3::Model::CompleteMultipartUploadRequest
  make_multipart_complete_request(const MultiPartUploadState& state);

  /**
   * Builds and returns a AbortMultipartUploadRequest for aborting an
   * in-progress multipart upload.
   *
   * @param state The state of an in-progress multipart upload transaction.
   * @return AbortMultipartUploadRequest
   */
  Aws::S3::Model::AbortMultipartUploadRequest make_multipart_abort_request(
      const MultiPartUploadState& state);

  /**
   * Helper routine for finalizing a flush from either a complete or abort
   * request.
   *
   * @param outcome The returned outcome from the complete or abort request.
   * @param uri The URI of the S3 file to be written to.
   * @param buff The file buffer associated with 'uri'.
   * @return Status
   */
  template <typename R, typename E>
  Status finish_flush_object(
      const Aws::Utils::Outcome<R, E>& outcome,
      const URI& uri,
      Buffer* const buff);

  /**
   * Writes the input buffer to a file by issuing one PutObject
   * request. If the file does not exist, then it is created. If the file
   * exists then it is appended to.
   *
   * @param uri The URI of the S3 file to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @return Status
   */
  Status flush_direct(const URI& uri);

  /**
   * Writes the input buffer to a file by issuing one PutObject
   * request. If the file does not exist, then it is created. If the file
   * exists then it is appended to.
   *
   * @param uri The URI of the S3 file to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   */
  void write_direct(const URI& uri, const void* buffer, uint64_t length);

  /**
   * Writes the input buffer to a file by issuing one or more multipart upload
   * requests. If the file does not exist, then it is created. If the file
   * exists then it is appended to. This command will upload chunks of an
   * in-progress write each time the parallelization buffer size is exceeded
   * (calculated as product of  `multipart_part_size`` * ``max_parallel_ops``
   *  configuration options).
   *
   * @param uri The URI of the S3 file to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @param last_part Should be true only when this is the last multipart write
   * for an object.
   * @return Status
   */
  Status write_multipart(
      const URI& uri, const void* buffer, uint64_t length, bool last_part);

  /**
   * Issues an async multipart upload request.
   *
   * @param uri The URI of the S3 file to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @param upload_id The ID of the upload.
   * @param upload_part_num The part number of the upload.
   * @return MakeUploadPartCtx
   */
  MakeUploadPartCtx make_upload_part_req(
      const Aws::Http::URI& aws_uri,
      const void* buffer,
      uint64_t length,
      const Aws::String& upload_id,
      int upload_part_num);

  /**
   * Waits for an async multipart upload request to complete.
   *
   * @param uri The URI of the S3 file to be written to.
   * @param uri_path The URI path.
   * @param MakeUploadPartCtx The context of the pending multipart upload
   * request.
   * @return Status
   */
  Status get_make_upload_part_req(
      const URI& uri, const std::string& uri_path, MakeUploadPartCtx& ctx);

  /**
   * Used in serialization of global order writes to set the multipart upload
   * state in multipart_upload_states_ during deserialization
   *
   * @param uri The file uri used as key in the internal map
   * @param state The multipart upload state info
   * @return Status
   */
  Status set_multipart_upload_state(
      const std::string& uri, S3::MultiPartUploadState& state);

  /**
   * Returns the multipart upload state identified by uri
   *
   * @param uri The URI of the multipart state
   * @return an optional MultiPartUploadState object
   */
  std::optional<S3::MultiPartUploadState> multipart_upload_state(
      const URI& uri);

  /**
   * Generate a URI for an intermediate chunk based on the attribute URI
   * and a chunk id
   *
   * @param uri The URI of the intermediate chunk
   * @param id A numeric id for the new chunk
   * @return generated URI
   */
  URI generate_chunk_uri(const URI& attribute_uri, uint64_t id);

  /**
   * Generate a URI for an intermediate chunk based on the attribute URI
   * and a chunk id
   *
   * @param uri The URI of the intermediate chunk
   * @param chunk_name A previously generated intermediate chunk name in
   * in the form of "<attributename>_<numericid>"
   * @return generated URI
   */
  URI generate_chunk_uri(
      const URI& attribute_uri, const std::string& chunk_name);
};

}  // namespace tiledb::sm

#endif  // HAVE_S3
#endif  // TILEDB_S3_H
