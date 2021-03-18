/**
 * @file   s3.h
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
 * This file defines the S3 class.
 */

#ifndef TILEDB_S3_H
#define TILEDB_S3_H

#ifdef HAVE_S3
#include "tiledb/common/logger.h"
#include "tiledb/common/rwlock.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/s3_thread_pool_executor.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/stats/stats.h"

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
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <sys/types.h>
#include <mutex>
#include <string>
#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * This class implements the various S3 filesystem functions. It also
 * maintains buffer caches for writing into the various attribute files.
 */
class S3 {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  S3();

  /** Destructor. */
  ~S3();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Initializes and connects an S3 client.
   *
   * @param config Configuration parameters.
   * @param thread_pool The parent VFS thread pool.
   * @return Status
   */
  Status init(const Config& config, ThreadPool* thread_pool);

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
    S3RetryStrategy(const uint64_t max_retries, const uint64_t scale_factor)
        : max_retries_(max_retries)
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

        STATS_ADD_COUNTER(
            stats::Stats::CounterType::VFS_S3_SLOW_DOWN_RETRIES, 1)

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
    MakeUploadPartCtx(
        Aws::S3::Model::UploadPartOutcomeCallable&&
            in_upload_part_outcome_callable,
        const int in_upload_part_num)
        : upload_part_outcome_callable(
              std::move(in_upload_part_outcome_callable))
        , upload_part_num(in_upload_part_num) {
    }

    /** The AWS future to wait on for a pending upload part request. */
    Aws::S3::Model::UploadPartOutcomeCallable upload_part_outcome_callable;

    /** The part number of the pending upload part request. */
    const int upload_part_num;
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
    }

    // Copy initialization
    MultiPartUploadState(const MultiPartUploadState& other) {
      this->part_number = other.part_number;
      this->bucket = other.bucket;
      this->key = other.key;
      this->upload_id = other.upload_id;
      this->completed_parts = other.completed_parts;
      this->st = other.st;
    }

    MultiPartUploadState& operator=(const MultiPartUploadState& other) {
      this->part_number = other.part_number;
      this->bucket = other.bucket;
      this->key = other.key;
      this->upload_id = other.upload_id;
      this->completed_parts = other.completed_parts;
      this->st = other.st;
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

    /** Mutex for thread safety */
    mutable std::mutex mtx;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The current state. */
  State state_;

  /**
   * The lazily-initialized S3 client. This is mutable so that nominally const
   * functions can call init_client().
   */
  mutable tdb_shared_ptr<Aws::S3::S3Client> client_;

  /**
   * Mutex protecting client initialization. This is mutable so that nominally
   * const functions can call init_client().
   */
  mutable std::mutex client_init_mtx_;

  /** Configuration object used to initialize the client. */
  mutable tdb_unique_ptr<Aws::Client::ClientConfiguration> client_config_;

  /** The executor used by 'client_'. */
  mutable std::shared_ptr<S3ThreadPoolExecutor> s3_tp_executor_;

  /** The size of the file buffers used in multipart uploads. */
  uint64_t file_buffer_size_;

  /** AWS options. */
  Aws::SDKOptions options_;

  /** Maps a file name to its multipart upload state. */
  std::unordered_map<std::string, MultiPartUploadState>
      multipart_upload_states_;

  /** Protects 'multipart_upload_states_'. */
  RWLock multipart_upload_rwlock_;

  /** The maximum number of parallel operations issued. */
  uint64_t max_parallel_ops_;

  /** The length of a non-terminal multipart part. */
  uint64_t multipart_part_size_;

  /** File buffers used in the multi-part uploads. */
  std::unordered_map<std::string, Buffer*> file_buffers_;

  /** The AWS region this instance was initialized with. */
  std::string region_;

  /** Pointer to thread pool owned by parent VFS instance. */
  ThreadPool* vfs_thread_pool_;

  /** Whether or not to use virtual addressing. */
  bool use_virtual_addressing_;

  /** Whether or not to use multipart upload. */
  bool use_multipart_upload_;

  /** Config stored from init for lazy client_init. */
  Config config_;

  /** Set the request payer for a s3 request. */
  Aws::S3::Model::RequestPayer request_payer_;

  /** The server-side encryption algorithm. */
  Aws::S3::Model::ServerSideEncryption sse_;

  /** The server-side encryption kms key. */
  std::string sse_kms_key_id_;

  /** Protects file_buffers map */
  std::mutex file_buffers_mtx_;

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
   * @param buff The input buffer to flust.
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
   * Writes the input buffer to a file by issuing one or more multipart upload
   * requests. If the file does not exist, then it is created. If the file
   * exists then it is appended to. This command will upload chunks of an
   * in-progress write each time the parallelisation buffer size is exceeded
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
};

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_S3
#endif  // TILEDB_S3_H
