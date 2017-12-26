/**
 * @file   s3.h
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
 * This file defines the S3 class.
 */

#ifdef HAVE_S3

#ifndef TILEDB_S3_H
#define TILEDB_S3_H

#include "buffer.h"
#include "constants.h"
#include "status.h"
#include "uri.h"

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
#include <sys/types.h>
#include <string>
#include <vector>

namespace tiledb {

class BufferCache;

/**
 * This class implements the various S3 filesystem functions. It also
 * maintains buffer caches for writing into the various attribute files.
 */
class S3 {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** S3 configuration parameters. */
  struct S3Config {
    S3Config() {
      region_ = constants::s3_region;
      scheme_ = constants::s3_scheme;
      endpoint_override_ = constants::s3_endpoint_override;
      use_virtual_addressing_ = constants::s3_use_virtual_addressing;
      file_buffer_size_ = constants::s3_file_buffer_size;
      connect_timeout_ms_ = constants::s3_connect_timeout_ms;
      request_timeout_ms_ = constants::s3_request_timeout_ms;
    }

    std::string region_;
    std::string scheme_;
    std::string endpoint_override_;
    bool use_virtual_addressing_;
    uint64_t file_buffer_size_;
    long connect_timeout_ms_;
    long request_timeout_ms_;
  };

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
   * Check if a bucket exists.
   *
   * @param bucket The name of the bucket.
   * @return bool
   */
  bool bucket_exists(const char* bucket);

  /**
   * Connects an S3 client.
   *
   * @param s3_config The S3 configuration parameters.
   * @return Status
   */
  Status connect(const S3Config& s3_config);

  /**
   * Creates a bucket.
   *
   * @param bucket The name of the bucket to be created.
   * @return Status
   */
  Status create_bucket(const char* bucket);

  /**
   * Creates a new directory. Directories are not really supported in S3.
   * Instead we just create an empty file having a ".dir" suffix
   *
   * @param uri The URI of the directory resource to be created.
   * @return Status
   */
  Status create_dir(const URI& uri) const;

  /**
   * Creates an empty object.
   *
   * @param uri The URI of the object to be created.
   * @return Status
   */
  Status create_file(const URI& uri) const;

  /**
   * Disconnects a S3 client.
   *
   * @return Status
   */
  Status disconnect();

  /**
   * Deletes a bucket.
   *
   * @param bucket The name of the bucket to be deleted.
   * @return Status
   */
  Status delete_bucket(const char* bucket);

  /**
   * Returns the size of the input file with a given URI in bytes.
   *
   * @param uri The URI of the file.
   * @param nbytes Pointer to `uint64_t` bytes to return.
   * @return Status
   */
  Status file_size(const URI& uri, uint64_t* nbytes) const;

  /**
   * Flushes a file to s3, finalizing the multpart upload.
   *
   * @param uri The URI of the object to be flushed.
   * @return Status
   */
  Status flush_file(const URI& uri);

  /**
   * Checks if the URI is an existing S3 directory. Checks if the ".dir" object
   * exists
   *
   * @param uri The URI of the directory to be checked
   * @return `True` if `uri` is an existing directory, `false` otherwise.
   */
  bool is_dir(const URI& uri) const;

  /**
   * Checks if the given URI is an existing S3 object.
   *
   * @param uri The URI of the file to be checked.
   * @return `True` if `uri` is an existing file, and `false` otherwise.
   */
  bool is_file(const URI& uri) const;

  /**
   * Lists the files one level deep under a given path.
   *
   * @param uri The URI of the parent directory path.
   * @param paths Pointer of a vector of URIs to store the retrieved paths.
   * @return Status
   */
  Status ls(const URI& uri, std::vector<std::string>* paths) const;

  /**
   * Move a given filesystem path. This is a difficult task for S3 if the
   * path is a directory, because we need to recursively rename all objects
   * inside the directory.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   * @return Status
   */
  Status move_path(const URI& old_uri, const URI& new_uri);

  /**
   *  Reads data from a file into a buffer.
   *
   * @param uri The URI of the file to be read.
   * @param offset The offset in the file from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param length The size of the data to be read from the file.
   * @return Status
   */
  Status read_from_file(
      const URI& uri, off_t offset, void* buffer, uint64_t length) const;

  /**
   * Deletes a file with a given URI.
   *
   * @param uri The URI of the file to be deleted.
   * @return Status
   */
  Status remove_file(const URI& uri) const;

  /**
   * Removes a path with a given URI (recursively)
   *
   * @param uri The URI of the path to be removed.
   * @return Status
   */
  Status remove_path(const URI& uri) const;

  /**
   * Writes the input buffer to an S3 object. Note that this is essentially
   * an append operation implemented via multipart uploads.
   *
   * @param uri The URI of the object to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @return Status
   */
  Status write_to_file(const URI& uri, const void* buffer, uint64_t length);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The S3 client. */
  std::shared_ptr<Aws::S3::S3Client> client_;

  /** The size of the file buffers used in multipart uploads. */
  uint64_t file_buffer_size_;

  /** AWS options. */
  Aws::SDKOptions options_;

  /** Used in multi-part uploads. */
  std::unordered_map<std::string, Aws::String> multipart_upload_IDs_;

  /** Used in multi-part uploads. */
  std::unordered_map<std::string, int> multipart_upload_part_number_;

  /** Used in multi-part uploads. */
  std::
      unordered_map<std::string, Aws::S3::Model::CompleteMultipartUploadRequest>
          multipart_upload_request_;

  /** Used in multi-part uploads. */
  std::unordered_map<std::string, Aws::S3::Model::CompletedMultipartUpload>
      multipart_upload_;

  /** File buffers used in the multi-part uploads. */
  std::unordered_map<std::string, Buffer*> file_buffers_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Copies the object identified by `old_uri` to a new one identified by
   * `new_uri`. In the case of directories, this is done recursively for
   * all the objects that have as prefix the directory path.
   *
   * @param old_uri The object to be copied.
   * @param new_uri The newly created object.
   * @return Status
   */
  Status copy_path(const URI& old_uri, const URI& new_uri);

  /** Removes the contents of an S3 bucker. */
  Status empty_bucket(const Aws::String& bucketName);

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
   * Simply removes a potential `/` character from the front of `object_key`.
   */
  Aws::String fix_path(const Aws::String& object_key) const;

  /**
   * Writes the contents of the input buffer to the S3 object given by
   * the input `uri` as a new series of multipart uploads. It then
   * resets the buffer.
   *
   * @param uri The S3 object to write to.
   * @param buff The input buffer to flust.
   * @return Status
   */
  Status flush_file_buffer(const URI& uri, Buffer* buff);

  /**
   * Gets the local file buffer of an S3 object with a given URI.
   *
   * @param uri The URI of the S3 object whose file buffer is retrieved.
   * @param buff The local file buffer to be retrieved.
   * @return Status
   */
  Status get_file_buffer(const URI& uri, Buffer** buff);

  /** Initiates a new multipart upload request for the input URI. */
  Status initiate_multipart_request(Aws::Http::URI aws_uri);

  /**
   * Replaces in the `str` string the string `from` with string `to`.
   *
   * @param str The target string.
   * @param from The string to be replaced.
   * @param to The new string that will substitute `from`.
   * @return `true` if `from` exists in `str` and `false` otherwise.
   */
  bool replace(
      std::string& str, const std::string& from, const std::string& to) const;

  /** Waits for the input bucket to be emptied. */
  Status wait_for_bucket_to_empty(const Aws::String& bucketName);

  /** Waits for the input object to be propagated. */
  bool wait_for_object_to_propagate(
      const Aws::String& bucketName, const Aws::String& objectKey) const;

  /**
   * Writes the input buffer to a file using a multipart upload. If the file
   * does not exist, then it is created. If the file exists then it is appended
   * to.
   *
   * @param uri The URI of the S3 file to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @return Status
   */
  Status write_multipart(const URI& uri, const void* buffer, uint64_t length);
};

}  // namespace tiledb

#endif  // TILEDB_S3_H

#endif  // HAVE_S3