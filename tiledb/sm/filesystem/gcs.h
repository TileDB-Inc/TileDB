/**
 * @file   gcs.h
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
 * This file defines the GCS class.
 */

#ifndef TILEDB_GCS_H
#define TILEDB_GCS_H

#ifdef HAVE_GCS

#include <google/cloud/storage/client.h>

#include "tiledb/common/rwlock.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/uri.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class GCS {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  GCS();

  /** Destructor. */
  ~GCS();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Initializes and connects a GCS client.
   *
   * @param config Configuration parameters.
   * @param thread_pool The parent VFS thread pool.
   * @return Status
   */
  Status init(const Config& config, ThreadPool* thread_pool);

  /**
   * Creates a bucket.
   *
   * @param uri The uri of the bucket to be created.
   * @return Status
   */
  Status create_bucket(const URI& uri) const;

  /** Removes the contents of a GCS bucket. */
  Status empty_bucket(const URI& uri) const;

  /**
   * Check if a bucket is empty.
   *
   * @param bucket The name of the bucket.
   * @param is_empty Mutates to `true` if the bucket is empty.
   * @return Status
   */
  Status is_empty_bucket(const URI& uri, bool* is_empty) const;

  /**
   * Check if a bucket exists.
   *
   * @param bucket The name of the bucket.
   * @param is_bucket Mutates to `true` if `uri` is a bucket.
   * @return Status
   */
  Status is_bucket(const URI& uri, bool* is_bucket) const;

  /**
   * Check if 'is_object' is a object on GCS.
   *
   * @param uri Uri of the object.
   * @param is_object Mutates to the output.
   * @return Status
   */
  Status is_object(const URI& uri, bool* const is_object) const;

  /**
   * Checks if there is an object with prefix `uri/`. For instance, suppose
   * the following objects exist:
   *
   * `gcs://some_gcs/foo/bar1`
   * `gcs://some_gcs/foo2`
   *
   * `is_dir(`gcs://some_gcs/foo`) and
   * `is_dir(`gcs://some_gcs/foo`) will both return `true`, whereas
   * `is_dir(`gcs://some_gcs/foo2`) will return `false`. This is because
   * the function will first convert the input to `gcs://some_gcs/foo2/`
   * (appending `/` in the end) and then check if there exists any object with
   * prefix `gcs://some_gcs/foo2/` (in this case there is not).
   *
   * @param uri The URI to check.
   * @param exists Sets it to `true` if the above mentioned condition holds.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* exists) const;

  /**
   * Deletes a bucket.
   *
   * @param uri The URI of the bucket to be deleted.
   * @return Status
   */
  Status remove_bucket(const URI& uri) const;

  /**
   * Deletes an object with a given URI.
   *
   * @param uri The URI of the object to be deleted.
   * @return Status
   */
  Status remove_object(const URI& uri) const;

  /**
   * Deletes all objects with prefix `uri/` (if the ending `/` does not
   * exist in `uri`, it is added by the function.
   *
   * For instance, suppose there exist the following objects:
   * - `gcs://some_gcs/foo/bar1`
   * - `gcs://some_gcs/foo/bar2/bar3
   * - `gcs://some_gcs/foo/bar4
   * - `gcs://some_gcs/foo2`
   *
   * `remove("gcs://some_gcs/foo")` and
   * `remove("gcs://some_gcs/foo/")` will delete objects:
   *
   * - `gcs://some_gcs/foo/bar1`
   * - `gcs://some_gcs/foo/bar2/bar3
   * - `gcs://some_gcs/foo/bar4
   *
   * In contrast, `remove("gcs://some_gcs/foo2")` will not delete
   * anything; the function internally appends `/` to the end of the URI, and
   * therefore there is not object with prefix "gcs://some_gcs/foo2/" in
   * this example.
   *
   * @param uri The prefix uri of the objects to be deleted.
   * @return Status
   */
  Status remove_dir(const URI& uri) const;

  /**
   * Lists the objects that start with `uri`. Full URI paths are
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
   * @param uri The prefix URI.
   * @param paths Pointer of a vector of URIs to store the retrieved paths.
   * @param delimiter The delimiter that will
   * @param max_paths The maximum number of paths to be retrieved. The default
   *     `-1` indicates that no upper bound is specified.
   * @return Status
   */
  Status ls(
      const URI& uri,
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
   * Creates an empty object.
   *
   * @param uri The URI of the object to be created.
   * @return Status
   */
  Status touch(const URI& uri) const;

  /**
   * Writes the input buffer to an GCS object. Note that this is essentially
   * an append operation implemented via multipart uploads.
   *
   * @param uri The URI of the object to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @return Status
   */
  Status write(const URI& uri, const void* buffer, uint64_t length);

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
   * Returns the size of the input object with a given URI in bytes.
   *
   * @param uri The URI of the object.
   * @param nbytes Pointer to `uint64_t` bytes to return.
   * @return Status
   */
  Status object_size(const URI& uri, uint64_t* nbytes) const;

  /**
   * Flushes an object to GCS, finalizing the upload.
   *
   * @param uri The URI of the object to be flushed.
   * @return Status
   */
  Status flush_object(const URI& uri);

 private:
  /* ********************************* */
  /*         PRIVATE DATATYPES         */
  /* ********************************* */

  /**
   * Identifies the current state of this class.
   */
  enum State { UNINITIALIZED, INITIALIZED };

  /** Contains all state associated with a part list upload transaction. */
  class MultiPartUploadState {
   public:
    MultiPartUploadState(const std::string& object_path)
        : object_path_(remove_trailing_slash(object_path))
        , next_part_id_(0)
        , st_(Status::Ok()) {
    }

    MultiPartUploadState(MultiPartUploadState&& other) noexcept {
      this->object_path_ = std::move(other.object_path_);
      this->next_part_id_ = other.next_part_id_;
      this->part_paths_ = std::move(other.part_paths_);
      this->st_ = other.st_;
    }

    // Copy initialization
    MultiPartUploadState(const MultiPartUploadState& other) {
      this->object_path_ = other.object_path_;
      this->next_part_id_ = other.next_part_id_;
      this->part_paths_ = other.part_paths_;
      this->st_ = other.st_;
    }

    MultiPartUploadState& operator=(const MultiPartUploadState& other) {
      this->object_path_ = other.object_path_;
      this->next_part_id_ = other.next_part_id_;
      this->part_paths_ = other.part_paths_;
      this->st_ = other.st_;
      return *this;
    }

    /* Generates the next part path. */
    std::string next_part_path() {
      const uint64_t part_id = next_part_id_++;
      const std::string part_path =
          object_path_ + "__tiledb_" + std::to_string(part_id);
      part_paths_.emplace_back(part_path);
      return part_path;
    }

    /* Returns all generated part paths. */
    std::vector<std::string> get_part_paths() const {
      return part_paths_;
    }

    /* Returns the aggregate status. */
    Status st() const {
      return st_;
    }

    /* Updates 'st_' if 'st' is non-OK */
    void update_st(const Status& st) {
      if (!st.ok()) {
        st_ = st;
      }
    }

    /** Mutex for thread safety */
    mutable std::mutex mtx_;

   private:
    // The object path for the final composed object.
    std::string object_path_;

    // The next part id to generate.
    uint64_t next_part_id_;

    // A list of all generated part ids.
    std::vector<std::string> part_paths_;

    // The aggregate status. If any individual part
    // upload fails, this will be in a non-OK status.
    Status st_;
  };
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The current state. */
  State state_;

  /**
   * Mutex protecting client initialization. This is mutable so that nominally
   * const functions can call init_client().
   */
  mutable std::mutex client_init_mtx_;

  /** The VFS thread pool. */
  ThreadPool* thread_pool_;

  // The GCS project id.
  std::string project_id_;

  // The GCS REST client.
  mutable google::cloud::StatusOr<google::cloud::storage::Client> client_;

  /** Maps a object URI to an write cache buffer. */
  std::unordered_map<std::string, Buffer> write_cache_map_;

  /** Protects 'write_cache_map_'. */
  std::mutex write_cache_map_lock_;

  /**  The maximum size of each value-element in 'write_cache_map_'. */
  uint64_t write_cache_max_size_;

  /**  The maximum number of parallel requests. */
  uint64_t max_parallel_ops_;

  /**  The target part size in a part list upload */
  uint64_t multi_part_part_size_;

  /** Whether or not to use part list upload. */
  bool use_multi_part_upload_;

  /** The timeout for network requests. */
  uint64_t request_timeout_ms_;

  /** Maps a object URI to its part list upload state. */
  std::unordered_map<std::string, MultiPartUploadState>
      multi_part_upload_states_;

  /** Protects 'multipart_upload_states_'. */
  RWLock multipart_upload_rwlock_;

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
   * Parses a URI into a bucket name and object path. For example,
   * URI "gcs://my-bucket/dir1/file1" will parse into
   * `*bucket_name == "my-bucket"` and `*object_path == "dir1/file1"`.
   *
   * @param uri The URI to parse.
   * @param bucket_name Mutates to the bucket name.
   * @param object_path Mutates to the object path.
   * @return Status
   */
  Status parse_gcs_uri(
      const URI& uri, std::string* bucket_name, std::string* object_path) const;

  /**
   * Removes a leading slash from 'path' if it exists.
   *
   * @param path the string to remove the leading slash from.
   */
  static std::string remove_front_slash(const std::string& path);

  /**
   * Adds a trailing slash from 'path' if it doesn't already have one.
   *
   * @param path the string to add the trailing slash to.
   */
  static std::string add_trailing_slash(const std::string& path);

  /**
   * Removes a trailing slash from 'path' if it exists.
   *
   * @param path the string to remove the trailing slash from.
   */
  static std::string remove_trailing_slash(const std::string& path);

  /**
   * Copies the object at 'old_uri' to `new_uri`.
   *
   * @param old_uri The object's current URI.
   * @param new_uri The object's URI to move to.
   * @return Status
   */
  Status copy_object(const URI& old_uri, const URI& new_uri);

  /**
   * Waits for a object with `bucket_name` and `object_path`
   * to exist on GCS.
   *
   * @param bucket_name The object's bucket name.
   * @param object_path The object's path
   * @return Status
   */
  Status wait_for_object_to_propagate(
      const std::string& bucket_name, const std::string& object_path) const;

  /**
   * Waits for a object with `bucket_name` and `object_path`
   * to not exist on GCS.
   *
   * @param bucket_name The object's bucket name.
   * @param object_path The object's path
   * @return Status
   */
  Status wait_for_object_to_be_deleted(
      const std::string& bucket_name, const std::string& object_path) const;

  /**
   * Waits for a bucket with `bucket_name`
   * to exist on GCS.
   *
   * @param bucket_name The bucket's name.
   * @return Status
   */
  Status wait_for_bucket_to_propagate(const std::string& bucket_name) const;

  /**
   * Waits for a bucket with `bucket_name`
   * to not exist on GCS.
   *
   * @param bucket_name The bucket's name.
   * @return Status
   */
  Status wait_for_bucket_to_be_deleted(const std::string& bucket_name) const;

  /**
   * Check if 'is_object' is a object on GCS.
   *
   * @param bucket_name The object's bucket name.
   * @param object_path The object's path.
   * @param is_object Mutates to the output.
   * @return Status
   */
  Status is_object(
      const std::string& bucket_name,
      const std::string& object_path,
      bool* const is_object) const;

  /**
   * Check if 'bucket_name' is a bucket on GCS.
   *
   * @param bucket_name The bucket's name.
   * @param is_bucket Mutates to the output.
   * @return Status
   */
  Status is_bucket(const std::string& bucket_name, bool* const is_bucket) const;

  /**
   * Thread-safe fetch of the write cache buffer in `write_cache_map_`.
   * If a buffer does not exist for `uri`, it will be created.
   *
   * @param uri The object URI.
   * @return Buffer
   */
  Buffer* get_write_cache_buffer(const std::string& uri);

  /**
   * Fills the write cache buffer (given as an input `Buffer` object) from
   * the input binary `buffer`, up until the size of the file buffer becomes
   * `write_cache_max_size_`. It also retrieves the number of bytes filled.
   *
   * @param write_cache_buffer The destination write cache buffer to fill.
   * @param buffer The source binary buffer to fill the data from.
   * @param length The length of `buffer`.
   * @param nbytes_filled The number of bytes filled into `write_cache_buffer`.
   * @return Status
   */
  Status fill_write_cache(
      Buffer* write_cache_buffer,
      const void* buffer,
      const uint64_t length,
      uint64_t* nbytes_filled);

  /**
   * Writes the contents of the input buffer to the object given by
   * the input `uri` as a new series of part uploads. Resets
   * 'write_cache_buffer'.
   *
   * @param uri The object URI.
   * @param write_cache_buffer The input buffer to flush.
   * @param last_part Should be true only when the flush corresponds to the
   * last part(s) of a part list upload.
   * @return Status
   */
  Status flush_write_cache(
      const URI& uri, Buffer* write_cache_buffer, bool last_part);

  /**
   * Writes the input buffer as an uncommited part to GCS by issuing one
   * or more part upload requests.
   *
   * @param uri The object URI.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @param last_part Should be true only when this is the last part of a
   * object.
   * @return Status
   */
  Status write_parts(
      const URI& uri, const void* buffer, uint64_t length, bool last_part);

  /**
   * Executes and waits for a single, uncommited part upload.
   *
   * @param bucket_name The object's bucket name.
   * @param object_part_path The object's part path.
   * @param length The length of `buffer`.
   * @param part_id A unique integer identifier for this part.
   * @return Status
   */
  Status upload_part(
      const std::string& bucket_name,
      const std::string& object_part_path,
      const void* const buffer,
      const uint64_t length);

  /**
   * Performs a best-effort to delete all objects in 'part_paths'.
   *
   * @param bucket_name The object's bucket name.
   * @param part_paths All object part paths to delete.
   */
  void delete_parts(
      const std::string& bucket_name,
      const std::vector<std::string>& part_paths);

  /**
   * Executres a delete object request on 'part_path'. This blocks
   * for a response from the server but does not wait for the object
   * to be deleted.
   *
   * @param bucket_name The object's bucket name.
   * @param part_path The object part's path to delete.
   * @return Status
   */
  Status delete_part(
      const std::string& bucket_name, const std::string& part_path);

  /**
   * Clears all instance state related to a multi-part upload on 'uri'.
   */
  void finish_multi_part_upload(const URI& uri);

  /**
   * Uploads the write cache buffer associated with 'uri' as an entire
   * object.
   */
  Status flush_object_direct(const URI& uri);
};

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_GCS
#endif  // TILEDB_GCS_H
