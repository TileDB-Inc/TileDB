/**
 * @file   gcs.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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

#include <google/cloud/version.h>

#include "tiledb/common/rwlock.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/curl/curl_init.h"
#include "tiledb/sm/filesystem/filesystem_base.h"
#include "tiledb/sm/filesystem/ls_scanner.h"
#include "tiledb/sm/filesystem/ssl_config.h"
#include "tiledb/sm/misc/constants.h"
#include "uri.h"

using namespace tiledb::common;

namespace google::cloud {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
class Credentials;
class Options;
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
class Client;
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace google::cloud

namespace tiledb::common::filesystem {
class directory_entry;
}  // namespace tiledb::common::filesystem

namespace tiledb::sm {

/** Class for GCS status exceptions. */
class GCSException : public StatusException {
 public:
  explicit GCSException(const std::string& msg)
      : StatusException("GCS", msg) {
  }
};

/**
 * The GCS-specific configuration parameters.
 *
 * @note The member variables' default declarations have not yet been moved
 * from the Config declaration into this struct.
 */
struct GCSParameters {
  GCSParameters() = delete;

  GCSParameters(const Config& config);

  ~GCSParameters() = default;

  /** The GCS endpoint.  */
  std::string endpoint_;

  /** The project ID to create new buckets on using the VFS. */
  std::string project_id_;

  /**
   * The GCS service account credentials JSON string.
   *
   * Set the JSON string with GCS service account key. Takes precedence
   * over `vfs.gcs.workload_identity_configuration` if both are specified. If
   * neither is specified, Application Default Credentials will be used.
   *
   * @note Experimental
   */
  std::string service_account_key_;

  /**
   * The GCS external account credentials JSON string.
   *
   * Set the JSON string with Workload Identity Federation configuration.
   * `vfs.gcs.service_account_key` takes precedence over this if both are
   * specified. If neither is specified, Application Default Credentials will
   * be used.
   *
   * @note Experimental
   */
  std::string workload_identity_configuration_;

  /**
   * A comma-separated list with the GCS service accounts to impersonate.
   *
   * Set the GCS service account to impersonate. A chain of impersonated
   * accounts can be formed by specifying many service accounts, separated by a
   * comma.
   *
   * @note Experimental
   */
  std::string impersonate_service_account_;

  /**
   * The part size (in bytes) used in multi part writes.
   *
   * @note `vfs.gcs.multi_part_size` * `vfs.gcs.max_parallel_ops` bytes will
   * be buffered before issuing part uploads in parallel.
   */
  uint64_t multi_part_size_;

  /** The maximum number of parallel operations issued. */
  uint64_t max_parallel_ops_;

  /** Whether or not to use chunked part uploads. */
  bool use_multi_part_upload_;

  /** The maximum amount of time to retry network requests. */
  uint64_t request_timeout_ms_;

  /**
   * The maximum size in bytes of a direct upload to GCS.
   * Ignored if `vfs.gcs.use_multi_part_upload` is set to true.
   */
  uint64_t max_direct_upload_size_;
};

class GCS : public FilesystemBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param thread_pool The parent VFS thread pool.
   * @param config Configuration parameters.
   */
  GCS(ThreadPool* thread_pool, const Config& config);

  /** Destructor. Must be explicitly defined. */
  ~GCS();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Checks if this filesystem supports the given URI.
   *
   * @param uri The URI to check.
   * @return `true` if `uri` is supported on this filesystem, `false` otherwise.
   */
  bool supports_uri(const URI& uri) const;

  /**
   * Creates a bucket.
   *
   * @param uri The uri of the bucket to be created.
   */
  void create_bucket(const URI& uri) const;

  /** Removes the contents of a GCS bucket. */
  void empty_bucket(const URI& uri) const;

  /**
   * Check if a bucket is empty.
   *
   * @param bucket The name of the bucket.
   * @return `true` if the bucket is empty, `false` otherwise.
   */
  bool is_empty_bucket(const URI& uri) const;

  /**
   * Check if a bucket exists.
   *
   * @param bucket The name of the bucket.
   * @return `true` if `uri` is a bucket, `false` otherwise.
   */
  bool is_bucket(const URI& uri) const;

  /**
   * Check if 'uri' is an object on GCS.
   *
   * @param uri URI of the object.
   * @return `true` if `uri` is an obkect on GCS, `false` otherwise.
   */
  bool is_file(const URI& uri) const;

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
   * @return`true` if the above mentioned condition holds, `false` otherwise.
   */
  bool is_dir(const URI& uri) const;

  /**
   * Deletes a bucket.
   *
   * @param uri The URI of the bucket to be deleted.
   */
  void remove_bucket(const URI& uri) const;

  /**
   * Deletes an object with a given URI.
   *
   * @param uri The URI of the object to be deleted.
   */
  void remove_file(const URI& uri) const;

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
   */
  void remove_dir(const URI& uri) const;

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
   * Lists objects and object information that start with `prefix`, invoking
   * the FilePredicate on each entry collected and the DirectoryPredicate on
   * common prefixes for pruning.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param file_filter The FilePredicate to invoke on each object for
   * filtering.
   * @param directory_filter The DirectoryPredicate to invoke on each common
   * prefix for pruning. This is currently unused, but is kept here for future
   * support.
   * @param recursive Whether to recursively list subdirectories.
   * @return Vector of results with each entry being a pair of the string URI
   * and object size.
   */
  template <FilePredicate F, DirectoryPredicate D>
  LsObjects ls_filtered(
      const URI& uri,
      F file_filter,
      [[maybe_unused]] D directory_filter = accept_all_dirs,
      bool recursive = false) const {
    // We use the constructor of std::function that accepts an F&& to convert
    // the generic F to a polymorphic std::function.
    return ls_filtered_impl(uri, std::move(file_filter), recursive);
  }

  /**
   *
   * Lists objects and object information that start with `prefix`.
   *
   * @param uri The parent path to list sub-paths.
   * @param delimiter The uri is truncated to the first delimiter
   * @param max_paths The maximum number of paths to be retrieved
   * @return A list of directory_entry objects
   */
  std::vector<tiledb::common::filesystem::directory_entry> ls_with_sizes(
      const URI& uri, const std::string& delimiter, int max_paths) const;

  /**
   *
   * Lists objects and object information that start with `prefix`.
   *
   * @param uri The parent path to list sub-paths.
   * @return A list of directory_entry objects.
   */
  std::vector<tiledb::common::filesystem::directory_entry> ls_with_sizes(
      const URI& uri) const;

  /**
   * Copies the directory at 'old_uri' to `new_uri`.
   *
   * @param old_uri The directory's current URI.
   * @param new_uri The directory's URI to move to.
   */
  void copy_dir(const URI&, const URI&) const;

  /**
   * Copies the blob at 'old_uri' to `new_uri`.
   *
   * @param old_uri The blob's current URI.
   * @param new_uri The blob's URI to move to.
   */
  void copy_file(const URI& old_uri, const URI& new_uri) const;

  /**
   * Renames an object.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   */
  void move_file(const URI& old_uri, const URI& new_uri) const;

  /**
   * Renames a directory. Note that this is an expensive operation.
   * The function will essentially copy all objects with directory
   * prefix `old_uri` to new objects with prefix `new_uri` and then
   * delete the old ones.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   */
  void move_dir(const URI& old_uri, const URI& new_uri) const;

  /**
   * Creates an empty object.
   *
   * @param uri The URI of the object to be created.
   */
  void touch(const URI& uri) const;

  /**
   * Writes the input buffer to an GCS object. Note that this is essentially
   * an append operation implemented via multipart uploads.
   *
   * @param uri The URI of the object to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @param remote_global_order_write Unused flag. Reserved for S3 objects only.
   */
  void write(
      const URI& uri,
      const void* buffer,
      uint64_t length,
      bool remote_global_order_write);

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @param read_ahead_nbytes The number of bytes to read ahead.
   */
  uint64_t read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      uint64_t read_ahead_nbytes = 0);

  /**
   * Returns the size of the input object with a given URI in bytes.
   *
   * @param uri The URI of the object.
   * @return The size of the object.
   */
  uint64_t file_size(const URI& uri) const;

  /**
   * Flushes an object to GCS, finalizing the upload.
   *
   * @param uri The URI of the object to be flushed.
   * @param finalize Unused flag. Reserved for finalizing S3 object upload only.
   */
  void flush(const URI& uri, bool finalize);

  /**
   * Creates a GCS credentials object.
   *
   * This method is intended to be used by testing code only.
   *
   * @param options Options to configure the credentials.
   * @return shared pointer to credentials
   */
  std::shared_ptr<google::cloud::Credentials> make_credentials(
      const google::cloud::Options& options) const;

  /**
   * Creates a directory.
   *
   * @param uri The directory's URI.
   */
  void create_dir(const URI&) const {
    // No-op. Stub function for other filesystems.
  }

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
          object_path_ + ".part" + std::to_string(part_id) + ".tiledb.tmp";
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

  /** The GCS configuration parameters. */
  GCSParameters gcs_params_;

  /**
   * A libcurl initializer instance. This should remain
   * the first member variable to ensure that libcurl is
   * initialized before any calls that may require it.
   */
  tiledb::sm::curl::LibCurlInitializer curl_inited_;

  /** The current state. */
  State state_;

  /** SSLConfig options. */
  SSLConfig ssl_cfg_;

  /**
   * Mutex protecting client initialization. This is mutable so that nominally
   * const functions can call init_client().
   */
  mutable std::mutex client_init_mtx_;

  /** The VFS thread pool. */
  ThreadPool* thread_pool_;

  // The GCS REST client.
  mutable tdb_unique_ptr<google::cloud::storage::Client> client_;

  /** Maps a object URI to an write cache buffer. */
  std::unordered_map<std::string, Buffer> write_cache_map_;

  /** Protects 'write_cache_map_'. */
  std::mutex write_cache_map_lock_;

  /**  The maximum size of each value-element in 'write_cache_map_'. */
  uint64_t write_cache_max_size_;

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
  Status copy_object(const URI& old_uri, const URI& new_uri) const;

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
   * Check if 'bucket_name/object_path' is an object on GCS.
   *
   * @param bucket_name The object's bucket name.
   * @param object_path The object's path.
   * @return `true` if the given object exists on GCS, `false` otherwise
   */
  bool is_object(
      const std::string& bucket_name, const std::string& object_path) const;

  /**
   * Check if 'bucket_name' is a bucket on GCS.
   *
   * @param bucket_name The bucket's name.
   * @return `true` if `bucket_name` is a bucket on GCS, `false` otherwise.
   */
  bool is_bucket(const std::string& bucket_name) const;

  /**
   * Contains the implementation of ls_filtered.
   *
   * @section Notes
   *
   * The use of the non-generic std::function is necessary to keep the
   * function's implementation in gcs.cc and avoid leaking the Google Cloud
   * SDK headers, which would cause significant build performance regressions
   * (see PR 4777). In the public-facing ls_filtered, we still use a generic
   * callback which we convert.
   *
   * This has the consequence that the callback cannot capture variables that
   * are not copy-constructible. It could be rectified with C++ 23's
   * std::move_only_function, when it becomes available.
   *
   * @param uri The parent path to list sub-paths.
   * @param file_filter The FilePredicate to invoke on each object for
   * filtering.
   * @param recursive Whether to recursively list subdirectories.
   * @return Vector of results with each entry being a pair of the string URI
   * and object size.
   */
  LsObjects ls_filtered_impl(
      const URI& uri,
      std::function<bool(const std::string_view, uint64_t)> file_filter,
      bool recursive) const;

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

}  // namespace tiledb::sm

#endif  // HAVE_GCS
#endif  // TILEDB_GCS_H
