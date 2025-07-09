/**
 * @file   azure.h
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
 * This file defines the Azure class.
 */

#ifndef TILEDB_AZURE_H
#define TILEDB_AZURE_H

#ifdef HAVE_AZURE
#include "ls_scanner.h"
#include "tiledb/common/common.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/filesystem_base.h"
#include "tiledb/sm/filesystem/ssl_config.h"
#include "tiledb/sm/misc/constants.h"
#include "uri.h"

#if !defined(NOMINMAX)
#define NOMINMAX  // avoid min/max macros from windows headers
#endif
#include <list>
#include <unordered_map>

// Forward declaration
namespace Azure::Storage::Blobs {
class BlobServiceClient;
}

using namespace tiledb::common;

namespace tiledb::common::filesystem {
class directory_entry;
}

namespace tiledb::sm {

/** Class for Azure status exceptions. */
class AzureException : public StatusException {
 public:
  explicit AzureException(const std::string& msg)
      : StatusException("Azure", msg) {
  }
};

/** Helper function to retrieve the given parameter from the config or env. */
std::string get_config_with_env_fallback(
    const Config& config, const std::string& key, const char* env_name);

/** Helper function to retrieve the blob endpoint from the config or env. */
std::string get_blob_endpoint(
    const Config& config, const std::string& account_name);

/**
 * The Azure-specific configuration parameters.
 *
 * @note The member variables' default declarations have not yet been moved
 * from the Config declaration into this struct.
 */
struct AzureParameters {
  AzureParameters() = delete;

  AzureParameters(const Config& config);

  /**  The maximum number of parallel requests. */
  uint64_t max_parallel_ops_;

  /**  The target block size in a block list upload */
  uint64_t block_list_block_size_;

  /**  The maximum size of each value-element in 'write_cache_map_'. */
  uint64_t write_cache_max_size_;

  /** The maximum number of retries. */
  int max_retries_;

  /** The minimum time to wait between retries. */
  std::chrono::milliseconds retry_delay_;

  /** The maximum time to wait between retries. */
  std::chrono::milliseconds max_retry_delay_;

  /** Whether or not to use block list upload. */
  bool use_block_list_upload_;

  /** The Blob Storage account name. */
  std::string account_name_;

  /** The Blob Storage account key. */
  std::string account_key_;

  /** The Blob Storage endpoint to connect to. */
  std::string blob_endpoint_;

  /** SSL configuration. */
  SSLConfig ssl_cfg_;

  /** Whether the config specifies a SAS token. */
  bool has_sas_token_;
};

class Azure;

/**
 * AzureScanner wraps the Azure ListBlobs request and provides an iterator for
 * results. If we reach the end of the current batch of results and results are
 * truncated, we fetch the next batch of results from Azure.
 *
 * For each batch of results collected by fetch_results(), the begin_ and end_
 * members are initialized to the first and last elements of the batch. The scan
 * steps through each result in the range [begin_, end_), using next() to
 * advance to the next object accepted by the filters for this scan.
 *
 * @section Known Defect
 *      The iterators of AzureScanner are initialized by the Azure ListBlobs
 *      results and there is no way to determine if they are different from
 *      iterators returned by a previous request. To be able to detect this, we
 *      can track the batch number and compare it to the batch number associated
 *      with the iterator returned by the previous request. Batch number can be
 *      tracked by the total number of times we submit a ListBlobs request
 *      within fetch_results().
 *
 * @tparam F The FilePredicate type used to filter object results.
 * @tparam D The DirectoryPredicate type used to prune prefix results.
 */
template <FilePredicate F, DirectoryPredicate D = DirectoryFilter>
class AzureScanner : public LsScanner<F, D> {
 public:
  /** Declare LsScanIterator as a friend class for access to call next(). */
  template <class scanner_type, class T, class Allocator>
  friend class LsScanIterator;
  using Iterator = LsScanIterator<AzureScanner<F, D>, LsObjects::value_type>;

  /** Constructor. */
  AzureScanner(
      const Azure& client,
      const URI& prefix,
      F file_filter,
      D dir_filter = accept_all_dirs,
      bool recursive = false,
      int max_keys = 1000);

  /**
   * Returns true if there are more results to fetch from Azure.
   */
  [[nodiscard]] inline bool more_to_fetch() const {
    return !has_fetched_ || continuation_token_.has_value();
  }

  /**
   * @return Iterator to the beginning of the results being iterated on.
   *    Input iterators are single-pass, so we return a copy of this iterator at
   *    it's current position.
   */
  Iterator begin() {
    return Iterator(this);
  }

  /**
   * @return Default constructed iterator, which marks the end of results using
   *    nullptr.
   */
  Iterator end() {
    return Iterator();
  }

 private:
  /**
   * Advance to the next object accepted by the filters for this scan.
   *
   * @param ptr Reference to the current data pointer.
   * @sa LsScanIterator::operator++()
   */
  void next(typename Iterator::pointer& ptr);

  /**
   * If the iterator is at the end of the current batch, this will fetch the
   * next batch of results from Azure. This does not check if the results are
   * accepted by the filters for this scan.
   *
   * @param ptr Reference to the current data iterator.
   */
  void advance(typename Iterator::pointer& ptr) {
    ptr++;
    if (ptr == end_) {
      if (more_to_fetch()) {
        // Fetch results and reset the iterator.
        ptr = fetch_results();
      } else {
        // Set the pointer to nullptr to indicate the end of results.
        end_ = ptr = typename Iterator::pointer();
      }
    }
  }

  /**
   * Fetch the next batch of results from Azure. This also handles setting the
   * continuation token for the next request, if the results were truncated.
   *
   * @return A pointer to the first result in the new batch. The return value
   *    is used to update the pointer managed by the iterator during traversal.
   *    If the request returned no results, this will return nullptr to mark the
   *    end of the scan.
   * @sa LsScanIterator::operator++()
   * @sa AzureScanner::next(typename Iterator::pointer&)
   */
  typename Iterator::pointer fetch_results();

  /** Reference to the Azure VFS. */
  const Azure& client_;
  /** Name of container. */
  std::string container_name_;
  /** Blob path. */
  std::string blob_path_;
  /** Maximum number of items to request from Azure. */
  int max_keys_;
  /** Iterators for the current objects fetched from Azure. */
  typename Iterator::pointer begin_, end_;

  /** Whether blobs have been fetched at least once. */
  bool has_fetched_;
  /**
   * Token to pass to Azure to continue iteration.
   *
   * If it is nullopt, and has_fetched_ is false, it means that th
   */
  std::optional<std::string> continuation_token_;
  LsObjects blobs_;
};

/**
 * @note in Azure, the concept of "buckets" are truly called "containers".
 * The virtual filesystem's base class may view a `directory` and `bucket`
 * as the same. It may also view an Azure `blob` and traditional `file` as the
 * same. All internal methods have been renamed to use the "bucket" and "file"
 * verbiage in compliance with the FilesystemBase class.
 */
class Azure : FilesystemBase {
  template <FilePredicate, DirectoryPredicate>
  friend class AzureScanner;

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
  Azure(ThreadPool* thread_pool, const Config& config);

  /**
   * Destructor.
   *
   * @note The default destructor may cause undefined behavior with
   * `Azure::Storage::Blobs::BlobServiceClient`, so this destructor must be
   * explicitly defined.
   */
  ~Azure();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Creates a container.
   *
   * @param container The name of the container to be created.
   */
  void create_bucket(const URI& container) const;

  /** Removes the contents of an Azure container. */
  void empty_bucket(const URI& container) const;

  /**
   * Flushes a blob to Azure, finalizing the upload.
   *
   * @param uri The URI of the blob to be flushed.
   * @param finalize Unused flag. Reserved for finalizing S3 object upload only.
   */
  void flush(const URI& uri, bool finalize);

  /**
   * Check if a container is empty.
   *
   * @param container The name of the container.
   * @return `true` if the container is empty, `false` otherwise.
   */
  bool is_empty_container(const URI& uri) const;

  /**
   * Check if a container exists.
   *
   * @param container The name of the container.
   * @return `true` if `uri` is a container, `false` otherwise.
   */
  bool is_container(const URI& uri) const;

  /**
   * Checks if there is an object with prefix `uri/`. For instance, suppose
   * the following objects exist:
   *
   * `azure://some_container/foo/bar1`
   * `azure://some_container/foo2`
   *
   * `is_dir(`azure://some_container/foo`) and
   * `is_dir(`azure://some_container/foo`) will both return `true`, whereas
   * `is_dir(`azure://some_container/foo2`) will return `false`. This is because
   * the function will first convert the input to `azure://some_container/foo2/`
   * (appending `/` in the end) and then check if there exists any object with
   * prefix `azure://some_container/foo2/` (in this case there is not).
   *
   * @param uri The URI to check.
   * @return `true` if the above mentioned condition holds, `false` otherwise.
   */
  bool is_dir(const URI& uri) const;

  /**
   * Checks if the given URI is an existing Azure blob.
   *
   * @param uri The URI of the object to be checked.
   * @return `true` if `uri` is an existing blob, `false` otherwise.
   */
  bool is_file(const URI& uri) const;

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
   * @param delimiter The delimiter that will
   * @param max_paths The maximum number of paths to be retrieved. The default
   *     `-1` indicates that no upper bound is specified.
   * @return The retrieved paths.
   */
  std::vector<std::string> ls(
      const URI& uri,
      const std::string& delimiter = "/",
      int max_paths = -1) const;

  /**
   * Lists objects and object information that start with `uri`.
   *
   * @param uri The prefix uri.
   * @return All entries that are contained in the prefix URI.
   */
  std::vector<filesystem::directory_entry> ls_with_sizes(const URI& uri) const;

  /**
   * Lists objects and object information that start with `uri`.
   *
   * @param uri The prefix URI.
   * @param delimiter The uri is truncated to the first delimiter
   * @param max_paths The maximum number of paths to be retrieved
   * @return A list of directory_entry objects
   */
  std::vector<filesystem::directory_entry> ls_with_sizes(
      const URI& uri, const std::string& delimiter, int max_paths) const;

  /**
   * Lists objects and object information that start with `prefix`, invoking
   * the FilePredicate on each entry collected and the DirectoryPredicate on
   * common prefixes for pruning.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param f The FilePredicate to invoke on each object for filtering.
   * @param d The DirectoryPredicate to invoke on each common prefix for
   *    pruning. This is currently unused, but is kept here for future support.
   * @param recursive Whether to recursively list subdirectories.
   */
  template <FilePredicate F, DirectoryPredicate D>
  LsObjects ls_filtered(
      const URI& parent,
      F f,
      D d = accept_all_dirs,
      bool recursive = false) const {
    AzureScanner<F, D> azure_scanner(*this, parent, f, d, recursive);

    LsObjects objects;
    for (auto object : azure_scanner) {
      objects.push_back(std::move(object));
    }
    return objects;
  }

  /**
   * Constructs a scanner for listing Azure objects. The scanner can be used to
   * retrieve an InputIterator for passing to algorithms such as `std::copy_if`
   * or STL constructors supporting initialization via input iterators.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param f The FilePredicate to invoke on each object for filtering.
   * @param d The DirectoryPredicate to invoke on each common prefix for
   *    pruning. This is currently unused, but is kept here for future support.
   * @param recursive Whether to recursively list subdirectories.
   * @param max_keys The maximum number of keys to retrieve per request.
   * @return Fully constructed AzureScanner object.
   */
  template <FilePredicate F, DirectoryPredicate D>
  AzureScanner<F, D> scanner(
      const URI& parent,
      F f,
      D d = accept_all_dirs,
      bool recursive = false,
      int max_keys = 1000) const {
    return AzureScanner<F, D>(*this, parent, f, d, recursive, max_keys);
  }

  /**
   * Creates a directory.
   *
   * @param uri The directory's URI.
   */
  void create_dir(const URI&) const {
    // No-op. Stub function for other filesystems.
  }

  /**
   * Copies the directory at 'old_uri' to `new_uri`.
   *
   * @param old_uri The directory's current URI.
   * @param new_uri The directory's URI to move to.
   */
  void copy_dir(const URI&, const URI&) const {
    // No-op. Stub function for other filesystems.
  }

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
   * Returns the size of the input blob with a given URI in bytes.
   *
   * @param uri The URI of the blob.
   * @return The size of the input blob, in bytes
   */
  uint64_t file_size(const URI& uri) const;

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
  Status read_impl(
      const URI& uri,
      off_t offset,
      void* buffer,
      uint64_t length,
      uint64_t read_ahead_length,
      uint64_t* length_returned) const;

  /**
   * Reads data from an object into a buffer.
   *
   * @param uri The URI of the object to be read.
   * @param offset The offset in the object from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param length The size of the data to be read from the object.
   * @param use_read_ahead Whether to use the read-ahead cache.
   */
  void read(const URI&, uint64_t, void*, uint64_t, bool) const {
    // #TODO. Currently a no-op until read refactor.
  }

  /**
   * Deletes a container.
   *
   * @param uri The URI of the container to be deleted.
   */
  void remove_bucket(const URI& uri) const;

  /**
   * Deletes an blob with a given URI.
   *
   * @param uri The URI of the blob to be deleted.
   */
  void remove_file(const URI& uri) const;

  /**
   * Deletes all objects with prefix `uri/` (if the ending `/` does not
   * exist in `uri`, it is added by the function.
   *
   * For instance, suppose there exist the following objects:
   * - `azure://some_container/foo/bar1`
   * - `azure://some_container/foo/bar2/bar3
   * - `azure://some_container/foo/bar4
   * - `azure://some_container/foo2`
   *
   * `remove("azure://some_container/foo")` and
   * `remove("azure://some_container/foo/")` will delete objects:
   *
   * - `azure://some_container/foo/bar1`
   * - `azure://some_container/foo/bar2/bar3
   * - `azure://some_container/foo/bar4
   *
   * In contrast, `remove("azure://some_container/foo2")` will not delete
   * anything; the function internally appends `/` to the end of the URI, and
   * therefore there is not object with prefix "azure://some_container/foo2/" in
   * this example.
   *
   * @param uri The prefix uri of the objects to be deleted.
   */
  void remove_dir(const URI& uri) const;

  /**
   * Creates an empty blob.
   *
   * @param uri The URI of the blob to be created.
   */
  void touch(const URI& uri) const;

  /**
   * Writes the input buffer to an Azure object. Note that this is essentially
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
   * Initializes the Azure blob service client and returns a reference to it.
   *
   * Calling code should include the Azure SDK headers to make
   * use of the BlobServiceClient.
   */
  const ::Azure::Storage::Blobs::BlobServiceClient& client() const {
    if (azure_params_.blob_endpoint_.empty()) {
      throw AzureException(
          "Azure VFS is not configured. Please set the "
          "'vfs.azure.storage_account_name' and/or "
          "'vfs.azure.blob_endpoint' configuration options.");
    }
    return client_singleton_.get(azure_params_);
  }

 private:
  /* ********************************* */
  /*         PRIVATE DATATYPES         */
  /* ********************************* */

  /** Contains all state associated with a block list upload transaction. */
  class BlockListUploadState {
   public:
    BlockListUploadState()
        : next_block_id_(0)
        , st_(Status::Ok()) {
    }

    /* Generates the next base64-encoded block id. */
    std::string next_block_id();

    /* Returns all generated block ids. */
    std::list<std::string> get_block_ids() const {
      return block_ids_;
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

   private:
    // The next block id to generate.
    uint64_t next_block_id_;

    // A list of all generated block ids.
    std::list<std::string> block_ids_;

    // The aggregate status. If any individual block
    // upload fails, this will be in a non-OK status.
    Status st_;
  };

  /**
   * Encapsulates access to an Azure BlobServiceClient.
   *
   * This class ensures that:
   * * Callers access the client in an initialized state.
   * * The client gets initialized only once even for concurrent accesses.
   */
  class AzureClientSingleton {
   public:
    /**
     * Gets a reference to the Azure BlobServiceClient, and initializes it if it
     * is not initialized.
     *
     * @param params The parameters to initialize the client with.
     */
    const ::Azure::Storage::Blobs::BlobServiceClient& get(
        const AzureParameters& params);

   private:
    /** The Azure blob service client. */
    tdb_unique_ptr<::Azure::Storage::Blobs::BlobServiceClient> client_;

    /** Protects from creating the client many times. */
    std::mutex client_init_mtx_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The Azure configuration parameters. */
  AzureParameters azure_params_;

  /** The VFS thread pool. */
  ThreadPool* thread_pool_;

  /** A holder for the Azure blob service client. */
  mutable AzureClientSingleton client_singleton_;

  /** Maps a blob URI to a write cache buffer. */
  std::unordered_map<std::string, Buffer> write_cache_map_;

  /** Protects 'write_cache_map_'. */
  std::mutex write_cache_map_lock_;

  /** Maps a blob URI to its block list upload state. */
  std::unordered_map<std::string, BlockListUploadState>
      block_list_upload_states_;

  /** Protects 'block_list_upload_states_'. */
  std::mutex block_list_upload_states_lock_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Thread-safe fetch of the write cache buffer in `write_cache_map_`.
   * If a buffer does not exist for `uri`, it will be created.
   *
   * @param uri The blob URI.
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
   */
  void fill_write_cache(
      Buffer* write_cache_buffer,
      const void* buffer,
      const uint64_t length,
      uint64_t* nbytes_filled);

  /**
   * Writes the contents of the input buffer to the blob given by
   * the input `uri` as a new series of block uploads. Resets
   * 'write_cache_buffer'.
   *
   * @param uri The blob URI.
   * @param write_cache_buffer The input buffer to flush.
   * @param last_block Should be true only when the flush corresponds to the
   * last block(s) of a block list upload.
   */
  void flush_write_cache(
      const URI& uri, Buffer* write_cache_buffer, bool last_block);

  /**
   * Writes the input buffer as an uncommited block to Azure by issuing one
   * or more block upload requests.
   *
   * @param uri The blob URI.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @param last_part Should be true only when this is the last block of a blob.
   */
  void write_blocks(
      const URI& uri, const void* buffer, uint64_t length, bool last_block);

  /**
   * Executes and waits for a single, uncommited block upload.
   *
   * @param container_name The blob's container name.
   * @param blob_path The blob's file path relative to the container.
   * @param length The length of `buffer`.
   * @param block_id A base64-encoded string that is unique to this block
   * within the blob.
   * @return Status
   */
  Status upload_block(
      const std::string& container_name,
      const std::string& blob_path,
      const void* const buffer,
      const uint64_t length,
      const std::string& block_id);

  /**
   * Clears all instance state related to a block list upload on 'uri'.
   */
  void finish_block_list_upload(const URI& uri);

  /**
   * Uploads the write cache buffer associated with 'uri' as an entire
   * blob.
   */
  void flush_blob_direct(const URI& uri);

  /**
   * Performs an Azure ListBlobs operation.
   *
   * @param container_name The container's name.
   * @param blob_path The prefix of the blobs to list.
   * @param recursive Whether to list blobs recursively.
   * @param max_keys A hint to Azure for the maximum number of keys to return.
   * @param continuation_token On entry, holds the token to pass to Azure to
   * continue a listing operation, or nullopt if the operation is starting. On
   * exit, will hold the continuation token to pass to the next listing
   * operation, or nullopt if there are no more results.
   *
   * @return Vector of results with each entry being a pair of the string URI
   * and object size.
   *
   * @note If continuation_token is not nullopt, callers must ensure that the
   * container_name, blob_path and recursive parameters are the same as the
   * previous call, going back to the first call when continuation_token was
   * nullopt. This is not enforced by the function, which is the reason it is
   * not public.
   */
  LsObjects list_blobs_impl(
      const std::string& container_name,
      const std::string& blob_path,
      bool recursive,
      int max_keys,
      optional<std::string>& continuation_token) const;

  /**
   * Parses a URI into a container name and blob path. For example,
   * URI "azure://my-container/dir1/file1" will parse into
   * `*container_name == "my-container"` and `*blob_path == "dir1/file1"`.
   *
   * @param uri The URI to parse.
   * @return A tuple of the container name and blob path.
   */
  static std::tuple<std::string, std::string> parse_azure_uri(const URI& uri);

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
};

template <FilePredicate F, DirectoryPredicate D>
AzureScanner<F, D>::AzureScanner(
    const Azure& client,
    const URI& prefix,
    F file_filter,
    D dir_filter,
    bool recursive,
    int max_keys)
    : LsScanner<F, D>(prefix, file_filter, dir_filter, recursive)
    , client_(client)
    , max_keys_(max_keys)
    , has_fetched_(false) {
  if (!prefix.is_azure()) {
    throw AzureException("URI is not an Azure URI: " + prefix.to_string());
  }

  auto [container_name, blob_path] =
      Azure::parse_azure_uri(prefix.add_trailing_slash());
  container_name_ = container_name;
  blob_path_ = blob_path;
  fetch_results();
  next(begin_);
}

template <FilePredicate F, DirectoryPredicate D>
void AzureScanner<F, D>::next(typename Iterator::pointer& ptr) {
  if (ptr == end_) {
    ptr = fetch_results();
  }

  while (ptr != end_) {
    auto& object = *ptr;

    // TODO: Add support for directory pruning.
    if (this->file_filter_(object.first, object.second)) {
      // Iterator is at the next object within results accepted by the filters.
      return;
    } else {
      // Object was rejected by the FilePredicate, do not include it in results.
      advance(ptr);
    }
  }
}

template <FilePredicate F, DirectoryPredicate D>
typename AzureScanner<F, D>::Iterator::pointer
AzureScanner<F, D>::fetch_results() {
  if (!more_to_fetch()) {
    begin_ = end_ = typename Iterator::pointer();
    return end_;
  }

  blobs_ = client_.list_blobs_impl(
      container_name_,
      blob_path_,
      this->is_recursive_,
      max_keys_,
      continuation_token_);
  has_fetched_ = true;
  // Update pointers to the newly fetched results.
  begin_ = blobs_.begin();
  end_ = blobs_.end();

  return begin_;
}
}  // namespace tiledb::sm

#endif  // HAVE_AZURE
#endif  // TILEDB_AZURE_H
