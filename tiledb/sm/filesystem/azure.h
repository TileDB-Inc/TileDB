/**
 * @file   azure.h
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
 * This file defines the Azure class.
 */

#ifndef TILEDB_AZURE_H
#define TILEDB_AZURE_H

#ifdef HAVE_AZURE
#include <azure/core/base64.hpp>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
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

namespace tiledb {

namespace common::filesystem {
class directory_entry;
}

namespace sm {

class Azure {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Azure();

  /** Destructor. */
  ~Azure();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Initializes and connects an Azure client.
   *
   * @param config Configuration parameters.
   * @param thread_pool The parent VFS thread pool.
   * @return Status
   */
  Status init(const Config& config, ThreadPool* thread_pool);

  /**
   * Creates a container.
   *
   * @param container The name of the container to be created.
   * @return Status
   */
  Status create_container(const URI& container) const;

  /** Removes the contents of an Azure container. */
  Status empty_container(const URI& container) const;

  /**
   * Flushes an blob to Azure, finalizing the upload.
   *
   * @param uri The URI of the blob to be flushed.
   * @return Status
   */
  Status flush_blob(const URI& uri);

  /**
   * Check if a container is empty.
   *
   * @param container The name of the container.
   * @param is_empty Mutates to `true` if the container is empty.
   * @return Status
   */
  Status is_empty_container(const URI& uri, bool* is_empty) const;

  /**
   * Check if a container exists.
   *
   * @param container The name of the container.
   * @param is_container Mutates to `true` if `uri` is a container.
   * @return Status
   */
  Status is_container(const URI& uri, bool* is_container) const;

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
   * @param exists Sets it to `true` if the above mentioned condition holds.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* exists) const;

  /**
   * Checks if the given URI is an existing Azure blob.
   *
   * @param uri The URI of the object to be checked.
   * @param is_blob Mutates to `true` if `uri` is an existing blob, and `false`
   * otherwise.
   */
  Status is_blob(const URI& uri, bool* is_blob) const;

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
   *
   * Lists objects and object information that start with `uri`.
   *
   * @param uri The prefix URI.
   * @param delimiter The uri is truncated to the first delimiter
   * @param max_paths The maximum number of paths to be retrieved
   * @return A list of directory_entry objects
   */
  tuple<Status, optional<std::vector<filesystem::directory_entry>>>
  ls_with_sizes(
      const URI& uri,
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
   * Returns the size of the input blob with a given URI in bytes.
   *
   * @param uri The URI of the blob.
   * @param nbytes Pointer to `uint64_t` bytes to return.
   * @return Status
   */
  Status blob_size(const URI& uri, uint64_t* nbytes) const;

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
   * Deletes a container.
   *
   * @param uri The URI of the container to be deleted.
   * @return Status
   */
  Status remove_container(const URI& uri) const;

  /**
   * Deletes an blob with a given URI.
   *
   * @param uri The URI of the blob to be deleted.
   * @return Status
   */
  Status remove_blob(const URI& uri) const;

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
   * @return Status
   */
  Status remove_dir(const URI& uri) const;

  /**
   * Creates an empty blob.
   *
   * @param uri The URI of the blob to be created.
   * @return Status
   */
  Status touch(const URI& uri) const;

  /**
   * Writes the input buffer to an Azure object. Note that this is essentially
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

  /** Contains all state associated with a block list upload transaction. */
  class BlockListUploadState {
   public:
    BlockListUploadState()
        : next_block_id_(0)
        , st_(Status::Ok()) {
    }

    /* Generates the next base64-encoded block id. */
    std::string next_block_id() {
      const uint64_t block_id = next_block_id_++;
      const std::string block_id_str = std::to_string(block_id);

      // Pad the block id string with enough leading zeros to support
      // the maximum number of blocks (50,000). All block ids must be
      // of equal length among a single blob.
      const int block_id_chars = 5;
      std::vector<uint8_t> padded_block_id(
          block_id_chars - block_id_str.length(), '0');
      std::copy(
          block_id_str.begin(),
          block_id_str.end(),
          std::back_inserter(padded_block_id));

      const std::string b64_block_id_str =
          ::Azure::Core::Convert::Base64Encode(padded_block_id);

      block_ids_.emplace_back(b64_block_id_str);

      return b64_block_id_str;
    }

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

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The VFS thread pool. */
  ThreadPool* thread_pool_;

  /** The Azure blob service client. */
  tdb_unique_ptr<::Azure::Storage::Blobs::BlobServiceClient> client_;

  /** Maps a blob URI to a write cache buffer. */
  std::unordered_map<std::string, Buffer> write_cache_map_;

  /** Protects 'write_cache_map_'. */
  std::mutex write_cache_map_lock_;

  /**  The maximum size of each value-element in 'write_cache_map_'. */
  uint64_t write_cache_max_size_;

  /**  The maximum number of parallel requests. */
  uint64_t max_parallel_ops_;

  /**  The target block size in a block list upload */
  uint64_t block_list_block_size_;

  /** Whether or not to use block list upload. */
  bool use_block_list_upload_;

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
   * @return Status
   */
  Status fill_write_cache(
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
   * @return Status
   */
  Status flush_write_cache(
      const URI& uri, Buffer* write_cache_buffer, bool last_block);

  /**
   * Writes the input buffer as an uncommited block to Azure by issuing one
   * or more block upload requests.
   *
   * @param uri The blob URI.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @param last_part Should be true only when this is the last block of a blob.
   * @return Status
   */
  Status write_blocks(
      const URI& uri, const void* buffer, uint64_t length, bool last_block);

  /**
   * Executes and waits for a single, uncommited block upload.
   *
   * @param container_name The blob's container name.
   * @param blob_path The blob's file path relative to the container.
   * @param length The length of `buffer`.
   * @param block_id A base64-encoded string that is unique to this block
   * within the blob.
   * @param result The returned future to fetch the async upload result from.
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
  Status flush_blob_direct(const URI& uri);

  /**
   * Parses a URI into a container name and blob path. For example,
   * URI "azure://my-container/dir1/file1" will parse into
   * `*container_name == "my-container"` and `*blob_path == "dir1/file1"`.
   *
   * @param uri The URI to parse.
   * @param container_name Mutates to the container name.
   * @param blob_path Mutates to the blob path.
   * @return Status
   */
  Status parse_azure_uri(
      const URI& uri,
      std::string* container_name,
      std::string* blob_path) const;

  /**
   * Copies the blob at 'old_uri' to `new_uri`.
   *
   * @param old_uri The blob's current URI.
   * @param new_uri The blob's URI to move to.
   * @return Status
   */
  Status copy_blob(const URI& old_uri, const URI& new_uri);

  /**
   * Waits for a blob with `container_name` and `blob_path`
   * to exist on Azure.
   *
   * @param container_name The blob's container name.
   * @param blob_path The blob's path
   * @return Status
   */
  Status wait_for_blob_to_propagate(
      const std::string& container_name, const std::string& blob_path) const;

  /**
   * Check if 'container_name' is a container on Azure.
   *
   * @param container_name The container's name.
   * @param is_container Mutates to the output.
   * @return Status
   */
  Status is_container(
      const std::string& container_name, bool* const is_container) const;

  /**
   * Check if 'is_blob' is a blob on Azure.
   *
   * @param container_name The blob's container name.
   * @param blob_path The blob's path.
   * @param is_blob Mutates to the output.
   * @return Status
   */
  Status is_blob(
      const std::string& container_name,
      const std::string& blob_path,
      bool* const is_blob) const;

  /**
   * Removes a leading slash from 'path' if it exists.
   *
   * @param path the string to remove the leading slash from.
   */
  std::string remove_front_slash(const std::string& path) const;

  /**
   * Adds a trailing slash from 'path' if it doesn't already have one.
   *
   * @param path the string to add the trailing slash to.
   */
  std::string add_trailing_slash(const std::string& path) const;

  /**
   * Removes a trailing slash from 'path' if it exists.
   *
   * @param path the string to remove the trailing slash from.
   */
  std::string remove_trailing_slash(const std::string& path) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_AZURE
#endif  // TILEDB_AZURE_H
