/**
 * @file vfs.h
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
 * This file declares the VFS class.
 */

#ifndef TILEDB_VFS_H
#define TILEDB_VFS_H

#include <string>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/platform/platform.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/cache/lru_cache.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/filesystem.h"
#include "tiledb/sm/filesystem/azure/azure.h"
#include "tiledb/sm/filesystem/gcs/gcs.h"
#include "tiledb/sm/filesystem/hdfs/hdfs.h"
#include "tiledb/sm/filesystem/memfs/memfs.h"
#include "tiledb/sm/filesystem/posix/posix.h"
#include "tiledb/sm/filesystem/s3/s3.h"
#include "tiledb/sm/filesystem/win/win.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/cancelable_tasks.h"
#include "tiledb/sm/stats/stats.h"

namespace tiledb::sm {

enum class VFSMode : uint8_t;

/**
 * This class implements a virtual filesystem that directs filesystem-related
 * function execution to the appropriate backend based on the input URI.
 */
class VFS {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor.
   * @param resources The ContextResources to use.
   **/
  VFS(ContextResources& resources);

  /** Destructor. */
  ~VFS() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(VFS);
  DISABLE_MOVE_AND_MOVE_ASSIGN(VFS);

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Return a config object containing the VFS parameters. All other non-VFS
   * parameters will are set to default values.
   */
  Config config() const;

  /**
   * Check if a URI refers to a bucket.
   *
   * Throws if the URI filesystem doesn't support buckets.
   *
   * @param uri The name of the object store bucket.
   * @return bool Whether the URI represents a bucket.
   */
  bool is_bucket(const URI& uri) const;

  /**
   * Checks if a bucket is empty.
   *
   * Throws if the bucket does not exist.
   *
   * @param uri The URI of the bucket to check.
   * @return bool Whether the bucket is empty.
   */
  bool is_empty_bucket(const URI& uri) const;

  /**
   * Check if a URI refers to a directory.
   *
   * @param uri The URI to check.
   * @return bool Whether uri refers to a directory or not.
   */
  bool is_dir(const URI& uri) const;

  /**
   * Check if a URI refers to a file.
   *
   * @param uri The URI to check.
   * @return bool Whether uri refers to a file or not.
   */
  bool is_file(const URI& uri) const;

  /**
   * Create a bucket.
   *
   * Throws if the URI filesystem doesn't support buckets.
   *
   * @param uri The URI of the bucket to create.
   * @return Status
   */
  void create_bucket(const URI& uri) const;

  /**
   * Recursively delete the contents of a bucket.
   *
   * Throws if the URI filesystem doesn't support buckets.
   *
   * @param uri The name of the bucket to be emptied.
   * @return Status
   */
  void empty_bucket(const URI& uri) const;

  /**
   * Remove a bucket.
   *
   * @param uri The URI of the bucket to be deleted.
   */
  void remove_bucket(const URI& uri) const;

  /**
   * Create a directory.
   *
   * @param uri The URI of the directory.
   */
  void create_dir(const URI& uri);

  /**
   * Get the total size of all files below directory at URI
   *
   * @param uri The URI of the directory.
   */
  uint64_t dir_size(const URI& uri);

  /**
   * Retrieves all the entries contained in the parent.
   *
   * @param parent The target directory to list.
   * @return All entries that are contained in the parent
   */
  std::vector<FilesystemEntry> ls(const URI& parent) const;

  /**
   * Copy a directory.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  void copy_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Recursively remove a directory.
   *
   * @param uri The uri of the directory to be removed.
   */
  void remove_dir(const URI& uri);

  /**
   * Deletes directories in parallel from the given vector of directories.
   *
   * @param uris The URIs of the directories to delete.
   */
  void remove_dirs(const std::vector<URI>& uris);

  /**
   * Create an empty file.
   *
   * @param uri The URI of the file.
   */
  void touch(const URI& uri);

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @return uint64_t The size of the file in bytes.
   */
  uint64_t file_size(const URI& uri) const;

  /**
   * Write the contents of a buffer to a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   */
  void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size);

  /**
   * Read from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   */
  void read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes);

  /**
   * Syncs (flushes) a file.
   *
   * @param uri The URI of the file.
   */
  void sync(const URI& uri);

  /**
   * Copy a file.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   */
  void copy_file(const URI& old_uri, const URI& new_uri);

  /**
   * Rename a file.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   */
  void move_file(const URI& old_uri, const URI& new_uri);

  /**
   * Delete a file.
   *
   * @param uri The URI of the file to remove.
   */
  void remove_file(const URI& uri);

  /**
   * Delete files in parallel from the given vector of files.
   *
   * @param uris The URIs of the files to delete.
   */
  void remove_files(const std::vector<URI>& uris) const;

  /**
   * Open a file.
   *
   * @param uri The URI of the file.
   * @param mode The mode in which the file is opened:
   *     - READ <br>
   *       The file is opened for reading. An error is throw if the file
   *       does not exist.
   *     - WRITE <br>
   *       The file is opened for writing. If the file exists, it will be
   *       overwritten.
   *     - APPEND <br>
   *       This is an alias for WRITE. Our filesystem abstraction forces all
   *       writes to be append only.
   */
  void open_file(const URI& uri, VFSMode mode);

  /**
   * Close a file, flushing its contents to persistent storage.
   *
   * @param uri The URI of the file.
   */
  void close_file(const URI& uri);

  /**
   * Closes a file, flushing its contents to persistent storage.
   * This function has special S3 logic tailored to work best with remote
   * global order writes.
   *
   * @param uri The URI of the file.
   */
  void finalize_and_close_file(const URI& uri);

  /**
   * Used in serialization to share the multipart upload state
   * among cloud executors during global order writes
   *
   * @param uri The file uri used as key in the internal map of the backend
   * @return MultiPartUploadState The state of the write.
   */
  MultiPartUploadState get_write_stats(const URI& uri);

  /**
   * Used in serialization of global order writes to set the multipart upload
   * state in the internal maps of cloud backends during deserialization
   *
   * @param uri The file uri used as key in the internal map of the backend
   * @param state The multipart upload state info
   */
  void set_write_state(const URI& uri, const MultiPartUploadState& state);

  /**
   * Used in remote global order writes to flush the internal
   * in-memory buffer for an URI that backends maintain to modulate the
   * frequency of multipart upload requests.
   *
   * @param uri The file uri identifying the backend file buffer
   */
  void flush_write(const URI& uri);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The ContextResources. */
  ContextResources& resources_;

  /* The VFS configuration parameters. */
  VFSParameters vfs_params_;

  /** The read-ahead cache. */
  tdb_unique_ptr<ReadAheadCache> read_ahead_cache_;

  /** The map of enabled filesystems. */
  std::unordered_map<std::string, shared_ptr<Filesystem>> filesystems_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Get the filesystem implementation for a given URI. */
  shared_ptr<Filesystem> get_filesystem_for_uri(const URI& uri);
};

}  // namespace tiledb::sm

#endif  // TILEDB_VFS_H
