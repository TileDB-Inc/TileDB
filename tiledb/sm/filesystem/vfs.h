/**
 * @file   vfs.h
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
 * This file declares the VFS class.
 */

#ifndef TILEDB_VFS_H
#define TILEDB_VFS_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/filelock.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/storage_manager/config.h"

#ifdef HAVE_S3
#include "tiledb/sm/filesystem/s3.h"
#endif

#ifdef HAVE_HDFS
#include "tiledb/sm/filesystem/hdfs_filesystem.h"
#endif

#include <set>
#include <string>
#include <vector>

namespace tiledb {
namespace sm {

/**
 * This class implements a virtual filesystem that directs filesystem-related
 * function execution to the appropriate backend based on the input URI.
 */
class VFS {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  VFS();

  /** Destructor. */
  ~VFS() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns the absolute path of the input string (mainly useful for
   * posix URI's).
   *
   * @param path The input path.
   * @return The string with the absolute path.
   */
  static std::string abs_path(const std::string& path);

  /**
   * Return a config object containing the VFS parameters. All other non-VFS
   * parameters will are set to default values.
   */
  Config config() const;

  /**
   * Creates a directory.
   *
   * - On S3, this is a noop.
   * - On all other backends, if the directory exists, the function
   *   just succeeds without doing anything.
   *
   * @param uri The URI of the directory.
   * @return Status
   */
  Status create_dir(const URI& uri) const;

  /**
   * Creates an empty file.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status touch(const URI& uri) const;

  /**
   * Cancels all background or queued tasks.
   */
  Status cancel_all_tasks();

  /**
   * Creates an object-store bucket.
   *
   * @param uri The name of the bucket to be created.
   * @return Status
   */
  Status create_bucket(const URI& uri) const;

  /**
   * Deletes an object-store bucket.
   *
   * @param uri The name of the bucket to be deleted.
   * @return Status
   */
  Status remove_bucket(const URI& uri) const;

  /**
   * Deletes the contents of an object-store bucket.
   *
   * @param uri The name of the bucket to be emptied.
   * @return Status
   */
  Status empty_bucket(const URI& uri) const;

  /**
   * Removes a given directory (recursive)
   *
   * @param uri The uri of the directory to be removed
   * @return Status
   */
  Status remove_dir(const URI& uri) const;

  /**
   * Deletes a file.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status remove_file(const URI& uri) const;

  /**
   * Locks a filelock.
   *
   * @param uri The URI of the filelock.
   * @param lock A handle for the filelock (used in unlocking the
   *     filelock).
   * @param shared *True* if it is a shared lock, *false* if it is an
   *     exclusive lock.
   * @return Status
   */
  Status filelock_lock(const URI& uri, filelock_t* lock, bool shared) const;

  /**
   * Unlocks a filelock.
   *
   * @param uri The URI of the filelock.
   * @param lock The handle of the filelock.
   * @return Status
   */
  Status filelock_unlock(const URI& uri, filelock_t fd) const;

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @param size The file size to be retrieved.
   * @return Status
   */
  Status file_size(const URI& uri, uint64_t* size) const;

  /**
   * Checks if a directory exists.
   *
   * @param uri The URI of the directory.
   * @param is_dir Set to `true` if the directory exists and `false` otherwise.
   * @return Status
   *
   * @note For S3, this function will return `true` if there is an object
   *     with prefix `uri/` (TileDB will append `/` internally to `uri`
   *     only if it does not exist), and `false` othewise.
   */
  Status is_dir(const URI& uri, bool* is_dir) const;

  /**
   * Checks if a file exists.
   *
   * @param uri The URI of the file.
   * @param is_file Set to `true` if the file exists and `false` otherwise.
   * @return Status
   */
  Status is_file(const URI& uri, bool* is_file) const;

  /**
   * Checks if an object-store bucket exists.
   *
   * @param uri The name of the S3 bucket.
   * @return is_bucket Set to `true` if the bucket exists and `false` otherwise.
   * @return Status
   */
  Status is_bucket(const URI& uri, bool* is_bucket) const;

  /**
   * Checks if an object-store bucket is empty.
   *
   * @param uri The name of the S3 bucket.
   * @param is_empty Set to `true` if the bucket is empty and `false` otherwise.
   */
  Status is_empty_bucket(const URI& uri, bool* is_empty) const;

  /**
   * Initializes the virtual filesystem with the given configuration.
   *
   * @param vfs_params VFS Configuration
   * @return Status
   */
  Status init(const Config::VFSParams& vfs_params);

  /**
   * Retrieves all the URIs that have the first input as parent.
   *
   * @param parent The target directory to list.
   * @param uris The URIs that are contained in the parent.
   * @return Status
   */
  Status ls(const URI& parent, std::vector<URI>* uris) const;

  /**
   * Renames a file.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  Status move_file(const URI& old_uri, const URI& new_uri);

  /**
   * Renames a directory.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  Status move_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @return Status
   */
  Status read(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const;

  /** Checks if a given filesystem is supported. */
  bool supports_fs(Filesystem fs) const;

  /**
   * Syncs (flushes) a file. Note that for S3 this is a noop.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status sync(const URI& uri);

  /**
   * Opens a file in a given mode.
   *
   *
   * @param uri The URI of the file.
   * @param mode The mode in which the file is opened:
   *     - READ <br>
   *       The file is opened for reading. An error is returned if the file
   *       does not exist.
   *     - WRITE <br>
   *       The file is opened for writing. If the file exists, it will be
   *       overwritten.
   *     - APPEND <b>
   *       The file is opened for writing. If the file exists, the write
   *       will start from the end of the file. Note that S3 does not
   *       support this operation and, thus, an error will be thrown in
   *       that case.
   * @return Status
   */
  Status open_file(const URI& uri, VFSMode mode);

  /**
   * Closes a file, flushing its contents to persistent storage.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status close_file(const URI& uri);

  /**
   * Writes the contents of a buffer into a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   * @return Status
   */
  Status write(const URI& uri, const void* buffer, uint64_t buffer_size);

 private:
/* ********************************* */
/*         PRIVATE ATTRIBUTES        */
/* ********************************* */
#ifdef HAVE_S3
  S3 s3_;
#endif

#ifdef HAVE_HDFS
  std::unique_ptr<hdfs::HDFS> hdfs_;
#endif

  /** VFS parameters. */
  Config::VFSParams vfs_params_;

  /** The set with the supported filesystems. */
  std::set<Filesystem> supported_fs_;

  /** Thread pool for parallel I/O operations. */
  std::unique_ptr<ThreadPool> thread_pool_;

  /**
   * Reads from a file by calling the specific backend read function.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @return Status
   */
  Status read_impl(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const;

  /**
   * Increment the lock count of the given URI.
   *
   * @param uri The URI
   * @return True if the new lock count is > 1.
   */
  bool incr_lock_count(const URI& uri) const;

  /**
   * Decrement the lock count of the given URI.
   *
   * @param uri The URI
   * @param is_zero Set to true if the new lock count is 0.
   * @return Status
   */
  Status decr_lock_count(const URI& uri, bool* is_zero) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_VFS_H
