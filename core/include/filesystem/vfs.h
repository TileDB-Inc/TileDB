/**
 * @file   vfs.h
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
 * This file declares the VFS class.
 */

#ifndef TILEDB_VFS_H
#define TILEDB_VFS_H

#include "buffer.h"
#include "status.h"
#include "uri.h"

#ifdef HAVE_HDFS
#include "hdfs.h"
#endif

#ifdef HAVE_S3
#include "s3.h"
#endif

#include <string>
#include <vector>

namespace tiledb {

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
  ~VFS();

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
   * Creates a directory.
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
  Status create_file(const URI& uri) const;

  /**
   * Removes a given path (recursive)
   * @param uri The uri of the path to be removed
   * @return Status
   */
  Status remove_path(const URI& uri) const;

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
   * @param fd A file descriptor for the filelock (used in unlocking the
   *     filelock).
   * @param shared *True* if it is a shared lock, *false* if it is an
   *     exclusive lock.
   * @return Status
   */
  Status filelock_lock(const URI& uri, int* fd, bool shared) const;

  /**
   * Unlocks a filelock.
   *
   * @param uri The URI of the filelock.
   * @param fd The descriptor of the filelock.
   * @return Status
   */
  Status filelock_unlock(const URI& uri, int fd) const;

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
   * @return *True* if the directory exists and *false* otherwise.
   */
  bool is_dir(const URI& uri) const;

  /**
   * Checks if a file exists.
   *
   * @param uri The URI of the file.
   * @return *True* if the file exists and *false* otherwise.
   */
  bool is_file(const URI& uri) const;

#ifdef HAVE_S3
  Status init(const S3::S3Config& s3_config);
#else
  /** Initializes the virtual filesystem. */
  Status init();
#endif

  /**
   * Retrieves all the URIs that have the first input as parent.
   *
   * @param parent The target directory to list.
   * @param uris The URIs that are contained in the parent.
   * @return Status
   */
  Status ls(const URI& parent, std::vector<URI>* uris) const;

  /**
   * Renames a TileDB resource path.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  Status move_path(const URI& old_uri, const URI& new_uri);

  /**
   * Reads the entire file into a buffer.
   *
   * @param uri The URI of the file.
   * @param buff The buffer to read into.
   * @return Status
   */
  //  Status read_from_file(const URI& uri, Buffer** buff);

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @return Status
   */
  Status read_from_file(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const;

  /**
   * Syncs (flushes) a file.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status sync(const URI& uri);

  /**
   * Writes the contents of a buffer into a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   * @return Status
   */
  Status write_to_file(
      const URI& uri, const void* buffer, uint64_t buffer_size);

 private:
/* ********************************* */
/*         PRIVATE ATTRIBUTES        */
/* ********************************* */
#ifdef HAVE_HDFS
  hdfsFS hdfs_;
#endif

#ifdef HAVE_S3
  S3 s3_;
#endif
};

}  // namespace tiledb

#endif  // TILEDB_VFS_H
