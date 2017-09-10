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

#include <string>

#include "status.h"
#include "uri.h"

namespace tiledb {

/** This class implements virtual filesystem functions. */
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

  Status sync(const URI& uri) const;

  /**
   * Returns the absolute path of the input string (mainly useful for
   * posix URI's).
   */
  static std::string abs_path(const std::string& path);

  /** Creates a directory. */
  Status create_dir(const URI& uri) const;

  /** Creates an empty file. */
  Status create_file(const URI& uri) const;

  Status delete_file(const URI& uri) const;

  Status filelock_lock(const URI& uri, int* fd, bool shared) const;

  /** Unlocks a filelock. */
  Status filelock_unlock(const URI& uri, int fd) const;

  /** Checks if a directory exists. */
  bool is_dir(const URI& uri) const;

  /** Checks if a file exists. */
  bool is_file(const URI& uri) const;

  /** Retrieves all the URIs that have the first input as parent. */
  Status ls(const URI& parent, std::vector<URI>* uris) const;

  Status move_dir(const URI& old_uri, const URI& new_uri);

  Status read_from_file(const URI& uri, Buffer** buff);

  Status read_from_file(
      const URI& uri, uint64_t offset, void* buffer, uint64_t length) const;

  Status write_to_file(
      const URI& uri, const void* buffer, uint64_t buffer_size) const;

  Status file_size(const URI& uri, uint64_t* size) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_VFS_H
