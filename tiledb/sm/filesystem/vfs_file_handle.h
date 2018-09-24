/**
 * @file   vfs_file_handle.h
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
 * This file declares the VFSFileHandle.
 */

#ifndef TILEDB_VFS_FILE_HANDLE_H
#define TILEDB_VFS_FILE_HANDLE_H

#include "tiledb/sm/filesystem/vfs.h"

namespace tiledb {
namespace sm {

/**
 * This class implements a file handle to be used along with
 * the virtual filesystem functionality, i.e., for opening a VFS file,
 * reading/writing, etc.
 */
class VFSFileHandle {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  VFSFileHandle(const URI& uri, VFS* vfs, VFSMode mode);

  /** Destructor. */
  ~VFSFileHandle() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /** Closes the file handle. */
  Status close();

  /** Returns `true` if the file handle is open. */
  bool is_open() const;

  /** Opens the file handle. */
  Status open();

  /** Reads from a file.
   *
   * @param offset The offset to start the reading from.
   * @param buffer The buffer to read into.
   * @param nbytes The number of bytes to read.
   * @return Status
   */
  Status read(uint64_t offset, void* buffer, uint64_t nbytes);

  /** Syncs the file handle (applicable to write mode only). */
  Status sync();

  /** Returns the URI associated with the file handle. */
  URI uri() const;

  /** Writes to a file.
   *
   * @param buffer The buffer to write from.
   * @param nbytes The number of bytes to write.
   * @return Status
   */
  Status write(const void* buffer, uint64_t nbytes);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The URI of the VFS file. */
  URI uri_;

  /** True if the VFS file is open. */
  std::atomic<bool> is_open_;

  /** A VFS object handling all VFS I/O. */
  VFS* vfs_;

  /** The mode which the VFS file was opened in. */
  VFSMode mode_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_VFS_FILE_HANDLE_H
