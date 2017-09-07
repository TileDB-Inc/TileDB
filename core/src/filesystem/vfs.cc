/**
 * @file   vfs.cc
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
 * This file implements the VFS class.
 */

#include "vfs.h"
#include "hdfs_filesystem.h"
#include "posix_filesystem.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS() = default;

VFS::~VFS() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

std::string VFS::abs_path(const std::string& path) const {
  if (URI::is_posix(path))
    return posix::abs_path(path);

  // Certainly starts with "<resource>://" other than "file://"
  return path;
}

Status VFS::create_dir(const URI& uri) const {
  if (uri.is_posix())
    return posix::create_dir(uri.to_path());

  // TODO: Handle all other file systems here !
}

Status VFS::create_file(const URI& uri) const {
  if (uri.is_posix())
    return posix::create_file(uri.to_path());

  // TODO: Handle all other file systems here !
}

Status VFS::filelock_lock(const URI& uri, int* fd, bool shared) const {
  if (uri.is_posix())
    return posix::filelock_lock(uri.to_path(), fd, shared);

  // TODO: Handle all other file systems here !
}

Status VFS::filelock_unlock(const URI& uri, int fd) const {
  if (uri.is_posix())
    return posix::filelock_unlock(fd);

  // TODO: Handle all other file systems here !
}

bool VFS::is_file(const URI& uri) const {
  if (uri.is_posix())
    return posix::is_file(uri.to_path());

  // TODO: Handle all other file systems here !
}

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  if (parent.is_posix()) {
    std::vector<std::string> files;
    RETURN_NOT_OK(posix::ls(parent.to_path(), &files));
    for (auto& file : files)
      uris->push_back(URI(file));
  }

  // TODO: Handle all other file systems here !
}

}  // namespace tiledb