/**
 * @file vfs_mode.h
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
 * This file defines the tiledb VFSMode enum that maps to the
 * `tiledb_vfs_mode_t` C-api enum.
 */

#ifndef TILEDB_VFS_MODE_H
#define TILEDB_VFS_MODE_H

#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

enum class VFSMode : uint8_t {
#define TILEDB_VFS_MODE_ENUM(id) id
#include "tiledb/api/c_api/vfs/vfs_api_enum.h"
#undef TILEDB_VFS_MODE_ENUM
};

/** Returns the string representation of the input vfsmode type. */
inline const std::string& vfsmode_str(VFSMode vfsmode) {
  switch (vfsmode) {
    case VFSMode::VFS_READ:
      return constants::vfsmode_read_str;
    case VFSMode::VFS_WRITE:
      return constants::vfsmode_write_str;
    case VFSMode::VFS_APPEND:
      return constants::vfsmode_append_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the vfsmode given a string representation. */
inline Status vfsmode_enum(const std::string& vfsmode_str, VFSMode* vfsmode) {
  if (vfsmode_str == constants::vfsmode_read_str)
    *vfsmode = VFSMode::VFS_READ;
  else if (vfsmode_str == constants::vfsmode_write_str)
    *vfsmode = VFSMode::VFS_WRITE;
  else if (vfsmode_str == constants::vfsmode_append_str)
    *vfsmode = VFSMode::VFS_APPEND;
  else
    return Status_Error("Invalid VFSMode " + vfsmode_str);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_VFS_MODE_H
