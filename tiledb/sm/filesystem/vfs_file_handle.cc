/**
 * @file   vfs_file_handle.cc
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
 * This file implements the VFSFileHandle class.
 */

#include <sstream>

#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "tiledb/sm/filesystem/vfs.h"

namespace tiledb::sm {

class VFSHandleException : public StatusException {
 public:
  explicit FilesystemException(const std::string& message)
      : StatusException("VFS Handle", message) {
  }
};

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFSFileHandle::VFSFileHandle(const URI& uri, VFS* vfs, VFSMode mode)
    : vfs_(vfs)
    , uri_(uri)
    , is_open_(true)
    , mode_(mode) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

void VFSFileHandle::open() {
  vfs_->open_file(uri_, mode_);
}

bool VFSFileHandle::is_open() const {
  return is_open_;
}

void VFSFileHandle::close() {
  if (!is_open_) {
    throw VFSHandleException("Error closing file '" << uri_.to_string() << "'; File is not open");
  }

  // Close file in write or append mode
  if (mode_ != tiledb::sm::VFSMode::VFS_READ) {
    vfs_->close_file(uri_);

    // Create an empty file if the file does not exist
    if (!vfs_->is_file(uri_)) {
      vfs_->touch(uri_);
    }
  }

  is_open_ = false;
}

Status VFSFileHandle::write(const void* buffer, uint64_t nbytes) {
  if (!is_open_) {
    throw VFSHandleException("Error reading to file '" + uri_.to_string() +
  "'; File is not open.");
  }

  vfs_->write(uri_, buffer, nbytes);
}

void VFSFileHandle::read(uint64_t offset, void* buffer, uint64_t nbytes) {
  if (!is_open_) {
    throw VFSHandleException("Error reading from file '" + uri_.to_string() +
      "'; File is not open.");
  }

  vfs_->read(uri_, offset, buffer, nbytes);
}

void VFSFileHandle::sync() {
  if (!is_open_) {
    throw VFSHandleException("Error syncing file '" + uri_.to_string() +
  "'; File is not open.");
  }

  vfs_->sync(uri_);
}

}  // namespace sm
}  // namespace tiledb
