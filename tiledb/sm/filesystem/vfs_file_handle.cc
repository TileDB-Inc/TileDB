/**
 * @file   vfs_file_handle.cc
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
 * This file implements the VFSFileHandle class.
 */

#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "tiledb/sm/misc/logger.h"

#include <sstream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFSFileHandle::VFSFileHandle(const URI& uri, VFS* vfs, VFSMode mode)
    : uri_(uri)
    , vfs_(vfs)
    , mode_(mode) {
  is_open_ = true;
}

/* ********************************* */
/*                API                */
/* ********************************* */

Status VFSFileHandle::close() {
  if (!is_open_) {
    std::stringstream msg;
    msg << "Cannot close file '" << uri_.to_string() << "'; File is not open";
    auto st = tiledb::sm::Status::VFSFileHandleError(msg.str());
    return LOG_STATUS(st);
  }

  // Close file in write or append mode
  if (mode_ != tiledb::sm::VFSMode::VFS_READ) {
    RETURN_NOT_OK(vfs_->close_file(uri_));

    // Create an empty file if the file does not exist
    bool exists;
    RETURN_NOT_OK(vfs_->is_file(uri_, &exists));
    if (!exists)
      RETURN_NOT_OK(vfs_->touch(uri_));
  }

  is_open_ = false;

  return Status::Ok();
}

bool VFSFileHandle::is_open() const {
  return is_open_;
}

Status VFSFileHandle::open() {
  return vfs_->open_file(uri_, mode_);
}

Status VFSFileHandle::read(uint64_t offset, void* buffer, uint64_t nbytes) {
  if (!is_open_) {
    std::stringstream msg;
    msg << "Cannot read from file '" << uri_.to_string()
        << "'; File is not open";
    auto st = tiledb::sm::Status::VFSFileHandleError(msg.str());
    return LOG_STATUS(st);
  }

  return vfs_->read(uri_, offset, buffer, nbytes);
}

Status VFSFileHandle::sync() {
  if (!is_open_) {
    std::stringstream msg;
    msg << "Cannot sync file '" << uri_.to_string() << "'; File is not open";
    auto st = tiledb::sm::Status::VFSFileHandleError(msg.str());
    return LOG_STATUS(st);
  }

  return vfs_->sync(uri_);
}

URI VFSFileHandle::uri() const {
  return uri_;
}

Status VFSFileHandle::write(const void* buffer, uint64_t nbytes) {
  if (!is_open_) {
    std::stringstream msg;
    msg << "Cannot write to file '" << uri_.to_string()
        << "'; File is not open";
    auto st = tiledb::sm::Status::VFSFileHandleError(msg.str());
    return LOG_STATUS(st);
  }

  return vfs_->write(uri_, buffer, nbytes);
}

}  // namespace sm
}  // namespace tiledb
