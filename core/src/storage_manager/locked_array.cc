/**
 * @file   locked_array.cc
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
 * This file implements the LockedArray class.
 */

#include "locked_array.h"

#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

LockedArray::LockedArray() {
  shared_locks_ = 0;
  total_locks_ = 0;
  filelock_ = -1;
  exclusive_lock_ = false;
}

/* ****************************** */
/*               API              */
/* ****************************** */

void LockedArray::decr_total_locks() {
  --total_locks_;
}

void LockedArray::incr_total_locks() {
  ++total_locks_;
}

Status LockedArray::lock(VFS* vfs, const URI& uri, bool shared) {
  return shared ? lock_shared(vfs, uri) : lock_exclusive(vfs, uri);
}

bool LockedArray::no_locks() const {
  return total_locks_ == 0;
}

Status LockedArray::unlock(VFS* vfs, const URI& uri, bool shared) {
  return shared ? unlock_shared(vfs, uri) : unlock_exclusive(vfs, uri);
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status LockedArray::lock_exclusive(VFS* vfs, const URI& uri) {
  std::unique_lock<std::mutex> lk(mtx_);
  cv_.wait(lk, [this] { return !exclusive_lock_ && (shared_locks_ == 0); });

  if (filelock_ == -1)
    RETURN_NOT_OK(vfs->filelock_lock(uri, &filelock_, false));

  exclusive_lock_ = true;
  lk.unlock();

  return Status::Ok();
}

Status LockedArray::lock_shared(VFS* vfs, const URI& uri) {
  std::unique_lock<std::mutex> lk(mtx_);
  cv_.wait(lk, [this] { return !exclusive_lock_; });

  if (filelock_ == -1)
    RETURN_NOT_OK(vfs->filelock_lock(uri, &filelock_, true));

  ++shared_locks_;
  lk.unlock();

  return Status::Ok();
}

Status LockedArray::unlock_exclusive(VFS* vfs, const URI& uri) {
  if (filelock_ != -1)
    RETURN_NOT_OK(vfs->filelock_unlock(uri, filelock_));
  filelock_ = -1;

  exclusive_lock_ = false;
  cv_.notify_all();

  return Status::Ok();
}

Status LockedArray::unlock_shared(VFS* vfs, const URI& uri) {
  if (filelock_ != -1)
    RETURN_NOT_OK(vfs->filelock_unlock(uri, filelock_));
  filelock_ = -1;

  --shared_locks_;
  if (shared_locks_ == 0)
    cv_.notify_one();

  return Status::Ok();
}

}  // namespace tiledb