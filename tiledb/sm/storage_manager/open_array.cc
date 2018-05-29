/**
 * @file   open_array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the OpenArray class.
 */

#include "tiledb/sm/storage_manager/open_array.h"

#include <iostream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

OpenArray::OpenArray(const URI& array_uri)
    : array_uri_(array_uri) {
  array_schema_ = nullptr;
  cnt_ = 0;
  filelock_ = INVALID_FILELOCK;
}

OpenArray::~OpenArray() {
  delete array_schema_;
  for (auto& fragment : fragment_metadata_)
    delete fragment;
}

/* ****************************** */
/*               API              */
/* ****************************** */

ArraySchema* OpenArray::array_schema() const {
  return array_schema_;
}

const URI& OpenArray::array_uri() const {
  return array_schema_->array_uri();
}

uint64_t OpenArray::cnt() const {
  return cnt_;
}

void OpenArray::cnt_decr() {
  --cnt_;
}

void OpenArray::cnt_incr() {
  ++cnt_;
}

Status OpenArray::file_lock(VFS* vfs, FilelockType lock_type) {
  auto uri = array_uri_.join_path(constants::array_filelock);
  auto lt = (lock_type == SLOCK) ? VFS::SLOCK : VFS::XLOCK;
  if (filelock_ == INVALID_FILELOCK)
    RETURN_NOT_OK(vfs->filelock_lock(uri, &filelock_, lt));

  return Status::Ok();
}

Status OpenArray::file_unlock(VFS* vfs) {
  auto uri = array_uri_.join_path(constants::array_filelock);
  if (filelock_ != INVALID_FILELOCK)
    RETURN_NOT_OK(vfs->filelock_unlock(uri, filelock_));
  filelock_ = INVALID_FILELOCK;

  return Status::Ok();
}

const std::vector<FragmentMetadata*>& OpenArray::fragment_metadata() const {
  return fragment_metadata_;
}

FragmentMetadata* OpenArray::fragment_metadata_get(
    const URI& fragment_uri) const {
  auto it = fragment_metadata_map_.find(fragment_uri.to_string());
  if (it == fragment_metadata_map_.end())
    return nullptr;

  return it->second;
}

bool OpenArray::fragment_metadata_empty() const {
  return fragment_metadata_.empty();
}

void OpenArray::mtx_lock() {
  mtx_.lock();
}

void OpenArray::mtx_unlock() {
  mtx_.unlock();
}

void OpenArray::set_array_schema(ArraySchema* array_schema) {
  array_schema_ = array_schema;
}

void OpenArray::set_fragment_metadata(
    const std::vector<FragmentMetadata*>& metadata) {
  fragment_metadata_ = metadata;

  fragment_metadata_map_.clear();
  for (const auto& f : metadata)
    fragment_metadata_map_[f->fragment_uri().to_string()] = f;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

}  // namespace sm
}  // namespace tiledb
