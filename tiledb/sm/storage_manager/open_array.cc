/**
 * @file   open_array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/constants.h"

#include <cassert>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

OpenArray::OpenArray(const URI& array_uri, QueryType query_type)
    : array_uri_(array_uri)
    , query_type_(query_type) {
  array_schema_ = nullptr;
  cnt_ = 0;
  filelock_ = INVALID_FILELOCK;
}

OpenArray::~OpenArray() {
  tdb_delete(array_schema_);
  fragment_metadata_.clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

ArraySchema* OpenArray::array_schema() const {
  return array_schema_;
}

std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>&
OpenArray::array_schemas() {
  return array_schemas_;
}

Status OpenArray::get_array_schema(
    const std::string& schema_name, tdb_shared_ptr<ArraySchema>* array_schema) {
  (*array_schema) = (array_schemas_.find(schema_name) == array_schemas_.end()) ?
                        tdb_shared_ptr<ArraySchema>(nullptr) :
                        array_schemas_[schema_name];
  return Status::Ok();
}

const URI& OpenArray::array_uri() const {
  return array_uri_;
}

Status OpenArray::set_encryption_key(const EncryptionKey& encryption_key) {
  return key_validation_.check_encryption_key(encryption_key);
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

bool OpenArray::is_empty(uint64_t timestamp) const {
  std::lock_guard<std::mutex> lock(local_mtx_);
  return fragment_metadata_.empty() ||
         (*fragment_metadata_.cbegin())->first_timestamp() > timestamp;
}

Status OpenArray::file_lock(VFS* vfs) {
  auto uri = array_uri_.join_path(constants::filelock_name);
  if (filelock_ == INVALID_FILELOCK)
    RETURN_NOT_OK(vfs->filelock_lock(uri, &filelock_, true));

  return Status::Ok();
}

Status OpenArray::file_unlock(VFS* vfs) {
  auto uri = array_uri_.join_path(constants::filelock_name);
  if (filelock_ != INVALID_FILELOCK)
    RETURN_NOT_OK(vfs->filelock_unlock(uri));
  filelock_ = INVALID_FILELOCK;

  return Status::Ok();
}

tdb_shared_ptr<FragmentMetadata> OpenArray::fragment_metadata(
    const URI& uri) const {
  std::lock_guard<std::mutex> lock(local_mtx_);
  auto it = fragment_metadata_set_.find(uri.to_string());
  return (it == fragment_metadata_set_.end()) ? nullptr : it->second;
}

OpenArrayMemoryTracker* OpenArray::memory_tracker() {
  return &memory_tracker_;
}

tdb_shared_ptr<Buffer> OpenArray::array_metadata(const URI& uri) const {
  std::lock_guard<std::mutex> lock(local_mtx_);
  auto it = array_metadata_.find(uri.to_string());
  return (it == array_metadata_.end()) ? nullptr : it->second;
}

void OpenArray::mtx_lock() {
  mtx_.lock();
}

void OpenArray::mtx_unlock() {
  mtx_.unlock();
}

QueryType OpenArray::query_type() const {
  return query_type_;
}

void OpenArray::set_array_schema(ArraySchema* array_schema) {
  array_schema_ = array_schema;
}

void OpenArray::insert_fragment_metadata(
    tdb_shared_ptr<FragmentMetadata> metadata) {
  std::lock_guard<std::mutex> lock(local_mtx_);
  assert(metadata != nullptr);
  fragment_metadata_.insert(metadata);
  fragment_metadata_set_[metadata->fragment_uri().to_string()] = metadata;
}

void OpenArray::insert_array_metadata(
    const URI& uri, const tdb_shared_ptr<Buffer>& metadata) {
  std::lock_guard<std::mutex> lock(local_mtx_);
  assert(metadata != nullptr);
  array_metadata_[uri.to_string()] = metadata;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

}  // namespace sm
}  // namespace tiledb
