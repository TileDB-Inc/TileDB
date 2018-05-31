/**
 * @file   kv_iter.cc
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
 * This file implements class KV.
 */

#include "tiledb/sm/kv/kv_iter.h"
#include "tiledb/sm/misc/logger.h"

#include <iostream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

KVIter::KVIter(StorageManager* storage_manager)
    : storage_manager_(storage_manager) {
  query_ = nullptr;
  coords_buffer_ = nullptr;
  coords_buffer_size_ = 0;
  current_item_ = 0;
  kv_ = nullptr;
  status_ = QueryStatus::COMPLETED;
  item_num_ = 0;
  max_item_num_ = constants::kv_max_items;
}

KVIter::~KVIter() {
  clear();
}

/* ********************************* */
/*                API                */
/* ********************************* */

bool KVIter::done() const {
  return (status_ == QueryStatus::COMPLETED) && (current_item_ == item_num_);
}

Status KVIter::here(KVItem** kv_item) const {
  if (done())
    return Status::Ok();

  KVItem::Hash hash;
  hash.first = coords_buffer_[2 * current_item_];
  hash.second = coords_buffer_[2 * current_item_ + 1];

  return kv_->get_item(hash, kv_item);
}

Status KVIter::init(KV* kv) {
  kv_ = kv;

  coords_buffer_ = new (std::nothrow) uint64_t[2 * max_item_num_];
  if (coords_buffer_ == nullptr)
    return LOG_STATUS(Status::KVIterError(
        "Cannot initialize kv iterator; Memory allocation failed"));

  RETURN_NOT_OK(
      storage_manager_->query_create(&query_, kv_->open_array_for_reads()));
  RETURN_NOT_OK(submit_read_query());

  return Status::Ok();
}

Status KVIter::next() {
  current_item_++;

  if (current_item_ == item_num_ && status_ == QueryStatus::INCOMPLETE)
    return submit_read_query();

  return Status::Ok();
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

void KVIter::clear() {
  kv_ = nullptr;
  delete query_;
  query_ = nullptr;
  delete[] coords_buffer_;
  coords_buffer_ = nullptr;
  coords_buffer_size_ = 0;
  current_item_ = 0;
  status_ = QueryStatus::INPROGRESS;
  max_item_num_ = 0;
  item_num_ = 0;
}

Status KVIter::submit_read_query() {
  // Always reset the size of the read buffer and offset
  coords_buffer_size_ = 2 * max_item_num_ * sizeof(uint64_t);
  current_item_ = 0;

  RETURN_NOT_OK(query_->set_buffer(
      constants::coords.c_str(), coords_buffer_, &coords_buffer_size_));
  RETURN_NOT_OK(storage_manager_->query_submit(query_));

  status_ = query_->status();
  item_num_ = coords_buffer_size_ / (2 * sizeof(uint64_t));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
