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

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

KVIter::KVIter(StorageManager* storage_manager)
    : storage_manager_(storage_manager) {
  query_ = nullptr;
  read_buffers_[0] = nullptr;
  read_buffer_sizes_[0] = 0;
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
  hash.first = read_buffers_[0][2 * current_item_];
  hash.second = read_buffers_[0][2 * current_item_ + 1];

  return kv_->get_item(hash, kv_item);
}

Status KVIter::init(
    const std::string& kv_uri,
    const char** attributes,
    unsigned attribute_num) {
  kv_uri_ = URI(kv_uri);
  kv_ = new (std::nothrow) KV(storage_manager_);
  if (kv_ == nullptr)
    return LOG_STATUS(Status::KVIterError(
        "Cannot initialize key-value iterator; Memory allocation failed"));
  RETURN_NOT_OK(kv_->init(kv_uri, attributes, attribute_num, true));
  RETURN_NOT_OK(init_read_query());
  RETURN_NOT_OK(submit_read_query());

  return Status::Ok();
}

Status KVIter::finalize() {
  auto st = finalize_read_query();
  clear();

  return st;
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
  delete kv_;
  kv_ = nullptr;
  delete query_;
  query_ = nullptr;
  delete[] read_buffers_[0];
  read_buffers_[0] = nullptr;
  read_buffer_sizes_[0] = 0;
  current_item_ = 0;
  status_ = QueryStatus::INPROGRESS;
  max_item_num_ = 0;
  item_num_ = 0;
}

Status KVIter::init_read_query() {
  // Create and init query
  RETURN_NOT_OK(
      storage_manager_->query_init(&query_, kv_uri_.c_str(), QueryType::READ));

  // Set buffers
  read_buffers_[0] = new (std::nothrow) uint64_t[2 * max_item_num_];
  if (read_buffers_[0] == nullptr)
    return LOG_STATUS(Status::KVIterError(
        "Cannot initialize read query; Memory allocation failed"));
  const char* attributes[] = {constants::coords};
  return query_->set_buffers(
      attributes, 1, (void**)read_buffers_, read_buffer_sizes_);
}

Status KVIter::submit_read_query() {
  // Always reset the size of the read buffer and offset
  read_buffer_sizes_[0] = 2 * max_item_num_ * sizeof(uint64_t);
  current_item_ = 0;

  // Submit query
  RETURN_NOT_OK(storage_manager_->query_submit(query_));
  status_ = query_->status();
  item_num_ = read_buffer_sizes_[0] / (2 * sizeof(uint64_t));

  return Status::Ok();
}

Status KVIter::finalize_read_query() {
  if (query_ == nullptr)
    return Status::Ok();

  auto st = storage_manager_->query_finalize(query_);
  delete query_;
  query_ = nullptr;
  return st;
}

}  // namespace sm
}  // namespace tiledb
