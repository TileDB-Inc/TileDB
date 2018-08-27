/**
 * @file   filter_storage.cc
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
 * This file defines class FilterStorage.
 */

#include "tiledb/sm/filter/filter_storage.h"

namespace tiledb {
namespace sm {

std::shared_ptr<Buffer> FilterStorage::get_buffer() {
  if (available_.empty())
    available_.emplace_back(new Buffer());

  std::shared_ptr<Buffer> buf = std::move(available_.front());
  Buffer* buf_ptr = buf.get();
  available_.pop_front();
  in_use_.push_back(std::move(buf));
  in_use_list_map_[buf_ptr] = --(in_use_.end());

  return in_use_.back();
}

uint64_t FilterStorage::num_available() const {
  return available_.size();
}

uint64_t FilterStorage::num_in_use() const {
  return in_use_.size();
}

Status FilterStorage::reclaim(Buffer* buffer) {
  auto it = in_use_list_map_.find(buffer);

  // If the buffer is not managed by this class, do nothing.
  if (it == in_use_list_map_.end())
    return Status::Ok();

  // An "unused" buffer will have exactly one reference, which is the held by
  // the in_use_ list.
  if (it->second->use_count() == 1) {
    buffer->reset_offset();
    buffer->reset_size();

    auto list_node = it->second;
    std::shared_ptr<Buffer> ptr = std::move(*list_node);
    in_use_.erase(list_node);
    in_use_list_map_.erase(it);
    available_.push_front(std::move(ptr));
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb