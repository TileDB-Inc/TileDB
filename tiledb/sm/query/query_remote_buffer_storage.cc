/**
 * @file query_remote_buffer_storage.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2022 TileDB, Inc.
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
 * This file defines class query_remote_buffer_storage.
 */

#include "tiledb/sm/query/query_remote_buffer_storage.h"

namespace tiledb::sm {
class QueryRemoteBufferStorageException : public StatusException {
 public:
  explicit QueryRemoteBufferStorageException(const std::string& msg)
      : StatusException("QueryRemoteBufferStorage", msg) {
  }
};

uint64_t* QueryRemoteBufferStorage::get_cell_offset(uint64_t cell_index) {
  if (buffer.size() < cell_index * constants::cell_var_offset_size) {
    throw QueryRemoteBufferStorageException(
        "Attempt to retrieve offset beyond cache bounds.");
  }
  return (uint64_t*)buffer.data(cell_index * constants::cell_var_offset_size);
}

void QueryRemoteBufferStorage::update_cache(const QueryBuffer& query_buffer) {
  uint64_t cache_cells = cache_bytes / cell_size;
  buffer.reset_size();
  uint64_t bytes_submitted = query_buffer.original_buffer_size_ - cache_bytes;
  throw_if_not_ok(
      buffer.write((char*)query_buffer.buffer_ + bytes_submitted, cache_bytes));

  if (query_buffer.buffer_var_size_ != nullptr) {
    buffer_var.reset_size();
    uint64_t shift_var_bytes =
        query_buffer.original_buffer_var_size_ - *query_buffer.buffer_var_size_;
    throw_if_not_ok(buffer_var.write(
        (char*)query_buffer.buffer_var_ + *query_buffer.buffer_var_size_,
        shift_var_bytes));

    // Ensure cached offsets ascend from 0.
    if (cache_bytes > 0) {
      auto* data = buffer.data_as<uint64_t>();
      std::adjacent_difference(data, data + cache_cells, data);
      // Set the first offset to 0 and ensure others are in ascending order.
      data[0] = 0;
      for (uint64_t i = 1; i < cache_cells; i++) {
        data[i] += data[i - 1];
      }
    }
  }

  if (query_buffer.validity_vector_.buffer_size() != nullptr) {
    buffer_validity.reset_size();
    throw_if_not_ok(buffer_validity.write(
        query_buffer.validity_vector_.buffer() + bytes_submitted / cell_size,
        cache_bytes / cell_size));
  }
}

}  // namespace tiledb::sm
