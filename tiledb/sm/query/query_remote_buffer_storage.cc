/**
 * @file query_remote_buffer_storage.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/query/query.h"

namespace tiledb::sm {
class QueryRemoteBufferStorageException : public StatusException {
 public:
  explicit QueryRemoteBufferStorageException(const std::string& msg)
      : StatusException("QueryRemoteBufferStorage", msg) {
  }
};

/* ***************************** */
/*         BufferCache           */
/* ***************************** */

void BufferCache::adjust_offsets(uint64_t cached_bytes) {
  if (buffer_.size() < cached_bytes) {
    throw QueryRemoteBufferStorageException(
        "Attempt to correct offsets outside of fixed buffer bounds.");
  }
  // Adjust any appended offsets to be relative to already cached data.
  for (uint64_t pos = buffer_.size() - cached_bytes; pos < buffer_.size();
       pos += cell_size_) {
    *(uint64_t*)buffer_.data(pos) += buffer_var_.size();
  }
}

/* ***************************** */
/*   QueryRemoteBufferStorage    */
/* ***************************** */

QueryRemoteBufferStorage::QueryRemoteBufferStorage(Query* query)
    : query_(query) {
  if (!query_->array()->is_remote()) {
    return;
  }
  cell_num_per_tile_ = query_->array_schema().dense() ?
                           query_->array_schema().domain().cell_num_per_tile() :
                           query_->array_schema().capacity();

  // Only allocate cache buffers for remote global order writes.
  // For other types or layouts the keys will exist with empty buffers.
  if (query_->type() == QueryType::READ ||
      query_->layout() != Layout::GLOBAL_ORDER) {
    cell_num_per_tile_ = 0;
  }

  for (const auto& attr : query_->array_schema().attributes()) {
    std::string name = attr->name();
    bool is_var = query_->array_schema().var_size(name);
    bool is_nullable = query_->array_schema().is_nullable(name);
    uint64_t data_size = datatype_size(query_->array_schema().type(name));
    uint64_t cell_size = is_var ? constants::cell_var_offset_size : data_size;

    caches_[name].cell_size_ = cell_size;
    // Construct and move a preallocated Buffer to initialize cache.
    caches_[name].buffer_ = {cell_num_per_tile_ * cell_size};
    if (is_nullable) {
      caches_[name].buffer_validity_ = {cell_num_per_tile_};
    }
  }

  // Initialize coordinate buffers for sparse arrays.
  if (!query_->is_dense()) {
    for (const auto& name : query_->array_schema().dim_names()) {
      uint64_t cell_size = datatype_size(query_->array_schema().type(name));
      caches_[name].cell_size_ = cell_size;
      caches_[name].buffer_ = {cell_num_per_tile_ * cell_size};
    }
  }
}

void QueryRemoteBufferStorage::cache_non_tile_aligned_data() {
  if (query_->type() != QueryType::WRITE) {
    return;
  }

  for (const auto& name : query_->buffer_names()) {
    auto query_buffer = query_->buffer(name);
    auto& cache = caches_[name];
    uint64_t cache_cells = cache.cache_bytes_ / cache.cell_size_;

    cache.buffer_.reset_size();
    throw_if_not_ok(cache.buffer_.write(
        (char*)query_buffer.buffer_ + *query_buffer.buffer_size_,
        cache.cache_bytes_));

    if (query_buffer.buffer_var_size_ != nullptr) {
      cache.buffer_var_.reset_size();
      uint64_t shift_var_bytes = query_buffer.original_buffer_var_size_ -
                                 *query_buffer.buffer_var_size_;
      throw_if_not_ok(cache.buffer_var_.write(
          (char*)query_buffer.buffer_var_ + *query_buffer.buffer_var_size_,
          shift_var_bytes));

      // Ensure cached offsets ascend from 0.
      if (cache.cache_bytes_ > 0) {
        auto* data = cache.buffer_.data_as<uint64_t>();
        std::adjacent_difference(data, data + cache_cells, data);
        data[0] = 0;
        for (uint64_t i = 1; i < cache_cells; i++) {
          data[i] += data[i - 1];
        }
      }
    }

    if (query_buffer.validity_vector_.buffer_size() != nullptr) {
      cache.buffer_validity_.reset_size();
      throw_if_not_ok(cache.buffer_validity_.write(
          query_buffer.validity_vector_.buffer() +
              *query_buffer.validity_vector_.buffer_size(),
          cache.cache_bytes_ / cache.cell_size_));
    }
  }
}

bool QueryRemoteBufferStorage::should_cache_write() {
  // Only applicable for remote global order writes
  if (query_->type() != QueryType::WRITE ||
      query_->layout() != Layout::GLOBAL_ORDER) {
    return false;
  }

  bool cache_write = false;
  for (const auto& cache : caches_) {
    uint64_t query_buffer_size = *query_->buffer(cache.first).buffer_size_;
    uint64_t buffer_cells = query_buffer_size / cache.second.cell_size_;
    uint64_t cache_buffer_cells =
        cache.second.buffer_.size() / cache.second.cell_size_;

    if (buffer_cells + cache_buffer_cells <= cell_num_per_tile_) {
      cache_write = true;
    }
  }

  return cache_write;
}

void QueryRemoteBufferStorage::cache_write() {
  for (const auto& name : query_->buffer_names()) {
    auto query_buffer = query_->buffer(name);
    bool is_var = query_->array_schema().var_size(name);
    bool is_nullable = query_->array_schema().is_nullable(name);
    auto& cache = caches_[name];

    // Buffer element size, or size of offsets if attribute is var-sized.
    uint64_t buffer_cells = *query_buffer.buffer_size_ / cache.cell_size_;
    uint64_t buffer_cache_bytes = buffer_cells * cache.cell_size_;

    // Write fixed attribute data into cache buffer.
    throw_if_not_ok(
        cache.buffer_.write(query_buffer.buffer_, buffer_cache_bytes));
    *query_buffer.buffer_size_ -= buffer_cache_bytes;

    if (is_var) {
      // Adjust appended offsets to be relative to already cached data.
      cache.adjust_offsets(buffer_cache_bytes);

      // Write the variable sized data into cache.
      throw_if_not_ok(cache.buffer_var_.write(
          query_buffer.buffer_var_, *query_buffer.buffer_var_size_));
      *query_buffer.buffer_var_size_ -= *query_buffer.buffer_var_size_;
    }

    if (is_nullable) {
      // Write the validity buffers into cache.
      throw_if_not_ok(cache.buffer_validity_.write(
          query_buffer.validity_vector_.buffer(), buffer_cells));
      *query_buffer.validity_vector_.buffer_size() -= buffer_cells;
    }
  }
}

void QueryRemoteBufferStorage::make_buffers_tile_aligned() {
  // Only applicable for remote global order writes
  if (query_->type() != QueryType::WRITE ||
      query_->layout() != Layout::GLOBAL_ORDER) {
    return;
  }

  for (auto& cache : caches_) {
    const auto& query_buffer = query_->buffer(cache.first);
    bool is_var = query_->array_schema().var_size(cache.first);
    bool is_nullable = query_->array_schema().is_nullable(cache.first);

    // Buffer element size, or size of offsets if attribute is var-sized.
    uint64_t buffer_cells =
        *query_buffer.buffer_size_ / cache.second.cell_size_;
    uint64_t cache_buffer_cells =
        cache.second.buffer_.size() / cache.second.cell_size_;
    uint64_t total_buffer_cells = buffer_cells + cache_buffer_cells;

    // If buffer_cells does not fill a tile, cache the entire buffer.
    uint64_t tile_cache_cells = buffer_cells;
    uint64_t buffer_cache_bytes = tile_cache_cells * cache.second.cell_size_;
    // Check if data between cache and user buffer is tile-aligned.
    if (total_buffer_cells >= cell_num_per_tile_ &&
        total_buffer_cells % cell_num_per_tile_ == 0) {
      // Submit all data and cache nothing after submit.
      cache.second.cache_bytes_ = 0;
      continue;
    }

    if (buffer_cells >= cell_num_per_tile_) {
      // The user buffer alone contains enough data to fill one or more tiles.
      tile_cache_cells =
          (cache_buffer_cells + buffer_cells) % cell_num_per_tile_;
      buffer_cache_bytes = tile_cache_cells * cache.second.cell_size_;
    } else if (total_buffer_cells >= cell_num_per_tile_) {
      // The user buffer and cache data combined can fill one or more tiles.
      tile_cache_cells = cell_num_per_tile_ % cache_buffer_cells;
      buffer_cache_bytes =
          (buffer_cells - tile_cache_cells) * cache.second.cell_size_;
    }

    // Hold tile overflow cells from this submission and cache afterwards.
    *query_buffer.buffer_size_ -= buffer_cache_bytes;
    cache.second.cache_bytes_ = buffer_cache_bytes;
    if (is_var) {
      uint64_t var_buffer_size = *query_buffer.buffer_var_size_;
      uint64_t first_cached_offset = ((uint64_t*)query_buffer.buffer_)
          [buffer_cells - (buffer_cache_bytes / cache.second.cell_size_)];

      // Use the first cached offset to calculate unaligned var bytes.
      uint64_t var_data_unaligned_bytes = var_buffer_size - first_cached_offset;
      *query_buffer.buffer_var_size_ -= var_data_unaligned_bytes;
    }
    if (is_nullable) {
      *query_buffer.validity_vector_.buffer_size() -=
          buffer_cache_bytes / cache.second.cell_size_;
    }
  }
}

const BufferCache& QueryRemoteBufferStorage::get_cache(
    const std::string& name) const {
  if (caches_.count(name) == 0) {
    throw QueryRemoteBufferStorageException(
        "BufferCache with name '" + name + "' does not exist.");
  }
  return caches_.at(name);
}

}  // namespace tiledb::sm
