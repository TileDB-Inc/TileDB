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

void QueryBufferCache::adjust_offsets(uint64_t cached_bytes) {
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

QueryRemoteBufferStorage::QueryRemoteBufferStorage(
    Query& query, std::unordered_map<std::string, QueryBuffer>& buffers)
    : query_buffers_(buffers)
    , cell_num_per_tile_(
          query.array_schema().dense() ?
              query.array_schema().domain().cell_num_per_tile() :
              query.array_schema().capacity()) {
  for (const auto& name : query.buffer_names()) {
    bool is_var = query.array_schema().var_size(name);
    uint64_t data_size = datatype_size(query.array_schema().type(name));
    uint64_t cell_size = is_var ? constants::cell_var_offset_size : data_size;
    bool is_nullable = query.array_schema().is_nullable(name);

    query_buffer_caches_.emplace(
        name,
        QueryBufferCache(cell_num_per_tile_, cell_size, is_var, is_nullable));
  }
}

void QueryRemoteBufferStorage::cache_non_tile_aligned_data() {
  for (auto& query_buffer_cache : query_buffer_caches_) {
    const auto& query_buffer = query_buffers_[query_buffer_cache.first];
    auto& cache = query_buffer_cache.second;
    uint64_t cache_cells = cache.fixed_bytes_to_cache_ / cache.cell_size_;

    // Cache fixed data.
    cache.buffer_.reset_size();
    throw_if_not_ok(cache.buffer_.write(
        (char*)query_buffer.buffer_ + *query_buffer.buffer_size_,
        cache.fixed_bytes_to_cache_));

    // Cache var data.
    if (query_buffer.buffer_var_size_ != nullptr) {
      cache.buffer_var_.reset_size();
      uint64_t shift_var_bytes = query_buffer.original_buffer_var_size_ -
                                 *query_buffer.buffer_var_size_;
      throw_if_not_ok(cache.buffer_var_.write(
          (char*)query_buffer.buffer_var_ + *query_buffer.buffer_var_size_,
          shift_var_bytes));

      // Ensure cached offsets ascend from 0.
      if (cache_cells > 0) {
        auto* data = cache.buffer_.data_as<uint64_t>();
        uint64_t diff = data[0];
        for (uint64_t i = 0; i < cache_cells; i++) {
          data[i] -= diff;
        }
      }
    }

    // Cache validity data.
    if (query_buffer.validity_vector_.buffer_size() != nullptr) {
      cache.buffer_validity_.reset_size();
      throw_if_not_ok(cache.buffer_validity_.write(
          query_buffer.validity_vector_.buffer() +
              *query_buffer.validity_vector_.buffer_size(),
          cache_cells));
    }
  }
}

bool QueryRemoteBufferStorage::should_cache_write() {
  for (const auto& query_buffer_cache : query_buffer_caches_) {
    uint64_t query_buffer_size =
        *query_buffers_[query_buffer_cache.first].buffer_size_;
    uint64_t buffer_cells =
        query_buffer_size / query_buffer_cache.second.cell_size_;
    uint64_t cache_buffer_cells = query_buffer_cache.second.buffer_.size() /
                                  query_buffer_cache.second.cell_size_;

    if (buffer_cells + cache_buffer_cells <= cell_num_per_tile_) {
      return true;
    }
  }

  return false;
}

void QueryRemoteBufferStorage::cache_write() {
  for (auto& query_buffer_cache : query_buffer_caches_) {
    auto query_buffer = query_buffers_[query_buffer_cache.first];
    auto& cache = query_buffer_cache.second;

    // Buffer element size, or size of offsets if attribute is var-sized.
    uint64_t buffer_cells = *query_buffer.buffer_size_ / cache.cell_size_;
    uint64_t buffer_cache_bytes = buffer_cells * cache.cell_size_;

    // Write fixed attribute data into cache buffer.
    throw_if_not_ok(
        cache.buffer_.write(query_buffer.buffer_, buffer_cache_bytes));
    *query_buffer.buffer_size_ -= buffer_cache_bytes;

    if (cache.is_var_) {
      // Adjust appended offsets to be relative to already cached data.
      cache.adjust_offsets(buffer_cache_bytes);

      // Write the variable sized data into cache.
      throw_if_not_ok(cache.buffer_var_.write(
          query_buffer.buffer_var_, *query_buffer.buffer_var_size_));
      *query_buffer.buffer_var_size_ -= *query_buffer.buffer_var_size_;
    }

    if (cache.is_nullable_) {
      // Write the validity buffers into cache.
      throw_if_not_ok(cache.buffer_validity_.write(
          query_buffer.validity_vector_.buffer(), buffer_cells));
      *query_buffer.validity_vector_.buffer_size() -= buffer_cells;
    }
  }
}

void QueryRemoteBufferStorage::make_buffers_tile_aligned() {
  for (auto& query_buffer_cache : query_buffer_caches_) {
    const auto& query_buffer = query_buffers_[query_buffer_cache.first];
    auto& cache = query_buffer_cache.second;

    // Buffer element size, or size of offsets if attribute is var-sized.
    uint64_t buffer_cells = *query_buffer.buffer_size_ / cache.cell_size_;
    uint64_t cache_buffer_cells = cache.buffer_.size() / cache.cell_size_;
    uint64_t total_buffer_cells = buffer_cells + cache_buffer_cells;
    // Check if data between cache and user buffer is tile-aligned.
    if (total_buffer_cells % cell_num_per_tile_ == 0) {
      // Submit all data and cache nothing after submit.
      cache.fixed_bytes_to_cache_ = 0;
      continue;
    }
    uint64_t buffer_cache_bytes =
        (total_buffer_cells % cell_num_per_tile_) * cache.cell_size_;

    // Hold tile overflow cells from this submission and cache afterwards.
    *query_buffer.buffer_size_ -= buffer_cache_bytes;
    cache.fixed_bytes_to_cache_ = buffer_cache_bytes;
    if (cache.is_var_) {
      uint64_t var_buffer_size = *query_buffer.buffer_var_size_;
      uint64_t first_cached_offset = ((uint64_t*)query_buffer.buffer_)
          [buffer_cells - (buffer_cache_bytes / cache.cell_size_)];

      // Use the first cached offset to calculate unaligned var bytes.
      uint64_t var_data_unaligned_bytes = var_buffer_size - first_cached_offset;
      *query_buffer.buffer_var_size_ -= var_data_unaligned_bytes;
    }

    if (cache.is_nullable_) {
      *query_buffer.validity_vector_.buffer_size() -=
          buffer_cache_bytes / cache.cell_size_;
    }
  }
}

const QueryBufferCache& QueryRemoteBufferStorage::get_query_buffer_cache(
    const std::string& name) const {
  return query_buffer_caches_.at(name);
}

}  // namespace tiledb::sm
