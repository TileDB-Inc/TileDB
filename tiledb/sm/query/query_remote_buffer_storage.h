/**
 * @file query_remote_buffer_storage.h
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

#ifndef TILEDB_QUERY_REMOTE_BUFFER_H
#define TILEDB_QUERY_REMOTE_BUFFER_H

#include <numeric>

#include "tiledb/sm/buffer/buffer.h"

using namespace tiledb::common;

namespace tiledb::sm {
class Query;
class QueryBuffer;

class QueryBufferCache {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor for fixed / var-size data. */
  QueryBufferCache(
      uint64_t cell_num_per_tile,
      uint64_t cell_size,
      bool is_var,
      bool is_nullable)
      : is_var_(is_var)
      , is_nullable_(is_nullable)
      , cell_size_(cell_size)
      , fixed_bytes_to_cache_(0)
      , buffer_(cell_num_per_tile * cell_size_)
      , buffer_validity_(is_nullable ? cell_num_per_tile : 0) {
  }

  /* ********************************* */
  /*           PUBLIC METHODS          */
  /* ********************************* */

  /**
   * Adjust offsets added to the cache to ensure ascending order.
   *
   * @param cached_bytes The number of bytes added to the offset buffer.
   */
  void adjust_offsets(uint64_t cached_bytes);

  /* ********************************* */
  /*          PUBLIC ATTRIBUTES        */
  /* ********************************* */

  /** True if the cache represents var-size data. */
  bool is_var_;

  /** True if the cache represents nullable data. */
  bool is_nullable_;

  /** Size of a single cell stored in fixed buffer. */
  uint8_t cell_size_;

  /** Tile overflow fixed bytes to write to cache after submission. */
  uint64_t fixed_bytes_to_cache_;

  Buffer buffer_;
  Buffer buffer_var_;
  Buffer buffer_validity_;
};

class QueryRemoteBufferStorage {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  QueryRemoteBufferStorage(
      Query& query, std::unordered_map<std::string, QueryBuffer>& buffers);

  ~QueryRemoteBufferStorage() = default;

  /* ********************************* */
  /*           PUBLIC METHODS          */
  /* ********************************* */

  /**
   * Updates cache with any unsubmitted data from a previous unaligned write.
   */
  void cache_non_tile_aligned_data();

  /**
   * Checks if user buffer and cached data is smaller than one tile.
   *
   * @return True if we should cache all buffers from this write.
   */
  bool should_cache_write();

  /**
   * Caches all data in user buffers for this write.
   */
  void cache_write();

  /**
   * Adjust user buffer sizes to be tile aligned if they are not already.
   */
  void make_buffers_tile_aligned();

  /**
   * Retrieve QueryBufferCache associated with attribute or dimension name.
   *
   * @param name Name of attribute or dimension QueryBufferCache to retrieve.
   * @return Const reference to QueryBufferCache.
   */
  const QueryBufferCache& get_query_buffer_cache(const std::string& name) const;

 private:
  /* ********************************* */
  /*           PRIVATE ATTRIBUTES      */
  /* ********************************* */

  /** Pointer to the query that owns this cache. */
  std::unordered_map<std::string, QueryBuffer>& query_buffers_;

  /** Number of cells in one tile. */
  uint64_t cell_num_per_tile_;

  /** Cache buffers for the query. */
  std::unordered_map<std::string, QueryBufferCache> query_buffer_caches_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_QUERY_REMOTE_BUFFER_H
