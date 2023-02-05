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
struct BufferCache {
  Buffer buffer_;
  Buffer buffer_var_;
  Buffer buffer_validity_;

  /** Size of a single cell stored in fixed buffer. */
  uint8_t cell_size_;

  /** Tile overflow fixed bytes to write to cache after submission. */
  uint64_t cache_bytes_;

  /**
   * Adjust offsets added to the cache to ensure ascending order.
   *
   * @param cached_bytes The number of bytes added to the offset buffer.
   */
  void adjust_offsets(uint64_t cached_bytes);
};

class QueryRemoteBufferStorage {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  QueryRemoteBufferStorage(Query* query);

  ~QueryRemoteBufferStorage() = default;

  /* ********************************* */
  /*           PUBLIC METHODS          */
  /* ********************************* */

  /**
   * Updates cache with any unsubmitted data from a previous unaligned write.
   *
   * @param query The Query to cache unaligned buffer data.
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
   * @param name Name of attribute or dimension BufferCache to retrieve.
   * @return Const reference to BufferCache. Throws if the cache doesn't exist.
   */
  const BufferCache& get_cache(const std::string& name) const;

  /**
   * Checks if this storage has already been allocated.
   *
   * @return True if the buffer storage has been allocated.
   */
  inline bool is_allocated() const {
    return caches_.begin()->second.buffer_.alloced_size() > 0;
  }

 private:
  /* ********************************* */
  /*           PRIVATE ATTRIBUTES      */
  /* ********************************* */

  /** Number of cells in one tile. */
  uint64_t cell_num_per_tile_;

  /** Pointer to the query that owns this cache. */
  Query* query_;

  /** Cache buffers for the query. */
  std::unordered_map<std::string, BufferCache> caches_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_QUERY_REMOTE_BUFFER_H
