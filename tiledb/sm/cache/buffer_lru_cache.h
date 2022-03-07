/**
 * @file   buffer_lru_cache.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines class BufferLRUCache.
 */

#ifndef TILEDB_BUFFER_LRU_CACHE_H
#define TILEDB_BUFFER_LRU_CACHE_H

#include "tiledb/common/status.h"
#include "tiledb/sm/cache/lru_cache.h"
#include "tiledb/sm/tile/filtered_buffer.h"

#include <list>
#include <map>
#include <mutex>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;

/**
 * Provides a least-recently used cache for `FilteredBuffer` objects
 * mapped by a `std::string` key. The maximum capacity of the
 * cache is defined as a total allocated byte size among all
 * `FilteredBuffer` objects.
 *
 * This class is thread-safe.
 */
class BufferLRUCache : public LRUCache<std::string, FilteredBuffer> {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param size The maximum cache byte size.
   */
  BufferLRUCache(uint64_t max_size);

  /** Destructor. */
  virtual ~BufferLRUCache() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Inserts an object with a given key and size into the cache. Note that
   * the cache *owns* the object after insertion.
   *
   * @param key The key that describes the inserted object.
   * @param buffer The buffer to store.
   * @param overwrite If `true`, if the object exists in the cache it will be
   *     overwritten. Otherwise, the new object will be deleted.
   * @return Status
   */
  Status insert(
      const std::string& key, FilteredBuffer&& buffer, bool overwrite = true);

  /**
   * Reads a portion of the object labeled by `key`.
   *
   * @param key The label of the object to be read.
   * @param buffer The buffer that will store the data to be read.
   * @param offset The offset where the read will start.
   * @param nbytes The number of bytes to be read.
   * @param success `true` if the data were read from the cache and `false`
   *     otherwise.
   * @return Status.
   */
  Status read(
      const std::string& key,
      FilteredBuffer& buffer,
      uint64_t offset,
      uint64_t nbytes,
      bool* success);

  /** Clears the cache, deleting all cached items. */
  void clear();

  /**
   * Invalidates and evicts the object in the cache with the given key.
   *
   * @param key The key that describes the object to be invalidated.
   * @param success Set to `true` if the object was removed successfully; if
   *    the object did not exist in the cache, set to `false`.
   * @return Status
   */
  Status invalidate(const std::string& key, bool* success);

  /**
   * Returns a constant iterator at the beginning of the linked list of
   * cached items, where items closest to the head (beginning) are going
   * to be evicted from the cache sooner. This is public for unit test
   * purposes only.
   */
  typename std::list<LRUCacheItem>::const_iterator item_iter_begin() const;

  /**
   * Returns a constant iterator at the end of the linked list of
   * cached items, where items closest to the head (beginning) are going
   * to be evicted from the cache sooner. This is public for unit test
   * purposes only.
   */
  typename std::list<LRUCacheItem>::const_iterator item_iter_end() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  // Protects LRUCache routines.
  mutable std::mutex lru_mtx_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BUFFER_LRU_CACHE_H
