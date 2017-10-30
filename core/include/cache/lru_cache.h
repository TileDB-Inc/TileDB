/**
 * @file   lru_cache.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class LRUCache.
 */

#ifndef TILEDB_LRU_CACHE_H
#define TILEDB_LRU_CACHE_H

#include "status.h"

#include <list>
#include <map>
#include <mutex>

namespace tiledb {

/**
 * Implements an LRU cache of opaque (`void*`) objects that can be located via
 * a string key. This class is thread-safe, providing also thread-safe
 * copying of portions of the opaque objects. Note that, after inserting
 * an object into the cache, the cache **owns** the object and will delete
 * it upon eviction.
 */
class LRUCache {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  struct LRUCacheItem {
    /** The object lable. */
    std::string key_;
    /** The opaque object. */
    void* object_;
    /** The object size. */
    uint64_t size_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor.
   *
   * @param size The maximum cache size.
   */
  explicit LRUCache(uint64_t max_size);

  /** Destructor. */
  ~LRUCache();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Inserts an object with a given key and size into the cache. If an object
   * exists with the same key, the new object overwrites the old one.
   *
   * @param key The key that describes the inserted object.
   * @param object The opaque object to be stored.
   * @param size The size of the object.
   * @return Status
   */
  Status insert(const std::string& key, void* object, uint64_t size);

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
      void* buffer,
      uint64_t offset,
      uint64_t nbytes,
      bool* success);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** A logical clock, which ticks upon every object insertion or reference. */
  uint64_t clock_;

  /**
   * Doubly-connected linked list of cache items. The head of the list is the
   * next item to be evicted.
   */
  std::list<LRUCacheItem> item_ll_;

  /** Maps a key label to an iterator (list node of) of `item_ll_`. */
  std::map<std::string, std::list<LRUCacheItem>::iterator> item_map_;

  /** The maximum cache size. */
  uint64_t max_size_;

  /** The mutex for thread-safety. */
  std::mutex mtx_;

  /** The current cache size. */
  uint64_t size_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Evicts the next object. */
  void evict();
};

}  // namespace tiledb

#endif  // TILEDB_LRU_CACHE_H
