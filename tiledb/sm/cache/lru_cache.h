/**
 * @file   lru_cache.h
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
 * This file defines class LRUCache.
 */

#ifndef TILEDB_LRU_CACHE_H
#define TILEDB_LRU_CACHE_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/misc/status.h"

#include <list>
#include <map>
#include <mutex>

namespace tiledb {
namespace sm {

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
   * @param evict_callback The function to be called upon evicting a cache
   *     object. It takes as input the cache object to be evicted, and
   *     `evict_callback_data`.
   * @param evict_callback_data The data input to `evict_callback`.
   */
  LRUCache(
      uint64_t max_size,
      void* (*evict_callback)(LRUCacheItem*, void*) = nullptr,
      void* evict_callback_data = nullptr);

  /** Destructor. */
  ~LRUCache();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Clears the cache, deleting all cached items. */
  void clear();

  /**
   * Returns a constant iterator at the beginning of the linked list of
   * cached items, where items closest to the head (beginning) are going
   * to be evicted from the cache sooner.
   */
  std::list<LRUCacheItem>::const_iterator item_iter_begin() const;

  /**
   * Returns a constant iterator at the end of the linked list of
   * cached items, where items closest to the head (beginning) are going
   * to be evicted from the cache sooner.
   */
  std::list<LRUCacheItem>::const_iterator item_iter_end() const;

  /**
   * Inserts an object with a given key and size into the cache. Note that
   * the cache *owns* the object after insertion.
   *
   * @param key The key that describes the inserted object.
   * @param object The opaque object to be stored.
   * @param size The size of the object.
   * @param overwrite If `true`, if the object exists in the cache it will be
   *     overwritten. Otherwise, the new object will be deleted.
   * @return Status
   */
  Status insert(
      const std::string& key,
      void* object,
      uint64_t size,
      bool overwrite = true);

  /**
   * Invalidates and evicts the object in the cache with the given key.
   *
   * @param key The key that describes the object to be invalidated.
   * @param success Set to `true` if the object was removed successfully; if
   *    the object did not exist in the cache, set to `false`.
   * @return Status
   */
  Status invalidate(const std::string& key, bool* success);

  /** Returns the current size of cache in bytes. */
  uint64_t size() const;

  /** Returns the maximum size of the cache in bytes. */
  uint64_t max_size() const;

  /**
   * Reads an entire cached object labeled by `key`.
   *
   * @param key The label of the object to be read.
   * @param buffer The buffer that will store the data to be read. It will be
   *     resized appropriately.
   * @param success `true` if the data were read from the cache and `false`
   *     otherwise.
   * @return Status.
   */
  Status read(const std::string& key, Buffer* buffer, bool* success);

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
      Buffer* buffer,
      uint64_t offset,
      uint64_t nbytes,
      bool* success);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * A function that will be called upon evicting a cache object. I takes
   * as input the cache object to be evicted and `evict_callback_data_`.
   */
  void* (*evict_callback_)(LRUCacheItem*, void*);

  /** The data input to the evict callback function. */
  void* evict_callback_data_;

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

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_LRU_CACHE_H
