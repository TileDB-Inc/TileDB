/**
 * @file   lru_cache.h
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
 * This file defines class LRUCache.
 */

#ifndef TILEDB_LRU_CACHE_H
#define TILEDB_LRU_CACHE_H

#include "tiledb/common/assert.h"
#include "tiledb/common/macros.h"

#include <list>
#include <mutex>
#include <unordered_map>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A base class for implementing an LRU. This maps a unique
 * key to a value. The LRU takes ownership of the objects
 * stored in the cache.
 *
 * This class is not thread-safe.
 *
 * @tparam K the type of the key.
 * @tparam V the type of the value.
 */
template <typename K, typename V>
class LRUCache {
 public:
  /* ********************************* */
  /*       PUBLIC DATA STRUCTURES      */
  /* ********************************* */

  /**
   * The internal data structure stored on the cache. This
   * is public for unit test purposes only.
   */
  struct LRUCacheItem {
    /* ********************************* */
    /*            CONSTRUCTORS           */
    /* ********************************* */

    /** Value Constructor. */
    LRUCacheItem(const K& key, V&& object, const uint64_t size)
        : key_(key)
        , object_(std::move(object))
        , size_(size) {
    }

    DISABLE_MOVE(LRUCacheItem);

    /* ********************************* */
    /*             OPERATORS             */
    /* ********************************* */

    /** Move-Assign Operator. */
    LRUCacheItem& operator=(LRUCacheItem&& other) {
      key_ = other.key_;
      object_ = std::move(other.object_);
      size_ = other.size_;
      return *this;
    }

    DISABLE_COPY_AND_COPY_ASSIGN(LRUCacheItem);

    /* ********************************* */
    /*             ATTRIBUTES            */
    /* ********************************* */

    /** The key that maps to the object. */
    K key_;

    /** The object. */
    V object_;

    /** The logical object size. */
    uint64_t size_;
  };

 protected:
  /* ************************************ */
  /* PROTECTED CONSTRUCTORS & DESTRUCTORS */
  /* ************************************ */

  /**
   * Constructor.
   *
   * @param size The maximum logical cache size.
   */
  explicit LRUCache(const uint64_t max_size)
      : max_size_(max_size)
      , size_(0) {
  }

  /** Destructor. */
  virtual ~LRUCache() = default;

  /* ********************************* */
  /*        PROTECTED ROUTINES         */
  /* ********************************* */

  /** Clears the cache, deleting all cached items. */
  void clear() {
    item_ll_.clear();
  }

  /**
   * Inserts an object with a given key and size into the cache. Note that
   * the cache *owns* the object after insertion.
   *
   * @param key The key that describes the inserted object.
   * @param object The opaque object to be stored.
   * @param size The logical size of the object.
   * @param overwrite If `true`, if the object exists in the cache it will be
   *     overwritten. Otherwise, the new object will be deleted.
   */
  void insert(const K& key, V&& object, uint64_t size, bool overwrite = true) {
    // Do nothing if the object size is bigger than the cache maximum size
    if (size > max_size_) {
      return;
    }

    const bool exists = item_map_.count(key) == 1;
    if (exists && !overwrite) {
      return;
    }

    // Evict objects until there is room for `object`. Note that this
    // invalidates the state in `exists`.
    while (size_ + size > max_size_)
      evict();

    // If an object associated with `key` still exists in the cache, replace it.
    // Otherwise, add a new entry in the cache.
    auto item_it = item_map_.find(key);
    if (item_it != item_map_.end()) {
      // Replace cache item
      auto& node = item_it->second;
      auto& item = *node;

      // Replace the object in the cache item.
      item.object_ = std::move(object);

      // Subtract the old size from `size_`.
      size_ -= item.size_;

      // Replace the object size in the cache item.
      item.size_ = size;

      // Move cache item node to the end of the list
      if (std::next(node) != item_ll_.end()) {
        item_ll_.splice(item_ll_.end(), item_ll_, node, std::next(node));
      }
    } else {
      // Create new node in linked list
      item_ll_.emplace_back(key, std::move(object), size);

      // Create new element in the hash table
      item_map_[key] = --(item_ll_.end());
    }

    size_ += size;
  }

  /**
   * Returns true if an item in the cache exists with the given key.
   *
   * @param key The item key.
   * @return bool
   */
  bool has_item(const K& key) {
    return item_map_.count(key) > 0;
  }

  /**
   * Returns the item in the cache associated with `key`. The item
   * will be invalid if later evicted from the cache. The caller
   * must be certain that an item exists for `key`.
   *
   * @param key The item key.
   * @return V* A pointer to the item instance.
   */
  const V* get_item(const K& key) {
    passert(item_map_.count(key) == 1, "count = {}", item_map_.count(key));
    return &item_map_.at(key)->object_;
  }

  /**
   * Touches the item associated with `key` to make it the most
   * recently used item. The caller must be certain that an item
   * exists for `key`.
   *
   * @param key The item key.
   */
  void touch_item(const K& key) {
    auto& item = item_map_.at(key);
    if (std::next(item) != item_ll_.end())
      item_ll_.splice(item_ll_.end(), item_ll_, item, std::next(item));
  }

  /**
   * Invalidates and evicts the object in the cache with the given key.
   *
   * @param key The key that describes the object to be invalidated.
   * @return Return `true` if the object was removed successfully; if
   *    the object did not exist in the cache, return `false`.
   */
  bool invalidate(const K& key) {
    const auto item_it = item_map_.find(key);
    const bool exists = item_it != item_map_.end();
    if (!exists) {
      return false;
    }

    // Move item to the head of the list and evict it.
    auto& node = item_it->second;
    item_ll_.splice(item_ll_.begin(), item_ll_, node);
    evict();
    return true;
  }

  /**
   * Returns a constant iterator at the beginning of the linked list of
   * cached items, where items closest to the head (beginning) are going
   * to be evicted from the cache sooner.
   */
  typename std::list<LRUCacheItem>::const_iterator item_iter_begin() const {
    return item_ll_.cbegin();
  }

  /**
   * Returns a constant iterator at the end of the linked list of
   * cached items, where items closest to the head (beginning) are going
   * to be evicted from the cache sooner.
   */
  typename std::list<LRUCacheItem>::const_iterator item_iter_end() const {
    return item_ll_.cend();
  }

 private:
  /* ********************************* */
  /*         PRIVATE OPERATORS         */
  /* ********************************* */

  DISABLE_COPY_AND_COPY_ASSIGN(LRUCache);
  DISABLE_MOVE_AND_MOVE_ASSIGN(LRUCache);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * Doubly-connected linked list of cache items. The head of the list is the
   * next item to be evicted.
   */
  std::list<LRUCacheItem> item_ll_;

  /** Maps a key label to an iterator (list node of) of `item_ll_`. */
  std::unordered_map<K, typename std::list<LRUCacheItem>::iterator> item_map_;

  /** The maximum cache size. */
  const uint64_t max_size_;

  /** The current cache size. */
  uint64_t size_;

  /* ********************************* */
  /*         PRIVATE ROUTINES          */
  /* ********************************* */

  /** Evicts the next object. */
  void evict() {
    passert(!item_ll_.empty());

    auto& item = item_ll_.front();
    item_map_.erase(item.key_);
    size_ -= item.size_;
    item_ll_.pop_front();
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_LRU_CACHE_H
