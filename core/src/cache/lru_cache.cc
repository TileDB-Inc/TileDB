/**
 * @file   lru_cache.cc
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
 * This file implements class LRUCache.
 */

#include "lru_cache.h"
#include "logger.h"

#include <cassert>
#include <cstdlib>
#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

LRUCache::LRUCache(
    uint64_t max_size,
    void *(*evict_callback)(LRUCacheItem *, void *),
    void *evict_callback_data) {
  evict_callback_ = evict_callback;
  evict_callback_data_ = evict_callback_data;
  max_size_ = max_size;
  size_ = 0;
}

LRUCache::~LRUCache() {
  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

void LRUCache::clear() {
  for (auto &item : item_ll_) {
    if (evict_callback_ == nullptr)
      std::free(item.object_);
    else
      (*evict_callback_)(&item, evict_callback_data_);
  }
  item_ll_.clear();
}

Status LRUCache::insert(const std::string &key, void *object, uint64_t size) {
  // Do nothing if the object size is bigger than the cache maximum size
  if (size > max_size_)
    return Status::Ok();

  if (object == nullptr)
    return LOG_STATUS(Status::LRUCacheError(
        "Cannot insert into cache; Object cannot be null"));

  // Lock mutex
  mtx_.lock();

  // Evict if necessary
  while (size_ + size > max_size_)
    evict();

  // Key exists
  auto item_it = item_map_.find(key);
  if (item_it != item_map_.end()) {
    // Replace cache item
    auto &node = item_it->second;
    auto &item = *node;
    if (evict_callback_ == nullptr)
      std::free(item.object_);
    else
      (*evict_callback_)(&item, evict_callback_data_);
    item.object_ = object;
    item.size_ = size;

    // Move cache item node to the end of the list
    if (std::next(node) != item_ll_.end()) {
      item_ll_.splice(item_ll_.end(), item_ll_, node, std::next(node));
    }
  } else {  // Key does not exist
    // Create a new cache item
    LRUCacheItem new_item;
    new_item.key_ = key;
    new_item.object_ = object;
    new_item.size_ = size;

    // Create new node in linked list
    item_ll_.emplace_back(new_item);

    // Create new element in the hash table
    item_map_[key] = --(item_ll_.end());
  }

  size_ += size;

  // Unlock mutex
  mtx_.unlock();

  return Status::Ok();
}

Status LRUCache::read(const std::string &key, Buffer *buffer, bool *success) {
  // Lock mutex
  mtx_.lock();

  // Find cached item
  auto item_it = item_map_.find(key);
  if (item_it == item_map_.end()) {
    mtx_.unlock();
    *success = false;
    return Status::Ok();
  }

  // Write item object to buffer
  auto &item = item_it->second;
  buffer->write(item->object_, item->size_);

  // Move cache item node to the end of the list
  if (std::next(item) != item_ll_.end()) {
    item_ll_.splice(item_ll_.end(), item_ll_, item, std::next(item));
  }

  // Unlock mutex
  mtx_.unlock();

  *success = true;
  return Status::Ok();
}

Status LRUCache::read(
    const std::string &key,
    void *buffer,
    uint64_t offset,
    uint64_t nbytes,
    bool *success) {
  // Lock mutex
  mtx_.lock();

  // Find cached item
  auto item_it = item_map_.find(key);
  if (item_it == item_map_.end()) {
    mtx_.unlock();
    *success = false;
    return Status::Ok();
  }

  // Copy from item object
  auto &item = item_it->second;
  if (item->size_ < offset + nbytes) {
    mtx_.unlock();
    return LOG_STATUS(
        Status::LRUCacheError("Failed to read item; Byte range out of bounds"));
  }
  std::memcpy(buffer, (char *)item->object_ + offset, nbytes);

  // Move cache item node to the end of the list
  if (std::next(item) != item_ll_.end()) {
    item_ll_.splice(item_ll_.end(), item_ll_, item, std::next(item));
  }

  // Unlock mutex
  mtx_.unlock();

  *success = true;
  return Status::Ok();
}

std::list<LRUCache::LRUCacheItem>::const_iterator LRUCache::item_iter_begin()
    const {
  return item_ll_.begin();
}

std::list<LRUCache::LRUCacheItem>::const_iterator LRUCache::item_iter_end()
    const {
  return item_ll_.end();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void LRUCache::evict() {
  assert(!item_ll_.empty());

  auto item = item_ll_.front();
  if (evict_callback_ == nullptr)
    std::free(item.object_);
  else
    (*evict_callback_)(&item, evict_callback_data_);
  item_map_.erase(item.key_);
  size_ -= item.size_;
  item_ll_.pop_front();
}

}  // namespace tiledb