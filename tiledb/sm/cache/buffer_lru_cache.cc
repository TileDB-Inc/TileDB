/**
 * @file   buffer_lru_cache.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
 * This file implements class BufferLRUCache.
 */

#include "tiledb/sm/cache/buffer_lru_cache.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"

#include <cassert>
#include <cstdlib>
#include <iostream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

BufferLRUCache::BufferLRUCache(const uint64_t max_size)
    : LRUCache(max_size) {
}

Status BufferLRUCache::insert(
    const std::string& key, FilteredBuffer&& buffer, const bool overwrite) {
  const uint64_t alloced_size = buffer.size();

  std::lock_guard<std::mutex> lg(lru_mtx_);
  return LRUCache<std::string, FilteredBuffer>::insert(
      key, std::move(buffer), alloced_size, overwrite);
}

Status BufferLRUCache::read(
    const std::string& key,
    FilteredBuffer& buffer,
    const uint64_t offset,
    const uint64_t nbytes,
    bool* const success) {
  assert(success);
  *success = false;

  std::lock_guard<std::mutex> lg(lru_mtx_);

  // Check if the cache contains the item at `key`.
  if (!has_item(key))
    return Status::Ok();

  // Store a reference to the cached buffer.
  const FilteredBuffer* const cached_buffer = get_item(key);

  // Check the bounds of the read.
  if (cached_buffer->size() < offset + nbytes) {
    return LOG_STATUS(
        Status_LRUCacheError("Failed to read item; Byte range out of bounds"));
  }

  // Copy the requested range into the output `buffer`.
  memcpy(buffer.data(), cached_buffer->data() + offset, nbytes);

  // Touch the item to make it the most recently used item.
  touch_item(key);

  *success = true;
  return Status::Ok();
}

void BufferLRUCache::clear() {
  std::lock_guard<std::mutex> lg(lru_mtx_);
  return LRUCache<std::string, FilteredBuffer>::clear();
}

Status BufferLRUCache::invalidate(const std::string& key, bool* success) {
  std::lock_guard<std::mutex> lg(lru_mtx_);
  return LRUCache<std::string, FilteredBuffer>::invalidate(key, success);
}

typename std::list<BufferLRUCache::LRUCacheItem>::const_iterator
BufferLRUCache::item_iter_begin() const {
  std::lock_guard<std::mutex> lg(lru_mtx_);
  return LRUCache<std::string, FilteredBuffer>::item_iter_begin();
}

typename std::list<BufferLRUCache::LRUCacheItem>::const_iterator
BufferLRUCache::item_iter_end() const {
  std::lock_guard<std::mutex> lg(lru_mtx_);
  return LRUCache<std::string, FilteredBuffer>::item_iter_end();
}

}  // namespace sm
}  // namespace tiledb
