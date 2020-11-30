/**
 * @file   arena_alloc.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
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
 * This file declares the ArenaAlloc class.
 */

#ifndef TILEDB_ARENA_ALLOC_H
#define TILEDB_ARENA_ALLOC_H

#include <iostream>

#include <mutex>
#include <map>

#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/macros.h"

namespace tiledb {
namespace common {

class ArenaAlloc {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  ArenaAlloc() :
    capacity_(0),
    buffer_(nullptr) {
  }

  /** Destructor. */
  ~ArenaAlloc() {
    // Destroy this instance. The return status may be
    // non-OK if this instance has not been initialized.
    // We intentionally ignore the return value.
    destroy();
  }

  /** Copy constructor. */
  DISABLE_COPY(ArenaAlloc);

  /** Move constructor. */
  DISABLE_MOVE(ArenaAlloc);

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Copy-assign operator. */
  DISABLE_COPY_ASSIGN(ArenaAlloc);

  /** Move-assign operator. */
  DISABLE_MOVE_ASSIGN(ArenaAlloc);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Initializes the contiguous memory buffer.
   *
   * @param capacity The arena capacity, in bytes.
   * @return Status
   */
  Status init(const uint64_t capacity) {
    if (capacity == 0)
      return Status::ArenaAllocError("Arena must be initialized with a non-zero capacity.");

    if (capacity == 1 || (capacity & (capacity - 1)))
      return Status::ArenaAllocError("Arena capacity must be a power-of-two.");

    // Protect `capacity_`, `buffer_`, `free_list_map_`,
    // and `block_size_map_`.
    std::unique_lock<std::mutex> ul(mutex_);

    if (capacity_ > 0)
      return Status::ArenaAllocError("Arena can not be initialized more than once.");

    capacity_ = capacity;
    buffer_ = std::malloc(capacity_);

    if (buffer_ == nullptr)
      return Status::ArenaAllocError("Failed to initialize the Arena buffer.");

    // Build the free lists by creating a free list for each
    // power-of-two block size less-than-or-equal-to `capacity_`.
    for (uint64_t block_size = 2; block_size <= capacity_; block_size <<= 1)
      free_list_map_.emplace(block_size, std::vector<void*>());

    // Insert `buffer_` into the free list that corresponds to
    // the block size of `capacity_`.
    free_list_map_[capacity_].emplace_back(buffer_);
    
    return Status::Ok();
  }

  Status destroy() {
    // Protect `capacity_`, `buffer_`, `free_list_map_`,
    // and `block_size_map_`.
    std::unique_lock<std::mutex> ul(mutex_);

    if (capacity_ == 0)
      return Status::ArenaAllocError("Unable to destroy an uninitialized Arena.");

    // Reset the capacity.
    capacity_ = 0;

    // Free the contiguous buffer.
    assert(buffer_ != nullptr);
    std::free(buffer_);

    free_list_map_.clear();
    block_size_map_.clear();

    return Status::Ok();
  }

  void* malloc(const uint64_t size) {
    if (size == 0)
      return nullptr;

    // Round up the user-requested `size` to the next
    // power-of-two. We have free lists for each power-of-two
    // block size through our total capacity.
    const uint64_t block_size = pad_size(size);

    // Protect `capacity_`, `free_list_map_`, and `block_size_map_`.
    std::unique_lock<std::mutex> ul(mutex_);

    if (size > capacity_)
      return nullptr;

    // Starting at the free list for the minimum block size,
    // iterate in ascending order through the map to find
    // the first non-empty free list.
    auto iter = free_list_map_.find(block_size);
    while (iter != free_list_map_.end()) {
      if (!iter->second.empty())
        break;
      ++iter;
    }

    // If we were unable to find a free block, return
    // a nullptr.
    if (iter == free_list_map_.end())
      return nullptr;

    // Take a block off of the free list.
    uint64_t free_list_block_size = iter->first;
    void *const free_list_block = iter->second.back();
    iter->second.pop_back();

    // Return all buddy blocks back to the free lists.
    free_list_block_size >>= 1;
    while (block_size <= free_list_block_size) {
      void *const buddy_free_list_block =
        static_cast<char *>(free_list_block) + free_list_block_size;
      free_list_map_[free_list_block_size].emplace_back(buddy_free_list_block);
      free_list_block_size >>= 1;
    }

    // Track the malloc'd block on a malloc list.
    assert(block_size_map_.count(free_list_block) == 0);
    block_size_map_[free_list_block] = block_size;
    
    return free_list_block;
  }

  void free(void* const ptr) {
    // Protect `free_list_map_` and `block_size_map_`.
    std::unique_lock<std::mutex> ul(mutex_);

    // Lookup the block size.
    auto iter = block_size_map_.find(ptr);
    if (iter == block_size_map_.end())
      LOG_FATAL("Unable to free an unallocated address");
    const uint64_t block_size = iter->second;

    // Erase the entry for `ptr` within the `block_size_map_`.
    block_size_map_.erase(iter);

    // Return `ptr` to a free list.
    free_list_map_[block_size].emplace_back(ptr);    
  }

  uint64_t capacity() {
    // Protect `capacity_`.
    std::unique_lock<std::mutex> ul(mutex_);

    return capacity_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The allocated size of the arena buffer. */
  uint64_t capacity_;

  /** The contiguous buffer to allocate from. */
  void* buffer_;

  /**
   * Maps a free list for each valid block size. Each
   * free list contains free blocks for the associated
   * block size.
   */
  std::map<uint64_t, std::vector<void*>> free_list_map_;

  /** Maps user-allocated blocks to their size. */
  std::unordered_map<void *, uint64_t> block_size_map_;

  /** Protects all member variables. */
  std::mutex mutex_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Rounds `size` up to the next power-of-two.
   *
   * @param size The size to pad.
   * @return The padded size.
   */
  uint64_t pad_size(uint64_t size) {
    --size;
    size |= size >> 1;
    size |= size >> 2;
    size |= size >> 4;
    size |= size >> 8;
    size |= size >> 16;
    size |= size >> 32;
    ++size;

    assert((size & (size - 1)) == 0);

    return size;
  }
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_ARENA_ALLOC_H
