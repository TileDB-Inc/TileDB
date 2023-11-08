/**
 * @file   memory_tracker.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class MemoryTracker.
 */

#ifndef TILEDB_MEMORY_TRACKER_H
#define TILEDB_MEMORY_TRACKER_H

#include "tiledb/common/common.h"
#include "tiledb/sm/stats/stats.h"

namespace tiledb::sm {

enum class MemoryType : uint8_t {
  RTREE,
  FOOTER,
  TILE_OFFSETS,
  TILE_VAR_OFFSETS,
  TILE_VAR_SIZES,
  TILE_VALIDITY_OFFSETS,
  MIN_BUFFER,
  MAX_BUFFER,
  SUMS,
  NULL_COUNTS,
  ENUMERATION,
  COORD_TILES,
  ATTR_TILES,
  TIMESTAMP_TILES,
  DELETE_TIMESTAMP_TILES,
  DELETE_CONDITION_TILES,
  WRITER_FIXED_DATA,
  WRITER_VAR_DATA
};

std::string memory_type_to_str(MemoryType mem_type);

class MemoryTracker;

class [[nodiscard]] MemoryToken {
 public:
  /** Disable the default constructor. */
  MemoryToken() = delete;

  /** Constructor. */
  MemoryToken(
      std::weak_ptr<MemoryTracker> parent, uint64_t size, MemoryType mem_type);

  /** Destructor. */
  ~MemoryToken();

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryToken);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryToken);

  /** Create a copy of a token. This creates a new reservation. */
  shared_ptr<MemoryToken> copy();

  /** The size of this token's reservation. */
  inline uint64_t size() const {
    return size_;
  }

  /** The type of reserved memory. */
  inline MemoryType memory_type() const {
    return mem_type_;
  }

 private:
  std::weak_ptr<MemoryTracker> parent_;
  uint64_t size_;
  MemoryType mem_type_;
};

class MemoryTracker : std::enable_shared_from_this<MemoryTracker> {
 public:
  /** Constructor. */
  MemoryTracker(stats::Stats* stats = nullptr);

  /** Destructor. */
  ~MemoryTracker() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTracker);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTracker);

  /** Reserve memory budget. */
  shared_ptr<MemoryToken> reserve(uint64_t size, MemoryType mem_type);

  /** Release a reservation. */
  void release(MemoryToken& token);

  /** Leak. Provocatively named to make sure we come back to it. */
  void leak(uint64_t size, MemoryType mem_type);

  /** Set total memory budget. */
  void set_budget(uint64_t size);

  /** Get total memory budget. */
  inline uint64_t get_budget() {
    return budget_;
  }

  /** Get total usage. */
  inline uint64_t get_usage() {
    return usage_;
  }

  /** Get usage by type. */
  inline uint64_t get_usage(MemoryType mem_type) {
    return usage_by_type_[mem_type];
  }

  /** Get available memory. */
  inline uint64_t get_available() {
    return budget_ - usage_;
  }

  std::string to_string() const;

 private:
  void do_reservation(uint64_t size, MemoryType mem_type);

  /** The stats instance. */
  stats::Stats* stats_;

  /** Protects tracking member variables. */
  std::mutex mutex_;

  /** Memory usage for tracked structures. */
  uint64_t usage_;

  /** Memory usage high water mark. */
  uint64_t usage_hwm_;

  /** Memory budget. */
  uint64_t budget_;

  /** Memory usage by type. */
  std::unordered_map<MemoryType, uint64_t> usage_by_type_;
};

struct MemoryTokenKey {
  MemoryTokenKey(MemoryType type, const std::vector<uint64_t>& id);

  bool operator==(const MemoryTokenKey& rhs) const {
    if (type_ != rhs.type_ || id_len_ != rhs.id_len_) {
      return false;
    }

    return memcmp(id_.get(), rhs.id_.get(), id_len_ * sizeof(uint64_t)) == 0;
  }

  static std::size_t hash(const MemoryTokenKey& key);

  MemoryType type_;
  std::shared_ptr<uint64_t[]> id_;
  size_t id_len_;
};

struct MemoryTokenKeyHasher {
  std::size_t operator()(const MemoryTokenKey& key) const;
};

class MemoryTokenBag {
 public:
  MemoryTokenBag() = default;
  MemoryTokenBag(shared_ptr<MemoryTracker> memory_tracker);
  MemoryTokenBag(MemoryTokenBag&& rhs);
  MemoryTokenBag& operator=(MemoryTokenBag&& rhs);

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTokenBag);

  /** Reset all tokens and drop the memory tracker. */
  void clear();

  /** Get the memory tracker used by this bag. */
  inline shared_ptr<MemoryTracker> get_memory_tracker() const {
    std::lock_guard<std::mutex> lg(mutex_);
    return memory_tracker_;
  }

  /** Set the memory tracker to use for this bag. Throws if already set. */
  void set_memory_tracker(shared_ptr<MemoryTracker> memory_tracker);

  bool has_reservation(MemoryType mem_type, std::vector<uint64_t> id);

  /** Reserve memory for a type and id pair. */
  void reserve(uint64_t size, MemoryType mem_type, std::vector<uint64_t> id);

  /** Release memory for a type and id. */
  void release(MemoryType mem_type, std::vector<uint64_t> id);

  /** Syntactic helper. */
  void reserve(uint64_t size, MemoryType mem_type) {
    reserve(size, mem_type, std::vector<uint64_t>{});
  }

  /** Syntactic helper. */
  void reserve(uint64_t size, MemoryType mem_type, uint64_t x) {
    reserve(size, mem_type, std::vector<uint64_t>{x});
  }

  /** Syntactic helper. */
  void reserve(uint64_t size, MemoryType mem_type, uint64_t x, uint64_t y) {
    reserve(size, mem_type, std::vector<uint64_t>{x, y});
  }

  /** Syntactic helper. */
  void reserve(uint64_t size, MemoryType mem_type, uint64_t x, uint64_t y, uint64_t z) {
    reserve(size, mem_type, std::vector<uint64_t>{x, y, z});
  }

  /** Syntactic helper. */
  inline void release(MemoryType mem_type) {
    release(mem_type, std::vector<uint64_t>{});
  }

  /** Syntactic helper. */
  inline void release(MemoryType mem_type, uint64_t x) {
    release(mem_type, std::vector<uint64_t>{x});
  }

  /** Syntactic helper. */
  inline void release(MemoryType mem_type, uint64_t x, uint64_t y) {
    release(mem_type, std::vector<uint64_t>{x, y});
  }

  /** Syntactic helper. */
  inline void release(MemoryType mem_type, uint64_t x, uint64_t y, uint64_t z) {
    release(mem_type, std::vector<uint64_t>{x, y, z});
  }

  // Swap memory trackers.
  void swap(MemoryTokenBag& rhs);

 private:
  mutable std::mutex mutex_;
  shared_ptr<MemoryTracker> memory_tracker_;
  std::unordered_map<
      MemoryTokenKey,
      shared_ptr<MemoryToken>,
      MemoryTokenKeyHasher>
      tokens_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_MEMORY_TRACKER_H
