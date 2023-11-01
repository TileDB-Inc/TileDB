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
  TILE_DATA,
  FILTERED_TILE_DATA
};

std::string memory_type_to_str(MemoryType mem_type);

class MemoryTracker;

class [[nodiscard]] MemoryToken {
 public:
  /** Disable the default constructor. */
  MemoryToken() = delete;

  /** Constructor. */
  MemoryToken(
      std::weak_ptr<MemoryTracker> parent, MemoryType mem_type, uint64_t size);

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
  MemoryType mem_type_;
  uint64_t size_;
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
  shared_ptr<MemoryToken> reserve(MemoryType mem_type, uint64_t size);

  /** Release a reservation. */
  void release(MemoryToken& token);

  /** Leak. Provocatively named to make sure we come back to it. */
  void leak(MemoryType mem_type, uint64_t size);

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

 private:
  void do_reservation(MemoryType mem_type, uint64_t size);

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
  MemoryTokenKey(MemoryType type, uint64_t id)
      : type_(type)
      , id_(id) {
  }

  bool operator==(const MemoryTokenKey& rhs) const {
    return (type_ == rhs.type_) && (id_ == rhs.id_);
  }

  static std::size_t hash(const MemoryTokenKey& key);

  MemoryType type_;
  uint64_t id_;
};

struct MemoryTokenKeyHasher {
  std::size_t operator()(const MemoryTokenKey& key) const;
};

class MemoryTokenBag {
 public:
  MemoryTokenBag() = default;
  MemoryTokenBag(shared_ptr<MemoryTracker> memory_tracker);

  /** Reserve memory for a type that has no id. Like rtree or footer. */
  void reserve(MemoryType mem_type, uint64_t size);

  /** Reserve memory for a type and id pair. */
  void reserve(MemoryType mem_type, uint64_t id, uint64_t size);

  /** Release memory for a type with no id. */
  void release(MemoryType mem_type);

  /** Release memory for a type and id. */
  void release(MemoryType mem_type, uint64_t id);

 private:
  shared_ptr<MemoryTracker> memory_tracker_;
  std::unordered_map<
      MemoryTokenKey,
      shared_ptr<MemoryToken>,
      MemoryTokenKeyHasher>
      tokens_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_MEMORY_TRACKER_H
