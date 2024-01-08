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
#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"

namespace tdb = tiledb::common;

namespace tiledb {
namespace sm {

enum class MemoryType : uint8_t {
  RTREE,
  FOOTER,
  TILE_OFFSETS,
  MIN_MAX_SUM_NULL_COUNTS,
  ENUMERATION
};

class MemoryTracker;

class MemoryTrackingResource : public tdb::pmr::memory_resource {
 public:
  explicit MemoryTrackingResource(shared_ptr<MemoryTracker> tracker, MemoryType type)
      : tracker_(tracker)
      , type_(type) {
  }

  ~MemoryTrackingResource() = default;

 protected:
  void* do_allocate(size_t bytes, size_t alignment) override;
  void do_deallocate(void* p, size_t bytes, size_t alignment) override;
  bool do_is_equal(
      const tdb::pmr::memory_resource& other) const noexcept override;

 private:
  shared_ptr<MemoryTracker> tracker_;
  MemoryType type_;
};

class MemoryTracker : public std::enable_shared_from_this<MemoryTracker> {
 public:
  friend class MemoryTrackingResource;

  /** Constructor. */
  MemoryTracker(tdb::pmr::memory_resource* = nullptr);

  /** Destructor. */
  ~MemoryTracker();

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTracker);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTracker);

  tdb::pmr::memory_resource* get_resource(MemoryType type);

  void leak_memory(MemoryType type, size_t bytes);

  /**
   * Set the memory budget.
   *
   * @param size The memory budget size.
   * @return true if the budget can be set, false otherwise.
   */
  bool set_budget(uint64_t size) {
    std::lock_guard<std::mutex> lg(mutex_);
    if (usage_ > size) {
      return false;
    }

    budget_ = size;
    return true;
  }

  /**
   * Get the memory usage.
   */
  uint64_t get_memory_usage() {
    std::lock_guard<std::mutex> lg(mutex_);
    return usage_;
  }

  /**
   * Get the memory usage by type.
   */
  uint64_t get_memory_usage(MemoryType type) {
    std::lock_guard<std::mutex> lg(mutex_);
    return usage_by_type_[type];
  }

  /**
   * Get available room based on budget
   * @return available amount left in budget
   */
  uint64_t get_memory_available() {
    std::lock_guard<std::mutex> lg(mutex_);
    return budget_ - usage_;
  }

  /**
   * Get memory budget
   * @return budget
   */
  uint64_t get_memory_budget() {
    std::lock_guard<std::mutex> lg(mutex_);
    return budget_;
  }

  std::string to_string() const;

 protected:
  void* allocate(MemoryType type, size_t bytes, size_t alignment);
  void deallocate(MemoryType type, void* p, size_t bytes, size_t alignment);

 private:
  void check_budget(MemoryType type, size_t bytes);

  /** Protects all member variables. */
  std::mutex mutex_;

  /** The pmr memory resource. */
  tdb::pmr::memory_resource* upstream_;

  /** Child trackers. */
  std::unordered_map<MemoryType, shared_ptr<MemoryTrackingResource>> trackers_;

  /** Memory budget. */
  uint64_t budget_;

  /** Memory usage for tracked structures. */
  uint64_t usage_;

  /** Memory usage high water mark. */
  uint64_t usage_hwm_;

  /** Memory usage by type. */
  std::unordered_map<MemoryType, uint64_t> usage_by_type_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OPEN_ARRAY_MEMORY_TRACKER_H
