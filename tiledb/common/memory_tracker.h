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

#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"

namespace tiledb {
namespace sm {

enum class MemoryType {
  RTREE,
  FOOTER,
  TILE_OFFSETS,
  TILE_MIN_VALS,
  TILE_MAX_VALS,
  TILE_SUMS,
  TILE_NULL_COUNTS,
  ENUMERATION
};

enum class MemoryTrackerType {
  ANONYMOUS,
  ARRAY_READ,
  ARRAY_WRITE,
  QUERY_READ,
  QUERY_WRITE,
  CONSOLIDATOR
};

class MemoryTrackerResource : public tdb::pmr::memory_resource {
 public:
  // Disable all default generated constructors.
  MemoryTrackerResource() = delete;
  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTrackerResource);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTrackerResource);

  explicit MemoryTrackerResource(
      tdb::pmr::memory_resource* upstream,
      std::atomic<uint64_t>& total_counter,
      std::atomic<uint64_t>& type_counter)
      : upstream_(upstream)
      , total_counter_(total_counter)
      , type_counter_(type_counter) {
  }

 protected:
  void* do_allocate(size_t bytes, size_t alignment) override;
  void do_deallocate(void* p, size_t bytes, size_t alignment) override;
  bool do_is_equal(
      const tdb::pmr::memory_resource& other) const noexcept override;

 private:
  tdb::pmr::memory_resource* upstream_;
  std::atomic<uint64_t>& total_counter_;
  std::atomic<uint64_t>& type_counter_;
};

class MemoryTracker {
 public:
  /** Constructor. */
  MemoryTracker()
      : memory_usage_(0)
      , memory_budget_(std::numeric_limits<uint64_t>::max())
      , type_(MemoryTrackerType::ANONYMOUS)
      , upstream_(tdb::pmr::get_default_resource())
      , total_counter_(0){};

  /** Destructor. */
  ~MemoryTracker();

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTracker);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTracker);

  /**
   * Create a memory resource instance.
   *
   * @param type The type of memory that is being tracked.
   * @return A memory resource derived from std::pmr::memory_resource.
   */
  tdb::pmr::memory_resource* get_resource(MemoryType);

  /** Get the type of this memory tracker. */
  inline MemoryTrackerType get_type() {
    std::lock_guard<std::mutex> lg(mutex_);
    return type_;
  }

  /** Set the type of this memory tracker. */
  void set_type(MemoryTrackerType type) {
    std::lock_guard<std::mutex> lg(mutex_);
    type_ = type;
  }

  /**
   * Take memory from the budget.
   *
   * @param size The memory size.
   * @return true if the memory is available, false otherwise.
   */
  bool take_memory(uint64_t size, MemoryType mem_type) {
    std::lock_guard<std::mutex> lg(mutex_);
    if (memory_usage_ + size <= memory_budget_) {
      memory_usage_ += size;
      memory_usage_by_type_[mem_type] += size;
      return true;
    }

    return false;
  }

  /**
   * Release memory from the budget.
   *
   * @param size The memory size.
   */
  void release_memory(uint64_t size, MemoryType mem_type) {
    std::lock_guard<std::mutex> lg(mutex_);
    memory_usage_ -= size;
    memory_usage_by_type_[mem_type] -= size;
  }

  /**
   * Set the memory budget.
   *
   * @param size The memory budget size.
   * @return true if the budget can be set, false otherwise.
   */
  bool set_budget(uint64_t size) {
    std::lock_guard<std::mutex> lg(mutex_);
    if (memory_usage_ > size) {
      return false;
    }

    memory_budget_ = size;
    return true;
  }

  /**
   * Get the memory usage.
   */
  uint64_t get_memory_usage() {
    std::lock_guard<std::mutex> lg(mutex_);
    return memory_usage_;
  }

  /**
   * Get the memory usage by type.
   */
  uint64_t get_memory_usage(MemoryType mem_type) {
    std::lock_guard<std::mutex> lg(mutex_);
    return memory_usage_by_type_[mem_type];
  }

  /**
   * Get available room based on budget
   * @return available amount left in budget
   */
  uint64_t get_memory_available() {
    std::lock_guard<std::mutex> lg(mutex_);
    return memory_budget_ - memory_usage_;
  }

  /**
   * Get memory budget
   * @return budget
   */
  uint64_t get_memory_budget() {
    std::lock_guard<std::mutex> lg(mutex_);
    return memory_budget_;
  }

 private:
  /** Protects all non-atomic member variables. */
  std::mutex mutex_;

  /** Memory usage for tracked structures. */
  uint64_t memory_usage_;

  /** Memory budget. */
  uint64_t memory_budget_;

  /** Memory usage by type. */
  std::unordered_map<MemoryType, uint64_t> memory_usage_by_type_;

  /** The type of this MemoryTracker. */
  MemoryTrackerType type_;

  /** The upstream memory resource. */
  tdb::pmr::memory_resource* upstream_;

  /** MemoryTrackerResource by MemoryType. */
  std::unordered_map<MemoryType, std::shared_ptr<MemoryTrackerResource>>
      resources_;

  /** Memory counters by MemoryType. */
  std::unordered_map<MemoryType, std::atomic<uint64_t>> counters_;

  /** The total memory usage of this MemoryTracker. */
  std::atomic<uint64_t> total_counter_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OPEN_ARRAY_MEMORY_TRACKER_H
