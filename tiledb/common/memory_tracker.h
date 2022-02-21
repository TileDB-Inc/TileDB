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

#include "tiledb/common/status.h"

namespace tiledb {
namespace sm {

class MemoryTracker {
 public:
  /** Constructor. */
  MemoryTracker() {
    memory_usage_ = 0;
    memory_budget_ = std::numeric_limits<uint32_t>::max();
  };

  /** Destructor. */
  ~MemoryTracker() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTracker);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTracker);

  /**
   * Take memory from the budget.
   *
   * @param size The memory size.
   * @return true if the memory is available, false otherwise.
   */
  bool take_memory(uint64_t size) {
    std::lock_guard<std::mutex> lg(mutex_);
    if (memory_usage_ + size <= memory_budget_) {
      memory_usage_ += size;
      return true;
    }

    return false;
  }

  /**
   * Release memory from the budget.
   *
   * @param size The memory size.
   */
  void release_memory(uint64_t size) {
    std::lock_guard<std::mutex> lg(mutex_);
    memory_usage_ -= size;
  }

  /**
   * Set the memory budget.
   *
   * @param size The memory budget size.
   */
  void set_budget(uint64_t size) {
    std::lock_guard<std::mutex> lg(mutex_);
    memory_budget_ = size;
  }

  /**
   * Get the memory usage.
   */
  uint64_t get_memory_usage() {
    std::lock_guard<std::mutex> lg(mutex_);
    return memory_usage_;
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
  /** Protects all member variables. */
  std::mutex mutex_;

  /** Memory usage for tracked structures. */
  uint64_t memory_usage_;

  /** Memory budget. */
  uint64_t memory_budget_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OPEN_ARRAY_MEMORY_TRACKER_H