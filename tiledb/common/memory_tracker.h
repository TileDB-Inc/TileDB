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

#include <chrono>
#include <condition_variable>
#include <thread>

#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"

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

  /** The number of bytes tracked by this resource. */
  uint64_t get_count();

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
  /** Destructor. */
  ~MemoryTracker();

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTracker);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTracker);

  /** Get the id of this MemoryTracker instance. */
  inline uint64_t get_id() {
    return id_;
  }

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
   * Create a memory resource instance.
   *
   * @param type The type of memory that is being tracked.
   * @return A memory resource derived from std::pmr::memory_resource.
   */
  tdb::pmr::memory_resource* get_resource(MemoryType);

  /** Return the total and counts of this tracker. */
  std::tuple<uint64_t, std::unordered_map<MemoryType, uint64_t>> get_counts();

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

 protected:
  /**
   * Constructor.
   *
   * This constructor is protected on purpose to discourage creating instances
   * of this class that aren't connected to a ContextResources. When writing
   * library code, you should almost always be using an existing instance of
   * a MemoryTracker from the places those exist, i.e., on an Array, Query,
   * or in the Consolidator. Occasionally, we'll need to create new instances
   * for specific reasons. In those cases you need to have a reference to the
   * ContextResources to call ContextResource::create_memory_tracker().
   *
   * For tests that need to have a temporary MemoryTracker instance, there is
   * a `create_test_memory_tracker()` API available in the test support library.
   */
  MemoryTracker()
      : memory_usage_(0)
      , memory_budget_(std::numeric_limits<uint64_t>::max())
      , id_(generate_id())
      , type_(MemoryTrackerType::ANONYMOUS)
      , upstream_(tdb::pmr::get_default_resource())
      , total_counter_(0){};

 private:
  /** Protects all non-atomic member variables. */
  std::mutex mutex_;

  /** Memory usage for tracked structures. */
  uint64_t memory_usage_;

  /** Memory budget. */
  uint64_t memory_budget_;

  /** Memory usage by type. */
  std::unordered_map<MemoryType, uint64_t> memory_usage_by_type_;

  /** The id of this MemoryTracker. */
  uint64_t id_;

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

  /** Generate a unique id for this MemoryTracker. */
  static uint64_t generate_id();
};

class MemoryTrackerManager {
 public:
  /** Constructor. */
  MemoryTrackerManager() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTrackerManager);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTrackerManager);

  /**
   * Create a new memory tracker.
   *
   * @return The created MemoryTracker.
   */
  shared_ptr<MemoryTracker> create_tracker();

  /**
   * Generate a JSON string representing the current state of tracked memory.
   *
   * @return A string containing the JSON representation of tracked memory.
   */
  std::string to_json();

 private:
  /** A mutext to protect our list of trackers. */
  std::mutex mutex_;

  /** A weak_ptr to the instances of MemoryTracker we create. */
  std::vector<std::weak_ptr<MemoryTracker>> trackers_;
};

class MemoryTrackerReporter {
 public:
  /**
   * Constructor.
   *
   * @param cfg The Config instance for the parent context.
   * @param manager The MemoryTrackerManager instance on the context resources.
   */
  MemoryTrackerReporter(
      const Config& cfg, shared_ptr<MemoryTrackerManager> manager)
      : manager_(manager)
      , filename_(cfg.get<std::string>("sm.memory.tracker.reporter.filename"))
      , stop_(false) {
  }

  /** Destructor. */
  ~MemoryTrackerReporter();

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTrackerReporter);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTrackerReporter);

  /** Start the background reporter thread if configured. */
  void start();

  /** Stop the background reporter thread if started. */
  void stop();

  /** The background reporter thread's main loop. */
  void run();

 private:
  /** The MemoryTrackerManager instance on the parent ContextResources. */
  shared_ptr<MemoryTrackerManager> manager_;

  /** An filename set in the config. */
  std::optional<std::string> filename_;

  /** The background reporter thread. */
  std::thread thread_;

  /** A mutex for communication with the background thread. */
  std::mutex mutex_;

  /** A condition variable for signaling the background thread. */
  std::condition_variable cv_;

  /** A stop flag to signal shutdown to the background thread. */
  bool stop_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OPEN_ARRAY_MEMORY_TRACKER_H
