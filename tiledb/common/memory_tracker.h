/**
 * @file   memory_tracker.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file contains the definitions for classes related to tracking memory
 * using the polymorphic memory resources feature introduced in C++17.
 *
 * There are four main classes to be aware of:
 *
 *   - MemoryTrackerResource
 *   - MemoryTracker
 *   - MemoryTrackerManager
 *   - MemoryTrackerReporter
 *
 * MemoryTrackerResource
 * =====================
 *
 * The MemoryTrackerResource class is responsible for actually tracking
 * individual allocations. Each MemoryTrackerResource represents a single type
 * of memory as enumerated in the MemoryType enum. To create instances of this
 * class, users should use the MemoryTrackerManager::get_resource API.
 *
 * MemoryTracker
 * =============
 *
 * The MemoryTracker class is responsible for managing instances of
 * MemoryTrackerResource. A MemoryTracker represents some section or behavior
 * inside the TileDB library as enumerated in the MemoryTrackerType enum.
 * Instances of MemoryTracker should be created using the
 * MemoryTrackerManager::create_tracker() API or via the helper method
 * ContextResources::create_memory_tracker(). Generally speaking, there should
 * be very few of these instances outside of test code and instead existing
 * instances should be referenced.
 *
 * For instance, there is currently an existing MemoryTracker member variable
 * on both Array and Query. Most code in the library should be using one of
 * these two trackers. There are a few specialized instances like in the
 * Consolidator or for things like deserializing GenericTileIO tiles.
 *
 * MemoryTrackerManager
 * ====================
 *
 * The MemoryTrackerManager is a member variable on the ContextResources
 * class. Users should not need to interact with this class directly as its
 * just a container that holds references to all the MemoryTracker instances
 * for a given context. Its used by the MemoryTrackerReport when logging
 * memory usage.
 *
 * MemoryTrackerReporter
 * =====================
 *
 * The MemoryTrackerReporter class is a member variable on the ContextResources
 * class. Users should not need to interact with this class directly as its
 * just used to log memory statistics to a special log file when configured.
 *
 * Users wishing to run memory usage experiments should use the
 * 'sm.memory.tracker.reporter.filename' configuration key to set a filename
 * that will contain the logged memory statistics in JSONL format (i.e., JSON
 * objects and arrays encoded one per line). At runtime the reporter appends
 * a JSON blob once a second to this logfile that can then be analyzed using
 * whatever scripts or software as appropriate.
 *
 * Users may also set configuration key
 * 'sm.memory.tracker.reporter.wait_time_ms' to toggle the duration, in
 * milliseconds, that the calling thread is blocked in
 * 'MemoryTrackerReporter::run' before the condition variable is notified. By
 * default, the thread will wait for 1000 ms.
 */

#ifndef TILEDB_MEMORY_TRACKER_H
#define TILEDB_MEMORY_TRACKER_H

#include <condition_variable>
#include <functional>
#include <thread>

#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"

namespace tiledb::sm {

/** The type of memory to track. */
enum class MemoryType {
  ATTRIBUTES,
  CONSOLIDATION_BUFFERS,
  DENSE_TILE_SUBARRAY,
  DIMENSION_LABELS,
  DIMENSIONS,
  DOMAINS,
  ENUMERATION,
  ENUMERATION_PATHS,
  FILTERED_DATA,
  FILTERED_DATA_BLOCK,
  FOOTER,
  GENERIC_TILE_IO,
  METADATA,
  QUERY_CONDITION,
  RESULT_TILE,
  RESULT_TILE_BITMAP,
  RTREE,
  TILE_DATA,
  TILE_HILBERT_VALUES,
  TILE_MAX_VALS,
  TILE_MIN_VALS,
  TILE_NULL_COUNTS,
  TILE_OFFSETS,
  TILE_SUMS,
  WRITER_DATA,
  WRITER_TILE_DATA
};

/**
 * Return a string representation of type
 *
 * @param type The MemoryType to convert.
 * @return A string representation.
 */
std::string memory_type_to_str(MemoryType type);

/** The type of MemoryTracker. */
enum class MemoryTrackerType {
  ANONYMOUS,
  ARRAY_CREATE,
  ARRAY_LOAD,
  ARRAY_READ,
  ARRAY_WRITE,
  CONSOLIDATOR,
  ENUMERATION_CREATE,
  EPHEMERAL,
  FRAGMENT_INFO_LOAD,
  GROUP,
  QUERY_READ,
  QUERY_WRITE,
  REST_CLIENT,
  SCHEMA_EVOLUTION
};

/**
 * Return a string representation of type
 *
 * @param type The MemoryTrackerType to convert.
 * @return A string representation.
 */
std::string memory_tracker_type_to_str(MemoryTrackerType type);

class MemoryTrackerResource : public tdb::pmr::memory_resource {
 public:
  // Disable all default generated constructors.
  MemoryTrackerResource() = delete;
  DISABLE_COPY_AND_COPY_ASSIGN(MemoryTrackerResource);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryTrackerResource);

  /** Constructor. */
  explicit MemoryTrackerResource(
      tdb::pmr::memory_resource* upstream,
      std::atomic<uint64_t>& total_counter,
      std::atomic<uint64_t>& type_counter,
      const std::atomic<uint64_t>& memory_budget,
      std::function<void()> on_budget_exceeded)
      : upstream_(upstream)
      , total_counter_(total_counter)
      , type_counter_(type_counter)
      , memory_budget_(memory_budget)
      , on_budget_exceeded_(on_budget_exceeded) {
  }

  /** The number of bytes tracked by this resource. */
  uint64_t get_count();

 protected:
  /** Perform an allocation, returning a pointer to the allocated memory. */
  void* do_allocate(size_t bytes, size_t alignment) override;

  /** Deallocate a previously allocated chunk of memory. */
  void do_deallocate(void* p, size_t bytes, size_t alignment) override;

  /** Check if two memory trackers are equal. */
  bool do_is_equal(
      const tdb::pmr::memory_resource& other) const noexcept override;

 private:
  /** The upstream memory resource to use for the actual allocation. */
  tdb::pmr::memory_resource* upstream_;

  /** A reference to a total counter for the MemoryTracker. */
  std::atomic<uint64_t>& total_counter_;

  /** A reference to the memory type counter this resource is tracking. */
  std::atomic<uint64_t>& type_counter_;

  /** The memory budget for this resource. */
  const std::atomic<uint64_t>& memory_budget_;

  /** A function to call when the budget is exceeded. */
  std::function<void()> on_budget_exceeded_;
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
    if (legacy_memory_usage_ + size <= legacy_memory_budget_) {
      legacy_memory_usage_ += size;
      legacy_memory_usage_by_type_[mem_type] += size;
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
    legacy_memory_usage_ -= size;
    legacy_memory_usage_by_type_[mem_type] -= size;
  }

  /**
   * Set the memory budget.
   *
   * @param size The memory budget size.
   * @return true if the budget can be set, false otherwise.
   */
  bool set_budget(uint64_t size) {
    std::lock_guard<std::mutex> lg(mutex_);
    if (legacy_memory_usage_ > size) {
      return false;
    }

    legacy_memory_budget_ = size;
    return true;
  }

  /**
   * Get the memory usage.
   */
  uint64_t get_memory_usage() {
    std::lock_guard<std::mutex> lg(mutex_);
    return legacy_memory_usage_;
  }

  /**
   * Get the memory usage by type.
   */
  uint64_t get_memory_usage(MemoryType mem_type) {
    std::lock_guard<std::mutex> lg(mutex_);
    return legacy_memory_usage_by_type_[mem_type];
  }

  /**
   * Get available room based on budget
   * @return available amount left in budget
   */
  uint64_t get_memory_available() {
    std::lock_guard<std::mutex> lg(mutex_);
    if (legacy_memory_usage_ + counters_[MemoryType::TILE_OFFSETS] >
        legacy_memory_budget_) {
      return 0;
    }
    return legacy_memory_budget_ - legacy_memory_usage_ -
           counters_[MemoryType::TILE_OFFSETS];
  }

  /**
   * Get memory budget
   * @return budget
   */
  uint64_t get_memory_budget() {
    std::lock_guard<std::mutex> lg(mutex_);
    return legacy_memory_budget_;
  }

  /**
   * Refresh the memory budget used by the new system.
   *
   * This method should be called only by Query::set_config, which gets called
   * by the REST server after deserializing the query and creating its strategy,
   * with a config that contains the new memory budget.
   *
   * @param new_budget The new memory budget.
   */
  void refresh_memory_budget(uint64_t new_budget) {
    memory_budget_.store(new_budget, std::memory_order_relaxed);
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
   *
   * @param memory_budget The memory budget for this MemoryTracker.
   * @param on_budget_exceeded The function to call when the budget is exceeded.
   * Defaults to a function that does nothing.
   */
  MemoryTracker(
      uint64_t memory_budget = std::numeric_limits<uint64_t>::max(),
      std::function<void()> on_budget_exceeded = []() {})
      : legacy_memory_usage_(0)
      , legacy_memory_budget_(std::numeric_limits<uint64_t>::max())
      , id_(generate_id())
      , type_(MemoryTrackerType::ANONYMOUS)
      , upstream_(tdb::pmr::get_default_resource())
      , total_counter_(0)
      , memory_budget_(memory_budget)
      , on_budget_exceeded_(on_budget_exceeded){};

 private:
  /** Protects all non-atomic member variables. */
  std::mutex mutex_;

  /* Legacy memory tracker infrastructure */

  /** Memory usage for tracked structures. */
  uint64_t legacy_memory_usage_;

  /** Memory budget. */
  uint64_t legacy_memory_budget_;

  /** Memory usage by type. */
  std::unordered_map<MemoryType, uint64_t> legacy_memory_usage_by_type_;

  /* Modern memory tracker infrastructure */

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

  /** Memory budget. */
  std::atomic<uint64_t> memory_budget_;

  /** A function to call when the budget is exceeded. */
  std::function<void()> on_budget_exceeded_;

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
   * @param memory_budget The memory budget for the new MemoryTracker. Defaults
   * to unlimited.
   *
   * @param on_budget_exceeded The function to call when the budget is exceeded.
   * Defaults to a function that does nothing.
   *
   * @return The created MemoryTracker.
   */
  shared_ptr<MemoryTracker> create_tracker(
      uint64_t memory_budget = std::numeric_limits<uint64_t>::max(),
      std::function<void()> on_budget_exceeded = []() {});

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
      , wait_time_ms_(cfg.get<int>("sm.memory.tracker.reporter.wait_time_ms"))
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

  /** A filename set in the config. */
  std::optional<std::string> filename_;

  /** A wait time (in milliseconds) set in the config. */
  std::optional<int> wait_time_ms_;

  /** The background reporter thread. */
  std::thread thread_;

  /** A mutex for communication with the background thread. */
  std::mutex mutex_;

  /** A condition variable for signaling the background thread. */
  std::condition_variable cv_;

  /** A stop flag to signal shutdown to the background thread. */
  bool stop_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_OPEN_ARRAY_MEMORY_TRACKER_H
