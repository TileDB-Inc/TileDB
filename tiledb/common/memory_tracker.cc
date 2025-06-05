/**
 * @file memory_tracker.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file contains implementations for the PMR memory tracking classes. See
 * the top level description in memory_tracker.h.
 */

#include <fstream>

#include "external/include/nlohmann/json.hpp"

#include "tiledb/common/assert.h"
#include "tiledb/common/exception/exception.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"

namespace tiledb::sm {

class MemoryTrackerException : public common::StatusException {
 public:
  explicit MemoryTrackerException(const std::string& message)
      : StatusException("MemoryTracker", message) {
  }
};

std::string memory_type_to_str(MemoryType type) {
  switch (type) {
    case MemoryType::ATTRIBUTES:
      return "Attributes";
    case MemoryType::CONSOLIDATION_BUFFERS:
      return "ConsolidationBuffers";
    case MemoryType::DENSE_TILE_SUBARRAY:
      return "DenseTileSubarray";
    case MemoryType::DIMENSION_LABELS:
      return "DimensionLabels";
    case MemoryType::DIMENSIONS:
      return "Dimensions";
    case MemoryType::DOMAINS:
      return "Domains";
    case MemoryType::ENUMERATION:
      return "Enumeration";
    case MemoryType::ENUMERATION_PATHS:
      return "EnumerationPaths";
    case MemoryType::FILTERED_DATA:
      return "FilteredData";
    case MemoryType::FILTERED_DATA_BLOCK:
      return "FilteredDataBlock";
    case MemoryType::FOOTER:
      return "Footer";
    case MemoryType::GENERIC_TILE_IO:
      return "GenericTileIO";
    case MemoryType::METADATA:
      return "Metadata";
    case MemoryType::PARALLEL_MERGE_CONTROL:
      return "ParallelMergeControl";
    case MemoryType::QUERY_CONDITION:
      return "QueryCondition";
    case MemoryType::RESULT_TILE:
      return "ResultTile";
    case MemoryType::RESULT_TILE_BITMAP:
      return "ResultTileBitmap";
    case MemoryType::RTREE:
      return "RTree";
    case MemoryType::SERIALIZATION_BUFFER:
      return "SerializationBuffer";
    case MemoryType::TILE_DATA:
      return "TileData";
    case MemoryType::TILE_HILBERT_VALUES:
      return "TileHilbertValues";
    case MemoryType::TILE_MAX_VALS:
      return "TileMaxVals";
    case MemoryType::TILE_MIN_VALS:
      return "TileMinVals";
    case MemoryType::TILE_NULL_COUNTS:
      return "TileNullCounts";
    case MemoryType::TILE_OFFSETS:
      return "TileOffsets";
    case MemoryType::TILE_SUMS:
      return "TileSums";
    case MemoryType::WRITER_DATA:
      return "WriterData";
    case MemoryType::WRITER_TILE_DATA:
      return "WriterTileData";
  }

  auto val = std::to_string(static_cast<uint32_t>(type));
  throw std::logic_error("Invalid memory type: " + val);
}

std::string memory_tracker_type_to_str(MemoryTrackerType type) {
  switch (type) {
    case MemoryTrackerType::ANONYMOUS:
      return "Anonymous";
    case MemoryTrackerType::ARRAY_CREATE:
      return "ArrayCreate";
    case MemoryTrackerType::ARRAY_LOAD:
      return "ArrayLoad";
    case MemoryTrackerType::ARRAY_READ:
      return "ArrayRead";
    case MemoryTrackerType::ARRAY_WRITE:
      return "ArrayWrite";
    case MemoryTrackerType::CONSOLIDATOR:
      return "Consolidator";
    case MemoryTrackerType::ENUMERATION_CREATE:
      return "EnumerationCreate";
    case MemoryTrackerType::EPHEMERAL:
      return "Ephemeral";
    case MemoryTrackerType::FRAGMENT_INFO_LOAD:
      return "FragmentInfoLoad";
    case MemoryTrackerType::GROUP:
      return "Group";
    case MemoryTrackerType::QUERY_READ:
      return "QueryRead";
    case MemoryTrackerType::QUERY_WRITE:
      return "QueryWrite";
    case MemoryTrackerType::REST_CLIENT:
      return "RestClient";
    case MemoryTrackerType::SCHEMA_EVOLUTION:
      return "SchemaEvolution";
    case MemoryTrackerType::SERIALIZATION:
      return "Serialization";
  }

  auto val = std::to_string(static_cast<uint32_t>(type));
  throw std::logic_error("Invalid memory tracker type: " + val);
}

uint64_t MemoryTrackerResource::get_count() {
  return type_counter_.fetch_add(0, std::memory_order_relaxed);
}

void* MemoryTrackerResource::do_allocate(size_t bytes, size_t alignment) {
  auto previous_total =
      total_counter_.fetch_add(bytes, std::memory_order_relaxed);
  type_counter_.fetch_add(bytes, std::memory_order_relaxed);
  if (previous_total + bytes > memory_budget_ && on_budget_exceeded_) {
    try {
      on_budget_exceeded_();
    } catch (...) {
      // If the callback throws, undo the counter increments and rethrow.
      // If we don't do this, the assert in the MemoryTracker destructor
      // will fail because the total_counter_ will be non-zero.
      total_counter_.fetch_sub(bytes, std::memory_order_relaxed);
      type_counter_.fetch_sub(bytes, std::memory_order_relaxed);
      throw;
    }
  }
  return upstream_->allocate(bytes, alignment);
}

void MemoryTrackerResource::do_deallocate(
    void* ptr, size_t bytes, size_t alignment) {
  upstream_->deallocate(ptr, bytes, alignment);
  type_counter_.fetch_sub(bytes, std::memory_order_relaxed);
  total_counter_.fetch_sub(bytes, std::memory_order_relaxed);
}

bool MemoryTrackerResource::do_is_equal(
    const tdb::pmr::memory_resource& other) const noexcept {
  return this == &other;
}

MemoryTracker::~MemoryTracker() {
  PAssertFailureCallbackRegistration dumpMemory(
      PAssertFailureCallbackDumpMemoryTracker(*this));
  passert(
      total_counter_.fetch_add(0) == 0 &&
      "MemoryTracker destructed with outstanding allocations.");
}

tdb::pmr::memory_resource* MemoryTracker::get_resource(MemoryType type) {
  std::lock_guard<std::mutex> lg(mutex_);

  // If we've already created an instance for this type, return it.
  auto iter = resources_.find(type);
  if (iter != resources_.end()) {
    return iter->second.get();
  }

  // Add a new counter if it doesn't exist.
  if (counters_.find(type) == counters_.end()) {
    counters_.emplace(type, 0);
  } else {
    // There's no outstanding memory resource for this type, so it must be zero.
    passert(
        counters_[type] == 0,
        "Invalid memory tracking state: counters[{}] = {}",
        memory_type_to_str(type),
        counters_[type].load());
  }

  // Create and track a shared_ptr to the new memory resource.
  auto ret = make_shared<MemoryTrackerResource>(
      HERE(),
      upstream_,
      total_counter_,
      counters_[type],
      memory_budget_,
      on_budget_exceeded_);
  resources_.emplace(type, ret);

  // Return the raw memory resource pointer for use by pmr containers.
  return ret.get();
}

std::tuple<uint64_t, std::unordered_map<MemoryType, uint64_t>>
MemoryTracker::get_counts() const {
  std::lock_guard<std::mutex> lg(mutex_);

  auto total = total_counter_.load(std::memory_order_relaxed);
  std::unordered_map<MemoryType, uint64_t> by_type;
  std::vector<MemoryType> to_del;
  for (auto& [mem_type, resource] : resources_) {
    by_type[mem_type] = resource->get_count();
  }

  return {total, by_type};
}

uint64_t MemoryTracker::generate_id() {
  static std::atomic<uint64_t> curr_id{0};
  return curr_id.fetch_add(1);
}

nlohmann::json MemoryTracker::to_json() const {
  nlohmann::json val;

  // Set an distinguishing id
  val["tracker_id"] = std::to_string(get_id());

  // Mark the stats with the tracker type.
  val["tracker_type"] = memory_tracker_type_to_str(get_type());

  // Add memory stats
  auto [total, by_type] = get_counts();
  val["total_memory"] = total;
  val["by_type"] = nlohmann::json::object();
  for (auto& [type, count] : by_type) {
    val["by_type"][memory_type_to_str(type)] = count;
  }

  return val;
}

shared_ptr<MemoryTracker> MemoryTrackerManager::create_tracker(
    uint64_t memory_budget, std::function<void()> on_budget_exceeded) {
  /*
   * The MemoryTracker class has a protected constructor to hopefully help
   * self-document that instances should almost never be created directly
   * except in test code. There exists a
   * `tiledb::test::create_test_memory_tracker()` API that can be used in
   * tests to create untracked instances of MemoryTracker.
   *
   * This just uses the standard private derived class to enable the use of
   * `make_shared` to create instances in specific bits of code.
   */
  class MemoryTrackerCreator : public MemoryTracker {
   public:
    /**
     * Pass through to the protected MemoryTracker constructor for
     * make_shared.
     */
    MemoryTrackerCreator(
        uint64_t memory_budget, std::function<void()> on_budget_exceeded)
        : MemoryTracker(memory_budget, on_budget_exceeded) {
    }
  };

  std::lock_guard<std::mutex> lg(mutex_);

  // Delete any expired weak_ptr instances
  size_t idx = 0;
  while (idx < trackers_.size()) {
    if (trackers_[idx].expired()) {
      trackers_.erase(trackers_.begin() + idx);
    } else {
      idx++;
    }
  }

  // Create a new tracker
  auto ret = make_shared<MemoryTrackerCreator>(
      HERE(), memory_budget, on_budget_exceeded);
  trackers_.emplace(trackers_.begin(), ret);

  return ret;
}

std::string MemoryTrackerManager::to_json() {
  std::lock_guard<std::mutex> lg(mutex_);
  nlohmann::json rv;

  /*
   * The reason for this being a while-loop instead of a for-loop is that we're
   * modifying the trackers_ vector while iterating over it. The reference docs
   * for std::vector::erase make it sound like a standard for-loop with
   * iterators would work here, but the subtle key point in the docs is that
   * the end() iterator used during iteration is invalidated after a call to
   * erase. The end result of which is that it will lead to a subtle bug when
   * every weak_ptr in the vector is expired (such as at shutdown) which leads
   * to deleting random bits of memory. Thankfully The address sanitizer
   * managed to point out the issue rather quickly.
   */
  size_t idx = 0;
  while (idx < trackers_.size()) {
    auto ptr = trackers_[idx].lock();
    // If the weak_ptr is expired, we just remove it from trackers_ and
    // carry on.
    if (!ptr) {
      trackers_.erase(trackers_.begin() + idx);
      continue;
    }

    rv.push_back(ptr->to_json());

    idx++;
  }

  return rv.dump();
}

MemoryTrackerReporter::~MemoryTrackerReporter() {
  if (!filename_.has_value()) {
    return;
  }

  // Scoped lock_guard so we don't hold the lock while waiting for threads
  // to join.
  {
    std::lock_guard<std::mutex> lg(mutex_);
    stop_ = true;
    cv_.notify_all();
  }

  // Wait for the background thread to quit so that we don't cause a segfault
  // when we destruct our synchronization primitives.
  try {
    thread_.join();
  } catch (std::exception& exc) {
    LOG_ERROR(
        "Error stopping MemoryTrackerReporter thread: " +
        std::string(exc.what()));
  }
}

void MemoryTrackerReporter::start() {
  if (!filename_.has_value()) {
    LOG_INFO("No filename set, not starting the MemoryTrackerReporter.");
    return;
  }

  {
    // Scoped so we release this before the thread starts. Probably unnecessary
    // but better safe than sorry.
    std::lock_guard<std::mutex> lg(mutex_);
    if (stop_) {
      throw std::runtime_error("MemoryTrackerReporters cannot be restarted.");
    }
  }

  // Thread start logic mirrored from the ThreadPool.
  for (size_t i = 0; i < 3; i++) {
    try {
      thread_ = std::thread(&MemoryTrackerReporter::run, this);
      return;
    } catch (const std::system_error& e) {
      if (e.code() == std::errc::resource_unavailable_try_again) {
        continue;
      }

      throw MemoryTrackerException(
          "Error starting the MemoryTrackerReporter: " + std::string(e.what()));
    }
  }

  throw MemoryTrackerException(
      "No threads avaiable to start the MemoryTrackerReporter.");
}

void MemoryTrackerReporter::run() {
  std::stringstream ss;
  std::ofstream out;

  while (true) {
    std::unique_lock<std::mutex> lk(mutex_);
    int wait_time = wait_time_ms_.has_value() ? wait_time_ms_.value() : 1000;
    cv_.wait_for(
        lk, std::chrono::milliseconds(wait_time), [&] { return stop_; });

    if (stop_) {
      return;
    }

    // Open the log file, possibly re-opening after encountering an error. Log
    // any errors and continue trying in case whatever issue resolves itself.
    if (!out.is_open()) {
      // Clear any error state.
      out.clear();
      out.open(filename_.value(), std::ios::app);
    }

    // If we failed to open the file, log a message and try again on the next
    // iteration of this loop. This logic is in a background thread so the
    // only real other options would be to crash the entire program or exit
    // the thread. Retrying to see if its an ephemeral error seems better and
    // also informs users that something is wrong with their config while not
    // causing excessive chaos.
    if (!out) {
      LOG_ERROR(
          "Error opening MemoryTrackerReporter file: " + filename_.value());
      continue;
    }

    // Generate a JSON report from our MemoryTrackerManager.
    auto json = manager_->to_json();
    if (json == "null") {
      // This happens if the manager doesn't have any trackers registered.
      // Rather than log noise we just ignore it.
      continue;
    }

    // Append our report to the log.
    ss.str("");
    ss.clear();
    ss << json << std::endl;
    out << ss.str();

    // If writing to the file fails, we make a note, close it and then attempt
    // to re-open it on the next iteration. See the note above on open errors
    // for more context.
    if (!out) {
      LOG_ERROR(
          "Error writing to MemoryTrackeReporter file: " + filename_.value());
      out.close();
      continue;
    }
  }
}

void PAssertFailureCallbackDumpMemoryTracker::operator()() const {
  const auto json = tracker_.to_json();

  std::cerr << "MEMORY REPORT:" << std::endl;
  std::cerr << json << std::endl;
}

}  // namespace tiledb::sm
