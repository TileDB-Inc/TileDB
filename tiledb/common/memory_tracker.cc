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
 * This file contains the MemoryTracker implementation.
 */

#include <fstream>

#include "external/include/nlohmann/json.hpp"

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
    case MemoryType::RTREE:
      return "RTree";
    case MemoryType::FOOTER:
      return "Footer";
    case MemoryType::TILE_OFFSETS:
      return "TileOffsets";
    case MemoryType::TILE_MIN_VALS:
      return "TileMinVals";
    case MemoryType::TILE_MAX_VALS:
      return "TileMaxVals";
    case MemoryType::TILE_SUMS:
      return "TileSums";
    case MemoryType::TILE_NULL_COUNTS:
      return "TileNullCounts";
    case MemoryType::ENUMERATION:
      return "Enumeration";
    default:
      auto val = std::to_string(static_cast<uint32_t>(type));
      throw std::logic_error("Invalid memory type: " + val);
  }
}

std::string memory_tracker_type_to_str(MemoryTrackerType type) {
  switch (type) {
    case MemoryTrackerType::ANONYMOUS:
      return "Anonymous";
    case MemoryTrackerType::ARRAY_READ:
      return "ArrayRead";
    case MemoryTrackerType::ARRAY_WRITE:
      return "ArrayWrite";
    case MemoryTrackerType::QUERY_READ:
      return "QueryRead";
    case MemoryTrackerType::QUERY_WRITE:
      return "QueryWrite";
    case MemoryTrackerType::CONSOLIDATOR:
      return "Consolidator";
    default:
      auto val = std::to_string(static_cast<uint32_t>(type));
      throw std::logic_error("Invalid memory tracker type: " + val);
  }
}

uint64_t MemoryTrackerResource::get_count() {
  return type_counter_.fetch_add(0, std::memory_order_relaxed);
}

void* MemoryTrackerResource::do_allocate(size_t bytes, size_t alignment) {
  total_counter_.fetch_add(bytes, std::memory_order_relaxed);
  type_counter_.fetch_add(bytes, std::memory_order_relaxed);
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
  // TODO: Make this spit out some loud log noise if we've not
  //       properly deallocated all our things.
}

tdb::pmr::memory_resource* MemoryTracker::get_resource(MemoryType type) {
  std::lock_guard<std::mutex> lg(mutex_);
  auto iter = resources_.find(type);

  if (iter != resources_.end()) {
    return iter->second.get();
  }

  if (counters_.find(type) != counters_.end() && counters_[type] != 0) {
    throw MemoryTrackerException("Invalid internal state");
  }

  counters_.emplace(type, 0);

  auto ret = make_shared<MemoryTrackerResource>(
      HERE(), upstream_, total_counter_, counters_[type]);
  resources_.emplace(type, ret);

  return ret.get();
}

std::tuple<uint64_t, std::unordered_map<MemoryType, uint64_t>>
MemoryTracker::get_counts() {
  std::lock_guard<std::mutex> lg(mutex_);

  auto total = total_counter_.fetch_add(0, std::memory_order_relaxed);
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

shared_ptr<MemoryTracker> MemoryTrackerManager::create_tracker() {
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
  auto ret = make_shared<MemoryTracker>(HERE());
  trackers_.emplace(trackers_.begin(), ret);

  return ret;
}

std::string MemoryTrackerManager::to_json() {
  std::lock_guard<std::mutex> lg(mutex_);
  nlohmann::json rv;

  for (auto iter = trackers_.begin(); iter != trackers_.end(); ++iter) {
    auto ptr = iter->lock();
    if (ptr) {
      nlohmann::json val;

      // Set an distinguishing id
      val["tracker_id"] = std::to_string(ptr->get_id());

      // Mark the stats with the tracker type.
      val["tracker_type"] = memory_tracker_type_to_str(ptr->get_type());

      // Add memory stats
      auto [total, by_type] = ptr->get_counts();
      val["total_memory"] = total;
      val["by_type"] = nlohmann::json::object();
      for (auto& [type, count] : by_type) {
        val["by_type"][memory_type_to_str(type)] = count;
      }
      rv.push_back(val);
    } else {
      trackers_.erase(iter);
    }
  }

  return rv.dump();
}

MemoryTrackerReporter::~MemoryTrackerReporter() {
  if (!filename_.has_value()) {
    return;
  }

  {
    std::lock_guard<std::mutex> lg(mutex_);
    stop_ = true;
    cv_.notify_all();
  }
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
    // by better safe than sorry.
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

      LOG_ERROR(
          "Error starting the MemoryTrackerReporter: " + std::string(e.what()));
    }
  }

  LOG_ERROR("Failed to start the MemoryTrackerReporter thread.");
}

void MemoryTrackerReporter::run() {
  std::stringstream ss;
  std::ofstream out;
  out.open(filename_.value(), std::ios::app);

  if (!out) {
    LOG_ERROR("Error opening MemoryTrackerReporter file: " + filename_.value());
    return;
  }

  while (true) {
    std::unique_lock<std::mutex> lk(mutex_);
    cv_.wait_for(lk, std::chrono::milliseconds(1000), [&] { return stop_; });

    if (stop_) {
      return;
    }

    if (!out) {
      LOG_ERROR(
          "Error writing to MemoryTrackerReporter file: " + filename_.value());
      return;
    }

    auto json = manager_->to_json();
    if (json == "null") {
      // This happens if the manager doesn't have any trackers registered.
      // Rather than log noise we just ignore it.
      continue;
    }

    ss.str("");
    ss.clear();
    ss << json << std::endl;
    out << ss.str();
  }
}

}  // namespace tiledb::sm
