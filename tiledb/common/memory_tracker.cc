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

#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/exception/exception.h"
#include "tiledb/common/logger.h"

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

}  // namespace tiledb::sm
