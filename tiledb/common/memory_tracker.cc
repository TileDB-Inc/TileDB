/**
 * @file memory_tracker.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include <iostream>
#include <sstream>
#include <string>

#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/exception/exception.h"

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
    case MemoryType::MIN_MAX_SUM_NULL_COUNTS:
      return "MinMaxSumNullCounts";
    case MemoryType::ENUMERATION:
      return "Enumeration";
    default:
      auto val = std::to_string(static_cast<uint32_t>(type));
      throw std::logic_error("Unknown memory type: " + val);
  }
}

void* MemoryTrackingResource::do_allocate(size_t bytes, size_t alignment) {
  return tracker_->allocate(type_, bytes, alignment);
}

void MemoryTrackingResource::do_deallocate(
    void* ptr, size_t bytes, size_t alignment) {
  tracker_->deallocate(type_, ptr, bytes, alignment);
}

bool MemoryTrackingResource::do_is_equal(
    const tdb::pmr::memory_resource& other) const noexcept {

  const MemoryTrackingResource* rhs = dynamic_cast<const MemoryTrackingResource*>(&other);
  if (rhs == nullptr) {
    return false;
  }

  return tracker_.get() == rhs->tracker_.get() && type_ == rhs->type_;
}

MemoryTracker::MemoryTracker(tdb::pmr::memory_resource* upstream)
  : budget_(std::numeric_limits<uint64_t>::max())
  , usage_(0)
  , usage_hwm_(0) {
  if (upstream == nullptr) {
    upstream_ = cpp17::pmr::get_default_resource();
  } else {
    upstream_ = upstream;
  }
}

MemoryTracker::~MemoryTracker() {
  // Eventually I should check here to make sure that we've not leaked
  // any memory.
}

tdb::pmr::memory_resource* MemoryTracker::get_resource(MemoryType type) {
  std::lock_guard<std::mutex> lg(mutex_);
  auto iter = trackers_.find(type);
  if (iter == trackers_.end()) {
    auto ret = make_shared<MemoryTrackingResource>(HERE(), shared_from_this(), type);
    trackers_[type] = ret;
    return ret.get();
  } else {
    return iter->second.get();
  }
}

void MemoryTracker::leak_memory(MemoryType type, size_t bytes) {
  std::lock_guard<std::mutex> lg(mutex_);
  check_budget(type, bytes);
  usage_ += bytes;
  usage_by_type_[type] += bytes;
}

void* MemoryTracker::allocate(MemoryType type, size_t bytes, size_t alignment) {
  std::lock_guard<std::mutex> lg(mutex_);
  check_budget(type, bytes);
  auto ret = upstream_->allocate(bytes, alignment);

  if (ret != nullptr) {
    usage_ += bytes;
    usage_by_type_[type] += bytes;

    if (usage_ > usage_hwm_) {
      usage_hwm_ = usage_;
    }
  }

  return ret;
}

void MemoryTracker::deallocate(MemoryType type, void* ptr, size_t bytes, size_t alignment) {
  std::lock_guard<std::mutex> lg(mutex_);
  upstream_->deallocate(ptr, bytes, alignment);
  usage_ -= bytes;
  usage_by_type_[type] -= bytes;
}

std::string MemoryTracker::to_string() const {
  std::stringstream ss;
  ss << "[Budget: " << budget_ << "]";
  ss << " [Usage: " << usage_ << "]";
  ss << " [Usage HWM: " << usage_hwm_ << "]";
  for (auto& [type, count] : usage_by_type_) {
    ss << " [" << memory_type_to_str(type) << ": " << count << "]";
  }
  return ss.str();
}

void MemoryTracker::check_budget(MemoryType type, size_t bytes) {
  if (usage_ + bytes > budget_) {
    std::string type_str = memory_type_to_str(type);
    throw MemoryTrackerException(
        "Insufficient " + type_str + " memory budget; Need " +
        std::to_string(bytes) + " bytes but only had " +
        std::to_string(budget_ - usage_) +
        " bytes remaining from original budget of " + std::to_string(budget_)
        + std::to_string(budget_));
  }
}

}  // namespace tiledb::sm
