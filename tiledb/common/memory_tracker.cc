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
 * This file implements class MemoryTracker.
 */

#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/stats/stats.h"

namespace tiledb::sm {

class MemoryTrackerException : public common::StatusException {
 public:
  explicit MemoryTrackerException(const std::string& message)
      : StatusException("MemoryTracker", message) {
  }
};

std::string memory_type_to_str(MemoryType mem_type) {
  switch (mem_type) {
    case MemoryType::RTREE:
      return "RTree";
    case MemoryType::FOOTER:
      return "Footer";
    case MemoryType::TILE_OFFSETS:
      return "TileOffsets";
    case MemoryType::TILE_VAR_OFFSETS:
      return "TileVarOffsets";
    case MemoryType::TILE_VAR_SIZES:
      return "TileVarSizes";
    case MemoryType::TILE_VALIDITY_OFFSETS:
      return "TileValidityOffsets";
    case MemoryType::MIN_BUFFER:
      return "MinBuffer";
    case MemoryType::MAX_BUFFER:
      return "MaxBuffer";
    case MemoryType::SUMS:
      return "Sums";
    case MemoryType::NULL_COUNTS:
      return "NullCounts";
    case MemoryType::ENUMERATION:
      return "Enumeration";
    case MemoryType::TILE_DATA:
      return "TileData";
    case MemoryType::FILTERED_TILE_DATA:
      return "FilteredTileData";
    default:
      auto val = std::to_string(static_cast<uint32_t>(mem_type));
      throw std::logic_error("Unknown memory type: " + val);
  }
}

MemoryToken::MemoryToken(
    std::weak_ptr<MemoryTracker> parent, MemoryType mem_type, uint64_t size)
    : parent_(parent)
    , mem_type_(mem_type)
    , size_(size) {
}

MemoryToken::~MemoryToken() {
  if (auto ptr = parent_.lock()) {
    ptr->release(*this);
  }
}

shared_ptr<MemoryToken> MemoryToken::copy() {
  if (auto ptr = parent_.lock()) {
    return ptr->reserve(mem_type_, size_);
  } else {
    return nullptr;
  }
}

MemoryTracker::MemoryTracker(stats::Stats* stats)
    : stats_(stats ? stats->create_child("MemoryTracker") : nullptr)
    , usage_(0)
    , usage_hwm_(0)
    , budget_(std::numeric_limits<uint64_t>::max()) {
}

shared_ptr<MemoryToken> MemoryTracker::reserve(
    MemoryType mem_type, uint64_t size) {
  do_reservation(mem_type, size);
  return make_shared<MemoryToken>(HERE(), weak_from_this(), mem_type, size);
}

void MemoryTracker::release(MemoryToken& token) {
  std::lock_guard<std::mutex> lg(mutex_);
  usage_ -= token.size();
  usage_by_type_[token.memory_type()] -= token.size();

  if (stats_) {
    stats_->sub_counter("Usage", token.size());
    stats_->sub_counter(
        "Usage" + memory_type_to_str(token.memory_type()), token.size());
  }
}

void MemoryTracker::leak(MemoryType mem_type, uint64_t size) {
  do_reservation(mem_type, size);
}

void MemoryTracker::set_budget(uint64_t size) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (usage_ > size) {
    throw MemoryTrackerException("Unable to set budget smaller than usage.");
  }

  budget_ = size;
}

void MemoryTracker::do_reservation(MemoryType mem_type, uint64_t size) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (usage_ + size > budget_) {
    std::string mem_type_str = memory_type_to_str(mem_type);
    throw MemoryTrackerException(
        "Insufficient " + mem_type_str + " memory budget; Need " +
        std::to_string(size) + " bytes but only had " +
        std::to_string(budget_ - usage_) +
        " bytes remaining from original budget of " + std::to_string(budget_));
  }

  usage_ += size;
  usage_by_type_[mem_type] += size;

  if (usage_ > usage_hwm_) {
    usage_hwm_ = usage_;
  }

  if (stats_) {
    stats_->add_counter("Usage", size);
    stats_->add_counter("Usage" + memory_type_to_str(mem_type), size);
    stats_->set_max_counter("UsageHighWaterMark", usage_hwm_);
  }
}

// This funky thing is boost::hash_combine from:
// https://www.boost.org/doc/libs/1_55_0/doc/html/hash/reference.html#boost.hash_combine
std::size_t hash_combine(uint64_t seed, uint64_t val) {
  return val + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

std::size_t MemoryTokenKeyHasher::operator()(const MemoryTokenKey& key) const {
  auto h1 = std::hash<uint8_t>()(static_cast<uint8_t>(key.type_));
  auto h2 = std::hash<uint64_t>()(key.id_);
  return hash_combine(hash_combine(0, h1), h2);
}

MemoryTokenBag::MemoryTokenBag(shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker) {
}

void MemoryTokenBag::reserve(MemoryType mem_type, uint64_t size) {
  switch (mem_type) {
    case MemoryType::RTREE:
    case MemoryType::FOOTER:
      reserve(mem_type, 0, size);
      return;
    default:
      throw MemoryTrackerException("This memory type requires an id.");
  }
}

void MemoryTokenBag::reserve(MemoryType mem_type, uint64_t id, uint64_t size) {
  if (!memory_tracker_) {
    return;
  }

  auto token = memory_tracker_->reserve(mem_type, size);
  tokens_[{mem_type, id}] = token;
}

void MemoryTokenBag::release(MemoryType mem_type) {
  switch (mem_type) {
    case MemoryType::RTREE:
    case MemoryType::FOOTER:
      release(mem_type, 0);
      return;
    default:
      throw MemoryTrackerException("This memory type requires an id.");
  }
}

void MemoryTokenBag::release(MemoryType mem_type, uint64_t id) {
  if (!memory_tracker_) {
    return;
  }

  tokens_.erase({mem_type, id});
}

}  // namespace tiledb::sm
