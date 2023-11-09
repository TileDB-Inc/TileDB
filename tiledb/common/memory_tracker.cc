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

 #include <chrono>

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
    case MemoryType::COORD_TILES:
      return "CoordinateTiles";
    case MemoryType::ATTR_TILES:
      return "AttributeTiles";
    case MemoryType::TIMESTAMP_TILES:
      return "TimestampTiles";
    case MemoryType::DELETE_TIMESTAMP_TILES:
      return "DeleteTimestampTiles";
    case MemoryType::DELETE_CONDITION_TILES:
      return "DeleteConditionTiles";
    case MemoryType::WRITER_FIXED_DATA:
      return "WriterFixedData";
    case MemoryType::WRITER_VAR_DATA:
      return "WriterVarData";
    default:
      auto val = std::to_string(static_cast<uint32_t>(mem_type));
      throw std::logic_error("Unknown memory type: " + val);
  }
}

std::vector<MemoryType> memory_types() {
  return {
    MemoryType::RTREE,
    MemoryType::FOOTER,
    MemoryType::TILE_OFFSETS,
    MemoryType::TILE_VAR_OFFSETS,
    MemoryType::TILE_VAR_SIZES,
    MemoryType::TILE_VALIDITY_OFFSETS,
    MemoryType::MIN_BUFFER,
    MemoryType::MAX_BUFFER,
    MemoryType::SUMS,
    MemoryType::NULL_COUNTS,
    MemoryType::ENUMERATION,
    MemoryType::COORD_TILES,
    MemoryType::ATTR_TILES,
    MemoryType::TIMESTAMP_TILES,
    MemoryType::DELETE_TIMESTAMP_TILES,
    MemoryType::DELETE_CONDITION_TILES,
    MemoryType::WRITER_FIXED_DATA,
    MemoryType::WRITER_VAR_DATA
  };
}

MemoryToken::MemoryToken(
    std::weak_ptr<MemoryTracker> parent, uint64_t size, MemoryType mem_type)
    : parent_(parent)
    , size_(size)
    , mem_type_(mem_type) {
}

MemoryToken::~MemoryToken() {
  if (auto ptr = parent_.lock()) {
    // std::stringstream ss;
    // ss << "RELEASE TOKEN: " << memory_type_to_str(mem_type_) << " : " << size_ << std::endl;
    // std::cerr << ss.str();
    ptr->release(*this);
  }
}

shared_ptr<MemoryToken> MemoryToken::copy() {
  if (auto ptr = parent_.lock()) {
    return ptr->reserve(size_, mem_type_);
  } else {
    return nullptr;
  }
}

std::string this_to_str(void* ptr) {
  std::stringstream ss;
  ss << ptr;
  return ss.str();
}

MemoryTracker::MemoryTracker(stats::Stats* stats)
    : stats_(stats ? stats->create_child("MemoryTracker." + this_to_str(this)) : nullptr)
    , usage_(0)
    , usage_hwm_(0)
    , budget_(std::numeric_limits<uint64_t>::max())
    , num_tokens_(0) {
  //std::cerr << "CONSTRUCT: " << (void*) this << std::endl;
  // Initialize counters just because
  if (stats_) {
    for (auto& type : memory_types()) {
      stats_->set_counter(memory_type_to_str(type), 0);
    }
  }
}

MemoryTracker::~MemoryTracker() {
  //std::cerr << "DESTRUCT: " << (void*) this << std::endl;
  if (!stats_) {
    return;
  }

  stats_->set_counter("Total", usage_);
  for (auto& [type, size] : usage_by_type_) {
    stats_->set_counter(memory_type_to_str(type), size);
  }
}

shared_ptr<MemoryTracker> MemoryTracker::create(stats::Stats* stats) {
  struct EnableMakeShared : public MemoryTracker {
      EnableMakeShared(stats::Stats* stats) : MemoryTracker(stats) {}
    };
    return std::make_shared<EnableMakeShared>(stats);
}

shared_ptr<MemoryToken> MemoryTracker::reserve(
    uint64_t size, MemoryType mem_type) {
  std::lock_guard<std::mutex> lg(mutex_);
  num_tokens_ += 1;
  do_reservation(size, mem_type);
  auto sptr = shared_from_this();
  return make_shared<MemoryToken>(HERE(), weak_from_this(), size, mem_type);
}

void MemoryTracker::release(MemoryToken& token) {
  std::lock_guard<std::mutex> lg(mutex_);
  usage_ -= token.size();
  usage_by_type_[token.memory_type()] -= token.size();
  num_tokens_ -= 1;

  if (stats_) {
    stats_->set_counter("Total", usage_);
    stats_->set_counter("NumTokens", num_tokens_);
    stats_->set_counter(
        memory_type_to_str(token.memory_type()),
        get_usage(token.memory_type()));
  }
}

void MemoryTracker::leak(uint64_t size, MemoryType mem_type) {
  std::lock_guard<std::mutex> lg(mutex_);
  do_reservation(size, mem_type);
}

void MemoryTracker::set_budget(uint64_t size) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (usage_ > size) {
    throw MemoryTrackerException("Unable to set budget smaller than usage."
        + to_string());
  }

  budget_ = size;
}

std::string MemoryTracker::to_string() const {
  std::stringstream ss;
  ss << "[Budget: " << budget_ << "] ";
  ss << "[Usage: " << usage_ << "] ";
  ss << "[HWM: " << usage_hwm_ << "]";

  for (MemoryType t : memory_types()) {
    ss << "[" << memory_type_to_str(t) << ": " << get_usage(t) << "] ";
  }
  return ss.str();
}

void MemoryTracker::do_reservation(uint64_t size, MemoryType mem_type) {
  if (usage_ + size > budget_) {
    std::string mem_type_str = memory_type_to_str(mem_type);
    throw MemoryTrackerException(
        "Insufficient " + mem_type_str + " memory budget; Need " +
        std::to_string(size) + " bytes but only had " +
        std::to_string(budget_ - usage_) +
        " bytes remaining from original budget of " + std::to_string(budget_)
        + to_string());
  }

  usage_ += size;
  usage_by_type_[mem_type] += size;

  if (usage_ > usage_hwm_) {
    usage_hwm_ = usage_;
  }

  if (stats_) {
    stats_->set_counter("Total", usage_);
    stats_->set_counter("NumTokens", num_tokens_);
    stats_->set_counter(
        memory_type_to_str(mem_type), get_usage(mem_type));
    stats_->set_max_counter("HighWaterMark", usage_hwm_);
  }
}

MemoryTokenKey::MemoryTokenKey(MemoryType type, const std::vector<uint64_t>& id)
      : type_(type)
      , id_len_(id.size()) {
  id_ = make_shared<uint64_t[]>(HERE(), id_len_);
  for (size_t i = 0; i < id.size(); i++) {
    id_[i] = id[i];
  }
}

// This funky thing is boost::hash_combine from:
// https://www.boost.org/doc/libs/1_55_0/doc/html/hash/reference.html#boost.hash_combine
std::size_t hash_combine(uint64_t seed, uint64_t val) {
  return val + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

std::size_t MemoryTokenKeyHasher::operator()(const MemoryTokenKey& key) const {
  auto type_hash = std::hash<uint8_t>()(static_cast<uint8_t>(key.type_));
  auto curr_hash = hash_combine(0, type_hash);

  auto hasher = std::hash<uint64_t>();
  for (size_t i = 0; i < key.id_len_; i++) {
    curr_hash = hash_combine(curr_hash, hasher(key.id_[i]));
  }

  return curr_hash;
}

MemoryTokenBag::MemoryTokenBag(const std::string& tag, shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , tag_(tag) {
}

MemoryTokenBag::MemoryTokenBag(MemoryTokenBag&& rhs)
    : memory_tracker_(std::move(rhs.memory_tracker_))
    , tokens_(std::move(rhs.tokens_)) {
}

MemoryTokenBag& MemoryTokenBag::operator=(MemoryTokenBag&& rhs) {
  tokens_ = std::move(rhs.tokens_);
  memory_tracker_ = std::move(rhs.memory_tracker_);
  return *this;
}

void MemoryTokenBag::clear() {
  std::lock_guard<std::mutex> lg(mutex_);
  tokens_.clear();
  memory_tracker_.reset();

  // std::stringstream ss;
  // ss << tag_ << ":" << (void*) this << " CLEAR " << tokens_.size() << std::endl;
  // std::cerr << ss.str();
}

void MemoryTokenBag::set_memory_tracker(shared_ptr<MemoryTracker> memory_tracker) {
  std::lock_guard<std::mutex> lg(mutex_);

  if (memory_tracker_) {
    cpptrace::generate_trace().print();
    throw MemoryTrackerException("Unable to replace existing memory tracker.");
  }

  assert(tokens_.empty() && "Non-empty tokens with no memory tracker");

  memory_tracker_ = memory_tracker;
}

bool MemoryTokenBag::has_reservation(MemoryType mem_type, std::vector<uint64_t> id) {
  std::lock_guard<std::mutex> lg(mutex_);

  if (!memory_tracker_) {
    return false;
  }

  MemoryTokenKey key{mem_type, id};
  return tokens_.contains(key);
}

void MemoryTokenBag::reserve(uint64_t size, MemoryType mem_type, std::vector<uint64_t> id) {
  std::lock_guard<std::mutex> lg(mutex_);

  if (!memory_tracker_) {
    return;
  }

  MemoryTokenKey key{mem_type, id};
  auto token = memory_tracker_->reserve(size, mem_type);
  tokens_[key] = token;

  // std::stringstream ss;
  // ss << tag_ << ":" << (void*) this << " RESERVE " << tokens_.size() << std::endl;
  // std::cerr << ss.str();
}

void MemoryTokenBag::release(MemoryType mem_type, std::vector<uint64_t> id) {
  std::lock_guard<std::mutex> lg(mutex_);

  if (!memory_tracker_) {
    return;
  }

  MemoryTokenKey key{mem_type, id};

  auto it = tokens_.find(key);
  if (it == tokens_.end()) {
    return;
  }

  tokens_.erase(it);

  // std::stringstream ss;
  // ss << tag_ << ":" << (void*) this << " RELEASE " << tokens_.size() << std::endl;
  // std::cerr << ss.str();
}

void MemoryTokenBag::swap(MemoryTokenBag& rhs) {
  std::lock_guard<std::mutex> lg(mutex_);
  std::lock_guard<std::mutex> rhs_lg(rhs.mutex_);
  std::swap(memory_tracker_, rhs.memory_tracker_);
  std::swap(tokens_, rhs.tokens_);
}

}  // namespace tiledb::sm
