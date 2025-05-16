/**
 * @file subarray_tile_overlap.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file implements class SubarrayTileOverlap.
 */

#include "tiledb/common/assert.h"
#include "tiledb/common/common.h"

#include "tiledb/sm/subarray/subarray_tile_overlap.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

SubarrayTileOverlap::SubarrayTileOverlap()
    : range_idx_start_(0)
    , range_idx_end_(0)
    , range_idx_start_offset_(0)
    , range_idx_end_offset_(0) {
}

SubarrayTileOverlap::SubarrayTileOverlap(const SubarrayTileOverlap& rhs)
    : tile_overlap_idx_(rhs.tile_overlap_idx_)
    , range_idx_start_(rhs.range_idx_start_)
    , range_idx_end_(rhs.range_idx_end_)
    , range_idx_start_offset_(rhs.range_idx_start_offset_)
    , range_idx_end_offset_(rhs.range_idx_end_offset_) {
}

SubarrayTileOverlap::SubarrayTileOverlap(SubarrayTileOverlap&& rhs)
    : tile_overlap_idx_(std::move(rhs.tile_overlap_idx_))
    , range_idx_start_(rhs.range_idx_start_)
    , range_idx_end_(rhs.range_idx_end_)
    , range_idx_start_offset_(rhs.range_idx_start_offset_)
    , range_idx_end_offset_(rhs.range_idx_end_offset_) {
}

SubarrayTileOverlap::SubarrayTileOverlap(
    const uint64_t fragment_num,
    const uint64_t range_idx_start,
    const uint64_t range_idx_end)
    : range_idx_start_(range_idx_start)
    , range_idx_end_(range_idx_end)
    , range_idx_start_offset_(0)
    , range_idx_end_offset_(0) {
  const uint64_t range_num = range_idx_end_ - range_idx_start_ + 1;
  tile_overlap_idx_ = make_shared<TileOverlapIndex>(HERE());
  tile_overlap_idx_->resize(fragment_num);
  for (size_t i = 0; i < tile_overlap_idx_->size(); ++i)
    (*tile_overlap_idx_)[i].resize(range_num);
}

SubarrayTileOverlap::~SubarrayTileOverlap() {
}

SubarrayTileOverlap& SubarrayTileOverlap::operator=(
    const SubarrayTileOverlap& rhs) {
  if (this != &rhs) {
    tile_overlap_idx_ = rhs.tile_overlap_idx_;
    range_idx_start_ = rhs.range_idx_start_;
    range_idx_end_ = rhs.range_idx_end_;
    range_idx_start_offset_ = rhs.range_idx_start_offset_;
    range_idx_end_offset_ = rhs.range_idx_end_offset_;
  }

  return *this;
}

SubarrayTileOverlap& SubarrayTileOverlap::operator=(SubarrayTileOverlap&& rhs) {
  if (this != &rhs) {
    tile_overlap_idx_ = std::move(rhs.tile_overlap_idx_);
    range_idx_start_ = rhs.range_idx_start_;
    range_idx_end_ = rhs.range_idx_end_;
    range_idx_start_offset_ = rhs.range_idx_start_offset_;
    range_idx_end_offset_ = rhs.range_idx_end_offset_;
  }

  return *this;
}

uint64_t SubarrayTileOverlap::range_idx_start() const {
  return range_idx_start_ + range_idx_start_offset_;
}

uint64_t SubarrayTileOverlap::range_idx_end() const {
  return range_idx_end_ - range_idx_end_offset_;
}

uint64_t SubarrayTileOverlap::range_num() const {
  return range_idx_end() - range_idx_start() + 1;
}

bool SubarrayTileOverlap::contains_range(
    const uint64_t range_idx_start, const uint64_t range_idx_end) const {
  if (!tile_overlap_idx_) {
    return false;
  }

  if (range_idx_start < range_idx_start_ || range_idx_end > range_idx_end_) {
    return false;
  }

  return true;
}

void SubarrayTileOverlap::update_range(
    const uint64_t range_idx_start, const uint64_t range_idx_end) {
  iassert(contains_range(range_idx_start, range_idx_end));

  range_idx_start_offset_ = range_idx_start - range_idx_start_;
  range_idx_end_offset_ = range_idx_end_ - range_idx_end;
}

void SubarrayTileOverlap::expand(const uint64_t range_idx_end) {
  if (range_idx_end <= range_idx_end_) {
    return;
  }

  range_idx_end_ = range_idx_end;

  const uint64_t range_num = range_idx_end_ - range_idx_start_ + 1;
  for (size_t i = 0; i < tile_overlap_idx_->size(); ++i)
    (*tile_overlap_idx_)[i].resize(range_num);
}

void SubarrayTileOverlap::clear() {
  tile_overlap_idx_ = nullptr;
  range_idx_start_ = 0;
  range_idx_end_ = 0;
  range_idx_start_offset_ = 0;
  range_idx_end_offset_ = 0;
}

uint64_t SubarrayTileOverlap::byte_size() const {
  uint64_t size = 0;
  if (tile_overlap_idx_) {
    for (const auto& tile_overlaps : *tile_overlap_idx_) {
      for (const auto& tile_overlap : tile_overlaps) {
        size += tile_overlap.byte_size();
      }
    }
  }

  return size;
}

}  // namespace sm
}  // namespace tiledb
