/**
 * @file   rtree.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file implements class RTree.
 */

#include "tiledb/sm/rtree/rtree.h"
#include <cassert>
#include <iostream>
#include <list>
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

RTree::RTree() {
  dim_num_ = 0;
  fanout_ = 0;
  type_ = Datatype::INT32;
}

RTree::RTree(
    Datatype type,
    unsigned dim_num,
    unsigned fanout,
    const std::vector<void*>& mbrs)
    : dim_num_(dim_num)
    , fanout_(fanout)
    , type_(type) {
  build_tree(mbrs);
}

RTree::~RTree() = default;

RTree::RTree(const RTree& rtree)
    : RTree() {
  auto clone = rtree.clone();
  swap(clone);
}

RTree::RTree(RTree&& rtree)
    : RTree() {
  swap(rtree);
}

RTree& RTree::operator=(const RTree& rtree) {
  auto clone = rtree.clone();
  swap(clone);

  return *this;
}

RTree& RTree::operator=(RTree&& rtree) {
  swap(rtree);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

unsigned RTree::dim_num() const {
  return dim_num_;
}

unsigned RTree::fanout() const {
  return fanout_;
}

template <class T>
TileOverlap RTree::get_tile_overlap(const std::vector<const T*>& range) const {
  TileOverlap overlap;

  // Empty tree
  if (dim_num_ == 0 || levels_.empty())
    return overlap;

  // This will keep track of the traversal
  std::list<Entry> traversal;
  traversal.push_front({0, 0});
  auto leaf_num = levels_.back().mbr_num_;
  auto height = this->height();
  uint64_t mbr_size = 2 * dim_num_ * datatype_size(type_);

  while (!traversal.empty()) {
    // Get next entry
    auto entry = traversal.front();
    traversal.pop_front();
    auto level = entry.level_;
    auto mbr_idx = entry.mbr_idx_;
    auto offset = entry.mbr_idx_ * mbr_size;
    auto mbr = levels_[level].mbrs_.data() + offset;

    // Get
    auto ratio = this->range_overlap<T>(range, (T*)mbr);

    // If there is overlap
    if (ratio != 0.0) {
      // If there is full overlap
      if (ratio == 1.0) {
        auto subtree_leaf_num = this->subtree_leaf_num(level);
        assert(subtree_leaf_num > 0);
        uint64_t start = mbr_idx * subtree_leaf_num;
        uint64_t end = start + MIN(subtree_leaf_num, leaf_num - start) - 1;
        auto tile_range = std::pair<uint64_t, uint64_t>(start, end);
        overlap.tile_ranges_.emplace_back(tile_range);
      } else {  // Partial overlap
        // If this is the leaf level, insert into results
        if (level == height - 1) {
          auto mbr_idx_ratio = std::pair<uint64_t, double>(mbr_idx, ratio);
          overlap.tiles_.emplace_back(mbr_idx_ratio);
        } else {  // Insert all "children" to traversal
          auto next_mbr_num = levels_[level + 1].mbr_num_;
          auto start = mbr_idx * fanout_;
          auto end = MIN(start + fanout_ - 1, next_mbr_num - 1);
          for (uint64_t i = start; i <= end; ++i)
            traversal.push_front({level + 1, end - (i - start)});
        }
      }
    }
  }

  return overlap;
}

unsigned RTree::height() const {
  return (unsigned)levels_.size();
}

template <class T>
double RTree::range_overlap(
    const std::vector<const T*>& range, const T* mbr) const {
  double ratio = 1.0;
  assert(range.size() == dim_num_);

  for (unsigned i = 0; i < dim_num_; ++i) {
    assert(range[i][0] <= range[i][1]);
    assert(mbr[2 * i] <= mbr[2 * i + 1]);

    // No overlap
    if (range[i][0] > mbr[2 * i + 1] || range[i][1] < mbr[2 * i]) {
      ratio = 0.0;
      break;
    }

    // Update ratio
    auto overlap_start = MAX(range[i][0], mbr[2 * i]);
    auto overlap_end = MIN(range[i][1], mbr[2 * i + 1]);
    auto overlap_range = overlap_end - overlap_start;
    overlap_range += std::numeric_limits<T>::is_integer;
    auto mbr_range = mbr[2 * i + 1] - mbr[2 * i];
    mbr_range += std::numeric_limits<T>::is_integer;
    ratio *= (double)overlap_range / mbr_range;
  }

  return ratio;
}

uint64_t RTree::subtree_leaf_num(uint64_t level) const {
  // Check invalid level
  if (level >= levels_.size())
    return 0;

  uint64_t leaf_num = 1;
  auto subtree_height = this->height() - level;
  for (unsigned i = 0; i < subtree_height - 1; ++i)
    leaf_num *= fanout_;

  return leaf_num;
}

Datatype RTree::type() const {
  return type_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status RTree::build_tree(const std::vector<void*>& mbrs) {
  switch (type_) {
    case Datatype::INT8:
      return build_tree<int8_t>(mbrs);
    case Datatype::UINT8:
      return build_tree<uint8_t>(mbrs);
    case Datatype::INT16:
      return build_tree<int16_t>(mbrs);
    case Datatype::UINT16:
      return build_tree<uint16_t>(mbrs);
    case Datatype::INT32:
      return build_tree<int32_t>(mbrs);
    case Datatype::UINT32:
      return build_tree<uint32_t>(mbrs);
    case Datatype::INT64:
      return build_tree<int64_t>(mbrs);
    case Datatype::UINT64:
      return build_tree<uint64_t>(mbrs);
    case Datatype::FLOAT32:
      return build_tree<float>(mbrs);
    case Datatype::FLOAT64:
      return build_tree<double>(mbrs);
    default:
      assert(false);
      return LOG_STATUS(
          Status::RTreeError("Cannot build R-Tree; Unsupported type"));
  }

  return Status::Ok();
}

template <class T>
Status RTree::build_tree(const std::vector<void*>& mbrs) {
  // Handle empty tree
  if (mbrs.empty())
    return Status::Ok();

  // Build leaf level
  auto leaf_level = build_leaf_level(mbrs);
  auto leaf_num = leaf_level.mbr_num_;
  levels_.push_back(std::move(leaf_level));
  if (leaf_num == 1)
    return Status::Ok();

  // Build rest of the tree bottom up
  auto height = (size_t)ceil(utils::math::log(fanout_, leaf_num)) + 1;
  for (size_t i = 0; i < height - 1; ++i) {
    auto new_level = build_level<T>(levels_.back());
    levels_.emplace_back(new_level);
  }

  // Make the root as the first level
  std::reverse(std::begin(levels_), std::end(levels_));

  return Status::Ok();
}

RTree::Level RTree::build_leaf_level(const std::vector<void*>& mbrs) {
  assert(!mbrs.empty());

  Level new_level;

  // Allocate space
  uint64_t mbr_size = 2 * dim_num_ * datatype_size(type_);
  uint64_t leaf_level_size = mbrs.size() * mbr_size;
  new_level.mbr_num_ = mbrs.size();
  new_level.mbrs_.resize(leaf_level_size);

  // Copy MBRs
  uint64_t offset = 0;
  for (auto mbr : mbrs) {
    auto copy_loc = new_level.mbrs_.data() + offset;
    std::memcpy(copy_loc, mbr, mbr_size);
    offset += mbr_size;
  }

  return new_level;
}

template <class T>
RTree::Level RTree::build_level(const Level& level) {
  Level new_level;

  uint64_t mbr_size = 2 * dim_num_ * datatype_size(type_);
  new_level.mbr_num_ = (uint64_t)ceil((double)level.mbr_num_ / fanout_);
  uint64_t new_level_size = new_level.mbr_num_ * mbr_size;
  new_level.mbrs_.resize(new_level_size);

  uint64_t mbrs_visited = 0, offset_level = 0, offset_new_level = 0;
  for (uint64_t i = 0; i < new_level.mbr_num_; ++i) {
    auto mbr_num = MIN(fanout_, level.mbr_num_ - mbrs_visited);
    auto mbr_at = (T*)(level.mbrs_.data() + offset_level);
    auto union_loc = (T*)(new_level.mbrs_.data() + offset_new_level);
    utils::geometry::compute_mbr_union<T>(dim_num_, mbr_at, mbr_num, union_loc);
    mbrs_visited += mbr_num;
    offset_level += mbr_num * mbr_size;
    offset_new_level += mbr_size;
  }
  assert(mbrs_visited == level.mbr_num_);

  return new_level;
}

RTree RTree::clone() const {
  RTree clone;
  clone.dim_num_ = dim_num_;
  clone.fanout_ = fanout_;
  clone.type_ = type_;
  clone.levels_ = levels_;

  return clone;
}

void RTree::swap(RTree& rtree) {
  std::swap(dim_num_, rtree.dim_num_);
  std::swap(fanout_, rtree.fanout_);
  std::swap(type_, rtree.type_);
  std::swap(levels_, rtree.levels_);
}

// Explicit template instantiations

template TileOverlap RTree::get_tile_overlap<int8_t>(
    const std::vector<const int8_t*>& range) const;
template TileOverlap RTree::get_tile_overlap<uint8_t>(
    const std::vector<const uint8_t*>& range) const;
template TileOverlap RTree::get_tile_overlap<int16_t>(
    const std::vector<const int16_t*>& range) const;
template TileOverlap RTree::get_tile_overlap<uint16_t>(
    const std::vector<const uint16_t*>& range) const;
template TileOverlap RTree::get_tile_overlap<int32_t>(
    const std::vector<const int32_t*>& range) const;
template TileOverlap RTree::get_tile_overlap<uint32_t>(
    const std::vector<const uint32_t*>& range) const;
template TileOverlap RTree::get_tile_overlap<int64_t>(
    const std::vector<const int64_t*>& range) const;
template TileOverlap RTree::get_tile_overlap<uint64_t>(
    const std::vector<const uint64_t*>& range) const;
template TileOverlap RTree::get_tile_overlap<float>(
    const std::vector<const float*>& range) const;
template TileOverlap RTree::get_tile_overlap<double>(
    const std::vector<const double*>& range) const;

template double RTree::range_overlap<int8_t>(
    const std::vector<const int8_t*>& range, const int8_t* mbr) const;
template double RTree::range_overlap<uint8_t>(
    const std::vector<const uint8_t*>& range, const uint8_t* mbr) const;
template double RTree::range_overlap<int16_t>(
    const std::vector<const int16_t*>& range, const int16_t* mbr) const;
template double RTree::range_overlap<uint16_t>(
    const std::vector<const uint16_t*>& range, const uint16_t* mbr) const;
template double RTree::range_overlap<int32_t>(
    const std::vector<const int32_t*>& range, const int32_t* mbr) const;
template double RTree::range_overlap<uint32_t>(
    const std::vector<const uint32_t*>& range, const uint32_t* mbr) const;
template double RTree::range_overlap<int64_t>(
    const std::vector<const int64_t*>& range, const int64_t* mbr) const;
template double RTree::range_overlap<uint64_t>(
    const std::vector<const uint64_t*>& range, const uint64_t* mbr) const;
template double RTree::range_overlap<float>(
    const std::vector<const float*>& range, const float* mbr) const;
template double RTree::range_overlap<double>(
    const std::vector<const double*>& range, const double* mbr) const;

}  // namespace sm
}  // namespace tiledb
