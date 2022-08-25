/**
 * @file   rtree.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/storage_format/serialization/serializers.h"

#include <cassert>
#include <cmath>
#include <iostream>
#include <list>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

RTree::RTree() {
  domain_ = nullptr;
  fanout_ = 0;
  deserialized_buffer_size_ = 0;
}

RTree::RTree(const Domain* domain, unsigned fanout)
    : domain_(domain)
    , fanout_(fanout) {
}

RTree::~RTree() = default;

RTree::RTree(const RTree& rtree)
    : RTree() {
  auto clone = rtree.clone();
  swap(clone);
}

RTree::RTree(RTree&& rtree) noexcept
    : RTree() {
  swap(rtree);
}

RTree& RTree::operator=(const RTree& rtree) {
  auto clone = rtree.clone();
  swap(clone);

  return *this;
}

RTree& RTree::operator=(RTree&& rtree) noexcept {
  swap(rtree);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

void RTree::build_tree() {
  if (levels_.empty()) {
    return;
  }

  assert(levels_.size() == 1);

  auto leaf_num = levels_[0].size();
  assert(leaf_num >= 1);
  if (leaf_num == 1) {
    return;
  }

  // Build the tree bottom up
  auto height = (size_t)std::ceil(utils::math::log(fanout_, leaf_num)) + 1;
  for (size_t i = 0; i < height - 1; ++i) {
    auto new_level = build_level(levels_.back());
    levels_.emplace_back(new_level);
  }

  // Make the root as the first level
  std::reverse(std::begin(levels_), std::end(levels_));
}

uint64_t RTree::free_memory() {
  auto ret = deserialized_buffer_size_;
  levels_.clear();
  deserialized_buffer_size_ = 0;
  return ret;
}

unsigned RTree::dim_num() const {
  return (domain_ == nullptr) ? 0 : domain_->dim_num();
}

unsigned RTree::fanout() const {
  return fanout_;
}

TileOverlap RTree::get_tile_overlap(
    const NDRange& range, std::vector<bool>& is_default) const {
  TileOverlap overlap;

  // Empty tree
  if (domain_ == nullptr || levels_.empty())
    return overlap;

  // This will keep track of the traversal
  std::list<Entry> traversal;
  traversal.push_front({0, 0});
  auto leaf_num = levels_.back().size();
  auto height = this->height();

  while (!traversal.empty()) {
    // Get next entry
    auto entry = traversal.front();
    traversal.pop_front();
    const auto& mbr = levels_[entry.level_][entry.mbr_idx_];

    // Get overlap ratio
    auto ratio = domain_->overlap_ratio(range, is_default, mbr);

    // If there is overlap
    if (ratio != 0.0) {
      // If there is full overlap
      if (ratio == 1.0) {
        auto subtree_leaf_num = this->subtree_leaf_num(entry.level_);
        assert(subtree_leaf_num > 0);
        uint64_t start = entry.mbr_idx_ * subtree_leaf_num;
        uint64_t end = start + std::min(subtree_leaf_num, leaf_num - start) - 1;
        auto tile_range = std::pair<uint64_t, uint64_t>(start, end);
        overlap.tile_ranges_.emplace_back(tile_range);
      } else {  // Partial overlap
        // If this is the leaf level, insert into results
        if (entry.level_ == height - 1) {
          auto mbr_idx_ratio =
              std::pair<uint64_t, double>(entry.mbr_idx_, ratio);
          overlap.tiles_.emplace_back(mbr_idx_ratio);
        } else {  // Insert all "children" to traversal
          auto next_mbr_num = (uint64_t)levels_[entry.level_ + 1].size();
          auto start = entry.mbr_idx_ * fanout_;
          auto end = std::min(start + fanout_ - 1, next_mbr_num - 1);
          for (uint64_t i = start; i <= end; ++i)
            traversal.push_front({entry.level_ + 1, end - (i - start)});
        }
      }
    }
  }

  return overlap;
}

void RTree::compute_tile_bitmap(
    const Range& range, unsigned d, std::vector<uint8_t>* tile_bitmap) const {
  // Empty tree
  if (domain_ == nullptr || levels_.empty())
    return;

  // This will keep track of the traversal
  std::list<Entry> traversal;
  traversal.push_front({0, 0});
  auto leaf_num = levels_.back().size();
  auto height = this->height();

  while (!traversal.empty()) {
    // Get next entry
    auto entry = traversal.front();
    traversal.pop_front();
    const auto& mbr = levels_[entry.level_][entry.mbr_idx_];

    // If there is overlap
    if (domain_->dimension_ptr(d)->overlap(range, mbr[d])) {
      // If there is full overlap
      if (domain_->dimension_ptr(d)->covered(mbr[d], range)) {
        auto subtree_leaf_num = this->subtree_leaf_num(entry.level_);
        assert(subtree_leaf_num > 0);
        uint64_t start = entry.mbr_idx_ * subtree_leaf_num;
        uint64_t end = start + std::min(subtree_leaf_num, leaf_num - start);
        for (uint64_t i = start; i < end; i++) {
          tile_bitmap->at(i) = 1;
        }
      } else {  // Partial overlap
        // If this is the leaf level, insert into results
        if (entry.level_ == height - 1) {
          tile_bitmap->at(entry.mbr_idx_) = 1;
        } else {  // Insert all "children" to traversal
          auto next_mbr_num = (uint64_t)levels_[entry.level_ + 1].size();
          auto start = entry.mbr_idx_ * fanout_;
          auto end = std::min(start + fanout_ - 1, next_mbr_num - 1);
          for (uint64_t i = start; i <= end; ++i)
            traversal.push_front({entry.level_ + 1, end - (i - start)});
        }
      }
    }
  }
}

unsigned RTree::height() const {
  return (unsigned)levels_.size();
}

const NDRange& RTree::leaf(uint64_t leaf_idx) const {
  assert(leaf_idx < levels_.back().size());
  return levels_.back()[leaf_idx];
}

const std::vector<NDRange>& RTree::leaves() const {
  assert(!levels_.empty());
  return levels_.back();
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

void RTree::serialize(Serializer& serializer) const {
  serializer.write<uint32_t>(fanout_);
  auto level_num = (unsigned)levels_.size();
  serializer.write<uint32_t>(level_num);
  auto dim_num = domain_->dim_num();

  for (unsigned l = 0; l < level_num; ++l) {
    auto mbr_num = (uint64_t)levels_[l].size();
    serializer.write<uint64_t>(mbr_num);
    for (uint64_t m = 0; m < mbr_num; ++m) {
      for (unsigned d = 0; d < dim_num; ++d) {
        const auto& r = levels_[l][m][d];
        if (!domain_->dimension_ptr(d)->var_size()) {  // Fixed-sized
          // Just write the plain range
          serializer.write(r.data(), r.size());
        } else {  // Var-sized
          // range_size | start_size | range
          serializer.write<uint64_t>(r.size());
          serializer.write<uint64_t>(r.start_size());
          serializer.write(r.data(), r.size());
        }
      }
    }
  }
}

Status RTree::set_leaf(uint64_t leaf_id, const NDRange& mbr) {
  if (levels_.size() != 1)
    return LOG_STATUS(Status_RTreeError(
        "Cannot set leaf; There are more than one levels in the tree"));

  if (leaf_id >= levels_[0].size())
    return LOG_STATUS(Status_RTreeError("Cannot set leaf; Invalid lead index"));

  levels_[0][leaf_id] = mbr;

  return Status::Ok();
}

Status RTree::set_leaves(const std::vector<NDRange>& mbrs) {
  levels_.clear();
  levels_.resize(1);
  levels_[0] = mbrs;
  return Status::Ok();
}

Status RTree::set_leaf_num(uint64_t num) {
  // There should be exactly one level (the leaf level)
  if (levels_.size() != 1)
    levels_.resize(1);

  if (num < levels_[0].size())
    return LOG_STATUS(
        Status_RTreeError("Cannot set number of leaves; provided number "
                          "cannot be smaller than the current leaf number"));

  levels_[0].resize(num);
  return Status::Ok();
}

void RTree::deserialize(
    Deserializer& deserializer, const Domain* domain, uint32_t version) {
  if (version < 5) {
    deserialize_v1_v4(deserializer, domain);
    return;
  }

  deserialize_v5(deserializer, domain);
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

RTree::Level RTree::build_level(const Level& level) {
  auto cur_mbr_num = (uint64_t)level.size();
  Level new_level((uint64_t)std::ceil((double)cur_mbr_num / fanout_));
  auto new_mbr_num = (uint64_t)new_level.size();

  uint64_t mbrs_visited = 0;
  for (uint64_t i = 0; i < new_mbr_num; ++i) {
    auto mbr_num = std::min((uint64_t)fanout_, cur_mbr_num - mbrs_visited);
    for (uint64_t j = 0; j < mbr_num; ++j, ++mbrs_visited)
      domain_->expand_ndrange(level[mbrs_visited], &new_level[i]);
  }

  return new_level;
}

RTree RTree::clone() const {
  RTree clone;
  clone.domain_ = domain_;
  clone.fanout_ = fanout_;
  clone.levels_ = levels_;

  return clone;
}

void RTree::deserialize_v1_v4(
    Deserializer& deserializer, const Domain* domain) {
  deserialized_buffer_size_ = deserializer.size();

  // For backwards compatibility, ignored
  auto dim_num_i = deserializer.read<unsigned>();
  (void)dim_num_i;

  fanout_ = deserializer.read<unsigned>();

  // For backwards compatibility, ignored
  auto type_i = deserializer.read<uint8_t>();
  (void)type_i;

  auto level_num = deserializer.read<unsigned>();
  levels_.clear();
  levels_.resize(level_num);
  auto dim_num = domain->dim_num();
  for (unsigned l = 0; l < level_num; ++l) {
    auto mbr_num = deserializer.read<uint64_t>();
    levels_[l].resize(mbr_num);
    for (uint64_t m = 0; m < mbr_num; ++m) {
      levels_[l][m].resize(dim_num);
      for (unsigned d = 0; d < dim_num; ++d) {
        auto r_size{2 * domain->dimension_ptr(d)->coord_size()};
        auto data = deserializer.get_ptr<void>(r_size);
        levels_[l][m][d].set_range(data, r_size);
      }
    }
  }

  domain_ = domain;
}

void RTree::deserialize_v5(Deserializer& deserializer, const Domain* domain) {
  deserialized_buffer_size_ = deserializer.size();

  fanout_ = deserializer.read<unsigned>();
  auto level_num = deserializer.read<unsigned>();

  levels_.clear();
  levels_.resize(level_num);
  auto dim_num = domain->dim_num();

  for (unsigned l = 0; l < level_num; ++l) {
    auto mbr_num = deserializer.read<uint64_t>();
    levels_[l].resize(mbr_num);

    for (uint64_t m = 0; m < mbr_num; ++m) {
      levels_[l][m].resize(dim_num);
      for (unsigned d = 0; d < dim_num; ++d) {
        auto dim{domain->dimension_ptr(d)};
        if (!dim->var_size()) {  // Fixed-sized
          auto r_size = 2 * dim->coord_size();
          auto data = deserializer.get_ptr<void>(r_size);
          levels_[l][m][d].set_range(data, r_size);
        } else {  // Var-sized
          // range_size | start_size | range
          auto r_size = deserializer.read<uint64_t>();
          auto start_size = deserializer.read<uint64_t>();
          auto data = deserializer.get_ptr<void>(r_size);
          levels_[l][m][d].set_range(data, r_size, start_size);
        }
      }
    }
  }

  domain_ = domain;
}

void RTree::swap(RTree& rtree) {
  std::swap(domain_, rtree.domain_);
  std::swap(fanout_, rtree.fanout_);
  std::swap(levels_, rtree.levels_);
}

}  // namespace sm
}  // namespace tiledb
