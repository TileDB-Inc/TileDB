/**
 * @file   rtree.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <iostream>
#include <list>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

RTree::RTree() {
  domain_ = nullptr;
  fanout_ = 0;
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

Status RTree::build_tree() {
  if (levels_.empty())
    return Status::Ok();

  assert(levels_.size() == 1);

  auto leaf_num = levels_[0].size();
  assert(leaf_num >= 1);
  if (leaf_num == 1)
    return Status::Ok();

  // Build the tree bottom up
  auto height = (size_t)ceil(utils::math::log(fanout_, leaf_num)) + 1;
  for (size_t i = 0; i < height - 1; ++i) {
    auto new_level = build_level(levels_.back());
    levels_.emplace_back(new_level);
  }

  // Make the root as the first level
  std::reverse(std::begin(levels_), std::end(levels_));

  return Status::Ok();
}

unsigned RTree::dim_num() const {
  return (domain_ == nullptr) ? 0 : domain_->dim_num();
}

const Domain* RTree::domain() const {
  return domain_;
}

unsigned RTree::fanout() const {
  return fanout_;
}

TileOverlap RTree::get_tile_overlap(const NDRange& range) const {
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
    auto ratio = domain_->overlap_ratio(range, mbr);

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

Status RTree::serialize(Buffer* buff) const {
  RETURN_NOT_OK(buff->write(&fanout_, sizeof(fanout_)));
  auto level_num = (unsigned)levels_.size();
  RETURN_NOT_OK(buff->write(&level_num, sizeof(level_num)));
  auto dim_num = domain_->dim_num();

  for (unsigned l = 0; l < level_num; ++l) {
    auto mbr_num = (uint64_t)levels_[l].size();
    RETURN_NOT_OK(buff->write(&mbr_num, sizeof(uint64_t)));
    for (uint64_t m = 0; m < mbr_num; ++m) {
      for (unsigned d = 0; d < dim_num; ++d) {
        const auto& r = levels_[l][m][d];
        auto dim = domain_->dimension(d);
        if (!dim->var_size()) {  // Fixed-sized
          // Just write the plain range
          RETURN_NOT_OK(buff->write(r.data(), r.size()));
        } else {  // Var-sized
          // range_size | start_size | range
          auto size = r.size();
          auto start_size = r.start_size();
          RETURN_NOT_OK(buff->write(&size, sizeof(uint64_t)));
          RETURN_NOT_OK(buff->write(&start_size, sizeof(uint64_t)));
          RETURN_NOT_OK(buff->write(r.data(), size));
        }
      }
    }
  }

  return Status::Ok();
}

Status RTree::set_leaf(uint64_t leaf_id, const NDRange& mbr) {
  if (levels_.size() != 1)
    return LOG_STATUS(Status::RTreeError(
        "Cannot set leaf; There are more than one levels in the tree"));

  if (leaf_id >= levels_[0].size())
    return LOG_STATUS(
        Status::RTreeError("Cannot set leaf; Invalid lead index"));

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
        Status::RTreeError("Cannot set number of leaves; provided number "
                           "cannot be smaller than the current leaf number"));

  levels_[0].resize(num);
  return Status::Ok();
}

Status RTree::deserialize(
    ConstBuffer* cbuff, const Domain* domain, uint32_t version) {
  if (version < 5)
    return deserialize_v1_v4(cbuff, domain);
  return deserialize_v5(cbuff, domain);
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

RTree::Level RTree::build_level(const Level& level) {
  auto cur_mbr_num = (uint64_t)level.size();
  Level new_level((uint64_t)ceil((double)cur_mbr_num / fanout_));
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

Status RTree::deserialize_v1_v4(ConstBuffer* cbuff, const Domain* domain) {
  // For backwards compatibility, they will be ignored
  unsigned dim_num_i;
  uint8_t type_i;

  RETURN_NOT_OK(cbuff->read(&dim_num_i, sizeof(dim_num_i)));
  RETURN_NOT_OK(cbuff->read(&fanout_, sizeof(fanout_)));
  RETURN_NOT_OK(cbuff->read(&type_i, sizeof(type_i)));
  unsigned level_num;
  RETURN_NOT_OK(cbuff->read(&level_num, sizeof(level_num)));

  levels_.clear();
  levels_.resize(level_num);
  auto dim_num = domain->dim_num();
  uint64_t mbr_num;
  for (unsigned l = 0; l < level_num; ++l) {
    RETURN_NOT_OK(cbuff->read(&mbr_num, sizeof(uint64_t)));
    levels_[l].resize(mbr_num);
    for (uint64_t m = 0; m < mbr_num; ++m) {
      levels_[l][m].resize(dim_num);
      for (unsigned d = 0; d < dim_num; ++d) {
        auto r_size = 2 * domain->dimension(d)->coord_size();
        levels_[l][m][d].set_range(cbuff->cur_data(), r_size);
        cbuff->advance_offset(r_size);
      }
    }
  }

  domain_ = domain;

  return Status::Ok();
}

Status RTree::deserialize_v5(ConstBuffer* cbuff, const Domain* domain) {
  RETURN_NOT_OK(cbuff->read(&fanout_, sizeof(fanout_)));
  unsigned level_num;
  RETURN_NOT_OK(cbuff->read(&level_num, sizeof(level_num)));

  if (level_num == 0)
    return Status::Ok();

  levels_.clear();
  levels_.resize(level_num);
  auto dim_num = domain->dim_num();
  uint64_t mbr_num;
  for (unsigned l = 0; l < level_num; ++l) {
    RETURN_NOT_OK(cbuff->read(&mbr_num, sizeof(uint64_t)));
    levels_[l].resize(mbr_num);
    for (uint64_t m = 0; m < mbr_num; ++m) {
      levels_[l][m].resize(dim_num);
      for (unsigned d = 0; d < dim_num; ++d) {
        auto dim = domain_->dimension(d);
        if (!dim->var_size()) {  // Fixed-sized
          auto r_size = 2 * domain->dimension(d)->coord_size();
          levels_[l][m][d].set_range(cbuff->cur_data(), r_size);
          cbuff->advance_offset(r_size);
        } else {  // Var-sized
          // range_size | start_size | range
          uint64_t r_size, start_size;
          RETURN_NOT_OK(cbuff->read(&r_size, sizeof(uint64_t)));
          RETURN_NOT_OK(cbuff->read(&start_size, sizeof(uint64_t)));
          levels_[l][m][d].set_range(cbuff->cur_data(), r_size, start_size);
          cbuff->advance_offset(r_size);
        }
      }
    }
  }

  domain_ = domain;

  return Status::Ok();
}

void RTree::swap(RTree& rtree) {
  std::swap(domain_, rtree.domain_);
  std::swap(fanout_, rtree.fanout_);
  std::swap(levels_, rtree.levels_);
}

}  // namespace sm
}  // namespace tiledb
