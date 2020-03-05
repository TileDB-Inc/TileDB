/**
 * @file   subarray_partitioner.cc
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
 * This file implements class SubarrayPartitioner.
 */

#include "tiledb/sm/subarray/subarray_partitioner.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"

#include <iomanip>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifndef MAX
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#endif

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

SubarrayPartitioner::SubarrayPartitioner() = default;

SubarrayPartitioner::SubarrayPartitioner(
    const Subarray& subarray,
    uint64_t memory_budget,
    uint64_t memory_budget_var)
    : subarray_(subarray)
    , memory_budget_(memory_budget)
    , memory_budget_var_(memory_budget_var) {
  subarray_.compute_tile_overlap();
  state_.start_ = 0;
  auto range_num = subarray_.range_num();
  state_.end_ = (range_num > 0) ? range_num - 1 : 0;
}

SubarrayPartitioner::~SubarrayPartitioner() = default;

SubarrayPartitioner::SubarrayPartitioner(const SubarrayPartitioner& partitioner)
    : SubarrayPartitioner() {
  auto clone = partitioner.clone();
  swap(clone);
}

SubarrayPartitioner::SubarrayPartitioner(
    SubarrayPartitioner&& partitioner) noexcept
    : SubarrayPartitioner() {
  swap(partitioner);
}

SubarrayPartitioner& SubarrayPartitioner::operator=(
    const SubarrayPartitioner& partitioner) {
  auto clone = partitioner.clone();
  swap(clone);

  return *this;
}

SubarrayPartitioner& SubarrayPartitioner::operator=(
    SubarrayPartitioner&& partitioner) noexcept {
  swap(partitioner);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

Subarray& SubarrayPartitioner::current() {
  return current_.partition_;
}

const SubarrayPartitioner::PartitionInfo*
SubarrayPartitioner::current_partition_info() const {
  return &current_;
}

SubarrayPartitioner::PartitionInfo*
SubarrayPartitioner::current_partition_info() {
  return &current_;
}

bool SubarrayPartitioner::done() const {
  return subarray_.empty() || state_.start_ > state_.end_;
}

Status SubarrayPartitioner::get_result_budget(
    const char* attr_name, uint64_t* budget) const {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot get result budget; Attribute name cannot be null"));

  if (attr_name != constants::coords) {
    // Check attribute name
    auto attr = subarray_.array()->array_schema()->attribute(attr_name);
    if (attr == nullptr)
      return LOG_STATUS(Status::SubarrayPartitionerError(
          "Cannot get result budget; Invalid attribute"));

    // Check budget pointer
    if (budget == nullptr)
      return LOG_STATUS(Status::SubarrayPartitionerError(
          "Cannot get result budget; Invalid budget input"));

    // Check if the attribute is fixed-sized
    if (attr->var_size())
      return LOG_STATUS(Status::SubarrayPartitionerError(
          "Cannot get result budget; Attribute must be fixed-sized"));
  }

  // Check if budget has been set
  auto b_it = budget_.find(attr_name);
  if (b_it == budget_.end())
    return LOG_STATUS(Status::SubarrayPartitionerError(
        std::string(
            "Cannot get result budget; Budget not set for attribute '") +
        attr_name + "'"));

  // Get budget
  *budget = b_it->second.size_fixed_;

  return Status::Ok();
}

Status SubarrayPartitioner::get_result_budget(
    const char* attr_name, uint64_t* budget_off, uint64_t* budget_val) const {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot get result budget; Attribute name cannot be null"));

  if (attr_name == constants::coords)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot get result budget; Attribute must be var-sized"));

  // Check attribute
  auto attr = subarray_.array()->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot get result budget; Invalid attribute"));

  // Check budget pointer
  if (budget_off == nullptr || budget_val == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot get result budget; Invalid budget input"));

  // Check if the attribute is var-sized
  if (!attr->var_size())
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot get result budget; Attribute must be var-sized"));

  // Check if budget has been set
  auto b_it = budget_.find(attr_name);
  if (b_it == budget_.end())
    return LOG_STATUS(Status::SubarrayPartitionerError(
        std::string(
            "Cannot get result budget; Budget not set for attribute '") +
        attr_name + "'"));

  // Get budget
  *budget_off = b_it->second.size_fixed_;
  *budget_val = b_it->second.size_var_;

  return Status::Ok();
}

const std::unordered_map<std::string, SubarrayPartitioner::ResultBudget>*
SubarrayPartitioner::get_attr_result_budgets() const {
  return &budget_;
}

Status SubarrayPartitioner::get_memory_budget(
    uint64_t* budget, uint64_t* budget_var) const {
  *budget = memory_budget_;
  *budget_var = memory_budget_var_;
  return Status::Ok();
}

Status SubarrayPartitioner::next(bool* unsplittable) {
  *unsplittable = false;

  if (done())
    return Status::Ok();

  // Handle single range partitions, remaining from previous iteration
  if (!state_.single_range_.empty())
    return next_from_single_range(unsplittable);

  // Handle multi-range partitions, remaining from slab splits
  if (!state_.multi_range_.empty())
    return next_from_multi_range(unsplittable);

  // Find the [start, end] of the subarray ranges that fit in the budget
  bool interval_found;
  RETURN_NOT_OK(compute_current_start_end(&interval_found));

  // Single-range partition that must be split
  // Note: this applies only to UNORDERED and GLOBAL_ORDER layouts,
  // since otherwise we may have to calibrate the range start and end
  if (!interval_found && (subarray_.layout() == Layout::UNORDERED ||
                          subarray_.layout() == Layout::GLOBAL_ORDER))
    return next_from_single_range(unsplittable);

  // An interval of whole ranges that may need calibration
  bool must_split_slab;
  calibrate_current_start_end(&must_split_slab);

  // Handle case the next partition is composed of whole ND ranges
  if (interval_found && !must_split_slab) {
    current_.partition_ =
        subarray_.get_subarray(current_.start_, current_.end_);
    current_.split_multi_range_ = false;
    state_.start_ = current_.end_ + 1;

    return Status::Ok();
  }

  // Must split a multi-range subarray slab
  return next_from_multi_range(unsplittable);
}

Status SubarrayPartitioner::set_result_budget(
    const char* attr_name, uint64_t budget) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Attribute name cannot be null"));

  if (attr_name != constants::coords) {
    // Check attribute
    auto attr = subarray_.array()->array_schema()->attribute(attr_name);
    if (attr == nullptr)
      return LOG_STATUS(Status::SubarrayPartitionerError(
          std::string("Cannot set result budget; Invalid attribute '") +
          attr_name + "'"));

    // Check if the attribute is fixed-sized
    if (attr->var_size())
      return LOG_STATUS(Status::SubarrayPartitionerError(
          "Cannot set result budget; Attribute must be fixed-sized"));
  }

  budget_[attr_name] = ResultBudget{budget, 0};

  return Status::Ok();
}

Status SubarrayPartitioner::set_result_budget(
    const char* attr_name, uint64_t budget_off, uint64_t budget_val) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Attribute name cannot be null"));

  if (attr_name == constants::coords)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Attribute must be var-sized"));

  // Check attribute
  auto attr = subarray_.array()->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        std::string("Cannot set result budget; Invalid attribute '") +
        attr_name + "'"));

  // Check if the attribute is var-sized
  if (!attr->var_size())
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Attribute must be var-sized"));

  budget_[attr_name] = ResultBudget{budget_off, budget_val};

  return Status::Ok();
}

Status SubarrayPartitioner::set_memory_budget(
    uint64_t budget, uint64_t budget_var) {
  memory_budget_ = budget;
  memory_budget_var_ = budget_var;
  return Status::Ok();
}

Status SubarrayPartitioner::split_current(bool* unsplittable) {
  *unsplittable = false;

  // Current came from splitting a multi-range partition
  if (current_.split_multi_range_) {
    if (state_.multi_range_.empty())
      state_.start_ = current_.start_;
    state_.multi_range_.push_front(current_.partition_);
    split_top_multi_range(unsplittable);
    return next_from_multi_range(unsplittable);
  }

  // Current came from retrieving a multi-range partition from subarray
  if (current_.start_ < current_.end_) {
    auto range_num = (current_.end_ - current_.start_ + 1);
    assert(1 - constants::multi_range_reduction_in_split <= 1);
    auto new_range_num =
        range_num * (1 - constants::multi_range_reduction_in_split);
    current_.end_ = current_.start_ + (uint64_t)new_range_num - 1;
    current_.partition_ =
        subarray_.get_subarray(current_.start_, current_.end_);
    state_.start_ = current_.end_ + 1;
    return Status::Ok();
  }

  // Current came from splitting a single-range partition
  if (state_.single_range_.empty())
    state_.start_--;
  state_.single_range_.push_front(current_.partition_);
  split_top_single_range(unsplittable);
  return next_from_single_range(unsplittable);
}

const SubarrayPartitioner::State* SubarrayPartitioner::state() const {
  return &state_;
}

SubarrayPartitioner::State* SubarrayPartitioner::state() {
  return &state_;
}

const Subarray* SubarrayPartitioner::subarray() const {
  return &subarray_;
}

Subarray* SubarrayPartitioner::subarray() {
  return &subarray_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void SubarrayPartitioner::calibrate_current_start_end(bool* must_split_slab) {
  // Initialize (may be reset below)
  *must_split_slab = false;

  // Special case of single range and global layout
  if (subarray_.layout() == Layout::GLOBAL_ORDER) {
    assert(current_.start_ == current_.end_);
    return;
  }

  auto start_coords = subarray_.get_range_coords(current_.start_);
  auto end_coords = subarray_.get_range_coords(current_.end_);

  std::vector<uint64_t> range_num;
  auto dim_num = subarray_.dim_num();
  uint64_t num;
  for (unsigned i = 0; i < dim_num; ++i) {
    subarray_.get_range_num(i, &num);
    range_num.push_back(num);
  }

  auto layout = subarray_.layout();
  auto cell_order = subarray_.array()->array_schema()->cell_order();
  layout = (layout == Layout::UNORDERED) ? cell_order : layout;
  assert(layout == Layout::ROW_MAJOR || layout == Layout::COL_MAJOR);

  for (unsigned d = 0; d < dim_num - 1; ++d) {
    unsigned major_dim = (layout == Layout::ROW_MAJOR) ? d : dim_num - d - 1;
    std::vector<unsigned> minor_dims;
    if (layout == Layout::ROW_MAJOR) {
      for (unsigned i = major_dim + 1; i < dim_num; ++i)
        minor_dims.push_back(i);
    } else {
      for (unsigned i = major_dim - 1;; --i) {
        minor_dims.push_back(i);
        if (i == 0)
          break;
      }
    }

    bool start_minor_coords_at_beginning = true;
    for (auto dim : minor_dims) {
      if (start_coords[dim] != 0) {
        start_minor_coords_at_beginning = false;
        break;
      }
    }

    bool end_minor_coords_at_end = true;
    for (auto dim : minor_dims) {
      if (end_coords[dim] != range_num[dim] - 1) {
        end_minor_coords_at_end = false;
        break;
      }
    }

    if (start_minor_coords_at_beginning) {
      if (end_minor_coords_at_end) {
        break;
      } else if (start_coords[major_dim] < end_coords[major_dim]) {
        end_coords[major_dim]--;
        for (auto dim : minor_dims)
          end_coords[dim] = range_num[dim] - 1;
        break;
      } else {
        // (!end_minor_coords_at_end &&
        // start_coords[major_dim] == end_coords[major_dim])
        // Do nothing and proceed to the next iteration of the loop
      }
    } else {
      if (end_coords[major_dim] > start_coords[major_dim]) {
        end_coords[major_dim] = start_coords[major_dim];
        for (auto dim : minor_dims)
          end_coords[dim] = range_num[dim] - 1;
      }
    }
  }

  // Calibrate the range to a slab if layout is row-/col-major
  if (dim_num > 1 && subarray_.layout() != Layout::UNORDERED) {
    auto d = (subarray_.layout() == Layout::ROW_MAJOR) ? dim_num - 1 : 0;
    if (end_coords[d] != range_num[d] - 1) {
      end_coords[d] = range_num[d] - 1;
      *must_split_slab = true;
    }
  }

  // Get current_.end_. based on end_coords
  current_.end_ = subarray_.range_idx(end_coords);
}

SubarrayPartitioner SubarrayPartitioner::clone() const {
  SubarrayPartitioner clone;
  clone.subarray_ = subarray_;
  clone.budget_ = budget_;
  clone.current_ = current_;
  clone.state_ = state_;
  clone.memory_budget_ = memory_budget_;
  clone.memory_budget_var_ = memory_budget_var_;

  return clone;
}

Status SubarrayPartitioner::compute_current_start_end(bool* found) {
  // Preparation
  auto array_schema = subarray_.array()->array_schema();
  std::unordered_map<std::string, ResultBudget> cur_sizes, mem_sizes;
  for (const auto& it : budget_) {
    cur_sizes[it.first] = ResultBudget();  // Est budget
    mem_sizes[it.first] = ResultBudget();  // Max memory budget
  }

  current_.start_ = state_.start_;
  for (current_.end_ = state_.start_; current_.end_ <= state_.end_;
       ++current_.end_) {
    // Update current sizes
    for (const auto& budget_it : budget_) {
      auto attr_name = budget_it.first;
      auto var_size = array_schema->var_size(attr_name);
      Subarray::ResultSize est_size;
      RETURN_NOT_OK(subarray_.compute_est_result_size(
          attr_name, current_.end_, var_size, &est_size));
      auto& cur_size = cur_sizes[attr_name];
      auto& mem_size = mem_sizes[attr_name];
      cur_size.size_fixed_ += (uint64_t)ceil(est_size.size_fixed_);
      cur_size.size_var_ += (uint64_t)ceil(est_size.size_var_);
      mem_size.size_fixed_ += (uint64_t)ceil(est_size.mem_size_fixed_);
      mem_size.size_var_ += (uint64_t)ceil(est_size.mem_size_var_);
      if (cur_size.size_fixed_ > budget_it.second.size_fixed_ ||
          cur_size.size_var_ > budget_it.second.size_var_ ||
          mem_size.size_fixed_ > memory_budget_ ||
          mem_size.size_var_ > memory_budget_var_) {
        // Cannot find range that fits in the buffer
        if (current_.end_ == current_.start_) {
          *found = false;
          return Status::Ok();
        }

        // Range found, make it inclusive
        current_.end_--;
        *found = true;
        return Status::Ok();
      }
    }
  }

  // Range found, make it inclusive
  current_.end_--;
  *found = true;

  return Status::Ok();
}

void SubarrayPartitioner::compute_splitting_value_on_tiles(
    const Subarray& range,
    unsigned* splitting_dim,
    ByteVecValue* splitting_value,
    bool* unsplittable) {
  assert(range.layout() == Layout::GLOBAL_ORDER);
  *unsplittable = true;

  // For easy reference
  auto array_schema = subarray_.array()->array_schema();
  auto dim_num = subarray_.array()->array_schema()->dim_num();
  auto layout = subarray_.array()->array_schema()->tile_order();
  *splitting_dim = UINT32_MAX;

  std::vector<unsigned> dims;
  if (layout == Layout::ROW_MAJOR) {
    for (unsigned i = 0; i < dim_num; ++i)
      dims.push_back(i);
  } else {
    for (unsigned i = 0; i < dim_num; ++i)
      dims.push_back(dim_num - i - 1);
  }

  // Compute splitting dimension and value
  const Range* r;
  for (auto d : dims) {
    auto dim = array_schema->domain()->dimension(d);
    range.get_range(d, 0, &r);
    auto tiles_apart = dim->tile_num(*r) - 1;
    if (tiles_apart != 0) {
      *splitting_dim = d;
      dim->ceil_to_tile(
          *r, MAX(1, floor(tiles_apart / 2)) - 1, splitting_value);
      *unsplittable = false;
      break;
    }
  }
}

// TODO (sp): in the future this can be more sophisticated, taking into
// TODO (sp): account MBRs (i.e., the distirbution of the data) as well
void SubarrayPartitioner::compute_splitting_value_single_range(
    const Subarray& range,
    unsigned* splitting_dim,
    ByteVecValue* splitting_value,
    bool* unsplittable) {
  // Special case for global order
  if (subarray_.layout() == Layout::GLOBAL_ORDER) {
    compute_splitting_value_on_tiles(
        range, splitting_dim, splitting_value, unsplittable);

    // Splitting dim/value found
    if (!*unsplittable)
      return;

    // Else `range` is contained within a tile.
    // The rest of the function will find the splitting dim/value
  }

  // For easy reference
  auto array_schema = subarray_.array()->array_schema();
  auto dim_num = array_schema->dim_num();
  auto cell_order = array_schema->cell_order();
  assert(!range.is_unary());
  auto layout = subarray_.layout();
  layout = (layout == Layout::UNORDERED || layout == Layout::GLOBAL_ORDER) ?
               cell_order :
               layout;
  *splitting_dim = UINT32_MAX;

  std::vector<unsigned> dims;
  if (layout == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num; ++d)
      dims.push_back(d);
  } else {
    for (unsigned d = 0; d < dim_num; ++d)
      dims.push_back(dim_num - d - 1);
  }

  // Compute splitting dimension and value
  const Range* r;
  for (auto d : dims) {
    auto dim = array_schema->dimension(d);
    range.get_range(d, 0, &r);
    if (!r->unary()) {
      *splitting_dim = d;
      dim->splitting_value(*r, splitting_value, unsplittable);

      // Splitting dim/value found
      if (!*unsplittable)
        break;

      // Else continue to the next dimension
    }
  }

  assert(*splitting_dim != UINT32_MAX);
}

void SubarrayPartitioner::compute_splitting_value_multi_range(
    unsigned* splitting_dim,
    uint64_t* splitting_range,
    ByteVecValue* splitting_value,
    bool* unsplittable) {
  const auto& partition = state_.multi_range_.front();

  // Single-range partittion
  if (partition.range_num() == 1) {
    compute_splitting_value_single_range(
        partition, splitting_dim, splitting_value, unsplittable);
    return;
  }

  // Multi-range partition
  auto layout = subarray_.layout();
  auto array_schema = subarray_.array()->array_schema();
  auto dim_num = array_schema->dim_num();
  auto cell_order = array_schema->cell_order();
  layout = (layout == Layout::UNORDERED) ? cell_order : layout;
  *splitting_dim = UINT32_MAX;
  uint64_t range_num;

  std::vector<unsigned> dims;
  if (layout == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num; ++d)
      dims.push_back(d);
  } else {
    for (unsigned d = 0; d < dim_num; ++d)
      dims.push_back(dim_num - d - 1);
  }

  // Compute splitting dimension, range and value
  const Range* r;
  for (auto d : dims) {
    // Check if we need to split the multiple ranges
    partition.get_range_num(d, &range_num);
    if (range_num > 1) {
      assert(d == dims.back());
      *splitting_dim = d;
      *splitting_range = (range_num - 1) / 2;
      *unsplittable = false;
      break;
    }

    // Check if we need to split single range
    partition.get_range(d, 0, &r);
    auto dim = array_schema->dimension(d);
    if (!r->unary()) {
      *splitting_dim = d;
      dim->splitting_value(*r, splitting_value, unsplittable);
      break;
    }
  }

  assert(*splitting_dim != UINT32_MAX);
}

bool SubarrayPartitioner::must_split(Subarray* partition) {
  auto array_schema = subarray_.array()->array_schema();
  bool must_split = false;

  uint64_t size_fixed, size_var, mem_size_fixed, mem_size_var;
  for (const auto& b : budget_) {
    // Compute max sizes
    auto attr_name = b.first;
    auto var_size = array_schema->var_size(attr_name);

    // Compute est sizes
    size_fixed = 0;
    size_var = 0;
    mem_size_fixed = 0;
    mem_size_var = 0;
    if (var_size) {
      partition->get_est_result_size(b.first.c_str(), &size_fixed, &size_var);
      partition->get_max_memory_size(
          b.first.c_str(), &mem_size_fixed, &mem_size_var);
    } else {
      partition->get_est_result_size(b.first.c_str(), &size_fixed);
      partition->get_max_memory_size(b.first.c_str(), &mem_size_fixed);
    }

    // Check for budget overflow
    if (size_fixed > b.second.size_fixed_ || size_var > b.second.size_var_ ||
        mem_size_fixed > memory_budget_ || mem_size_var > memory_budget_var_) {
      must_split = true;
      break;
    }
  }

  return must_split;
}

Status SubarrayPartitioner::next_from_multi_range(bool* unsplittable) {
  // A new multi-range subarray may need to be put in the list and split
  if (state_.multi_range_.empty()) {
    auto s = subarray_.get_subarray(current_.start_, current_.end_);
    state_.multi_range_.push_front(std::move(s));
    split_top_multi_range(unsplittable);
  }

  // Loop until you find a partition that fits or unsplittable
  if (!*unsplittable) {
    bool must_split;
    do {
      auto& partition = state_.multi_range_.front();
      must_split = this->must_split(&partition);
      if (must_split)
        RETURN_NOT_OK(split_top_multi_range(unsplittable));
    } while (must_split && !*unsplittable);
  }

  // At this point, the top mulit-range is the next partition
  current_.partition_ = std::move(state_.multi_range_.front());
  current_.split_multi_range_ = true;
  state_.multi_range_.pop_front();
  if (state_.multi_range_.empty())
    state_.start_ = current_.end_ + 1;

  return Status::Ok();
}

Status SubarrayPartitioner::next_from_single_range(bool* unsplittable) {
  // Handle case where a new single range must be put in the list and split
  if (state_.single_range_.empty()) {
    auto s = subarray_.get_subarray(current_.start_, current_.end_);
    state_.single_range_.push_front(std::move(s));
    split_top_single_range(unsplittable);
  }

  // Loop until you find a partition that fits or unsplittable
  if (!*unsplittable) {
    bool must_split;
    do {
      auto& partition = state_.single_range_.front();
      must_split = this->must_split(&partition);
      if (must_split)
        RETURN_NOT_OK(split_top_single_range(unsplittable));
    } while (must_split && !*unsplittable);
  }

  // At this point, the top range is the next partition
  current_.partition_ = std::move(state_.single_range_.front());
  current_.split_multi_range_ = false;
  state_.single_range_.pop_front();
  if (state_.single_range_.empty())
    state_.start_++;

  return Status::Ok();
}

Status SubarrayPartitioner::split_top_single_range(bool* unsplittable) {
  // For easy reference
  const auto& range = state_.single_range_.front();

  // Check if unsplittable
  if (range.is_unary()) {
    *unsplittable = true;
    return Status::Ok();
  }

  // Finding splitting value
  ByteVecValue splitting_value;
  unsigned splitting_dim;
  compute_splitting_value_single_range(
      range, &splitting_dim, &splitting_value, unsplittable);

  if (*unsplittable)
    return Status::Ok();

  // Split remaining range into two ranges
  Subarray r1, r2;
  RETURN_NOT_OK(range.split(splitting_dim, splitting_value, &r1, &r2));

  // Update list
  state_.single_range_.pop_front();
  state_.single_range_.push_front(std::move(r2));
  state_.single_range_.push_front(std::move(r1));

  return Status::Ok();
}

Status SubarrayPartitioner::split_top_multi_range(bool* unsplittable) {
  // For easy reference
  const auto& partition = state_.multi_range_.front();

  // Check if unsplittable
  if (partition.is_unary()) {
    *unsplittable = true;
    return Status::Ok();
  }

  // Finding splitting value
  unsigned splitting_dim;
  uint64_t splitting_range = UINT64_MAX;
  ByteVecValue splitting_value;
  compute_splitting_value_multi_range(
      &splitting_dim, &splitting_range, &splitting_value, unsplittable);

  if (*unsplittable)
    return Status::Ok();

  // Split partition into two partitions
  Subarray p1(subarray_.array(), subarray_.layout());
  Subarray p2(subarray_.array(), subarray_.layout());
  RETURN_NOT_OK(partition.split(
      splitting_range, splitting_dim, splitting_value, &p1, &p2));

  // Update list
  state_.multi_range_.pop_front();
  state_.multi_range_.push_front(std::move(p2));
  state_.multi_range_.push_front(std::move(p1));

  return Status::Ok();
}

void SubarrayPartitioner::swap(SubarrayPartitioner& partitioner) {
  std::swap(subarray_, partitioner.subarray_);
  std::swap(budget_, partitioner.budget_);
  std::swap(current_, partitioner.current_);
  std::swap(state_, partitioner.state_);
  std::swap(memory_budget_, partitioner.memory_budget_);
  std::swap(memory_budget_var_, partitioner.memory_budget_var_);
}

}  // namespace sm
}  // namespace tiledb
