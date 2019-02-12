/**
 * @file   subarray_partitioner.cc
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
 * This file implements class SubarrayPartitioner.
 */

#include "tiledb/sm/subarray/subarray_partitioner.h"
#include <iomanip>
#include "tiledb/sm/array/array.h"

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

SubarrayPartitioner::SubarrayPartitioner(const Subarray& subarray)
    : subarray_(subarray) {
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

SubarrayPartitioner::SubarrayPartitioner(SubarrayPartitioner&& partitioner)
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
    SubarrayPartitioner&& partitioner) {
  swap(partitioner);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

const Subarray& SubarrayPartitioner::current() const {
  return current_.partition_;
}

bool SubarrayPartitioner::done() const {
  return subarray_.empty() || state_.start_ > state_.end_;
}

Status SubarrayPartitioner::get_result_budget(
    const char* attr_name, uint64_t* budget) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot get result budget; Invalid attribute"));

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
        "Cannot get result budget; Budget not set for the input attribute"));

  // Get budget
  *budget = b_it->second.size_fixed_;

  return Status::Ok();
}

Status SubarrayPartitioner::get_result_budget(
    const char* attr_name, uint64_t* budget_off, uint64_t* budget_val) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot get result budget; Invalid attribute"));

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
        "Cannot get result budget; Budget not set for the input attribute"));

  // Get budget
  *budget_off = b_it->second.size_fixed_;
  *budget_val = b_it->second.size_var_;

  return Status::Ok();
}

Status SubarrayPartitioner::next(bool* unsplittable) {
  auto type = subarray_.array()->array_schema()->domain()->type();
  switch (type) {
    case Datatype::INT8:
      return next<int8_t>(unsplittable);
    case Datatype::UINT8:
      return next<uint8_t>(unsplittable);
    case Datatype::INT16:
      return next<int16_t>(unsplittable);
    case Datatype::UINT16:
      return next<uint16_t>(unsplittable);
    case Datatype::INT32:
      return next<int32_t>(unsplittable);
    case Datatype::UINT32:
      return next<uint32_t>(unsplittable);
    case Datatype::INT64:
      return next<int64_t>(unsplittable);
    case Datatype::UINT64:
      return next<uint64_t>(unsplittable);
    case Datatype::FLOAT32:
      return next<float>(unsplittable);
    case Datatype::FLOAT64:
      return next<double>(unsplittable);
    default:
      return LOG_STATUS(Status::SubarrayPartitionerError(
          "Cannot get next partition; Unsupported subarray domain type"));
  }

  return Status::Ok();
}

template <class T>
Status SubarrayPartitioner::next(bool* unsplittable) {
  *unsplittable = false;

  if (done())
    return Status::Ok();

  // Handle single range partitions, remaining from previous iteration
  if (!state_.single_range_.empty())
    return next_from_single_range<T>(unsplittable);

  // Handle multi-range partitions, remaining from slab splits
  if (!state_.multi_range_.empty())
    return next_from_multi_range<T>(unsplittable);

  // Find the [start, end] of the subarray ranges that fit in the budget
  bool interval_found = compute_current_start_end<T>();

  // Single-range partition that must be split
  if (!interval_found && subarray_.layout() == Layout::UNORDERED)
    return next_from_single_range<T>(unsplittable);

  // An interval of whole ranges that may need calibration
  bool must_split_slab;
  calibrate_current_start_end(&must_split_slab);

  // Handle case the next partition is composed of whole ND ranges
  if (interval_found && !must_split_slab) {
    current_.partition_ =
        std::move(subarray_.get_subarray<T>(current_.start_, current_.end_));
    current_.split_multi_range_ = false;
    state_.start_ = current_.end_ + 1;
    return Status::Ok();
  }

  // Must split a multi-range subarray slab
  return next_from_multi_range<T>(unsplittable);
}

Status SubarrayPartitioner::set_result_budget(
    const char* attr_name, uint64_t budget) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Invalid attribute"));

  if (attr_name != constants::coords) {
    // Check attribute
    auto attr = subarray_.array()->array_schema()->attribute(attr_name);
    if (attr == nullptr)
      return LOG_STATUS(Status::SubarrayPartitionerError(
          "Cannot set result budget; Invalid attribute"));

    // Check if the attribute is fixed-sized
    if (attr->var_size())
      return LOG_STATUS(Status::SubarrayPartitionerError(
          "Cannot set result budget; Attribute must be fixed-sized"));
  }

  budget_[attr_name] = {budget, 0};

  return Status::Ok();
}

Status SubarrayPartitioner::set_result_budget(
    const char* attr_name, uint64_t budget_off, uint64_t budget_val) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Invalid attribute"));

  if (attr_name == constants::coords)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Attribute must be var-sized"));

  // Check attribute
  auto attr = subarray_.array()->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Invalid attribute"));

  // Check if the attribute is var-sized
  if (!attr->var_size())
    return LOG_STATUS(Status::SubarrayPartitionerError(
        "Cannot set result budget; Attribute must be var-sized"));

  budget_[attr_name] = {budget_off, budget_val};

  return Status::Ok();
}

template <class T>
Status SubarrayPartitioner::split_current(bool* unsplittable) {
  *unsplittable = false;

  // Current came from splitting a multi-range partition
  if (current_.split_multi_range_) {
    if (state_.multi_range_.empty())
      state_.start_ = current_.start_;
    state_.multi_range_.push_front(current_.partition_);
    split_top_multi_range<T>(unsplittable);
    return next_from_multi_range<T>(unsplittable);
  }

  // Current came from retrieving a multi-range partition from subarray
  if (current_.start_ < current_.end_) {
    auto range_num = (current_.end_ - current_.start_ + 1);
    assert(1 - constants::multi_range_reduction_in_split <= 1);
    auto new_range_num =
        range_num * (1 - constants::multi_range_reduction_in_split);
    current_.end_ = current_.start_ + (uint64_t)new_range_num - 1;
    current_.partition_ =
        std::move(subarray_.get_subarray<T>(current_.start_, current_.end_));
    state_.start_ = current_.end_ + 1;
    return Status::Ok();
  }

  // Current came from splitting a single-range partition
  if (state_.single_range_.empty())
    state_.start_--;
  state_.single_range_.push_front(current_.partition_);
  split_top_single_range<T>(unsplittable);
  return next_from_single_range<T>(unsplittable);
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void SubarrayPartitioner::calibrate_current_start_end(bool* must_split_slab) {
  auto start_coords = subarray_.get_range_coords(current_.start_);
  auto end_coords = subarray_.get_range_coords(current_.end_);

  // Initialize (may be reset below)
  *must_split_slab = false;

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

  return clone;
}

template <class T>
bool SubarrayPartitioner::compute_current_start_end() {
  // Preparation
  auto array_schema = subarray_.array()->array_schema();
  std::unordered_map<std::string, ResultBudget> cur_sizes;
  for (const auto& it : budget_)
    cur_sizes[it.first] = ResultBudget{0, 0};

  current_.start_ = state_.start_;
  for (current_.end_ = state_.start_; current_.end_ <= state_.end_;
       ++current_.end_) {
    // Update current sizes
    for (const auto& budget_it : budget_) {
      auto attr_name = budget_it.first;
      auto var_size = array_schema->var_size(attr_name);
      auto est_size = subarray_.compute_est_result_size<T>(
          attr_name, current_.end_, var_size);
      auto& cur_size = cur_sizes[attr_name];
      cur_size.size_fixed_ += est_size.size_fixed_;
      cur_size.size_var_ += est_size.size_var_;

      if (cur_size.size_fixed_ > budget_it.second.size_fixed_ ||
          cur_size.size_var_ > budget_it.second.size_var_) {
        // Cannot find range that fits in the buffer
        if (current_.end_ == current_.start_)
          return false;

        // Range found, make it inclusive
        current_.end_--;
        return true;
      }
    }
  }

  // Range found, make it inclusive
  current_.end_--;
  return true;
}

// TODO (sp): in the future this can be more sophisticated, taking into
// TODO (sp): account MBRs (i.e., the distirbution of the data) as well
template <class T>
void SubarrayPartitioner::compute_splitting_point_single_range(
    const Subarray& range,
    unsigned* splitting_dim,
    T* splitting_point,
    bool* unsplittable) {
  // For easy reference
  auto layout = subarray_.layout();
  auto dim_num = subarray_.array()->array_schema()->dim_num();
  auto cell_order = subarray_.array()->array_schema()->cell_order();
  assert(!range.is_unary());
  layout = (layout == Layout::UNORDERED) ? cell_order : layout;
  const void* r_v;
  *splitting_dim = UINT32_MAX;

  std::vector<unsigned> dims;
  if (layout == Layout::ROW_MAJOR) {
    for (unsigned i = 0; i < dim_num; ++i)
      dims.push_back(i);
  } else {
    for (unsigned i = 0; i < dim_num; ++i)
      dims.push_back(dim_num - i - 1);
  }

  // Compute splitting dimension and point
  for (auto i : dims) {
    range.get_range(i, 0, &r_v);
    auto r = (T*)r_v;
    if (std::memcmp(r, &r[1], sizeof(T)) != 0) {
      *splitting_dim = i;
      *splitting_point = r[0] + (r[1] - r[0]) / 2;
      *unsplittable = !std::memcmp(splitting_point, &r[1], sizeof(T));
      break;
    }
  }

  assert(*splitting_dim != UINT32_MAX);
}

template <class T>
void SubarrayPartitioner::compute_splitting_point_multi_range(
    unsigned* splitting_dim,
    uint64_t* splitting_range,
    T* splitting_point,
    bool* unsplittable) {
  const auto& partition = state_.multi_range_.front();

  // Single-range partittion
  if (partition.range_num() == 1) {
    compute_splitting_point_single_range(
        partition, splitting_dim, splitting_point, unsplittable);
    return;
  }

  // Multi-range partition
  auto layout = subarray_.layout();
  auto dim_num = subarray_.array()->array_schema()->dim_num();
  auto cell_order = subarray_.array()->array_schema()->cell_order();
  layout = (layout == Layout::UNORDERED) ? cell_order : layout;
  const void* r_v;
  *splitting_dim = UINT32_MAX;
  uint64_t range_num;

  std::vector<unsigned> dims;
  if (layout == Layout::ROW_MAJOR) {
    for (unsigned i = 0; i < dim_num; ++i)
      dims.push_back(i);
  } else {
    for (unsigned i = 0; i < dim_num; ++i)
      dims.push_back(dim_num - i - 1);
  }

  // Compute splitting dimension, range and point
  for (auto i : dims) {
    // Check if we need to split the multiple ranges
    partition.get_range_num(i, &range_num);
    if (range_num > 1) {
      assert(i == dims.back());
      *splitting_dim = i;
      *splitting_range = (range_num - 1) / 2;
      *unsplittable = false;
      break;
    }

    // Check if we need to split single range
    partition.get_range(i, 0, &r_v);
    auto r = (T*)r_v;
    if (std::memcmp(r, &r[1], sizeof(T)) != 0) {
      *splitting_dim = i;
      *splitting_point = r[0] + (r[1] - r[0]) / 2;
      *unsplittable = !std::memcmp(splitting_point, &r[1], sizeof(T));
      break;
    }
  }

  assert(*splitting_dim != UINT32_MAX);
}

template <class T>
bool SubarrayPartitioner::must_split(Subarray* partition) {
  auto array_schema = subarray_.array()->array_schema();
  bool must_split = false;

  uint64_t size_fixed, size_var;
  for (const auto& b : budget_) {
    // Compute max sizes
    auto attr_name = b.first;
    auto var_size = array_schema->var_size(attr_name);

    // Compute est sizes
    size_fixed = 0;
    size_var = 0;
    if (var_size)
      partition->get_est_result_size(b.first.c_str(), &size_fixed, &size_var);
    else
      partition->get_est_result_size(b.first.c_str(), &size_fixed);

    // Check for budget overflow
    if (size_fixed > b.second.size_fixed_ || size_var > b.second.size_var_) {
      must_split = true;
      break;
    }
  }

  return must_split;
}

template <class T>
Status SubarrayPartitioner::next_from_multi_range(bool* unsplittable) {
  // A new multi-range subarray may need to be put in the list and split
  if (state_.multi_range_.empty()) {
    auto s = subarray_.get_subarray<T>(current_.start_, current_.end_);
    state_.multi_range_.push_front(std::move(s));
    split_top_multi_range<T>(unsplittable);
  }

  // Loop until you find a partition that fits or unsplittable
  if (!*unsplittable) {
    bool must_split;
    do {
      auto& partition = state_.multi_range_.front();
      must_split = this->must_split<T>(&partition);
      if (must_split)
        RETURN_NOT_OK(split_top_multi_range<T>(unsplittable));
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

template <class T>
Status SubarrayPartitioner::next_from_single_range(bool* unsplittable) {
  // Handle case where a new single range must be put in the list and split
  if (state_.single_range_.empty()) {
    auto s = subarray_.get_subarray<T>(current_.start_, current_.end_);
    state_.single_range_.push_front(std::move(s));
    split_top_single_range<T>(unsplittable);
  }

  // Loop until you find a partition that fits or unsplittable
  if (!*unsplittable) {
    bool must_split;
    do {
      auto& partition = state_.single_range_.front();
      must_split = this->must_split<T>(&partition);
      if (must_split)
        RETURN_NOT_OK(split_top_single_range<T>(unsplittable));
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

template <class T>
Status SubarrayPartitioner::split_top_single_range(bool* unsplittable) {
  // For easy reference
  const auto& range = state_.single_range_.front();
  auto dim_num = subarray_.dim_num();
  auto max = std::numeric_limits<T>::max();
  bool int_domain = std::numeric_limits<T>::is_integer;
  const void* range_1d;

  // Check if unsplittable
  if (range.is_unary()) {
    *unsplittable = true;
    return Status::Ok();
  }

  // Finding splitting point
  T splitting_point;
  unsigned splitting_dim;
  compute_splitting_point_single_range<T>(
      range, &splitting_dim, &splitting_point, unsplittable);

  if (*unsplittable)
    return Status::Ok();

  // Split remaining range into two ranges
  Subarray r1(subarray_.array(), subarray_.layout());
  Subarray r2(subarray_.array(), subarray_.layout());

  for (unsigned i = 0; i < dim_num; ++i) {
    range.get_range(i, 0, &range_1d);
    if (i == splitting_dim) {
      T r[2];
      r[0] = ((const T*)range_1d)[0];
      r[1] = splitting_point;
      r1.add_range(i, r);
      r[0] = (int_domain) ? (splitting_point + 1) :
                            std::nextafter(splitting_point, max);
      r[1] = ((const T*)range_1d)[1];
      r2.add_range(i, r);
    } else {
      r1.add_range(i, range_1d);
      r2.add_range(i, range_1d);
    }
  }

  // Important
  r1.compute_tile_overlap();
  r2.compute_tile_overlap();

  // Update list
  state_.single_range_.pop_front();
  state_.single_range_.push_front(std::move(r2));
  state_.single_range_.push_front(std::move(r1));

  return Status::Ok();
}

template <class T>
Status SubarrayPartitioner::split_top_multi_range(bool* unsplittable) {
  // For easy reference
  const auto& partition = state_.multi_range_.front();
  auto dim_num = subarray_.dim_num();
  auto max = std::numeric_limits<T>::max();
  bool int_domain = std::numeric_limits<T>::is_integer;
  const void* range_1d;
  uint64_t range_num;

  // Check if unsplittable
  if (partition.is_unary()) {
    *unsplittable = true;
    return Status::Ok();
  }

  // Finding splitting point
  unsigned splitting_dim;
  uint64_t splitting_range = UINT64_MAX;
  T splitting_point;
  compute_splitting_point_multi_range<T>(
      &splitting_dim, &splitting_range, &splitting_point, unsplittable);

  if (*unsplittable)
    return Status::Ok();

  // Split partition into two partitions
  Subarray p1(subarray_.array(), subarray_.layout());
  Subarray p2(subarray_.array(), subarray_.layout());

  for (unsigned i = 0; i < dim_num; ++i) {
    RETURN_NOT_OK(partition.get_range_num(i, &range_num));
    if (i != splitting_dim) {
      for (uint64_t j = 0; j < range_num; ++j) {
        partition.get_range(i, j, &range_1d);
        p1.add_range(i, range_1d);
        p2.add_range(i, range_1d);
      }
    } else {                                // i == splitting_dim
      if (splitting_range != UINT64_MAX) {  // Need to split multiple ranges
        for (uint64_t j = 0; j <= splitting_range; ++j) {
          partition.get_range(i, j, &range_1d);
          p1.add_range(i, range_1d);
        }
        for (uint64_t j = splitting_range + 1; j < range_num; ++j) {
          partition.get_range(i, j, &range_1d);
          p2.add_range(i, range_1d);
        }
      } else {  // Need to split a single range
        partition.get_range(i, 0, &range_1d);
        T r[2];
        r[0] = ((const T*)range_1d)[0];
        r[1] = splitting_point;
        p1.add_range(i, r);
        r[0] = (int_domain) ? (splitting_point + 1) :
                              std::nextafter(splitting_point, max);
        r[1] = ((const T*)range_1d)[1];
        p2.add_range(i, r);
      }
    }
  }

  // Important
  p1.compute_tile_overlap();
  p2.compute_tile_overlap();

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
}

// Explicit template instantiations
template Status SubarrayPartitioner::split_current<int8_t>(bool* unsplittable);
template Status SubarrayPartitioner::split_current<uint8_t>(bool* unsplittable);
template Status SubarrayPartitioner::split_current<int16_t>(bool* unsplittable);
template Status SubarrayPartitioner::split_current<uint16_t>(
    bool* unsplittable);
template Status SubarrayPartitioner::split_current<int32_t>(bool* unsplittable);
template Status SubarrayPartitioner::split_current<uint32_t>(
    bool* unsplittable);
template Status SubarrayPartitioner::split_current<int64_t>(bool* unsplittable);
template Status SubarrayPartitioner::split_current<uint64_t>(
    bool* unsplittable);
template Status SubarrayPartitioner::split_current<float>(bool* unsplittable);
template Status SubarrayPartitioner::split_current<double>(bool* unsplittable);

}  // namespace sm
}  // namespace tiledb
