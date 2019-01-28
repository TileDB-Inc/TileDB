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
#include "tiledb/sm/array/array.h"

#include <iomanip>

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

  // Find the [start, end] of the subarray ranges that fit in the budget
  bool interval_found = compute_current_start_end();

  // Single-range partition that must be split
  if (!interval_found)
    return next_from_single_range<T>(unsplittable);

  // An interval of whole ranges that may need calibration
  calibrate_current_start_end();

  return next_from_multiple_ranges<T>();
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
  // Split multi-range subarray
  if (current_.start_ < current_.end_) {
    current_.end_ *= (1 - constants::multi_range_reduction_in_split);
    current_.partition_ =
        std::move(subarray_.get_subarray<T>(current_.start_, current_.end_));
    state_.start_ = current_.end_ + 1;
    *unsplittable = false;
    return Status::Ok();
  }

  // Split single-range subarray
  state_.single_range_.push_front(current_.partition_);
  split_top_single_range<T>(unsplittable);
  if (!unsplittable) {
    current_.partition_ = std::move(state_.single_range_.front());
    state_.single_range_.pop_front();
  }

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void SubarrayPartitioner::calibrate_current_start_end() {
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

bool SubarrayPartitioner::compute_current_start_end() {
  // Preparation
  auto tile_overlap = subarray_.tile_overlap();
  auto array_schema = subarray_.array()->array_schema();
  auto fragment_num = tile_overlap.size();
  std::unordered_map<std::string, ResultBudget> sizes;
  for (const auto& it : budget_)
    sizes[it.first] = ResultBudget{0, 0};

  // TODO: calibrate size here not to exceed the maximum possible

  current_.start_ = state_.start_;
  for (current_.end_ = state_.start_; current_.end_ <= state_.end_;
       ++current_.end_) {
    // TODO: Update the maximum size here

    for (unsigned i = 0; i < fragment_num; ++i) {
      for (const auto& budget_it : budget_) {
        auto attr_name = budget_it.first;
        auto var_size = array_schema->var_size(attr_name);
        auto size = subarray_.compute_est_result_size(
            attr_name, var_size, i, tile_overlap[i][current_.end_]);
        auto& size_it = sizes[attr_name];
        size_it.size_fixed_ += size.size_fixed_;
        size_it.size_var_ += size.size_var_;

        // TODO: calibrate here before the check

        if (size_it.size_fixed_ > budget_it.second.size_fixed_ ||
            size_it.size_var_ > budget_it.second.size_var_) {
          // Cannot find range that fits in the buffer
          if (current_.end_ == current_.start_)
            return false;

          // Range found, make it inclusive
          current_.end_--;
          return true;
        }
      }
    }
  }

  // Cannot find range that fits in the buffer
  if (current_.end_ == current_.start_)
    return false;

  // Range found, make it inclusive
  current_.end_--;
  return true;

  // TODO: generalize to different layouts
  // TODO: handle splitting of multi-range subarrays
}

// TODO: Split current

// TODO (sp): in the future this can be more sophisticated, taking into
// TODO (sp): account MBRs (i.e., the distirbution of the data) as well
template <class T>
void SubarrayPartitioner::compute_splitting_point(
    unsigned* splitting_dim, T* splitting_point, bool* unsplittable) {
  // For easy reference
  auto layout = subarray_.layout();
  auto dim_num = subarray_.array()->array_schema()->dim_num();
  auto cell_order = subarray_.array()->array_schema()->cell_order();
  const auto& range = state_.single_range_.front();
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

bool SubarrayPartitioner::must_split_top_single_range() {
  auto& range = state_.single_range_.front();
  auto array_schema = subarray_.array()->array_schema();
  bool must_split = false;
  uint64_t size_fixed, size_var;
  for (const auto& b : budget_) {
    size_fixed = 0;
    size_var = 0;
    if (array_schema->var_size(b.first))
      range.get_est_result_size(b.first.c_str(), &size_fixed, &size_var);
    else
      range.get_est_result_size(b.first.c_str(), &size_fixed);

    if (size_fixed > b.second.size_fixed_ || size_var > b.second.size_var_) {
      must_split = true;
      break;
    }
  }

  return must_split;
}

template <class T>
Status SubarrayPartitioner::next_from_multiple_ranges() {
  current_.partition_ =
      std::move(subarray_.get_subarray<T>(current_.start_, current_.end_));
  state_.start_ = current_.end_ + 1;

  return Status::Ok();
}

template <class T>
Status SubarrayPartitioner::next_from_single_range(bool* unsplittable) {
  *unsplittable = false;

  // Handle case a new single range must be put in the list and split
  if (state_.single_range_.empty()) {
    auto s = subarray_.get_subarray<T>(current_.start_, current_.end_);
    state_.single_range_.push_front(std::move(s));
    split_top_single_range<T>(unsplittable);
    if (*unsplittable)
      return Status::Ok();
  }

  // Loop until you find a partition that fits or unsplittable
  bool must_split;
  do {
    must_split = must_split_top_single_range();
    if (must_split) {
      RETURN_NOT_OK(split_top_single_range<T>(unsplittable));
      if (*unsplittable)
        return Status::Ok();
    }
  } while (must_split);

  // At this point, the top range is the next partition
  current_.partition_ = std::move(state_.single_range_.front());
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
  compute_splitting_point<T>(&splitting_dim, &splitting_point, unsplittable);

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

  // Update list
  state_.single_range_.pop_front();
  state_.single_range_.push_front(std::move(r2));
  state_.single_range_.push_front(std::move(r1));

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
