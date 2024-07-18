/**
 * @file   subarray_partitioner.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/stats/global_stats.h"

#include <iomanip>

class SubarrayPartitionerException : public StatusException {
 public:
  explicit SubarrayPartitionerException(const std::string& message)
      : StatusException("SubarrayPartitioner", message) {
  }
};

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifndef MAX
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#endif

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::sm::stats;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

SubarrayPartitioner::SubarrayPartitioner() = default;

SubarrayPartitioner::SubarrayPartitioner(
    const Config* const config,
    const Subarray& subarray,
    const uint64_t memory_budget,
    const uint64_t memory_budget_var,
    const uint64_t memory_budget_validity,
    ThreadPool* const compute_tp,
    Stats* const parent_stats,
    shared_ptr<Logger> logger)
    : stats_(parent_stats->create_child("SubarrayPartitioner"))
    , logger_(logger->clone("SubarrayPartitioner", ++logger_id_))
    , config_(config)
    , subarray_(subarray)
    , memory_budget_(memory_budget)
    , memory_budget_var_(memory_budget_var)
    , memory_budget_validity_(memory_budget_validity)
    , compute_tp_(compute_tp) {
  state_.start_ = 0;
  auto range_num = subarray_.range_num();
  state_.end_ = (range_num > 0) ? range_num - 1 : 0;

  bool found = false;
  throw_if_not_ok(config_->get<bool>(
      "sm.skip_est_size_partitioning", &skip_split_on_est_size_, &found));
  assert(found);

  throw_if_not_ok(config_->get<bool>(
      "sm.skip_unary_partitioning_budget_check",
      &skip_unary_partitioning_budget_check_,
      &found));
  (void)found;
  assert(found);
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

const Subarray& SubarrayPartitioner::current() const {
  return current_.partition_;
}

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
    const char* name, uint64_t* budget) const {
  // Check attribute/dimension name
  if (name == nullptr) {
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget; Attribute/Dimension name cannot be null"));
  }

  // Check budget pointer
  if (budget == nullptr) {
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget; Invalid budget input"));
  }

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  bool is_dim = array_schema.is_dim(name);
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (!ArraySchema::is_special_attribute(name) && !is_dim && !is_attr) {
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Invalid attribute/dimension '") +
        name + "'"));
  }

  // Check if the attribute/dimension is fixed-sized
  if (array_schema.var_size(name)) {
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Input attribute/dimension '") +
        name + "' is var-sized"));
  }

  // Check if the attribute is nullable
  if (array_schema.is_nullable(name)) {
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Input attribute/dimension '") +
        name + "' is nullable"));
  }

  // Check if budget has been set
  auto b_it = budget_.find(name);
  if (b_it == budget_.end()) {
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Budget not set for "
                    "attribute/dimension '") +
        name + "'"));
  }

  // Get budget
  *budget = b_it->second.size_fixed_;

  return Status::Ok();
}

Status SubarrayPartitioner::get_result_budget(
    const char* name, uint64_t* budget_off, uint64_t* budget_val) const {
  // Check attribute/dimension name
  if (name == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget; Attribute/Dimension name cannot be null"));

  // Check budget pointers
  if (budget_off == nullptr || budget_val == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget; Invalid budget input"));

  // Check zipped coordinates
  if (name == constants::coords)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget for zipped coordinates; Attribute/Dimension "
        "must be var-sized"));

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  bool is_dim = array_schema.is_dim(name);
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (!is_dim && !is_attr)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Invalid attribute/dimension '") +
        name + "'"));

  // Check if the attribute/dimension is var-sized
  if (!array_schema.var_size(name))
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Input attribute/dimension '") +
        name + "' is fixed-sized"));

  // Check if the attribute is nullable
  if (array_schema.is_nullable(name))
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Input attribute/dimension '") +
        name + "' is nullable"));

  // Check if budget has been set
  auto b_it = budget_.find(name);
  if (b_it == budget_.end())
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Budget not set for "
                    "attribute/dimension '") +
        name + "'"));

  // Get budget
  *budget_off = b_it->second.size_fixed_;
  *budget_val = b_it->second.size_var_;

  return Status::Ok();
}

Status SubarrayPartitioner::get_result_budget_nullable(
    const char* name, uint64_t* budget, uint64_t* budget_validity) const {
  // Check attribute name
  if (name == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget; Attribute name cannot be null"));

  // Check budget pointers
  if (budget == nullptr || budget_validity == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget; Invalid budget input"));

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Invalid attribute '") + name +
        "'"));

  // Check if the attribute is fixed-sized
  if (array_schema.var_size(name))
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Input attribute '") + name +
        "' is var-sized"));

  // Check if the attribute is nullable
  if (!array_schema.is_nullable(name))
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Input attribute '") + name +
        "' is not nullable"));

  // Check if budget has been set
  auto b_it = budget_.find(name);
  if (b_it == budget_.end())
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Budget not set for "
                    "attribute '") +
        name + "'"));

  // Get budgets
  *budget = b_it->second.size_fixed_;
  *budget_validity = b_it->second.size_validity_;

  return Status::Ok();
}

Status SubarrayPartitioner::get_result_budget_nullable(
    const char* name,
    uint64_t* budget_off,
    uint64_t* budget_val,
    uint64_t* budget_validity) const {
  // Check attribute/dimension name
  if (name == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget; Attribute/Dimension name cannot be null"));

  // Check budget pointers
  if (budget_off == nullptr || budget_val == nullptr ||
      budget_validity == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot get result budget; Invalid budget input"));

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Invalid attribute '") + name +
        "'"));

  // Check if the attribute is var-sized
  if (!array_schema.var_size(name))
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Input attribute '") + name +
        "' is fixed-sized"));

  // Check if the attribute is nullable
  if (!array_schema.is_nullable(name))
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Input attribute '") + name +
        "' is not nullable"));

  // Check if budget has been set
  auto b_it = budget_.find(name);
  if (b_it == budget_.end())
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot get result budget; Budget not set for "
                    "attribute '") +
        name + "'"));

  // Get budget
  *budget_off = b_it->second.size_fixed_;
  *budget_val = b_it->second.size_var_;
  *budget_validity = b_it->second.size_validity_;

  return Status::Ok();
}

const std::unordered_map<std::string, SubarrayPartitioner::ResultBudget>*
SubarrayPartitioner::get_result_budgets() const {
  return &budget_;
}

Status SubarrayPartitioner::get_memory_budget(
    uint64_t* budget, uint64_t* budget_var, uint64_t* budget_validity) const {
  *budget = memory_budget_;
  *budget_var = memory_budget_var_;
  *budget_validity = memory_budget_validity_;
  return Status::Ok();
}

Status SubarrayPartitioner::next(bool* unsplittable) {
  auto timer_se = stats_->start_timer("read_next_partition");

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
  RETURN_NOT_OK(calibrate_current_start_end(&must_split_slab));

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
    const char* name, uint64_t budget) {
  // Check attribute/dimension name
  if (name == nullptr) {
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot set result budget; Attribute/Dimension name cannot be null"));
  }

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  bool is_dim = array_schema.is_dim(name);
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (!ArraySchema::is_special_attribute(name) && !is_dim && !is_attr) {
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Invalid attribute/dimension '") +
        name + "'"));
  }

  // Check if the attribute/dimension is fixed-sized
  bool var_size = (name != constants::coords && array_schema.var_size(name));
  if (var_size) {
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Input attribute/dimension '") +
        name + "' is var-sized"));
  }

  // Check if the attribute/dimension is nullable
  bool nullable = array_schema.is_nullable(name);
  if (nullable) {
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Input attribute/dimension '") +
        name + "' is nullable"));
  }

  budget_[name] = ResultBudget{budget, 0, 0};

  return Status::Ok();
}

Status SubarrayPartitioner::set_result_budget(
    const char* name, uint64_t budget_off, uint64_t budget_val) {
  // Check attribute/dimension name
  if (name == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot set result budget; Attribute/Dimension name cannot be null"));

  if (name == constants::coords)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot set result budget for zipped coordinates; Attribute/Dimension "
        "must be var-sized"));

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  bool is_dim = array_schema.is_dim(name);
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (!is_dim && !is_attr)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Invalid attribute/dimension '") +
        name + "'"));

  // Check if the attribute/dimension is var-sized
  if (!array_schema.var_size(name))
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Input attribute/dimension '") +
        name + "' is fixed-sized"));

  // Check if the attribute/dimension is nullable
  bool nullable = array_schema.is_nullable(name);
  if (nullable)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Input attribute/dimension '") +
        name + "' is nullable"));

  budget_[name] = ResultBudget{budget_off, budget_val, 0};

  return Status::Ok();
}

Status SubarrayPartitioner::set_result_budget_nullable(
    const char* name, uint64_t budget, uint64_t budget_validity) {
  // Check attribute/dimension name
  if (name == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot set result budget; Attribute name cannot be null"));

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Invalid attribute '") + name +
        "'"));

  // Check if the attribute is fixed-sized
  bool var_size = array_schema.var_size(name);
  if (var_size)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Input attribute '") + name +
        "' is var-sized"));

  // Check if the attribute is nullable
  bool nullable = array_schema.is_nullable(name);
  if (!nullable)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Input attribute '") + name +
        "' is not nullable"));

  budget_[name] = ResultBudget{budget, 0, budget_validity};

  return Status::Ok();
}

Status SubarrayPartitioner::set_result_budget_nullable(
    const char* name,
    uint64_t budget_off,
    uint64_t budget_val,
    uint64_t budget_validity) {
  // Check attribute/dimension name
  if (name == nullptr)
    return logger_->status(Status_SubarrayPartitionerError(
        "Cannot set result budget; Attribute name cannot be null"));

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Invalid attribute '") + name +
        "'"));

  // Check if the attribute is var-sized
  if (!array_schema.var_size(name))
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Input attribute '") + name +
        "' is fixed-sized"));

  // Check if the attribute is nullable
  bool nullable = array_schema.is_nullable(name);
  if (!nullable)
    return logger_->status(Status_SubarrayPartitionerError(
        std::string("Cannot set result budget; Input attribute '") + name +
        "' is not nullable"));

  budget_[name] = ResultBudget{budget_off, budget_val, budget_validity};

  return Status::Ok();
}

Status SubarrayPartitioner::set_memory_budget(
    uint64_t budget, uint64_t budget_var, uint64_t budget_validity) {
  memory_budget_ = budget;
  memory_budget_var_ = budget_var;
  memory_budget_validity_ = budget_validity;
  return Status::Ok();
}

Status SubarrayPartitioner::split_current(bool* unsplittable) {
  auto timer_se = stats_->start_timer("read_split_current_partition");

  *unsplittable = false;

  // Current came from splitting a multi-range partition
  if (current_.split_multi_range_) {
    if (state_.multi_range_.empty())
      state_.start_ = current_.start_;
    state_.multi_range_.push_front(current_.partition_);
    throw_if_not_ok(split_top_multi_range(unsplittable));
    return next_from_multi_range(unsplittable);
  }

  // Current came from retrieving a multi-range partition from subarray
  if (current_.start_ < current_.end_) {
    auto range_num = (current_.end_ - current_.start_ + 1);
    assert(1 - constants::multi_range_reduction_in_split <= 1);
    auto new_range_num =
        range_num * (1 - constants::multi_range_reduction_in_split);
    current_.end_ = current_.start_ + (uint64_t)new_range_num - 1;

    bool must_split_slab;
    RETURN_NOT_OK(calibrate_current_start_end(&must_split_slab));

    // If the range between `current_.start_` and `current_.end_`
    // will not fit within the memory contraints, `must_split_slab`
    // will be true. We must split the current partition.
    //
    // This is a difficult path to reach, but this has been manually
    // tested. This path was reached by re-assigning the query
    // buffers with smaller buffers after an incomplete read.
    if (must_split_slab) {
      if (state_.multi_range_.empty())
        state_.start_ = current_.start_;
      state_.multi_range_.push_front(current_.partition_);
      throw_if_not_ok(split_top_multi_range(unsplittable));
      return next_from_multi_range(unsplittable);
    }

    current_.partition_ =
        subarray_.get_subarray(current_.start_, current_.end_);
    state_.start_ = current_.end_ + 1;

    return Status::Ok();
  }

  // Current came from splitting a single-range partition
  if (state_.single_range_.empty())
    state_.start_--;
  state_.single_range_.push_front(current_.partition_);
  throw_if_not_ok(split_top_single_range(unsplittable));
  return next_from_single_range(unsplittable);
}

const SubarrayPartitioner::State* SubarrayPartitioner::state() const {
  return &state_;
}

SubarrayPartitioner::State* SubarrayPartitioner::state() {
  return &state_;
}

const Subarray& SubarrayPartitioner::subarray() const {
  return subarray_;
}

Subarray& SubarrayPartitioner::subarray() {
  return subarray_;
}

const stats::Stats& SubarrayPartitioner::stats() const {
  return *stats_;
}

void SubarrayPartitioner::set_stats(const stats::StatsData& data) {
  stats_->populate_with_data(data);
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status SubarrayPartitioner::calibrate_current_start_end(bool* must_split_slab) {
  // Initialize (may be reset below)
  *must_split_slab = false;

  // Special case of single range and global layout
  if (subarray_.layout() == Layout::GLOBAL_ORDER) {
    assert(current_.start_ == current_.end_);
    return Status::Ok();
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
  auto cell_order = subarray_.array()->array_schema_latest().cell_order();
  cell_order = (cell_order == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order;
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
  return Status::Ok();
}

SubarrayPartitioner SubarrayPartitioner::clone() const {
  SubarrayPartitioner clone;
  clone.stats_ = stats_;
  clone.logger_ = logger_;
  clone.config_ = config_;
  clone.subarray_ = subarray_;
  clone.budget_ = budget_;
  clone.current_ = current_;
  clone.state_ = state_;
  clone.memory_budget_ = memory_budget_;
  clone.memory_budget_var_ = memory_budget_var_;
  clone.memory_budget_validity_ = memory_budget_validity_;
  clone.skip_split_on_est_size_ = skip_split_on_est_size_;
  clone.skip_unary_partitioning_budget_check_ =
      skip_unary_partitioning_budget_check_;
  clone.compute_tp_ = compute_tp_;

  return clone;
}

Status SubarrayPartitioner::compute_current_start_end(bool* found) {
  // Compute the tile overlap. Note that the ranges in `tile_overlap` may have
  // been truncated the ending bound due to memory constraints.
  subarray_.precompute_tile_overlap(
      state_.start_, state_.end_, config_, compute_tp_);
  const SubarrayTileOverlap* const tile_overlap =
      subarray_.subarray_tile_overlap();
  assert(tile_overlap->range_idx_start() == state_.start_);
  assert(tile_overlap->range_idx_end() <= state_.end_);

  // Preparation
  auto array = subarray_.array();
  auto meta = array->fragment_metadata();
  std::vector<Subarray::ResultSize> cur_sizes;
  std::vector<Subarray::MemorySize> mem_sizes;
  std::vector<std::string> names;
  std::vector<ResultBudget> budgets;
  names.reserve(budget_.size());
  budgets.reserve(budget_.size());
  cur_sizes.resize(budget_.size(), Subarray::ResultSize({0.0, 0.0, 0.0}));
  mem_sizes.resize(budget_.size(), Subarray::MemorySize({0, 0, 0}));
  for (const auto& budget_it : budget_) {
    names.emplace_back(budget_it.first);
    budgets.emplace_back(budget_it.second);
  }

  // Compute the estimated result sizes
  std::vector<std::vector<Subarray::ResultSize>> result_sizes;
  std::vector<std::vector<Subarray::MemorySize>> memory_sizes;
  subarray_.compute_relevant_fragment_est_result_sizes(
      names,
      tile_overlap->range_idx_start(),
      tile_overlap->range_idx_end(),
      &result_sizes,
      &memory_sizes,
      compute_tp_);

  bool done = false;
  current_.start_ = tile_overlap->range_idx_start();
  for (current_.end_ = tile_overlap->range_idx_start();
       current_.end_ <= tile_overlap->range_idx_end();
       ++current_.end_) {
    size_t r = current_.end_ - tile_overlap->range_idx_start();
    for (size_t i = 0; i < names.size(); ++i) {
      auto& cur_size = cur_sizes[i];
      auto& mem_size = mem_sizes[i];
      const auto& budget = budgets[i];
      cur_size.size_fixed_ += result_sizes[r][i].size_fixed_;
      cur_size.size_var_ += result_sizes[r][i].size_var_;
      cur_size.size_validity_ += result_sizes[r][i].size_validity_;
      mem_size.size_fixed_ += memory_sizes[r][i].size_fixed_;
      mem_size.size_var_ += memory_sizes[r][i].size_var_;
      mem_size.size_validity_ += memory_sizes[r][i].size_validity_;
      if ((!skip_split_on_est_size_ &&
           (cur_size.size_fixed_ > budget.size_fixed_ ||
            cur_size.size_var_ > budget.size_var_ ||
            cur_size.size_validity_ > budget.size_validity_)) ||
          mem_size.size_fixed_ > memory_budget_ ||
          mem_size.size_var_ > memory_budget_var_ ||
          mem_size.size_validity_ > memory_budget_validity_) {
        if (cur_size.size_fixed_ > budget.size_fixed_) {
          stats_->add_counter(
              "compute_current_start_end.fixed_result_size_overflow", 1);
        } else if (cur_size.size_var_ > budget.size_var_) {
          stats_->add_counter(
              "compute_current_start_end.var_result_size_overflow", 1);
        } else if (cur_size.size_validity_ > budget.size_validity_) {
          stats_->add_counter(
              "compute_current_start_end.validity_result_size_overflow", 1);
        } else if (mem_size.size_fixed_ > memory_budget_) {
          stats_->add_counter(
              "compute_current_start_end.fixed_tile_size_overflow", 1);
        } else if (mem_size.size_var_ > memory_budget_var_) {
          stats_->add_counter(
              "compute_current_start_end.var_tile_size_overflow", 1);
        } else if (mem_size.size_validity_ > memory_budget_validity_) {
          stats_->add_counter(
              "compute_current_start_end.validity_tile_size_overflow", 1);
        }

        done = true;
        break;
      }
    }

    if (done) {
      break;
    }
  }

  *found = current_.end_ != current_.start_;
  if (*found) {
    // If the range was found, make it inclusive before returning.
    current_.end_--;

    stats_->add_counter("compute_current_start_end.found", 1);
    stats_->add_counter(
        "compute_current_start_end.ranges",
        tile_overlap->range_idx_end() - tile_overlap->range_idx_start() + 1);
    stats_->add_counter(
        "compute_current_start_end.adjusted_ranges",
        current_.end_ - current_.start_ + 1);

  } else {
    stats_->add_counter("compute_current_start_end.not_found", 1);
  }

  return Status::Ok();
}

void SubarrayPartitioner::compute_splitting_value_on_tiles(
    const Subarray& range,
    unsigned* splitting_dim,
    ByteVecValue* splitting_value,
    bool* unsplittable) {
  assert(range.layout() == Layout::GLOBAL_ORDER);
  *unsplittable = true;

  // Inapplicable to Hilbert cell order
  if (subarray_.array()->array_schema_latest().cell_order() == Layout::HILBERT)
    return;

  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  auto layout = array_schema.tile_order();
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
    auto dim{array_schema.domain().dimension_ptr(d)};
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
    bool* normal_order,
    bool* unsplittable) {
  *normal_order = true;

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
  const auto& array_schema = subarray_.array()->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  auto cell_order = array_schema.cell_order();
  assert(!range.is_unary());
  auto layout = subarray_.layout();
  if (layout == Layout::UNORDERED && cell_order == Layout::HILBERT) {
    cell_order = Layout::ROW_MAJOR;
  } else {
    layout = (layout == Layout::UNORDERED || layout == Layout::GLOBAL_ORDER) ?
                 cell_order :
                 layout;
  }
  *splitting_dim = UINT32_MAX;

  // Special case for Hilbert cell order
  if (cell_order == Layout::HILBERT) {
    compute_splitting_value_single_range_hilbert(
        range, splitting_dim, splitting_value, normal_order, unsplittable);
    return;
  }

  // Cell order is either row- or col-major
  assert(cell_order == Layout::ROW_MAJOR || cell_order == Layout::COL_MAJOR);

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
    auto dim{array_schema.dimension_ptr(d)};
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

void SubarrayPartitioner::compute_splitting_value_single_range_hilbert(
    const Subarray& range,
    unsigned* splitting_dim,
    ByteVecValue* splitting_value,
    bool* normal_order,
    bool* unsplittable) {
  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  Hilbert h(dim_num);

  // Compute the uint64 mapping of the range (bits properly shifted)
  std::vector<std::array<uint64_t, 2>> range_uint64;
  compute_range_uint64(range, &range_uint64, unsplittable);

  // Check if unsplittable (range_uint64 is unary)
  if (*unsplittable)
    return;

  // Compute the splitting dimension
  compute_splitting_dim_hilbert(range_uint64, splitting_dim);

  // Compute splitting value
  compute_splitting_value_hilbert(
      range_uint64[*splitting_dim], *splitting_dim, splitting_value);

  // Check for unsplittable again
  auto dim{array_schema.dimension_ptr(*splitting_dim)};
  const Range* r;
  range.get_range(*splitting_dim, 0, &r);
  if (dim->smaller_than(*splitting_value, *r)) {
    *unsplittable = true;
    return;
  }

  // Set normal order
  std::vector<uint64_t> hilbert_coords(dim_num);
  for (uint32_t d = 0; d < dim_num; ++d)
    hilbert_coords[d] = range_uint64[d][0];
  auto hilbert_left = h.coords_to_hilbert(&hilbert_coords[0]);
  for (uint32_t d = 0; d < dim_num; ++d) {
    if (d == *splitting_dim)
      hilbert_coords[d] = range_uint64[d][1];
    else
      hilbert_coords[d] = range_uint64[d][0];
  }
  auto hilbert_right = h.coords_to_hilbert(&hilbert_coords[0]);
  *normal_order = (hilbert_left < hilbert_right);
}

Status SubarrayPartitioner::compute_splitting_value_multi_range(
    unsigned* splitting_dim,
    uint64_t* splitting_range,
    ByteVecValue* splitting_value,
    bool* normal_order,
    bool* unsplittable) {
  const auto& partition = state_.multi_range_.front();
  *normal_order = true;

  // Single-range partittion
  if (partition.range_num() == 1) {
    compute_splitting_value_single_range(
        partition, splitting_dim, splitting_value, normal_order, unsplittable);
    return Status::Ok();
  }

  // Multi-range partition
  auto layout = subarray_.layout();
  const auto& array_schema = subarray_.array()->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  auto cell_order = (array_schema.cell_order() == Layout::HILBERT) ?
                        Layout::ROW_MAJOR :
                        array_schema.cell_order();
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
    auto dim{array_schema.dimension_ptr(d)};
    if (!r->unary()) {
      *splitting_dim = d;
      dim->splitting_value(*r, splitting_value, unsplittable);
      break;
    }
  }

  assert(*splitting_dim != UINT32_MAX);
  return Status::Ok();
}

bool SubarrayPartitioner::must_split(Subarray* partition) {
  for (const auto& b : budget_) {
    /*
     * Compute max memory size and, if needed, estimated result size
     */
    auto name = b.first;
    auto mem_size{
        partition->get_max_memory_size(b.first.c_str(), config_, compute_tp_)};
    auto est_size{
        skip_split_on_est_size_ ?
            // Skip the estimate and use a default object that's all zeros.
            FieldDataSize{} :
            // Perform the estimate
            partition->get_est_result_size(
                b.first.c_str(), config_, compute_tp_)};

    // If we try to split a unary range because of memory budgets, throw an
    // error. This can happen when the memory budget cannot fit even one tile.
    // It will cause the reader to process the query cell by cell, which will
    // make it very slow.
    if (!skip_unary_partitioning_budget_check_ &&
        (mem_size.fixed_ > memory_budget_ ||
         mem_size.variable_ > memory_budget_var_ ||
         mem_size.validity_ > memory_budget_validity_)) {
      if (partition->is_unary()) {
        throw SubarrayPartitionerException(
            "Trying to partition a unary range because of memory budget, this "
            "will cause the query to run very slow. Increase "
            "`sm.memory_budget` and `sm.memory_budget_var` through the "
            "configuration settings to avoid this issue. To override and run "
            "the query with the same budget, set "
            "`sm.skip_unary_partitioning_budget_check` to `true`.");
      }
    }

    // Check for budget overflow
    if ((!skip_split_on_est_size_ &&
         (est_size.fixed_ > b.second.size_fixed_ ||
          est_size.variable_ > b.second.size_var_ ||
          est_size.validity_ > b.second.size_validity_)) ||
        mem_size.fixed_ > memory_budget_ ||
        mem_size.variable_ > memory_budget_var_ ||
        mem_size.validity_ > memory_budget_validity_) {
      return true;
    }
  }
  return false;
}

Status SubarrayPartitioner::next_from_multi_range(bool* unsplittable) {
  // A new multi-range subarray may need to be put in the list and split
  if (state_.multi_range_.empty()) {
    auto s = subarray_.get_subarray(current_.start_, current_.end_);
    state_.multi_range_.push_front(std::move(s));
    throw_if_not_ok(split_top_multi_range(unsplittable));
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
    throw_if_not_ok(split_top_single_range(unsplittable));
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
  bool normal_order;
  compute_splitting_value_single_range(
      range, &splitting_dim, &splitting_value, &normal_order, unsplittable);

  if (*unsplittable)
    return Status::Ok();

  // Split remaining range into two ranges
  Subarray r1, r2;
  range.split(splitting_dim, splitting_value, &r1, &r2);

  // Update list
  state_.single_range_.pop_front();
  if (normal_order) {
    state_.single_range_.push_front(std::move(r2));
    state_.single_range_.push_front(std::move(r1));
  } else {
    state_.single_range_.push_front(std::move(r1));
    state_.single_range_.push_front(std::move(r2));
  }

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
  bool normal_order;
  RETURN_NOT_OK(compute_splitting_value_multi_range(
      &splitting_dim,
      &splitting_range,
      &splitting_value,
      &normal_order,
      unsplittable));

  if (*unsplittable)
    return Status::Ok();

  // Split partition into two partitions
  Subarray p1;
  Subarray p2;
  partition.split(splitting_range, splitting_dim, splitting_value, &p1, &p2);

  // Update list
  state_.multi_range_.pop_front();
  if (normal_order) {
    state_.multi_range_.push_front(std::move(p2));
    state_.multi_range_.push_front(std::move(p1));
  } else {
    state_.multi_range_.push_front(std::move(p1));
    state_.multi_range_.push_front(std::move(p2));
  }

  return Status::Ok();
}

void SubarrayPartitioner::swap(SubarrayPartitioner& partitioner) {
  std::swap(stats_, partitioner.stats_);
  std::swap(logger_, partitioner.logger_);
  std::swap(config_, partitioner.config_);
  std::swap(subarray_, partitioner.subarray_);
  std::swap(budget_, partitioner.budget_);
  std::swap(current_, partitioner.current_);
  std::swap(state_, partitioner.state_);
  std::swap(memory_budget_, partitioner.memory_budget_);
  std::swap(memory_budget_var_, partitioner.memory_budget_var_);
  std::swap(memory_budget_validity_, partitioner.memory_budget_validity_);
  std::swap(skip_split_on_est_size_, partitioner.skip_split_on_est_size_);
  std::swap(
      skip_unary_partitioning_budget_check_,
      partitioner.skip_unary_partitioning_budget_check_);
  std::swap(compute_tp_, partitioner.compute_tp_);
}

void SubarrayPartitioner::compute_range_uint64(
    const Subarray& range,
    std::vector<std::array<uint64_t, 2>>* range_uint64,
    bool* unsplittable) const {
  // Initializations
  const auto& array_schema = subarray_.array()->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  const Range* r;
  *unsplittable = true;
  range_uint64->resize(dim_num);
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Default values for empty range start/end
  auto max_string = std::string("\x7F\x7F\x7F\x7F\x7F\x7F\x7F\x7F", 8);

  // Calculate mapped range
  bool empty_start, empty_end;
  for (uint32_t d = 0; d < dim_num; ++d) {
    auto dim{array_schema.dimension_ptr(d)};
    auto var = dim->var_size();
    range.get_range(d, 0, &r);
    empty_start = var ? (r->start_size() == 0) : r->empty();
    empty_end = var ? (r->end_size() == 0) : r->empty();
    auto max_default =
        var ? dim->map_to_uint64(
                  max_string.data(), max_string.size(), bits, max_bucket_val) :
              (UINT64_MAX >> (64 - bits));
    if (r->var_size()) {
      auto start_str = r->start_str();
      (*range_uint64)[d][0] =
          empty_start ? 0 :  // min default
                        dim->map_to_uint64(
                  start_str.data(), start_str.size(), bits, max_bucket_val);
      auto end_str = r->end_str();
      (*range_uint64)[d][1] =
          empty_end ? max_default :
                      dim->map_to_uint64(
                          end_str.data(), end_str.size(), bits, max_bucket_val);
    } else {
      // Note: coord_size is ignored for fixed size in map_to_uint64.
      (*range_uint64)[d][0] =
          empty_start ? 0 :  // min default
              dim->map_to_uint64(r->start_fixed(), 0, bits, max_bucket_val);
      (*range_uint64)[d][1] =
          empty_end ?
              max_default :
              dim->map_to_uint64(r->end_fixed(), 0, bits, max_bucket_val);
    }

    assert((*range_uint64)[d][0] <= (*range_uint64)[d][1]);

    if ((*range_uint64)[d][0] != (*range_uint64)[d][1])
      *unsplittable = false;
  }
}

void SubarrayPartitioner::compute_splitting_dim_hilbert(
    const std::vector<std::array<uint64_t, 2>>& range_uint64,
    uint32_t* splitting_dim) const {
  // For easy reference
  const auto& array_schema = subarray_.array()->array_schema_latest();
  auto dim_num = array_schema.dim_num();

  // Prepare candidate splitting dimensions
  std::set<uint32_t> splitting_dims;
  for (uint32_t d = 0; d < dim_num; ++d) {
    if (range_uint64[d][0] != range_uint64[d][1])  // If not unary
      splitting_dims.insert(d);
  }

  // This vector stores the coordinates of the range grid
  // defined over the potential split of a range across all
  // the dimensions. If there are dim_num dimensions, this
  // will contain 2^{dim_num} elements. The coordinates will be
  // (1,1,..., 1), (1,1,...,1), (2,1,...,1), (2,1,....,2), ...,
  // Each such coordinate is also associated with a hilbert value.
  std::vector<std::pair<uint64_t, std::vector<uint64_t>>> range_grid;

  // Auxiliary grid size in order to exclude unary ranges. For instance,
  // for 2D, if the range on the second dimension is unary, only
  // coordinates (1,1) and (2,1) will appear, with coordinates
  // (1,2) and (2,2) being excluded.
  std::vector<uint64_t> grid_size(dim_num);
  bool unary;
  for (uint32_t d = 0; d < dim_num; ++d) {
    unary = (range_uint64[d][0] == range_uint64[d][1]);
    grid_size[d] = 1 + (int32_t)!unary;
  }

  // Prepare the grid
  std::vector<uint64_t> grid_coords(dim_num, 1);
  std::vector<uint64_t> hilbert_coords(dim_num);
  uint64_t hilbert_value;
  Hilbert h(dim_num);
  while (grid_coords[0] < grid_size[0] + 1) {
    // Map hilbert values of range_uint64 endpoints to range grid
    for (uint32_t d = 0; d < dim_num; ++d)
      hilbert_coords[d] = range_uint64[d][grid_coords[d] - 1];
    hilbert_value = h.coords_to_hilbert(&hilbert_coords[0]);
    range_grid.push_back(std::make_pair(hilbert_value, grid_coords));

    // Advance coordinates
    auto d = (int32_t)dim_num - 1;
    ++grid_coords[d];
    while (d > 0 && grid_coords[d] == grid_size[d] + 1) {
      grid_coords[d--] = 1;
      ++grid_coords[d];
    }
  }

  // Choose splitting dimension
  std::sort(range_grid.begin(), range_grid.end());
  auto next_coords = range_grid[0].second;
  size_t c = 1;
  while (splitting_dims.size() != 1) {
    assert(c < range_grid.size());
    for (uint32_t d = 0; d < dim_num; ++d) {
      if (range_grid[c].second[d] != next_coords[d]) {  // Exclude dimension
        splitting_dims.erase(d);
        break;
      }
    }
    ++c;
  }

  // The remaining dimension is the splitting dimension
  assert(splitting_dims.size() == 1);
  *splitting_dim = *(splitting_dims.begin());
}

void SubarrayPartitioner::compute_splitting_value_hilbert(
    const std::array<uint64_t, 2>& range_uint64,
    uint32_t splitting_dim,
    ByteVecValue* splitting_value) const {
  const auto& array_schema = subarray_.array()->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  uint64_t splitting_value_uint64 = range_uint64[0];  // Splitting value
  if (range_uint64[0] + 1 != range_uint64[1]) {
    uint64_t left_p2_m1, right_p2_m1;  // Left/right powers of 2 minus 1

    // Compute left and right (2^i-1) enclosing the uint64 range
    left_p2_m1 = utils::math::left_p2_m1(range_uint64[0]);
    right_p2_m1 = utils::math::right_p2_m1(range_uint64[1]);
    assert(left_p2_m1 != right_p2_m1);  // Cannot be unary

    // Compute splitting value
    uint64_t splitting_offset = 0;
    auto range_uint64_start = range_uint64[0];
    auto range_uint64_end = range_uint64[1];
    while (true) {
      if (((left_p2_m1 << 1) + 1) != right_p2_m1) {
        // More than one power of 2 apart, split at largest power of 2 in
        // between
        splitting_value_uint64 = splitting_offset + (right_p2_m1 >> 1);
        break;
      } else if (left_p2_m1 == range_uint64_start) {
        splitting_value_uint64 = splitting_offset + left_p2_m1;
        break;
      } else {  // One power apart - need to normalize and repeat
        range_uint64_start -= (left_p2_m1 + 1);
        range_uint64_end -= (left_p2_m1 + 1);
        splitting_offset += (left_p2_m1 + 1);
        left_p2_m1 = utils::math::left_p2_m1(range_uint64_start);
        right_p2_m1 = utils::math::right_p2_m1(range_uint64_end);
        assert(left_p2_m1 != right_p2_m1);  // Cannot be unary
      }
    }
  }

  // Set real splitting value
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  *splitting_value =
      array_schema.dimension_ptr(splitting_dim)
          ->map_from_uint64(splitting_value_uint64, bits, max_bucket_val);
}
