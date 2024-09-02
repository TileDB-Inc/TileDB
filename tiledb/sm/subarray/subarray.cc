/**
 * @file   subarray.cc
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
 * This file implements class Subarray.
 */

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <sstream>
#include <unordered_set>

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/hash.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/rectangle.h"
#include "tiledb/sm/misc/resource_pool.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/readers/sparse_index_reader_base.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/rtree/rtree.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/subarray/relevant_fragment_generator.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/type/apply_with_type.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;
using namespace tiledb::type;

namespace tiledb::sm {

/** Class for query status exceptions. */
class SubarrayException : public StatusException {
 public:
  explicit SubarrayException(const std::string& msg)
      : StatusException("Subarray", msg) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Subarray::Subarray()
    : array_(nullptr)
    , layout_(Layout::UNORDERED)
    , cell_order_(Layout::ROW_MAJOR)
    , est_result_size_computed_(false)
    , relevant_fragments_(0)
    , coalesce_ranges_(true)
    , ranges_sorted_(false)
    , merge_overlapping_ranges_(true) {
}

Subarray::Subarray(
    const Array* array,
    Stats* const parent_stats,
    shared_ptr<Logger> logger,
    const bool coalesce_ranges)
    : Subarray(
          array->opened_array(),
          Layout::UNORDERED,
          parent_stats,
          logger,
          coalesce_ranges) {
}

Subarray::Subarray(
    const Array* const array,
    const Layout layout,
    Stats* const parent_stats,
    shared_ptr<Logger> logger,
    const bool coalesce_ranges)
    : Subarray(
          array->opened_array(),
          layout,
          parent_stats,
          logger,
          coalesce_ranges) {
}

Subarray::Subarray(
    const shared_ptr<OpenedArray> opened_array,
    const Layout layout,
    Stats* const parent_stats,
    shared_ptr<Logger> logger,
    const bool coalesce_ranges)
    : stats_(
          parent_stats ?
              parent_stats->create_child("Subarray") :
              opened_array->resources().stats().create_child("subSubarray"))
    , logger_(logger->clone("Subarray", ++logger_id_))
    , array_(opened_array)
    , layout_(layout)
    , cell_order_(opened_array->array_schema_latest().cell_order())
    , est_result_size_computed_(false)
    , relevant_fragments_(opened_array->fragment_metadata().size())
    , coalesce_ranges_(coalesce_ranges)
    , ranges_sorted_(false)
    , merge_overlapping_ranges_(true) {
  add_default_ranges();
}

Subarray::Subarray(const Subarray& subarray)
    : Subarray() {
  // Make a deep-copy clone
  auto clone = subarray.clone();
  // Swap with the clone
  swap(clone);
}

Subarray::Subarray(Subarray&& subarray) noexcept
    : Subarray() {
  // Swap with the argument
  swap(subarray);
}

Subarray& Subarray::operator=(const Subarray& subarray) {
  // Make a deep-copy clone
  auto clone = subarray.clone();
  // Swap with the clone
  swap(clone);

  return *this;
}

Subarray& Subarray::operator=(Subarray&& subarray) noexcept {
  // Swap with the argument
  swap(subarray);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

void Subarray::add_index_ranges_from_label(
    const uint32_t dim_idx,
    const bool is_point_ranges,
    const void* start,
    const uint64_t count) {
  is_point_ranges ? add_point_ranges(dim_idx, start, count, false) :
                    add_ranges_list(dim_idx, start, count, false);
}

void Subarray::add_label_range(
    const DimensionLabel& dim_label_ref,
    Range&& range,
    const bool read_range_oob_error) {
  const auto dim_idx = dim_label_ref.dimension_index();
  if (label_range_subset_[dim_idx].has_value()) {
    // A label range has already been set on this dimension. Do the following:
    //  * Check this label is the same label that rangers were already set.
    if (dim_label_ref.name() != label_range_subset_[dim_idx].value().name_) {
      throw SubarrayException(
          "[add_label_range] Dimension is already to set to use "
          "dimension label '" +
          label_range_subset_[dim_idx].value().name_ + "'");
    }
  } else {
    // A label range has not yet been set on this dimension. Do the following:
    //  * Verify no ranges explicitly set on the dimension.
    //  * Construct LabelRangeSubset for this dimension label.
    //  * Clear implicitly set range from the dimension ranges.
    //  * Update is_default (tracks if the range on the dimension is the default
    //    value of the entire domain).
    if (range_subset_[dim_label_ref.dimension_index()]
            .is_explicitly_initialized()) {
      throw SubarrayException(
          "[add_label_range] Dimension '" + std::to_string(dim_idx) +
          "' already has ranges set to it.");
    }
    label_range_subset_[dim_idx] =
        LabelRangeSubset(dim_label_ref, coalesce_ranges_);
    range_subset_[dim_label_ref.dimension_index()].clear();
    is_default_[dim_idx] = false;  // Only need to clear default once.
  }

  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_.clear();

  // Restrict the range to the dimension domain and add.
  auto&& [error_status, oob_warning] =
      label_range_subset_[dim_idx].value().ranges_.add_range(
          range, read_range_oob_error);
  if (!error_status.ok()) {
    throw SubarrayException(
        "[add_label_range] Cannot add label range for dimension label '" +
        dim_label_ref.name() + "'; " + error_status.message());
  }
  if (oob_warning.has_value()) {
    LOG_WARN(
        oob_warning.value() + " for dimension label '" + dim_label_ref.name() +
        "'");
  }
}

void Subarray::add_label_range(
    const std::string& label_name, const void* start, const void* end) {
  // Check input range data is valid data.
  if (start == nullptr || end == nullptr) {
    throw SubarrayException("[add_label_range] Invalid range");
  }
  // Get dimension label range and check the label is in fact fixed-sized.
  const auto& dim_label_ref =
      array_->array_schema_latest().dimension_label(label_name);
  if (dim_label_ref.label_cell_val_num() == constants::var_num) {
    throw SubarrayException(
        "[add_label_range] Cannot add a fixed-sized range to a variable sized "
        "dimension label");
  }
  // Add the label range to this subarray.
  return this->add_label_range(
      dim_label_ref,
      Range(start, end, datatype_size(dim_label_ref.label_type())),
      err_on_range_oob_);
}

void Subarray::add_label_range_var(
    const std::string& label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  // Check the input range data is valid.
  if ((start == nullptr && start_size != 0) ||
      (end == nullptr && end_size != 0)) {
    throw SubarrayException("[add_label_range_var] Invalid range");
  }
  // Get the dimension label range and check the label is in fact
  // variable-sized.
  const auto& dim_label_ref =
      array_->array_schema_latest().dimension_label(label_name);
  if (dim_label_ref.label_cell_val_num() != constants::var_num) {
    throw SubarrayException(
        "[add_label_range_var] Cannot add a variable-sized range to a "
        "fixed-sized dimension label");
  }
  // Add the label range to this subarray.
  return this->add_label_range(
      dim_label_ref,
      Range(start, start_size, end, end_size),
      err_on_range_oob_);
}

void Subarray::add_range(
    uint32_t dim_idx, Range&& range, const bool read_range_oob_error) {
  auto dim_num = array_->array_schema_latest().dim_num();
  if (dim_idx >= dim_num) {
    throw SubarrayException(
        "Cannot add range to dimension; Invalid dimension index");
  }

  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_.clear();

  // Restrict the range to the dimension domain and add.
  auto dim_name = array_->array_schema_latest().dimension_ptr(dim_idx)->name();
  auto&& [error_status, oob_warning] =
      range_subset_[dim_idx].add_range(range, read_range_oob_error);
  if (!error_status.ok()) {
    throw SubarrayException(
        "Cannot add range to dimension '" + dim_name + "'; " +
        error_status.message());
  }
  if (oob_warning.has_value()) {
    LOG_WARN(oob_warning.value() + " on dimension '" + dim_name + "'");
  }

  // Update is default and stats counter.
  is_default_[dim_idx] = range_subset_[dim_idx].is_implicitly_initialized();
  stats_->add_counter("add_range_dim_" + std::to_string(dim_idx), 1);
}

void Subarray::add_range_unsafe(uint32_t dim_idx, const Range& range) {
  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_.clear();

  // Add the range
  throw_if_not_ok(range_subset_[dim_idx].add_range_unrestricted(range));
  is_default_[dim_idx] = range_subset_[dim_idx].is_implicitly_initialized();
}

void Subarray::set_subarray(const void* subarray) {
  if (!array_->array_schema_latest().domain().all_dims_same_type())
    throw SubarrayException(
        "Cannot set subarray; Function not applicable to "
        "heterogeneous domains");

  if (!array_->array_schema_latest().domain().all_dims_fixed())
    throw SubarrayException(
        "Cannot set subarray; Function not applicable to "
        "domains with variable-sized dimensions");

  add_default_ranges();
  if (subarray != nullptr) {
    auto dim_num = array_->array_schema_latest().dim_num();
    auto s_ptr = (const unsigned char*)subarray;
    uint64_t offset = 0;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size =
          2 * array_->array_schema_latest().dimension_ptr(d)->coord_size();
      Range range(&s_ptr[offset], r_size);
      this->add_range(d, std::move(range), err_on_range_oob_);
      offset += r_size;
    }
  }
}

void Subarray::set_subarray_unsafe(const void* subarray) {
  add_default_ranges();
  if (subarray != nullptr) {
    auto dim_num = array_->array_schema_latest().dim_num();
    auto s_ptr = (const unsigned char*)subarray;
    uint64_t offset = 0;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size =
          2 * array_->array_schema_latest().dimension_ptr(d)->coord_size();
      Range range(&s_ptr[offset], r_size);
      this->add_range_unsafe(d, std::move(range));
      offset += r_size;
    }
  }
}

void Subarray::add_range(unsigned dim_idx, const void* start, const void* end) {
  if (dim_idx >= this->array_->array_schema_latest().dim_num())
    throw SubarrayException("Cannot add range; Invalid dimension index");

  if (has_label_ranges(dim_idx)) {
    throw SubarrayException(
        "Cannot add range to to dimension; A range is already set on a "
        "dimension label for this dimension");
  }

  if (start == nullptr || end == nullptr) {
    throw SubarrayException("Cannot add range; Invalid range");
  }

  if (this->array_->array_schema_latest()
          .domain()
          .dimension_ptr(dim_idx)
          ->var_size()) {
    throw SubarrayException("Cannot add range; Range must be fixed-sized");
  }

  // Prepare a temp range
  std::vector<uint8_t> range;
  auto coord_size =
      this->array_->array_schema_latest().dimension_ptr(dim_idx)->coord_size();
  range.resize(2 * coord_size);
  std::memcpy(&range[0], start, coord_size);
  std::memcpy(&range[coord_size], end, coord_size);

  // Add range
  this->add_range(dim_idx, Range(&range[0], 2 * coord_size), err_on_range_oob_);
}

void Subarray::add_point_ranges(
    unsigned dim_idx, const void* start, uint64_t count, bool check_for_label) {
  if (dim_idx >= this->array_->array_schema_latest().dim_num()) {
    throw SubarrayException("Cannot add range; Invalid dimension index");
  }

  if (check_for_label && label_range_subset_[dim_idx].has_value()) {
    throw SubarrayException(
        "Cannot add range to to dimension; A range is already set on a "
        "dimension label for this dimension");
  }

  if (start == nullptr) {
    throw SubarrayException("Cannot add ranges; Invalid start pointer");
  }

  if (this->array_->array_schema_latest()
          .domain()
          .dimension_ptr(dim_idx)
          ->var_size()) {
    throw SubarrayException("Cannot add range; Range must be fixed-sized");
  }

  // Prepare a temp range
  std::vector<uint8_t> range;
  auto coord_size =
      this->array_->array_schema_latest().dimension_ptr(dim_idx)->coord_size();
  range.resize(2 * coord_size);

  for (size_t i = 0; i < count; i++) {
    uint8_t* ptr = (uint8_t*)start + coord_size * i;
    // point ranges
    std::memcpy(&range[0], ptr, coord_size);
    std::memcpy(&range[coord_size], ptr, coord_size);

    // Add range
    this->add_range(
        dim_idx, Range(&range[0], 2 * coord_size), err_on_range_oob_);
  }
}

void Subarray::add_ranges_list(
    unsigned dim_idx, const void* start, uint64_t count, bool check_for_label) {
  if (dim_idx >= this->array_->array_schema_latest().dim_num()) {
    throw SubarrayException("Cannot add range; Invalid dimension index");
  }

  if (check_for_label && label_range_subset_[dim_idx].has_value()) {
    throw SubarrayException(
        "Cannot add range to to dimension; A range is already set on a "
        "dimension label for this dimension");
  }

  if (count % 2) {
    throw SubarrayException(
        "add_ranges_list: Invalid count " + std::to_string(count) +
        ", count must be a multiple of 2 ");
  }

  if (start == nullptr) {
    throw SubarrayException("Cannot add ranges; Invalid start pointer");
  }

  if (this->array_->array_schema_latest()
          .domain()
          .dimension_ptr(dim_idx)
          ->var_size()) {
    throw SubarrayException("Cannot add range; Range must be fixed-sized");
  }

  // Prepare a temp range
  auto coord_size =
      this->array_->array_schema_latest().dimension_ptr(dim_idx)->coord_size();

  for (size_t i = 0; i < count / 2; i++) {
    uint8_t* ptr = (uint8_t*)start + 2 * coord_size * i;

    // Add range
    this->add_range(dim_idx, Range(ptr, 2 * coord_size), err_on_range_oob_);
  }
}

void Subarray::add_range_by_name(
    const std::string& dim_name, const void* start, const void* end) {
  unsigned dim_idx =
      array_->array_schema_latest().domain().get_dimension_index(dim_name);
  add_range(dim_idx, start, end);
}

void Subarray::add_range_var(
    unsigned dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (dim_idx >= array_->array_schema_latest().dim_num()) {
    throw SubarrayException("Cannot add range; Invalid dimension index");
  }

  if (has_label_ranges(dim_idx)) {
    throw SubarrayException(
        "Cannot add range to to dimension; A range is already set on a "
        "dimension label for this dimension");
  }

  if ((start == nullptr && start_size != 0) ||
      (end == nullptr && end_size != 0)) {
    throw SubarrayException("Cannot add range; Invalid range");
  }

  if (!array_->array_schema_latest()
           .domain()
           .dimension_ptr(dim_idx)
           ->var_size()) {
    throw SubarrayException("Cannot add range; Range must be variable-sized");
  }

  // Add range
  this->add_range(
      dim_idx, Range(start, start_size, end, end_size), err_on_range_oob_);
}

void Subarray::add_range_var_by_name(
    const std::string& dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  unsigned dim_idx =
      array_->array_schema_latest().domain().get_dimension_index(dim_name);
  add_range_var(dim_idx, start, start_size, end, end_size);
}

const std::vector<Range>& Subarray::get_attribute_ranges(
    const std::string& attr_name) const {
  const auto& ranges = attr_range_subset_.at(attr_name);
  return ranges;
}

const std::string& Subarray::get_label_name(const uint32_t dim_index) const {
  if (!has_label_ranges(dim_index)) {
    throw SubarrayException("[get_label_name] No label ranges set");
  }
  return label_range_subset_[dim_index]->name_;
}

void Subarray::get_label_range(
    const std::string& label_name,
    uint64_t range_idx,
    const void** start,
    const void** end) const {
  auto dim_idx = array_->array_schema_latest()
                     .dimension_label(label_name)
                     .dimension_index();
  if (!label_range_subset_[dim_idx].has_value() ||
      label_range_subset_[dim_idx].value().name_ != label_name) {
    throw SubarrayException(
        "[get_label_range] No ranges set on dimension label '" + label_name +
        "'");
  }
  const auto& range = label_range_subset_[dim_idx].value().ranges_[range_idx];
  *start = range.start_fixed();
  *end = range.end_fixed();
}

void Subarray::get_label_range_num(
    const std::string& label_name, uint64_t* range_num) const {
  auto dim_idx = array_->array_schema_latest()
                     .dimension_label(label_name)
                     .dimension_index();
  *range_num = (label_range_subset_[dim_idx].has_value() &&
                label_range_subset_[dim_idx].value().name_ == label_name) ?
                   label_range_subset_[dim_idx].value().ranges_.num_ranges() :
                   0;
}

void Subarray::get_label_range_var(
    const std::string& label_name,
    uint64_t range_idx,
    void* start,
    void* end) const {
  auto dim_idx = array_->array_schema_latest()
                     .dimension_label(label_name)
                     .dimension_index();
  if (!label_range_subset_[dim_idx].has_value() ||
      label_range_subset_[dim_idx].value().name_ != label_name) {
    throw SubarrayException(
        "[get_label_range_var] No ranges set on dimension label '" +
        label_name + "'");
  }
  const auto& range = label_range_subset_[dim_idx].value().ranges_[range_idx];
  std::memcpy(start, range.start_str().data(), range.start_size());
  std::memcpy(end, range.end_str().data(), range.end_size());
}

void Subarray::get_label_range_var_size(
    const std::string& label_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  auto dim_idx = array_->array_schema_latest()
                     .dimension_label(label_name)
                     .dimension_index();
  if (!label_range_subset_[dim_idx].has_value() ||
      label_range_subset_[dim_idx].value().name_ != label_name) {
    throw SubarrayException(
        "[get_label_range_var_size] No ranges set on dimension label '" +
        label_name + "'");
  }
  const auto& range = label_range_subset_[dim_idx].value().ranges_[range_idx];
  *start_size = range.start_size();
  *end_size = range.end_size();
}

void Subarray::get_range_var(
    unsigned dim_idx, uint64_t range_idx, void* start, void* end) const {
  uint64_t start_size = 0;
  uint64_t end_size = 0;
  this->get_range_var_size(dim_idx, range_idx, &start_size, &end_size);

  const void* range_start;
  const void* range_end;
  get_range(dim_idx, range_idx, &range_start, &range_end);

  std::memcpy(start, range_start, start_size);
  std::memcpy(end, range_end, end_size);
}

void Subarray::get_range_num_from_name(
    const std::string& dim_name, uint64_t* range_num) const {
  unsigned dim_idx =
      array_->array_schema_latest().domain().get_dimension_index(dim_name);
  get_range_num(dim_idx, range_num);
}

void Subarray::get_range_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end) const {
  unsigned dim_idx =
      array_->array_schema_latest().domain().get_dimension_index(dim_name);
  get_range(dim_idx, range_idx, start, end);
}

void Subarray::get_range_var_size_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  unsigned dim_idx =
      array_->array_schema_latest().domain().get_dimension_index(dim_name);
  get_range_var_size(dim_idx, range_idx, start_size, end_size);
}

void Subarray::get_range_var_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    void* start,
    void* end) const {
  unsigned dim_idx =
      array_->array_schema_latest().domain().get_dimension_index(dim_name);
  get_range_var(dim_idx, range_idx, start, end);
}
const shared_ptr<OpenedArray> Subarray::array() const {
  return array_;
}

uint64_t Subarray::cell_num() const {
  const auto& array_schema = array_->array_schema_latest();
  unsigned dim_num = array_schema.dim_num();
  uint64_t ret = 1;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim{array_schema.dimension_ptr(d)};
    uint64_t num = 0;
    auto& range_subset = range_subset_[d];
    for (uint64_t index = 0; index < range_subset.num_ranges(); ++index)
      num += dim->domain_range(range_subset[index]);
    ret = utils::math::safe_mul(ret, num);
  }

  return ret;
}

uint64_t Subarray::cell_num(uint64_t range_idx) const {
  uint64_t cell_num = 1, range_cell_num;
  auto& domain{array_->array_schema_latest().domain()};
  unsigned dim_num = array_->array_schema_latest().dim_num();
  auto layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;
  uint64_t tmp_idx = range_idx;

  // Unary case or GLOBAL_ORDER
  if (range_num() == 1) {
    for (unsigned d = 0; d < dim_num; ++d) {
      range_cell_num =
          domain.dimension_ptr(d)->domain_range(range_subset_[d][0]);
      if (range_cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
        return range_cell_num;

      cell_num = utils::math::safe_mul(range_cell_num, cell_num);
      if (cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
        return cell_num;
    }

    return cell_num;
  }

  // Non-unary case (range_offsets_ must be computed)
  if (layout == Layout::ROW_MAJOR) {
    assert(!range_offsets_.empty());
    for (unsigned d = 0; d < dim_num; ++d) {
      range_cell_num = domain.dimension_ptr(d)->domain_range(
          range_subset_[d][tmp_idx / range_offsets_[d]]);
      tmp_idx %= range_offsets_[d];
      if (range_cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
        return range_cell_num;

      cell_num = utils::math::safe_mul(range_cell_num, cell_num);
      if (cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
        return cell_num;
    }
  } else if (layout == Layout::COL_MAJOR) {
    assert(!range_offsets_.empty());
    for (unsigned d = dim_num - 1;; --d) {
      range_cell_num = domain.dimension_ptr(d)->domain_range(
          range_subset_[d][tmp_idx / range_offsets_[d]]);
      tmp_idx %= range_offsets_[d];
      if (range_cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
        return range_cell_num;

      cell_num = utils::math::safe_mul(range_cell_num, cell_num);
      if (cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
        return cell_num;

      if (d == 0)
        break;
    }
  } else {  // GLOBAL_ORDER handled above
    assert(false);
  }

  return cell_num;
}

uint64_t Subarray::cell_num(const std::vector<uint64_t>& range_coords) const {
  const auto& array_schema = array_->array_schema_latest();
  auto dim_num = array_->array_schema_latest().dim_num();
  assert(dim_num == range_coords.size());

  uint64_t ret = 1;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim{array_schema.dimension_ptr(d)};
    ret = utils::math::safe_mul(
        ret, dim->domain_range(range_subset_[d][range_coords[d]]));
    if (ret == std::numeric_limits<uint64_t>::max())  // Overflow
      return ret;
  }

  return ret;
}

void Subarray::clear() {
  range_offsets_.clear();
  range_subset_.clear();
  label_range_subset_.clear();
  is_default_.clear();
  est_result_size_computed_ = false;
  tile_overlap_.clear();
}

void Subarray::clear_tile_overlap() {
  tile_overlap_.clear();
}

uint64_t Subarray::tile_overlap_byte_size() const {
  return tile_overlap_.byte_size();
}

bool Subarray::coincides_with_tiles() const {
  if (range_num() != 1)
    return false;

  auto dim_num = array_->array_schema_latest().dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim{array_->array_schema_latest().dimension_ptr(d)};
    if (!dim->coincides_with_tiles(range_subset_[d][0]))
      return false;
  }

  return true;
}

void Subarray::check_oob() {
  for (auto& subset : range_subset_) {
    subset.check_oob();
  }
}

template <class T>
Subarray Subarray::crop_to_tile(const T* tile_coords, Layout layout) const {
  // TBD: is it ok that Subarray log id will increase as if it's a new subarray?
  Subarray ret(array_, layout, stats_->parent(), logger_, false);
  crop_to_tile_impl<T, Subarray>(tile_coords, ret);

  return ret;
}

template <class T>
void Subarray::crop_to_tile(
    DenseTileSubarray<T>& ret, const T* tile_coords) const {
  crop_to_tile_impl<T, DenseTileSubarray<T>>(tile_coords, ret);
}

template <class T>
uint64_t Subarray::tile_cell_num(const T* tile_coords) const {
  uint64_t ret = 1;
  T new_range[2];
  bool overlaps;

  // Get tile subarray based on the input coordinates
  const auto& array_schema = array_->array_schema_latest();
  std::vector<T> tile_subarray(2 * dim_num());
  array_schema.domain().get_tile_subarray(tile_coords, &tile_subarray[0]);

  // Compute cell num per dims.
  for (unsigned d = 0; d < dim_num(); ++d) {
    uint64_t cell_num_for_dim = 0;
    for (size_t r = 0; r < range_subset_[d].num_ranges(); ++r) {
      const auto& range = range_subset_[d][r];
      rectangle::overlap(
          (const T*)range.data(),
          &tile_subarray[2 * d],
          1,
          new_range,
          &overlaps);

      if (overlaps) {
        // Here we need to do +1 because both the start and end are included in
        // the number of cells.
        cell_num_for_dim +=
            static_cast<uint64_t>(new_range[1] - new_range[0] + 1);
      }
    }

    ret *= cell_num_for_dim;
  }

  return ret;
}

uint32_t Subarray::dim_num() const {
  return array_->array_schema_latest().dim_num();
}

NDRange Subarray::domain() const {
  return array_->array_schema_latest().domain().domain();
}

bool Subarray::empty() const {
  return range_num() == 0;
}

bool Subarray::empty(uint32_t dim_idx) const {
  return range_subset_[dim_idx].is_empty();
}

void Subarray::get_range(
    uint32_t dim_idx, uint64_t range_idx, const Range** range) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  if (dim_idx >= dim_num) {
    throw SubarrayException("Cannot get range; Invalid dimension index");
  }

  auto range_num = range_subset_[dim_idx].num_ranges();
  if (range_idx >= range_num) {
    throw SubarrayException("Cannot get range; Invalid range index");
  }

  *range = &range_subset_[dim_idx][range_idx];
}

void Subarray::get_range(
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  if (dim_idx >= dim_num) {
    throw SubarrayException("Cannot get range; Invalid dimension index");
  }

  auto range_num = range_subset_[dim_idx].num_ranges();
  if (range_idx >= range_num) {
    throw SubarrayException("Cannot get range; Invalid range index");
  }

  auto& range = range_subset_[dim_idx][range_idx];
  auto is_var = range.var_size();
  if (is_var) {
    *start = range_subset_[dim_idx][range_idx].start_str().data();
    *end = range_subset_[dim_idx][range_idx].end_str().data();
  } else {
    *start = range_subset_[dim_idx][range_idx].start_fixed();
    *end = range_subset_[dim_idx][range_idx].end_fixed();
  }
}

void Subarray::get_range_var_size(
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  const auto& schema = array_->array_schema_latest();
  auto dim_num = schema.dim_num();
  if (dim_idx >= dim_num) {
    throw SubarrayException(
        "Cannot get var range size; Invalid dimension index");
  }

  auto dim{schema.domain().dimension_ptr(dim_idx)};
  if (!dim->var_size()) {
    throw SubarrayException(
        "Cannot get var range size; Dimension " + dim->name() +
        " is not var sized");
  }

  auto range_num = range_subset_[dim_idx].num_ranges();
  if (range_idx >= range_num) {
    throw SubarrayException("Cannot get var range size; Invalid range index");
  }

  *start_size = range_subset_[dim_idx][range_idx].start_size();
  *end_size = range_subset_[dim_idx][range_idx].end_size();
}

void Subarray::get_range_num(uint32_t dim_idx, uint64_t* range_num) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  if (dim_idx >= dim_num) {
    throw SubarrayException(
        "Cannot get number of ranges for a dimension; Invalid dimension "
        "index " +
        std::to_string(dim_idx) + " requested, " + std::to_string(dim_num - 1) +
        " max avail.");
  }
  *range_num = range_subset_[dim_idx].num_ranges();
}

Subarray Subarray::get_subarray(uint64_t start, uint64_t end) const {
  // TBD: is it ok that Subarray log id will increase as if it's a new subarray?
  Subarray ret(array_, layout_, stats_->parent(), logger_, coalesce_ranges_);

  auto start_coords = get_range_coords(start);
  auto end_coords = get_range_coords(end);

  auto dim_num = this->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    if (!range_subset_[d].is_implicitly_initialized()) {
      for (uint64_t r = start_coords[d]; r <= end_coords[d]; ++r) {
        ret.add_range_unsafe(d, range_subset_[d][r]);
      }
    }
  }

  ret.tile_overlap_ = tile_overlap_;
  ret.tile_overlap_.update_range(start, end);

  // Compute range offsets
  ret.compute_range_offsets();

  return ret;
}

bool Subarray::has_label_ranges() const {
  return std::any_of(
      label_range_subset_.cbegin(),
      label_range_subset_.cend(),
      [](const auto& range_subset) {
        return range_subset.has_value() && !range_subset->ranges_.is_empty();
      });
}

bool Subarray::has_label_ranges(const uint32_t dim_index) const {
  return label_range_subset_[dim_index].has_value() &&
         !label_range_subset_[dim_index]->ranges_.is_empty();
}

int Subarray::label_ranges_num() const {
  return std::count_if(
      std::begin(label_range_subset_),
      std::end(label_range_subset_),
      [](const auto& range_subset) {
        return range_subset.has_value() && !range_subset->ranges_.is_empty();
      });
}

bool Subarray::is_default(uint32_t dim_index) const {
  return range_subset_[dim_index].is_implicitly_initialized();
}

bool Subarray::is_set() const {
  for (auto& range_subset : range_subset_)
    if (!range_subset.is_implicitly_initialized())
      return true;
  return false;
}

int32_t Subarray::count_set_ranges() const {
  int32_t num_set = 0;
  for (auto& range_subset : range_subset_)
    if (!range_subset.is_implicitly_initialized())
      ++num_set;
  return num_set;
}

bool Subarray::is_set(unsigned dim_idx) const {
  assert(dim_idx < dim_num());
  return !range_subset_[dim_idx].is_implicitly_initialized();
}

bool Subarray::is_unary() const {
  for (const auto& range_subset : range_subset_) {
    if (!range_subset.has_single_element())
      return false;
  }
  return true;
}

void Subarray::set_is_default(uint32_t dim_index, bool is_default) {
  if (is_default) {
    auto dim{array_->array_schema_latest().dimension_ptr(dim_index)};
    range_subset_.at(dim_index) = RangeSetAndSuperset(
        dim->type(), dim->domain(), is_default, coalesce_ranges_);
  }
  is_default_[dim_index] = is_default;
}

void Subarray::set_layout(Layout layout) {
  layout_ = layout;
}

void Subarray::set_config(const QueryType query_type, const Config& config) {
  merge_overlapping_ranges_ = config.get<bool>(
      "sm.merge_overlapping_ranges_experimental", Config::MustFindMarker());

  if (query_type == QueryType::READ) {
    bool found = false;
    std::string read_range_oob_str = config.get("sm.read_range_oob", &found);
    assert(found);
    if (read_range_oob_str != "error" && read_range_oob_str != "warn")
      throw SubarrayException(
          "[set_config] Invalid value " + read_range_oob_str +
          " for sm.read_range_obb. Acceptable values are 'error' or 'warn'.");
    err_on_range_oob_ = read_range_oob_str == "error";
  }
};

void Subarray::set_coalesce_ranges(bool coalesce_ranges) {
  if (count_set_ranges()) {
    throw SubarrayException(
        "non-default ranges have been set, cannot change "
        "coalesce_ranges setting!");
  }
  // trying to mimic conditions at ctor()
  coalesce_ranges_ = coalesce_ranges;
  add_default_ranges();
}

void Subarray::to_byte_vec(std::vector<uint8_t>* byte_vec) const {
  if (range_num() != 1) {
    throw SubarrayException(
        "Cannot export to byte vector; The subarray must be unary");
  }
  byte_vec->clear();
  for (const auto& subset : range_subset_) {
    auto offset = byte_vec->size();
    byte_vec->resize(offset + subset[0].size());
    std::memcpy(&(*byte_vec)[offset], subset[0].data(), subset[0].size());
  }
}

Layout Subarray::layout() const {
  return layout_;
}

FieldDataSize Subarray::get_est_result_size(
    std::string_view field_name, const Config* config, ThreadPool* compute_tp) {
  /*
   * This check throws a logic error because we expect the field name to have
   * already been validated in the C API.
   */
  if (field_name.data() == nullptr) {
    throw std::logic_error(
        "Cannot get estimated result size; field name is null");
  }

  // Check if name is attribute or dimension
  const auto& array_schema = array_->array_schema_latest();
  const bool is_dim = array_schema.is_dim(field_name.data());
  const bool is_attr = array_schema.is_attr(field_name.data());

  // Check if attribute/dimension exists
  if (!ArraySchema::is_special_attribute(field_name.data()) && !is_dim &&
      !is_attr) {
    throw SubarrayException(
        std::string("Cannot get estimated result size; ") +
        "there is no field named '" + std::string(field_name) + "'");
  }

  bool is_variable_sized{
      field_name != constants::coords &&
      array_schema.var_size(field_name.data())};
  bool is_nullable{array_schema.is_nullable(field_name.data())};

  // Compute tile overlap for each fragment
  compute_est_result_size(config, compute_tp);

  FieldDataSize r{
      static_cast<size_t>(
          std::ceil(est_result_size_[field_name.data()].size_fixed_)),
      is_variable_sized ? static_cast<size_t>(std::ceil(
                              est_result_size_[field_name.data()].size_var_)) :
                          0,
      is_nullable ? static_cast<size_t>(std::ceil(
                        est_result_size_[field_name.data()].size_validity_)) :
                    0};
  /*
   * Special fix-ups may be necessary if data is empty or very short.
   */
  if (is_variable_sized) {
    if (r.variable_ == 0) {
      // Assert: no variable data for a variable-sized field
      // Ensure that there are no offsets.
      r.fixed_ = 0;
      r.validity_ = 0;
    } else {
      // Ensure that there's space for at least one offset value.
      const auto off_cell_size = constants::cell_var_offset_size;
      if (r.fixed_ < off_cell_size) {
        r.fixed_ = off_cell_size;
      }
      // Ensure that there's space for at least one data value.
      const auto val_cell_size =
          datatype_size(array_schema.type(field_name.data()));
      if (r.variable_ < val_cell_size) {
        r.variable_ = val_cell_size;
      }
      if (is_nullable) {
        const auto validity_cell_size = constants::cell_validity_size;
        if (r.validity_ < validity_cell_size) {
          r.validity_ = validity_cell_size;
        }
      }
    }
  } else {
    /*
     * If the fixed data is not empty, ensure it is large enough to contain at
     * least one cell.
     */
    const auto cell_size = array_schema.cell_size(field_name.data());
    if (0 < r.fixed_ && r.fixed_ < cell_size) {
      r.fixed_ = cell_size;
      if (is_nullable) {
        r.validity_ = 1;
      }
    }
  }
  return r;
}

FieldDataSize Subarray::get_max_memory_size(
    const char* name, const Config* config, ThreadPool* compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr) {
    throw SubarrayException(
        "Cannot get max memory size; field name cannot be null");
  }

  // Check if name is attribute or dimension
  const auto& array_schema = array_->array_schema_latest();
  bool is_dim = array_schema.is_dim(name);
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (!ArraySchema::is_special_attribute(name) && !is_dim && !is_attr) {
    throw SubarrayException(
        std::string("Cannot get max memory size; ") +
        "there is no field named '" + name + "'");
  }

  bool is_variable_sized{
      name != constants::coords && array_schema.var_size(name)};
  bool is_nullable{array_schema.is_nullable(name)};

  // Compute tile overlap for each fragment
  compute_est_result_size(config, compute_tp);
  return {
      max_mem_size_[name].size_fixed_,
      is_variable_sized ? max_mem_size_[name].size_var_ : 0,
      is_nullable ? max_mem_size_[name].size_validity_ : 0};
}

std::vector<uint64_t> Subarray::get_range_coords(uint64_t range_idx) const {
  std::vector<uint64_t> ret;

  uint64_t tmp_idx = range_idx;
  auto dim_num = this->dim_num();
  auto layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;

  if (layout == Layout::ROW_MAJOR) {
    for (unsigned i = 0; i < dim_num; ++i) {
      ret.push_back(tmp_idx / range_offsets_[i]);
      tmp_idx %= range_offsets_[i];
    }
  } else if (layout == Layout::COL_MAJOR) {
    for (unsigned i = dim_num - 1;; --i) {
      ret.push_back(tmp_idx / range_offsets_[i]);
      tmp_idx %= range_offsets_[i];
      if (i == 0)
        break;
    }
    std::reverse(ret.begin(), ret.end());
  } else {
    // Global order or Hilbert - single range
    assert(layout == Layout::GLOBAL_ORDER || layout == Layout::HILBERT);
    assert(range_num() == 1);
    for (unsigned i = 0; i < dim_num; ++i)
      ret.push_back(0);
  }

  return ret;
}

uint64_t Subarray::range_idx(const std::vector<uint64_t>& range_coords) const {
  uint64_t ret = 0;
  auto dim_num = this->dim_num();
  for (unsigned i = 0; i < dim_num; ++i)
    ret += range_offsets_[i] * range_coords[i];

  return ret;
}

uint64_t Subarray::range_num() const {
  if (range_subset_.empty()) {
    return 0;
  }

  uint64_t ret = 1;
  for (const auto& subset : range_subset_) {
    ret *= subset.num_ranges();
  }

  return ret;
}

NDRange Subarray::ndrange(uint64_t range_idx) const {
  NDRange ret;
  uint64_t tmp_idx = range_idx;
  auto dim_num = this->dim_num();
  auto layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;
  ret.reserve(dim_num);

  // Unary case or GLOBAL_ORDER
  if (range_idx == 0 && range_num() == 1) {
    for (unsigned d = 0; d < dim_num; ++d)
      ret.emplace_back(range_subset_[d][0]);
    return ret;
  }

  // Non-unary case (range_offsets_ must be computed)
  if (layout == Layout::ROW_MAJOR) {
    assert(!range_offsets_.empty());
    for (unsigned d = 0; d < dim_num; ++d) {
      ret.emplace_back(range_subset_[d][tmp_idx / range_offsets_[d]]);
      tmp_idx %= range_offsets_[d];
    }
  } else if (layout == Layout::COL_MAJOR) {
    assert(!range_offsets_.empty());
    for (unsigned d = dim_num - 1;; --d) {
      ret.emplace_back(range_subset_[d][tmp_idx / range_offsets_[d]]);
      tmp_idx %= range_offsets_[d];
      if (d == 0)
        break;
    }
    std::reverse(ret.begin(), ret.end());
  } else {  // GLOBAL_ORDER handled above
    assert(false);
  }

  return ret;
}

NDRange Subarray::ndrange(const std::vector<uint64_t>& range_coords) const {
  auto dim_num = this->dim_num();
  NDRange ret;
  ret.reserve(dim_num);
  for (unsigned d = 0; d < dim_num; ++d)
    ret.emplace_back(range_subset_[d][range_coords[d]]);
  return ret;
}

void Subarray::set_attribute_ranges(
    const std::string& attr_name, const std::vector<Range>& ranges) {
  if (!array_->array_schema_latest().is_attr(attr_name)) {
    throw SubarrayException(
        "[set_attribute_ranges] No attribute named " + attr_name + "'.");
  }
  auto search = attr_range_subset_.find(attr_name);
  if (search != attr_range_subset_.end()) {
    throw SubarrayException(
        "[set_attribute_ranges] Ranges are already set for attribute '" +
        attr_name + "'.");
  }
  attr_range_subset_[attr_name] = ranges;
}

const std::vector<Range>& Subarray::ranges_for_label(
    const std::string& label_name) const {
  auto dim_idx = array_->array_schema_latest()
                     .dimension_label(label_name)
                     .dimension_index();
  if (!label_range_subset_[dim_idx].has_value() ||
      label_range_subset_[dim_idx].value().name_ != label_name) {
    throw SubarrayException(
        "[ranges_for_label] No ranges set on dimension label '" + label_name +
        "'");
  }
  return label_range_subset_[dim_idx]->get_ranges();
}

void Subarray::set_ranges_for_dim(
    uint32_t dim_idx, const std::vector<Range>& ranges) {
  auto dim{array_->array_schema_latest().dimension_ptr(dim_idx)};
  range_subset_[dim_idx] =
      RangeSetAndSuperset(dim->type(), dim->domain(), false, coalesce_ranges_);
  is_default_[dim_idx] = false;
  // Add each range individually so that contiguous
  // ranges may be coalesced.
  for (const auto& range : ranges) {
    throw_if_not_ok(range_subset_[dim_idx].add_range_unrestricted(range));
  }
  is_default_[dim_idx] = range_subset_[dim_idx].is_implicitly_initialized();
}

void Subarray::set_label_ranges_for_dim(
    const uint32_t dim_idx,
    const std::string& name,
    const std::vector<Range>& ranges) {
  auto dim{array_->array_schema_latest().dimension_ptr(dim_idx)};
  label_range_subset_[dim_idx] =
      LabelRangeSubset(name, dim->type(), coalesce_ranges_);
  for (const auto& range : ranges) {
    throw_if_not_ok(
        label_range_subset_[dim_idx].value().ranges_.add_range_unrestricted(
            range));
  }
}

void Subarray::split(
    unsigned splitting_dim,
    const ByteVecValue& splitting_value,
    Subarray* r1,
    Subarray* r2) const {
  assert(r1 != nullptr);
  assert(r2 != nullptr);
  *r1 = Subarray(array_, layout_, stats_->parent(), logger_, coalesce_ranges_);
  *r2 = Subarray(array_, layout_, stats_->parent(), logger_, coalesce_ranges_);

  auto dim_num = array_->array_schema_latest().dim_num();

  Range sr1, sr2;
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& r = range_subset_[d][0];
    if (d == splitting_dim) {
      auto dim{array_->array_schema_latest().dimension_ptr(d)};
      dim->split_range(r, splitting_value, &sr1, &sr2);
      r1->add_range_unsafe(d, sr1);
      r2->add_range_unsafe(d, sr2);
    } else {
      if (!range_subset_[d].is_implicitly_initialized()) {
        r1->add_range_unsafe(d, r);
        r2->add_range_unsafe(d, r);
      }
    }
  }
}

void Subarray::split(
    uint64_t splitting_range,
    unsigned splitting_dim,
    const ByteVecValue& splitting_value,
    Subarray* r1,
    Subarray* r2) const {
  assert(r1 != nullptr);
  assert(r2 != nullptr);
  *r1 = Subarray(array_, layout_, stats_->parent(), logger_, coalesce_ranges_);
  *r2 = Subarray(array_, layout_, stats_->parent(), logger_, coalesce_ranges_);

  // For easy reference
  const auto& array_schema = array_->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  uint64_t range_num;
  Range sr1, sr2;

  for (unsigned d = 0; d < dim_num; ++d) {
    this->get_range_num(d, &range_num);
    if (d != splitting_dim) {
      if (!range_subset_[d].is_implicitly_initialized()) {
        for (uint64_t j = 0; j < range_num; ++j) {
          const auto& r = range_subset_[d][j];
          r1->add_range_unsafe(d, r);
          r2->add_range_unsafe(d, r);
        }
      }
    } else {                                // d == splitting_dim
      if (splitting_range != UINT64_MAX) {  // Need to split multiple ranges
        for (uint64_t j = 0; j <= splitting_range; ++j) {
          const auto& r = range_subset_[d][j];
          r1->add_range_unsafe(d, r);
        }
        for (uint64_t j = splitting_range + 1; j < range_num; ++j) {
          const auto& r = range_subset_[d][j];
          r2->add_range_unsafe(d, r);
        }
      } else {  // Need to split a single range
        const auto& r = range_subset_[d][0];
        auto dim{array_schema.dimension_ptr(d)};
        dim->split_range(r, splitting_value, &sr1, &sr2);
        r1->add_range_unsafe(d, sr1);
        r2->add_range_unsafe(d, sr2);
      }
    }
  }
}

const std::vector<std::vector<uint8_t>>& Subarray::tile_coords() const {
  return tile_coords_;
}

template <class T>
void Subarray::compute_tile_coords() {
  auto timer_se = stats_->start_timer("read_compute_tile_coords");
  if (array_->array_schema_latest().tile_order() == Layout::ROW_MAJOR) {
    compute_tile_coords_row<T>();
  } else {
    compute_tile_coords_col<T>();
  }
}

template <class T>
const T* Subarray::tile_coords_ptr(
    const std::vector<T>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  auto coord_size{array_->array_schema_latest().dimension_ptr(0)->coord_size()};
  std::memcpy(&((*aux_tile_coords)[0]), &tile_coords[0], dim_num * coord_size);
  auto it = tile_coords_map_.find(*aux_tile_coords);
  if (it == tile_coords_map_.end())
    return nullptr;
  return (const T*)&(tile_coords_[it->second][0]);
}

const SubarrayTileOverlap* Subarray::subarray_tile_overlap() const {
  return &tile_overlap_;
}

void Subarray::compute_relevant_fragment_est_result_sizes(
    const std::vector<std::string>& names,
    uint64_t range_start,
    uint64_t range_end,
    std::vector<std::vector<ResultSize>>* result_sizes,
    std::vector<std::vector<MemorySize>>* mem_sizes,
    ThreadPool* compute_tp) {
  // For easy reference
  const auto& array_schema = array_->array_schema_latest();
  auto fragment_metadata = array_->fragment_metadata();
  auto dim_num = array_->array_schema_latest().dim_num();
  auto layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;

  load_relevant_fragment_tile_var_sizes(names, compute_tp);

  // Prepare result sizes vectors
  auto range_num = range_end - range_start + 1;
  result_sizes->resize(range_num);
  std::vector<std::set<std::pair<unsigned, uint64_t>>> frag_tiles(range_num);
  for (size_t r = 0; r < range_num; ++r)
    (*result_sizes)[r].reserve(names.size());

  // Create vector of var and validity flags
  std::vector<bool> var_sizes;
  std::vector<bool> nullable;
  var_sizes.reserve(names.size());
  nullable.reserve(names.size());
  for (const auto& name : names) {
    var_sizes.push_back(array_schema.var_size(name));
    nullable.push_back(array_schema.is_nullable(name));
  }

  auto all_dims_same_type = array_schema.domain().all_dims_same_type();
  auto all_dims_fixed = array_schema.domain().all_dims_fixed();
  auto num_threads = compute_tp->concurrency_level();
  auto ranges_per_thread = (uint64_t)std::ceil((double)range_num / num_threads);
  auto status = parallel_for(compute_tp, 0, num_threads, [&](uint64_t t) {
    auto r_start = range_start + t * ranges_per_thread;
    auto r_end =
        std::min(range_start + (t + 1) * ranges_per_thread - 1, range_end);
    auto r_coords = get_range_coords(r_start);
    for (uint64_t r = r_start; r <= r_end; ++r) {
      compute_relevant_fragment_est_result_sizes(
          array_schema,
          all_dims_same_type,
          all_dims_fixed,
          fragment_metadata,
          names,
          var_sizes,
          nullable,
          r,
          r_coords,
          &(*result_sizes)[r - range_start],
          &frag_tiles[r - range_start]);

      // Get next range coordinates
      if (layout == Layout::ROW_MAJOR) {
        auto d = dim_num - 1;
        ++(r_coords)[d];
        while ((r_coords)[d] >= range_subset_[d].num_ranges() && d != 0) {
          (r_coords)[d] = 0;
          --d;
          ++(r_coords)[d];
        }
      } else if (layout == Layout::COL_MAJOR) {
        auto d = (unsigned)0;
        ++(r_coords)[d];
        while ((r_coords)[d] >= range_subset_[d].num_ranges() &&
               d != dim_num - 1) {
          (r_coords)[d] = 0;
          ++d;
          ++(r_coords)[d];
        }
      } else {
        // Global order - noop
      }
    }
    return Status::Ok();
  });
  throw_if_not_ok(status);

  // Compute the mem sizes vector
  mem_sizes->resize(range_num);
  for (auto& ms : *mem_sizes)
    ms.resize(names.size(), {0, 0, 0});
  std::unordered_set<std::pair<unsigned, uint64_t>, utils::hash::pair_hash>
      all_frag_tiles;
  for (uint64_t r = 0; r < range_num; ++r) {
    auto& mem_vec = (*mem_sizes)[r];
    for (const auto& ft : frag_tiles[r]) {
      auto it = all_frag_tiles.insert(ft);
      if (it.second) {  // If the fragment/tile pair is new
        auto meta = fragment_metadata[ft.first];
        for (size_t i = 0; i < names.size(); ++i) {
          // If this attribute does not exist, skip it as this is likely a new
          // attribute added as a result of schema evolution
          if (!meta->array_schema()->is_field(names[i])) {
            continue;
          }

          auto tile_size = meta->tile_size(names[i], ft.second);
          auto cell_size = array_schema.cell_size(names[i]);
          if (!var_sizes[i]) {
            mem_vec[i].size_fixed_ += tile_size;
            if (nullable[i])
              mem_vec[i].size_validity_ +=
                  tile_size / cell_size * constants::cell_validity_size;
          } else {
            tile_size -= constants::cell_var_offset_size;
            auto tile_var_size =
                meta->loaded_metadata()->tile_var_size(names[i], ft.second);
            mem_vec[i].size_fixed_ += tile_size;
            mem_vec[i].size_var_ += tile_var_size;
            if (nullable[i])
              mem_vec[i].size_validity_ +=
                  tile_var_size / cell_size * constants::cell_validity_size;
          }
        }
      }
    }
  }
}

std::unordered_map<std::string, Subarray::ResultSize>
Subarray::get_est_result_size_map(
    const Config* const config, ThreadPool* const compute_tp) {
  // If the result sizes have not been computed, compute them first
  if (!est_result_size_computed_)
    compute_est_result_size(config, compute_tp);

  return est_result_size_;
}

std::unordered_map<std::string, Subarray::MemorySize>
Subarray::get_max_mem_size_map(
    const Config* const config, ThreadPool* const compute_tp) {
  // If the result sizes have not been computed, compute them first
  if (!est_result_size_computed_)
    compute_est_result_size(config, compute_tp);

  return max_mem_size_;
}

void Subarray::set_est_result_size(
    std::unordered_map<std::string, ResultSize>& est_result_size,
    std::unordered_map<std::string, MemorySize>& max_mem_size) {
  est_result_size_ = est_result_size;
  max_mem_size_ = max_mem_size;
  est_result_size_computed_ = true;
}

void Subarray::sort_and_merge_ranges(ThreadPool* const compute_tp) {
  std::scoped_lock<std::mutex> lock(ranges_sort_mtx_);
  if (ranges_sorted_)
    return;

  // Sort and conditionally merge ranges
  auto timer = stats_->start_timer("sort_and_merge_ranges");
  throw_if_not_ok(parallel_for(
      compute_tp,
      0,
      array_->array_schema_latest().dim_num(),
      [&](uint64_t dim_idx) {
        range_subset_[dim_idx].sort_and_merge_ranges(
            compute_tp, merge_overlapping_ranges_);
        return Status::Ok();
      }));
  ranges_sorted_ = true;
}

bool Subarray::non_overlapping_ranges(ThreadPool* compute_tp) {
  sort_and_merge_ranges(compute_tp);

  std::atomic<bool> non_overlapping_ranges = true;
  auto st = parallel_for(
      compute_tp,
      0,
      array_->array_schema_latest().dim_num(),
      [&](uint64_t dim_idx) {
        bool nor = non_overlapping_ranges_for_dim(dim_idx);
        if (!nor) {
          non_overlapping_ranges = false;
        }
        return Status::Ok();
      });
  throw_if_not_ok(st);
  return non_overlapping_ranges;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Subarray::add_default_ranges() {
  auto& domain{array_->array_schema_latest().domain()};
  auto dim_num = array_->array_schema_latest().dim_num();

  range_subset_.clear();
  for (unsigned dim_index = 0; dim_index < dim_num; ++dim_index) {
    auto dim{domain.dimension_ptr(dim_index)};
    range_subset_.push_back(RangeSetAndSuperset(
        dim->type(), dim->domain(), true, coalesce_ranges_));
  }
  is_default_.resize(dim_num, true);
  add_default_label_ranges(dim_num);
}

void Subarray::add_default_label_ranges(dimension_size_type dim_num) {
  label_range_subset_.clear();
  label_range_subset_.resize(dim_num, nullopt);
}

void Subarray::reset_default_ranges() {
  if (array_->non_empty_domain_computed()) {
    auto dim_num = array_->array_schema_latest().dim_num();
    auto& domain{array_->array_schema_latest().domain()};

    // Process all dimensions one by one.
    for (unsigned d = 0; d < dim_num; d++) {
      // Only enter the check if there are only one range set on the dimension.
      if (!is_default_[d] && range_subset_[d].num_ranges() == 1) {
        // If the range set is the same as the non empty domain.
        auto& ned = array_->non_empty_domain()[d];
        if (ned == range_subset_[d][0]) {
          // Reset the default flag and reset the range subset to be default.
          is_default_[d] = true;
          auto dim{domain.dimension_ptr(d)};
          range_subset_[d] = RangeSetAndSuperset(
              dim->type(), dim->domain(), true, coalesce_ranges_);
        }
      }
    }
  }
}

void Subarray::compute_range_offsets() {
  range_offsets_.clear();

  auto dim_num = this->dim_num();
  auto layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;

  if (layout == Layout::COL_MAJOR) {
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = 1; i < dim_num; ++i)
        range_offsets_.push_back(
            range_offsets_.back() * range_subset_[i - 1].num_ranges());
    }
  } else if (layout == Layout::ROW_MAJOR) {
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = dim_num - 2;; --i) {
        range_offsets_.push_back(
            range_offsets_.back() * range_subset_[i + 1].num_ranges());
        if (i == 0)
          break;
      }
    }
    std::reverse(range_offsets_.begin(), range_offsets_.end());
  } else {
    // Global order or Hilbert - single range
    assert(layout == Layout::GLOBAL_ORDER || layout == Layout::HILBERT);
    assert(range_num() == 1);
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = 1; i < dim_num; ++i)
        range_offsets_.push_back(1);
    }
  }
}

void Subarray::compute_est_result_size(
    const Config* config, ThreadPool* compute_tp) {
  auto timer_se = stats_->start_timer("read_compute_est_result_size");
  if (est_result_size_computed_) {
    return;
  }

  // TODO: This routine is used in the path for the C APIs that estimate
  // result sizes. We need to refactor this routine to handle the scenario
  // where `tile_overlap_` may be truncated to fit the memory budget.
  precompute_tile_overlap(0, range_num() - 1, config, compute_tp, true);

  // Prepare estimated result size vector for all
  // attributes/dimension and zipped coords
  const auto& array_schema = array_->array_schema_latest();
  auto attribute_num = array_schema.attribute_num();
  auto dim_num = array_schema.dim_num();
  auto& attributes = array_schema.attributes();
  auto num = attribute_num + dim_num + 1;
  auto range_num = this->range_num();

  // Compute estimated result in parallel over fragments and ranges
  auto meta = array_->fragment_metadata();

  // Get attribute and dimension names
  std::vector<std::string> names(num);
  for (unsigned i = 0; i < num; ++i) {
    if (i < attribute_num)
      names[i] = attributes[i]->name();
    else if (i < attribute_num + dim_num)
      names[i] = array_schema.domain().dimension_ptr(i - attribute_num)->name();
    else
      names[i] = constants::coords;
  }

  // Compute the estimated result and max memory sizes
  std::vector<std::vector<ResultSize>> result_sizes;
  std::vector<std::vector<MemorySize>> mem_sizes;
  compute_relevant_fragment_est_result_sizes(
      names, 0, range_num - 1, &result_sizes, &mem_sizes, compute_tp);

  // Accummulate the individual estimated result sizes
  std::vector<ResultSize> est_vec(num, ResultSize{0.0, 0.0, 0.0});
  std::vector<MemorySize> mem_vec(num, MemorySize{0, 0, 0});
  for (uint64_t r = 0; r < range_num; ++r) {
    for (size_t i = 0; i < result_sizes[r].size(); ++i) {
      est_vec[i].size_fixed_ += result_sizes[r][i].size_fixed_;
      est_vec[i].size_var_ += result_sizes[r][i].size_var_;
      est_vec[i].size_validity_ += result_sizes[r][i].size_validity_;
      mem_vec[i].size_fixed_ += mem_sizes[r][i].size_fixed_;
      mem_vec[i].size_var_ += mem_sizes[r][i].size_var_;
      mem_vec[i].size_validity_ += mem_sizes[r][i].size_validity_;
    }
  }

  // Calibrate for dense arrays
  uint64_t min_size_fixed, min_size_var, min_size_validity;
  if (array_schema.dense()) {
    auto cell_num = this->cell_num();
    for (unsigned i = 0; i < num; ++i) {
      if (!array_schema.var_size(names[i])) {
        min_size_fixed = cell_num * array_schema.cell_size(names[i]);
        min_size_var = 0;
      } else {
        min_size_fixed = cell_num * constants::cell_var_offset_size;
        min_size_var =
            cell_num * array_schema.attribute(names[i])->fill_value().size();
      }

      if (array_schema.is_nullable(names[i])) {
        min_size_validity = cell_num * constants::cell_validity_size;
      } else {
        min_size_validity = 0;
      }

      if (est_vec[i].size_fixed_ < min_size_fixed)
        est_vec[i].size_fixed_ = min_size_fixed;
      if (est_vec[i].size_var_ < min_size_var)
        est_vec[i].size_var_ = min_size_var;
      if (est_vec[i].size_validity_ < min_size_validity)
        est_vec[i].size_validity_ = min_size_validity;
    }
  }

  // Amplify result estimation
  if (constants::est_result_size_amplification != 1.0) {
    for (auto& r : est_vec) {
      r.size_fixed_ *= constants::est_result_size_amplification;
      r.size_var_ *= constants::est_result_size_amplification;
      r.size_validity_ *= constants::est_result_size_amplification;
    }
  }

  // Set the estimated result size map
  est_result_size_.clear();
  max_mem_size_.clear();
  for (unsigned i = 0; i < num; ++i) {
    est_result_size_[names[i]] = est_vec[i];
    max_mem_size_[names[i]] = mem_vec[i];
  }
  est_result_size_computed_ = true;
}

bool Subarray::est_result_size_computed() {
  return est_result_size_computed_;
}

void Subarray::compute_relevant_fragment_est_result_sizes(
    const ArraySchema& array_schema,
    bool all_dims_same_type,
    bool all_dims_fixed,
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_meta,
    const std::vector<std::string>& name,
    const std::vector<bool>& var_sizes,
    const std::vector<bool>& nullable,
    uint64_t range_idx,
    const std::vector<uint64_t>& range_coords,
    std::vector<ResultSize>* result_sizes,
    std::set<std::pair<unsigned, uint64_t>>* frag_tiles) {
  result_sizes->resize(name.size(), {0.0, 0.0, 0.0});

  const uint64_t translated_range_idx =
      range_idx - tile_overlap_.range_idx_start();

  // Compute estimated result
  auto fragment_num = (unsigned)relevant_fragments_.size();
  for (unsigned i = 0; i < fragment_num; ++i) {
    auto f = relevant_fragments_[i];
    const TileOverlap* const overlap =
        tile_overlap_.at(f, translated_range_idx);
    auto meta = fragment_meta[f];

    // Parse tile ranges
    for (const auto& tr : overlap->tile_ranges_) {
      for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
        for (size_t n = 0; n < name.size(); ++n) {
          // Zipped coords applicable only in homogeneous domains
          if (name[n] == constants::coords && !all_dims_same_type)
            continue;

          // If this attribute does not exist, skip it as this is likely a new
          // attribute added as a result of schema evolution
          if (!meta->array_schema()->is_field(name[n])) {
            continue;
          }

          frag_tiles->insert(std::pair<unsigned, uint64_t>(f, tid));
          auto tile_size = meta->tile_size(name[n], tid);
          auto attr_datatype_size = datatype_size(array_schema.type(name[n]));
          if (!var_sizes[n]) {
            (*result_sizes)[n].size_fixed_ += tile_size;
            if (nullable[n])
              (*result_sizes)[n].size_validity_ +=
                  tile_size / attr_datatype_size *
                  constants::cell_validity_size;
          } else {
            tile_size -= constants::cell_var_offset_size;
            (*result_sizes)[n].size_fixed_ += tile_size;
            auto tile_var_size =
                meta->loaded_metadata()->tile_var_size(name[n], tid);
            (*result_sizes)[n].size_var_ += tile_var_size;
            if (nullable[n])
              (*result_sizes)[n].size_validity_ +=
                  tile_size / constants::cell_var_offset_size *
                  constants::cell_validity_size;
          }
        }
      }
    }

    // Parse individual tiles
    for (const auto& t : overlap->tiles_) {
      auto tid = t.first;
      auto ratio = t.second;
      for (size_t n = 0; n < name.size(); ++n) {
        // Zipped coords applicable only in homogeneous domains
        if (name[n] == constants::coords && !all_dims_same_type)
          continue;

        // If this attribute does not exist, skip it as this is likely a new
        // attribute added as a result of schema evolution
        if (!meta->array_schema()->is_field(name[n])) {
          continue;
        }

        frag_tiles->insert(std::pair<unsigned, uint64_t>(f, tid));
        auto tile_size = meta->tile_size(name[n], tid);
        auto attr_datatype_size = datatype_size(array_schema.type(name[n]));
        if (!var_sizes[n]) {
          (*result_sizes)[n].size_fixed_ += tile_size * ratio;
          if (nullable[n])
            (*result_sizes)[n].size_validity_ +=
                (tile_size / attr_datatype_size *
                 constants::cell_validity_size) *
                ratio;

        } else {
          tile_size -= constants::cell_var_offset_size;
          (*result_sizes)[n].size_fixed_ += tile_size * ratio;
          auto tile_var_size =
              meta->loaded_metadata()->tile_var_size(name[n], tid);
          (*result_sizes)[n].size_var_ += tile_var_size * ratio;
          if (nullable[n])
            (*result_sizes)[n].size_validity_ +=
                (tile_size / constants::cell_var_offset_size *
                 constants::cell_validity_size) *
                ratio;
        }
      }
    }
  }

  // Calibrate result - applicable only to arrays without coordinate duplicates
  // and fixed dimensions
  if (!array_schema.allows_dups() && all_dims_fixed) {
    // Calculate cell num
    uint64_t cell_num = 1;
    auto dim_num = array_schema.dim_num();
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim{array_schema.dimension_ptr(d)};
      cell_num = utils::math::safe_mul(
          cell_num, dim->domain_range(range_subset_[d][range_coords[d]]));
    }

    uint64_t max_size_fixed = UINT64_MAX;
    uint64_t max_size_var = UINT64_MAX;
    uint64_t max_size_validity = UINT64_MAX;
    for (size_t n = 0; n < name.size(); ++n) {
      // Zipped coords applicable only in homogeneous domains
      if (name[n] == constants::coords && !all_dims_same_type)
        continue;

      if (var_sizes[n]) {
        max_size_fixed =
            utils::math::safe_mul(cell_num, constants::cell_var_offset_size);
      } else {
        max_size_fixed =
            utils::math::safe_mul(cell_num, array_schema.cell_size(name[n]));
      }
      if (nullable[n])
        max_size_validity =
            utils::math::safe_mul(cell_num, constants::cell_validity_size);

      (*result_sizes)[n].size_fixed_ =
          std::min<double>((*result_sizes)[n].size_fixed_, max_size_fixed);
      (*result_sizes)[n].size_var_ =
          std::min<double>((*result_sizes)[n].size_var_, max_size_var);
      (*result_sizes)[n].size_validity_ = std::min<double>(
          (*result_sizes)[n].size_validity_, max_size_validity);
    }
  }
}

template <class T>
void Subarray::compute_tile_coords_col() {
  std::vector<std::set<T>> coords_set;
  const auto& array_schema = array_->array_schema_latest();
  auto domain = array_schema.domain().domain();
  auto dim_num = this->dim_num();
  uint64_t tile_start, tile_end;

  // Compute unique tile coords per dimension
  coords_set.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto tile_extent = *(const T*)array_schema.domain().tile_extent(d).data();
    for (uint64_t j = 0; j < range_subset_[d].num_ranges(); ++j) {
      auto dim_dom = (const T*)domain[d].data();
      auto r = (const T*)range_subset_[d][j].data();
      tile_start = Dimension::tile_idx(r[0], dim_dom[0], tile_extent);
      tile_end = Dimension::tile_idx(r[1], dim_dom[0], tile_extent);
      for (uint64_t t = tile_start; t <= tile_end; ++t)
        coords_set[d].insert(t);
    }
  }

  // Compute `tile_coords_`
  std::vector<typename std::set<T>::iterator> iters;
  size_t tile_coords_num = 1;
  for (unsigned d = 0; d < dim_num; ++d) {
    iters.push_back(coords_set[d].begin());
    tile_coords_num *= coords_set[d].size();
  }

  tile_coords_.resize(tile_coords_num);
  std::vector<uint8_t> coords;
  auto coords_size{dim_num * array_schema.dimension_ptr(0)->coord_size()};
  coords.resize(coords_size);
  size_t coord_size = sizeof(T);
  size_t tile_coords_pos = 0;
  while (iters[dim_num - 1] != coords_set[dim_num - 1].end()) {
    for (unsigned d = 0; d < dim_num; ++d)
      std::memcpy(&(coords[d * coord_size]), &(*iters[d]), coord_size);
    tile_coords_[tile_coords_pos++] = coords;

    // Advance the iterators
    unsigned d = 0;
    while (d < dim_num) {
      iters[d]++;
      if (iters[d] != coords_set[d].end())
        break;
      if (d < dim_num - 1)
        iters[d] = coords_set[d].begin();
      d++;
    }
  }

  // Compute `tile_coords_map_`
  for (size_t i = 0; i < tile_coords_.size(); ++i, ++tile_coords_pos)
    tile_coords_map_[tile_coords_[i]] = i;
}

template <class T>
void Subarray::compute_tile_coords_row() {
  std::vector<std::set<T>> coords_set;
  const auto& array_schema = array_->array_schema_latest();
  auto domain = array_schema.domain().domain();
  auto dim_num = this->dim_num();
  uint64_t tile_start, tile_end;

  // Compute unique tile coords per dimension
  coords_set.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto tile_extent = *(const T*)array_schema.domain().tile_extent(d).data();
    auto dim_dom = (const T*)domain[d].data();
    for (uint64_t j = 0; j < range_subset_[d].num_ranges(); ++j) {
      auto r = (const T*)range_subset_[d][j].data();
      tile_start = Dimension::tile_idx(r[0], dim_dom[0], tile_extent);
      tile_end = Dimension::tile_idx(r[1], dim_dom[0], tile_extent);
      for (uint64_t t = tile_start; t <= tile_end; ++t)
        coords_set[d].insert(t);
    }
  }

  // Compute `tile_coords_`
  std::vector<typename std::set<T>::iterator> iters;
  size_t tile_coords_num = 1;
  for (unsigned d = 0; d < dim_num; ++d) {
    iters.push_back(coords_set[d].begin());
    tile_coords_num *= coords_set[d].size();
  }

  tile_coords_.resize(tile_coords_num);
  std::vector<uint8_t> coords;
  auto coords_size{dim_num * array_schema.dimension_ptr(0)->coord_size()};
  coords.resize(coords_size);
  size_t coord_size = sizeof(T);
  size_t tile_coords_pos = 0;
  while (iters[0] != coords_set[0].end()) {
    for (unsigned d = 0; d < dim_num; ++d)
      std::memcpy(&(coords[d * coord_size]), &(*iters[d]), coord_size);
    tile_coords_[tile_coords_pos++] = coords;

    // Advance the iterators
    auto d = (int)dim_num - 1;
    while (d >= 0) {
      iters[d]++;
      if (iters[d] != coords_set[d].end())
        break;
      if (d > 0)
        iters[d] = coords_set[d].begin();
      d--;
    }
  }

  // Compute `tile_coords_map_`
  for (size_t i = 0; i < tile_coords_.size(); ++i, ++tile_coords_pos)
    tile_coords_map_[tile_coords_[i]] = i;
}

void Subarray::precompute_tile_overlap(
    uint64_t start_range_idx,
    uint64_t end_range_idx,
    const Config* config,
    ThreadPool* compute_tp,
    bool override_memory_constraint) {
  auto timer_se = stats_->start_timer("read_compute_tile_overlap");

  // If the `tile_overlap_` has already been precomputed and contains
  // the given range, re-use it with new range.
  const bool tile_overlap_computed =
      tile_overlap_.contains_range(start_range_idx, end_range_idx);
  if (tile_overlap_computed) {
    stats_->add_counter("precompute_tile_overlap.tile_overlap_cache_hit", 1);
    tile_overlap_.update_range(start_range_idx, end_range_idx);
    return;
  }

  stats_->add_counter(
      "precompute_tile_overlap.ranges_requested",
      end_range_idx - start_range_idx + 1);

  compute_range_offsets();

  auto meta = array_->fragment_metadata();
  auto fragment_num = meta.size();

  // Lookup the target maximum tile overlap size.
  bool found = false;
  uint64_t max_tile_overlap_size = 0;
  throw_if_not_ok(config->get<uint64_t>(
      "sm.max_tile_overlap_size", &max_tile_overlap_size, &found));
  assert(found);

  uint64_t tile_overlap_start = start_range_idx;
  uint64_t tile_overlap_end = end_range_idx;

  // Currently, we allow the caller to override the memory constraint
  // imposed by `constants::max_tile_overlap_size`. This is temporary
  // until we refactor for all callers to become aware that `tile_overlap_`
  // may be truncated.
  uint64_t tmp_tile_overlap_end = tile_overlap_start;
  if (override_memory_constraint || fragment_num == 0) {
    tmp_tile_overlap_end = tile_overlap_end;
  }

  // Incrementally compute the tile overlap until either:
  //   1). Tile overlap has been computed for all of the requested ranges.
  //   2). The size of the current tile overlap has exceed our budget.
  //
  // Each loop is expensive, so we will double the range to compute for
  // each successive loop. The intent is to minimize the number of loops
  // at the risk of exceeding our target maximum memory usage for the
  // tile overlap data.
  RelevantFragmentGenerator relevant_fragment_generator(array_, *this, stats_);
  ComputeRelevantTileOverlapCtx tile_overlap_ctx;
  SubarrayTileOverlap tile_overlap(
      fragment_num, tile_overlap_start, tmp_tile_overlap_end);
  do {
    if (relevant_fragment_generator.update_range_coords(&tile_overlap)) {
      relevant_fragments_ =
          relevant_fragment_generator.compute_relevant_fragments(compute_tp);
    }
    load_relevant_fragment_rtrees(compute_tp);
    compute_relevant_fragment_tile_overlap(
        compute_tp, &tile_overlap, &tile_overlap_ctx);

    if (tmp_tile_overlap_end == tile_overlap_end ||
        tile_overlap.byte_size() >= max_tile_overlap_size) {
      tile_overlap_ = std::move(tile_overlap);
      break;
    }

    // Double the range for the next loop.
    tmp_tile_overlap_end = std::min<uint64_t>(
        tile_overlap_end, tmp_tile_overlap_end + tile_overlap.range_num());
    tile_overlap.expand(tmp_tile_overlap_end);
  } while (true);

  stats_->add_counter("precompute_tile_overlap.fragment_num", fragment_num);
  stats_->add_counter(
      "precompute_tile_overlap.relevant_fragment_num",
      relevant_fragments_.size());
  stats_->add_counter(
      "precompute_tile_overlap.tile_overlap_byte_size",
      tile_overlap_.byte_size());
  stats_->add_counter(
      "precompute_tile_overlap.ranges_computed",
      tile_overlap_.range_idx_end() - tile_overlap_.range_idx_start() + 1);
}

void Subarray::precompute_all_ranges_tile_overlap(
    ThreadPool* compute_tp,
    const std::vector<FragIdx>& frag_tile_idx,
    ITileRange* tile_ranges) {
  auto timer_se = stats_->start_timer("read_compute_simple_tile_overlap");

  // For easy reference.
  const auto meta = array_->fragment_metadata();
  const auto dim_num = array_->array_schema_latest().dim_num();

  // Get the results ready.
  tile_ranges->clear_tile_ranges();

  compute_range_offsets();

  // Compute relevant fragments and load rtrees.
  ComputeRelevantTileOverlapCtx tile_overlap_ctx;
  RelevantFragmentGenerator relevant_fragment_generator(array_, *this, stats_);
  relevant_fragment_generator.update_range_coords(nullptr);
  relevant_fragments_ =
      relevant_fragment_generator.compute_relevant_fragments(compute_tp);
  load_relevant_fragment_rtrees(compute_tp);

  // Each thread will use one bitmap per dimensions.
  const auto num_threads = compute_tp->concurrency_level();
  BlockingResourcePool<std::vector<std::vector<uint8_t>>>
      all_threads_tile_bitmaps(static_cast<unsigned int>(num_threads));

  // Run all fragments in parallel.
  auto status =
      parallel_for(compute_tp, 0, relevant_fragments_.size(), [&](uint64_t i) {
        const auto f = relevant_fragments_[i];
        auto tile_bitmaps_resource_guard =
            ResourceGuard(all_threads_tile_bitmaps);
        auto tile_bitmaps = tile_bitmaps_resource_guard.get();

        // Make sure all bitmaps have the correct size.
        if (tile_bitmaps.size() == 0) {
          tile_bitmaps.resize(dim_num);
          for (unsigned d = 0; d < dim_num; d++)
            tile_bitmaps[d].resize(meta[f]->tile_num());
        } else {
          uint64_t memset_length =
              std::min((uint64_t)tile_bitmaps[0].size(), meta[f]->tile_num());
          for (unsigned d = 0; d < dim_num; d++) {
            // TODO we might be able to skip the memset if
            // tile_bitmaps.capacity() <= meta[f]->tile_num().
            memset(tile_bitmaps[d].data(), 0, memset_length * sizeof(uint8_t));
            tile_bitmaps[d].resize(meta[f]->tile_num());
          }
        }

        for (unsigned d = 0; d < dim_num; d++) {
          if (is_default_[d]) {
            continue;
          }

          // Run all ranges in parallel.
          const uint64_t range_num = range_subset_[d].num_ranges();

          // Compute tile bitmaps for this fragment.
          const auto ranges_per_thread =
              (uint64_t)std::ceil((double)range_num / num_threads);
          const auto status_ranges =
              parallel_for(compute_tp, 0, num_threads, [&](uint64_t t) {
                const auto r_start = t * ranges_per_thread;
                const auto r_end =
                    std::min((t + 1) * ranges_per_thread - 1, range_num - 1);
                for (uint64_t r = r_start; r <= r_end; ++r) {
                  meta[f]->loaded_metadata()->compute_tile_bitmap(
                      range_subset_[d][r], d, &tile_bitmaps[d]);
                }
                return Status::Ok();
              });
          RETURN_NOT_OK(status_ranges);
        }

        // Go through the bitmaps in reverse, whenever there is a "hole" in tile
        // contiguity, push a new result tile range.
        uint64_t end = tile_bitmaps[0].size() - 1;
        uint64_t length = 0;
        int64_t min = static_cast<int64_t>(frag_tile_idx[f].tile_idx_);
        for (int64_t t = tile_bitmaps[0].size() - 1; t >= min; t--) {
          bool comb = true;
          for (unsigned d = 0; d < dim_num; d++) {
            comb &= is_default_[d] || (bool)tile_bitmaps[d][t];
          }

          if (!comb) {
            if (length != 0) {
              tile_ranges->add_tile_range(f, end + 1 - length, end);
              length = 0;
            }

            end = t - 1;
          } else {
            length++;
          }
        }

        // Push the last result tile range.
        if (length != 0) {
          tile_ranges->add_tile_range(f, end + 1 - length, end);
        }

        return Status::Ok();
      });
  throw_if_not_ok(status);
  tile_ranges->done_adding_tile_ranges();
}

Subarray Subarray::clone() const {
  Subarray clone;
  clone.stats_ = stats_;
  clone.logger_ = logger_;
  clone.array_ = array_;
  clone.layout_ = layout_;
  clone.cell_order_ = cell_order_;
  clone.range_subset_ = range_subset_;
  clone.is_default_ = is_default_;
  clone.range_offsets_ = range_offsets_;
  clone.label_range_subset_ = label_range_subset_;
  clone.attr_range_subset_ = attr_range_subset_;
  clone.tile_overlap_ = tile_overlap_;
  clone.est_result_size_computed_ = est_result_size_computed_;
  clone.relevant_fragments_ = relevant_fragments_;
  clone.coalesce_ranges_ = coalesce_ranges_;
  clone.est_result_size_ = est_result_size_;
  clone.max_mem_size_ = max_mem_size_;
  clone.original_range_idx_ = original_range_idx_;
  clone.merge_overlapping_ranges_ = merge_overlapping_ranges_;

  return clone;
}

TileOverlap Subarray::compute_tile_overlap(
    uint64_t range_idx, unsigned fid) const {
  assert(array_->array_schema_latest().dense());
  auto type{array_->array_schema_latest().dimension_ptr(0)->type()};

  auto g = [&](auto T) {
    if constexpr (std::is_same_v<decltype(T), char>) {
      assert(false);
      return TileOverlap();
    } else {
      return compute_tile_overlap<decltype(T)>(range_idx, fid);
    }
  };
  return apply_with_type(g, type);
}

template <class T>
TileOverlap Subarray::compute_tile_overlap(
    uint64_t range_idx, unsigned fid) const {
  assert(array_->array_schema_latest().dense());
  TileOverlap ret;
  auto ndrange = this->ndrange(range_idx);

  // Prepare a range copy
  auto dim_num = array_->array_schema_latest().dim_num();
  std::vector<T> range_cpy;
  range_cpy.resize(2 * dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto r = (const T*)ndrange[d].data();
    range_cpy[2 * d] = r[0];
    range_cpy[2 * d + 1] = r[1];
  }

  // Get tile overlap from fragment
  auto meta = array_->fragment_metadata()[fid];
  auto frag_overlap = meta->compute_overlapping_tile_ids_cov<T>(&range_cpy[0]);

  // Prepare ret. Contiguous tile ids with full overlap
  // will be grouped together
  uint64_t start_tid = UINT64_MAX;  // Indicates no new range has started
  uint64_t end_tid = UINT64_MAX;    // Indicates no new range has started
  for (auto o : frag_overlap) {
    // Partial overlap
    if (o.second != 1.0) {
      // Add previous range (if started) and reset
      if (start_tid != UINT64_MAX) {
        if (start_tid != end_tid)
          ret.tile_ranges_.emplace_back(start_tid, end_tid);
        else
          ret.tiles_.emplace_back(start_tid, 1.0);
        start_tid = UINT64_MAX;
        end_tid = UINT64_MAX;
      }
      // Add this tile overlap
      ret.tiles_.push_back(o);
    } else {  // Full overlap
      // New range starting
      if (start_tid == UINT64_MAX) {
        start_tid = o.first;
        end_tid = o.first;
      } else {
        if (o.first == end_tid + 1) {  // Group into previous range
          end_tid++;
        } else {
          // Add previous range
          if (start_tid != end_tid)
            ret.tile_ranges_.emplace_back(start_tid, end_tid);
          else
            ret.tiles_.emplace_back(start_tid, 1.0);
          // New range starting
          start_tid = o.first;
          end_tid = o.first;
        }
      }
    }
  }

  // Potentially add last tile range
  if (start_tid != UINT64_MAX) {
    if (start_tid != end_tid)
      ret.tile_ranges_.emplace_back(start_tid, end_tid);
    else
      ret.tiles_.emplace_back(start_tid, 1.0);
  }

  return ret;
}

void Subarray::swap(Subarray& subarray) {
  std::swap(stats_, subarray.stats_);
  std::swap(logger_, subarray.logger_);
  std::swap(array_, subarray.array_);
  std::swap(layout_, subarray.layout_);
  std::swap(cell_order_, subarray.cell_order_);
  std::swap(range_subset_, subarray.range_subset_);
  std::swap(label_range_subset_, subarray.label_range_subset_);
  std::swap(attr_range_subset_, subarray.attr_range_subset_);
  std::swap(is_default_, subarray.is_default_);
  std::swap(range_offsets_, subarray.range_offsets_);
  std::swap(tile_overlap_, subarray.tile_overlap_);
  std::swap(est_result_size_computed_, subarray.est_result_size_computed_);
  std::swap(relevant_fragments_, subarray.relevant_fragments_);
  std::swap(coalesce_ranges_, subarray.coalesce_ranges_);
  std::swap(est_result_size_, subarray.est_result_size_);
  std::swap(max_mem_size_, subarray.max_mem_size_);
  std::swap(original_range_idx_, subarray.original_range_idx_);
  std::swap(merge_overlapping_ranges_, subarray.merge_overlapping_ranges_);
}

void Subarray::get_expanded_coordinates(
    const uint64_t range_idx_start,
    const uint64_t range_idx_end,
    std::vector<uint64_t>* const start_coords,
    std::vector<uint64_t>* const end_coords) const {
  // Fetch the multi-dimensional coordinates from the
  // flattened (total order) range indexes.
  *start_coords = get_range_coords(range_idx_start);
  *end_coords = get_range_coords(range_idx_end);

  // This is only applicable to row-major, column-major, or unordered
  // layouts. We will treat unordered layouts as the cell layout.
  const Layout coords_layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;
  if (coords_layout == Layout::GLOBAL_ORDER ||
      coords_layout == Layout::HILBERT) {
    assert(*start_coords == *end_coords);
    return;
  }

  assert(
      coords_layout == Layout::ROW_MAJOR || coords_layout == Layout::COL_MAJOR);

  const uint32_t dim_num = array_->array_schema_latest().dim_num();

  // Locate the first dimension where the start/end coordinates deviate.
  int64_t deviation_d;
  if (coords_layout == Layout::ROW_MAJOR) {
    deviation_d = 0;
    while (deviation_d < dim_num - 1) {
      if ((*start_coords)[deviation_d] != (*end_coords)[deviation_d])
        break;
      ++deviation_d;
    }
  } else {
    assert(coords_layout == Layout::COL_MAJOR);
    deviation_d = dim_num - 1;
    while (deviation_d > 0) {
      if ((*start_coords)[deviation_d] != (*end_coords)[deviation_d])
        break;
      --deviation_d;
    }
  }

  // Calculate the first dimension to start the expansion. This is the
  // the dimension that immediately follows the dimension where the
  // coordinates deviate.
  int64_t expand_d;
  if (coords_layout == Layout::ROW_MAJOR) {
    expand_d = deviation_d + 1;
  } else {
    assert(coords_layout == Layout::COL_MAJOR);
    expand_d = deviation_d - 1;
  }

  // Expand each dimension at-and-after the expansion dimension so that
  // the coordinates align with the first and last ranges.
  if (coords_layout == Layout::ROW_MAJOR) {
    for (int64_t d = expand_d; d < dim_num; ++d) {
      (*start_coords)[d] = 0;
      (*end_coords)[d] = range_subset_[d].num_ranges() - 1;
    }
  } else {
    assert(coords_layout == Layout::COL_MAJOR);
    for (int64_t d = expand_d; d >= 0; --d) {
      (*start_coords)[d] = 0;
      (*end_coords)[d] = range_subset_[d].num_ranges() - 1;
    }
  }
}

void Subarray::load_relevant_fragment_rtrees(ThreadPool* compute_tp) const {
  auto timer_se = stats_->start_timer("read_load_relevant_rtrees");

  auto meta = array_->fragment_metadata();
  auto encryption_key = array_->encryption_key();

  auto status =
      parallel_for(compute_tp, 0, relevant_fragments_.size(), [&](uint64_t f) {
        meta[relevant_fragments_[f]]->loaded_metadata()->load_rtree(
            *encryption_key);
        return Status::Ok();
      });
  throw_if_not_ok(status);
}

void Subarray::compute_relevant_fragment_tile_overlap(
    ThreadPool* compute_tp,
    SubarrayTileOverlap* tile_overlap,
    ComputeRelevantTileOverlapCtx* fn_ctx) {
  auto timer_se = stats_->start_timer("read_compute_relevant_tile_overlap");

  const auto range_num = tile_overlap->range_num();
  fn_ctx->range_idx_offset_ = fn_ctx->range_idx_offset_ + fn_ctx->range_len_;
  fn_ctx->range_len_ = range_num - fn_ctx->range_idx_offset_;

  const auto& meta = array_->fragment_metadata();

  const auto num_threads = compute_tp->concurrency_level();
  const auto ranges_per_thread =
      (uint64_t)std::ceil((double)fn_ctx->range_len_ / num_threads);

  auto status = parallel_for_2d(
      compute_tp,
      0,
      relevant_fragments_.size(),
      0,
      num_threads,
      [&](uint64_t i, uint64_t t) {
        const auto frag_idx = relevant_fragments_[i];
        const auto dense = meta[frag_idx]->dense();

        const auto r_start =
            fn_ctx->range_idx_offset_ + (t * ranges_per_thread);
        const auto r_end =
            fn_ctx->range_idx_offset_ +
            std::min((t + 1) * ranges_per_thread - 1, fn_ctx->range_len_ - 1);
        for (uint64_t r = r_start; r <= r_end; ++r) {
          if (dense) {  // Dense fragment
            *tile_overlap->at(frag_idx, r) = compute_tile_overlap(
                r + tile_overlap->range_idx_start(), frag_idx);
          } else {  // Sparse fragment
            const auto& range =
                this->ndrange(r + tile_overlap->range_idx_start());
            meta[frag_idx]->loaded_metadata()->get_tile_overlap(
                range, is_default_, tile_overlap->at(frag_idx, r));
          }
        }

        return Status::Ok();
      });
  throw_if_not_ok(status);
}

void Subarray::load_relevant_fragment_tile_var_sizes(
    const std::vector<std::string>& names, ThreadPool* compute_tp) const {
  const auto& array_schema = array_->array_schema_latest();
  auto encryption_key = array_->encryption_key();
  auto meta = array_->fragment_metadata();

  // Find the names of the var-sized dimensions or attributes
  std::vector<std::string> var_names;
  var_names.reserve(names.size());
  for (unsigned i = 0; i < names.size(); ++i) {
    if (array_schema.var_size(names[i]))
      var_names.emplace_back(names[i]);
  }

  // No var-sized attributes/dimensions in `names`
  if (var_names.empty()) {
    return;
  }

  // Load all metadata for tile var sizes among fragments.
  for (const auto& var_name : var_names) {
    const auto status = parallel_for(
        compute_tp, 0, relevant_fragments_.size(), [&](const size_t i) {
          auto f = relevant_fragments_[i];
          // Gracefully skip loading tile sizes for attributes added in schema
          // evolution that do not exists in this fragment
          const auto& schema = meta[f]->array_schema();
          if (!schema->is_field(var_name)) {
            return Status::Ok();
          }
          meta[f]->loaded_metadata()->load_tile_var_sizes(
              *encryption_key, var_name);
          return Status::Ok();
        });
    throw_if_not_ok(status);
  }
}

const RelevantFragments& Subarray::relevant_fragments() const {
  return relevant_fragments_;
}

RelevantFragments& Subarray::relevant_fragments() {
  return relevant_fragments_;
}

const stats::Stats& Subarray::stats() const {
  return *stats_;
}

void Subarray::set_stats(const stats::StatsData& data) {
  stats_->populate_with_data(data);
}

bool Subarray::non_overlapping_ranges_for_dim(const uint64_t dim_idx) {
  const auto& ranges = range_subset_[dim_idx].ranges();
  auto dim{array_->array_schema_latest().dimension_ptr(dim_idx)};

  if (ranges.size() > 1) {
    for (uint64_t r = 0; r < ranges.size() - 1; r++) {
      if (dim->overlap(ranges[r], ranges[r + 1]))
        return false;
    }
  }
  return true;
}

template <class T, class SubarrayT>
void Subarray::crop_to_tile_impl(const T* tile_coords, SubarrayT& ret) const {
  T new_range[2];
  bool overlaps;

  // Get tile subarray based on the input coordinates
  const auto& array_schema = array_->array_schema_latest();
  std::vector<T> tile_subarray(2 * dim_num());
  array_schema.domain().get_tile_subarray(tile_coords, &tile_subarray[0]);

  // Compute cropped subarray
  for (unsigned d = 0; d < dim_num(); ++d) {
    auto r_size{2 * array_schema.dimension_ptr(d)->coord_size()};
    uint64_t i = 0;
    for (size_t r = 0; r < range_subset_[d].num_ranges(); ++r) {
      const auto& range = range_subset_[d][r];
      rectangle::overlap(
          (const T*)range.data(),
          &tile_subarray[2 * d],
          1,
          new_range,
          &overlaps);

      if (overlaps) {
        ret.add_range_unsafe(d, Range(new_range, r_size));
        ret.original_range_idx_unsafe().resize(dim_num());
        ret.original_range_idx_unsafe()[d].resize(i + 1);
        ret.original_range_idx_unsafe()[d][i++] = r;
      }
    }
  }
}

// Explicit instantiations
template void Subarray::compute_tile_coords<int8_t>();
template void Subarray::compute_tile_coords<uint8_t>();
template void Subarray::compute_tile_coords<int16_t>();
template void Subarray::compute_tile_coords<uint16_t>();
template void Subarray::compute_tile_coords<int32_t>();
template void Subarray::compute_tile_coords<uint32_t>();
template void Subarray::compute_tile_coords<int64_t>();
template void Subarray::compute_tile_coords<uint64_t>();
template void Subarray::compute_tile_coords<float>();
template void Subarray::compute_tile_coords<double>();

template const int8_t* Subarray::tile_coords_ptr<int8_t>(
    const std::vector<int8_t>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const uint8_t* Subarray::tile_coords_ptr<uint8_t>(
    const std::vector<uint8_t>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const int16_t* Subarray::tile_coords_ptr<int16_t>(
    const std::vector<int16_t>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const uint16_t* Subarray::tile_coords_ptr<uint16_t>(
    const std::vector<uint16_t>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const int32_t* Subarray::tile_coords_ptr<int32_t>(
    const std::vector<int32_t>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const uint32_t* Subarray::tile_coords_ptr<uint32_t>(
    const std::vector<uint32_t>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const int64_t* Subarray::tile_coords_ptr<int64_t>(
    const std::vector<int64_t>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const uint64_t* Subarray::tile_coords_ptr<uint64_t>(
    const std::vector<uint64_t>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const float* Subarray::tile_coords_ptr<float>(
    const std::vector<float>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;
template const double* Subarray::tile_coords_ptr<double>(
    const std::vector<double>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const;

template Subarray Subarray::crop_to_tile<int8_t>(
    const int8_t* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<uint8_t>(
    const uint8_t* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<int16_t>(
    const int16_t* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<uint16_t>(
    const uint16_t* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<int32_t>(
    const int32_t* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<uint32_t>(
    const uint32_t* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<int64_t>(
    const int64_t* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<uint64_t>(
    const uint64_t* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<float>(
    const float* tile_coords, Layout layout) const;
template Subarray Subarray::crop_to_tile<double>(
    const double* tile_coords, Layout layout) const;

template void Subarray::crop_to_tile<int8_t>(
    DenseTileSubarray<int8_t>& ret, const int8_t* tile_coords) const;
template void Subarray::crop_to_tile<uint8_t>(
    DenseTileSubarray<uint8_t>& ret, const uint8_t* tile_coords) const;
template void Subarray::crop_to_tile<int16_t>(
    DenseTileSubarray<int16_t>& ret, const int16_t* tile_coords) const;
template void Subarray::crop_to_tile<uint16_t>(
    DenseTileSubarray<uint16_t>& ret, const uint16_t* tile_coords) const;
template void Subarray::crop_to_tile<int32_t>(
    DenseTileSubarray<int32_t>& ret, const int32_t* tile_coords) const;
template void Subarray::crop_to_tile<uint32_t>(
    DenseTileSubarray<uint32_t>& ret, const uint32_t* tile_coords) const;
template void Subarray::crop_to_tile<int64_t>(
    DenseTileSubarray<int64_t>& ret, const int64_t* tile_coords) const;
template void Subarray::crop_to_tile<uint64_t>(
    DenseTileSubarray<uint64_t>& ret, const uint64_t* tile_coords) const;

template uint64_t Subarray::tile_cell_num<int8_t>(
    const int8_t* tile_coords) const;
template uint64_t Subarray::tile_cell_num<uint8_t>(
    const uint8_t* tile_coords) const;
template uint64_t Subarray::tile_cell_num<int16_t>(
    const int16_t* tile_coords) const;
template uint64_t Subarray::tile_cell_num<uint16_t>(
    const uint16_t* tile_coords) const;
template uint64_t Subarray::tile_cell_num<int32_t>(
    const int32_t* tile_coords) const;
template uint64_t Subarray::tile_cell_num<uint32_t>(
    const uint32_t* tile_coords) const;
template uint64_t Subarray::tile_cell_num<int64_t>(
    const int64_t* tile_coords) const;
template uint64_t Subarray::tile_cell_num<uint64_t>(
    const uint64_t* tile_coords) const;

/* ********************************* */
/*         LABEL RANGE SUBSET        */
/* ********************************* */

Subarray::LabelRangeSubset::LabelRangeSubset(
    const DimensionLabel& ref, bool coalesce_ranges)
    : name_{ref.name()}
    , ranges_{RangeSetAndSuperset(
          ref.label_type(), Range(), false, coalesce_ranges)} {
}

Subarray::LabelRangeSubset::LabelRangeSubset(
    const std::string& name, Datatype type, bool coalesce_ranges)
    : name_{name}
    , ranges_{RangeSetAndSuperset(type, Range(), false, coalesce_ranges)} {
}

}  // namespace tiledb::sm
