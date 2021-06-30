/**
 * @file   subarray.cc
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
 * This file implements class Subarray.
 */

#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/rtree/rtree.h"
#include "tiledb/sm/stats/global_stats.h"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <unordered_set>

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Subarray::Subarray()
    : array_(nullptr)
    , layout_(Layout::UNORDERED)
    , cell_order_(Layout::ROW_MAJOR)
    , est_result_size_computed_(false)
    , coalesce_ranges_(true) {
}

Subarray::Subarray(
    const Array* array, Stats* const parent_stats, const bool coalesce_ranges)
    : Subarray(array, Layout::UNORDERED, parent_stats, coalesce_ranges) {
}

Subarray::Subarray(
    const Array* const array,
    const Layout layout,
    Stats* const parent_stats,
    const bool coalesce_ranges)
    : stats_(parent_stats->create_child("Subarray"))
    , array_(array)
    , layout_(layout)
    , cell_order_(array_->array_schema()->cell_order())
    , est_result_size_computed_(false)
    , coalesce_ranges_(coalesce_ranges) {
  add_default_ranges();
  set_add_or_coalesce_range_func();
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

Status Subarray::add_range(
    uint32_t dim_idx, Range&& range, const bool read_range_oob_error) {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot add range to dimension; Invalid dimension index"));

  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_.clear();

  // Remove the default range
  if (is_default_[dim_idx]) {
    ranges_[dim_idx].clear();
    is_default_[dim_idx] = false;
  }

  // Correctness checks
  auto dim = array_->array_schema()->dimension(dim_idx);
  if (!read_range_oob_error)
    RETURN_NOT_OK(dim->adjust_range_oob(&range));
  RETURN_NOT_OK(dim->check_range(range));

  // Add the range
  add_or_coalesce_range_func_[dim_idx](this, dim_idx, range);

  return Status::Ok();
}

Status Subarray::add_range_unsafe(uint32_t dim_idx, const Range& range) {
  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_.clear();

  // Remove the default range
  if (is_default_[dim_idx]) {
    ranges_[dim_idx].clear();
    is_default_[dim_idx] = false;
  }

  // Add the range
  add_or_coalesce_range_func_[dim_idx](this, dim_idx, range);

  return Status::Ok();
}

const Array* Subarray::array() const {
  return array_;
}

uint64_t Subarray::cell_num() const {
  auto array_schema = array_->array_schema();
  unsigned dim_num = array_schema->dim_num();
  uint64_t ret = 1;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema->dimension(d);
    uint64_t num = 0;
    for (const auto& r : ranges_[d])
      num += dim->domain_range(r);
    ret = utils::math::safe_mul(ret, num);
  }

  return ret;
}

uint64_t Subarray::cell_num(uint64_t range_idx) const {
  uint64_t cell_num = 1, range;
  auto array_schema = array_->array_schema();
  unsigned dim_num = array_schema->dim_num();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order_ : layout_;
  uint64_t tmp_idx = range_idx;

  // Unary case or GLOBAL_ORDER
  if (range_num() == 1) {
    for (unsigned d = 0; d < dim_num; ++d) {
      range = array_schema->dimension(d)->domain_range(ranges_[d][0]);
      if (range == std::numeric_limits<uint64_t>::max())  // Overflow
        return range;

      cell_num = utils::math::safe_mul(range, cell_num);
      if (cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
        return cell_num;
    }

    return cell_num;
  }

  // Non-unary case (range_offsets_ must be computed)
  if (layout == Layout::ROW_MAJOR) {
    assert(!range_offsets_.empty());
    for (unsigned d = 0; d < dim_num; ++d) {
      range = array_schema->dimension(d)->domain_range(
          ranges_[d][tmp_idx / range_offsets_[d]]);
      tmp_idx %= range_offsets_[d];
      if (range == std::numeric_limits<uint64_t>::max())  // Overflow
        return range;

      cell_num = utils::math::safe_mul(range, cell_num);
      if (cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
        return cell_num;
    }
  } else if (layout == Layout::COL_MAJOR) {
    assert(!range_offsets_.empty());
    for (unsigned d = dim_num - 1;; --d) {
      range = array_schema->dimension(d)->domain_range(
          ranges_[d][tmp_idx / range_offsets_[d]]);
      tmp_idx %= range_offsets_[d];
      if (range == std::numeric_limits<uint64_t>::max())  // Overflow
        return range;

      cell_num = utils::math::safe_mul(range, cell_num);
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
  auto array_schema = array_->array_schema();
  auto dim_num = array_->array_schema()->dim_num();
  assert(dim_num == range_coords.size());

  uint64_t ret = 1;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema->dimension(d);
    ret = utils::math::safe_mul(
        ret, dim->domain_range(ranges_[d][range_coords[d]]));
    if (ret == std::numeric_limits<uint64_t>::max())  // Overflow
      return ret;
  }

  return ret;
}

void Subarray::clear() {
  ranges_.clear();
  range_offsets_.clear();
  est_result_size_computed_ = false;
  tile_overlap_.clear();
  add_or_coalesce_range_func_.clear();
}

bool Subarray::coincides_with_tiles() const {
  if (range_num() != 1)
    return false;

  auto dim_num = array_->array_schema()->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_->array_schema()->dimension(d);
    if (!dim->coincides_with_tiles(ranges_[d][0]))
      return false;
  }

  return true;
}

template <class T>
Subarray Subarray::crop_to_tile(const T* tile_coords, Layout layout) const {
  Subarray ret(array_, layout, stats_->parent(), coalesce_ranges_);

  T new_range[2];
  bool overlaps;

  // Get tile subarray based on the input coordinates
  auto array_schema = array_->array_schema();
  std::vector<T> tile_subarray(2 * dim_num());
  array_schema->domain()->get_tile_subarray(tile_coords, &tile_subarray[0]);

  // Compute cropped subarray
  for (unsigned d = 0; d < dim_num(); ++d) {
    auto r_size = 2 * array_schema->dimension(d)->coord_size();
    for (size_t r = 0; r < ranges_[d].size(); ++r) {
      const auto& range = ranges_[d][r];
      utils::geometry::overlap(
          (const T*)range.data(),
          &tile_subarray[2 * d],
          1,
          new_range,
          &overlaps);

      if (overlaps)
        ret.add_range_unsafe(d, Range(new_range, r_size));
    }
  }

  return ret;
}

uint32_t Subarray::dim_num() const {
  return array_->array_schema()->dim_num();
}

NDRange Subarray::domain() const {
  return array_->array_schema()->domain()->domain();
}

bool Subarray::empty() const {
  return range_num() == 0;
}

Status Subarray::get_query_type(QueryType* type) const {
  if (array_ == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get query type from array; Invalid array"));

  return array_->get_query_type(type);
}

Status Subarray::get_range(
    uint32_t dim_idx, uint64_t range_idx, const Range** range) const {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid dimension index"));

  auto range_num = ranges_[dim_idx].size();
  if (range_idx >= range_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid range index"));

  *range = &ranges_[dim_idx][range_idx];

  return Status::Ok();
}

Status Subarray::get_range(
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end) const {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid dimension index"));

  auto range_num = ranges_[dim_idx].size();
  if (range_idx >= range_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid range index"));

  *start = ranges_[dim_idx][range_idx].start();
  *end = ranges_[dim_idx][range_idx].end();

  return Status::Ok();
}

Status Subarray::get_range_var_size(
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start,
    uint64_t* end) const {
  auto schema = array_->array_schema();
  auto dim_num = schema->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get var range size; Invalid dimension index"));

  auto dim = schema->domain()->dimension(dim_idx);
  if (!dim->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get var range size; Dimension " + dim->name() +
        " is not var sized"));

  auto range_num = ranges_[dim_idx].size();
  if (range_idx >= range_num)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get var range size; Invalid range index"));

  *start = ranges_[dim_idx][range_idx].start_size();
  *end = ranges_[dim_idx][range_idx].end_size();

  return Status::Ok();
}

Status Subarray::get_range_num(uint32_t dim_idx, uint64_t* range_num) const {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get number of ranges for a dimension; "
                              "Invalid dimension index"));

  *range_num = ranges_[dim_idx].size();

  return Status::Ok();
}

Subarray Subarray::get_subarray(uint64_t start, uint64_t end) const {
  Subarray ret(array_, layout_, stats_->parent(), coalesce_ranges_);

  auto start_coords = get_range_coords(start);
  auto end_coords = get_range_coords(end);

  auto dim_num = this->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    for (uint64_t r = start_coords[d]; r <= end_coords[d]; ++r) {
      ret.add_range_unsafe(d, ranges_[d][r]);
    }
  }

  ret.tile_overlap_ = tile_overlap_;
  ret.tile_overlap_.update_range(start, end);

  // Compute range offsets
  ret.compute_range_offsets();

  return ret;
}

bool Subarray::is_default(uint32_t dim_index) const {
  return is_default_[dim_index];
}

bool Subarray::is_set() const {
  for (auto d : is_default_)
    if (d == false)
      return true;
  return false;
}

bool Subarray::is_set(unsigned dim_idx) const {
  assert(dim_idx < dim_num());
  return !is_default_[dim_idx];
}

bool Subarray::is_unary() const {
  if (range_num() != 1)
    return false;

  for (const auto& range : ranges_) {
    if (!range[0].unary())
      return false;
  }

  return true;
}

bool Subarray::is_unary(uint64_t range_idx) const {
  auto coords = get_range_coords(range_idx);
  auto dim_num = this->dim_num();

  for (unsigned d = 0; d < dim_num; ++d) {
    if (!ranges_[d][coords[d]].unary())
      return false;
  }

  return true;
}

void Subarray::set_is_default(uint32_t dim_index, bool is_default) {
  is_default_[dim_index] = is_default;
}

void Subarray::set_layout(Layout layout) {
  layout_ = layout;
}

Status Subarray::to_byte_vec(std::vector<uint8_t>* byte_vec) const {
  if (range_num() != 1)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot export to byte vector; The subarray must be unary"));

  byte_vec->clear();

  for (const auto& r : ranges_) {
    auto offset = byte_vec->size();
    byte_vec->resize(offset + r[0].size());
    std::memcpy(&(*byte_vec)[offset], r[0].data(), r[0].size());
  }

  return Status::Ok();
}

Layout Subarray::layout() const {
  return layout_;
}

Status Subarray::get_est_result_size(
    const char* name,
    uint64_t* size,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute/Dimension name cannot be null"));

  // Check size pointer
  if (size == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Input size cannot be null"));

  // Check if name is attribute or dimension
  const auto array_schema = array_->array_schema();
  const bool is_dim = array_schema->is_dim(name);
  const bool is_attr = array_schema->is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return LOG_STATUS(Status::SubarrayError(
        std::string("Cannot get estimated result size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if the attribute/dimension is fixed-sized
  if (array_schema->var_size(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute/Dimension must be fixed-sized"));

  // Check if attribute/dimension is nullable
  if (array_schema->is_nullable(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute/Dimension must not be nullable"));

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size(config, compute_tp));
  *size = static_cast<uint64_t>(ceil(est_result_size_[name].size_fixed_));

  // If the size is non-zero, ensure it is large enough to
  // contain at least one cell.
  const auto cell_size = array_schema->cell_size(name);
  if (*size > 0 && *size < cell_size)
    *size = cell_size;

  return Status::Ok();
}

Status Subarray::get_est_result_size(
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute/Dimension name cannot be null"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Input sizes cannot be null"));

  // Check if name is attribute or dimension
  const auto array_schema = array_->array_schema();
  const bool is_dim = array_schema->is_dim(name);
  const bool is_attr = array_schema->is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return LOG_STATUS(Status::SubarrayError(
        std::string("Cannot get estimated result size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if the attribute/dimension is var-sized
  if (!array_schema->var_size(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute/Dimension must be var-sized"));

  // Check if attribute/dimension is nullable
  if (array_schema->is_nullable(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute/Dimension must not be nullable"));

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size(config, compute_tp));
  *size_off = static_cast<uint64_t>(ceil(est_result_size_[name].size_fixed_));
  *size_val = static_cast<uint64_t>(ceil(est_result_size_[name].size_var_));

  // If the value size is non-zero, ensure both it and the offset size
  // are large enough to contain at least one cell. Otherwise, ensure
  // the offset size is also zero.
  if (*size_val > 0) {
    const auto off_cell_size = constants::cell_var_offset_size;
    if (*size_off < off_cell_size)
      *size_off = off_cell_size;

    const uint64_t val_cell_size = datatype_size(array_schema->type(name));
    if (*size_val < val_cell_size)
      *size_val = val_cell_size;
  } else {
    *size_off = 0;
  }

  return Status::Ok();
}

Status Subarray::get_est_result_size_nullable(
    const char* name,
    uint64_t* size,
    uint64_t* size_validity,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute name cannot be null"));

  // Check size pointer
  if (size == nullptr || size_validity == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Input sizes cannot be null"));

  // Check if name is attribute
  const auto array_schema = array_->array_schema();
  const bool is_attr = array_schema->is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return LOG_STATUS(Status::SubarrayError(
        std::string("Cannot get estimated result size; Attribute '") + name +
        "' does not exist"));

  // Check if the attribute is fixed-sized
  if (array_schema->var_size(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute must be fixed-sized"));

  // Check if attribute is nullable
  if (!array_schema->is_nullable(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute must be nullable"));

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size(config, compute_tp));
  *size = static_cast<uint64_t>(ceil(est_result_size_[name].size_fixed_));
  *size_validity =
      static_cast<uint64_t>(ceil(est_result_size_[name].size_validity_));

  // If the size is non-zero, ensure it is large enough to
  // contain at least one cell.
  const auto cell_size = array_schema->cell_size(name);
  if (*size > 0 && *size < cell_size) {
    *size = cell_size;
    *size_validity = 1;
  }

  return Status::Ok();
}

Status Subarray::get_est_result_size_nullable(
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    uint64_t* size_validity,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute name cannot be null"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr || size_validity == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Input sizes cannot be null"));

  // Check if name is attribute
  const auto array_schema = array_->array_schema();
  const bool is_attr = array_schema->is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return LOG_STATUS(Status::SubarrayError(
        std::string("Cannot get estimated result size; Attribute '") + name +
        "' does not exist"));

  // Check if the attribute is var-sized
  if (!array_schema->var_size(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute must be var-sized"));

  // Check if attribute is nullable
  if (!array_schema->is_nullable(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute must be nullable"));

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size(config, compute_tp));
  *size_off = static_cast<uint64_t>(ceil(est_result_size_[name].size_fixed_));
  *size_val = static_cast<uint64_t>(ceil(est_result_size_[name].size_var_));
  *size_validity =
      static_cast<uint64_t>(ceil(est_result_size_[name].size_validity_));

  // If the value size is non-zero, ensure both it and the offset and
  // validity sizes are large enough to contain at least one cell. Otherwise,
  // ensure the offset and validity sizes are also zero.
  if (*size_val > 0) {
    const uint64_t off_cell_size = constants::cell_var_offset_size;
    if (*size_off < off_cell_size)
      *size_off = off_cell_size;

    const uint64_t val_cell_size = datatype_size(array_schema->type(name));
    if (*size_val < val_cell_size)
      *size_val = val_cell_size;

    const uint64_t validity_cell_size = constants::cell_validity_size;
    if (*size_validity < validity_cell_size)
      *size_validity = validity_cell_size;
  } else {
    *size_off = 0;
    *size_validity = 0;
  }

  return Status::Ok();
}

Status Subarray::get_max_memory_size(
    const char* name,
    uint64_t* size,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute/Dimension cannot be null"));

  // Check size pointer
  if (size == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Input size cannot be null"));

  // Check if name is attribute or dimension
  auto array_schema = array_->array_schema();
  bool is_dim = array_schema->is_dim(name);
  bool is_attr = array_schema->is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return LOG_STATUS(Status::SubarrayError(
        std::string("Cannot get max memory size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if the attribute/dimension is fixed-sized
  if (name != constants::coords && array_schema->var_size(name))
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute/Dimension must be fixed-sized"));

  // Check if attribute/dimension is nullable
  if (array_schema->is_nullable(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute/Dimension must not be nullable"));

  // Compute tile overlap for each fragment
  compute_est_result_size(config, compute_tp);
  *size = max_mem_size_[name].size_fixed_;

  return Status::Ok();
}

Status Subarray::get_max_memory_size(
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute/Dimension cannot be null"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Input sizes cannot be null"));

  // Check if name is attribute or dimension
  auto array_schema = array_->array_schema();
  bool is_dim = array_schema->is_dim(name);
  bool is_attr = array_schema->is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return LOG_STATUS(Status::SubarrayError(
        std::string("Cannot get max memory size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if the attribute/dimension is var-sized
  if (!array_schema->var_size(name))
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute/Dimension must be var-sized"));

  // Check if attribute/dimension is nullable
  if (array_schema->is_nullable(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute/Dimension must not be nullable"));

  // Compute tile overlap for each fragment
  compute_est_result_size(config, compute_tp);
  *size_off = max_mem_size_[name].size_fixed_;
  *size_val = max_mem_size_[name].size_var_;

  return Status::Ok();
}

Status Subarray::get_max_memory_size_nullable(
    const char* name,
    uint64_t* size,
    uint64_t* size_validity,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute name
  if (name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute cannot be null"));

  // Check size pointer
  if (size == nullptr || size_validity == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Input sizes cannot be null"));

  // Check if name is attribute
  auto array_schema = array_->array_schema();
  bool is_attr = array_schema->is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return LOG_STATUS(Status::SubarrayError(
        std::string("Cannot get max memory size; Attribute '") + name +
        "' does not exist"));

  // Check if the attribute is fixed-sized
  if (array_schema->var_size(name))
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute must be fixed-sized"));

  // Check if attribute is nullable
  if (!array_schema->is_nullable(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute must be nullable"));

  // Compute tile overlap for each fragment
  compute_est_result_size(config, compute_tp);
  *size = max_mem_size_[name].size_fixed_;
  *size_validity = max_mem_size_[name].size_validity_;

  return Status::Ok();
}

Status Subarray::get_max_memory_size_nullable(
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    uint64_t* size_validity,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute/Dimension cannot be null"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr || size_validity == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Input sizes cannot be null"));

  // Check if name is attribute or dimension
  auto array_schema = array_->array_schema();
  bool is_attr = array_schema->is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return LOG_STATUS(Status::SubarrayError(
        std::string("Cannot get max memory size; Attribute '") + name +
        "' does not exist"));

  // Check if the attribute is var-sized
  if (!array_schema->var_size(name))
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute/Dimension must be var-sized"));

  // Check if attribute is nullable
  if (!array_schema->is_nullable(name))
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; "
                              "Attribute must be nullable"));

  // Compute tile overlap for each fragment
  compute_est_result_size(config, compute_tp);
  *size_off = max_mem_size_[name].size_fixed_;
  *size_val = max_mem_size_[name].size_var_;
  *size_validity = max_mem_size_[name].size_validity_;

  return Status::Ok();
}

std::vector<uint64_t> Subarray::get_range_coords(uint64_t range_idx) const {
  std::vector<uint64_t> ret;

  uint64_t tmp_idx = range_idx;
  auto dim_num = this->dim_num();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order_ : layout_;

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

void Subarray::get_next_range_coords(
    std::vector<uint64_t>* range_coords) const {
  auto dim_num = array_->array_schema()->dim_num();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order_ : layout_;

  if (layout == Layout::ROW_MAJOR) {
    auto d = dim_num - 1;
    ++(*range_coords)[d];
    while ((*range_coords)[d] >= ranges_[d].size() && d != 0) {
      (*range_coords)[d] = 0;
      --d;
      ++(*range_coords)[d];
    }
  } else if (layout == Layout::COL_MAJOR) {
    auto d = (unsigned)0;
    ++(*range_coords)[d];
    while ((*range_coords)[d] >= ranges_[d].size() && d != dim_num - 1) {
      (*range_coords)[d] = 0;
      ++d;
      ++(*range_coords)[d];
    }
  } else {
    // Global order - noop
  }
}

uint64_t Subarray::range_idx(const std::vector<uint64_t>& range_coords) const {
  uint64_t ret = 0;
  auto dim_num = this->dim_num();
  for (unsigned i = 0; i < dim_num; ++i)
    ret += range_offsets_[i] * range_coords[i];

  return ret;
}

uint64_t Subarray::range_num() const {
  if (ranges_.empty())
    return 0;

  uint64_t ret = 1;
  for (const auto& r : ranges_) {
    ret *= r.size();
  }

  return ret;
}

NDRange Subarray::ndrange(uint64_t range_idx) const {
  NDRange ret;
  uint64_t tmp_idx = range_idx;
  auto dim_num = this->dim_num();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order_ : layout_;
  ret.reserve(dim_num);

  // Unary case or GLOBAL_ORDER
  if (range_idx == 0 && range_num() == 1) {
    for (unsigned d = 0; d < dim_num; ++d)
      ret.emplace_back(ranges_[d][0]);
    return ret;
  }

  // Non-unary case (range_offsets_ must be computed)
  if (layout == Layout::ROW_MAJOR) {
    assert(!range_offsets_.empty());
    for (unsigned d = 0; d < dim_num; ++d) {
      ret.emplace_back(ranges_[d][tmp_idx / range_offsets_[d]]);
      tmp_idx %= range_offsets_[d];
    }
  } else if (layout == Layout::COL_MAJOR) {
    assert(!range_offsets_.empty());
    for (unsigned d = dim_num - 1;; --d) {
      ret.emplace_back(ranges_[d][tmp_idx / range_offsets_[d]]);
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
    ret.emplace_back(ranges_[d][range_coords[d]]);
  return ret;
}

const std::vector<Range>& Subarray::ranges_for_dim(uint32_t dim_idx) const {
  return ranges_[dim_idx];
}

Status Subarray::set_ranges_for_dim(
    uint32_t dim_idx, const std::vector<Range>& ranges) {
  ranges_.resize(dim_idx + 1, std::vector<Range>());

  // Add each range individually so that contiguous
  // ranges may be coalesced.
  ranges_[dim_idx].clear();
  for (const auto& range : ranges)
    add_or_coalesce_range_func_[dim_idx](this, dim_idx, range);

  return Status::Ok();
}

Status Subarray::split(
    unsigned splitting_dim,
    const ByteVecValue& splitting_value,
    Subarray* r1,
    Subarray* r2) const {
  assert(r1 != nullptr);
  assert(r2 != nullptr);
  *r1 = Subarray(array_, layout_, stats_->parent(), coalesce_ranges_);
  *r2 = Subarray(array_, layout_, stats_->parent(), coalesce_ranges_);

  auto dim_num = array_->array_schema()->dim_num();

  Range sr1, sr2;
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& r = ranges_[d][0];
    if (d == splitting_dim) {
      auto dim = array_->array_schema()->dimension(d);
      dim->split_range(r, splitting_value, &sr1, &sr2);
      RETURN_NOT_OK(r1->add_range_unsafe(d, sr1));
      RETURN_NOT_OK(r2->add_range_unsafe(d, sr2));
    } else {
      RETURN_NOT_OK(r1->add_range_unsafe(d, r));
      RETURN_NOT_OK(r2->add_range_unsafe(d, r));
    }
  }

  return Status::Ok();
}

Status Subarray::split(
    uint64_t splitting_range,
    unsigned splitting_dim,
    const ByteVecValue& splitting_value,
    Subarray* r1,
    Subarray* r2) const {
  assert(r1 != nullptr);
  assert(r2 != nullptr);
  *r1 = Subarray(array_, layout_, stats_->parent(), coalesce_ranges_);
  *r2 = Subarray(array_, layout_, stats_->parent(), coalesce_ranges_);

  // For easy reference
  auto array_schema = array_->array_schema();
  auto dim_num = array_schema->dim_num();
  uint64_t range_num;
  Range sr1, sr2;

  for (unsigned d = 0; d < dim_num; ++d) {
    RETURN_NOT_OK(this->get_range_num(d, &range_num));
    if (d != splitting_dim) {
      for (uint64_t j = 0; j < range_num; ++j) {
        const auto& r = ranges_[d][j];
        RETURN_NOT_OK(r1->add_range_unsafe(d, r));
        RETURN_NOT_OK(r2->add_range_unsafe(d, r));
      }
    } else {                                // d == splitting_dim
      if (splitting_range != UINT64_MAX) {  // Need to split multiple ranges
        for (uint64_t j = 0; j <= splitting_range; ++j) {
          const auto& r = ranges_[d][j];
          RETURN_NOT_OK(r1->add_range_unsafe(d, r));
        }
        for (uint64_t j = splitting_range + 1; j < range_num; ++j) {
          const auto& r = ranges_[d][j];
          RETURN_NOT_OK(r2->add_range_unsafe(d, r));
        }
      } else {  // Need to split a single range
        const auto& r = ranges_[d][0];
        auto dim = array_schema->dimension(d);
        dim->split_range(r, splitting_value, &sr1, &sr2);
        RETURN_NOT_OK(r1->add_range_unsafe(d, sr1));
        RETURN_NOT_OK(r2->add_range_unsafe(d, sr2));
      }
    }
  }

  return Status::Ok();
}

const std::vector<std::vector<uint8_t>>& Subarray::tile_coords() const {
  return tile_coords_;
}

template <class T>
Status Subarray::compute_tile_coords() {
  auto timer_se = stats_->start_timer("read_compute_tile_coords");

  if (array_->array_schema()->tile_order() == Layout::ROW_MAJOR)
    return compute_tile_coords_row<T>();
  return compute_tile_coords_col<T>();
}

template <class T>
const T* Subarray::tile_coords_ptr(
    const std::vector<T>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const {
  auto dim_num = array_->array_schema()->dim_num();
  auto coord_size = array_->array_schema()->dimension(0)->coord_size();
  std::memcpy(&((*aux_tile_coords)[0]), &tile_coords[0], dim_num * coord_size);
  auto it = tile_coords_map_.find(*aux_tile_coords);
  if (it == tile_coords_map_.end())
    return nullptr;
  return (const T*)&(tile_coords_[it->second][0]);
}

const SubarrayTileOverlap* Subarray::subarray_tile_overlap() const {
  return &tile_overlap_;
}

Status Subarray::compute_relevant_fragment_est_result_sizes(
    const std::vector<std::string>& names,
    uint64_t range_start,
    uint64_t range_end,
    std::vector<std::vector<ResultSize>>* result_sizes,
    std::vector<std::vector<MemorySize>>* mem_sizes,
    ThreadPool* const compute_tp) {
  // For easy reference
  auto array_schema = array_->array_schema();
  auto fragment_metadata = array_->fragment_metadata();
  auto encryption_key = array_->encryption_key();
  auto dim_num = array_->array_schema()->dim_num();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order_ : layout_;

  RETURN_NOT_OK(load_relevant_fragment_tile_var_sizes(names, compute_tp));

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
    var_sizes.push_back(array_schema->var_size(name));
    nullable.push_back(array_schema->is_nullable(name));
  }

  auto all_dims_same_type = array_schema->domain()->all_dims_same_type();
  auto all_dims_fixed = array_schema->domain()->all_dims_fixed();
  auto num_threads = compute_tp->concurrency_level();
  auto ranges_per_thread = (uint64_t)ceil((double)range_num / num_threads);
  auto status = parallel_for(compute_tp, 0, num_threads, [&](uint64_t t) {
    auto r_start = range_start + t * ranges_per_thread;
    auto r_end =
        std::min(range_start + (t + 1) * ranges_per_thread - 1, range_end);
    auto r_coords = get_range_coords(r_start);
    for (uint64_t r = r_start; r <= r_end; ++r) {
      RETURN_NOT_OK(compute_relevant_fragment_est_result_sizes(
          encryption_key,
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
          &frag_tiles[r - range_start]));

      // Get next range coordinates
      if (layout == Layout::ROW_MAJOR) {
        auto d = dim_num - 1;
        ++(r_coords)[d];
        while ((r_coords)[d] >= ranges_[d].size() && d != 0) {
          (r_coords)[d] = 0;
          --d;
          ++(r_coords)[d];
        }
      } else if (layout == Layout::COL_MAJOR) {
        auto d = (unsigned)0;
        ++(r_coords)[d];
        while ((r_coords)[d] >= ranges_[d].size() && d != dim_num - 1) {
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
  RETURN_NOT_OK(status);

  // Compute the mem sizes vector
  mem_sizes->resize(range_num);
  for (auto& ms : *mem_sizes)
    ms.resize(names.size(), {0, 0, 0});
  std::unordered_set<std::pair<unsigned, uint64_t>, utils::hash::pair_hash>
      all_frag_tiles;
  uint64_t tile_size, tile_var_size;
  for (uint64_t r = 0; r < range_num; ++r) {
    auto& mem_vec = (*mem_sizes)[r];
    for (const auto& ft : frag_tiles[r]) {
      auto it = all_frag_tiles.insert(ft);
      if (it.second) {  // If the fragment/tile pair is new
        auto meta = fragment_metadata[ft.first];
        for (size_t i = 0; i < names.size(); ++i) {
          tile_size = meta->tile_size(names[i], ft.second);
          auto cell_size = array_schema->cell_size(names[i]);
          if (!var_sizes[i]) {
            mem_vec[i].size_fixed_ += tile_size;
            if (nullable[i])
              mem_vec[i].size_validity_ +=
                  tile_size / cell_size * constants::cell_validity_size;
          } else {
            RETURN_NOT_OK(meta->tile_var_size(
                *encryption_key, names[i], ft.second, &tile_var_size));
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

  return Status::Ok();
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

Status Subarray::set_est_result_size(
    std::unordered_map<std::string, ResultSize>& est_result_size,
    std::unordered_map<std::string, MemorySize>& max_mem_size) {
  est_result_size_ = est_result_size;
  max_mem_size_ = max_mem_size;
  est_result_size_computed_ = true;

  return Status::Ok();
}

void Subarray::set_add_or_coalesce_range_func() {
  const unsigned int dim_num = array_->array_schema()->dim_num();

  // Bind an `add_or_coalesce_range_func_` for each dimension.
  add_or_coalesce_range_func_.resize(dim_num);
  for (unsigned int dim_idx = 0; dim_idx < dim_num; ++dim_idx) {
    const Dimension* const dim = array_->array_schema()->dimension(dim_idx);

    // We only coalesce ranges of fixed sizes. If the dimension
    // is of a var-sized data type, we will not attempt to
    // coalesce its ranges.
    if (dim->var_size()) {
      add_or_coalesce_range_func_[dim_idx] = std::bind(
          &Subarray::add_range_without_coalesce,
          std::placeholders::_1,
          std::placeholders::_2,
          std::placeholders::_3);
      continue;
    }

    // If this instance was constructed to disable coalescing
    // ranges, we will use the routine that does not attempt
    // to coalesce ranges.
    if (!coalesce_ranges_) {
      add_or_coalesce_range_func_[dim_idx] = std::bind(
          &Subarray::add_range_without_coalesce,
          std::placeholders::_1,
          std::placeholders::_2,
          std::placeholders::_3);
      continue;
    }

    const Datatype type = dim->type();
    switch (type) {
      case Datatype::INT8:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<int8_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::UINT8:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<uint8_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::INT16:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<int16_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::UINT16:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<uint16_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::INT32:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<int32_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::UINT32:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<uint32_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::INT64:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<int64_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::UINT64:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<uint64_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::FLOAT32:
      case Datatype::FLOAT64:
        // We can not reasonably coalesce floating point types.
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_range_without_coalesce,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::DATETIME_YEAR:
      case Datatype::DATETIME_MONTH:
      case Datatype::DATETIME_WEEK:
      case Datatype::DATETIME_DAY:
      case Datatype::DATETIME_HR:
      case Datatype::DATETIME_MIN:
      case Datatype::DATETIME_SEC:
      case Datatype::DATETIME_MS:
      case Datatype::DATETIME_US:
      case Datatype::DATETIME_NS:
      case Datatype::DATETIME_PS:
      case Datatype::DATETIME_FS:
      case Datatype::DATETIME_AS:
      case Datatype::TIME_HR:
      case Datatype::TIME_MIN:
      case Datatype::TIME_SEC:
      case Datatype::TIME_MS:
      case Datatype::TIME_US:
      case Datatype::TIME_NS:
      case Datatype::TIME_PS:
      case Datatype::TIME_FS:
      case Datatype::TIME_AS:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<int64_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::CHAR:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<char>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::STRING_ASCII:
      case Datatype::STRING_UTF8:
      case Datatype::STRING_UTF16:
      case Datatype::STRING_UTF32:
      case Datatype::STRING_UCS2:
      case Datatype::STRING_UCS4:
        // We can not reasonably coalesce string types.
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_range_without_coalesce,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      case Datatype::ANY:
        add_or_coalesce_range_func_[dim_idx] = std::bind(
            &Subarray::add_or_coalesce_range<uint8_t>,
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3);
        break;
      default:
        LOG_FATAL("Unexpected datatype " + datatype_str(type));
    }
  }
}

void Subarray::add_range_without_coalesce(
    const uint32_t dim_idx, const Range& range) {
  ranges_[dim_idx].emplace_back(range);
}

template <class T>
void Subarray::add_or_coalesce_range(
    const uint32_t dim_idx, const Range& range) {
  std::vector<Range>* const ranges = &ranges_[dim_idx];

  // If `ranges` is empty, there is not an existing range to coalesce with.
  if (ranges->empty()) {
    ranges->emplace_back(range);
    return;
  }

  // If the start index of `range` immediately proceeds the end of the
  // last range on `ranges`, they are contiguous and will be coalesced.
  Range& last_range = ranges->back();
  const bool contiguous = *static_cast<const T*>(last_range.end()) !=
                              std::numeric_limits<T>::max() &&
                          *static_cast<const T*>(last_range.end()) + 1 ==
                              *static_cast<const T*>(range.start());

  // Coalesce `range` with `last_range` if they are contiguous.
  if (contiguous)
    last_range.set_end(range.end());
  else
    ranges->emplace_back(range);
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Subarray::add_default_ranges() {
  auto array_schema = array_->array_schema();
  auto dim_num = array_schema->dim_num();
  auto domain = array_schema->domain()->domain();

  ranges_.resize(dim_num);
  is_default_.resize(dim_num, true);
  for (unsigned d = 0; d < dim_num; ++d)
    ranges_[d].emplace_back(domain[d]);
}

void Subarray::compute_range_offsets() {
  range_offsets_.clear();

  auto dim_num = this->dim_num();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order_ : layout_;

  if (layout == Layout::COL_MAJOR) {
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = 1; i < dim_num; ++i)
        range_offsets_.push_back(range_offsets_.back() * ranges_[i - 1].size());
    }
  } else if (layout == Layout::ROW_MAJOR) {
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = dim_num - 2;; --i) {
        range_offsets_.push_back(range_offsets_.back() * ranges_[i + 1].size());
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

Status Subarray::compute_est_result_size(
    const Config* const config, ThreadPool* const compute_tp) {
  auto timer_se = stats_->start_timer("read_compute_est_result_size");
  if (est_result_size_computed_)
    return Status::Ok();

  // TODO: This routine is used in the path for the C APIs that estimate
  // result sizes. We need to refactor this routine to handle the scenario
  // where `tile_overlap_` may be truncated to fit the memory budget.
  RETURN_NOT_OK(
      precompute_tile_overlap(0, range_num() - 1, config, compute_tp, true));

  // Prepare estimated result size vector for all
  // attributes/dimension and zipped coords
  auto array_schema = array_->array_schema();
  auto attribute_num = array_schema->attribute_num();
  auto dim_num = array_schema->dim_num();
  auto attributes = array_schema->attributes();
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
      names[i] = array_schema->domain()->dimension(i - attribute_num)->name();
    else
      names[i] = constants::coords;
  }

  // Compute the estimated result and max memory sizes
  std::vector<std::vector<ResultSize>> result_sizes;
  std::vector<std::vector<MemorySize>> mem_sizes;
  RETURN_NOT_OK(compute_relevant_fragment_est_result_sizes(
      names, 0, range_num - 1, &result_sizes, &mem_sizes, compute_tp));

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
  if (array_schema->dense()) {
    auto cell_num = this->cell_num();
    for (unsigned i = 0; i < num; ++i) {
      if (!array_schema->var_size(names[i])) {
        min_size_fixed = cell_num * array_schema->cell_size(names[i]);
        min_size_var = 0;
      } else {
        min_size_fixed = cell_num * constants::cell_var_offset_size;
        min_size_var =
            cell_num * array_schema->attribute(names[i])->fill_value().size();
      }

      if (array_schema->is_nullable(names[i])) {
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

  return Status::Ok();
}

bool Subarray::est_result_size_computed() {
  return est_result_size_computed_;
}

Status Subarray::compute_relevant_fragment_est_result_sizes(
    const EncryptionKey* encryption_key,
    const ArraySchema* array_schema,
    bool all_dims_same_type,
    bool all_dims_fixed,
    const std::vector<FragmentMetadata*>& fragment_meta,
    const std::vector<std::string>& names,
    const std::vector<bool>& var_sizes,
    const std::vector<bool>& nullable,
    uint64_t range_idx,
    const std::vector<uint64_t>& range_coords,
    std::vector<ResultSize>* result_sizes,
    std::set<std::pair<unsigned, uint64_t>>* frag_tiles) {
  result_sizes->resize(names.size(), {0.0, 0.0, 0.0});

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
    uint64_t tile_size = 0, tile_var_size = 0;
    for (const auto& tr : overlap->tile_ranges_) {
      for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
        for (size_t n = 0; n < names.size(); ++n) {
          // Zipped coords applicable only in homogeneous domains
          if (names[n] == constants::coords && !all_dims_same_type)
            continue;

          frag_tiles->insert(std::pair<unsigned, uint64_t>(f, tid));
          tile_size = meta->tile_size(names[n], tid);
          auto attr_datatype_size = datatype_size(array_schema->type(names[n]));
          if (!var_sizes[n]) {
            (*result_sizes)[n].size_fixed_ += tile_size;
            if (nullable[n])
              (*result_sizes)[n].size_validity_ +=
                  tile_size / attr_datatype_size *
                  constants::cell_validity_size;
          } else {
            (*result_sizes)[n].size_fixed_ += tile_size;
            RETURN_NOT_OK(meta->tile_var_size(
                *encryption_key, names[n], tid, &tile_var_size));
            (*result_sizes)[n].size_var_ += tile_var_size;
            if (nullable[n])
              (*result_sizes)[n].size_validity_ +=
                  tile_var_size / attr_datatype_size *
                  constants::cell_validity_size;
          }
        }
      }
    }

    // Parse individual tiles
    for (const auto& t : overlap->tiles_) {
      auto tid = t.first;
      auto ratio = t.second;
      for (size_t n = 0; n < names.size(); ++n) {
        // Zipped coords applicable only in homogeneous domains
        if (names[n] == constants::coords && !all_dims_same_type)
          continue;

        frag_tiles->insert(std::pair<unsigned, uint64_t>(f, tid));
        tile_size = meta->tile_size(names[n], tid);
        auto attr_datatype_size = datatype_size(array_schema->type(names[n]));
        if (!var_sizes[n]) {
          (*result_sizes)[n].size_fixed_ += tile_size * ratio;
          if (nullable[n])
            (*result_sizes)[n].size_validity_ +=
                (tile_size / attr_datatype_size *
                 constants::cell_validity_size) *
                ratio;

        } else {
          (*result_sizes)[n].size_fixed_ += tile_size * ratio;
          RETURN_NOT_OK(meta->tile_var_size(
              *encryption_key, names[n], tid, &tile_var_size));
          (*result_sizes)[n].size_var_ += tile_var_size * ratio;
          if (nullable[n])
            (*result_sizes)[n].size_validity_ +=
                (tile_var_size / attr_datatype_size *
                 constants::cell_validity_size) *
                ratio;
        }
      }
    }
  }

  // Calibrate result - applicable only to arrays without coordinate duplicates
  // and fixed dimensions
  if (!array_schema->allows_dups() && all_dims_fixed) {
    // Calculate cell num
    uint64_t cell_num = 1;
    auto dim_num = array_schema->dim_num();
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema->dimension(d);
      cell_num = utils::math::safe_mul(
          cell_num, dim->domain_range(ranges_[d][range_coords[d]]));
    }

    uint64_t max_size_fixed = UINT64_MAX;
    uint64_t max_size_var = UINT64_MAX;
    uint64_t max_size_validity = UINT64_MAX;
    for (size_t n = 0; n < names.size(); ++n) {
      // Zipped coords applicable only in homogeneous domains
      if (names[n] == constants::coords && !all_dims_same_type)
        continue;

      if (var_sizes[n]) {
        max_size_fixed =
            utils::math::safe_mul(cell_num, constants::cell_var_offset_size);
      } else {
        max_size_fixed =
            utils::math::safe_mul(cell_num, array_schema->cell_size(names[n]));
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

  return Status::Ok();
}

template <class T>
Status Subarray::compute_tile_coords_col() {
  std::vector<std::set<T>> coords_set;
  auto array_schema = array_->array_schema();
  auto domain = array_schema->domain()->domain();
  auto dim_num = this->dim_num();
  uint64_t tile_start, tile_end;

  // Compute unique tile coords per dimension
  coords_set.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto tile_extent = *(const T*)array_schema->domain()->tile_extent(d).data();
    for (uint64_t j = 0; j < ranges_[d].size(); ++j) {
      auto dim_dom = (const T*)domain[d].data();
      auto r = (const T*)ranges_[d][j].data();
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
  auto coords_size = dim_num * array_schema->dimension(0)->coord_size();
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

  return Status::Ok();
}

template <class T>
Status Subarray::compute_tile_coords_row() {
  std::vector<std::set<T>> coords_set;
  auto array_schema = array_->array_schema();
  auto domain = array_schema->domain()->domain();
  auto dim_num = this->dim_num();
  uint64_t tile_start, tile_end;

  // Compute unique tile coords per dimension
  coords_set.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto tile_extent = *(const T*)array_schema->domain()->tile_extent(d).data();
    auto dim_dom = (const T*)domain[d].data();
    for (uint64_t j = 0; j < ranges_[d].size(); ++j) {
      auto r = (const T*)ranges_[d][j].data();
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
  auto coords_size = dim_num * array_schema->dimension(0)->coord_size();
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

  return Status::Ok();
}

Status Subarray::precompute_tile_overlap(
    const uint64_t start_range_idx,
    const uint64_t end_range_idx,
    const Config* config,
    ThreadPool* const compute_tp,
    const bool override_memory_constraint) {
  auto timer_se = stats_->start_timer("read_compute_tile_overlap");

  // If the `tile_overlap_` has already been precomputed and contains
  // the given range, re-use it with new range.
  const bool tile_overlap_computed =
      tile_overlap_.contains_range(start_range_idx, end_range_idx);
  if (tile_overlap_computed) {
    stats_->add_counter("precompute_tile_overlap.tile_overlap_cache_hit", 1);
    tile_overlap_.update_range(start_range_idx, end_range_idx);
    return Status::Ok();
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
  RETURN_NOT_OK(config->get<uint64_t>(
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
  ComputeRelevantFragmentsCtx relevant_fragment_ctx;
  ComputeRelevantTileOverlapCtx tile_overlap_ctx;
  SubarrayTileOverlap tile_overlap(
      fragment_num, tile_overlap_start, tmp_tile_overlap_end);
  do {
    RETURN_NOT_OK(compute_relevant_fragments(
        compute_tp, &tile_overlap, &relevant_fragment_ctx));
    RETURN_NOT_OK(load_relevant_fragment_rtrees(compute_tp));
    RETURN_NOT_OK(compute_relevant_fragment_tile_overlap(
        compute_tp, &tile_overlap, &tile_overlap_ctx));

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

  return Status::Ok();
}

Subarray Subarray::clone() const {
  Subarray clone;
  clone.stats_ = stats_;
  clone.array_ = array_;
  clone.layout_ = layout_;
  clone.cell_order_ = cell_order_;
  clone.ranges_ = ranges_;
  clone.is_default_ = is_default_;
  clone.range_offsets_ = range_offsets_;
  clone.tile_overlap_ = tile_overlap_;
  clone.est_result_size_computed_ = est_result_size_computed_;
  clone.coalesce_ranges_ = coalesce_ranges_;
  clone.add_or_coalesce_range_func_ = add_or_coalesce_range_func_;
  clone.est_result_size_ = est_result_size_;
  clone.max_mem_size_ = max_mem_size_;
  clone.relevant_fragments_ = relevant_fragments_;

  return clone;
}

TileOverlap Subarray::compute_tile_overlap(
    uint64_t range_idx, unsigned fid) const {
  assert(array_->array_schema()->dense());
  auto type = array_->array_schema()->dimension(0)->type();
  switch (type) {
    case Datatype::INT8:
      return compute_tile_overlap<int8_t>(range_idx, fid);
    case Datatype::UINT8:
      return compute_tile_overlap<uint8_t>(range_idx, fid);
    case Datatype::INT16:
      return compute_tile_overlap<int16_t>(range_idx, fid);
    case Datatype::UINT16:
      return compute_tile_overlap<uint16_t>(range_idx, fid);
    case Datatype::INT32:
      return compute_tile_overlap<int32_t>(range_idx, fid);
    case Datatype::UINT32:
      return compute_tile_overlap<uint32_t>(range_idx, fid);
    case Datatype::INT64:
      return compute_tile_overlap<int64_t>(range_idx, fid);
    case Datatype::UINT64:
      return compute_tile_overlap<uint64_t>(range_idx, fid);
    case Datatype::FLOAT32:
      return compute_tile_overlap<float>(range_idx, fid);
    case Datatype::FLOAT64:
      return compute_tile_overlap<double>(range_idx, fid);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return compute_tile_overlap<int64_t>(range_idx, fid);
    default:
      assert(false);
  }
  return TileOverlap();
}

template <class T>
TileOverlap Subarray::compute_tile_overlap(
    uint64_t range_idx, unsigned fid) const {
  assert(array_->array_schema()->dense());
  TileOverlap ret;
  auto ndrange = this->ndrange(range_idx);

  // Prepare a range copy
  auto dim_num = array_->array_schema()->dim_num();
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
  std::swap(array_, subarray.array_);
  std::swap(layout_, subarray.layout_);
  std::swap(cell_order_, subarray.cell_order_);
  std::swap(ranges_, subarray.ranges_);
  std::swap(is_default_, subarray.is_default_);
  std::swap(range_offsets_, subarray.range_offsets_);
  std::swap(tile_overlap_, subarray.tile_overlap_);
  std::swap(est_result_size_computed_, subarray.est_result_size_computed_);
  std::swap(coalesce_ranges_, subarray.coalesce_ranges_);
  std::swap(add_or_coalesce_range_func_, subarray.add_or_coalesce_range_func_);
  std::swap(est_result_size_, subarray.est_result_size_);
  std::swap(max_mem_size_, subarray.max_mem_size_);
  std::swap(relevant_fragments_, subarray.relevant_fragments_);
}

Status Subarray::compute_relevant_fragments(
    ThreadPool* const compute_tp,
    const SubarrayTileOverlap* const tile_overlap,
    ComputeRelevantFragmentsCtx* const fn_ctx) {
  auto timer_se = stats_->start_timer("read_compute_relevant_frags");

  // Fetch the calibrated, multi-dimensional coordinates from the
  // flattened (total order) range indexes. In this context,
  // "calibration" implies that the coordinates contain the minimum
  // n-dimensional space to encapsulate all ranges within `tile_overlap`.
  std::vector<uint64_t> start_coords;
  std::vector<uint64_t> end_coords;
  get_expanded_coordinates(
      tile_overlap->range_idx_start(),
      tile_overlap->range_idx_end(),
      &start_coords,
      &end_coords);

  // If the calibrated coordinates have not changed from
  // the last call to this function, the computed relevant
  // fragments will not change.
  if (fn_ctx->initialized_ && start_coords == fn_ctx->last_start_coords_ &&
      end_coords == fn_ctx->last_end_coords_) {
    return Status::Ok();
  }

  // Perform lazy-initialization the context cache for this routine.
  const size_t fragment_num = array_->fragment_metadata().size();
  const uint32_t dim_num = array_->array_schema()->dim_num();
  if (!fn_ctx->initialized_) {
    fn_ctx->initialized_ = true;

    // Create a fragment bytemap for each dimension. Each
    // non-zero byte represents an overlap between a fragment
    // and at least one range in the corresponding dimension.
    fn_ctx->frag_bytemaps_.resize(dim_num);
    for (uint32_t d = 0; d < dim_num; ++d) {
      fn_ctx->frag_bytemaps_[d].resize(fragment_num, 0);
    }
  }

  // Store the current calibrated coordinates.
  fn_ctx->last_start_coords_ = start_coords;
  fn_ctx->last_end_coords_ = end_coords;

  // Populate the fragment bytemap for each dimension in parallel.
  RETURN_NOT_OK(parallel_for(compute_tp, 0, dim_num, [&](const uint32_t d) {
    return compute_relevant_fragments_for_dim(
        compute_tp,
        d,
        fragment_num,
        start_coords,
        end_coords,
        &fn_ctx->frag_bytemaps_[d]);
  }));

  // Recalculate relevant fragments.
  relevant_fragments_.clear();
  relevant_fragments_.reserve(fragment_num);
  for (unsigned f = 0; f < fragment_num; ++f) {
    bool relevant = true;
    for (uint32_t d = 0; d < dim_num; ++d) {
      if (fn_ctx->frag_bytemaps_[d][f] == 0) {
        relevant = false;
        break;
      }
    }

    if (relevant) {
      relevant_fragments_.emplace_back(f);
    }
  }

  return Status::Ok();
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
      (layout_ == Layout::UNORDERED) ? cell_order_ : layout_;
  if (coords_layout == Layout::GLOBAL_ORDER ||
      coords_layout == Layout::HILBERT) {
    assert(*start_coords == *end_coords);
    return;
  }

  assert(
      coords_layout == Layout::ROW_MAJOR || coords_layout == Layout::COL_MAJOR);

  const uint32_t dim_num = array_->array_schema()->dim_num();

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
      (*end_coords)[d] = ranges_[d].size() - 1;
    }
  } else {
    assert(coords_layout == Layout::COL_MAJOR);
    for (int64_t d = expand_d; d >= 0; --d) {
      (*start_coords)[d] = 0;
      (*end_coords)[d] = ranges_[d].size() - 1;
    }
  }
}

Status Subarray::compute_relevant_fragments_for_dim(
    ThreadPool* const compute_tp,
    const uint32_t dim_idx,
    const uint64_t fragment_num,
    const std::vector<uint64_t>& start_coords,
    const std::vector<uint64_t>& end_coords,
    std::vector<uint8_t>* const frag_bytemap) const {
  const std::vector<FragmentMetadata*> meta = array_->fragment_metadata();
  const Dimension* const dim = array_->array_schema()->dimension(dim_idx);

  return parallel_for(compute_tp, 0, fragment_num, [&](const uint64_t f) {
    // We're done when we have already determined fragment `f` to
    // be relevant for this dimension.
    if ((*frag_bytemap)[f] == 1) {
      return Status::Ok();
    }

    // The fragment `f` is relevant to this dimension's fragment bytemap
    // if it overlaps with any range between the start and end coordinates
    // on this dimension.
    const Range& frag_range = meta[f]->non_empty_domain()[dim_idx];
    for (uint64_t r = start_coords[dim_idx]; r <= end_coords[dim_idx]; ++r) {
      const Range& query_range = ranges_[dim_idx][r];

      if (dim->overlap(frag_range, query_range)) {
        (*frag_bytemap)[f] = 1;
        break;
      }
    }

    return Status::Ok();
  });
}

Status Subarray::load_relevant_fragment_rtrees(
    ThreadPool* const compute_tp) const {
  auto timer_se = stats_->start_timer("read_load_relevant_rtrees");

  auto meta = array_->fragment_metadata();
  auto encryption_key = array_->encryption_key();

  auto status =
      parallel_for(compute_tp, 0, relevant_fragments_.size(), [&](uint64_t f) {
        return meta[relevant_fragments_[f]]->load_rtree(*encryption_key);
      });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Subarray::compute_relevant_fragment_tile_overlap(
    ThreadPool* const compute_tp,
    SubarrayTileOverlap* const tile_overlap,
    ComputeRelevantTileOverlapCtx* const fn_ctx) {
  auto timer_se = stats_->start_timer("read_compute_relevant_tile_overlap");

  const auto range_num = tile_overlap->range_num();
  fn_ctx->range_idx_offset_ = fn_ctx->range_idx_offset_ + fn_ctx->range_len_;
  fn_ctx->range_len_ = range_num - fn_ctx->range_idx_offset_;

  const auto& meta = array_->fragment_metadata();

  auto status =
      parallel_for(compute_tp, 0, relevant_fragments_.size(), [&](uint64_t i) {
        const auto f = relevant_fragments_[i];
        const auto dense = meta[f]->dense();
        return compute_relevant_fragment_tile_overlap(
            meta[f], f, dense, compute_tp, tile_overlap, fn_ctx);
      });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Subarray::compute_relevant_fragment_tile_overlap(
    FragmentMetadata* meta,
    unsigned frag_idx,
    bool dense,
    ThreadPool* const compute_tp,
    SubarrayTileOverlap* const tile_overlap,
    ComputeRelevantTileOverlapCtx* const fn_ctx) {
  const auto num_threads = compute_tp->concurrency_level();
  const auto range_num = fn_ctx->range_len_;

  const auto ranges_per_thread =
      (uint64_t)ceil((double)range_num / num_threads);
  const auto status = parallel_for(compute_tp, 0, num_threads, [&](uint64_t t) {
    const auto r_start = fn_ctx->range_idx_offset_ + (t * ranges_per_thread);
    const auto r_end = fn_ctx->range_idx_offset_ +
                       std::min((t + 1) * ranges_per_thread - 1, range_num - 1);
    for (uint64_t r = r_start; r <= r_end; ++r) {
      if (dense) {  // Dense fragment
        *tile_overlap->at(frag_idx, r) =
            compute_tile_overlap(r + tile_overlap->range_idx_start(), frag_idx);
      } else {  // Sparse fragment
        const auto& range = this->ndrange(r + tile_overlap->range_idx_start());
        RETURN_NOT_OK(
            meta->get_tile_overlap(range, tile_overlap->at(frag_idx, r)));
      }
    }

    return Status::Ok();
  });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Subarray::load_relevant_fragment_tile_var_sizes(
    const std::vector<std::string>& names, ThreadPool* const compute_tp) const {
  auto array_schema = array_->array_schema();
  auto encryption_key = array_->encryption_key();
  auto meta = array_->fragment_metadata();

  // Find the names of the var-sized dimensions or attributes
  std::vector<std::string> var_names;
  var_names.reserve(names.size());
  for (unsigned i = 0; i < names.size(); ++i) {
    if (array_schema->var_size(names[i]))
      var_names.emplace_back(names[i]);
  }

  // No var-sized attributes/dimensions in `names`
  if (var_names.empty())
    return Status::Ok();

  // Load all metadata for tile var sizes among fragments.
  for (const auto& var_name : var_names) {
    const auto status = parallel_for(
        compute_tp, 0, relevant_fragments_.size(), [&](const size_t i) {
          auto f = relevant_fragments_[i];
          return meta[f]->load_tile_var_sizes(*encryption_key, var_name);
        });

    RETURN_NOT_OK(status);
  }

  return Status::Ok();
}

std::vector<unsigned> Subarray::relevant_fragments() const {
  return relevant_fragments_;
}

stats::Stats* Subarray::stats() const {
  return stats_;
}

// Explicit instantiations
template Status Subarray::compute_tile_coords<int8_t>();
template Status Subarray::compute_tile_coords<uint8_t>();
template Status Subarray::compute_tile_coords<int16_t>();
template Status Subarray::compute_tile_coords<uint16_t>();
template Status Subarray::compute_tile_coords<int32_t>();
template Status Subarray::compute_tile_coords<uint32_t>();
template Status Subarray::compute_tile_coords<int64_t>();
template Status Subarray::compute_tile_coords<uint64_t>();
template Status Subarray::compute_tile_coords<float>();
template Status Subarray::compute_tile_coords<double>();

template void Subarray::add_or_coalesce_range<int8_t>(uint32_t, const Range&);
template void Subarray::add_or_coalesce_range<uint8_t>(uint32_t, const Range&);
template void Subarray::add_or_coalesce_range<int16_t>(uint32_t, const Range&);
template void Subarray::add_or_coalesce_range<uint16_t>(uint32_t, const Range&);
template void Subarray::add_or_coalesce_range<int32_t>(uint32_t, const Range&);
template void Subarray::add_or_coalesce_range<uint32_t>(uint32_t, const Range&);
template void Subarray::add_or_coalesce_range<int64_t>(uint32_t, const Range&);
template void Subarray::add_or_coalesce_range<uint64_t>(uint32_t, const Range&);
template void Subarray::add_or_coalesce_range<char>(uint32_t, const Range&);

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

}  // namespace sm
}  // namespace tiledb
