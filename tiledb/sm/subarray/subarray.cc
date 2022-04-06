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
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/hash.h"
#include "tiledb/sm/misc/math.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/resource_pool.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/rtree/rtree.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/subarray/subarray.h"

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
    , coalesce_ranges_(true)
    , ranges_sorted_(false) {
}

Subarray::Subarray(
    const Array* array,
    Stats* const parent_stats,
    tdb_shared_ptr<Logger> logger,
    const bool coalesce_ranges,
    StorageManager* storage_manager)
    : Subarray(
          array,
          Layout::UNORDERED,
          parent_stats,
          logger,
          coalesce_ranges,
          storage_manager) {
}

Subarray::Subarray(
    const Array* const array,
    const Layout layout,
    Stats* const parent_stats,
    tdb_shared_ptr<Logger> logger,
    const bool coalesce_ranges,
    StorageManager* storage_manager)
    : stats_(
          parent_stats ? parent_stats->create_child("Subarray") :
                         storage_manager ?
                         storage_manager->stats()->create_child("subSubarray") :
                         nullptr)
    , logger_(logger->clone("Subarray", ++logger_id_))
    , array_(array)
    , layout_(layout)
    , cell_order_(array_->array_schema_latest().cell_order())
    , est_result_size_computed_(false)
    , coalesce_ranges_(coalesce_ranges)
    , ranges_sorted_(false) {
  if (!parent_stats && !storage_manager)
    throw std::runtime_error(
        "Subarray(): missing parent_stats requires live storage_manager!");
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

Status Subarray::add_range(
    uint32_t dim_idx, Range&& range, const bool read_range_oob_error) {
  auto dim_num = array_->array_schema_latest().dim_num();
  if (dim_idx >= dim_num)
    return logger_->status(Status_SubarrayError(
        "Cannot add range to dimension; Invalid dimension index"));

  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_.clear();

  // Global layout can only have one range in the range subset. Check if the
  // range was already set.
  if (layout_ == Layout::GLOBAL_ORDER &&
      range_subset_[dim_idx].is_explicitly_initialized()) {
    return logger_->status(
        Status_SubarrayError("Cannot add more than one range per dimension "
                             "to global order query"));
  }

  // Restrict the range to the dimension domain and add.
  auto dim = array_->array_schema_latest().dimension(dim_idx);
  if (!read_range_oob_error)
    RETURN_NOT_OK(dim->adjust_range_oob(&range));
  RETURN_NOT_OK(dim->check_range(range));
  range_subset_[dim_idx].add_range_unrestricted(range);

  // Update is default.
  is_default_[dim_idx] = range_subset_[dim_idx].is_implicitly_initialized();
  return Status::Ok();
}

Status Subarray::add_range_unsafe(uint32_t dim_idx, const Range& range) {
  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_.clear();

  // Add the range
  range_subset_[dim_idx].add_range_unrestricted(range);
  is_default_[dim_idx] = range_subset_[dim_idx].is_implicitly_initialized();
  return Status::Ok();
}

Status Subarray::set_subarray(const void* subarray) {
  if (!array_->array_schema_latest().domain()->all_dims_same_type())
    return LOG_STATUS(
        Status_SubarrayError("Cannot set subarray; Function not applicable to "
                             "heterogeneous domains"));

  if (!array_->array_schema_latest().domain()->all_dims_fixed())
    return LOG_STATUS(
        Status_SubarrayError("Cannot set subarray; Function not applicable to "
                             "domains with variable-sized dimensions"));

  add_default_ranges();
  if (subarray != nullptr) {
    auto dim_num = array_->array_schema_latest().dim_num();
    auto s_ptr = (const unsigned char*)subarray;
    uint64_t offset = 0;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size =
          2 * array_->array_schema_latest().dimension(d)->coord_size();
      Range range(&s_ptr[offset], r_size);
      RETURN_NOT_OK(this->add_range(d, std::move(range), err_on_range_oob_));
      offset += r_size;
    }
  }

  return Status::Ok();
}

Status Subarray::add_range(
    unsigned dim_idx, const void* start, const void* end, const void* stride) {
  if (dim_idx >= this->array_->array_schema_latest().dim_num())
    return LOG_STATUS(
        Status_SubarrayError("Cannot add range; Invalid dimension index"));

  QueryType array_query_type;
  RETURN_NOT_OK(array_->get_query_type(&array_query_type));
  if (array_query_type == tiledb::sm::QueryType::WRITE) {
    if (!array_->array_schema_latest().dense()) {
      return LOG_STATUS(Status_SubarrayError(
          "Adding a subarray range to a write query is not "
          "supported in sparse arrays"));
    }
    if (this->is_set(dim_idx))
      return LOG_STATUS(
          Status_SubarrayError("Cannot add range; Multi-range dense writes "
                               "are not supported"));
  }

  if (start == nullptr || end == nullptr)
    return LOG_STATUS(Status_SubarrayError("Cannot add range; Invalid range"));

  if (stride != nullptr)
    return LOG_STATUS(Status_SubarrayError(
        "Cannot add range; Setting range stride is currently unsupported"));

  if (this->array_->array_schema_latest()
          .domain()
          ->dimension(dim_idx)
          ->var_size())
    return LOG_STATUS(
        Status_SubarrayError("Cannot add range; Range must be fixed-sized"));

  // Prepare a temp range
  std::vector<uint8_t> range;
  auto coord_size =
      this->array_->array_schema_latest().dimension(dim_idx)->coord_size();
  range.resize(2 * coord_size);
  std::memcpy(&range[0], start, coord_size);
  std::memcpy(&range[coord_size], end, coord_size);

  // Add range
  return this->add_range(
      dim_idx, Range(&range[0], 2 * coord_size), err_on_range_oob_);
}

Status Subarray::add_point_ranges(
    unsigned dim_idx, const void* start, uint64_t count) {
  if (dim_idx >= this->array_->array_schema_latest().dim_num())
    return LOG_STATUS(
        Status_SubarrayError("Cannot add range; Invalid dimension index"));

  QueryType array_query_type;
  RETURN_NOT_OK(array_->get_query_type(&array_query_type));
  if (array_query_type == tiledb::sm::QueryType::WRITE) {
    if (!array_->array_schema_latest().dense()) {
      return LOG_STATUS(Status_SubarrayError(
          "Adding a subarray range to a write query is not "
          "supported in sparse arrays"));
    }
    if (this->is_set(dim_idx))
      return LOG_STATUS(
          Status_SubarrayError("Cannot add range; Multi-range dense writes "
                               "are not supported"));
  }

  if (start == nullptr)
    return LOG_STATUS(
        Status_SubarrayError("Cannot add ranges; Invalid start pointer"));

  if (this->array_->array_schema_latest()
          .domain()
          ->dimension(dim_idx)
          ->var_size())
    return LOG_STATUS(
        Status_SubarrayError("Cannot add range; Range must be fixed-sized"));

  // Prepare a temp range
  std::vector<uint8_t> range;
  auto coord_size =
      this->array_->array_schema_latest().dimension(dim_idx)->coord_size();
  range.resize(2 * coord_size);

  for (size_t i = 0; i < count; i++) {
    uint8_t* ptr = (uint8_t*)start + coord_size * i;
    // point ranges
    std::memcpy(&range[0], ptr, coord_size);
    std::memcpy(&range[coord_size], ptr, coord_size);

    // Add range
    auto st = this->add_range(
        dim_idx, Range(&range[0], 2 * coord_size), err_on_range_oob_);
    if (!st.ok()) {
      return LOG_STATUS(std::move(st));
    }
  }
  return Status::Ok();
}

Status Subarray::add_range_by_name(
    const std::string& dim_name,
    const void* start,
    const void* end,
    const void* stride) {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema_latest().domain()->get_dimension_index(
      dim_name, &dim_idx));

  return add_range(dim_idx, start, end, stride);
}

Status Subarray::add_range_var(
    unsigned dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (dim_idx >= array_->array_schema_latest().dim_num())
    return LOG_STATUS(
        Status_SubarrayError("Cannot add range; Invalid dimension index"));

  if ((start == nullptr && start_size != 0) ||
      (end == nullptr && end_size != 0))
    return LOG_STATUS(Status_SubarrayError("Cannot add range; Invalid range"));

  if (!array_->array_schema_latest().domain()->dimension(dim_idx)->var_size())
    return LOG_STATUS(
        Status_SubarrayError("Cannot add range; Range must be variable-sized"));

  QueryType array_query_type;
  RETURN_NOT_OK(array_->get_query_type(&array_query_type));
  if (array_query_type == tiledb::sm::QueryType::WRITE)
    return LOG_STATUS(Status_SubarrayError(
        "Cannot add range; Function applicable only to reads"));

  // Get read_range_oob config setting
  bool found = false;
  std::string read_range_oob = config_.get("sm.read_range_oob", &found);
  assert(found);

  if (read_range_oob != "error" && read_range_oob != "warn")
    return LOG_STATUS(Status_SubarrayError(
        "Invalid value " + read_range_oob +
        " for sm.read_range_obb. Acceptable values are 'error' or 'warn'."));

  // Add range
  Range r;
  r.set_range_var(start, start_size, end, end_size);
  return this->add_range(dim_idx, std::move(r), err_on_range_oob_);
}

Status Subarray::add_range_var_by_name(
    const std::string& dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema_latest().domain()->get_dimension_index(
      dim_name, &dim_idx));

  return add_range_var(dim_idx, start, start_size, end, end_size);
}

Status Subarray::get_range_var(
    unsigned dim_idx, uint64_t range_idx, void* start, void* end) const {
  QueryType array_query_type;
  RETURN_NOT_OK(array_->get_query_type(&array_query_type));
  if (array_query_type == tiledb::sm::QueryType::WRITE)
    return LOG_STATUS(Status_SubarrayError(
        "Getting a var range for a write query is not applicable"));

  uint64_t start_size = 0;
  uint64_t end_size = 0;
  this->get_range_var_size(dim_idx, range_idx, &start_size, &end_size);

  const void* range_start;
  const void* range_end;
  const void* stride;
  RETURN_NOT_OK(
      get_range(dim_idx, range_idx, &range_start, &range_end, &stride));

  std::memcpy(start, range_start, start_size);
  std::memcpy(end, range_end, end_size);

  return Status::Ok();
}

Status Subarray::get_range_num_from_name(
    const std::string& dim_name, uint64_t* range_num) const {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema_latest().domain()->get_dimension_index(
      dim_name, &dim_idx));

  return get_range_num(dim_idx, range_num);
}

Status Subarray::get_range(
    unsigned dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  QueryType array_query_type;
  RETURN_NOT_OK(array_->get_query_type(&array_query_type));
  if (array_query_type == tiledb::sm::QueryType::WRITE) {
    if (!array_->array_schema_latest().dense())
      return LOG_STATUS(
          Status_SubarrayError("Getting a range from a write query is not "
                               "applicable to sparse arrays"));
  }

  *stride = nullptr;
  return this->get_range(dim_idx, range_idx, start, end);
}

Status Subarray::get_range_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema_latest().domain()->get_dimension_index(
      dim_name, &dim_idx));

  return get_range(dim_idx, range_idx, start, end, stride);
}

Status Subarray::get_range_var_size_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema_latest().domain()->get_dimension_index(
      dim_name, &dim_idx));

  return get_range_var_size(dim_idx, range_idx, start_size, end_size);
}

Status Subarray::get_range_var_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    void* start,
    void* end) const {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema_latest().domain()->get_dimension_index(
      dim_name, &dim_idx));

  return get_range_var(dim_idx, range_idx, start, end);
}
const Array* Subarray::array() const {
  return array_;
}

uint64_t Subarray::cell_num() const {
  const auto& array_schema = array_->array_schema_latest();
  unsigned dim_num = array_schema.dim_num();
  uint64_t ret = 1;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema.dimension(d);
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
  auto domain = array_->array_schema_latest().domain();
  unsigned dim_num = array_->array_schema_latest().dim_num();
  auto layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;
  uint64_t tmp_idx = range_idx;

  // Unary case or GLOBAL_ORDER
  if (range_num() == 1) {
    for (unsigned d = 0; d < dim_num; ++d) {
      range_cell_num = domain->dimension(d)->domain_range(range_subset_[d][0]);
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
      range_cell_num = domain->dimension(d)->domain_range(
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
      range_cell_num = domain->dimension(d)->domain_range(
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
    auto dim = array_schema.dimension(d);
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
    auto dim = array_->array_schema_latest().dimension(d);
    if (!dim->coincides_with_tiles(range_subset_[d][0]))
      return false;
  }

  return true;
}

template <class T>
Subarray Subarray::crop_to_tile(const T* tile_coords, Layout layout) const {
  // TBD: is it ok that Subarray log id will increase as if it's a new subarray?
  Subarray ret(array_, layout, stats_->parent(), logger_, false);

  T new_range[2];
  bool overlaps;

  // Get tile subarray based on the input coordinates
  const auto& array_schema = array_->array_schema_latest();
  std::vector<T> tile_subarray(2 * dim_num());
  array_schema.domain()->get_tile_subarray(tile_coords, &tile_subarray[0]);

  // Compute cropped subarray
  for (unsigned d = 0; d < dim_num(); ++d) {
    auto r_size = 2 * array_schema.dimension(d)->coord_size();
    uint64_t i = 0;
    for (size_t r = 0; r < range_subset_[d].num_ranges(); ++r) {
      const auto& range = range_subset_[d][r];
      utils::geometry::overlap(
          (const T*)range.data(),
          &tile_subarray[2 * d],
          1,
          new_range,
          &overlaps);

      if (overlaps) {
        ret.add_range_unsafe(d, Range(new_range, r_size));
        ret.original_range_idx_.resize(dim_num());
        ret.original_range_idx_[d].resize(i + 1);
        ret.original_range_idx_[d][i++] = r;
      }
    }
  }

  return ret;
}

uint32_t Subarray::dim_num() const {
  return array_->array_schema_latest().dim_num();
}

NDRange Subarray::domain() const {
  return array_->array_schema_latest().domain()->domain();
}

bool Subarray::empty() const {
  return range_num() == 0;
}

Status Subarray::get_query_type(QueryType* type) const {
  if (array_ == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get query type from array; Invalid array"));

  return array_->get_query_type(type);
}

Status Subarray::get_range(
    uint32_t dim_idx, uint64_t range_idx, const Range** range) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  if (dim_idx >= dim_num)
    return logger_->status(
        Status_SubarrayError("Cannot get range; Invalid dimension index"));

  auto range_num = range_subset_[dim_idx].num_ranges();
  if (range_idx >= range_num)
    return logger_->status(
        Status_SubarrayError("Cannot get range; Invalid range index"));

  *range = &range_subset_[dim_idx][range_idx];

  return Status::Ok();
}

Status Subarray::get_range(
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  if (dim_idx >= dim_num)
    return logger_->status(
        Status_SubarrayError("Cannot get range; Invalid dimension index"));

  auto range_num = range_subset_[dim_idx].num_ranges();
  if (range_idx >= range_num)
    return logger_->status(
        Status_SubarrayError("Cannot get range; Invalid range index"));

  *start = range_subset_[dim_idx][range_idx].start();
  *end = range_subset_[dim_idx][range_idx].end();

  return Status::Ok();
}

Status Subarray::get_range_var_size(
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start,
    uint64_t* end) const {
  const auto& schema = array_->array_schema_latest();
  auto dim_num = schema.dim_num();
  if (dim_idx >= dim_num)
    return logger_->status(Status_SubarrayError(
        "Cannot get var range size; Invalid dimension index"));

  auto dim = schema.domain()->dimension(dim_idx);
  if (!dim->var_size())
    return logger_->status(Status_SubarrayError(
        "Cannot get var range size; Dimension " + dim->name() +
        " is not var sized"));

  auto range_num = range_subset_[dim_idx].num_ranges();
  if (range_idx >= range_num)
    return logger_->status(
        Status_SubarrayError("Cannot get var range size; Invalid range index"));

  *start = range_subset_[dim_idx][range_idx].start_size();
  *end = range_subset_[dim_idx][range_idx].end_size();

  return Status::Ok();
}

Status Subarray::get_range_num(uint32_t dim_idx, uint64_t* range_num) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  if (dim_idx >= dim_num) {
    std::stringstream msg;
    msg << "Cannot get number of ranges for a dimension; "
           "Invalid dimension index "
        << dim_idx << " requested, " << dim_num - 1 << " max avail.";
    return LOG_STATUS(Status_SubarrayError(msg.str()));
  }

  QueryType array_query_type;
  RETURN_NOT_OK(array_->get_query_type(&array_query_type));
  if (array_query_type == tiledb::sm::QueryType::WRITE &&
      !array_->array_schema_latest().dense()) {
    return LOG_STATUS(
        Status_SubarrayError("Getting the number of ranges from a write query "
                             "is not applicable to sparse arrays"));
  }

  *range_num = range_subset_[dim_idx].num_ranges();

  return Status::Ok();
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

bool Subarray::is_unary(uint64_t range_idx) const {
  auto coords = get_range_coords(range_idx);
  auto dim_num = this->dim_num();

  for (unsigned d = 0; d < dim_num; ++d) {
    if (!range_subset_[d][coords[d]].unary())
      return false;
  }

  return true;
}

void Subarray::set_is_default(uint32_t dim_index, bool is_default) {
  if (is_default) {
    auto dim = array_->array_schema_latest().dimension(dim_index);
    range_subset_.at(dim_index) = RangeSetAndSuperset(
        dim->type(), dim->domain(), is_default, coalesce_ranges_);
  }
  is_default_[dim_index] = is_default;
}

void Subarray::set_layout(Layout layout) {
  layout_ = layout;
}

Status Subarray::set_config(const Config& config) {
  config_ = config;

  QueryType array_query_type;
  RETURN_NOT_OK(array_->get_query_type(&array_query_type));

  if (array_query_type == tiledb::sm::QueryType::READ) {
    bool found = false;
    std::string read_range_oob_str = config.get("sm.read_range_oob", &found);
    assert(found);
    if (read_range_oob_str != "error" && read_range_oob_str != "warn")
      return LOG_STATUS(Status_SubarrayError(
          "Invalid value " + read_range_oob_str +
          " for sm.read_range_obb. Acceptable values are 'error' or 'warn'."));
    err_on_range_oob_ = read_range_oob_str == "error";
  }

  return Status::Ok();
};

const Config* Subarray::config() const {
  return &config_;
}

Status Subarray::set_coalesce_ranges(bool coalesce_ranges) {
  if (count_set_ranges())
    return LOG_STATUS(
        Status_SubarrayError("non-default ranges have been set, cannot change "
                             "coalesce_ranges setting!"));
  // trying to mimic conditions at ctor()
  coalesce_ranges_ = coalesce_ranges;
  add_default_ranges();
  return Status::Ok();
}

Status Subarray::to_byte_vec(std::vector<uint8_t>* byte_vec) const {
  if (range_num() != 1)
    return logger_->status(Status_SubarrayError(
        "Cannot export to byte vector; The subarray must be unary"));

  byte_vec->clear();

  for (const auto& subset : range_subset_) {
    auto offset = byte_vec->size();
    byte_vec->resize(offset + subset[0].size());
    std::memcpy(&(*byte_vec)[offset], subset[0].data(), subset[0].size());
  }

  return Status::Ok();
}

Layout Subarray::layout() const {
  return layout_;
}

Status Subarray::get_est_result_size_internal(
    const char* name,
    uint64_t* size,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute/Dimension name cannot be null"));

  // Check size pointer
  if (size == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get estimated result size; Input size cannot be null"));

  // Check if name is attribute or dimension
  const auto& array_schema = array_->array_schema_latest();
  const bool is_dim = array_schema.is_dim(name);
  const bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return logger_->status(Status_SubarrayError(
        std::string("Cannot get estimated result size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if the attribute/dimension is fixed-sized
  if (array_schema.var_size(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute/Dimension must be fixed-sized"));

  // Check if attribute/dimension is nullable
  if (array_schema.is_nullable(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute/Dimension must not be nullable"));

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size(config, compute_tp));
  *size = static_cast<uint64_t>(std::ceil(est_result_size_[name].size_fixed_));

  // If the size is non-zero, ensure it is large enough to
  // contain at least one cell.
  const auto cell_size = array_schema.cell_size(name);
  if (*size > 0 && *size < cell_size)
    *size = cell_size;

  return Status::Ok();
}

Status Subarray::get_est_result_size(
    const char* name, uint64_t* size, StorageManager* storage_manager) {
  QueryType type;
  // Note: various items below expect array open, get_query_type() providing
  // that audit.
  RETURN_NOT_OK(array_->get_query_type(&type));

  if (type == QueryType::WRITE)
    return LOG_STATUS(Status_SubarrayError(
        "Cannot get estimated result size; Operation currently "
        "unsupported for write queries"));

  if (name == nullptr)
    return LOG_STATUS(Status_SubarrayError(
        "Cannot get estimated result size; Name cannot be null"));

  if (name == constants::coords &&
      !array_->array_schema_latest().domain()->all_dims_same_type())
    return LOG_STATUS(Status_SubarrayError(
        "Cannot get estimated result size; Not applicable to zipped "
        "coordinates in arrays with heterogeneous domain"));

  if (name == constants::coords &&
      !array_->array_schema_latest().domain()->all_dims_fixed())
    return LOG_STATUS(Status_SubarrayError(
        "Cannot get estimated result size; Not applicable to zipped "
        "coordinates in arrays with domains with variable-sized dimensions"));

  if (array_->array_schema_latest().is_nullable(name))
    return LOG_STATUS(Status_SubarrayError(
        std::string(
            "Cannot get estimated result size; Input attribute/dimension '") +
        name + "' is nullable"));

  if (array_->is_remote()) {
    return LOG_STATUS(Status_SubarrayError(
        std::string("Error in query estimate result size; remote/REST "
                    "array functionality not implemented.")));
  }

  return get_est_result_size_internal(
      name, size, &config_, storage_manager->compute_tp());
}

Status Subarray::get_est_result_size(
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    const Config* const config,
    ThreadPool* const compute_tp) {
  // Check attribute/dimension name
  if (name == nullptr)
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute/Dimension name cannot be null"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get estimated result size; Input sizes cannot be null"));

  // Check if name is attribute or dimension
  const auto& array_schema = array_->array_schema_latest();
  const bool is_dim = array_schema.is_dim(name);
  const bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return logger_->status(Status_SubarrayError(
        std::string("Cannot get estimated result size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if the attribute/dimension is var-sized
  if (!array_schema.var_size(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute/Dimension must be var-sized"));

  // Check if attribute/dimension is nullable
  if (array_schema.is_nullable(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute/Dimension must not be nullable"));

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size(config, compute_tp));
  *size_off =
      static_cast<uint64_t>(std::ceil(est_result_size_[name].size_fixed_));
  *size_val =
      static_cast<uint64_t>(std::ceil(est_result_size_[name].size_var_));

  // If the value size is non-zero, ensure both it and the offset size
  // are large enough to contain at least one cell. Otherwise, ensure
  // the offset size is also zero.
  if (*size_val > 0) {
    const auto off_cell_size = constants::cell_var_offset_size;
    if (*size_off < off_cell_size)
      *size_off = off_cell_size;

    const uint64_t val_cell_size = datatype_size(array_schema.type(name));
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
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute name cannot be null"));

  // Check size pointer
  if (size == nullptr || size_validity == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get estimated result size; Input sizes cannot be null"));

  // Check if name is attribute
  const auto& array_schema = array_->array_schema_latest();
  const bool is_attr = array_schema.is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return logger_->status(Status_SubarrayError(
        std::string("Cannot get estimated result size; Attribute '") + name +
        "' does not exist"));

  // Check if the attribute is fixed-sized
  if (array_schema.var_size(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute must be fixed-sized"));

  // Check if attribute is nullable
  if (!array_schema.is_nullable(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute must be nullable"));

  if (array_->is_remote() && !this->est_result_size_computed()) {
    return LOG_STATUS(Status_SubarrayError(
        "Error in query estimate result size; unimplemented "
        "for nullable attributes in remote arrays."));
  }

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size(config, compute_tp));
  *size = static_cast<uint64_t>(std::ceil(est_result_size_[name].size_fixed_));
  *size_validity =
      static_cast<uint64_t>(std::ceil(est_result_size_[name].size_validity_));

  // If the size is non-zero, ensure it is large enough to
  // contain at least one cell.
  const auto cell_size = array_schema.cell_size(name);
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
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute name cannot be null"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr || size_validity == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get estimated result size; Input sizes cannot be null"));

  // Check if name is attribute
  const auto& array_schema = array_->array_schema_latest();
  const bool is_attr = array_schema.is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return logger_->status(Status_SubarrayError(
        std::string("Cannot get estimated result size; Attribute '") + name +
        "' does not exist"));

  // Check if the attribute is var-sized
  if (!array_schema.var_size(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute must be var-sized"));

  // Check if attribute is nullable
  if (!array_schema.is_nullable(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
                             "Attribute must be nullable"));

  if (array_->is_remote() && !this->est_result_size_computed()) {
    return LOG_STATUS(Status_SubarrayError(
        "Error in query estimate result size; unimplemented "
        "for nullable attributes in remote arrays."));
  }

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size(config, compute_tp));
  *size_off =
      static_cast<uint64_t>(std::ceil(est_result_size_[name].size_fixed_));
  *size_val =
      static_cast<uint64_t>(std::ceil(est_result_size_[name].size_var_));
  *size_validity =
      static_cast<uint64_t>(std::ceil(est_result_size_[name].size_validity_));

  // If the value size is non-zero, ensure both it and the offset and
  // validity sizes are large enough to contain at least one cell. Otherwise,
  // ensure the offset and validity sizes are also zero.
  if (*size_val > 0) {
    const uint64_t off_cell_size = constants::cell_var_offset_size;
    if (*size_off < off_cell_size)
      *size_off = off_cell_size;

    const uint64_t val_cell_size = datatype_size(array_schema.type(name));
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
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Attribute/Dimension cannot be null"));

  // Check size pointer
  if (size == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Input size cannot be null"));

  // Check if name is attribute or dimension
  const auto& array_schema = array_->array_schema_latest();
  bool is_dim = array_schema.is_dim(name);
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return logger_->status(Status_SubarrayError(
        std::string("Cannot get max memory size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if the attribute/dimension is fixed-sized
  if (name != constants::coords && array_schema.var_size(name))
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Attribute/Dimension must be fixed-sized"));

  // Check if attribute/dimension is nullable
  if (array_schema.is_nullable(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
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
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Attribute/Dimension cannot be null"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Input sizes cannot be null"));

  // Check if name is attribute or dimension
  const auto& array_schema = array_->array_schema_latest();
  bool is_dim = array_schema.is_dim(name);
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return logger_->status(Status_SubarrayError(
        std::string("Cannot get max memory size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if the attribute/dimension is var-sized
  if (!array_schema.var_size(name))
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Attribute/Dimension must be var-sized"));

  // Check if attribute/dimension is nullable
  if (array_schema.is_nullable(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
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
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Attribute cannot be null"));

  // Check size pointer
  if (size == nullptr || size_validity == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Input sizes cannot be null"));

  // Check if name is attribute
  const auto& array_schema = array_->array_schema_latest();
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return logger_->status(Status_SubarrayError(
        std::string("Cannot get max memory size; Attribute '") + name +
        "' does not exist"));

  // Check if the attribute is fixed-sized
  if (array_schema.var_size(name))
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Attribute must be fixed-sized"));

  // Check if attribute is nullable
  if (!array_schema.is_nullable(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
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
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Attribute/Dimension cannot be null"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr || size_validity == nullptr)
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Input sizes cannot be null"));

  // Check if name is attribute or dimension
  const auto& array_schema = array_->array_schema_latest();
  bool is_attr = array_schema.is_attr(name);

  // Check if attribute exists
  if (!is_attr)
    return logger_->status(Status_SubarrayError(
        std::string("Cannot get max memory size; Attribute '") + name +
        "' does not exist"));

  // Check if the attribute is var-sized
  if (!array_schema.var_size(name))
    return logger_->status(Status_SubarrayError(
        "Cannot get max memory size; Attribute/Dimension must be var-sized"));

  // Check if attribute is nullable
  if (!array_schema.is_nullable(name))
    return logger_->status(
        Status_SubarrayError("Cannot get estimated result size; "
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

void Subarray::get_next_range_coords(
    std::vector<uint64_t>* range_coords) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  auto layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;

  if (layout == Layout::ROW_MAJOR) {
    auto d = dim_num - 1;
    ++(*range_coords)[d];
    while ((*range_coords)[d] >= range_subset_[d].num_ranges() && d != 0) {
      (*range_coords)[d] = 0;
      --d;
      ++(*range_coords)[d];
    }
  } else if (layout == Layout::COL_MAJOR) {
    auto d = (unsigned)0;
    ++(*range_coords)[d];
    while ((*range_coords)[d] >= range_subset_[d].num_ranges() &&
           d != dim_num - 1) {
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

template <class T>
void Subarray::get_original_range_coords(
    const T* const range_coords,
    std::vector<uint64_t>* original_range_coords) const {
  auto dim_num = this->dim_num();
  for (unsigned i = 0; i < dim_num; ++i)
    original_range_coords->at(i) = original_range_idx_[i][range_coords[i]];
}

uint64_t Subarray::range_num() const {
  if (range_subset_.empty())
    return 0;

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

const std::vector<Range>& Subarray::ranges_for_dim(uint32_t dim_idx) const {
  return range_subset_[dim_idx].ranges();
}

Status Subarray::set_ranges_for_dim(
    uint32_t dim_idx, const std::vector<Range>& ranges) {
  auto dim = array_->array_schema_latest().dimension(dim_idx);
  range_subset_[dim_idx] =
      RangeSetAndSuperset(dim->type(), dim->domain(), false, coalesce_ranges_);
  is_default_[dim_idx] = false;
  // Add each range individually so that contiguous
  // ranges may be coalesced.
  for (const auto& range : ranges)
    range_subset_[dim_idx].add_range_unrestricted(range);
  is_default_[dim_idx] = range_subset_[dim_idx].is_implicitly_initialized();
  return Status::Ok();
}

Status Subarray::split(
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
      auto dim = array_->array_schema_latest().dimension(d);
      dim->split_range(r, splitting_value, &sr1, &sr2);
      RETURN_NOT_OK(r1->add_range_unsafe(d, sr1));
      RETURN_NOT_OK(r2->add_range_unsafe(d, sr2));
    } else {
      if (!range_subset_[d].is_implicitly_initialized()) {
        RETURN_NOT_OK(r1->add_range_unsafe(d, r));
        RETURN_NOT_OK(r2->add_range_unsafe(d, r));
      }
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
  *r1 = Subarray(array_, layout_, stats_->parent(), logger_, coalesce_ranges_);
  *r2 = Subarray(array_, layout_, stats_->parent(), logger_, coalesce_ranges_);

  // For easy reference
  const auto& array_schema = array_->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  uint64_t range_num;
  Range sr1, sr2;

  for (unsigned d = 0; d < dim_num; ++d) {
    RETURN_NOT_OK(this->get_range_num(d, &range_num));
    if (d != splitting_dim) {
      if (!range_subset_[d].is_implicitly_initialized()) {
        for (uint64_t j = 0; j < range_num; ++j) {
          const auto& r = range_subset_[d][j];
          RETURN_NOT_OK(r1->add_range_unsafe(d, r));
          RETURN_NOT_OK(r2->add_range_unsafe(d, r));
        }
      }
    } else {                                // d == splitting_dim
      if (splitting_range != UINT64_MAX) {  // Need to split multiple ranges
        for (uint64_t j = 0; j <= splitting_range; ++j) {
          const auto& r = range_subset_[d][j];
          RETURN_NOT_OK(r1->add_range_unsafe(d, r));
        }
        for (uint64_t j = splitting_range + 1; j < range_num; ++j) {
          const auto& r = range_subset_[d][j];
          RETURN_NOT_OK(r2->add_range_unsafe(d, r));
        }
      } else {  // Need to split a single range
        const auto& r = range_subset_[d][0];
        auto dim = array_schema.dimension(d);
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

  if (array_->array_schema_latest().tile_order() == Layout::ROW_MAJOR)
    return compute_tile_coords_row<T>();
  return compute_tile_coords_col<T>();
}

template <class T>
const T* Subarray::tile_coords_ptr(
    const std::vector<T>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const {
  auto dim_num = array_->array_schema_latest().dim_num();
  auto coord_size = array_->array_schema_latest().dimension(0)->coord_size();
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
  const auto& array_schema = array_->array_schema_latest();
  auto fragment_metadata = array_->fragment_metadata();
  auto dim_num = array_->array_schema_latest().dim_num();
  auto layout =
      (layout_ == Layout::UNORDERED) ?
          ((cell_order_ == Layout::HILBERT) ? Layout::ROW_MAJOR : cell_order_) :
          layout_;

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
    var_sizes.push_back(array_schema.var_size(name));
    nullable.push_back(array_schema.is_nullable(name));
  }

  auto all_dims_same_type = array_schema.domain()->all_dims_same_type();
  auto all_dims_fixed = array_schema.domain()->all_dims_fixed();
  auto num_threads = compute_tp->concurrency_level();
  auto ranges_per_thread = (uint64_t)std::ceil((double)range_num / num_threads);
  auto status = parallel_for(compute_tp, 0, num_threads, [&](uint64_t t) {
    auto r_start = range_start + t * ranges_per_thread;
    auto r_end =
        std::min(range_start + (t + 1) * ranges_per_thread - 1, range_end);
    auto r_coords = get_range_coords(r_start);
    for (uint64_t r = r_start; r <= r_end; ++r) {
      RETURN_NOT_OK(compute_relevant_fragment_est_result_sizes(
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
  RETURN_NOT_OK(status);

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
            auto&& [st, tile_var_size] =
                meta->tile_var_size(names[i], ft.second);
            RETURN_NOT_OK(st);
            mem_vec[i].size_fixed_ += tile_size;
            mem_vec[i].size_var_ += *tile_var_size;
            if (nullable[i])
              mem_vec[i].size_validity_ +=
                  *tile_var_size / cell_size * constants::cell_validity_size;
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

Status Subarray::sort_ranges(ThreadPool* const compute_tp) {
  std::scoped_lock<std::mutex> lock(ranges_sort_mtx_);
  if (ranges_sorted_)
    return Status::Ok();

  auto timer = stats_->start_timer("sort_ranges");
  auto st = parallel_for(
      compute_tp,
      0,
      array_->array_schema_latest().dim_num(),
      [&](uint64_t dim_idx) {
        return range_subset_[dim_idx].sort_ranges(compute_tp);
      });

  RETURN_NOT_OK(st);
  ranges_sorted_ = true;

  return Status::Ok();
}

tuple<Status, optional<bool>> Subarray::non_overlapping_ranges(
    ThreadPool* const compute_tp) {
  RETURN_NOT_OK_TUPLE(sort_ranges(compute_tp), nullopt);

  std::atomic<bool> non_overlapping_ranges = true;
  auto st = parallel_for(
      compute_tp,
      0,
      array_->array_schema_latest().dim_num(),
      [&](uint64_t dim_idx) {
        auto&& [status, nor]{non_overlapping_ranges_for_dim(dim_idx)};
        non_overlapping_ranges = *nor;

        return status;
      });
  RETURN_NOT_OK_TUPLE(st, nullopt);

  return {Status::Ok(), non_overlapping_ranges};
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Subarray::add_default_ranges() {
  auto domain = array_->array_schema_latest().domain();
  auto dim_num = array_->array_schema_latest().dim_num();

  range_subset_.clear();
  for (unsigned dim_index = 0; dim_index < dim_num; ++dim_index) {
    auto dim = domain->dimension(dim_index);
    range_subset_.push_back(RangeSetAndSuperset(
        dim->type(), dim->domain(), true, coalesce_ranges_));
  }
  is_default_.resize(dim_num, true);
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
  const auto& array_schema = array_->array_schema_latest();
  auto attribute_num = array_schema.attribute_num();
  auto dim_num = array_schema.dim_num();
  auto attributes = array_schema.attributes();
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
      names[i] = array_schema.domain()->dimension(i - attribute_num)->name();
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

  return Status::Ok();
}

bool Subarray::est_result_size_computed() {
  return est_result_size_computed_;
}

Status Subarray::compute_relevant_fragment_est_result_sizes(
    const ArraySchema& array_schema,
    bool all_dims_same_type,
    bool all_dims_fixed,
    const std::vector<tdb_shared_ptr<FragmentMetadata>>& fragment_meta,
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
    for (const auto& tr : overlap->tile_ranges_) {
      for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
        for (size_t n = 0; n < names.size(); ++n) {
          // Zipped coords applicable only in homogeneous domains
          if (names[n] == constants::coords && !all_dims_same_type)
            continue;

          // If this attribute does not exist, skip it as this is likely a new
          // attribute added as a result of schema evolution
          if (!meta->array_schema()->is_field(names[n])) {
            continue;
          }

          frag_tiles->insert(std::pair<unsigned, uint64_t>(f, tid));
          auto tile_size = meta->tile_size(names[n], tid);
          auto attr_datatype_size = datatype_size(array_schema.type(names[n]));
          if (!var_sizes[n]) {
            (*result_sizes)[n].size_fixed_ += tile_size;
            if (nullable[n])
              (*result_sizes)[n].size_validity_ +=
                  tile_size / attr_datatype_size *
                  constants::cell_validity_size;
          } else {
            (*result_sizes)[n].size_fixed_ += tile_size;
            auto&& [st, tile_var_size] = meta->tile_var_size(names[n], tid);
            RETURN_NOT_OK(st);
            (*result_sizes)[n].size_var_ += *tile_var_size;
            if (nullable[n])
              (*result_sizes)[n].size_validity_ +=
                  *tile_var_size / attr_datatype_size *
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

        // If this attribute does not exist, skip it as this is likely a new
        // attribute added as a result of schema evolution
        if (!meta->array_schema()->is_field(names[n])) {
          continue;
        }

        frag_tiles->insert(std::pair<unsigned, uint64_t>(f, tid));
        auto tile_size = meta->tile_size(names[n], tid);
        auto attr_datatype_size = datatype_size(array_schema.type(names[n]));
        if (!var_sizes[n]) {
          (*result_sizes)[n].size_fixed_ += tile_size * ratio;
          if (nullable[n])
            (*result_sizes)[n].size_validity_ +=
                (tile_size / attr_datatype_size *
                 constants::cell_validity_size) *
                ratio;

        } else {
          (*result_sizes)[n].size_fixed_ += tile_size * ratio;
          auto&& [st, tile_var_size] = meta->tile_var_size(names[n], tid);
          RETURN_NOT_OK(st);
          (*result_sizes)[n].size_var_ += *tile_var_size * ratio;
          if (nullable[n])
            (*result_sizes)[n].size_validity_ +=
                (*tile_var_size / attr_datatype_size *
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
      auto dim = array_schema.dimension(d);
      cell_num = utils::math::safe_mul(
          cell_num, dim->domain_range(range_subset_[d][range_coords[d]]));
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
            utils::math::safe_mul(cell_num, array_schema.cell_size(names[n]));
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
  const auto& array_schema = array_->array_schema_latest();
  auto domain = array_schema.domain()->domain();
  auto dim_num = this->dim_num();
  uint64_t tile_start, tile_end;

  // Compute unique tile coords per dimension
  coords_set.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto tile_extent = *(const T*)array_schema.domain()->tile_extent(d).data();
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
  auto coords_size = dim_num * array_schema.dimension(0)->coord_size();
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
  const auto& array_schema = array_->array_schema_latest();
  auto domain = array_schema.domain()->domain();
  auto dim_num = this->dim_num();
  uint64_t tile_start, tile_end;

  // Compute unique tile coords per dimension
  coords_set.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto tile_extent = *(const T*)array_schema.domain()->tile_extent(d).data();
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
  auto coords_size = dim_num * array_schema.dimension(0)->coord_size();
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

Status Subarray::precompute_all_ranges_tile_overlap(
    ThreadPool* const compute_tp,
    std::vector<std::pair<uint64_t, uint64_t>>& frag_tile_idx,
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>>*
        result_tile_ranges) {
  auto timer_se = stats_->start_timer("read_compute_simple_tile_overlap");

  // For easy reference.
  const auto meta = array_->fragment_metadata();
  const auto fragment_num = meta.size();
  const auto dim_num = array_->array_schema_latest().dim_num();

  // Get the results ready.
  result_tile_ranges->clear();
  result_tile_ranges->resize(fragment_num);

  compute_range_offsets();

  // Compute relevant fragments and load rtrees.
  ComputeRelevantFragmentsCtx relevant_fragment_ctx;
  ComputeRelevantTileOverlapCtx tile_overlap_ctx;
  RETURN_NOT_OK(
      compute_relevant_fragments(compute_tp, nullptr, &relevant_fragment_ctx));
  RETURN_NOT_OK(load_relevant_fragment_rtrees(compute_tp));

  // Each thread will use one bitmap per dimensions.
  const auto num_threads = compute_tp->concurrency_level();
  BlockingResourcePool<std::vector<std::vector<uint8_t>>>
      all_threads_tile_bitmaps(num_threads);

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
                  meta[f]->compute_tile_bitmap(
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
        int64_t min = static_cast<int64_t>(frag_tile_idx[f].first);
        for (int64_t t = tile_bitmaps[0].size() - 1; t >= min; t--) {
          bool comb = true;
          for (unsigned d = 0; d < dim_num; d++) {
            comb &= (bool)tile_bitmaps[d][t];
          }

          if (!comb) {
            if (length != 0) {
              result_tile_ranges->at(f).emplace_back(end + 1 - length, end);
              length = 0;
            }

            end = t - 1;
          } else {
            length++;
          }
        }

        // Push the last result tile range.
        if (length != 0)
          result_tile_ranges->at(f).emplace_back(end + 1 - length, end);

        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  return Status::Ok();
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
  clone.tile_overlap_ = tile_overlap_;
  clone.est_result_size_computed_ = est_result_size_computed_;
  clone.coalesce_ranges_ = coalesce_ranges_;
  clone.est_result_size_ = est_result_size_;
  clone.max_mem_size_ = max_mem_size_;
  clone.relevant_fragments_ = relevant_fragments_;
  clone.original_range_idx_ = original_range_idx_;

  return clone;
}

TileOverlap Subarray::compute_tile_overlap(
    uint64_t range_idx, unsigned fid) const {
  assert(array_->array_schema_latest().dense());
  auto type = array_->array_schema_latest().dimension(0)->type();
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
  std::swap(is_default_, subarray.is_default_);
  std::swap(range_offsets_, subarray.range_offsets_);
  std::swap(tile_overlap_, subarray.tile_overlap_);
  std::swap(est_result_size_computed_, subarray.est_result_size_computed_);
  std::swap(coalesce_ranges_, subarray.coalesce_ranges_);
  std::swap(est_result_size_, subarray.est_result_size_);
  std::swap(max_mem_size_, subarray.max_mem_size_);
  std::swap(relevant_fragments_, subarray.relevant_fragments_);
  std::swap(original_range_idx_, subarray.original_range_idx_);
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
  auto range_idx_start =
      tile_overlap == nullptr ? 0 : tile_overlap->range_idx_start();
  auto range_idx_end =
      tile_overlap == nullptr ? range_num() - 1 : tile_overlap->range_idx_end();
  get_expanded_coordinates(
      range_idx_start, range_idx_end, &start_coords, &end_coords);

  // If the calibrated coordinates have not changed from
  // the last call to this function, the computed relevant
  // fragments will not change.
  if (fn_ctx->initialized_ && start_coords == fn_ctx->last_start_coords_ &&
      end_coords == fn_ctx->last_end_coords_) {
    return Status::Ok();
  }

  // Perform lazy-initialization the context cache for this routine.
  const size_t fragment_num = array_->fragment_metadata().size();
  const uint32_t dim_num = array_->array_schema_latest().dim_num();
  if (!fn_ctx->initialized_) {
    fn_ctx->initialized_ = true;

    // Create a fragment bytemap for each dimension. Each
    // non-zero byte represents an overlap between a fragment
    // and at least one range in the corresponding dimension.
    fn_ctx->frag_bytemaps_.resize(dim_num);
    for (uint32_t d = 0; d < dim_num; ++d) {
      fn_ctx->frag_bytemaps_[d].resize(fragment_num, is_default(d) ? 1 : 0);
    }
  }

  // Store the current calibrated coordinates.
  fn_ctx->last_start_coords_ = start_coords;
  fn_ctx->last_end_coords_ = end_coords;

  // Populate the fragment bytemap for each dimension in parallel.
  RETURN_NOT_OK(parallel_for(compute_tp, 0, dim_num, [&](const uint32_t d) {
    if (is_default(d))
      return Status::Ok();

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

Status Subarray::compute_relevant_fragments_for_dim(
    ThreadPool* const compute_tp,
    const uint32_t dim_idx,
    const uint64_t fragment_num,
    const std::vector<uint64_t>& start_coords,
    const std::vector<uint64_t>& end_coords,
    std::vector<uint8_t>* const frag_bytemap) const {
  const auto meta = array_->fragment_metadata();
  const Dimension* const dim = array_->array_schema_latest().dimension(dim_idx);

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
      const Range& query_range = range_subset_[dim_idx][r];

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
    tdb_shared_ptr<FragmentMetadata> meta,
    unsigned frag_idx,
    bool dense,
    ThreadPool* const compute_tp,
    SubarrayTileOverlap* const tile_overlap,
    ComputeRelevantTileOverlapCtx* const fn_ctx) {
  const auto num_threads = compute_tp->concurrency_level();
  const auto range_num = fn_ctx->range_len_;

  const auto ranges_per_thread =
      (uint64_t)std::ceil((double)range_num / num_threads);
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
        RETURN_NOT_OK(meta->get_tile_overlap(
            range, is_default_, tile_overlap->at(frag_idx, r)));
      }
    }

    return Status::Ok();
  });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Subarray::load_relevant_fragment_tile_var_sizes(
    const std::vector<std::string>& names, ThreadPool* const compute_tp) const {
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
  if (var_names.empty())
    return Status::Ok();

  // Load all metadata for tile var sizes among fragments.
  for (const auto& var_name : var_names) {
    const auto status = parallel_for(
        compute_tp, 0, relevant_fragments_.size(), [&](const size_t i) {
          auto f = relevant_fragments_[i];
          // Gracefully skip loading tile sizes for attributes added in schema
          // evolution that do not exists in this fragment
          const auto& schema = meta[f]->array_schema();
          if (!schema->is_field(var_name))
            return Status::Ok();

          return meta[f]->load_tile_var_sizes(*encryption_key, var_name);
        });

    RETURN_NOT_OK(status);
  }

  return Status::Ok();
}

const std::vector<unsigned>* Subarray::relevant_fragments() const {
  return &relevant_fragments_;
}

std::vector<unsigned>* Subarray::relevant_fragments() {
  return &relevant_fragments_;
}

stats::Stats* Subarray::stats() const {
  return stats_;
}

template <typename T>
tuple<Status, optional<bool>> Subarray::non_overlapping_ranges_for_dim(
    const uint64_t dim_idx) {
  const auto& ranges = range_subset_[dim_idx].ranges();
  const Dimension* const dim = array_->array_schema_latest().dimension(dim_idx);

  if (ranges.size() > 1) {
    for (uint64_t r = 0; r < ranges.size() - 1; r++) {
      if (dim->overlap<T>(ranges[r], ranges[r + 1]))
        return {Status::Ok(), false};
    }
  }

  return {Status::Ok(), true};
}

tuple<Status, optional<bool>> Subarray::non_overlapping_ranges_for_dim(
    const uint64_t dim_idx) {
  const Datatype& datatype =
      array_->array_schema_latest().dimension(dim_idx)->type();
  switch (datatype) {
    case Datatype::INT8:
      return non_overlapping_ranges_for_dim<int8_t>(dim_idx);
    case Datatype::UINT8:
      return non_overlapping_ranges_for_dim<uint8_t>(dim_idx);
    case Datatype::INT16:
      return non_overlapping_ranges_for_dim<int16_t>(dim_idx);
    case Datatype::UINT16:
      return non_overlapping_ranges_for_dim<uint16_t>(dim_idx);
    case Datatype::INT32:
      return non_overlapping_ranges_for_dim<int32_t>(dim_idx);
    case Datatype::UINT32:
      return non_overlapping_ranges_for_dim<uint32_t>(dim_idx);
    case Datatype::INT64:
      return non_overlapping_ranges_for_dim<int64_t>(dim_idx);
    case Datatype::UINT64:
      return non_overlapping_ranges_for_dim<uint64_t>(dim_idx);
    case Datatype::FLOAT32:
      return non_overlapping_ranges_for_dim<float>(dim_idx);
    case Datatype::FLOAT64:
      return non_overlapping_ranges_for_dim<double>(dim_idx);
    case Datatype::STRING_ASCII:
      return non_overlapping_ranges_for_dim<char>(dim_idx);
    case Datatype::CHAR:
    case Datatype::BLOB:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      return {
          LOG_STATUS(Status_SubarrayError(
              "Invalid datatype " + datatype_str(datatype) + " for sorting")),
          false};
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
      return non_overlapping_ranges_for_dim<int64_t>(dim_idx);
  }
  return {Status::Ok(), false};
  ;
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

template void Subarray::get_original_range_coords<int8_t>(
    const int8_t* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<uint8_t>(
    const uint8_t* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<int16_t>(
    const int16_t* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<uint16_t>(
    const uint16_t* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<int32_t>(
    const int32_t* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<uint32_t>(
    const uint32_t* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<int64_t>(
    const int64_t* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<uint64_t>(
    const uint64_t* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<float>(
    const float* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;
template void Subarray::get_original_range_coords<double>(
    const double* const range_coords,
    std::vector<uint64_t>* original_range_coords) const;

}  // namespace sm
}  // namespace tiledb
