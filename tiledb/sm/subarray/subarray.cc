/**
 * @file   subarray.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/rtree/rtree.h"

#include <iomanip>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Subarray::Subarray() {
  array_ = nullptr;
  layout_ = Layout::UNORDERED;
  est_result_size_computed_ = false;
  tile_overlap_computed_ = false;
}

Subarray::Subarray(const Array* array, Layout layout)
    : array_(array)
    , layout_(layout) {
  auto dim_num = array->array_schema()->dim_num();
  auto domain_type = array->array_schema()->domain()->type();
  for (uint32_t i = 0; i < dim_num; ++i)
    ranges_.emplace_back(domain_type);
  est_result_size_computed_ = false;
  tile_overlap_computed_ = false;
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
    uint32_t dim_idx, const void* range, bool check_expanded_domain) {
  if (range == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot add range to dimension; Range cannot be null"));

  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot add range to dimension; Invalid dimension index"));

  auto type = array_->array_schema()->domain()->type();
  switch (type) {
    case Datatype::INT8:
      return add_range<int8_t>(
          dim_idx, (const int8_t*)range, check_expanded_domain);
    case Datatype::UINT8:
      return add_range<uint8_t>(
          dim_idx, (const uint8_t*)range, check_expanded_domain);
    case Datatype::INT16:
      return add_range<int16_t>(
          dim_idx, (const int16_t*)range, check_expanded_domain);
    case Datatype::UINT16:
      return add_range<uint16_t>(
          dim_idx, (const uint16_t*)range, check_expanded_domain);
    case Datatype::INT32:
      return add_range<int32_t>(
          dim_idx, (const int32_t*)range, check_expanded_domain);
    case Datatype::UINT32:
      return add_range<uint32_t>(
          dim_idx, (const uint32_t*)range, check_expanded_domain);
    case Datatype::INT64:
      return add_range<int64_t>(
          dim_idx, (const int64_t*)range, check_expanded_domain);
    case Datatype::UINT64:
      return add_range<uint64_t>(
          dim_idx, (const uint64_t*)range, check_expanded_domain);
    case Datatype::FLOAT32:
      return add_range<float>(
          dim_idx, (const float*)range, check_expanded_domain);
    case Datatype::FLOAT64:
      return add_range<double>(
          dim_idx, (const double*)range, check_expanded_domain);
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
      return add_range<int64_t>(
          dim_idx, (const int64_t*)range, check_expanded_domain);
    default:
      return LOG_STATUS(Status::SubarrayError(
          "Cannot add range to dimension; Unsupported subarray domain type"));
  }

  return Status::Ok();
}

Status Subarray::add_range(
    uint32_t dim_idx, const void* start, const void* end) {
  if (start == nullptr || end == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot add range to dimension; Range start/end cannot be null"));

  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot add range to dimension; Invalid dimension index"));

  auto type = array_->array_schema()->domain()->type();
  switch (type) {
    case Datatype::INT8:
      return add_range<int8_t>(
          dim_idx, (const int8_t*)start, (const int8_t*)end);
    case Datatype::UINT8:
      return add_range<uint8_t>(
          dim_idx, (const uint8_t*)start, (const uint8_t*)end);
    case Datatype::INT16:
      return add_range<int16_t>(
          dim_idx, (const int16_t*)start, (const int16_t*)end);
    case Datatype::UINT16:
      return add_range<uint16_t>(
          dim_idx, (const uint16_t*)start, (const uint16_t*)end);
    case Datatype::INT32:
      return add_range<int32_t>(
          dim_idx, (const int32_t*)start, (const int32_t*)end);
    case Datatype::UINT32:
      return add_range<uint32_t>(
          dim_idx, (const uint32_t*)start, (const uint32_t*)end);
    case Datatype::INT64:
      return add_range<int64_t>(
          dim_idx, (const int64_t*)start, (const int64_t*)end);
    case Datatype::UINT64:
      return add_range<uint64_t>(
          dim_idx, (const uint64_t*)start, (const uint64_t*)end);
    case Datatype::FLOAT32:
      return add_range<float>(dim_idx, (const float*)start, (const float*)end);
    case Datatype::FLOAT64:
      return add_range<double>(
          dim_idx, (const double*)start, (const double*)end);
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
      return add_range<int64_t>(
          dim_idx, (const int64_t*)start, (const int64_t*)end);
    default:
      return LOG_STATUS(Status::SubarrayError(
          "Cannot add range to dimension; Unsupported subarray domain type"));
  }

  return Status::Ok();
}

const Array* Subarray::array() const {
  return array_;
}

template <class T>
uint64_t Subarray::cell_num(uint64_t range_idx) const {
  // Special case if it unary
  if (is_unary(range_idx))
    return 1;

  // Inapplicable to non-unary real ranges
  if (!std::is_integral<T>::value)
    return UINT64_MAX;

  uint64_t ret = 1, length;
  auto range = this->range<T>(range_idx);

  for (const auto& r : range) {
    // The code below essentially computes
    // ret *= r[1] - r[0] + 1;
    // while performing overflow checks
    length = r[1] - r[0];
    if (length == UINT64_MAX)  // overflow
      return UINT64_MAX;
    ++length;
    ret = utils::math::safe_mul(length, ret);
  }

  return ret;
}

void Subarray::clear() {
  ranges_.clear();
  range_offsets_.clear();
  tile_overlap_.clear();
  est_result_size_computed_ = false;
  tile_overlap_computed_ = false;
}

Status Subarray::compute_tile_overlap() {
  auto type = array_->array_schema()->domain()->type();
  switch (type) {
    case Datatype::INT8:
      return compute_tile_overlap<int8_t>();
    case Datatype::UINT8:
      return compute_tile_overlap<uint8_t>();
    case Datatype::INT16:
      return compute_tile_overlap<int16_t>();
    case Datatype::UINT16:
      return compute_tile_overlap<uint16_t>();
    case Datatype::INT32:
      return compute_tile_overlap<int32_t>();
    case Datatype::UINT32:
      return compute_tile_overlap<uint32_t>();
    case Datatype::INT64:
      return compute_tile_overlap<int64_t>();
    case Datatype::UINT64:
      return compute_tile_overlap<uint64_t>();
    case Datatype::FLOAT32:
      return compute_tile_overlap<float>();
    case Datatype::FLOAT64:
      return compute_tile_overlap<double>();
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
      return compute_tile_overlap<int64_t>();
    default:
      return LOG_STATUS(Status::SubarrayError(
          "Failed to compute tile overlap; unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Subarray Subarray::crop_to_tile(const T* tile_coords, Layout layout) const {
  Subarray ret(array_, layout);
  const void* range;
  T new_range[2];
  bool overlaps;

  // Get tile subarray based on the input coordinates
  std::vector<T> tile_subarray(2 * dim_num());
  array_->array_schema()->domain()->get_tile_subarray(
      tile_coords, &tile_subarray[0]);

  // Compute cropped subarray
  for (unsigned d = 0; d < dim_num(); ++d) {
    for (size_t r = 0; r < ranges_[d].range_num(); ++r) {
      get_range(d, r, &range);
      utils::geometry::overlap(
          (const T*)range, &tile_subarray[2 * d], 1, new_range, &overlaps);
      if (overlaps)
        ret.add_range(d, new_range, true);
    }
  }

  return ret;
}

uint32_t Subarray::dim_num() const {
  return array_->array_schema()->dim_num();
}

const void* Subarray::domain() const {
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
    uint32_t dim_idx, uint64_t range_idx, const void** range) const {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid dimension index"));

  auto range_num = ranges_[dim_idx].range_num();
  if (range_idx >= range_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid range index"));

  *range = ranges_[dim_idx].get_range(range_idx);

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

  auto range_num = ranges_[dim_idx].range_num();
  if (range_idx >= range_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid range index"));

  *start = ranges_[dim_idx].get_range_start(range_idx);
  *end = ranges_[dim_idx].get_range_end(range_idx);

  return Status::Ok();
}

Status Subarray::get_range_num(uint32_t dim_idx, uint64_t* range_num) const {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get number of ranges for a dimension; "
                              "Invalid dimension index"));

  *range_num = ranges_[dim_idx].range_num();

  return Status::Ok();
}

template <class T>
Subarray Subarray::get_subarray(uint64_t start, uint64_t end) const {
  Subarray ret(array_, layout_);

  auto start_coords = get_range_coords(start);
  auto end_coords = get_range_coords(end);

  auto dim_num = this->dim_num();
  for (unsigned i = 0; i < dim_num; ++i) {
    for (uint64_t r = start_coords[i]; r <= end_coords[i]; ++r) {
      ret.add_range(i, ranges_[i].get_range(r), true);
    }
  }

  // Set tile overlap
  auto fragment_num = tile_overlap_.size();
  ret.tile_overlap_.resize(fragment_num);
  for (unsigned i = 0; i < fragment_num; ++i) {
    for (uint64_t r = start; r <= end; ++r) {
      ret.tile_overlap_[i].push_back(tile_overlap_[i][r]);
    }
  }

  // Compute range offsets
  ret.compute_range_offsets();

  return ret;
}

bool Subarray::is_set() const {
  for (const auto& r : ranges_)
    if (!r.has_default_range_)
      return true;
  return false;
}

bool Subarray::is_unary() const {
  if (range_num() != 1)
    return false;

  for (const auto& range : ranges_) {
    auto r = (const uint8_t*)range.get_range(0);
    auto range_size = range.range_size_;
    if (std::memcmp(r, r + range_size / 2, range_size / 2) != 0)
      return false;
  }

  return true;
}

bool Subarray::is_unary(uint64_t range_idx) const {
  auto coords = get_range_coords(range_idx);
  auto dim_num = this->dim_num();

  for (unsigned i = 0; i < dim_num; ++i) {
    auto r = (const uint8_t*)ranges_[i].get_range(coords[i]);
    auto range_size = ranges_[i].range_size_;
    if (std::memcmp(r, r + range_size / 2, range_size / 2) != 0)
      return false;
  }

  return true;
}

void Subarray::set_layout(Layout layout) {
  layout_ = layout;
}

Layout Subarray::layout() const {
  return layout_;
}

Status Subarray::get_est_result_size(const char* attr_name, uint64_t* size) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr_name != constants::coords && attr == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid attribute"));

  // Check size pointer
  if (size == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid size input"));

  // Check if the attribute is fixed-sized
  if (attr_name != constants::coords && attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Attribute must be fixed-sized"));

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size());
  *size = (uint64_t)ceil(est_result_size_[attr_name].size_fixed_);

  return Status::Ok();
}

Status Subarray::get_est_result_size(
    const char* attr_name, uint64_t* size_off, uint64_t* size_val) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid attribute"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid size input"));

  // Check if the attribute is var-sized
  if (!attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Attribute must be var-sized"));

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size());
  *size_off = (uint64_t)ceil(est_result_size_[attr_name].size_fixed_);
  *size_val = (uint64_t)ceil(est_result_size_[attr_name].size_var_);

  return Status::Ok();
}

Status Subarray::get_max_memory_size(const char* attr_name, uint64_t* size) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get max memory size; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr_name != constants::coords && attr == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get max memory size; Invalid attribute"));

  // Check size pointer
  if (size == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Invalid size input"));

  // Check if the attribute is fixed-sized
  if (attr_name != constants::coords && attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute must be fixed-sized"));

  // Compute tile overlap for each fragment
  compute_est_result_size();
  *size = (uint64_t)ceil(est_result_size_[attr_name].mem_size_fixed_);

  return Status::Ok();
}

Status Subarray::get_max_memory_size(
    const char* attr_name, uint64_t* size_off, uint64_t* size_val) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get max memory size; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get max memory size; Invalid attribute"));

  // Check size pointer
  if (size_off == nullptr || size_val == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Invalid size input"));

  // Check if the attribute is var-sized
  if (!attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute must be var-sized"));

  // Compute tile overlap for each fragment
  compute_est_result_size();
  *size_off = (uint64_t)ceil(est_result_size_[attr_name].mem_size_fixed_);
  *size_val = (uint64_t)ceil(est_result_size_[attr_name].mem_size_var_);

  return Status::Ok();
}

std::vector<uint64_t> Subarray::get_range_coords(uint64_t range_idx) const {
  std::vector<uint64_t> ret;

  uint64_t tmp_idx = range_idx;
  auto dim_num = this->dim_num();
  auto cell_order = array_->array_schema()->cell_order();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order : layout_;

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
    // Global order - single range
    assert(layout == Layout::GLOBAL_ORDER);
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
  if (ranges_.empty())
    return 0;

  uint64_t ret = 1;
  for (const auto& r : ranges_)
    ret *= r.range_num();

  return ret;
}

template <class T>
std::vector<const T*> Subarray::range(uint64_t range_idx) const {
  std::vector<const T*> ret;
  uint64_t tmp_idx = range_idx;
  auto dim_num = this->dim_num();
  auto cell_order = array_->array_schema()->cell_order();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order : layout_;

  if (layout == Layout::ROW_MAJOR) {
    for (unsigned i = 0; i < dim_num; ++i) {
      ret.push_back((T*)(ranges_[i].get_range(tmp_idx / range_offsets_[i])));
      tmp_idx %= range_offsets_[i];
    }
  } else if (layout == Layout::COL_MAJOR) {
    for (unsigned i = dim_num - 1;; --i) {
      ret.push_back((T*)(ranges_[i].get_range(tmp_idx / range_offsets_[i])));
      tmp_idx %= range_offsets_[i];
      if (i == 0)
        break;
    }
    std::reverse(ret.begin(), ret.end());
  } else {
    assert(layout == Layout::GLOBAL_ORDER);
    assert(range_num() == 1);
    for (unsigned i = 0; i < dim_num; ++i)
      ret.push_back((T*)ranges_[i].get_range(0));
  }

  return ret;
}

const Subarray::Ranges* Subarray::ranges_for_dim(uint32_t dim_idx) const {
  return &ranges_[dim_idx];
}

Status Subarray::set_ranges_for_dim(uint32_t dim_idx, const Ranges& ranges) {
  ranges_.resize(dim_idx + 1, Ranges(type()));
  ranges_[dim_idx] = ranges;
  return Status::Ok();
}

const std::vector<std::vector<uint8_t>>& Subarray::tile_coords() const {
  return tile_coords_;
}

const std::vector<std::vector<TileOverlap>>& Subarray::tile_overlap() const {
  return tile_overlap_;
}

Datatype Subarray::type() const {
  return array_->array_schema()->domain()->type();
}

template <class T>
void Subarray::compute_tile_coords() {
  if (array_->array_schema()->tile_order() == Layout::ROW_MAJOR)
    compute_tile_coords_row<T>();
  else
    compute_tile_coords_col<T>();
}

template <class T>
const T* Subarray::tile_coords_ptr(
    const std::vector<T>& tile_coords,
    std::vector<uint8_t>* aux_tile_coords) const {
  auto coords_size = array_->array_schema()->coords_size();
  std::memcpy(&((*aux_tile_coords)[0]), &tile_coords[0], coords_size);
  auto it = tile_coords_map_.find(*aux_tile_coords);
  if (it == tile_coords_map_.end())
    return nullptr;
  return (const T*)&(tile_coords_[it->second][0]);
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Subarray::add_default_ranges() {
  auto dim_num = array_->array_schema()->dim_num();
  auto domain = (unsigned char*)array_->array_schema()->domain()->domain();
  for (unsigned i = 0; i < dim_num; ++i) {
    auto range_size = ranges_[i].range_size_;
    ranges_[i].add_range(&(domain[i * range_size]), true);
  }
}

template <class T>
Status Subarray::add_range(
    uint32_t dim_idx, const T* range, bool check_expanded_domain) {
  assert(dim_idx < array_->array_schema()->dim_num());

  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_computed_ = false;

  // Check for NaN
  RETURN_NOT_OK(check_nan<T>(range));

  // Check range bounds
  if (range[0] > range[1])
    return LOG_STATUS(
        Status::SubarrayError("Cannot add range to dimension; Lower range "
                              "bound cannot be larger than the higher bound"));

  // Check range against the domain
  auto domain = array_->array_schema()->domain();
  auto dim_domain = static_cast<const T*>(domain->dimension(dim_idx)->domain());
  T low = dim_domain[0];
  T high = dim_domain[1];
  if (array_->array_schema()->dense() && check_expanded_domain) {
    auto tile_extent =
        *static_cast<const T*>(domain->dimension(dim_idx)->tile_extent());
    high =
        ((((dim_domain[1] - dim_domain[0]) / tile_extent) + 1) * tile_extent) -
        1 + dim_domain[0];
  }

  if (range[0] < low || range[1] > high)
    return LOG_STATUS(
        Status::SubarrayError("Cannot add range to dimension; Range must be in "
                              "the domain the subarray is constructed from"));

  // Add the range
  ranges_[dim_idx].add_range(range);

  return Status::Ok();
}

template <class T>
Status Subarray::add_range(uint32_t dim_idx, const T* start, const T* end) {
  assert(dim_idx < array_->array_schema()->dim_num());
  T range[] = {*start, *end};

  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_computed_ = false;

  // Check for NaN
  RETURN_NOT_OK(check_nan<T>(range));

  // Check range bounds
  if (*start > *end)
    return LOG_STATUS(
        Status::SubarrayError("Cannot add range to dimension; Range "
                              "start cannot be larger than the range end"));

  // Check range against the domain
  auto domain = (const T*)array_->array_schema()->domain()->domain();
  if (*start < domain[2 * dim_idx] || *end > domain[2 * dim_idx + 1])
    return LOG_STATUS(
        Status::SubarrayError("Cannot add range to dimension; Range must be in "
                              "the domain the subarray is constructed from"));

  // Add the range
  ranges_[dim_idx].add_range(range);

  return Status::Ok();
}

void Subarray::compute_range_offsets() {
  range_offsets_.clear();

  auto dim_num = this->dim_num();
  auto cell_order = array_->array_schema()->cell_order();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order : layout_;

  if (layout == Layout::COL_MAJOR) {
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = 1; i < dim_num; ++i)
        range_offsets_.push_back(
            range_offsets_.back() * ranges_[i - 1].range_num());
    }
  } else if (layout == Layout::ROW_MAJOR) {
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = dim_num - 2;; --i) {
        range_offsets_.push_back(
            range_offsets_.back() * ranges_[i + 1].range_num());
        if (i == 0)
          break;
      }
    }
    std::reverse(range_offsets_.begin(), range_offsets_.end());
  } else {
    // Global order - single range
    assert(layout == Layout::GLOBAL_ORDER);
    assert(range_num() == 1);
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = 1; i < dim_num; ++i)
        range_offsets_.push_back(1);
    }
  }
}

Status Subarray::compute_est_result_size() {
  if (est_result_size_computed_)
    return Status::Ok();

  auto type = array_->array_schema()->domain()->type();
  switch (type) {
    case Datatype::INT8:
      return compute_est_result_size<int8_t>();
    case Datatype::UINT8:
      return compute_est_result_size<uint8_t>();
    case Datatype::INT16:
      return compute_est_result_size<int16_t>();
    case Datatype::UINT16:
      return compute_est_result_size<uint16_t>();
    case Datatype::INT32:
      return compute_est_result_size<int32_t>();
    case Datatype::UINT32:
      return compute_est_result_size<uint32_t>();
    case Datatype::INT64:
      return compute_est_result_size<int64_t>();
    case Datatype::UINT64:
      return compute_est_result_size<uint64_t>();
    case Datatype::FLOAT32:
      return compute_est_result_size<float>();
    case Datatype::FLOAT64:
      return compute_est_result_size<double>();
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
      return compute_est_result_size<int64_t>();
    default:
      return LOG_STATUS(Status::SubarrayError(
          "Cannot compute estimated results size; unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Subarray::compute_est_result_size() {
  if (est_result_size_computed_)
    return Status::Ok();

  RETURN_NOT_OK(compute_tile_overlap<T>());

  std::mutex mtx;

  // Prepare estimated result size vector for all attributes and coords
  auto attributes = array_->array_schema()->attributes();
  auto attribute_num = attributes.size();
  std::vector<ResultSize> est_result_size_vec;
  for (unsigned i = 0; i < attribute_num + 1; ++i)
    est_result_size_vec.emplace_back(ResultSize{0.0, 0.0, 0, 0});

  // Compute estimated result in parallel over fragments and ranges
  auto meta = array_->fragment_metadata();
  auto range_num = this->range_num();

  auto statuses = parallel_for(0, range_num, [&](uint64_t i) {
    for (unsigned a = 0; a < attribute_num + 1; ++a) {
      auto attr_name =
          (a == attribute_num) ? constants::coords : attributes[a]->name();
      bool var_size = (a == attribute_num) ? false : attributes[a]->var_size();
      ResultSize result_size;
      RETURN_NOT_OK(
          compute_est_result_size<T>(attr_name, i, var_size, &result_size));
      std::lock_guard<std::mutex> block(mtx);
      est_result_size_vec[a].size_fixed_ += result_size.size_fixed_;
      est_result_size_vec[a].size_var_ += result_size.size_var_;
      est_result_size_vec[a].mem_size_fixed_ += result_size.mem_size_fixed_;
      est_result_size_vec[a].mem_size_var_ += result_size.mem_size_var_;
    }
    return Status::Ok();
  });
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Amplify result estimation
  if (constants::est_result_size_amplification != 1.0) {
    for (auto& r : est_result_size_vec) {
      r.size_fixed_ *= constants::est_result_size_amplification;
      r.size_var_ *= constants::est_result_size_amplification;
    }
  }

  // Set the estimated result size map
  est_result_size_.clear();
  for (unsigned a = 0; a < attribute_num + 1; ++a) {
    auto attr_name =
        (a == attribute_num) ? constants::coords : attributes[a]->name();
    est_result_size_[attr_name] = est_result_size_vec[a];
  }
  est_result_size_computed_ = true;

  return Status::Ok();
}

template <class T>
Status Subarray::compute_est_result_size(
    const std::string& attr_name,
    uint64_t range_idx,
    bool var_size,
    ResultSize* result_size) const {
  // For easy reference
  auto fragment_num = array_->fragment_metadata().size();
  ResultSize ret{0.0, 0.0, 0, 0};
  auto array_schema = array_->array_schema();
  auto encryption_key = array_->encryption_key();
  uint64_t size;

  // Compute estimated result
  for (unsigned f = 0; f < fragment_num; ++f) {
    const auto& overlap = tile_overlap_[f][range_idx];
    auto meta = array_->fragment_metadata()[f];

    // Parse tile ranges
    for (const auto& tr : overlap.tile_ranges_) {
      for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
        if (!var_size) {
          ret.size_fixed_ += meta->tile_size(attr_name, tid);
          ret.mem_size_fixed_ += meta->tile_size(attr_name, tid);
        } else {
          ret.size_fixed_ += meta->tile_size(attr_name, tid);
          RETURN_NOT_OK(
              meta->tile_var_size(*encryption_key, attr_name, tid, &size));
          ret.size_var_ += size;
          ret.mem_size_fixed_ += meta->tile_size(attr_name, tid);
          ret.mem_size_var_ += size;
        }
      }
    }

    // Parse individual tiles
    for (const auto& t : overlap.tiles_) {
      auto tid = t.first;
      auto ratio = t.second;
      if (!var_size) {
        ret.size_fixed_ += meta->tile_size(attr_name, tid) * ratio;
        ret.mem_size_fixed_ += meta->tile_size(attr_name, tid);
      } else {
        ret.size_fixed_ += meta->tile_size(attr_name, tid) * ratio;
        RETURN_NOT_OK(
            meta->tile_var_size(*encryption_key, attr_name, tid, &size));
        ret.size_var_ += size * ratio;
        ret.mem_size_fixed_ += meta->tile_size(attr_name, tid);
        ret.mem_size_var_ += size;
      }
    }
  }

  // Calibrate result
  uint64_t max_size_fixed, max_size_var = UINT64_MAX;
  auto cell_num = this->cell_num<T>(range_idx);
  if (var_size) {
    max_size_fixed =
        utils::math::safe_mul(cell_num, constants::cell_var_offset_size);
  } else {
    max_size_fixed =
        utils::math::safe_mul(cell_num, array_schema->cell_size(attr_name));
  }
  ret.size_fixed_ = MIN(ret.size_fixed_, max_size_fixed);
  ret.size_var_ = MIN(ret.size_var_, max_size_var);

  *result_size = ret;

  return Status::Ok();
}

template <class T>
void Subarray::compute_tile_coords_col() {
  std::vector<std::set<T>> coords_set;
  auto array_schema = array_->array_schema();
  auto domain = (const T*)array_schema->domain()->domain();
  auto dim_num = (int)this->dim_num();
  auto tile_extents = (const T*)array_schema->domain()->tile_extents();
  uint64_t tile_start, tile_end;

  // Compute unique tile coords per dimension
  coords_set.resize(dim_num);
  for (int i = 0; i < dim_num; ++i) {
    for (uint64_t j = 0; j < ranges_[i].range_num(); ++j) {
      auto r = (const T*)ranges_[i].get_range(j);
      tile_start = (r[0] - domain[2 * i]) / tile_extents[i];
      tile_end = (r[1] - domain[2 * i]) / tile_extents[i];
      for (uint64_t t = tile_start; t <= tile_end; ++t)
        coords_set[i].insert(t);
    }
  }

  // Compute `tile_coords_`
  std::vector<typename std::set<T>::iterator> iters;
  size_t tile_coords_num = 1;
  for (int i = 0; i < dim_num; ++i) {
    iters.push_back(coords_set[i].begin());
    tile_coords_num *= coords_set[i].size();
  }

  tile_coords_.resize(tile_coords_num);
  std::vector<uint8_t> coords;
  auto coords_size = array_schema->coords_size();
  coords.resize(coords_size);
  size_t coord_size = sizeof(T);
  size_t tile_coords_pos = 0;
  while (iters[dim_num - 1] != coords_set[dim_num - 1].end()) {
    for (int i = 0; i < dim_num; ++i)
      std::memcpy(&(coords[i * coord_size]), &(*iters[i]), coord_size);
    tile_coords_[tile_coords_pos++] = coords;

    // Advance the iterators
    auto d = 0;
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
  auto array_schema = array_->array_schema();
  auto domain = (const T*)array_schema->domain()->domain();
  auto dim_num = this->dim_num();
  auto tile_extents = (const T*)array_schema->domain()->tile_extents();
  uint64_t tile_start, tile_end;

  // Compute unique tile coords per dimension
  coords_set.resize(dim_num);
  for (unsigned i = 0; i < dim_num; ++i) {
    for (uint64_t j = 0; j < ranges_[i].range_num(); ++j) {
      auto r = (const T*)ranges_[i].get_range(j);
      tile_start = (r[0] - domain[2 * i]) / tile_extents[i];
      tile_end = (r[1] - domain[2 * i]) / tile_extents[i];
      for (uint64_t t = tile_start; t <= tile_end; ++t)
        coords_set[i].insert(t);
    }
  }

  // Compute `tile_coords_`
  std::vector<typename std::set<T>::iterator> iters;
  size_t tile_coords_num = 1;
  for (unsigned i = 0; i < dim_num; ++i) {
    iters.push_back(coords_set[i].begin());
    tile_coords_num *= coords_set[i].size();
  }

  tile_coords_.resize(tile_coords_num);
  std::vector<uint8_t> coords;
  auto coords_size = array_schema->coords_size();
  coords.resize(coords_size);
  size_t coord_size = sizeof(T);
  size_t tile_coords_pos = 0;
  while (iters[0] != coords_set[0].end()) {
    for (unsigned i = 0; i < dim_num; ++i)
      std::memcpy(&(coords[i * coord_size]), &(*iters[i]), coord_size);
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

template <class T>
Status Subarray::compute_tile_overlap() {
  if (tile_overlap_computed_)
    return Status::Ok();

  compute_range_offsets();
  tile_overlap_.clear();
  auto meta = array_->fragment_metadata();
  auto fragment_num = meta.size();
  tile_overlap_.resize(fragment_num);
  auto range_num = this->range_num();
  for (unsigned i = 0; i < fragment_num; ++i)
    tile_overlap_[i].resize(range_num);

  auto encryption_key = array_->encryption_key();

  // Compute estimated tile overlap in parallel over fragments and ranges
  auto statuses = parallel_for_2d(
      0, fragment_num, 0, range_num, [&](unsigned i, uint64_t j) {
        auto range = this->range<T>(j);
        if (meta[i]->dense()) {  // Dense fragment
          tile_overlap_[i][j] = get_tile_overlap<T>(range, i);
        } else {  // Sparse fragment
          RETURN_NOT_OK(meta[i]->get_tile_overlap<T>(
              *encryption_key, range, &(tile_overlap_[i][j])));
        }
        return Status::Ok();
      });
  for (const auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  tile_overlap_computed_ = true;

  return Status::Ok();
}

Subarray Subarray::clone() const {
  Subarray clone;
  clone.array_ = array_;
  clone.layout_ = layout_;
  clone.ranges_ = ranges_;
  clone.range_offsets_ = range_offsets_;
  clone.tile_overlap_ = tile_overlap_;
  clone.est_result_size_computed_ = est_result_size_computed_;
  clone.tile_overlap_computed_ = tile_overlap_computed_;
  clone.est_result_size_ = est_result_size_;

  return clone;
}

template <class T>
TileOverlap Subarray::get_tile_overlap(
    const std::vector<const T*>& range, unsigned fid) const {
  TileOverlap ret;

  // Prepare a range copy
  auto dim_num = array_->array_schema()->dim_num();
  std::vector<T> range_cpy;
  range_cpy.resize(2 * dim_num);
  for (unsigned i = 0; i < dim_num; ++i) {
    range_cpy[2 * i] = range[i][0];
    range_cpy[2 * i + 1] = range[i][1];
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
  std::swap(array_, subarray.array_);
  std::swap(layout_, subarray.layout_);
  std::swap(ranges_, subarray.ranges_);
  std::swap(range_offsets_, subarray.range_offsets_);
  std::swap(tile_overlap_, subarray.tile_overlap_);
  std::swap(est_result_size_computed_, subarray.est_result_size_computed_);
  std::swap(tile_overlap_computed_, subarray.tile_overlap_computed_);
  std::swap(est_result_size_, subarray.est_result_size_);
}

// Explicit instantiations
template Subarray Subarray::get_subarray<uint8_t>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<int8_t>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<uint16_t>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<int16_t>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<uint32_t>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<int32_t>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<uint64_t>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<int64_t>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<float>(
    uint64_t start, uint64_t end) const;
template Subarray Subarray::get_subarray<double>(
    uint64_t start, uint64_t end) const;

template uint64_t Subarray::cell_num<int8_t>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<uint8_t>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<int16_t>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<uint16_t>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<int32_t>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<uint32_t>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<int64_t>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<uint64_t>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<float>(uint64_t range_idx) const;
template uint64_t Subarray::cell_num<double>(uint64_t range_idx) const;

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

}  // namespace sm
}  // namespace tiledb
