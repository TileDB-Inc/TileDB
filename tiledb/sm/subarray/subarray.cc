/**
 * @file   subarray.cc
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
#include "tiledb/sm/stats/stats.h"

#include <iomanip>
#include <sstream>

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

Subarray::Subarray(const Array* array)
    : Subarray(array, Layout::UNORDERED) {
}

Subarray::Subarray(const Array* array, Layout layout)
    : array_(array)
    , layout_(layout) {
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

Status Subarray::add_range(uint32_t dim_idx, const Range& range) {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot add range to dimension; Invalid dimension index"));

  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_computed_ = false;

  // Remove the default range
  if (is_default_[dim_idx]) {
    ranges_[dim_idx].clear();
    is_default_[dim_idx] = false;
  }

  // Correctness checks
  auto dim = array_->array_schema()->dimension(dim_idx);
  RETURN_NOT_OK(dim->check_range(range));

  // Add the range
  ranges_[dim_idx].emplace_back(range);

  return Status::Ok();
}

Status Subarray::add_range_unsafe(uint32_t dim_idx, const Range& range) {
  // Must reset the result size and tile overlap
  est_result_size_computed_ = false;
  tile_overlap_computed_ = false;

  // Remove the default range
  if (is_default_[dim_idx]) {
    ranges_[dim_idx].clear();
    is_default_[dim_idx] = false;
  }

  // Add the range
  ranges_[dim_idx].emplace_back(range);

  return Status::Ok();
}

const Array* Subarray::array() const {
  return array_;
}

uint64_t Subarray::cell_num(uint64_t range_idx) const {
  uint64_t cell_num = 1, range;
  auto array_schema = array_->array_schema();
  unsigned dim_num = array_schema->dim_num();
  auto cell_order = array_->array_schema()->cell_order();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order : layout_;
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

void Subarray::clear() {
  ranges_.clear();
  range_offsets_.clear();
  tile_overlap_.clear();
  est_result_size_computed_ = false;
  tile_overlap_computed_ = false;
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
  Subarray ret(array_, layout);
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
  Subarray ret(array_, layout_);

  auto start_coords = get_range_coords(start);
  auto end_coords = get_range_coords(end);

  auto dim_num = this->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    for (uint64_t r = start_coords[d]; r <= end_coords[d]; ++r) {
      ret.add_range_unsafe(d, ranges_[d][r]);
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

bool Subarray::is_default(uint32_t dim_index) const {
  return is_default_[dim_index];
}

bool Subarray::is_set() const {
  for (const auto& d : is_default_)
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

Status Subarray::get_est_result_size(const char* name, uint64_t* size) {
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
  auto array_schema = array_->array_schema();
  bool is_dim = array_schema->is_dim(name);
  bool is_attr = array_schema->is_attr(name);

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

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size());
  *size = (uint64_t)ceil(est_result_size_[name].size_fixed_);

  return Status::Ok();
}

Status Subarray::get_est_result_size(
    const char* name, uint64_t* size_off, uint64_t* size_val) {
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
  auto array_schema = array_->array_schema();
  bool is_dim = array_schema->is_dim(name);
  bool is_attr = array_schema->is_attr(name);

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

  // Compute tile overlap for each fragment
  RETURN_NOT_OK(compute_est_result_size());
  *size_off = (uint64_t)ceil(est_result_size_[name].size_fixed_);
  *size_val = (uint64_t)ceil(est_result_size_[name].size_var_);

  return Status::Ok();
}

Status Subarray::get_max_memory_size(const char* name, uint64_t* size) {
  // Check attribute/dimension name
  if (name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Attribute/Dimension cannot be null"));

  // Check size pointer
  if (size == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get max memory size; Inpute size cannot be null"));

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

  // Compute tile overlap for each fragment
  compute_est_result_size();
  *size = (uint64_t)ceil(est_result_size_[name].mem_size_fixed_);

  return Status::Ok();
}

Status Subarray::get_max_memory_size(
    const char* name, uint64_t* size_off, uint64_t* size_val) {
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

  // Compute tile overlap for each fragment
  compute_est_result_size();
  *size_off = (uint64_t)ceil(est_result_size_[name].mem_size_fixed_);
  *size_val = (uint64_t)ceil(est_result_size_[name].mem_size_var_);

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
    ret *= r.size();

  return ret;
}

NDRange Subarray::ndrange(uint64_t range_idx) const {
  NDRange ret;
  uint64_t tmp_idx = range_idx;
  auto dim_num = this->dim_num();
  auto cell_order = array_->array_schema()->cell_order();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order : layout_;

  // Unary case or GLOBAL_ORDER
  if (range_num() == 1) {
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

const std::vector<Range>& Subarray::ranges_for_dim(uint32_t dim_idx) const {
  return ranges_[dim_idx];
}

Status Subarray::set_ranges_for_dim(
    uint32_t dim_idx, const std::vector<Range>& ranges) {
  ranges_.resize(dim_idx + 1);
  ranges_[dim_idx] = ranges;
  return Status::Ok();
}

Status Subarray::split(
    unsigned splitting_dim,
    const ByteVecValue& splitting_value,
    Subarray* r1,
    Subarray* r2) const {
  assert(r1 != nullptr);
  assert(r2 != nullptr);
  *r1 = Subarray(array_, layout_);
  *r2 = Subarray(array_, layout_);

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
  *r1 = Subarray(array_, layout_);
  *r2 = Subarray(array_, layout_);

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

const std::vector<std::vector<TileOverlap>>& Subarray::tile_overlap() const {
  return tile_overlap_;
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
  auto dim_num = array_->array_schema()->dim_num();
  auto coord_size = array_->array_schema()->dimension(0)->coord_size();
  std::memcpy(&((*aux_tile_coords)[0]), &tile_coords[0], dim_num * coord_size);
  auto it = tile_coords_map_.find(*aux_tile_coords);
  if (it == tile_coords_map_.end())
    return nullptr;
  return (const T*)&(tile_coords_[it->second][0]);
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
  auto cell_order = array_->array_schema()->cell_order();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order : layout_;

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

  stats::all_stats.start_timer(stats::Stats::StatType::COMPUTE_EST_RESULT_SIZE);

  RETURN_NOT_OK(compute_tile_overlap());

  std::mutex mtx;

  // Prepare estimated result size vector for all
  // attributes/dimension and zipped coords
  auto array_schema = array_->array_schema();
  auto attribute_num = array_schema->attribute_num();
  auto dim_num = array_schema->dim_num();
  auto attributes = array_schema->attributes();
  auto num = attribute_num + dim_num + 1;
  std::vector<ResultSize> est_result_size_vec;
  for (unsigned i = 0; i < num; ++i)
    est_result_size_vec.emplace_back(ResultSize{0.0, 0.0, 0, 0});

  // Compute estimated result in parallel over fragments and ranges
  auto meta = array_->fragment_metadata();
  auto fragment_num = meta.size();
  auto range_num = this->range_num();

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

  // Find the name of some var-sized dimension or attribute
  std::string name;
  for (unsigned i = 0; i < num; ++i) {
    bool var_size = array_schema->var_size(names[i]);
    if (var_size)
      name = names[i];
  }

  // Load in parallel all tile var sizes metadata across fragments
  auto encryption_key = array_->encryption_key();
  uint64_t size;
  if (!name.empty()) {
    auto statuses = parallel_for(0, fragment_num, [&](uint64_t f) {
      auto name = array_schema->attribute(0)->name();
      RETURN_NOT_OK(meta[f]->tile_var_size(*encryption_key, name, 0, &size));
      return Status::Ok();
    });
    for (auto st : statuses)
      RETURN_NOT_OK(st);
  }

  stats::all_stats.start_timer(stats::Stats::StatType::DBG);

  // Compute estimated result sizes in parallel across ranges
  auto statuses = parallel_for(0, range_num, [&](uint64_t r) {
    for (unsigned i = 0; i < num; ++i) {
      bool var_size = array_schema->var_size(names[i]);
      ResultSize result_size;
      RETURN_NOT_OK(compute_est_result_size(
          encryption_key,
          array_schema,
          meta,
          names[i],
          r,
          var_size,
          &result_size));
      std::lock_guard<std::mutex> block(mtx);
      est_result_size_vec[i].size_fixed_ += result_size.size_fixed_;
      est_result_size_vec[i].size_var_ += result_size.size_var_;
      est_result_size_vec[i].mem_size_fixed_ += result_size.mem_size_fixed_;
      est_result_size_vec[i].mem_size_var_ += result_size.mem_size_var_;
    }
    return Status::Ok();
  });
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  stats::all_stats.end_timer(stats::Stats::StatType::DBG);

  // Amplify result estimation
  if (constants::est_result_size_amplification != 1.0) {
    for (auto& r : est_result_size_vec) {
      r.size_fixed_ *= constants::est_result_size_amplification;
      r.size_var_ *= constants::est_result_size_amplification;
    }
  }

  // Set the estimated result size map
  est_result_size_.clear();
  for (unsigned i = 0; i < num; ++i)
    est_result_size_[names[i]] = est_result_size_vec[i];
  est_result_size_computed_ = true;

  stats::all_stats.end_timer(stats::Stats::StatType::COMPUTE_EST_RESULT_SIZE);

  return Status::Ok();
}

Status Subarray::compute_est_result_size(
    const EncryptionKey* encryption_key,
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& fragment_meta,
    const std::string& name,
    uint64_t range_idx,
    bool var_size,
    ResultSize* result_size) const {
  ResultSize ret{0.0, 0.0, 0, 0};

  // Zipped coords applicable only in homogeneous domains
  if (name == constants::coords &&
      !array_schema->domain()->all_dims_same_type()) {
    *result_size = ret;
    return Status::Ok();
  }

  // Compute estimated result
  auto fragment_num = (unsigned)fragment_meta.size();
  for (unsigned f = 0; f < fragment_num; ++f) {
    const auto& overlap = tile_overlap_[f][range_idx];
    auto meta = fragment_meta[f];

    // Parse tile ranges
    uint64_t tile_size = 0, tile_var_size = 0;
    for (const auto& tr : overlap.tile_ranges_) {
      for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
        tile_size = meta->tile_size(name, tid);
        if (!var_size) {
          ret.size_fixed_ += tile_size;
          ret.mem_size_fixed_ += tile_size;
        } else {
          ret.size_fixed_ += tile_size;
          RETURN_NOT_OK(
              meta->tile_var_size(*encryption_key, name, tid, &tile_var_size));
          ret.size_var_ += tile_var_size;
          ret.mem_size_fixed_ += tile_size;
          ret.mem_size_var_ += tile_var_size;
        }
      }
    }

    // Parse individual tiles
    for (const auto& t : overlap.tiles_) {
      auto tid = t.first;
      tile_size = meta->tile_size(name, tid);
      auto ratio = t.second;
      if (!var_size) {
        ret.size_fixed_ += tile_size * ratio;
        ret.mem_size_fixed_ += tile_size;
      } else {
        ret.size_fixed_ += tile_size * ratio;
        RETURN_NOT_OK(
            meta->tile_var_size(*encryption_key, name, tid, &tile_var_size));
        ret.size_var_ += tile_var_size * ratio;
        ret.mem_size_fixed_ += tile_size;
        ret.mem_size_var_ += tile_var_size;
      }
    }
  }

  // Calibrate result - applicable only to arrays without coordinate duplicates
  // and fixed dimensions
  auto domain = array_schema->domain();
  if (!array_schema->allows_dups() && domain->all_dims_fixed()) {
    uint64_t max_size_fixed, max_size_var = UINT64_MAX;
    auto cell_num = this->cell_num(range_idx);
    if (var_size) {
      max_size_fixed =
          utils::math::safe_mul(cell_num, constants::cell_var_offset_size);
    } else {
      max_size_fixed =
          utils::math::safe_mul(cell_num, array_schema->cell_size(name));
    }
    ret.size_fixed_ = std::min<double>(ret.size_fixed_, max_size_fixed);
    ret.size_var_ = std::min<double>(ret.size_var_, max_size_var);
  }

  *result_size = ret;

  return Status::Ok();
}

template <class T>
void Subarray::compute_tile_coords_col() {
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
      tile_start = (r[0] - dim_dom[0]) / tile_extent;
      tile_end = (r[1] - dim_dom[0]) / tile_extent;
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
}

template <class T>
void Subarray::compute_tile_coords_row() {
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
      tile_start = (r[0] - dim_dom[0]) / tile_extent;
      tile_end = (r[1] - dim_dom[0]) / tile_extent;
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
}

Status Subarray::compute_tile_overlap() {
  if (tile_overlap_computed_)
    return Status::Ok();

  stats::all_stats.start_timer(stats::Stats::StatType::COMPUTE_TILE_OVERLAP);

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
        if (meta[i]->dense()) {  // Dense fragment
          tile_overlap_[i][j] = get_tile_overlap(j, i);
        } else {  // Sparse fragment
          const auto& range = this->ndrange(j);
          RETURN_NOT_OK(meta[i]->get_tile_overlap(
              *encryption_key, range, &(tile_overlap_[i][j])));
        }
        return Status::Ok();
      });
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  tile_overlap_computed_ = true;

  stats::all_stats.end_timer(stats::Stats::StatType::COMPUTE_TILE_OVERLAP);

  return Status::Ok();
}

Subarray Subarray::clone() const {
  Subarray clone;
  clone.array_ = array_;
  clone.layout_ = layout_;
  clone.ranges_ = ranges_;
  clone.is_default_ = is_default_;
  clone.range_offsets_ = range_offsets_;
  clone.tile_overlap_ = tile_overlap_;
  clone.est_result_size_computed_ = est_result_size_computed_;
  clone.tile_overlap_computed_ = tile_overlap_computed_;
  clone.est_result_size_ = est_result_size_;

  return clone;
}

TileOverlap Subarray::get_tile_overlap(uint64_t range_idx, unsigned fid) const {
  assert(array_->array_schema()->dense());
  auto type = array_->array_schema()->dimension(0)->type();
  switch (type) {
    case Datatype::INT8:
      return get_tile_overlap<int8_t>(range_idx, fid);
    case Datatype::UINT8:
      return get_tile_overlap<uint8_t>(range_idx, fid);
    case Datatype::INT16:
      return get_tile_overlap<int16_t>(range_idx, fid);
    case Datatype::UINT16:
      return get_tile_overlap<uint16_t>(range_idx, fid);
    case Datatype::INT32:
      return get_tile_overlap<int32_t>(range_idx, fid);
    case Datatype::UINT32:
      return get_tile_overlap<uint32_t>(range_idx, fid);
    case Datatype::INT64:
      return get_tile_overlap<int64_t>(range_idx, fid);
    case Datatype::UINT64:
      return get_tile_overlap<uint64_t>(range_idx, fid);
    case Datatype::FLOAT32:
      return get_tile_overlap<float>(range_idx, fid);
    case Datatype::FLOAT64:
      return get_tile_overlap<double>(range_idx, fid);
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
      return get_tile_overlap<int64_t>(range_idx, fid);
    default:
      assert(false);
  }
  return TileOverlap();
}

template <class T>
TileOverlap Subarray::get_tile_overlap(uint64_t range_idx, unsigned fid) const {
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
  std::swap(array_, subarray.array_);
  std::swap(layout_, subarray.layout_);
  std::swap(ranges_, subarray.ranges_);
  std::swap(is_default_, subarray.is_default_);
  std::swap(range_offsets_, subarray.range_offsets_);
  std::swap(tile_overlap_, subarray.tile_overlap_);
  std::swap(est_result_size_computed_, subarray.est_result_size_computed_);
  std::swap(tile_overlap_computed_, subarray.tile_overlap_computed_);
  std::swap(est_result_size_, subarray.est_result_size_);
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

}  // namespace sm
}  // namespace tiledb
