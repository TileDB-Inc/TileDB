/**
 * @file   domain.cc
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
 * This file implements class Domain.
 */

#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/common/blank.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/math.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <iostream>
#include <limits>
#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Domain::Domain() {
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;
  dim_num_ = 0;
  cell_num_per_tile_ = 0;
}

Domain::Domain(const Domain* domain) {
  cell_num_per_tile_ = domain->cell_num_per_tile_;
  cell_order_ = domain->cell_order_;
  dim_num_ = domain->dim_num_;
  cell_order_cmp_func_ = domain->cell_order_cmp_func_;
  cell_order_cmp_func_2_ = domain->cell_order_cmp_func_2_;
  tile_order_cmp_func_ = domain->tile_order_cmp_func_;

  dimensions_.reserve(domain->dimensions_.size());
  for (const auto& dim : domain->dimensions_)
    dimensions_.emplace_back(tdb_new(Dimension, dim.get()));

  tile_order_ = domain->tile_order_;
}

Domain::Domain(Domain&& rhs)
    : cell_num_per_tile_(rhs.cell_num_per_tile_)
    , cell_order_(rhs.cell_order_)
    , dimensions_(std::move(rhs.dimensions_))
    , dim_num_(rhs.dim_num_)
    , tile_order_(rhs.tile_order_)
    , cell_order_cmp_func_(std::move(rhs.cell_order_cmp_func_))
    , cell_order_cmp_func_2_(std::move(rhs.cell_order_cmp_func_2_))
    , tile_order_cmp_func_(std::move(rhs.tile_order_cmp_func_)) {
}

Domain& Domain::operator=(Domain&& rhs) {
  cell_num_per_tile_ = rhs.cell_num_per_tile_;
  cell_order_ = rhs.cell_order_;
  dim_num_ = rhs.dim_num_;
  cell_order_cmp_func_ = std::move(rhs.cell_order_cmp_func_);
  tile_order_cmp_func_ = std::move(rhs.tile_order_cmp_func_);
  dimensions_ = std::move(rhs.dimensions_);
  tile_order_ = rhs.tile_order_;
  cell_order_cmp_func_2_ = std::move(rhs.cell_order_cmp_func_2_);

  return *this;
}

/* ********************************* */
/*                API                */
/* ********************************* */

Layout Domain::cell_order() const {
  return cell_order_;
}

Layout Domain::tile_order() const {
  return tile_order_;
}

Status Domain::add_dimension(const Dimension* dim) {
  dimensions_.emplace_back(tdb_new(Dimension, dim));
  ++dim_num_;

  return Status::Ok();
}

bool Domain::all_dims_fixed() const {
  for (const auto& dim : dimensions_) {
    if (dim->var_size())
      return false;
  }

  return true;
}

bool Domain::all_dims_int() const {
  for (const auto& dim : dimensions_) {
    if (!datatype_is_integer(dim->type()))
      return false;
  }

  return true;
}

bool Domain::all_dims_string() const {
  for (const auto& dim : dimensions_) {
    if (!datatype_is_string(dim->type()))
      return false;
  }

  return true;
}

bool Domain::all_dims_real() const {
  for (const auto& dim : dimensions_) {
    if (!datatype_is_real(dim->type()))
      return false;
  }

  return true;
}

bool Domain::all_dims_same_type() const {
  if (dim_num_ == 0)
    return true;

  auto type = dimensions_[0]->type();
  for (unsigned d = 1; d < dim_num_; ++d) {
    if (dimensions_[d]->type() != type)
      return false;
  }

  return true;
}

uint64_t Domain::cell_num_per_tile() const {
  return cell_num_per_tile_;
}

template <>
int Domain::cell_order_cmp<char>(
    const Dimension* dim, const QueryBuffer* buff, uint64_t a, uint64_t b) {
  // Must be var-sized
  assert(dim->var_size());
  (void)dim;

  auto offs = (const uint64_t*)buff->buffer_;
  auto offs_size = *(buff->buffer_size_);
  auto var_size = *(buff->buffer_var_size_);
  auto off_a = offs[a];
  auto off_b = offs[b];
  auto var_a = &((const char*)buff->buffer_var_)[off_a];
  auto var_b = &((const char*)buff->buffer_var_)[off_b];

  // If coordinates are last, the next offset is the size of the variable buffer
  // Otherwisse, it is just the next offset in the fixed buffer
  auto next_off_a = ((a + 1) * constants::cell_var_offset_size == offs_size) ?
                        var_size :
                        offs[a + 1];
  auto next_off_b = ((b + 1) * constants::cell_var_offset_size == offs_size) ?
                        var_size :
                        offs[b + 1];
  auto size_a = next_off_a - off_a;
  auto size_b = next_off_b - off_b;
  auto size = std::min(size_a, size_b);

  // Check common prefix of size `size`
  for (uint64_t i = 0; i < size; ++i) {
    if (var_a[i] < var_b[i])
      return -1;
    if (var_a[i] > var_b[i])
      return 1;
  }

  // Equal common prefix, so equal if they have the same size
  if (size_a == size_b)
    return 0;

  // Equal common prefix, so the smaller size wins
  return (size_a < size_b) ? -1 : 1;
}

template <class T>
int Domain::cell_order_cmp(
    const Dimension* dim, const QueryBuffer* buff, uint64_t a, uint64_t b) {
  // Must be fixed-sized
  assert(!dim->var_size());
  (void)dim;

  auto ca = ((const T*)buff->buffer_)[a];
  auto cb = ((const T*)buff->buffer_)[b];
  if (ca < cb)
    return -1;
  if (ca > cb)
    return 1;
  return 0;
}

int Domain::cell_order_cmp(
    unsigned dim_idx, const ResultCoords& a, const ResultCoords& b) const {
  // Handle variable-sized dimensions
  if (dimensions_[dim_idx]->var_size()) {
    auto s_a = a.coord_string(dim_idx);
    auto s_b = b.coord_string(dim_idx);

    if (s_a == s_b)
      return 0;
    if (s_a < s_b)
      return -1;
    return 1;
  }

  assert(cell_order_cmp_func_2_[dim_idx] != nullptr);
  auto coord_a = a.coord(dim_idx);
  auto coord_b = b.coord(dim_idx);
  return cell_order_cmp_func_2_[dim_idx](coord_a, coord_b);
}

template <class T>
int Domain::cell_order_cmp_2(const void* coord_a, const void* coord_b) {
  auto ca = (const T*)coord_a;
  auto cb = (const T*)coord_b;
  if (*ca < *cb)
    return -1;
  if (*ca > *cb)
    return 1;
  return 0;
}

int Domain::cell_order_cmp(
    const std::vector<const QueryBuffer*>& coord_buffs,
    uint64_t a,
    uint64_t b) const {
  if (cell_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto dim = dimension(d);
      auto res = cell_order_cmp_func_[d](dim, coord_buffs[d], a, b);

      if (res == 1 || res == -1)
        return res;
      // else same tile on dimension d --> continue
    }
  } else {  // COL_MAJOR
    for (unsigned d = dim_num_ - 1;; --d) {
      auto dim = dimension(d);
      auto res = cell_order_cmp_func_[d](dim, coord_buffs[d], a, b);

      if (res == 1 || res == -1)
        return res;
      // else same tile on dimension d --> continue

      if (d == 0)
        break;
    }
  }

  return 0;
}

void Domain::crop_ndrange(NDRange* ndrange) const {
  for (unsigned d = 0; d < dim_num_; ++d)
    dimensions_[d]->crop_range(&(*ndrange)[d]);
}

Status Domain::deserialize(ConstBuffer* buff, uint32_t version) {
  // Load type
  Datatype type = Datatype::INT32;
  if (version < 5) {
    uint8_t type_c;
    RETURN_NOT_OK(buff->read(&type_c, sizeof(uint8_t)));
    type = static_cast<Datatype>(type_c);
  }

  // Load dimensions
  RETURN_NOT_OK(buff->read(&dim_num_, sizeof(uint32_t)));
  for (uint32_t i = 0; i < dim_num_; ++i) {
    auto&& [st_dim, dim]{Dimension::deserialize(buff, version, type)};
    if (!st_dim.ok()) {
      return Status_DomainError("Cannot deserialize dimension.");
    }
    dimensions_.emplace_back(tdb_new(Dimension, dim.value().get()));
  }

  set_tile_cell_order_cmp_funcs();

  return Status::Ok();
}

unsigned int Domain::dim_num() const {
  return dim_num_;
}

const Range& Domain::domain(unsigned i) const {
  assert(i < dim_num_);
  return dimensions_[i]->domain();
}

NDRange Domain::domain() const {
  NDRange ret(dim_num_);
  for (unsigned d = 0; d < dim_num_; ++d)
    ret[d] = dimensions_[d]->domain();

  return ret;
}

const Dimension* Domain::dimension(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;
  return dimensions_[i].get();
}

const Dimension* Domain::dimension(const std::string& name) const {
  for (unsigned int i = 0; i < dim_num_; i++) {
    const auto& dim = dimensions_[i];
    if (dim->name() == name) {
      return dim.get();
    }
  }
  return nullptr;
}

void Domain::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  for (const auto& dim : dimensions_) {
    fprintf(out, "\n");
    dim->dump(out);
  }
}

void Domain::expand_ndrange(const NDRange& r1, NDRange* r2) const {
  assert(r2 != nullptr);

  // Assign r1 to r2 if r2 is empty
  if (r2->empty()) {
    *r2 = r1;
    return;
  }

  // Expand r2 along all dimensions
  for (unsigned d = 0; d < dim_num_; ++d) {
    const auto& dim = dimensions_[d];
    if (!dim->var_size())
      dim->expand_range(r1[d], &(*r2)[d]);
    else
      dim->expand_range_var(r1[d], &(*r2)[d]);
  }
}

void Domain::expand_to_tiles(NDRange* ndrange) const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    const auto& dim = dimensions_[d];
    // Applicable only to fixed-sized dimensions
    if (!dim->var_size())
      dimensions_[d]->expand_to_tile(&(*ndrange)[d]);
  }
}

template <class T>
void Domain::get_tile_coords(const T* coords, T* tile_coords) const {
  for (unsigned d = 0; d < dim_num_; d++) {
    auto tile_extent = *(const T*)this->tile_extent(d).data();
    auto dim_dom = (const T*)domain(d).data();
    tile_coords[d] = Dimension::tile_idx(coords[d], dim_dom[0], tile_extent);
  }
}

template <class T>
void Domain::get_end_of_cell_slab(
    T* subarray, T* start, Layout layout, T* end) const {
  if (layout == Layout::GLOBAL_ORDER || layout == cell_order_) {
    auto dim_dom = (const T*)domain(dim_num_ - 1).data();
    auto tile_extent = *(const T*)this->tile_extent(dim_num_ - 1).data();

    if (cell_order_ == Layout::ROW_MAJOR) {
      for (unsigned d = 0; d < dim_num_; ++d)
        end[d] = start[d];
      auto tile_idx =
          Dimension::tile_idx(start[dim_num_ - 1], dim_dom[0], tile_extent);
      end[dim_num_ - 1] =
          Dimension::tile_coord_low(tile_idx + 1, dim_dom[0], tile_extent) - 1;
      end[dim_num_ - 1] =
          std::min(end[dim_num_ - 1], subarray[2 * (dim_num_ - 1) + 1]);
    } else {
      auto dim_dom = (const T*)domain(0).data();
      auto tile_extent = *(const T*)this->tile_extent(0).data();
      for (unsigned d = 0; d < dim_num_; ++d)
        end[d] = start[d];
      auto tile_idx = Dimension::tile_idx(start[0], dim_dom[0], tile_extent);
      end[0] = Dimension::tile_coord_high(tile_idx, dim_dom[0], tile_extent);
      end[0] = std::min(end[0], subarray[1]);
    }
  } else {
    for (unsigned d = 0; d < dim_num_; ++d)
      end[d] = start[d];
    (void)subarray;
  }
}

template <class T>
void Domain::get_next_tile_coords(const T* domain, T* tile_coords) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    get_next_tile_coords_row(domain, tile_coords);
  else if (tile_order_ == Layout::COL_MAJOR)
    get_next_tile_coords_col(domain, tile_coords);
  else  // Sanity check
    assert(0);
}

template <class T>
void Domain::get_next_tile_coords(
    const T* domain, T* tile_coords, bool* in) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    get_next_tile_coords_row(domain, tile_coords, in);
  else if (tile_order_ == Layout::COL_MAJOR)
    get_next_tile_coords_col(domain, tile_coords, in);
  else  // Sanity check
    assert(0);
}

template <class T>
void Domain::get_tile_domain(const T* subarray, T* tile_subarray) const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto dim_dom = (const T*)domain(d).data();
    auto tile_extent = *(const T*)this->tile_extent(d).data();
    tile_subarray[2 * d] =
        Dimension::tile_idx(subarray[2 * d], dim_dom[0], tile_extent);
    tile_subarray[2 * d + 1] =
        Dimension::tile_idx(subarray[2 * d + 1], dim_dom[0], tile_extent);
  }
}

template <class T>
uint64_t Domain::get_tile_pos(const T* domain, const T* tile_coords) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return get_tile_pos_row(domain, tile_coords);
  // COL_MAJOR
  return get_tile_pos_col(domain, tile_coords);
}

template <class T>
void Domain::get_tile_subarray(const T* tile_coords, T* tile_subarray) const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto dim_dom = (const T*)domain(d).data();
    auto tile_extent = *(const T*)this->tile_extent(d).data();
    tile_subarray[2 * d] =
        Dimension::tile_coord_low(tile_coords[d], dim_dom[0], tile_extent);
    tile_subarray[2 * d + 1] =
        Dimension::tile_coord_high(tile_coords[d], dim_dom[0], tile_extent);
  }
}

template <class T>
void Domain::get_tile_subarray(
    const T* domain, const T* tile_coords, T* tile_subarray) const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto tile_extent = *(const T*)this->tile_extent(d).data();
    tile_subarray[2 * d] =
        Dimension::tile_coord_low(tile_coords[d], domain[2 * d], tile_extent);
    tile_subarray[2 * d + 1] =
        Dimension::tile_coord_high(tile_coords[d], domain[2 * d], tile_extent);
  }
}

Status Domain::has_dimension(const std::string& name, bool* has_dim) const {
  *has_dim = false;

  for (const auto& dim : dimensions_) {
    if (name == dim->name()) {
      *has_dim = true;
      break;
    }
  }

  return Status::Ok();
}

Status Domain::get_dimension_index(
    const std::string& name, unsigned* dim_idx) const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    if (dimensions_[d]->name() == name) {
      *dim_idx = d;
      return Status::Ok();
    }
  }

  return Status_DomainError(
      "Cannot get dimension index; Invalid dimension name");
}

Status Domain::init(Layout cell_order, Layout tile_order) {
  // Set cell and tile order
  cell_order_ = cell_order;
  tile_order_ = tile_order;

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute the tile/cell order cmp functions
  set_tile_cell_order_cmp_funcs();

  // Set tile_extent to empty if cell order is HILBERT
  if (cell_order_ == Layout::HILBERT) {
    ByteVecValue be;
    for (auto& d : dimensions_) {
      RETURN_NOT_OK(d->set_tile_extent(be));
    }
  }

  return Status::Ok();
}

bool Domain::null_tile_extents() const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    if (!tile_extent(d))
      return true;
  }

  return false;
}

// ===== FORMAT =====
// dim_num (uint32_t)
// dimension #1
// dimension #2
// ...
Status Domain::serialize(Buffer* buff, uint32_t version) {
  // Write dimensions
  RETURN_NOT_OK(buff->write(&dim_num_, sizeof(uint32_t)));
  for (const auto& dim : dimensions_)
    RETURN_NOT_OK(dim->serialize(buff, version));

  return Status::Ok();
}

Status Domain::set_null_tile_extents_to_range() {
  for (auto& d : dimensions_)
    RETURN_NOT_OK(d->set_null_tile_extent_to_range());
  return Status::Ok();
}

template <class T>
uint64_t Domain::stride(Layout subarray_layout) const {
  if (dim_num_ == 1 || subarray_layout == Layout::GLOBAL_ORDER ||
      subarray_layout == cell_order_)
    return UINT64_MAX;

  uint64_t ret = 1;
  if (cell_order_ == Layout::ROW_MAJOR) {
    for (unsigned i = 1; i < dim_num_; ++i)
      ret =
          Dimension::tile_extent_mult<T>(ret, *(const T*)tile_extent(i).data());
  } else {  // COL_MAJOR
    for (unsigned i = 0; i < dim_num_ - 1; ++i)
      ret =
          Dimension::tile_extent_mult<T>(ret, *(const T*)tile_extent(i).data());
  }

  return ret;
}

const ByteVecValue& Domain::tile_extent(unsigned i) const {
  assert(i < dim_num_);
  return dimensions_[i]->tile_extent();
}

std::vector<ByteVecValue> Domain::tile_extents() const {
  std::vector<ByteVecValue> ret(dim_num_);
  for (unsigned d = 0; d < dim_num_; ++d)
    ret[d] = tile_extent(d);

  return ret;
}

uint64_t Domain::tile_num(const NDRange& ndrange) const {
  uint64_t ret = 1;
  for (unsigned d = 0; d < dim_num_; ++d)
    ret *= dimensions_[d]->tile_num(ndrange[d]);

  return ret;
}

uint64_t Domain::cell_num(const NDRange& ndrange) const {
  assert(!ndrange.empty());
  uint64_t cell_num = 1, range;
  for (unsigned d = 0; d < dim_num_; ++d) {
    range = dimensions_[d]->domain_range(ndrange[d]);
    if (range == std::numeric_limits<uint64_t>::max())  // Overflow
      return range;

    cell_num = utils::math::safe_mul(range, cell_num);
    if (cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
      return cell_num;
  }

  return cell_num;
}

bool Domain::covered(const NDRange& r1, const NDRange& r2) const {
  assert(r1.size() == dim_num_);
  assert(r2.size() == dim_num_);

  for (unsigned d = 0; d < dim_num_; ++d) {
    if (!dimensions_[d]->covered(r1[d], r2[d]))
      return false;
  }

  return true;
}

bool Domain::overlap(const NDRange& r1, const NDRange& r2) const {
  assert(r1.size() == dim_num_);
  assert(r2.size() == dim_num_);

  for (unsigned d = 0; d < dim_num_; ++d) {
    if (!dimensions_[d]->overlap(r1[d], r2[d]))
      return false;
  }

  return true;
}

double Domain::overlap_ratio(
    const NDRange& r1,
    const std::vector<bool>& r1_default,
    const NDRange& r2) const {
  double ratio = 1.0;
  assert(dim_num_ == r1.size());
  assert(dim_num_ == r2.size());

  for (unsigned d = 0; d < dim_num_; ++d) {
    if (r1_default[d])
      continue;

    if (!dimensions_[d]->overlap(r1[d], r2[d]))
      return 0.0;

    ratio *= dimensions_[d]->overlap_ratio(r1[d], r2[d]);

    // If ratio goes to 0, then the subarray overlap is much smaller than the
    // volume of the MBR. Since we have already guaranteed that there is an
    // overlap above, we should set the ratio to epsilon.
    auto max = std::numeric_limits<double>::max();
    if (ratio == 0)
      ratio = std::nextafter(0, max);
  }

  return ratio;
}

template <class T>
int Domain::tile_order_cmp(
    const Dimension* dim, const void* coord_a, const void* coord_b) {
  if (!dim->tile_extent())
    return 0;

  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto ca = (const T*)coord_a;
  auto cb = (const T*)coord_b;
  auto domain = (const T*)dim->domain().data();
  uint64_t ta = Dimension::tile_idx(*ca, domain[0], tile_extent);
  uint64_t tb = Dimension::tile_idx(*cb, domain[0], tile_extent);
  if (ta < tb)
    return -1;
  if (ta > tb)
    return 1;
  return 0;
}

int Domain::tile_order_cmp(
    const std::vector<const QueryBuffer*>& coord_buffs,
    uint64_t a,
    uint64_t b) const {
  if (tile_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto dim = dimension(d);

      // Inapplicable to var-sized dimensions or absent tile extents
      if (dim->var_size() || !dim->tile_extent())
        continue;

      auto coord_size = dim->coord_size();
      auto ca = &(((unsigned char*)coord_buffs[d]->buffer_)[a * coord_size]);
      auto cb = &(((unsigned char*)coord_buffs[d]->buffer_)[b * coord_size]);
      auto res = tile_order_cmp_func_[d](dim, ca, cb);

      if (res == 1 || res == -1)
        return res;
      // else same tile on dimension d --> continue
    }
  } else {  // COL_MAJOR
    for (unsigned d = dim_num_ - 1;; --d) {
      auto dim = dimension(d);

      // Inapplicable to var-sized dimensions or absent tile extents
      if (!dim->var_size() && dim->tile_extent()) {
        auto coord_size = dim->coord_size();
        auto ca = &(((unsigned char*)coord_buffs[d]->buffer_)[a * coord_size]);
        auto cb = &(((unsigned char*)coord_buffs[d]->buffer_)[b * coord_size]);
        auto res = tile_order_cmp_func_[d](dim, ca, cb);

        if (res == 1 || res == -1)
          return res;
        // else same tile on dimension d --> continue
      }

      if (d == 0)
        break;
    }
  }

  return 0;
}

int Domain::tile_order_cmp(
    unsigned dim_idx, const void* coord_a, const void* coord_b) const {
  auto dim = dimension(dim_idx);
  return tile_order_cmp_func_[dim_idx](dim, coord_a, coord_b);
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void Domain::compute_cell_num_per_tile() {
  // Applicable to dimensions that have the same type
  if (!all_dims_same_type())
    return;

  // Invoke the proper templated function
  auto type = dimensions_[0]->type();
  switch (type) {
    case Datatype::INT32:
      compute_cell_num_per_tile<int>();
      break;
    case Datatype::INT64:
      compute_cell_num_per_tile<int64_t>();
      break;
    case Datatype::INT8:
      compute_cell_num_per_tile<int8_t>();
      break;
    case Datatype::UINT8:
      compute_cell_num_per_tile<uint8_t>();
      break;
    case Datatype::INT16:
      compute_cell_num_per_tile<int16_t>();
      break;
    case Datatype::UINT16:
      compute_cell_num_per_tile<uint16_t>();
      break;
    case Datatype::UINT32:
      compute_cell_num_per_tile<uint32_t>();
      break;
    case Datatype::UINT64:
      compute_cell_num_per_tile<uint64_t>();
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
      compute_cell_num_per_tile<int64_t>();
      break;
    default:
      return;
  }
}

template <class T>
void Domain::compute_cell_num_per_tile() {
  // Applicable only to integer domains
  if (!std::is_integral<T>::value)
    return;

  // Applicable only to non-NULL space tiles
  if (null_tile_extents())
    return;

  cell_num_per_tile_ = 1;
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto tile_extent = *(const T*)this->tile_extent(d).data();
    cell_num_per_tile_ =
        Dimension::tile_extent_mult<T>(cell_num_per_tile_, tile_extent);
  }
}

void Domain::set_tile_cell_order_cmp_funcs() {
  tile_order_cmp_func_.resize(dim_num_);
  cell_order_cmp_func_.resize(dim_num_);
  cell_order_cmp_func_2_.resize(dim_num_);
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto type = dimensions_[d]->type();
    switch (type) {
      case Datatype::INT32:
        tile_order_cmp_func_[d] = tile_order_cmp<int32_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int32_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int32_t>;
        break;
      case Datatype::INT64:
        tile_order_cmp_func_[d] = tile_order_cmp<int64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int64_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int64_t>;
        break;
      case Datatype::INT8:
        tile_order_cmp_func_[d] = tile_order_cmp<int8_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int8_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int8_t>;
        break;
      case Datatype::UINT8:
        tile_order_cmp_func_[d] = tile_order_cmp<uint8_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<uint8_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<uint8_t>;
        break;
      case Datatype::INT16:
        tile_order_cmp_func_[d] = tile_order_cmp<int16_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int16_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int16_t>;
        break;
      case Datatype::UINT16:
        tile_order_cmp_func_[d] = tile_order_cmp<uint16_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<uint16_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<uint16_t>;
        break;
      case Datatype::UINT32:
        tile_order_cmp_func_[d] = tile_order_cmp<uint32_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<uint32_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<uint32_t>;
        break;
      case Datatype::UINT64:
        tile_order_cmp_func_[d] = tile_order_cmp<uint64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<uint64_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<uint64_t>;
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
        tile_order_cmp_func_[d] = tile_order_cmp<int64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int64_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int64_t>;
        break;
      case Datatype::FLOAT32:
        tile_order_cmp_func_[d] = tile_order_cmp<float>;
        cell_order_cmp_func_[d] = cell_order_cmp<float>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<float>;
        break;
      case Datatype::FLOAT64:
        tile_order_cmp_func_[d] = tile_order_cmp<double>;
        cell_order_cmp_func_[d] = cell_order_cmp<double>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<double>;
        break;
      case Datatype::STRING_ASCII:
        tile_order_cmp_func_[d] = nullptr;
        cell_order_cmp_func_[d] = cell_order_cmp<char>;
        cell_order_cmp_func_2_[d] = nullptr;
        break;
      case Datatype::BLOB:
      case Datatype::CHAR:
      case Datatype::STRING_UTF8:
      case Datatype::STRING_UTF16:
      case Datatype::STRING_UTF32:
      case Datatype::STRING_UCS2:
      case Datatype::STRING_UCS4:
      case Datatype::ANY:
        tile_order_cmp_func_[d] = nullptr;
        cell_order_cmp_func_[d] = nullptr;
        cell_order_cmp_func_2_[d] = nullptr;
    }
  }
}

template <class T>
void Domain::get_next_tile_coords_col(const T* domain, T* tile_coords) const {
  unsigned int i = 0;
  ++tile_coords[i];

  while (i < dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[++i];
  }
}

template <class T>
void Domain::get_next_tile_coords_col(
    const T* domain, T* tile_coords, bool* in) const {
  unsigned int i = 0;
  ++tile_coords[i];

  while (i < dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[++i];
  }

  *in = !(i == dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]);
}

template <class T>
void Domain::get_next_tile_coords_row(const T* domain, T* tile_coords) const {
  unsigned int i = dim_num_ - 1;
  ++tile_coords[i];

  while (i > 0 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[--i];
  }
}

template <class T>
void Domain::get_next_tile_coords_row(
    const T* domain, T* tile_coords, bool* in) const {
  unsigned int i = dim_num_ - 1;
  ++tile_coords[i];

  while (i > 0 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[--i];
  }

  *in = !(i == 0 && tile_coords[i] > domain[2 * i + 1]);
}

template <class T>
uint64_t Domain::get_tile_pos_col(const T* domain, const T* tile_coords) const {
  // Calculate tile offsets
  std::vector<uint64_t> tile_offsets;
  tile_offsets.reserve(dim_num_);
  tile_offsets.push_back(1);
  for (unsigned d = 1; d < dim_num_; ++d) {
    // Per dimension
    auto tile_extent = *(const T*)this->tile_extent(d - 1).data();
    T v = domain[2 * (d - 1) + 1] + std::is_integral<T>::value;
    auto tile_num = Dimension::tile_idx(v, domain[2 * (d - 1)], tile_extent);
    tile_offsets.push_back(tile_offsets.back() * tile_num);
  }

  // Calculate position
  uint64_t pos = 0;
  for (unsigned d = 0; d < dim_num_; ++d)
    pos += tile_coords[d] * tile_offsets[d];

  // Return
  return pos;
}

template <class T>
uint64_t Domain::get_tile_pos_row(const T* domain, const T* tile_coords) const {
  // Calculate tile offsets
  std::vector<uint64_t> tile_offsets;
  tile_offsets.reserve(dim_num_);
  tile_offsets.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned d = dim_num_ - 2;; --d) {
      // Per dimension
      auto tile_extent = *(const T*)this->tile_extent(d + 1).data();
      T v = domain[2 * (d + 1) + 1] + std::is_integral<T>::value;
      auto tile_num = Dimension::tile_idx(v, domain[2 * (d + 1)], tile_extent);
      tile_offsets.push_back(tile_offsets.back() * tile_num);
      if (d == 0)
        break;
    }
  }
  std::reverse(tile_offsets.begin(), tile_offsets.end());

  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets[i];

  // Return
  return pos;
}

template void Domain::get_next_tile_coords<int>(
    const int* domain, int* tile_coords) const;
template void Domain::get_next_tile_coords<int64_t>(
    const int64_t* domain, int64_t* tile_coords) const;
template void Domain::get_next_tile_coords<float>(
    const float* domain, float* tile_coords) const;
template void Domain::get_next_tile_coords<double>(
    const double* domain, double* tile_coords) const;
template void Domain::get_next_tile_coords<int8_t>(
    const int8_t* domain, int8_t* tile_coords) const;
template void Domain::get_next_tile_coords<uint8_t>(
    const uint8_t* domain, uint8_t* tile_coords) const;
template void Domain::get_next_tile_coords<int16_t>(
    const int16_t* domain, int16_t* tile_coords) const;
template void Domain::get_next_tile_coords<uint16_t>(
    const uint16_t* domain, uint16_t* tile_coords) const;
template void Domain::get_next_tile_coords<uint32_t>(
    const uint32_t* domain, uint32_t* tile_coords) const;
template void Domain::get_next_tile_coords<uint64_t>(
    const uint64_t* domain, uint64_t* tile_coords) const;

template void Domain::get_next_tile_coords<int>(
    const int* domain, int* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<int64_t>(
    const int64_t* domain, int64_t* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<float>(
    const float* domain, float* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<double>(
    const double* domain, double* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<int8_t>(
    const int8_t* domain, int8_t* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<uint8_t>(
    const uint8_t* domain, uint8_t* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<int16_t>(
    const int16_t* domain, int16_t* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<uint16_t>(
    const uint16_t* domain, uint16_t* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<uint32_t>(
    const uint32_t* domain, uint32_t* tile_coords, bool* in) const;
template void Domain::get_next_tile_coords<uint64_t>(
    const uint64_t* domain, uint64_t* tile_coords, bool* in) const;

template uint64_t Domain::get_tile_pos<int>(
    const int* domain, const int* tile_coords) const;
template uint64_t Domain::get_tile_pos<int64_t>(
    const int64_t* domain, const int64_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<float>(
    const float* domain, const float* tile_coords) const;
template uint64_t Domain::get_tile_pos<double>(
    const double* domain, const double* tile_coords) const;
template uint64_t Domain::get_tile_pos<int8_t>(
    const int8_t* domain, const int8_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<uint8_t>(
    const uint8_t* domain, const uint8_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<int16_t>(
    const int16_t* domain, const int16_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<uint16_t>(
    const uint16_t* domain, const uint16_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<uint32_t>(
    const uint32_t* domain, const uint32_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<uint64_t>(
    const uint64_t* domain, const uint64_t* tile_coords) const;

template void Domain::get_tile_subarray<int>(
    const int* tile_coords, int* tile_subarray) const;
template void Domain::get_tile_subarray<int64_t>(
    const int64_t* tile_coords, int64_t* tile_subarray) const;
template void Domain::get_tile_subarray<int8_t>(
    const int8_t* tile_coords, int8_t* tile_subarray) const;
template void Domain::get_tile_subarray<uint8_t>(
    const uint8_t* tile_coords, uint8_t* tile_subarray) const;
template void Domain::get_tile_subarray<int16_t>(
    const int16_t* tile_coords, int16_t* tile_subarray) const;
template void Domain::get_tile_subarray<uint16_t>(
    const uint16_t* tile_coords, uint16_t* tile_subarray) const;
template void Domain::get_tile_subarray<uint32_t>(
    const uint32_t* tile_coords, uint32_t* tile_subarray) const;
template void Domain::get_tile_subarray<uint64_t>(
    const uint64_t* tile_coords, uint64_t* tile_subarray) const;
template void Domain::get_tile_subarray<double>(
    const double* tile_coords, double* tile_subarray) const;
template void Domain::get_tile_subarray<float>(
    const float* tile_coords, float* tile_subarray) const;

template void Domain::get_tile_subarray<int>(
    const int* domain, const int* tile_coords, int* tile_subarray) const;
template void Domain::get_tile_subarray<int64_t>(
    const int64_t* domain,
    const int64_t* tile_coords,
    int64_t* tile_subarray) const;
template void Domain::get_tile_subarray<int8_t>(
    const int8_t* domain,
    const int8_t* tile_coords,
    int8_t* tile_subarray) const;
template void Domain::get_tile_subarray<uint8_t>(
    const uint8_t* domain,
    const uint8_t* tile_coords,
    uint8_t* tile_subarray) const;
template void Domain::get_tile_subarray<int16_t>(
    const int16_t* domain,
    const int16_t* tile_coords,
    int16_t* tile_subarray) const;
template void Domain::get_tile_subarray<uint16_t>(
    const uint16_t* domain,
    const uint16_t* tile_coords,
    uint16_t* tile_subarray) const;
template void Domain::get_tile_subarray<uint32_t>(
    const uint32_t* domain,
    const uint32_t* tile_coords,
    uint32_t* tile_subarray) const;
template void Domain::get_tile_subarray<uint64_t>(
    const uint64_t* domain,
    const uint64_t* tile_coords,
    uint64_t* tile_subarray) const;
template void Domain::get_tile_subarray<float>(
    const float* domain, const float* tile_coords, float* tile_subarray) const;
template void Domain::get_tile_subarray<double>(
    const double* domain,
    const double* tile_coords,
    double* tile_subarray) const;

template void Domain::get_end_of_cell_slab<int8_t>(
    int8_t* subarray, int8_t* start, Layout layout, int8_t* end) const;
template void Domain::get_end_of_cell_slab<uint8_t>(
    uint8_t* subarray, uint8_t* start, Layout layout, uint8_t* end) const;
template void Domain::get_end_of_cell_slab<int16_t>(
    int16_t* subarray, int16_t* start, Layout layout, int16_t* end) const;
template void Domain::get_end_of_cell_slab<uint16_t>(
    uint16_t* subarray, uint16_t* start, Layout layout, uint16_t* end) const;
template void Domain::get_end_of_cell_slab<int>(
    int* subarray, int* start, Layout layout, int* end) const;
template void Domain::get_end_of_cell_slab<unsigned>(
    unsigned* subarray, unsigned* start, Layout layout, unsigned* end) const;
template void Domain::get_end_of_cell_slab<int64_t>(
    int64_t* subarray, int64_t* start, Layout layout, int64_t* end) const;
template void Domain::get_end_of_cell_slab<uint64_t>(
    uint64_t* subarray, uint64_t* start, Layout layout, uint64_t* end) const;

template void Domain::get_tile_coords<int8_t>(
    const int8_t* coords, int8_t* tile_coords) const;
template void Domain::get_tile_coords<uint8_t>(
    const uint8_t* coords, uint8_t* tile_coords) const;
template void Domain::get_tile_coords<int16_t>(
    const int16_t* coords, int16_t* tile_coords) const;
template void Domain::get_tile_coords<uint16_t>(
    const uint16_t* coords, uint16_t* tile_coords) const;
template void Domain::get_tile_coords<int>(
    const int* coords, int* tile_coords) const;
template void Domain::get_tile_coords<unsigned>(
    const unsigned* coords, unsigned* tile_coords) const;
template void Domain::get_tile_coords<int64_t>(
    const int64_t* coords, int64_t* tile_coords) const;
template void Domain::get_tile_coords<uint64_t>(
    const uint64_t* coords, uint64_t* tile_coords) const;

template void Domain::get_tile_domain<int8_t>(
    const int8_t* subarray, int8_t* tile_subarray) const;
template void Domain::get_tile_domain<uint8_t>(
    const uint8_t* subarray, uint8_t* tile_subarray) const;
template void Domain::get_tile_domain<int16_t>(
    const int16_t* subarray, int16_t* tile_subarray) const;
template void Domain::get_tile_domain<uint16_t>(
    const uint16_t* subarray, uint16_t* tile_subarray) const;
template void Domain::get_tile_domain<int>(
    const int* subarray, int* tile_subarray) const;
template void Domain::get_tile_domain<unsigned>(
    const unsigned* subarray, unsigned* tile_subarray) const;
template void Domain::get_tile_domain<int64_t>(
    const int64_t* subarray, int64_t* tile_subarray) const;
template void Domain::get_tile_domain<uint64_t>(
    const uint64_t* subarray, uint64_t* tile_subarray) const;

template uint64_t Domain::stride<int8_t>(Layout subarray_layout) const;
template uint64_t Domain::stride<uint8_t>(Layout subarray_layout) const;
template uint64_t Domain::stride<int16_t>(Layout subarray_layout) const;
template uint64_t Domain::stride<uint16_t>(Layout subarray_layout) const;
template uint64_t Domain::stride<int32_t>(Layout subarray_layout) const;
template uint64_t Domain::stride<uint32_t>(Layout subarray_layout) const;
template uint64_t Domain::stride<int64_t>(Layout subarray_layout) const;
template uint64_t Domain::stride<uint64_t>(Layout subarray_layout) const;
template uint64_t Domain::stride<float>(Layout subarray_layout) const;
template uint64_t Domain::stride<double>(Layout subarray_layout) const;

}  // namespace sm
}  // namespace tiledb
