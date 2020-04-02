/**
 * @file   domain.cc
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
 * This file implements class Domain.
 */

#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <iostream>
#include <limits>
#include <sstream>

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
  tile_order_cmp_func_ = domain->tile_order_cmp_func_;

  dimensions_.reserve(domain->dimensions_.size());
  for (auto dim : domain->dimensions_)
    dimensions_.emplace_back(new Dimension(dim));

  tile_order_ = domain->tile_order_;
  tile_offsets_col_ = domain->tile_offsets_col_;
  tile_offsets_row_ = domain->tile_offsets_row_;
}

Domain::~Domain() {
  for (auto dim : dimensions_)
    delete dim;
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
  dimensions_.emplace_back(new Dimension(dim));
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

template <class T>
int Domain::cell_order_cmp(const void* coord_a, const void* coord_b) {
  auto ca = (const T*)coord_a;
  auto cb = (const T*)coord_b;
  if (*ca < *cb)
    return -1;
  if (*ca > *cb)
    return 1;
  return 0;
}

int Domain::cell_order_cmp(
    unsigned dim_idx, const void* coord_a, const void* coord_b) const {
  return cell_order_cmp_func_[dim_idx](coord_a, coord_b);
}

int Domain::cell_order_cmp(
    const std::vector<const void*>& coord_buffs, uint64_t a, uint64_t b) const {
  if (cell_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto dim = dimension(d);
      auto coord_size = dim->coord_size();
      auto ca = &(((unsigned char*)coord_buffs[d])[a * coord_size]);
      auto cb = &(((unsigned char*)coord_buffs[d])[b * coord_size]);
      auto res = cell_order_cmp_func_[d](ca, cb);

      if (res == 1 || res == -1)
        return res;
      // else same tile on dimension d --> continue
    }
  } else {  // COL_MAJOR
    for (unsigned d = dim_num_ - 1;; --d) {
      auto dim = dimension(d);
      auto coord_size = dim->coord_size();
      auto ca = &(((unsigned char*)coord_buffs[d])[a * coord_size]);
      auto cb = &(((unsigned char*)coord_buffs[d])[b * coord_size]);
      auto res = cell_order_cmp_func_[d](ca, cb);

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

// ===== FORMAT =====
// type (uint8_t)
// dim_num (uint32_t)
// dimension #1
// dimension #2
// ...
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
    auto dim = new Dimension();
    dim->deserialize(buff, version, type);
    dimensions_.emplace_back(dim);
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
  return dimensions_[i];
}

const Dimension* Domain::dimension(const std::string& name) const {
  for (unsigned int i = 0; i < dim_num_; i++) {
    auto dim = dimensions_[i];
    if (dim->name() == name) {
      return dim;
    }
  }
  return nullptr;
}

void Domain::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  for (auto& dim : dimensions_) {
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
  for (unsigned d = 0; d < dim_num_; ++d)
    dimensions_[d]->expand_range(r1[d], &(*r2)[d]);
}

void Domain::expand_to_tiles(NDRange* ndrange) const {
  for (unsigned d = 0; d < dim_num_; ++d)
    dimensions_[d]->expand_to_tile(&(*ndrange)[d]);
}

template <class T>
void Domain::get_tile_coords(const T* coords, T* tile_coords) const {
  for (unsigned d = 0; d < dim_num_; d++) {
    auto tile_extent = *(const T*)this->tile_extent(d).data();
    auto dim_dom = (const T*)domain(d).data();
    tile_coords[d] = (coords[d] - dim_dom[0]) / tile_extent;
  }
}

template <class T>
Status Domain::get_cell_pos(const T* coords, uint64_t* pos) const {
  // Invoke the proper function based on the cell order
  if (cell_order_ == Layout::ROW_MAJOR) {
    *pos = get_cell_pos_row(coords);
    return Status::Ok();
  }
  if (cell_order_ == Layout::COL_MAJOR) {
    *pos = get_cell_pos_col(coords);
    return Status::Ok();
  }

  return LOG_STATUS(
      Status::DomainError("Cannot get cell position; Invalid cell order"));
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
      end[dim_num_ - 1] +=
          tile_extent - ((start[dim_num_ - 1] - dim_dom[0]) % tile_extent) - 1;
      end[dim_num_ - 1] =
          std::min(end[dim_num_ - 1], subarray[2 * (dim_num_ - 1) + 1]);
    } else {
      auto dim_dom = (const T*)domain(0).data();
      auto tile_extent = *(const T*)this->tile_extent(0).data();
      for (unsigned d = 0; d < dim_num_; ++d)
        end[d] = start[d];
      end[0] += tile_extent - ((start[0] - dim_dom[0]) % tile_extent) - 1;
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
    tile_subarray[2 * d] = (subarray[2 * d] - dim_dom[0]) / tile_extent;
    tile_subarray[2 * d + 1] = (subarray[2 * d + 1] - dim_dom[0]) / tile_extent;
  }
}

template <class T>
uint64_t Domain::get_tile_pos(const T* tile_coords) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return get_tile_pos_row(tile_coords);

  // ROW MAJOR
  return get_tile_pos_col(tile_coords);
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
    tile_subarray[2 * d] = tile_coords[d] * tile_extent + dim_dom[0];
    tile_subarray[2 * d + 1] =
        (tile_coords[d] + 1) * tile_extent - 1 + dim_dom[0];
  }
}

template <class T>
void Domain::get_tile_subarray(
    const T* domain, const T* tile_coords, T* tile_subarray) const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto tile_extent = *(const T*)this->tile_extent(d).data();
    tile_subarray[2 * d] = tile_coords[d] * tile_extent + domain[2 * d];
    tile_subarray[2 * d + 1] =
        (tile_coords[d] + 1) * tile_extent - 1 + domain[2 * d];
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

Status Domain::init(Layout cell_order, Layout tile_order) {
  // Set cell and tile order
  cell_order_ = cell_order;
  tile_order_ = tile_order;

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute tile offsets
  compute_tile_offsets();

  // Compute the tile/cell order cmp functions
  set_tile_cell_order_cmp_funcs();

  return Status::Ok();
}

bool Domain::null_tile_extents() const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    if (tile_extent(d).empty())
      return true;
  }

  return false;
}

// ===== FORMAT =====
// type (uint8_t)
// dim_num (uint32_t)
// dimension #1
// dimension #2
// ...
Status Domain::serialize(Buffer* buff, uint32_t version) {
  // Write dimensions
  RETURN_NOT_OK(buff->write(&dim_num_, sizeof(uint32_t)));
  for (auto dim : dimensions_)
    dim->serialize(buff, version);

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
      ret *= *(const T*)tile_extent(i).data();
  } else {  // COL_MAJOR
    for (unsigned i = 0; i < dim_num_ - 1; ++i)
      ret *= *(const T*)tile_extent(i).data();
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

double Domain::overlap_ratio(const NDRange& r1, const NDRange& r2) const {
  double ratio = 1.0;
  assert(dim_num_ == r1.size());
  assert(dim_num_ == r2.size());

  for (unsigned d = 0; d < dim_num_; ++d) {
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
  if (dim->tile_extent().empty())
    return 0;

  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto ca = (const T*)coord_a;
  auto cb = (const T*)coord_b;
  auto domain = (const T*)dim->domain().data();
  auto ta = (uint64_t)((*ca - domain[0]) / tile_extent);
  auto tb = (uint64_t)((*cb - domain[0]) / tile_extent);
  if (ta < tb)
    return -1;
  if (ta > tb)
    return 1;
  return 0;
}

int Domain::tile_order_cmp(
    const std::vector<const void*>& coord_buffs, uint64_t a, uint64_t b) const {
  if (tile_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto dim = dimension(d);
      auto coord_size = dim->coord_size();
      auto ca = &(((unsigned char*)coord_buffs[d])[a * coord_size]);
      auto cb = &(((unsigned char*)coord_buffs[d])[b * coord_size]);
      auto res = tile_order_cmp_func_[d](dim, ca, cb);

      if (res == 1 || res == -1)
        return res;
      // else same tile on dimension d --> continue
    }
  } else {  // COL_MAJOR
    for (unsigned d = dim_num_ - 1;; --d) {
      auto dim = dimension(d);
      auto coord_size = dim->coord_size();
      auto ca = &(((unsigned char*)coord_buffs[d])[a * coord_size]);
      auto cb = &(((unsigned char*)coord_buffs[d])[b * coord_size]);
      auto res = tile_order_cmp_func_[d](dim, ca, cb);

      if (res == 1 || res == -1)
        return res;
      // else same tile on dimension d --> continue

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
      compute_cell_num_per_tile<int64_t>();
      break;
    default:
      return;
  }
}

template <class T>
void Domain::compute_cell_num_per_tile() {
  // Applicable only to integer domains
  if (!std::numeric_limits<T>::is_integer)
    return;

  // Applicable only to non-NULL space tiles
  if (null_tile_extents())
    return;

  cell_num_per_tile_ = 1;
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto tile_extent = *(const T*)this->tile_extent(d).data();
    cell_num_per_tile_ *= tile_extent;
  }
}

void Domain::set_tile_cell_order_cmp_funcs() {
  tile_order_cmp_func_.resize(dim_num_);
  cell_order_cmp_func_.resize(dim_num_);
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto type = dimensions_[d]->type();
    switch (type) {
      case Datatype::INT32:
        tile_order_cmp_func_[d] = tile_order_cmp<int32_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int32_t>;
        break;
      case Datatype::INT64:
        tile_order_cmp_func_[d] = tile_order_cmp<int64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int64_t>;
        break;
      case Datatype::INT8:
        tile_order_cmp_func_[d] = tile_order_cmp<int8_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int8_t>;
        break;
      case Datatype::UINT8:
        tile_order_cmp_func_[d] = tile_order_cmp<uint8_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<uint8_t>;
        break;
      case Datatype::INT16:
        tile_order_cmp_func_[d] = tile_order_cmp<int16_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int16_t>;
        break;
      case Datatype::UINT16:
        tile_order_cmp_func_[d] = tile_order_cmp<uint16_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<uint16_t>;
        break;
      case Datatype::UINT32:
        tile_order_cmp_func_[d] = tile_order_cmp<uint32_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<uint32_t>;
        break;
      case Datatype::UINT64:
        tile_order_cmp_func_[d] = tile_order_cmp<uint64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<uint64_t>;
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
        tile_order_cmp_func_[d] = tile_order_cmp<int64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp<int64_t>;
        break;
      case Datatype::FLOAT32:
        tile_order_cmp_func_[d] = tile_order_cmp<float>;
        cell_order_cmp_func_[d] = cell_order_cmp<float>;
        break;
      case Datatype::FLOAT64:
        tile_order_cmp_func_[d] = tile_order_cmp<double>;
        cell_order_cmp_func_[d] = cell_order_cmp<double>;
        break;
      case Datatype::CHAR:
      case Datatype::STRING_ASCII:
      case Datatype::STRING_UTF8:
      case Datatype::STRING_UTF16:
      case Datatype::STRING_UTF32:
      case Datatype::STRING_UCS2:
      case Datatype::STRING_UCS4:
      case Datatype::ANY:
        // Not supported domain types
        assert(false);
    }
  }
}

void Domain::compute_tile_offsets() {
  // Applicable only to non-NULL space tiles
  if (null_tile_extents())
    return;

  // For easy reference
  uint64_t tile_num;  // Per dimension

  // Calculate tile offsets for column-major tile order
  tile_offsets_col_.reserve(dim_num_);
  tile_offsets_col_.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned d = 1; d < dim_num_; ++d) {
      tile_num = dimensions_[d]->tile_num(dimensions_[d]->domain());
      tile_offsets_col_.push_back(tile_offsets_col_.back() * tile_num);
    }
  }

  // Calculate tile offsets for row-major tile order
  tile_offsets_row_.reserve(dim_num_);
  tile_offsets_row_.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned d = dim_num_ - 2;; --d) {
      tile_num = dimensions_[d]->tile_num(dimensions_[d]->domain());
      tile_offsets_row_.push_back(tile_offsets_row_.back() * tile_num);
      if (d == 0)
        break;
    }
  }
  std::reverse(tile_offsets_row_.begin(), tile_offsets_row_.end());
}

template <class T>
uint64_t Domain::get_cell_pos_col(const T* coords) const {
  // For easy reference
  const T *dim_dom_0, *dim_dom_1, *dim_dom_2;
  T tile_extent_0, tile_extent_1, tile_extent_2;

  uint64_t pos = 0;
  T coords_norm;  // Normalized coordinates inside the tile

  // Special-case for low dimensions to an unrolled version of the default
  // loop.
  switch (dim_num_) {
    case 1:
      dim_dom_0 = (const T*)domain(0).data();
      tile_extent_0 = *(const T*)this->tile_extent(0).data();
      coords_norm = (coords[0] - dim_dom_0[0]);
      coords_norm -= (coords_norm / tile_extent_0) * tile_extent_0;
      pos += coords_norm * 1;
      break;
    case 2:
      dim_dom_0 = (const T*)domain(0).data();
      tile_extent_0 = *(const T*)this->tile_extent(0).data();
      coords_norm = (coords[0] - dim_dom_0[0]);
      coords_norm -= (coords_norm / tile_extent_0) * tile_extent_0;
      pos += coords_norm * 1;

      dim_dom_1 = (const T*)domain(1).data();
      tile_extent_1 = *(const T*)this->tile_extent(1).data();
      coords_norm = (coords[1] - dim_dom_1[0]);
      coords_norm -= (coords_norm / tile_extent_1) * tile_extent_1;
      pos += coords_norm * 1 * tile_extent_0;
      break;
    case 3:
      dim_dom_0 = (const T*)domain(0).data();
      tile_extent_0 = *(const T*)this->tile_extent(0).data();
      coords_norm = (coords[0] - dim_dom_0[0]);
      coords_norm -= (coords_norm / tile_extent_0) * tile_extent_0;
      pos += coords_norm * 1;

      dim_dom_1 = (const T*)domain(1).data();
      tile_extent_1 = *(const T*)this->tile_extent(1).data();
      coords_norm = (coords[1] - dim_dom_1[0]);
      coords_norm -= (coords_norm / tile_extent_1) * tile_extent_1;
      pos += coords_norm * 1 * tile_extent_0;

      dim_dom_2 = (const T*)domain(2).data();
      tile_extent_2 = *(const T*)this->tile_extent(2).data();
      coords_norm = (coords[2] - dim_dom_2[0]);
      coords_norm -= (coords_norm / tile_extent_2) * tile_extent_2;
      pos += coords_norm * 1 * tile_extent_0 * tile_extent_1;
      break;
    default: {
      uint64_t cell_offset = 1;
      for (unsigned d = 0; d < dim_num_; ++d) {
        auto dim_dom = (const T*)domain(d).data();
        auto tile_extent = *(const T*)this->tile_extent(d).data();
        coords_norm = (coords[d] - dim_dom[0]);
        coords_norm -= (coords_norm / tile_extent) * tile_extent;
        pos += coords_norm * cell_offset;
        cell_offset *= tile_extent;
      }
      break;
    }
  }

  return pos;
}

template <class T>
uint64_t Domain::get_cell_pos_col(const T* subarray, const T* coords) const {
  uint64_t pos = 0;

  // Special-case for low dimensions to an unrolled version of the default
  // loop.
  switch (dim_num_) {
    case 1:
      pos += (coords[0] - subarray[2 * 0]) * 1;
      break;
    case 2: {
      const uint64_t cell_num_0 = subarray[2 * 0 + 1] - subarray[2 * 0] + 1;
      pos += (coords[0] - subarray[2 * 0]) * 1;
      pos += (coords[1] - subarray[2 * 1]) * 1 * cell_num_0;
      break;
    }
    case 3: {
      const uint64_t cell_num_0 = subarray[2 * 0 + 1] - subarray[2 * 0] + 1;
      const uint64_t cell_num_1 = subarray[2 * 1 + 1] - subarray[2 * 1] + 1;
      pos += (coords[0] - subarray[2 * 0]) * 1;
      pos += (coords[1] - subarray[2 * 1]) * 1 * cell_num_0;
      pos += (coords[2] - subarray[2 * 2]) * 1 * cell_num_0 * cell_num_1;
      break;
    }
    default: {
      uint64_t cell_offset = 1;
      for (unsigned int i = 0; i < dim_num_; ++i) {
        pos += (coords[i] - subarray[2 * i]) * cell_offset;
        cell_offset *= subarray[2 * i + 1] - subarray[2 * i] + 1;
      }
      break;
    }
  }

  return pos;
}

template <class T>
uint64_t Domain::get_cell_pos_row(const T* coords) const {
  // For easy reference
  const T *dim_dom_0, *dim_dom_1, *dim_dom_2;
  T tile_extent_0, tile_extent_1, tile_extent_2;

  uint64_t pos = 0;
  T coords_norm;  // Normalized coordinates inside the tile

  // Special-case for low dimensions to an unrolled version of the default
  // loop.
  switch (dim_num_) {
    case 1:
      dim_dom_0 = (const T*)domain(0).data();
      tile_extent_0 = *(const T*)this->tile_extent(0).data();
      coords_norm = (coords[0] - dim_dom_0[0]);
      coords_norm -= (coords_norm / tile_extent_0) * tile_extent_0;
      pos += coords_norm;
      break;
    case 2:
      dim_dom_0 = (const T*)domain(0).data();
      tile_extent_0 = *(const T*)this->tile_extent(0).data();
      tile_extent_1 = *(const T*)this->tile_extent(1).data();
      coords_norm = (coords[0] - dim_dom_0[0]);
      coords_norm -= (coords_norm / tile_extent_0) * tile_extent_0;
      pos += coords_norm * tile_extent_1;

      dim_dom_1 = (const T*)domain(1).data();
      coords_norm = (coords[1] - dim_dom_1[0]);
      coords_norm -= (coords_norm / tile_extent_1) * tile_extent_1;
      pos += coords_norm * 1;
      break;
    case 3:
      dim_dom_0 = (const T*)domain(0).data();
      tile_extent_0 = *(const T*)this->tile_extent(0).data();
      tile_extent_1 = *(const T*)this->tile_extent(1).data();
      tile_extent_2 = *(const T*)this->tile_extent(2).data();
      coords_norm = (coords[0] - dim_dom_0[0]);
      coords_norm -= (coords_norm / tile_extent_0) * tile_extent_0;
      pos += coords_norm * tile_extent_1 * tile_extent_2;

      dim_dom_1 = (const T*)domain(1).data();
      coords_norm = (coords[1] - dim_dom_1[0]);
      coords_norm -= (coords_norm / tile_extent_1) * tile_extent_1;
      pos += coords_norm * tile_extent_2;

      dim_dom_2 = (const T*)domain(2).data();
      coords_norm = (coords[2] - dim_dom_2[0]);
      coords_norm -= (coords_norm / tile_extent_2) * tile_extent_2;
      pos += coords_norm * 1;
      break;
    default: {
      // Calculate initial cell_offset
      uint64_t cell_offset = 1;
      for (unsigned d = 1; d < dim_num_; ++d) {
        auto tile_extent = *(const T*)this->tile_extent(d).data();
        cell_offset *= tile_extent;
      }

      // Calculate position
      for (unsigned d = 0; d < dim_num_; ++d) {
        auto dim_dom = (const T*)domain(d).data();
        auto tile_extent = *(const T*)this->tile_extent(d).data();
        coords_norm = (coords[d] - dim_dom[0]);
        coords_norm -= (coords_norm / tile_extent) * tile_extent;
        pos += coords_norm * cell_offset;
        if (d < dim_num_ - 1) {
          auto tile_extent = *(const T*)this->tile_extent(d + 1).data();
          cell_offset /= tile_extent;
        }
      }
      break;
    }
  }

  return pos;
}

template <class T>
uint64_t Domain::get_cell_pos_row(const T* subarray, const T* coords) const {
  uint64_t pos = 0;

  // Special-case for low dimensions to an unrolled version of the default
  // loop.
  switch (dim_num_) {
    case 1:
      pos += (coords[0] - subarray[2 * 0]) * 1;
      break;
    case 2: {
      const uint64_t cell_num_0 =
          subarray[2 * (0 + 1) + 1] - subarray[2 * (0 + 1)] + 1;
      pos += (coords[0] - subarray[2 * 0]) * cell_num_0;
      pos += (coords[1] - subarray[2 * 1]) * 1;
      break;
    }
    case 3: {
      const uint64_t cell_num_0 =
          subarray[2 * (0 + 1) + 1] - subarray[2 * (0 + 1)] + 1;
      const uint64_t cell_num_1 =
          subarray[2 * (1 + 1) + 1] - subarray[2 * (1 + 1)] + 1;
      pos += (coords[0] - subarray[2 * 0]) * cell_num_0 * cell_num_1;
      pos += (coords[1] - subarray[2 * 1]) * cell_num_1;
      pos += (coords[2] - subarray[2 * 2]) * 1;
      break;
    }
    default: {
      // Calculate initial cell_offset
      uint64_t cell_offset = 1;
      for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
        const uint64_t cell_num_i =
            subarray[2 * (i + 1) + 1] - subarray[2 * (i + 1)] + 1;
        cell_offset *= cell_num_i;
      }

      // Calculate position
      for (unsigned int i = 0; i < dim_num_; ++i) {
        pos += (coords[i] - subarray[2 * i]) * cell_offset;
        if (i < dim_num_ - 1) {
          const uint64_t cell_num_i =
              subarray[2 * (i + 1) + 1] - subarray[2 * (i + 1)] + 1;
          cell_offset /= cell_num_i;
        }
      }
      break;
    }
  }

  return pos;
}

template <class T>
void Domain::get_next_cell_coords_col(
    const T* domain, T* cell_coords, bool* coords_retrieved) const {
  unsigned int i = 0;
  ++cell_coords[i];

  while (i < dim_num_ - 1 && cell_coords[i] > domain[2 * i + 1]) {
    cell_coords[i] = domain[2 * i];
    ++cell_coords[++i];
  }

  *coords_retrieved =
      !(i == dim_num_ - 1 && cell_coords[i] > domain[2 * i + 1]);
}

template <class T>
void Domain::get_next_cell_coords_row(
    const T* domain, T* cell_coords, bool* coords_retrieved) const {
  unsigned int i = dim_num_ - 1;
  ++cell_coords[i];

  while (i > 0 && cell_coords[i] > domain[2 * i + 1]) {
    cell_coords[i] = domain[2 * i];
    ++cell_coords[--i];
  }

  *coords_retrieved = !(i == 0 && cell_coords[i] > domain[2 * i + 1]);
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
uint64_t Domain::get_tile_pos_col(const T* tile_coords) const {
  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets_col_[i];

  // Return
  return pos;
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
    uint64_t tile_num;
    if (&typeid(T) != &typeid(float) && &typeid(T) != &typeid(double))
      tile_num =
          (domain[2 * (d - 1) + 1] - domain[2 * (d - 1)] + 1) / tile_extent;
    else
      tile_num = (domain[2 * (d - 1) + 1] - domain[2 * (d - 1)]) / tile_extent;
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
uint64_t Domain::get_tile_pos_row(const T* tile_coords) const {
  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets_row_[i];

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
      uint64_t tile_num;
      if (&typeid(T) != &typeid(float) && &typeid(T) != &typeid(double))
        tile_num =
            (domain[2 * (d + 1) + 1] - domain[2 * (d + 1)] + 1) / tile_extent;
      else
        tile_num =
            (domain[2 * (d + 1) + 1] - domain[2 * (d + 1)]) / tile_extent;
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

// Explicit template instantiations
template Status Domain::get_cell_pos<int>(
    const int* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<int64_t>(
    const int64_t* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<float>(
    const float* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<double>(
    const double* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<int8_t>(
    const int8_t* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<uint8_t>(
    const uint8_t* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<int16_t>(
    const int16_t* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<uint16_t>(
    const uint16_t* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<uint32_t>(
    const uint32_t* coords, uint64_t* pos) const;
template Status Domain::get_cell_pos<uint64_t>(
    const uint64_t* coords, uint64_t* pos) const;

template void Domain::get_next_cell_coords_row<int>(
    const int* domain, int* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_row<int64_t>(
    const int64_t* domain, int64_t* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_row<int8_t>(
    const int8_t* domain, int8_t* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_row<uint8_t>(
    const uint8_t* domain, uint8_t* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_row<int16_t>(
    const int16_t* domain, int16_t* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_row<uint16_t>(
    const uint16_t* domain,
    uint16_t* cell_coords,
    bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_row<uint32_t>(
    const uint32_t* domain,
    uint32_t* cell_coords,
    bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_row<uint64_t>(
    const uint64_t* domain,
    uint64_t* cell_coords,
    bool* coords_retrieved) const;

template void Domain::get_next_cell_coords_col<int>(
    const int* domain, int* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_col<int64_t>(
    const int64_t* domain, int64_t* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_col<int8_t>(
    const int8_t* domain, int8_t* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_col<uint8_t>(
    const uint8_t* domain, uint8_t* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_col<int16_t>(
    const int16_t* domain, int16_t* cell_coords, bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_col<uint16_t>(
    const uint16_t* domain,
    uint16_t* cell_coords,
    bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_col<uint32_t>(
    const uint32_t* domain,
    uint32_t* cell_coords,
    bool* coords_retrieved) const;
template void Domain::get_next_cell_coords_col<uint64_t>(
    const uint64_t* domain,
    uint64_t* cell_coords,
    bool* coords_retrieved) const;

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

template uint64_t Domain::get_tile_pos<int8_t>(const int8_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<uint8_t>(
    const uint8_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<int16_t>(
    const int16_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<uint16_t>(
    const uint16_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<int>(const int* tile_coords) const;
template uint64_t Domain::get_tile_pos<unsigned>(
    const unsigned* tile_coords) const;
template uint64_t Domain::get_tile_pos<int64_t>(
    const int64_t* tile_coords) const;
template uint64_t Domain::get_tile_pos<uint64_t>(
    const uint64_t* tile_coords) const;

template uint64_t Domain::get_cell_pos_col<int8_t>(
    const int8_t* subarray, const int8_t* coords) const;
template uint64_t Domain::get_cell_pos_col<uint8_t>(
    const uint8_t* subarray, const uint8_t* coords) const;
template uint64_t Domain::get_cell_pos_col<int16_t>(
    const int16_t* subarray, const int16_t* coords) const;
template uint64_t Domain::get_cell_pos_col<uint16_t>(
    const uint16_t* subarray, const uint16_t* coords) const;
template uint64_t Domain::get_cell_pos_col<int>(
    const int* subarray, const int* coords) const;
template uint64_t Domain::get_cell_pos_col<unsigned>(
    const unsigned* subarray, const unsigned* coords) const;
template uint64_t Domain::get_cell_pos_col<int64_t>(
    const int64_t* subarray, const int64_t* coords) const;
template uint64_t Domain::get_cell_pos_col<uint64_t>(
    const uint64_t* subarray, const uint64_t* coords) const;

template uint64_t Domain::get_cell_pos_row<int8_t>(
    const int8_t* subarray, const int8_t* coords) const;
template uint64_t Domain::get_cell_pos_row<uint8_t>(
    const uint8_t* subarray, const uint8_t* coords) const;
template uint64_t Domain::get_cell_pos_row<int16_t>(
    const int16_t* subarray, const int16_t* coords) const;
template uint64_t Domain::get_cell_pos_row<uint16_t>(
    const uint16_t* subarray, const uint16_t* coords) const;
template uint64_t Domain::get_cell_pos_row<int>(
    const int* subarray, const int* coords) const;
template uint64_t Domain::get_cell_pos_row<unsigned>(
    const unsigned* subarray, const unsigned* coords) const;
template uint64_t Domain::get_cell_pos_row<int64_t>(
    const int64_t* subarray, const int64_t* coords) const;
template uint64_t Domain::get_cell_pos_row<uint64_t>(
    const uint64_t* subarray, const uint64_t* coords) const;

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
