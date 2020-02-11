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
  type_ = Datatype::INT32;
  cell_num_per_tile_ = 0;
}

Domain::Domain(Datatype type)
    : type_(type) {
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;
  cell_num_per_tile_ = 0;
}

Domain::Domain(const Domain* domain) {
  cell_num_per_tile_ = domain->cell_num_per_tile_;
  cell_order_ = domain->cell_order_;
  type_ = domain->type_;
  cell_order_cmp_func_ = domain->cell_order_cmp_func_;
  tile_order_cmp_func_ = domain->tile_order_cmp_func_;

  for (auto dim : domain->dimensions_)
    dimensions_.emplace_back(new Dimension(dim));

  tile_order_ = domain->tile_order_;
  tile_offsets_col_ = domain->tile_offsets_col_;
  tile_offsets_row_ = domain->tile_offsets_row_;
  tile_domain_ = domain->tile_domain_;
}

Domain::~Domain() {
  for (auto dim : dimensions_)
    delete dim;
}

/* ********************************* */
/*                API                */
/* ********************************* */

double Domain::tile_coverage(
    const NDRange& domain,
    const NDPoint& tile_coords,
    const NDRange& subarray) const {
  double cov = 1.0, dim_cov;
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    dim_cov =
        dimensions_[d]->tile_coverage(domain[d], tile_coords[d], subarray[d]);

    // Return if coverage on any dimension is 0.0
    if (dim_cov == 0.0)
      return 0.0;

    cov *= dim_cov;

    // At this point, we know that the tile coverage should not be 0.
    // If cov goes to 0, then it is because it got too small, so we should set
    // it to epsilon.
    (cov != 0) ? cov : std::nextafter(0, std::numeric_limits<double>::max());
  }

  return cov;
}

double Domain::coverage(const NDRange& a, const NDRange& b) const {
  double cov = 1.0, dim_cov;
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    dim_cov = dimensions_[d]->coverage(a[d], b[d]);

    // Return if coverage on any dimension is 0.0
    if (dim_cov == 0.0)
      return 0.0;

    cov *= dim_cov;

    // At this point, we know that the coverage should not be 0.
    // If cov goes to 0, then it is because it got too small, so we should set
    // it to epsilon.
    (cov != 0) ? cov : std::nextafter(0, std::numeric_limits<double>::max());
  }

  return cov;
}

NDRange Domain::tile_domain(const NDRange& range, const NDRange& domain) const {
  auto dim_num = (unsigned)dimensions_.size();
  NDRange ret(dim_num);
  for (unsigned d = 0; d < dim_num; ++d)
    ret[d] = dimensions_[d]->tile_domain(range[d], domain[d]);
  return ret;
}

NDPoint Domain::start_coords(const NDRange& domain) const {
  auto dim_num = (unsigned)dimensions_.size();
  NDPoint ret(dim_num);
  for (unsigned d = 0; d < dim_num; ++d)
    ret[d] = dimensions_[d]->range_start(domain[d]);

  return ret;
}

bool Domain::point_in_range(const NDPoint& point, const NDRange& range) const {
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    if (!dimensions_[d]->value_in_range(point[d], range[d]))
      return false;
  }

  return true;
}

void Domain::expand_mbr(NDRange* mbr_a, const NDRange& mbr_b) const {
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d)
    dimensions_[d]->expand_range(&((*mbr_a)[d]), mbr_b[d]);
}

Layout Domain::cell_order() const {
  return cell_order_;
}

template <class T>
T Domain::floor_to_tile(T value, unsigned dim_idx) const {
  auto dim = dimensions_[dim_idx];
  const auto& tile_extent = dimensions_[dim_idx]->tile_extent();
  auto dim_domain_low = *(const T*)&dim->domain()[0];

  if (tile_extent == nullptr)
    return dim_domain_low;

  auto tile_extent_v = *((const T*)tile_extent);
  uint64_t div = (value - dim_domain_low) / tile_extent_v;
  return (T)div * tile_extent_v + dim_domain_low;
}

Layout Domain::tile_order() const {
  return tile_order_;
}

Status Domain::split_subarray(
    void* subarray, Layout layout, void** subarray_1, void** subarray_2) const {
  switch (type_) {
    case Datatype::INT8:
      return split_subarray<int8_t>(subarray, layout, subarray_1, subarray_2);
    case Datatype::UINT8:
      return split_subarray<uint8_t>(subarray, layout, subarray_1, subarray_2);
    case Datatype::INT16:
      return split_subarray<int16_t>(subarray, layout, subarray_1, subarray_2);
    case Datatype::UINT16:
      return split_subarray<uint16_t>(subarray, layout, subarray_1, subarray_2);
    case Datatype::INT32:
      return split_subarray<int>(subarray, layout, subarray_1, subarray_2);
    case Datatype::UINT32:
      return split_subarray<unsigned>(subarray, layout, subarray_1, subarray_2);
    case Datatype::INT64:
      return split_subarray<int64_t>(subarray, layout, subarray_1, subarray_2);
    case Datatype::UINT64:
      return split_subarray<uint64_t>(subarray, layout, subarray_1, subarray_2);
    case Datatype::FLOAT32:
      return split_subarray<float>(subarray, layout, subarray_1, subarray_2);
    case Datatype::FLOAT64:
      return split_subarray<double>(subarray, layout, subarray_1, subarray_2);
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
      return split_subarray<int64_t>(subarray, layout, subarray_1, subarray_2);
    default:
      return LOG_STATUS(Status::DomainError(
          "Cannot split subarray; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Domain::split_subarray(
    void* subarray, Layout layout, void** subarray_1, void** subarray_2) const {
  switch (layout) {
    case Layout::GLOBAL_ORDER:
      return split_subarray_global<T>(subarray, subarray_1, subarray_2);
    case Layout::ROW_MAJOR:
    case Layout::COL_MAJOR:
      return split_subarray_cell<T>(subarray, layout, subarray_1, subarray_2);
    default:
      return LOG_STATUS(
          Status::DomainError("Cannot split subarray; Unsupported layout"));
  }

  return Status::Ok();
}

template <class T>
Status Domain::split_subarray_global(
    void* subarray, void** subarray_1, void** subarray_2) const {
  // Find dimension to split by tile
  auto s = (T*)subarray;
  unsigned dim_to_split = UINT32_MAX;
  uint64_t tiles_apart = 0;
  auto dim_num = (unsigned)dimensions_.size();

  if (tile_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = dimensions_[d];
      const auto& tile_extent = dim->tile_extent();

      if (tile_extent == nullptr)
        continue;

      auto dim_domain_low = *((const T*)&dim->domain()[0]);
      auto tile_extent_v = *((const T*)tile_extent);

      tiles_apart =
          (T)floor(((s[2 * d + 1] - dim_domain_low) / tile_extent_v)) -
          (T)floor(((s[2 * d] - dim_domain_low) / tile_extent_v));

      if (tiles_apart != 0) {
        // Not in the same tile - can split
        dim_to_split = d;
        break;
      }
    }
  } else {
    for (unsigned d = dim_num - 1;; --d) {
      auto dim = dimensions_[d];
      const auto& tile_extent = dim->tile_extent();

      if (tile_extent == nullptr)
        continue;

      auto dim_domain_low = *((const T*)&dim->domain()[0]);
      auto tile_extent_v = *((const T*)tile_extent);

      tiles_apart =
          (T)floor(((s[2 * d + 1] - dim_domain_low) / tile_extent_v)) -
          (T)floor(((s[2 * d] - dim_domain_low) / tile_extent_v));
      if (tiles_apart != 0) {
        // Not in the same tile - can split
        dim_to_split = d;
        break;
      }
      if (d == 0)
        break;
    }
  }

  // Cannot split by tile, split by cell
  if (dim_to_split == UINT32_MAX)
    return split_subarray_cell<T>(
        subarray, cell_order_, subarray_1, subarray_2);

  // Split by tile
  *subarray_1 = std::malloc(2 * dim_num * sizeof(T));
  if (*subarray_1 == nullptr)
    return LOG_STATUS(
        Status::DomainError("Cannot split subarray; Memory allocation failed"));
  *subarray_2 = std::malloc(2 * dim_num * sizeof(T));
  if (*subarray_2 == nullptr) {
    std::free(subarray_1);
    *subarray_1 = nullptr;
    return LOG_STATUS(
        Status::DomainError("Cannot split subarray; Memory allocation failed"));
  }
  auto s1 = (T*)(*subarray_1);
  auto s2 = (T*)(*subarray_2);

  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimensions_[d];
    auto tile_extent_v = *((const T*)dim->tile_extent());

    if (d != dim_to_split) {
      s1[2 * d] = s[2 * d];
      s1[2 * d + 1] = s[2 * d + 1];
      s2[2 * d] = s[2 * d];
      s2[2 * d + 1] = s[2 * d + 1];
    } else {
      s1[2 * d] = s[2 * d];
      s1[2 * d + 1] =
          s1[2 * d] + std::max<T>(1, floor(tiles_apart / 2)) * tile_extent_v;

      if (std::numeric_limits<T>::is_integer) {
        s1[2 * d + 1] = floor_to_tile(s1[2 * d + 1], d) - 1;
        s2[2 * d] = s1[2 * d + 1] + 1;
      } else {
        s2[2 * d] = floor_to_tile(s1[2 * d + 1], d);
        s1[2 * d + 1] =
            std::nextafter(s2[2 * d], std::numeric_limits<T>::lowest());
      }
      s2[2 * d + 1] = s[2 * d + 1];

      assert(s1[2 * d + 1] >= s1[2 * d]);
      assert(s2[2 * d + 1] >= s2[2 * d]);
    }
  }

  return Status::Ok();
}

template <class T>
Status Domain::split_subarray_cell(
    void* subarray,
    Layout cell_layout,
    void** subarray_1,
    void** subarray_2) const {
  // Find dimension to split
  auto s = (T*)subarray;
  unsigned dim_to_split = UINT32_MAX;
  auto dim_num = (unsigned)dimensions_.size();

  if (cell_layout == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num; ++d) {
      if (s[2 * d] != s[2 * d + 1]) {
        dim_to_split = d;
        break;
      }
    }
  } else {
    for (unsigned d = dim_num - 1;; --d) {
      if (s[2 * d] != s[2 * d + 1]) {
        dim_to_split = d;
        break;
      }
      if (d == 0)
        break;
    }
  }

  // Cannot split
  if (dim_to_split == UINT32_MAX) {
    *subarray_1 = nullptr;
    *subarray_2 = nullptr;
    return Status::Ok();
  }

  // Split
  *subarray_1 = std::malloc(2 * dim_num * sizeof(T));
  if (*subarray_1 == nullptr)
    return LOG_STATUS(
        Status::DomainError("Cannot split subarray; Memory allocation failed"));
  *subarray_2 = std::malloc(2 * dim_num * sizeof(T));
  if (*subarray_2 == nullptr) {
    std::free(subarray_1);
    *subarray_1 = nullptr;
    return LOG_STATUS(
        Status::DomainError("Cannot split subarray; Memory allocation failed"));
  }
  auto s1 = (T*)(*subarray_1);
  auto s2 = (T*)(*subarray_2);
  for (unsigned d = 0; d < dim_num; ++d) {
    if (d != dim_to_split) {
      s1[2 * d] = s[2 * d];
      s1[2 * d + 1] = s[2 * d + 1];
      s2[2 * d] = s[2 * d];
      s2[2 * d + 1] = s[2 * d + 1];
    } else {
      s1[2 * d] = s[2 * d];
      if (std::numeric_limits<T>::is_integer) {  // Integers
        s1[2 * d + 1] = s[2 * d] + (s[2 * d + 1] - s[2 * d]) / 2;
        s2[2 * d] = s1[2 * d + 1] + 1;
      } else {  // Reals
        if (std::nextafter(s[2 * d], std::numeric_limits<T>::max()) ==
            s[2 * d + 1]) {
          s1[2 * d + 1] = s[2 * d];
          s2[2 * d] = s[2 * d + 1];
        } else {
          s1[2 * d + 1] = s[2 * d] + (s[2 * d + 1] - s[2 * d]) / 2;
          s2[2 * d] =
              std::nextafter(s1[2 * d + 1], std::numeric_limits<T>::max());
        }
      }
      s2[2 * d + 1] = s[2 * d + 1];
    }
  }

  return Status::Ok();
}

Status Domain::add_dimension(const Dimension* dim) {
  // Set domain type and do sanity check
  auto dim_num = (unsigned)dimensions_.size();
  if (dim_num == 0)
    type_ = dim->type();
  else if (dim->type() != type_)
    return LOG_STATUS(
        Status::DomainError("Cannot add dimension to domain; All added "
                            "dimensions must have the same type"));

  // Compute new dimension name
  std::string new_dim_name = dim->name();
  if (new_dim_name.empty())
    new_dim_name = default_dimension_name(dim_num);

  auto new_dim = new Dimension(new_dim_name, type_);
  RETURN_NOT_OK_ELSE(new_dim->set_domain(dim->domain()), delete new_dim);
  RETURN_NOT_OK_ELSE(
      new_dim->set_tile_extent(dim->tile_extent()), delete new_dim);

  dimensions_.emplace_back(new_dim);

  return Status::Ok();
}

uint64_t Domain::cell_num(const void* domain) const {
  switch (type_) {
    case Datatype::INT32:
      return cell_num<int>(static_cast<const int*>(domain));
    case Datatype::INT64:
      return cell_num<int64_t>(static_cast<const int64_t*>(domain));
    case Datatype::INT8:
      return cell_num<int8_t>(static_cast<const int8_t*>(domain));
    case Datatype::UINT8:
      return cell_num<uint8_t>(static_cast<const uint8_t*>(domain));
    case Datatype::INT16:
      return cell_num<int16_t>(static_cast<const int16_t*>(domain));
    case Datatype::UINT16:
      return cell_num<uint16_t>(static_cast<const uint16_t*>(domain));
    case Datatype::UINT32:
      return cell_num<uint32_t>(static_cast<const uint32_t*>(domain));
    case Datatype::UINT64:
      return cell_num<uint64_t>(static_cast<const uint64_t*>(domain));
    case Datatype::FLOAT32:
      return cell_num<float>(static_cast<const float*>(domain));
    case Datatype::FLOAT64:
      return cell_num<double>(static_cast<const double*>(domain));
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
      return cell_num<int64_t>(static_cast<const int64_t*>(domain));
    default:
      assert(false);
      return 0;
  }
}

template <class T>
uint64_t Domain::cell_num(const T* domain) const {
  if (&typeid(T) == &typeid(float) || &typeid(T) == &typeid(double))
    return 0;

  auto dim_num = (unsigned)dimensions_.size();
  uint64_t cell_num = 1, range, prod;
  for (unsigned d = 0; d < dim_num; ++d) {
    // The code below essentially computes
    // cell_num *= domain[2 * d + 1] - domain[2 * d] + 1;
    // while performing overflow checks
    range = domain[2 * d + 1] - domain[2 * d];
    if (range == std::numeric_limits<uint64_t>::max())  // overflow
      return 0;
    ++range;
    prod = range * cell_num;
    if (prod / range != cell_num)  // Overflow
      return 0;
    cell_num = prod;
  }

  return cell_num;
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
  auto dim_num = (unsigned)dimensions_.size();
  if (cell_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num; ++d) {
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
    for (unsigned d = dim_num - 1;; --d) {
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

void Domain::crop_domain(NDRange* domain) const {
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimensions_[d];
    dim->crop(&((*domain)[d]), dim->domain());
  }
}

// ===== FORMAT =====
// type (uint8_t)
// dim_num (uint32_t)
// dimension #1
// dimension #2
// ...
Status Domain::deserialize(ConstBuffer* buff) {
  // Load type
  uint8_t type;
  RETURN_NOT_OK(buff->read(&type, sizeof(uint8_t)));
  type_ = static_cast<Datatype>(type);

  // Load dimensions
  unsigned dim_num;
  RETURN_NOT_OK(buff->read(&dim_num, sizeof(uint32_t)));
  for (uint32_t d = 0; d < dim_num; ++d) {
    auto dim = new Dimension();
    dim->deserialize(buff, type_);
    dimensions_.emplace_back(dim);
  }

  set_tile_cell_order_cmp_funcs();

  return Status::Ok();
}

unsigned Domain::dim_num() const {
  return (unsigned)dimensions_.size();
}

const Dimension* Domain::dimension(unsigned dim_idx) const {
  if (dim_idx > (unsigned)dimensions_.size())
    return nullptr;
  return dimensions_[dim_idx];
}

const Dimension* Domain::dimension(const std::string& name) const {
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; d++) {
    auto dim = dimensions_[d];
    if (dim->name() == name) {
      return dim;
    }
  }
  return nullptr;
}

void Domain::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(out, "=== Domain ===\n");
  fprintf(out, "- Dimensions type: %s\n", datatype_str(type_).c_str());

  for (auto& dim : dimensions_) {
    fprintf(out, "\n");
    dim->dump(out);
  }
}

void Domain::expand_domain(NDRange* domain) const {
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned int d = 0; d < dim_num; ++d)
    dimensions_[d]->expand_range_to_tile(&(*domain)[d]);
}

template <class T>
void Domain::get_tile_coords(const T* coords, T* tile_coords) const {
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; d++) {
    auto dim = dimensions_[d];
    auto dim_domain_low = *((const T*)&dim->domain()[0]);
    auto tile_extent = *((const T*)dim->tile_extent());
    tile_coords[d] = (coords[d] - dim_domain_low) / tile_extent;
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
  auto dim_num = dimensions_.size();
  if (layout == Layout::GLOBAL_ORDER || layout == cell_order_) {
    auto dim_domain_f_low = *(const T*)&dimensions_[0]->domain()[0];
    auto dim_domain_l_low = *(const T*)&dimensions_[dim_num - 1]->domain()[0];
    auto tile_extent_f_v = *(const T*)dimensions_[0]->tile_extent();
    auto tile_extent_l_v = *(const T*)dimensions_[dim_num - 1]->tile_extent();

    if (cell_order_ == Layout::ROW_MAJOR) {
      for (unsigned d = 0; d < dim_num; ++d)
        end[d] = start[d];
      end[dim_num - 1] +=
          tile_extent_l_v -
          ((start[dim_num - 1] - dim_domain_l_low) % tile_extent_l_v) - 1;
      end[dim_num - 1] =
          std::min(end[dim_num - 1], subarray[2 * (dim_num - 1) + 1]);
    } else {
      for (unsigned i = 0; i < dim_num; ++i)
        end[i] = start[i];
      end[0] += tile_extent_f_v -
                ((start[0] - dim_domain_f_low) % tile_extent_f_v) - 1;
      end[0] = std::min(end[0], subarray[1]);
    }
  } else {
    for (unsigned d = 0; d < dim_num; ++d)
      end[d] = start[d];
    (void)subarray;
  }
}

void Domain::next_coords(const NDRange& domain, NDPoint* coords) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    next_coords_row(domain, coords);
  else if (tile_order_ == Layout::COL_MAJOR)
    next_coords_col(domain, coords);
  else  // Sanity check
    assert(0);
}

void Domain::next_coords(
    const NDRange& domain, NDPoint* coords, bool* in) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    next_coords_row(domain, coords, in);
  else if (tile_order_ == Layout::COL_MAJOR)
    next_coords_col(domain, coords, in);
  else  // Sanity check
    assert(0);
}

template <class T>
void Domain::get_tile_domain(const T* subarray, T* tile_subarray) const {
  auto dim_num = (unsigned)dimensions_.size();

  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_domain_low = *(const T*)&dimensions_[d]->domain()[0];
    auto tile_extent_v = *(const T*)dimensions_[d]->tile_extent();
    tile_subarray[2 * d] = (subarray[2 * d] - dim_domain_low) / tile_extent_v;
    tile_subarray[2 * d + 1] =
        (subarray[2 * d + 1] - dim_domain_low) / tile_extent_v;
  }
}

uint64_t Domain::tile_pos(const NDPoint& tile_coords) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return tile_pos_row(tile_coords);

  // ROW MAJOR
  return tile_pos_col(tile_coords);
}

uint64_t Domain::tile_pos(
    const NDRange& tile_domain, const NDPoint& tile_coords) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return tile_pos_row(tile_domain, tile_coords);
  // COL_MAJOR
  return tile_pos_col(tile_domain, tile_coords);
}

template <class T>
void Domain::get_tile_subarray(const T* tile_coords, T* tile_subarray) const {
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimensions_[d];
    auto dim_domain_low = *(const T*)&dim->domain()[0];
    auto tile_extent_v = *(const T*)dim->tile_extent();

    tile_subarray[2 * d] = tile_coords[d] * tile_extent_v + dim_domain_low;
    tile_subarray[2 * d + 1] =
        (tile_coords[d] + 1) * tile_extent_v - 1 + dim_domain_low;
  }
}

template <class T>
void Domain::get_tile_subarray(
    const T* domain, const T* tile_coords, T* tile_subarray) const {
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimensions_[d];
    auto tile_extent_v = *(const T*)dim->tile_extent();
    tile_subarray[2 * d] = tile_coords[d] * tile_extent_v + domain[2 * d];
    tile_subarray[2 * d + 1] =
        (tile_coords[d] + 1) * tile_extent_v - 1 + domain[2 * d];
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

  // Compute tile domain
  compute_tile_domain();

  // Compute tile offsets
  compute_tile_offsets();

  // Compute the tile/cell order cmp functions
  set_tile_cell_order_cmp_funcs();

  return Status::Ok();
}

bool Domain::overlap(const NDRange& a, const NDRange& b) const {
  // Sanity check
  if (a.size() != b.size() || a.empty())
    return false;

  auto dim_num = (unsigned)a.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    if (!dimensions_[d]->overlap(a[d], b[d]))
      return false;
  }

  return true;
}

// ===== FORMAT =====
// type (uint8_t)
// dim_num (uint32_t)
// dimension #1
// dimension #2
// ...
Status Domain::serialize(Buffer* buff) {
  // Write type
  auto type = static_cast<uint8_t>(type_);
  RETURN_NOT_OK(buff->write(&type, sizeof(uint8_t)));

  // Write dimensions
  auto dim_num = (unsigned)dimensions_.size();
  RETURN_NOT_OK(buff->write(&dim_num, sizeof(uint32_t)));
  for (auto dim : dimensions_)
    dim->serialize(buff);

  return Status::Ok();
}

Status Domain::set_null_tile_extents_to_range() {
  for (auto& d : dimensions_)
    RETURN_NOT_OK(d->set_null_tile_extent_to_range());
  return Status::Ok();
}

template <class T>
uint64_t Domain::stride(Layout subarray_layout) const {
  auto dim_num = (unsigned)dimensions_.size();
  if (dim_num == 1 || subarray_layout == Layout::GLOBAL_ORDER ||
      subarray_layout == cell_order_)
    return UINT64_MAX;

  uint64_t ret = 1;
  if (cell_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 1; d < dim_num; ++d) {
      assert(dimensions_[d]->tile_extent() != nullptr);
      ret *= *(const T*)dimensions_[d]->tile_extent();
    }
  } else {  // COL_MAJOR
    for (unsigned d = 0; d < dim_num - 1; ++d) {
      assert(dimensions_[d]->tile_extent() != nullptr);
      ret *= *(const T*)dimensions_[d]->tile_extent();
    }
  }

  return ret;
}

uint64_t Domain::tile_num(const NDRange& range) const {
  uint64_t ret = 1;
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned int d = 0; d < dim_num; ++d) {
    ret *= dimensions_[d]->tile_num(range[d]);
  }

  return ret;
}

template <class T>
int Domain::tile_order_cmp(
    const Dimension* dim, const void* coord_a, const void* coord_b) {
  const auto& tile_extent = dim->tile_extent();
  if (tile_extent == nullptr)
    return 0;

  auto ca = (T*)coord_a;
  auto cb = (T*)coord_b;
  auto dim_domain_low = *(const T*)&dim->domain()[0];
  auto tile_extent_v = *(const T*)tile_extent;
  auto ta = (T)((*ca - dim_domain_low) / tile_extent_v);
  auto tb = (T)((*cb - dim_domain_low) / tile_extent_v);
  if (ta < tb)
    return -1;
  if (ta > tb)
    return 1;
  return 0;
}

int Domain::tile_order_cmp(
    const std::vector<const void*>& coord_buffs, uint64_t a, uint64_t b) const {
  auto dim_num = (unsigned)dimensions_.size();
  if (tile_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num; ++d) {
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
    for (unsigned d = dim_num - 1;; --d) {
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

Datatype Domain::type() const {
  return type_;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void Domain::compute_cell_num_per_tile() {
  // Invoke the proper templated function
  switch (type_) {
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

  cell_num_per_tile_ = 1;
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimensions_[d];
    const auto& tile_extent = dim->tile_extent();

    // Applicable only to non-null tile extents
    if (tile_extent == nullptr) {
      cell_num_per_tile_ = 0;
      return;
    }
    auto tile_extent_v = *(const T*)tile_extent;
    cell_num_per_tile_ *= tile_extent_v;
  }
}

void Domain::compute_tile_domain() {
  // Invoke the proper templated function
  switch (type_) {
    case Datatype::INT32:
      compute_tile_domain<int>();
      break;
    case Datatype::INT64:
      compute_tile_domain<int64_t>();
      break;
    case Datatype::FLOAT32:
      compute_tile_domain<float>();
      break;
    case Datatype::FLOAT64:
      compute_tile_domain<double>();
      break;
    case Datatype::INT8:
      compute_tile_domain<int8_t>();
      break;
    case Datatype::UINT8:
      compute_tile_domain<uint8_t>();
      break;
    case Datatype::INT16:
      compute_tile_domain<int16_t>();
      break;
    case Datatype::UINT16:
      compute_tile_domain<uint16_t>();
      break;
    case Datatype::UINT32:
      compute_tile_domain<uint32_t>();
      break;
    case Datatype::UINT64:
      compute_tile_domain<uint64_t>();
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
      compute_tile_domain<int64_t>();
      break;
    default:
      assert(0);
  }
}

void Domain::set_tile_cell_order_cmp_funcs() {
  auto dim_num = (unsigned)dimensions_.size();
  tile_order_cmp_func_.resize(dim_num);
  cell_order_cmp_func_.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    switch (type_) {
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

template <class T>
void Domain::compute_tile_domain() {
  // Allocate space for the tile domain
  auto dim_num = (unsigned)dimensions_.size();
  assert(tile_domain_.empty());
  tile_domain_.resize(dim_num);

  T tile_num;  // Per dimension
  // Calculate tile domain
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimensions_[d];
    auto tile_extent = dim->tile_extent();

    // Applicable only to non-null tile extents
    if (tile_extent == nullptr) {
      tile_domain_.clear();
      return;
    }

    auto tile_extent_v = *(const T*)tile_extent;
    auto dim_domain = (const T*)&dim->domain()[0];
    tile_domain_[d].resize(2 * sizeof(T));
    auto tile_domain = (T*)&tile_domain_[d][0];

    tile_num = ceil(double(dim_domain[1] - dim_domain[0] + 1) / tile_extent_v);
    tile_domain[0] = 0;
    tile_domain[1] = tile_num - 1;
  }
}

void Domain::compute_tile_offsets() {
  // Invoke the proper templated function
  switch (type_) {
    case Datatype::INT32:
      compute_tile_offsets<int>();
      break;
    case Datatype::INT64:
      compute_tile_offsets<int64_t>();
      break;
    case Datatype::FLOAT32:
      compute_tile_offsets<float>();
      break;
    case Datatype::FLOAT64:
      compute_tile_offsets<double>();
      break;
    case Datatype::INT8:
      compute_tile_offsets<int8_t>();
      break;
    case Datatype::UINT8:
      compute_tile_offsets<uint8_t>();
      break;
    case Datatype::INT16:
      compute_tile_offsets<int16_t>();
      break;
    case Datatype::UINT16:
      compute_tile_offsets<uint16_t>();
      break;
    case Datatype::UINT32:
      compute_tile_offsets<uint32_t>();
      break;
    case Datatype::UINT64:
      compute_tile_offsets<uint64_t>();
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
      compute_tile_offsets<int64_t>();
      break;
    default:
      assert(0);
  }
}

template <class T>
void Domain::compute_tile_offsets() {
  // For easy reference
  auto dim_num = (unsigned)dimensions_.size();
  uint64_t tile_num;  // Per dimension

  // Calculate tile offsets for column-major tile order
  tile_offsets_col_.push_back(1);
  if (dim_num > 1) {
    for (unsigned d = 1; d < dim_num; ++d) {
      auto dim = dimensions_[d - 1];
      const auto& tile_extent = dim->tile_extent();

      if (tile_extent == nullptr) {
        tile_offsets_col_.clear();
        return;
      }

      auto dim_domain = (const T*)&dim->domain()[0];
      auto tile_extent_v = *(const T*)tile_extent;

      tile_num =
          utils::math::ceil(dim_domain[1] - dim_domain[0] + 1, tile_extent_v);
      tile_offsets_col_.push_back(tile_offsets_col_.back() * tile_num);
    }
  }

  // Calculate tile offsets for row-major tile order
  tile_offsets_row_.push_back(1);
  if (dim_num > 1) {
    for (unsigned d = dim_num - 2;; --d) {
      auto dim = dimensions_[d + 1];
      const auto& tile_extent = dim->tile_extent();

      if (tile_extent == nullptr) {
        tile_offsets_col_.clear();
        tile_offsets_row_.clear();
        return;
      }

      auto dim_domain = (const T*)&dim->domain()[0];
      auto tile_extent_v = *(const T*)tile_extent;

      tile_num =
          utils::math::ceil(dim_domain[1] - dim_domain[0] + 1, tile_extent_v);
      tile_offsets_row_.push_back(tile_offsets_row_.back() * tile_num);
      if (d == 0)
        break;
    }
  }
  std::reverse(tile_offsets_row_.begin(), tile_offsets_row_.end());
}

std::string Domain::default_dimension_name(unsigned int i) const {
  std::stringstream ss;
  ss << constants::default_dim_name << "_" << i;
  return ss.str();
}

template <class T>
uint64_t Domain::get_cell_pos_col(const T* coords) const {
  uint64_t pos = 0;
  T coords_norm;  // Normalized coordinates inside the tile
  T dim_domain_low_0, dim_domain_low_1, dim_domain_low_2;
  T tile_extent_v_0, tile_extent_v_1, tile_extent_v_2;

  // Special-case for low dimensions to an unrolled version of the default
  // loop.
  auto dim_num = (unsigned)dimensions_.size();
  switch (dim_num) {
    case 1:
      dim_domain_low_0 = *(const T*)&dimensions_[0]->domain()[0];
      tile_extent_v_0 = *(const T*)dimensions_[0]->tile_extent();

      coords_norm = (coords[0] - dim_domain_low_0);
      coords_norm -= (coords_norm / tile_extent_v_0) * tile_extent_v_0;
      pos += coords_norm * 1;
      break;
    case 2:
      dim_domain_low_0 = *(const T*)&dimensions_[0]->domain()[0];
      tile_extent_v_0 = *(const T*)dimensions_[0]->tile_extent();
      dim_domain_low_1 = *(const T*)&dimensions_[1]->domain()[0];
      tile_extent_v_1 = *(const T*)dimensions_[1]->tile_extent();

      coords_norm = (coords[0] - dim_domain_low_0);
      coords_norm -= (coords_norm / tile_extent_v_0) * tile_extent_v_0;
      pos += coords_norm * 1;

      coords_norm = (coords[1] - dim_domain_low_1);
      coords_norm -= (coords_norm / tile_extent_v_1) * tile_extent_v_1;
      pos += coords_norm * 1 * tile_extent_v_0;
      break;
    case 3:
      dim_domain_low_0 = *(const T*)&dimensions_[0]->domain()[0];
      tile_extent_v_0 = *(const T*)dimensions_[0]->tile_extent();
      dim_domain_low_1 = *(const T*)&dimensions_[1]->domain()[0];
      tile_extent_v_1 = *(const T*)dimensions_[1]->tile_extent();
      dim_domain_low_2 = *(const T*)&dimensions_[2]->domain()[0];
      tile_extent_v_2 = *(const T*)dimensions_[2]->tile_extent();

      coords_norm = (coords[0] - dim_domain_low_0);
      coords_norm -= (coords_norm / tile_extent_v_0) * tile_extent_v_0;
      pos += coords_norm * 1;

      coords_norm = (coords[1] - dim_domain_low_1);
      coords_norm -= (coords_norm / tile_extent_v_1) * tile_extent_v_1;
      pos += coords_norm * 1 * tile_extent_v_0;

      coords_norm = (coords[2] - dim_domain_low_2);
      coords_norm -= (coords_norm / tile_extent_v_2) * tile_extent_v_2;
      pos += coords_norm * 1 * tile_extent_v_0 * tile_extent_v_1;
      break;
    default: {
      uint64_t cell_offset = 1;
      for (unsigned d = 0; d < dim_num; ++d) {
        auto dim_domain_low = *(const T*)&dimensions_[d]->domain()[0];
        auto tile_extent_v = *(const T*)dimensions_[d]->tile_extent();
        coords_norm = (coords[d] - dim_domain_low);
        coords_norm -= (coords_norm / tile_extent_v) * tile_extent_v;
        pos += coords_norm * cell_offset;
        cell_offset *= tile_extent_v;
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
  auto dim_num = (unsigned)dimensions_.size();
  switch (dim_num) {
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
      for (unsigned d = 0; d < dim_num; ++d) {
        pos += (coords[d] - subarray[2 * d]) * cell_offset;
        cell_offset *= subarray[2 * d + 1] - subarray[2 * d] + 1;
      }
      break;
    }
  }

  return pos;
}

template <class T>
uint64_t Domain::get_cell_pos_row(const T* coords) const {
  uint64_t pos = 0;
  T coords_norm;  // Normalized coordinates inside the tile
  T dim_domain_low_0, dim_domain_low_1, dim_domain_low_2;
  T tile_extent_v_0, tile_extent_v_1, tile_extent_v_2;

  // Special-case for low dimensions to an unrolled version of the default
  // loop.
  auto dim_num = (unsigned)dimensions_.size();
  switch (dim_num) {
    case 1:
      dim_domain_low_0 = *(const T*)&dimensions_[0]->domain()[0];
      tile_extent_v_0 = *(const T*)dimensions_[0]->tile_extent();

      coords_norm = (coords[0] - dim_domain_low_0);
      coords_norm -= (coords_norm / tile_extent_v_0) * tile_extent_v_0;
      pos += coords_norm;
      break;
    case 2:
      dim_domain_low_0 = *(const T*)&dimensions_[0]->domain()[0];
      tile_extent_v_0 = *(const T*)dimensions_[0]->tile_extent();
      dim_domain_low_1 = *(const T*)&dimensions_[1]->domain()[0];
      tile_extent_v_1 = *(const T*)dimensions_[1]->tile_extent();

      coords_norm = (coords[0] - dim_domain_low_0);
      coords_norm -= (coords_norm / tile_extent_v_0) * tile_extent_v_0;
      pos += coords_norm * tile_extent_v_1;

      coords_norm = (coords[1] - dim_domain_low_1);
      coords_norm -= (coords_norm / tile_extent_v_1) * tile_extent_v_1;
      pos += coords_norm * 1;
      break;
    case 3:
      dim_domain_low_0 = *(const T*)&dimensions_[0]->domain()[0];
      tile_extent_v_0 = *(const T*)dimensions_[0]->tile_extent();
      dim_domain_low_1 = *(const T*)&dimensions_[1]->domain()[0];
      tile_extent_v_1 = *(const T*)dimensions_[1]->tile_extent();
      dim_domain_low_2 = *(const T*)&dimensions_[2]->domain()[0];
      tile_extent_v_2 = *(const T*)dimensions_[2]->tile_extent();

      coords_norm = (coords[0] - dim_domain_low_0);
      coords_norm -= (coords_norm / tile_extent_v_0) * tile_extent_v_0;
      pos += coords_norm * tile_extent_v_1 * tile_extent_v_2;

      coords_norm = (coords[1] - dim_domain_low_1);
      coords_norm -= (coords_norm / tile_extent_v_1) * tile_extent_v_1;
      pos += coords_norm * tile_extent_v_2;

      coords_norm = (coords[2] - dim_domain_low_2);
      coords_norm -= (coords_norm / tile_extent_v_2) * tile_extent_v_2;
      pos += coords_norm * 1;
      break;
    default: {
      // Calculate initial cell_offset
      uint64_t cell_offset = 1;
      for (unsigned d = 1; d < dim_num; ++d) {
        auto tile_extent_v = *(const T*)dimensions_[d]->tile_extent();
        cell_offset *= tile_extent_v;
      }

      // Calculate position
      for (unsigned d = 0; d < dim_num; ++d) {
        auto tile_extent_v = *(const T*)dimensions_[d]->tile_extent();
        auto dim_domain_low = *(const T*)&dimensions_[d]->domain()[0];
        coords_norm = (coords[d] - dim_domain_low);
        coords_norm -= (coords_norm / tile_extent_v) * tile_extent_v;
        pos += coords_norm * cell_offset;
        if (d < dim_num - 1) {
          tile_extent_v = *(const T*)dimensions_[d + 1]->tile_extent();
          cell_offset /= tile_extent_v;
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
  auto dim_num = (unsigned)dimensions_.size();
  switch (dim_num) {
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
      for (unsigned d = 0; d < dim_num - 1; ++d) {
        const uint64_t cell_num_i =
            subarray[2 * (d + 1) + 1] - subarray[2 * (d + 1)] + 1;
        cell_offset *= cell_num_i;
      }

      // Calculate position
      for (unsigned d = 0; d < dim_num; ++d) {
        pos += (coords[d] - subarray[2 * d]) * cell_offset;
        if (d < dim_num - 1) {
          const uint64_t cell_num_i =
              subarray[2 * (d + 1) + 1] - subarray[2 * (d + 1)] + 1;
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
  unsigned d = 0;
  ++cell_coords[d];

  auto dim_num = (unsigned)dimensions_.size();
  while (d < dim_num - 1 && cell_coords[d] > domain[2 * d + 1]) {
    cell_coords[d] = domain[2 * d];
    ++cell_coords[++d];
  }

  *coords_retrieved = !(d == dim_num - 1 && cell_coords[d] > domain[2 * d + 1]);
}

template <class T>
void Domain::get_next_cell_coords_row(
    const T* domain, T* cell_coords, bool* coords_retrieved) const {
  auto dim_num = (unsigned)dimensions_.size();
  unsigned d = dim_num - 1;
  ++cell_coords[d];

  while (d > 0 && cell_coords[d] > domain[2 * d + 1]) {
    cell_coords[d] = domain[2 * d];
    ++cell_coords[--d];
  }

  *coords_retrieved = !(d == 0 && cell_coords[d] > domain[2 * d + 1]);
}

void Domain::next_coords_col(const NDRange& domain, NDPoint* coords) const {
  unsigned d = 0;
  dimensions_[d]->add(&(*coords)[d], 1);

  auto dim_num = (unsigned)dimensions_.size();
  while (d < dim_num - 1 &&
         dimensions_[d]->value_after_range((*coords)[d], domain[d])) {
    dimensions_[d]->set_value_to_range_start(&(*coords)[d], domain[d]);
    ++d;
    dimensions_[d]->add(&(*coords)[d], 1);
  }
}

void Domain::next_coords_col(
    const NDRange& domain, NDPoint* coords, bool* in) const {
  unsigned d = 0;
  dimensions_[d]->add(&(*coords)[d], 1);

  auto dim_num = (unsigned)dimensions_.size();
  while (d < dim_num - 1 &&
         dimensions_[d]->value_after_range((*coords)[d], domain[d])) {
    dimensions_[d]->set_value_to_range_start(&(*coords)[d], domain[d]);
    ++d;
    dimensions_[d]->add(&(*coords)[d], 1);
  }

  *in =
      !(d == dim_num - 1 &&
        dimensions_[d]->value_after_range((*coords)[d], domain[d]));
}

void Domain::next_coords_row(const NDRange& domain, NDPoint* coords) const {
  auto dim_num = (unsigned)dimensions_.size();
  unsigned d = dim_num - 1;
  dimensions_[d]->add(&(*coords)[d], 1);

  while (d > 0 && dimensions_[d]->value_after_range((*coords)[d], domain[d])) {
    dimensions_[d]->set_value_to_range_start(&(*coords)[d], domain[d]);
    --d;
    dimensions_[d]->add(&(*coords)[d], 1);
  }
}

void Domain::next_coords_row(
    const NDRange& domain, NDPoint* coords, bool* in) const {
  auto dim_num = (unsigned)dimensions_.size();
  unsigned d = dim_num - 1;
  dimensions_[d]->add(&(*coords)[d], 1);

  while (d > 0 && dimensions_[d]->value_after_range((*coords)[d], domain[d])) {
    dimensions_[d]->set_value_to_range_start(&(*coords)[d], domain[d]);
    --d;
    dimensions_[d]->add(&(*coords)[d], 1);
  }

  *in = !(d == 0 && dimensions_[d]->value_after_range((*coords)[d], domain[d]));
}

uint64_t Domain::tile_pos_col(const NDPoint& tile_coords) const {
  // Calculate position
  uint64_t pos = 0;
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d)
    pos += dimensions_[d]->mul(tile_coords[d], tile_offsets_col_[d]);

  // Return
  return pos;
}

// Note: Both tile domain and tile coords are `uint64_t`
uint64_t Domain::tile_pos_col(
    const NDRange& tile_domain, const NDPoint& tile_coords) const {
  // Calculate tile offsets
  std::vector<uint64_t> tile_offsets;
  tile_offsets.push_back(1);
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 1; d < dim_num; ++d) {
    auto tile_num = dimensions_[d - 1]->tile_num(tile_domain[d - 1]);
    tile_offsets.push_back(tile_offsets.back() * tile_num);
  }

  // Calculate position
  uint64_t pos = 0;
  for (unsigned d = 0; d < dim_num; ++d)
    pos += dimensions_[d]->mul(tile_coords[d], tile_offsets[d]);

  // Return
  return pos;
}

uint64_t Domain::tile_pos_row(const NDPoint& tile_coords) const {
  // Calculate position
  uint64_t pos = 0;
  auto dim_num = (unsigned)dimensions_.size();
  for (unsigned d = 0; d < dim_num; ++d)
    pos += dimensions_[d]->mul(tile_coords[d], tile_offsets_row_[d]);

  // Return
  return pos;
}

uint64_t Domain::tile_pos_row(
    const NDRange& tile_domain, const NDPoint& tile_coords) const {
  // Calculate tile offsets
  std::vector<uint64_t> tile_offsets;
  tile_offsets.push_back(1);
  auto dim_num = (unsigned)dimensions_.size();
  if (dim_num > 1) {
    for (unsigned d = dim_num - 2;; --d) {
      auto tile_num = dimensions_[d + 1]->tile_num(tile_domain[d + 1]);
      tile_offsets.push_back(tile_offsets.back() * tile_num);
      if (d == 0)
        break;
    }
  }
  std::reverse(tile_offsets.begin(), tile_offsets.end());

  // Calculate position
  uint64_t pos = 0;
  for (unsigned d = 0; d < dim_num; ++d)
    pos += dimensions_[d]->mul(tile_coords[d], tile_offsets[d]);

  // Return
  return pos;
}

// Explicit template instantiations
template uint64_t Domain::cell_num<int8_t>(const int8_t* domain) const;
template uint64_t Domain::cell_num<uint8_t>(const uint8_t* domain) const;
template uint64_t Domain::cell_num<int16_t>(const int16_t* domain) const;
template uint64_t Domain::cell_num<uint16_t>(const uint16_t* domain) const;
template uint64_t Domain::cell_num<int>(const int* domain) const;
template uint64_t Domain::cell_num<unsigned>(const unsigned* domain) const;
template uint64_t Domain::cell_num<int64_t>(const int64_t* domain) const;
template uint64_t Domain::cell_num<uint64_t>(const uint64_t* domain) const;
template uint64_t Domain::cell_num<float>(const float* domain) const;
template uint64_t Domain::cell_num<double>(const double* domain) const;

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

template int8_t Domain::floor_to_tile<int8_t>(
    int8_t value, unsigned dim_idx) const;
template uint8_t Domain::floor_to_tile<uint8_t>(
    uint8_t value, unsigned dim_idx) const;
template int16_t Domain::floor_to_tile<int16_t>(
    int16_t value, unsigned dim_idx) const;
template uint16_t Domain::floor_to_tile<uint16_t>(
    uint16_t value, unsigned dim_idx) const;
template int32_t Domain::floor_to_tile<int32_t>(
    int32_t value, unsigned dim_idx) const;
template uint32_t Domain::floor_to_tile<uint32_t>(
    uint32_t value, unsigned dim_idx) const;
template int64_t Domain::floor_to_tile<int64_t>(
    int64_t value, unsigned dim_idx) const;
template uint64_t Domain::floor_to_tile<uint64_t>(
    uint64_t value, unsigned dim_idx) const;
template float Domain::floor_to_tile<float>(
    float value, unsigned dim_idx) const;
template double Domain::floor_to_tile<double>(
    double value, unsigned dim_idx) const;

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
