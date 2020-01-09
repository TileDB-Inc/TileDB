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
  type_ = Datatype::INT32;
  cell_num_per_tile_ = 0;
  domain_ = nullptr;
  tile_extents_ = nullptr;
  tile_domain_ = nullptr;
}

Domain::Domain(Datatype type)
    : type_(type) {
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;
  dim_num_ = 0;
  cell_num_per_tile_ = 0;
  domain_ = nullptr;
  tile_extents_ = nullptr;
  tile_domain_ = nullptr;
}

Domain::Domain(const Domain* domain) {
  cell_num_per_tile_ = domain->cell_num_per_tile_;
  cell_order_ = domain->cell_order_;
  dim_num_ = domain->dim_num_;
  type_ = domain->type_;
  cell_order_cmp_func_ = domain->cell_order_cmp_func_;
  tile_order_cmp_func_ = domain->tile_order_cmp_func_;

  for (auto dim : domain->dimensions_)
    dimensions_.emplace_back(new Dimension(dim));

  uint64_t coords_size = dim_num_ * datatype_size(type_);
  tile_order_ = domain->tile_order_;
  tile_offsets_col_ = domain->tile_offsets_col_;
  tile_offsets_row_ = domain->tile_offsets_row_;

  if (domain->domain_ == nullptr) {
    domain_ = nullptr;
  } else {
    domain_ = std::malloc(2 * coords_size);
    std::memcpy(domain_, domain->domain_, 2 * coords_size);
  }

  if (domain->tile_domain_ == nullptr) {
    tile_domain_ = nullptr;
  } else {
    tile_domain_ = std::malloc(2 * coords_size);
    std::memcpy(tile_domain_, domain->tile_domain_, 2 * coords_size);
  }

  if (domain->tile_extents_ == nullptr) {
    tile_extents_ = nullptr;
  } else {
    tile_extents_ = std::malloc(coords_size);
    std::memcpy(tile_extents_, domain->tile_extents_, coords_size);
  }
}

Domain::~Domain() {
  for (auto dim : dimensions_)
    delete dim;

  std::free(tile_extents_);
  tile_extents_ = nullptr;
  std::free(domain_);
  domain_ = nullptr;
  std::free(tile_domain_);
  tile_domain_ = nullptr;
}

/* ********************************* */
/*                API                */
/* ********************************* */

Layout Domain::cell_order() const {
  return cell_order_;
}

template <class T>
T Domain::floor_to_tile(T value, unsigned dim_idx) const {
  auto domain = (T*)domain_;
  auto tile_extents = (T*)tile_extents_;

  if (tile_extents_ == nullptr)
    return domain[2 * dim_idx];

  uint64_t div = (value - domain[2 * dim_idx]) / tile_extents[dim_idx];
  return (T)div * tile_extents[dim_idx] + domain[2 * dim_idx];
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
  int dim_to_split = -1;
  auto tile_extents = (T*)tile_extents_;
  auto domain = (T*)domain_;
  uint64_t tiles_apart = 0;

  if (tile_extents != nullptr) {
    if (tile_order_ == Layout::ROW_MAJOR) {
      for (int i = 0; i < (int)dim_num_; ++i) {
        tiles_apart =
            (T)floor(((s[2 * i + 1] - domain[2 * i]) / tile_extents[i])) -
            (T)floor(((s[2 * i] - domain[2 * i]) / tile_extents[i]));
        if (tiles_apart != 0) {
          // Not in the same tile - can split
          dim_to_split = i;
          break;
        }
      }
    } else {
      for (int i = (int)dim_num_ - 1;; --i) {
        tiles_apart =
            (T)floor(((s[2 * i + 1] - domain[2 * i]) / tile_extents[i])) -
            (T)floor(((s[2 * i] - domain[2 * i]) / tile_extents[i]));
        if (tiles_apart != 0) {
          // Not in the same tile - can split
          dim_to_split = i;
          break;
        }
        if (i == 0)
          break;
      }
    }
  }

  // Cannot split by tile, split by cell
  if (dim_to_split == -1)
    return split_subarray_cell<T>(
        subarray, cell_order_, subarray_1, subarray_2);

  // Split by tile
  *subarray_1 = std::malloc(2 * dim_num_ * sizeof(T));
  if (*subarray_1 == nullptr)
    return LOG_STATUS(
        Status::DomainError("Cannot split subarray; Memory allocation failed"));
  *subarray_2 = std::malloc(2 * dim_num_ * sizeof(T));
  if (*subarray_2 == nullptr) {
    std::free(subarray_1);
    *subarray_1 = nullptr;
    return LOG_STATUS(
        Status::DomainError("Cannot split subarray; Memory allocation failed"));
  }
  auto s1 = (T*)(*subarray_1);
  auto s2 = (T*)(*subarray_2);

  for (int i = 0; i < (int)dim_num_; ++i) {
    if (i != dim_to_split) {
      s1[2 * i] = s[2 * i];
      s1[2 * i + 1] = s[2 * i + 1];
      s2[2 * i] = s[2 * i];
      s2[2 * i + 1] = s[2 * i + 1];
    } else {
      s1[2 * i] = s[2 * i];
      s1[2 * i + 1] =
          s1[2 * i] + std::max<T>(1, floor(tiles_apart / 2)) * tile_extents[i];

      if (std::numeric_limits<T>::is_integer) {
        s1[2 * i + 1] = floor_to_tile(s1[2 * i + 1], i) - 1;
        s2[2 * i] = s1[2 * i + 1] + 1;
      } else {
        s2[2 * i] = floor_to_tile(s1[2 * i + 1], i);
        s1[2 * i + 1] =
            std::nextafter(s2[2 * i], std::numeric_limits<T>::lowest());
      }
      s2[2 * i + 1] = s[2 * i + 1];

      assert(s1[2 * i + 1] >= s1[2 * i]);
      assert(s2[2 * i + 1] >= s2[2 * i]);
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
  int dim_to_split = -1;

  if (cell_layout == Layout::ROW_MAJOR) {
    for (int i = 0; i < (int)dim_num_; ++i) {
      if (s[2 * i] != s[2 * i + 1]) {
        dim_to_split = i;
        break;
      }
    }
  } else {
    for (int i = (int)dim_num_ - 1;; --i) {
      if (s[2 * i] != s[2 * i + 1]) {
        dim_to_split = i;
        break;
      }
      if (i == 0)
        break;
    }
  }

  // Cannot split
  if (dim_to_split == -1) {
    *subarray_1 = nullptr;
    *subarray_2 = nullptr;
    return Status::Ok();
  }

  // Split
  *subarray_1 = std::malloc(2 * dim_num_ * sizeof(T));
  if (*subarray_1 == nullptr)
    return LOG_STATUS(
        Status::DomainError("Cannot split subarray; Memory allocation failed"));
  *subarray_2 = std::malloc(2 * dim_num_ * sizeof(T));
  if (*subarray_2 == nullptr) {
    std::free(subarray_1);
    *subarray_1 = nullptr;
    return LOG_STATUS(
        Status::DomainError("Cannot split subarray; Memory allocation failed"));
  }
  auto s1 = (T*)(*subarray_1);
  auto s2 = (T*)(*subarray_2);
  for (int i = 0; i < (int)dim_num_; ++i) {
    if (i != dim_to_split) {
      s1[2 * i] = s[2 * i];
      s1[2 * i + 1] = s[2 * i + 1];
      s2[2 * i] = s[2 * i];
      s2[2 * i + 1] = s[2 * i + 1];
    } else {
      s1[2 * i] = s[2 * i];
      if (std::numeric_limits<T>::is_integer) {  // Integers
        s1[2 * i + 1] = s[2 * i] + (s[2 * i + 1] - s[2 * i]) / 2;
        s2[2 * i] = s1[2 * i + 1] + 1;
      } else {  // Reals
        if (std::nextafter(s[2 * i], std::numeric_limits<T>::max()) ==
            s[2 * i + 1]) {
          s1[2 * i + 1] = s[2 * i];
          s2[2 * i] = s[2 * i + 1];
        } else {
          s1[2 * i + 1] = s[2 * i] + (s[2 * i + 1] - s[2 * i]) / 2;
          s2[2 * i] =
              std::nextafter(s1[2 * i + 1], std::numeric_limits<T>::max());
        }
      }
      s2[2 * i + 1] = s[2 * i + 1];
    }
  }

  return Status::Ok();
}

Status Domain::add_dimension(const Dimension* dim) {
  // Set domain type and do sanity check
  if (dim_num_ == 0)
    type_ = dim->type();
  else if (dim->type() != type_)
    return LOG_STATUS(
        Status::DomainError("Cannot add dimension to domain; All added "
                            "dimensions must have the same type"));

  // Compute new dimension name
  std::string new_dim_name = dim->name();
  if (new_dim_name.empty())
    new_dim_name = default_dimension_name(dim_num_);

  auto new_dim = new Dimension(new_dim_name, type_);
  RETURN_NOT_OK_ELSE(new_dim->set_domain(dim->domain()), delete new_dim);
  RETURN_NOT_OK_ELSE(
      new_dim->set_tile_extent(dim->tile_extent()), delete new_dim);

  dimensions_.emplace_back(new_dim);
  ++dim_num_;

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

  uint64_t cell_num = 1, range, prod;
  for (unsigned i = 0; i < dim_num_; ++i) {
    // The code below essentially computes
    // cell_num *= domain[2 * i + 1] - domain[2 * i] + 1;
    // while performing overflow checks
    range = domain[2 * i + 1] - domain[2 * i];
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

void Domain::crop_domain(void* domain) const {
  switch (type_) {
    case Datatype::INT32:
      crop_domain<int>(static_cast<int*>(domain));
      break;
    case Datatype::INT64:
      crop_domain<int64_t>(static_cast<int64_t*>(domain));
      break;
    case Datatype::INT8:
      crop_domain<int8_t>(static_cast<int8_t*>(domain));
      break;
    case Datatype::UINT8:
      crop_domain<uint8_t>(static_cast<uint8_t*>(domain));
      break;
    case Datatype::INT16:
      crop_domain<int16_t>(static_cast<int16_t*>(domain));
      break;
    case Datatype::UINT16:
      crop_domain<uint16_t>(static_cast<uint16_t*>(domain));
      break;
    case Datatype::UINT32:
      crop_domain<uint32_t>(static_cast<uint32_t*>(domain));
      break;
    case Datatype::UINT64:
      crop_domain<uint64_t>(static_cast<uint64_t*>(domain));
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
      crop_domain<int64_t>(static_cast<int64_t*>(domain));
      break;
    default:  // Non-applicable to non-integer domains
      break;
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
  RETURN_NOT_OK(buff->read(&dim_num_, sizeof(uint32_t)));
  for (uint32_t i = 0; i < dim_num_; ++i) {
    auto dim = new Dimension();
    dim->deserialize(buff, type_);
    dimensions_.emplace_back(dim);
  }

  set_tile_cell_order_cmp_funcs();

  return Status::Ok();
}

unsigned int Domain::dim_num() const {
  return dim_num_;
}

const void* Domain::domain() const {
  return domain_;
}

const void* Domain::domain(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;
  return dimensions_[i]->domain();
}

const Dimension* Domain::dimension(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;
  return dimensions_[i];
}

const Dimension* Domain::dimension(std::string name) const {
  for (unsigned int i = 0; i < dim_num_; i++) {
    auto dim = dimensions_[i];
    if (dim->name() == name) {
      return dim;
    }
  }
  return nullptr;
}

void Domain::dump(FILE* out) const {
  fprintf(out, "=== Domain ===\n");
  fprintf(out, "- Dimensions type: %s\n", datatype_str(type_).c_str());

  for (auto& dim : dimensions_) {
    fprintf(out, "\n");
    dim->dump(out);
  }
}

void Domain::expand_domain(void* domain) const {
  switch (type_) {
    case Datatype::INT32:
      expand_domain<int>(static_cast<int*>(domain));
      break;
    case Datatype::INT64:
      expand_domain<int64_t>(static_cast<int64_t*>(domain));
      break;
    case Datatype::INT8:
      expand_domain<int8_t>(static_cast<int8_t*>(domain));
      break;
    case Datatype::UINT8:
      expand_domain<uint8_t>(static_cast<uint8_t*>(domain));
      break;
    case Datatype::INT16:
      expand_domain<int16_t>(static_cast<int16_t*>(domain));
      break;
    case Datatype::UINT16:
      expand_domain<uint16_t>(static_cast<uint16_t*>(domain));
      break;
    case Datatype::UINT32:
      expand_domain<uint32_t>(static_cast<uint32_t*>(domain));
      break;
    case Datatype::UINT64:
      expand_domain<uint64_t>(static_cast<uint64_t*>(domain));
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
      expand_domain<int64_t>(static_cast<int64_t*>(domain));
      break;
    default:  // Non-applicable to non-integer domains
      break;
  }
}

template <class T>
void Domain::get_tile_coords(const T* coords, T* tile_coords) const {
  auto domain = (T*)domain_;
  auto tile_extents = (T*)tile_extents_;
  for (unsigned i = 0; i < dim_num_; i++)
    tile_coords[i] = (coords[i] - domain[2 * i]) / tile_extents[i];
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
    auto domain = (T*)domain_;
    auto tile_extents = (T*)tile_extents_;

    if (cell_order_ == Layout::ROW_MAJOR) {
      for (unsigned i = 0; i < dim_num_; ++i)
        end[i] = start[i];
      end[dim_num_ - 1] += tile_extents[dim_num_ - 1] -
                           ((start[dim_num_ - 1] - domain[2 * (dim_num_ - 1)]) %
                            tile_extents[dim_num_ - 1]) -
                           1;
      end[dim_num_ - 1] =
          std::min(end[dim_num_ - 1], subarray[2 * (dim_num_ - 1) + 1]);
    } else {
      for (unsigned i = 0; i < dim_num_; ++i)
        end[i] = start[i];
      end[0] +=
          tile_extents[0] - ((start[0] - domain[0]) % tile_extents[0]) - 1;
      end[0] = std::min(end[0], subarray[1]);
    }
  } else {
    for (unsigned i = 0; i < dim_num_; ++i)
      end[i] = start[i];
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
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_subarray[2 * i] = (subarray[2 * i] - domain[2 * i]) / tile_extents[i];
    tile_subarray[2 * i + 1] =
        (subarray[2 * i + 1] - domain[2 * i]) / tile_extents[i];
  }
}

template <class T>
uint64_t Domain::get_tile_pos(const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return get_tile_pos_row(tile_coords);

  // ROW MAJOR
  return get_tile_pos_col(tile_coords);
}

template <class T>
uint64_t Domain::get_tile_pos(const T* domain, const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return get_tile_pos_row(domain, tile_coords);
  // COL_MAJOR
  return get_tile_pos_col(domain, tile_coords);
}

template <class T>
void Domain::get_tile_subarray(const T* tile_coords, T* tile_subarray) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_subarray[2 * i] = tile_coords[i] * tile_extents[i] + domain[2 * i];
    tile_subarray[2 * i + 1] =
        (tile_coords[i] + 1) * tile_extents[i] - 1 + domain[2 * i];
  }
}

template <class T>
void Domain::get_tile_subarray(
    const T* domain, const T* tile_coords, T* tile_subarray) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_subarray[2 * i] = tile_coords[i] * tile_extents[i] + domain[2 * i];
    tile_subarray[2 * i + 1] =
        (tile_coords[i] + 1) * tile_extents[i] - 1 + domain[2 * i];
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

  // Set domain
  uint64_t coord_size = datatype_size(type_);
  uint64_t coords_size = dim_num_ * coord_size;
  std::free(domain_);
  domain_ = std::malloc(dim_num_ * 2 * coord_size);
  auto domain = (char*)domain_;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    std::memcpy(domain + i * 2 * coord_size, this->domain(i), 2 * coord_size);
  }

  // Set tile extents
  std::free(tile_extents_);
  if (null_tile_extents()) {
    tile_extents_ = nullptr;
  } else {
    tile_extents_ = std::malloc(coords_size);
    auto tile_extents = (char*)tile_extents_;
    for (unsigned int i = 0; i < dim_num_; ++i) {
      std::memcpy(tile_extents + i * coord_size, tile_extent(i), coord_size);
    }
  }

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

bool Domain::null_tile_extents() const {
  for (unsigned int i = 0; i < dim_num_; ++i) {
    if (tile_extent(i) == nullptr)
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
Status Domain::serialize(Buffer* buff) {
  // Write type
  auto type = static_cast<uint8_t>(type_);
  RETURN_NOT_OK(buff->write(&type, sizeof(uint8_t)));

  // Write dimensions
  RETURN_NOT_OK(buff->write(&dim_num_, sizeof(uint32_t)));
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
  if (dim_num_ == 1 || subarray_layout == Layout::GLOBAL_ORDER ||
      subarray_layout == cell_order_)
    return UINT64_MAX;

  uint64_t ret = 1;
  if (cell_order_ == Layout::ROW_MAJOR) {
    for (unsigned i = 1; i < dim_num_; ++i)
      ret *= *(const T*)tile_extent(i);
  } else {  // COL_MAJOR
    for (unsigned i = 0; i < dim_num_ - 1; ++i)
      ret *= *(const T*)tile_extent(i);
  }

  return ret;
}

const void* Domain::tile_extent(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;

  return dimensions_[i]->tile_extent();
}

const void* Domain::tile_extents() const {
  return tile_extents_;
}

uint64_t Domain::tile_num(const void* range) const {
  switch (type_) {
    case Datatype::INT32:
      return tile_num<int>(static_cast<const int*>(range));
    case Datatype::INT64:
      return tile_num<int64_t>(static_cast<const int64_t*>(range));
    case Datatype::INT8:
      return tile_num<int8_t>(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return tile_num<uint8_t>(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return tile_num<int16_t>(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return tile_num<uint16_t>(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return tile_num<uint32_t>(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return tile_num<uint64_t>(static_cast<const uint64_t*>(range));
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
      return tile_num<int64_t>(static_cast<const int64_t*>(range));
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
      // Operation not supported for float domains
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
      return 0;
  }

  assert(false);
  return 0;
}

template <class T>
int Domain::tile_order_cmp(
    const Dimension* dim, const void* coord_a, const void* coord_b) {
  auto tile_extent = (T*)dim->tile_extent();
  if (tile_extent == nullptr)
    return 0;

  auto ca = (T*)coord_a;
  auto cb = (T*)coord_b;
  auto domain = (T*)dim->domain();
  auto ta = (T)((*ca - domain[0]) / *tile_extent);
  auto tb = (T)((*cb - domain[0]) / *tile_extent);
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

  // Applicable only to non-NULL space tiles
  if (tile_extents_ == nullptr)
    return;

  cell_num_per_tile_ = 1;
  auto tile_extents = static_cast<const T*>(tile_extents_);
  for (unsigned int i = 0; i < dim_num_; ++i)
    cell_num_per_tile_ *= tile_extents[i];
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
  tile_order_cmp_func_.resize(dim_num_);
  cell_order_cmp_func_.resize(dim_num_);
  for (unsigned d = 0; d < dim_num_; ++d) {
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
  if (tile_extents_ == nullptr)
    return;

  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Allocate space for the tile domain
  assert(tile_domain_ == nullptr);
  tile_domain_ = std::malloc(2 * dim_num_ * sizeof(T));

  // For easy reference
  auto tile_domain = static_cast<T*>(tile_domain_);
  T tile_num;  // Per dimension

  // Calculate tile domain
  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_num =
        ceil(double(domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i]);
    tile_domain[2 * i] = 0;
    tile_domain[2 * i + 1] = tile_num - 1;
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
  //  // Non-applicable to real domains
  //  if(!std::is_integral<T>())
  //    return;

  // Applicable only to non-NULL space tiles
  if (tile_extents_ == nullptr)
    return;

  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);
  uint64_t tile_num;  // Per dimension

  // Calculate tile offsets for column-major tile order
  tile_offsets_col_.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned int i = 1; i < dim_num_; ++i) {
      tile_num = utils::math::ceil(
          domain[2 * (i - 1) + 1] - domain[2 * (i - 1)] + 1,
          tile_extents[i - 1]);
      tile_offsets_col_.push_back(tile_offsets_col_.back() * tile_num);
    }
  }

  // Calculate tile offsets for row-major tile order
  tile_offsets_row_.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2;; --i) {
      tile_num = utils::math::ceil(
          domain[2 * (i + 1) + 1] - domain[2 * (i + 1)] + 1,
          tile_extents[i + 1]);
      tile_offsets_row_.push_back(tile_offsets_row_.back() * tile_num);
      if (i == 0)
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
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  uint64_t pos = 0;
  T coords_norm;  // Normalized coordinates inside the tile

  // Special-case for low dimensions to an unrolled version of the default
  // loop.
  switch (dim_num_) {
    case 1:
      coords_norm = (coords[0] - domain[2 * 0]);
      coords_norm -= (coords_norm / tile_extents[0]) * tile_extents[0];
      pos += coords_norm * 1;
      break;
    case 2:
      coords_norm = (coords[0] - domain[2 * 0]);
      coords_norm -= (coords_norm / tile_extents[0]) * tile_extents[0];
      pos += coords_norm * 1;

      coords_norm = (coords[1] - domain[2 * 1]);
      coords_norm -= (coords_norm / tile_extents[1]) * tile_extents[1];
      pos += coords_norm * 1 * tile_extents[0];
      break;
    case 3:
      coords_norm = (coords[0] - domain[2 * 0]);
      coords_norm -= (coords_norm / tile_extents[0]) * tile_extents[0];
      pos += coords_norm * 1;

      coords_norm = (coords[1] - domain[2 * 1]);
      coords_norm -= (coords_norm / tile_extents[1]) * tile_extents[1];
      pos += coords_norm * 1 * tile_extents[0];

      coords_norm = (coords[2] - domain[2 * 2]);
      coords_norm -= (coords_norm / tile_extents[2]) * tile_extents[2];
      pos += coords_norm * 1 * tile_extents[0] * tile_extents[1];
      break;
    default: {
      uint64_t cell_offset = 1;
      for (unsigned int i = 0; i < dim_num_; ++i) {
        coords_norm = (coords[i] - domain[2 * i]);
        coords_norm -= (coords_norm / tile_extents[i]) * tile_extents[i];
        pos += coords_norm * cell_offset;
        cell_offset *= tile_extents[i];
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
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  uint64_t pos = 0;
  T coords_norm;  // Normalized coordinates inside the tile

  // Special-case for low dimensions to an unrolled version of the default
  // loop.
  switch (dim_num_) {
    case 1:
      coords_norm = (coords[0] - domain[2 * 0]);
      coords_norm -= (coords_norm / tile_extents[0]) * tile_extents[0];
      pos += coords_norm;
      break;
    case 2:
      coords_norm = (coords[0] - domain[2 * 0]);
      coords_norm -= (coords_norm / tile_extents[0]) * tile_extents[0];
      pos += coords_norm * tile_extents[1];

      coords_norm = (coords[1] - domain[2 * 1]);
      coords_norm -= (coords_norm / tile_extents[1]) * tile_extents[1];
      pos += coords_norm * 1;
      break;
    case 3:
      coords_norm = (coords[0] - domain[2 * 0]);
      coords_norm -= (coords_norm / tile_extents[0]) * tile_extents[0];
      pos += coords_norm * tile_extents[1] * tile_extents[2];

      coords_norm = (coords[1] - domain[2 * 1]);
      coords_norm -= (coords_norm / tile_extents[1]) * tile_extents[1];
      pos += coords_norm * tile_extents[2];

      coords_norm = (coords[2] - domain[2 * 2]);
      coords_norm -= (coords_norm / tile_extents[2]) * tile_extents[2];
      pos += coords_norm * 1;
      break;
    default: {
      // Calculate initial cell_offset
      uint64_t cell_offset = 1;
      for (unsigned int i = 1; i < dim_num_; ++i)
        cell_offset *= tile_extents[i];

      // Calculate position
      for (unsigned int i = 0; i < dim_num_; ++i) {
        coords_norm = (coords[i] - domain[2 * i]);
        coords_norm -= (coords_norm / tile_extents[i]) * tile_extents[i];
        pos += coords_norm * cell_offset;
        if (i < dim_num_ - 1)
          cell_offset /= tile_extents[i + 1];
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
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  std::vector<uint64_t> tile_offsets;
  tile_offsets.push_back(1);
  for (unsigned int i = 1; i < dim_num_; ++i) {
    // Per dimension
    uint64_t tile_num;
    if (&typeid(T) != &typeid(float) && &typeid(T) != &typeid(double))
      tile_num = (domain[2 * (i - 1) + 1] - domain[2 * (i - 1)] + 1) /
                 tile_extents[i - 1];
    else
      tile_num =
          (domain[2 * (i - 1) + 1] - domain[2 * (i - 1)]) / tile_extents[i - 1];
    tile_offsets.push_back(tile_offsets.back() * tile_num);
  }

  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets[i];

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
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  std::vector<uint64_t> tile_offsets;
  tile_offsets.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2;; --i) {
      // Per dimension
      uint64_t tile_num;
      if (&typeid(T) != &typeid(float) && &typeid(T) != &typeid(double))
        tile_num = (domain[2 * (i + 1) + 1] - domain[2 * (i + 1)] + 1) /
                   tile_extents[i + 1];
      else
        tile_num = (domain[2 * (i + 1) + 1] - domain[2 * (i + 1)]) /
                   tile_extents[i + 1];
      tile_offsets.push_back(tile_offsets.back() * tile_num);
      if (i == 0)
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
