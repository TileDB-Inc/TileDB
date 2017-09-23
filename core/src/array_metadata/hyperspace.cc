/**
 * @file   hyperspace.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements class Hyperspace.
 */

#include "hyperspace.h"
#include "logger.h"

#include <cassert>
#include <iostream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Hyperspace::Hyperspace() {
  dim_num_ = 0;
  type_ = Datatype::INT32;
  cell_num_per_tile_ = 0;
  domain_ = nullptr;
  tile_extents_ = nullptr;
  tile_domain_ = nullptr;
  tile_coords_aux_ = nullptr;
}

Hyperspace::Hyperspace(Datatype type)
    : type_(type) {
  dim_num_ = 0;
  cell_num_per_tile_ = 0;
  domain_ = nullptr;
  tile_extents_ = nullptr;
  tile_domain_ = nullptr;
  tile_coords_aux_ = nullptr;
}

Hyperspace::Hyperspace(const Hyperspace* hyperspace) {
  cell_num_per_tile_ = hyperspace->cell_num_per_tile_;
  cell_order_ = hyperspace->cell_order_;
  dim_num_ = hyperspace->dim_num_;
  for (auto dim : hyperspace->dimensions_)
    dimensions_.emplace_back(new Dimension(dim));
  type_ = hyperspace->type_;

  uint64_t coords_size = dim_num_ * datatype_size(type_);

  tile_order_ = hyperspace->tile_order_;
  tile_coords_aux_ = std::malloc(coords_size);
  tile_offsets_col_ = hyperspace->tile_offsets_col_;
  tile_offsets_row_ = hyperspace->tile_offsets_row_;

  if (hyperspace->domain_ == nullptr) {
    domain_ = nullptr;
  } else {
    domain_ = std::malloc(2 * coords_size);
    std::memcpy(domain_, hyperspace->domain_, 2 * coords_size);
  }

  if (hyperspace->tile_domain_ == nullptr) {
    tile_domain_ = nullptr;
  } else {
    tile_domain_ = std::malloc(2 * coords_size);
    std::memcpy(tile_domain_, hyperspace->tile_domain_, 2 * coords_size);
  }

  if (hyperspace->tile_extents_ == nullptr) {
    tile_extents_ = nullptr;
  } else {
    tile_extents_ = std::malloc(coords_size);
    std::memcpy(tile_extents_, hyperspace->tile_extents_, coords_size);
  }
}

Hyperspace::~Hyperspace() {
  for (auto dim : dimensions_)
    delete dim;

  if (tile_extents_ != nullptr) {
    std::free(tile_extents_);
    tile_extents_ = nullptr;
  }
  if (domain_ != nullptr) {
    std::free(domain_);
    domain_ = nullptr;
  }
  if (tile_domain_ != nullptr) {
    std::free(tile_domain_);
    tile_domain_ = nullptr;
  }
  if (tile_coords_aux_ != nullptr) {
    std::free(tile_coords_aux_);
    tile_coords_aux_ = nullptr;
  }
}

/* ********************************* */
/*                API                */
/* ********************************* */

Status Hyperspace::add_dimension(
    const char* name, const void* domain, const void* tile_extent) {
  auto dim = new Dimension(name, type_);
  RETURN_NOT_OK_ELSE(dim->set_domain(domain), delete dim);
  RETURN_NOT_OK_ELSE(dim->set_tile_extent(tile_extent), delete dim);

  dimensions_.emplace_back(dim);
  ++dim_num_;

  return Status::Ok();
}

uint64_t Hyperspace::cell_num_per_tile() const {
  return cell_num_per_tile_;
}

template <class T>
int Hyperspace::cell_order_cmp(const T* coords_a, const T* coords_b) const {
  // Check if they are equal
  if (std::memcmp(coords_a, coords_b, dim_num_ * datatype_size(type_)) == 0)
    return 0;

  // Check for precedence
  if (cell_order_ == Layout::COL_MAJOR) {  // COLUMN-MAJOR
    for (unsigned int i = dim_num_ - 1;; --i) {
      if (coords_a[i] < coords_b[i])
        return -1;
      if (coords_a[i] > coords_b[i])
        return 1;
      if (i == 0)
        break;
    }
  } else if (cell_order_ == Layout::ROW_MAJOR) {  // ROW-MAJOR
    for (unsigned int i = 0; i < dim_num_; ++i) {
      if (coords_a[i] < coords_b[i])
        return -1;
      if (coords_a[i] > coords_b[i])
        return 1;
    }
  } else {  // Invalid cell order
    assert(0);
  }

  // The program should never reach this point
  assert(0);
  return 0;
}

// ===== FORMAT =====
// type (char)
// dim_num (unsigned int)
// dimension #1
// dimension #2
// ...
Status Hyperspace::deserialize(ConstBuffer* buff) {
  // Load type
  char type;
  RETURN_NOT_OK(buff->read(&type, sizeof(char)));
  type_ = static_cast<Datatype>(type);

  // Load dimensions
  RETURN_NOT_OK(buff->read(&dim_num_, sizeof(unsigned int)));
  for (unsigned int i = 0; i < dim_num_; ++i) {
    auto dim = new Dimension();
    dim->deserialize(buff, type_);
    dimensions_.emplace_back(dim);
  }

  return Status::Ok();
}

unsigned int Hyperspace::dim_num() const {
  return dim_num_;
}

const void* Hyperspace::domain() const {
  return domain_;
}

const void* Hyperspace::domain(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;

  return dimensions_[i]->domain();
}

const Dimension* Hyperspace::dimension(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;

  return dimensions_[i];
}

void Hyperspace::dump(FILE* out) const {
  const char* type_s = datatype_str(type_);

  fprintf(out, "=== Hyperspace ===\n");
  fprintf(out, "- Dimensions type: %s\n", type_s);

  for (auto& dim : dimensions_) {
    fprintf(out, "\n");
    dim->dump(out);
  }
}

void Hyperspace::expand_domain(void* domain) const {
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
    default:
      assert(0);
  }
}

template <class T>
void Hyperspace::expand_domain(T* domain) const {
  // Applicable only to regular tiles
  if (tile_extents_ == nullptr)
    return;

  auto tile_extents = static_cast<const T*>(tile_extents_);
  auto array_domain = static_cast<const T*>(domain_);

  for (unsigned int i = 0; i < dim_num_; ++i) {
    domain[2 * i] = ((domain[2 * i] - array_domain[2 * i]) / tile_extents[i] *
                     tile_extents[i]) +
                    array_domain[2 * i];
    domain[2 * i + 1] =
        ((domain[2 * i + 1] - array_domain[2 * i]) / tile_extents[i] + 1) *
            tile_extents[i] -
        1 + array_domain[2 * i];
  }
}

template <class T>
Status Hyperspace::get_cell_pos(const T* coords, uint64_t* pos) const {
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
      Status::HyperspaceError("Cannot get cell position; Invalid cell order"));
}

template <class T>
void Hyperspace::get_next_cell_coords(
    const T* domain, T* cell_coords, bool* coords_retrieved) const {
  // Invoke the proper function based on the tile order
  if (cell_order_ == Layout::ROW_MAJOR)
    get_next_cell_coords_row(domain, cell_coords, coords_retrieved);
  else if (cell_order_ == Layout::COL_MAJOR)
    get_next_cell_coords_col(domain, cell_coords, coords_retrieved);
  else  // Sanity check
    assert(0);
}

template <class T>
void Hyperspace::get_next_tile_coords(const T* domain, T* tile_coords) const {
  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    get_next_tile_coords_row(domain, tile_coords);
  else if (tile_order_ == Layout::COL_MAJOR)
    get_next_tile_coords_col(domain, tile_coords);
  else  // Sanity check
    assert(0);
}

template <class T>
void Hyperspace::get_previous_cell_coords(
    const T* domain, T* cell_coords) const {
  // Invoke the proper function based on the tile order
  if (cell_order_ == Layout::ROW_MAJOR)
    get_previous_cell_coords_row(domain, cell_coords);
  else if (cell_order_ == Layout::COL_MAJOR)
    get_previous_cell_coords_col(domain, cell_coords);
  else  // Sanity check
    assert(0);
}

template <class T>
void Hyperspace::get_subarray_tile_domain(
    const T* subarray, T* tile_domain, T* subarray_tile_domain) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Get tile domain
  T tile_num;  // Per dimension
  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_num =
        ceil(double(domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i]);
    tile_domain[2 * i] = 0;
    tile_domain[2 * i + 1] = tile_num - 1;
  }

  // Calculate subarray in tile domain
  for (unsigned int i = 0; i < dim_num_; ++i) {
    subarray_tile_domain[2 * i] =
        MAX((subarray[2 * i] - domain[2 * i]) / tile_extents[i],
            tile_domain[2 * i]);
    subarray_tile_domain[2 * i + 1] =
        MIN((subarray[2 * i + 1] - domain[2 * i]) / tile_extents[i],
            tile_domain[2 * i + 1]);
  }
}

template <class T>
uint64_t Hyperspace::get_tile_pos(const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return get_tile_pos_row(tile_coords);

  // ROW MAJOR
  return get_tile_pos_col(tile_coords);
}

template <class T>
uint64_t Hyperspace::get_tile_pos(const T* domain, const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    return get_tile_pos_row(domain, tile_coords);
  // COL_MAJOR
  return get_tile_pos_col(domain, tile_coords);
}

template <class T>
void Hyperspace::get_tile_subarray(
    const T* tile_coords, T* tile_subarray) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_subarray[2 * i] = tile_coords[i] * tile_extents[i] + domain[2 * i];
    tile_subarray[2 * i + 1] =
        (tile_coords[i] + 1) * tile_extents[i] - 1 + domain[2 * i];
  }
}

Status Hyperspace::init(Layout cell_order, Layout tile_order) {
  // Set cell and tile order
  cell_order_ = cell_order;
  tile_order_ = tile_order;

  // Set domain
  uint64_t coord_size = datatype_size(type_);
  uint64_t coords_size = dim_num_ * coord_size;
  if (domain_ != nullptr)
    std::free(domain_);
  domain_ = std::malloc(dim_num_ * 2 * coord_size);
  auto domain = (char*)domain_;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    std::memcpy(domain + i * 2 * coord_size, this->domain(i), 2 * coord_size);
  }

  // Set tile extents
  // TODO: this needs mild fixing
  if (tile_extents_ != nullptr)
    std::free(tile_extents_);
  if (tile_extent(0) == nullptr) {
    tile_extents_ = nullptr;
  } else {
    tile_extents_ = std::malloc(dim_num_ * coord_size);
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

  // Initialize static auxiliary variables
  if (tile_coords_aux_ != nullptr)
    std::free(tile_coords_aux_);
  tile_coords_aux_ = std::malloc(coords_size);

  return Status::Ok();
}

bool Hyperspace::is_contained_in_tile_slab_col(const void* range) const {
  switch (type_) {
    case Datatype::INT32:
      return is_contained_in_tile_slab_col(static_cast<const int*>(range));
    case Datatype::INT64:
      return is_contained_in_tile_slab_col(static_cast<const int64_t*>(range));
    case Datatype::FLOAT32:
      return is_contained_in_tile_slab_col(static_cast<const float*>(range));
    case Datatype::FLOAT64:
      return is_contained_in_tile_slab_col(static_cast<const double*>(range));
    case Datatype::INT8:
      return is_contained_in_tile_slab_col(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return is_contained_in_tile_slab_col(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return is_contained_in_tile_slab_col(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return is_contained_in_tile_slab_col(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return is_contained_in_tile_slab_col(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return is_contained_in_tile_slab_col(static_cast<const uint64_t*>(range));
    default:
      return false;
  }
}

template <class T>
bool Hyperspace::is_contained_in_tile_slab_col(const T* range) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);
  uint64_t tile_l, tile_h;

  // Check if range is not contained in a column tile slab
  for (unsigned int i = 1; i < dim_num_; ++i) {
    tile_l =
        (uint64_t)floor(double(range[2 * i] - domain[2 * i]) / tile_extents[i]);
    tile_h = (uint64_t)floor(
        double(range[2 * i + 1] - domain[2 * i]) / tile_extents[i]);
    if (tile_l != tile_h)
      return false;
  }

  // Range contained in the column tile slab
  return true;
}

bool Hyperspace::is_contained_in_tile_slab_row(const void* range) const {
  switch (type_) {
    case Datatype::INT32:
      return is_contained_in_tile_slab_row(static_cast<const int*>(range));
    case Datatype::INT64:
      return is_contained_in_tile_slab_row(static_cast<const int64_t*>(range));
    case Datatype::FLOAT32:
      return is_contained_in_tile_slab_row(static_cast<const float*>(range));
    case Datatype::FLOAT64:
      return is_contained_in_tile_slab_row(static_cast<const double*>(range));
    case Datatype::INT8:
      return is_contained_in_tile_slab_row(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return is_contained_in_tile_slab_row(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return is_contained_in_tile_slab_row(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return is_contained_in_tile_slab_row(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return is_contained_in_tile_slab_row(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return is_contained_in_tile_slab_row(static_cast<const uint64_t*>(range));
    default:
      return false;
  }
}

template <class T>
bool Hyperspace::is_contained_in_tile_slab_row(const T* range) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);
  uint64_t tile_l, tile_h;

  // Check if range is not contained in a row tile slab
  for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
    tile_l =
        (uint64_t)floor(double(range[2 * i] - domain[2 * i]) / tile_extents[i]);
    tile_h = (uint64_t)floor(
        double(range[2 * i + 1] - domain[2 * i]) / tile_extents[i]);
    if (tile_l != tile_h)
      return false;
  }

  // Range contained in the row tile slab
  return true;
}

// ===== FORMAT =====
// type (char)
// dim_num (unsigned int)
// dimension #1
// dimension #2
// ...
Status Hyperspace::serialize(Buffer* buff) {
  // Write type
  auto type = static_cast<char>(type_);
  RETURN_NOT_OK(buff->write(&type, sizeof(char)));

  // Write dimensions
  RETURN_NOT_OK(buff->write(&dim_num_, sizeof(unsigned int)));
  for (auto dim : dimensions_)
    dim->serialize(buff);

  return Status::Ok();
}

template <class T>
unsigned int Hyperspace::subarray_overlap(
    const T* subarray_a, const T* subarray_b, T* overlap_subarray) const {
  // Get overlap range
  for (unsigned int i = 0; i < dim_num_; ++i) {
    overlap_subarray[2 * i] = MAX(subarray_a[2 * i], subarray_b[2 * i]);
    overlap_subarray[2 * i + 1] =
        MIN(subarray_a[2 * i + 1], subarray_b[2 * i + 1]);
  }

  // Check overlap
  unsigned int overlap = 1;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    if (overlap_subarray[2 * i] > subarray_b[2 * i + 1] ||
        overlap_subarray[2 * i + 1] < subarray_b[2 * i]) {
      overlap = 0;
      break;
    }
  }

  // Check partial overlap
  if (overlap == 1) {
    for (unsigned int i = 0; i < dim_num_; ++i) {
      if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
          overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
        overlap = 2;
        break;
      }
    }
  }

  // Check contig overlap
  if (overlap == 2 && dim_num_ > 1) {
    overlap = 3;
    if (cell_order_ == Layout::ROW_MAJOR) {  // Row major
      for (unsigned int i = 1; i < dim_num_; ++i) {
        if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
            overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
          overlap = 2;
          break;
        }
      }
    } else if (cell_order_ == Layout::COL_MAJOR) {  // Column major
      if (dim_num_ > 1) {
        for (unsigned int i = dim_num_ - 2;; --i) {
          if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
              overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
            overlap = 2;
            break;
          }
          if (i == 0)
            break;
        }
      }
    }
  }

  // Return
  return overlap;
}

template <class T>
int Hyperspace::tile_cell_order_cmp(
    const T* coords_a, const T* coords_b) const {
  // Check tile order
  int tile_cmp = tile_order_cmp(coords_a, coords_b);
  if (tile_cmp)
    return tile_cmp;

  // Check cell order
  return cell_order_cmp(coords_a, coords_b);
}

const void* Hyperspace::tile_extent(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;

  return dimensions_[i]->tile_extent();
}

const void* Hyperspace::tile_extents() const {
  return tile_extents_;
}

template <typename T>
inline uint64_t Hyperspace::tile_id(const T* cell_coords) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Trivial case
  if (tile_extents == nullptr)
    return 0;

  // Calculate tile coordinates
  // TODO
  //  T* tile_coords = static_cast<T*>(tile_coords_aux_);

  // TODO: This is VERY inefficient. Fix!
  auto tile_coords = new T[dim_num_];

  for (unsigned int i = 0; i < dim_num_; ++i)
    tile_coords[i] = (cell_coords[i] - domain[2 * i]) / tile_extents[i];

  uint64_t tile_id = get_tile_pos(tile_coords);

  // TODO: remove
  delete[] tile_coords;

  // Return
  return tile_id;
}

uint64_t Hyperspace::tile_num() const {
  // Invoke the proper template function
  switch (type_) {
    case Datatype::INT32:
      return tile_num<int>();
    case Datatype::INT64:
      return tile_num<int64_t>();
    case Datatype::INT8:
      return tile_num<int8_t>();
    case Datatype::UINT8:
      return tile_num<uint8_t>();
    case Datatype::INT16:
      return tile_num<int16_t>();
    case Datatype::UINT16:
      return tile_num<uint16_t>();
    case Datatype::UINT32:
      return tile_num<uint32_t>();
    case Datatype::UINT64:
      return tile_num<uint64_t>();
    case Datatype::CHAR:
      assert(false);
      return 0;
    case Datatype::FLOAT32:
      assert(false);
      return 0;
    case Datatype::FLOAT64:
      assert(false);
      return 0;
  }
}

template <class T>
uint64_t Hyperspace::tile_num() const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  uint64_t ret = 1;
  for (unsigned int i = 0; i < dim_num_; ++i)
    ret *= (domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i];

  return ret;
}

uint64_t Hyperspace::tile_num(const void* range) const {
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
    case Datatype::CHAR:
      assert(false);
      return 0;
    case Datatype::FLOAT32:
      assert(false);
      return 0;
    case Datatype::FLOAT64:
      assert(false);
      return 0;
  }
}

template <class T>
uint64_t Hyperspace::tile_num(const T* range) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);
  auto domain = static_cast<const T*>(domain_);

  uint64_t ret = 1;
  uint64_t start, end;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    start = (range[2 * i] - domain[2 * i]) / tile_extents[i];
    end = (range[2 * i + 1] - domain[2 * i]) / tile_extents[i];
    ret *= (end - start + 1);
  }

  return ret;
}

template <class T>
int Hyperspace::tile_order_cmp(const T* coords_a, const T* coords_b) const {
  // Calculate tile ids
  auto id_a = tile_id(coords_a);
  auto id_b = tile_id(coords_b);

  // Compare ids
  if (id_a < id_b)
    return -1;
  if (id_a > id_b)
    return 1;

  // id_a == id_b
  return 0;
}

uint64_t Hyperspace::tile_slab_col_cell_num(const void* subarray) const {
  // Invoke the proper templated function
  switch (type_) {
    case Datatype::INT32:
      return tile_slab_col_cell_num(static_cast<const int*>(subarray));
    case Datatype::INT64:
      return tile_slab_col_cell_num(static_cast<const int64_t*>(subarray));
    case Datatype::FLOAT32:
      return tile_slab_col_cell_num(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return tile_slab_col_cell_num(static_cast<const double*>(subarray));
    case Datatype::INT8:
      return tile_slab_col_cell_num(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return tile_slab_col_cell_num(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return tile_slab_col_cell_num(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return tile_slab_col_cell_num(static_cast<const uint16_t*>(subarray));
    case Datatype::UINT32:
      return tile_slab_col_cell_num(static_cast<const uint32_t*>(subarray));
    case Datatype::UINT64:
      return tile_slab_col_cell_num(static_cast<const uint64_t*>(subarray));
    case Datatype::CHAR:
      assert(false);
      return 0;
  }
}

uint64_t Hyperspace::tile_slab_row_cell_num(const void* subarray) const {
  // Invoke the proper templated function
  switch (type_) {
    case Datatype::INT32:
      return tile_slab_row_cell_num(static_cast<const int*>(subarray));
    case Datatype::INT64:
      return tile_slab_row_cell_num(static_cast<const int64_t*>(subarray));
    case Datatype::FLOAT32:
      return tile_slab_row_cell_num(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return tile_slab_row_cell_num(static_cast<const double*>(subarray));
    case Datatype::INT8:
      return tile_slab_row_cell_num(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return tile_slab_row_cell_num(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return tile_slab_row_cell_num(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return tile_slab_row_cell_num(static_cast<const uint16_t*>(subarray));
    case Datatype::UINT32:
      return tile_slab_row_cell_num(static_cast<const uint32_t*>(subarray));
    case Datatype::UINT64:
      return tile_slab_row_cell_num(static_cast<const uint64_t*>(subarray));
    case Datatype::CHAR:
      assert(false);
      return 0;
  }
}

Datatype Hyperspace::type() const {
  return type_;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void Hyperspace::compute_cell_num_per_tile() {
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
    default:
      assert(0);
  }
}

template <class T>
void Hyperspace::compute_cell_num_per_tile() {
  auto tile_extents = static_cast<const T*>(tile_extents_);
  cell_num_per_tile_ = 1;

  for (unsigned int i = 0; i < dim_num_; ++i)
    cell_num_per_tile_ *= tile_extents[i];
}

void Hyperspace::compute_tile_domain() {
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
    default:
      assert(0);
  }
}

template <class T>
void Hyperspace::compute_tile_domain() {
  if (tile_extents_ == nullptr)
    return;

  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Allocate space for the tile domain
  assert(tile_domain_ == NULL);
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

void Hyperspace::compute_tile_offsets() {
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
    default:
      assert(0);
  }
}

template <class T>
void Hyperspace::compute_tile_offsets() {
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
      tile_num = (domain[2 * (i - 1) + 1] - domain[2 * (i - 1)] + 1) /
                 tile_extents[i - 1];
      tile_offsets_col_.push_back(tile_offsets_col_.back() * tile_num);
    }
  }

  // Calculate tile offsets for row-major tile order
  tile_offsets_row_.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2;; --i) {
      tile_num = (domain[2 * (i + 1) + 1] - domain[2 * (i + 1)] + 1) /
                 tile_extents[i + 1];
      tile_offsets_row_.push_back(tile_offsets_row_.back() * tile_num);
      if (i == 0)
        break;
    }
  }
  std::reverse(tile_offsets_row_.begin(), tile_offsets_row_.end());
}

template <class T>
uint64_t Hyperspace::get_cell_pos_col(const T* coords) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate cell offsets
  uint64_t cell_num;  // Per dimension
  std::vector<uint64_t> cell_offsets;
  cell_offsets.push_back(1);
  for (unsigned int i = 1; i < dim_num_; ++i) {
    cell_num = tile_extents[i - 1];
    cell_offsets.push_back(cell_offsets.back() * cell_num);
  }

  // Calculate position
  T coords_norm;  // Normalized coordinates inside the tile
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    coords_norm = (coords[i] - domain[2 * i]);
    coords_norm -= (coords_norm / tile_extents[i]) * tile_extents[i];
    pos += coords_norm * cell_offsets[i];
  }

  // Return
  return pos;
}

template <class T>
uint64_t Hyperspace::get_cell_pos_row(const T* coords) const {
  // For easy reference
  auto domain = static_cast<const T*>(domain_);
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate cell offsets
  uint64_t cell_num;  // Per dimension
  std::vector<uint64_t> cell_offsets;
  cell_offsets.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2;; --i) {
      cell_num = tile_extents[i + 1];
      cell_offsets.push_back(cell_offsets.back() * cell_num);
      if (i == 0)
        break;
    }
  }
  std::reverse(cell_offsets.begin(), cell_offsets.end());

  // Calculate position
  T coords_norm;  // Normalized coordinates inside the tile
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i) {
    coords_norm = (coords[i] - domain[2 * i]);
    coords_norm -= (coords_norm / tile_extents[i]) * tile_extents[i];
    pos += coords_norm * cell_offsets[i];
  }

  // Return
  return pos;
}

template <class T>
void Hyperspace::get_next_cell_coords_col(
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
void Hyperspace::get_next_cell_coords_row(
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
void Hyperspace::get_previous_cell_coords_col(
    const T* domain, T* cell_coords) const {
  unsigned int i = 0;
  --cell_coords[i];

  while (i < dim_num_ - 1 && cell_coords[i] < domain[2 * i]) {
    cell_coords[i] = domain[2 * i + 1];
    --cell_coords[++i];
  }
}

template <class T>
void Hyperspace::get_previous_cell_coords_row(
    const T* domain, T* cell_coords) const {
  unsigned int i = dim_num_ - 1;
  --cell_coords[i];

  while (i > 0 && cell_coords[i] < domain[2 * i]) {
    cell_coords[i] = domain[2 * i + 1];
    --cell_coords[--i];
  }
}

template <class T>
void Hyperspace::get_next_tile_coords_col(
    const T* domain, T* tile_coords) const {
  unsigned int i = 0;
  ++tile_coords[i];

  while (i < dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[++i];
  }
}

template <class T>
void Hyperspace::get_next_tile_coords_row(
    const T* domain, T* tile_coords) const {
  unsigned int i = dim_num_ - 1;
  ++tile_coords[i];

  while (i > 0 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[--i];
  }
}

template <class T>
uint64_t Hyperspace::get_tile_pos_col(const T* tile_coords) const {
  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets_col_[i];

  // Return
  return pos;
}

template <class T>
uint64_t Hyperspace::get_tile_pos_col(
    const T* domain, const T* tile_coords) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  uint64_t tile_num;  // Per dimension
  std::vector<uint64_t> tile_offsets;
  tile_offsets.push_back(1);
  for (unsigned int i = 1; i < dim_num_; ++i) {
    tile_num = (domain[2 * (i - 1) + 1] - domain[2 * (i - 1)] + 1) /
               tile_extents[i - 1];
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
uint64_t Hyperspace::get_tile_pos_row(const T* tile_coords) const {
  // Calculate position
  uint64_t pos = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets_row_[i];

  // Return
  return pos;
}

template <class T>
uint64_t Hyperspace::get_tile_pos_row(
    const T* domain, const T* tile_coords) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  uint64_t tile_num;  // Per dimension
  std::vector<uint64_t> tile_offsets;
  tile_offsets.push_back(1);
  if (dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2;; --i) {
      tile_num = (domain[2 * (i + 1) + 1] - domain[2 * (i + 1)] + 1) /
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

template <class T>
uint64_t Hyperspace::tile_slab_col_cell_num(const T* subarray) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Initialize the cell num to be returned to the maximum number of rows
  // in the slab
  uint64_t cell_num =
      MIN(tile_extents[dim_num_ - 1],
          subarray[2 * (dim_num_ - 1) + 1] - subarray[2 * (dim_num_ - 1)] + 1);

  // Calculate the number of cells in the slab
  for (unsigned int i = 0; i < dim_num_ - 1; ++i)
    cell_num *= (subarray[2 * i + 1] - subarray[2 * i] + 1);

  // Return
  return cell_num;
}

template <class T>
uint64_t Hyperspace::tile_slab_row_cell_num(const T* subarray) const {
  // For easy reference
  auto tile_extents = static_cast<const T*>(tile_extents_);

  // Initialize the cell num to be returned to the maximum number of rows
  // in the slab
  uint64_t cell_num = MIN(tile_extents[0], subarray[1] - subarray[0] + 1);

  // Calculate the number of cells in the slab
  for (unsigned int i = 1; i < dim_num_; ++i)
    cell_num *= (subarray[2 * i + 1] - subarray[2 * i] + 1);

  // Return
  return cell_num;
}
// Explicit template instantiations

template int Hyperspace::cell_order_cmp<int>(
    const int* coords_a, const int* coords_b) const;
template int Hyperspace::cell_order_cmp<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b) const;
template int Hyperspace::cell_order_cmp<float>(
    const float* coords_a, const float* coords_b) const;
template int Hyperspace::cell_order_cmp<double>(
    const double* coords_a, const double* coords_b) const;
template int Hyperspace::cell_order_cmp<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b) const;
template int Hyperspace::cell_order_cmp<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b) const;
template int Hyperspace::cell_order_cmp<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b) const;
template int Hyperspace::cell_order_cmp<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b) const;
template int Hyperspace::cell_order_cmp<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b) const;
template int Hyperspace::cell_order_cmp<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b) const;

template Status Hyperspace::get_cell_pos<int>(
    const int* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<int64_t>(
    const int64_t* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<float>(
    const float* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<double>(
    const double* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<int8_t>(
    const int8_t* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<uint8_t>(
    const uint8_t* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<int16_t>(
    const int16_t* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<uint16_t>(
    const uint16_t* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<uint32_t>(
    const uint32_t* coords, uint64_t* pos) const;
template Status Hyperspace::get_cell_pos<uint64_t>(
    const uint64_t* coords, uint64_t* pos) const;

template void Hyperspace::get_next_cell_coords<int>(
    const int* domain, int* cell_coords, bool* coords_retrieved) const;
template void Hyperspace::get_next_cell_coords<int64_t>(
    const int64_t* domain, int64_t* cell_coords, bool* coords_retrieved) const;
template void Hyperspace::get_next_cell_coords<float>(
    const float* domain, float* cell_coords, bool* coords_retrieved) const;
template void Hyperspace::get_next_cell_coords<double>(
    const double* domain, double* cell_coords, bool* coords_retrieved) const;

template void Hyperspace::get_next_cell_coords<int8_t>(
    const int8_t* domain, int8_t* cell_coords, bool* coords_retrieved) const;
template void Hyperspace::get_next_cell_coords<uint8_t>(
    const uint8_t* domain, uint8_t* cell_coords, bool* coords_retrieved) const;
template void Hyperspace::get_next_cell_coords<int16_t>(
    const int16_t* domain, int16_t* cell_coords, bool* coords_retrieved) const;
template void Hyperspace::get_next_cell_coords<uint16_t>(
    const uint16_t* domain,
    uint16_t* cell_coords,
    bool* coords_retrieved) const;
template void Hyperspace::get_next_cell_coords<uint32_t>(
    const uint32_t* domain,
    uint32_t* cell_coords,
    bool* coords_retrieved) const;
template void Hyperspace::get_next_cell_coords<uint64_t>(
    const uint64_t* domain,
    uint64_t* cell_coords,
    bool* coords_retrieved) const;

template void Hyperspace::get_next_tile_coords<int>(
    const int* domain, int* tile_coords) const;
template void Hyperspace::get_next_tile_coords<int64_t>(
    const int64_t* domain, int64_t* tile_coords) const;
template void Hyperspace::get_next_tile_coords<float>(
    const float* domain, float* tile_coords) const;
template void Hyperspace::get_next_tile_coords<double>(
    const double* domain, double* tile_coords) const;
template void Hyperspace::get_next_tile_coords<int8_t>(
    const int8_t* domain, int8_t* tile_coords) const;
template void Hyperspace::get_next_tile_coords<uint8_t>(
    const uint8_t* domain, uint8_t* tile_coords) const;
template void Hyperspace::get_next_tile_coords<int16_t>(
    const int16_t* domain, int16_t* tile_coords) const;
template void Hyperspace::get_next_tile_coords<uint16_t>(
    const uint16_t* domain, uint16_t* tile_coords) const;
template void Hyperspace::get_next_tile_coords<uint32_t>(
    const uint32_t* domain, uint32_t* tile_coords) const;
template void Hyperspace::get_next_tile_coords<uint64_t>(
    const uint64_t* domain, uint64_t* tile_coords) const;

template void Hyperspace::get_previous_cell_coords<int>(
    const int* domain, int* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<int64_t>(
    const int64_t* domain, int64_t* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<float>(
    const float* domain, float* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<double>(
    const double* domain, double* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<int8_t>(
    const int8_t* domain, int8_t* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<uint8_t>(
    const uint8_t* domain, uint8_t* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<int16_t>(
    const int16_t* domain, int16_t* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<uint16_t>(
    const uint16_t* domain, uint16_t* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<uint32_t>(
    const uint32_t* domain, uint32_t* cell_coords) const;
template void Hyperspace::get_previous_cell_coords<uint64_t>(
    const uint64_t* domain, uint64_t* cell_coords) const;

template void Hyperspace::get_subarray_tile_domain<int>(
    const int* subarray, int* tile_domain, int* subarray_tile_domain) const;
template void Hyperspace::get_subarray_tile_domain<int64_t>(
    const int64_t* subarray,
    int64_t* tile_domain,
    int64_t* subarray_tile_domain) const;
template void Hyperspace::get_subarray_tile_domain<int8_t>(
    const int8_t* subarray,
    int8_t* tile_domain,
    int8_t* subarray_tile_domain) const;
template void Hyperspace::get_subarray_tile_domain<uint8_t>(
    const uint8_t* subarray,
    uint8_t* tile_domain,
    uint8_t* subarray_tile_domain) const;
template void Hyperspace::get_subarray_tile_domain<int16_t>(
    const int16_t* subarray,
    int16_t* tile_domain,
    int16_t* subarray_tile_domain) const;
template void Hyperspace::get_subarray_tile_domain<uint16_t>(
    const uint16_t* subarray,
    uint16_t* tile_domain,
    uint16_t* subarray_tile_domain) const;
template void Hyperspace::get_subarray_tile_domain<uint32_t>(
    const uint32_t* subarray,
    uint32_t* tile_domain,
    uint32_t* subarray_tile_domain) const;
template void Hyperspace::get_subarray_tile_domain<uint64_t>(
    const uint64_t* subarray,
    uint64_t* tile_domain,
    uint64_t* subarray_tile_domain) const;

template uint64_t Hyperspace::get_tile_pos<int>(
    const int* domain, const int* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<int64_t>(
    const int64_t* domain, const int64_t* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<float>(
    const float* domain, const float* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<double>(
    const double* domain, const double* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<int8_t>(
    const int8_t* domain, const int8_t* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<uint8_t>(
    const uint8_t* domain, const uint8_t* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<int16_t>(
    const int16_t* domain, const int16_t* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<uint16_t>(
    const uint16_t* domain, const uint16_t* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<uint32_t>(
    const uint32_t* domain, const uint32_t* tile_coords) const;
template uint64_t Hyperspace::get_tile_pos<uint64_t>(
    const uint64_t* domain, const uint64_t* tile_coords) const;

template void Hyperspace::get_tile_subarray<int>(
    const int* tile_coords, int* tile_subarray) const;
template void Hyperspace::get_tile_subarray<int64_t>(
    const int64_t* tile_coords, int64_t* tile_subarray) const;
template void Hyperspace::get_tile_subarray<int8_t>(
    const int8_t* tile_coords, int8_t* tile_subarray) const;
template void Hyperspace::get_tile_subarray<uint8_t>(
    const uint8_t* tile_coords, uint8_t* tile_subarray) const;
template void Hyperspace::get_tile_subarray<int16_t>(
    const int16_t* tile_coords, int16_t* tile_subarray) const;
template void Hyperspace::get_tile_subarray<uint16_t>(
    const uint16_t* tile_coords, uint16_t* tile_subarray) const;
template void Hyperspace::get_tile_subarray<uint32_t>(
    const uint32_t* tile_coords, uint32_t* tile_subarray) const;
template void Hyperspace::get_tile_subarray<uint64_t>(
    const uint64_t* tile_coords, uint64_t* tile_subarray) const;

template bool Hyperspace::is_contained_in_tile_slab_col<int>(
    const int* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<int64_t>(
    const int64_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<float>(
    const float* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<double>(
    const double* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<int8_t>(
    const int8_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<uint8_t>(
    const uint8_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<int16_t>(
    const int16_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<uint16_t>(
    const uint16_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<uint32_t>(
    const uint32_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_col<uint64_t>(
    const uint64_t* range) const;

template bool Hyperspace::is_contained_in_tile_slab_row<int>(
    const int* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<int64_t>(
    const int64_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<float>(
    const float* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<double>(
    const double* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<int8_t>(
    const int8_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<uint8_t>(
    const uint8_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<int16_t>(
    const int16_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<uint16_t>(
    const uint16_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<uint32_t>(
    const uint32_t* range) const;
template bool Hyperspace::is_contained_in_tile_slab_row<uint64_t>(
    const uint64_t* range) const;

template unsigned int Hyperspace::subarray_overlap<int>(
    const int* subarray_a, const int* subarray_b, int* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<int64_t>(
    const int64_t* subarray_a,
    const int64_t* subarray_b,
    int64_t* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<float>(
    const float* subarray_a,
    const float* subarray_b,
    float* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<double>(
    const double* subarray_a,
    const double* subarray_b,
    double* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<int8_t>(
    const int8_t* subarray_a,
    const int8_t* subarray_b,
    int8_t* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<uint8_t>(
    const uint8_t* subarray_a,
    const uint8_t* subarray_b,
    uint8_t* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<int16_t>(
    const int16_t* subarray_a,
    const int16_t* subarray_b,
    int16_t* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<uint16_t>(
    const uint16_t* subarray_a,
    const uint16_t* subarray_b,
    uint16_t* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<uint32_t>(
    const uint32_t* subarray_a,
    const uint32_t* subarray_b,
    uint32_t* overlap_subarray) const;
template unsigned int Hyperspace::subarray_overlap<uint64_t>(
    const uint64_t* subarray_a,
    const uint64_t* subarray_b,
    uint64_t* overlap_subarray) const;

template int Hyperspace::tile_cell_order_cmp<int>(
    const int* coords_a, const int* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<float>(
    const float* coords_a, const float* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<double>(
    const double* coords_a, const double* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b) const;
template int Hyperspace::tile_cell_order_cmp<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b) const;

template uint64_t Hyperspace::tile_id<int>(const int* cell_coords) const;
template uint64_t Hyperspace::tile_id<int64_t>(
    const int64_t* cell_coords) const;
template uint64_t Hyperspace::tile_id<float>(const float* cell_coords) const;
template uint64_t Hyperspace::tile_id<double>(const double* cell_coords) const;
template uint64_t Hyperspace::tile_id<int8_t>(const int8_t* cell_coords) const;
template uint64_t Hyperspace::tile_id<uint8_t>(
    const uint8_t* cell_coords) const;
template uint64_t Hyperspace::tile_id<int16_t>(
    const int16_t* cell_coords) const;
template uint64_t Hyperspace::tile_id<uint16_t>(
    const uint16_t* cell_coords) const;
template uint64_t Hyperspace::tile_id<uint32_t>(
    const uint32_t* cell_coords) const;
template uint64_t Hyperspace::tile_id<uint64_t>(
    const uint64_t* cell_coords) const;

}  // namespace tiledb
