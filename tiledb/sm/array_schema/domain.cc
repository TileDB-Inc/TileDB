/**
 * @file   domain.cc
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
 * This file implements class Domain.
 */

#include "domain.h"
#include "current_domain.h"
#include "dimension.h"
#include "domain_data_ref.h"
#include "domain_typed_data_view.h"
#include "ndrectangle.h"
#include "tiledb/common/assert.h"
#include "tiledb/common/blank.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/type/apply_with_type.h"
#include "tiledb/type/range/range.h"

#include <cassert>
#include <iostream>
#include <limits>
#include <stdexcept>

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Domain::Domain(shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , dimensions_(memory_tracker_->get_resource(MemoryType::DIMENSIONS))
    , dimension_ptrs_(memory_tracker_->get_resource(MemoryType::DIMENSIONS))
    , cell_order_cmp_func_(memory_tracker_->get_resource(MemoryType::DOMAINS))
    , cell_order_cmp_func_2_(memory_tracker_->get_resource(MemoryType::DOMAINS))
    , tile_order_cmp_func_(memory_tracker_->get_resource(MemoryType::DOMAINS)) {
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;
  dim_num_ = 0;
  cell_num_per_tile_ = 0;
}

Domain::Domain(
    Layout cell_order,
    const std::vector<shared_ptr<Dimension>> dimensions,
    Layout tile_order,
    shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , cell_order_(cell_order)
    , dimensions_(
          dimensions.begin(),
          dimensions.end(),
          memory_tracker_->get_resource(MemoryType::DIMENSIONS))
    , dimension_ptrs_(memory_tracker_->get_resource(MemoryType::DIMENSIONS))
    , dim_num_(static_cast<dimension_size_type>(dimensions.size()))
    , tile_order_(tile_order)
    , cell_order_cmp_func_(memory_tracker_->get_resource(MemoryType::DOMAINS))
    , cell_order_cmp_func_2_(memory_tracker_->get_resource(MemoryType::DOMAINS))
    , tile_order_cmp_func_(memory_tracker_->get_resource(MemoryType::DOMAINS)) {
  /*
   * Verify that the input vector has no non-null elements in order to meet the
   * class invariant. Initialize the dimensions mirror.
   */
  dimension_ptrs_.reserve(dimensions_.size());
  for (const auto& dim : dimensions_) {
    auto p{dim.get()};
    if (p == nullptr) {
      throw std::invalid_argument("May not have null dimensions in a domain");
    }
    dimension_ptrs_.emplace_back(p);
  }

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute number of cells per tile
  set_tile_cell_order_cmp_funcs();
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

void Domain::add_dimension(shared_ptr<Dimension> dim) {
  auto p{dim.get()};
  if (p == nullptr) {
    // Class invariant prohibits null dimensions in a domain.
    throw std::invalid_argument("May not add null dimensions to a domain");
  }
  dimensions_.emplace_back(dim);
  dimension_ptrs_.emplace_back(p);
  ++dim_num_;

  // Compute number of cells per tile
  compute_cell_num_per_tile();
}

bool Domain::all_dims_fixed() const {
  for (const auto dim : dimension_ptrs_) {
    if (dim->var_size())
      return false;
  }

  return true;
}

bool Domain::all_dims_int() const {
  for (const auto dim : dimension_ptrs_) {
    if (!datatype_is_integer(dim->type()))
      return false;
  }

  return true;
}

bool Domain::all_dims_string() const {
  for (const auto dim : dimension_ptrs_) {
    if (!datatype_is_string(dim->type()))
      return false;
  }

  return true;
}

bool Domain::all_dims_real() const {
  for (const auto dim : dimension_ptrs_) {
    if (!datatype_is_real(dim->type()))
      return false;
  }

  return true;
}

bool Domain::all_dims_same_type() const {
  if (dim_num_ <= 1)
    return true;

  auto type = dimension_ptrs_[0]->type();
  for (unsigned d = 1; d < dim_num_; ++d) {
    if (dimension_ptrs_[d]->type() != type)
      return false;
  }

  return true;
}

uint64_t Domain::cell_num_per_tile() const {
  return cell_num_per_tile_;
}

template <>
int Domain::cell_order_cmp_impl<char>(
    const Dimension*, const UntypedDatumView a, const UntypedDatumView b) {
  auto var_a = reinterpret_cast<const char*>(a.content());
  auto var_b = reinterpret_cast<const char*>(b.content());
  auto size_a{a.size()};
  auto size_b{b.size()};
  auto size = std::min(size_a, size_b);

  if (size != 0) {
    size_t i = 0;
    while (var_a[i] == var_b[i]) {
      if (i == size - 1) {
        break;
      }
      ++i;
    }

    if (var_a[i] < var_b[i]) {
      return -1;
    }

    if (var_a[i] > var_b[i]) {
      return 1;
    }
  }

  // Equal common prefix, so equal if they have the same size
  if (size_a == size_b) {
    return 0;
  }

  // Equal common prefix, so the smaller size wins
  return (size_a < size_b) ? -1 : 1;
}

template <class T>
int Domain::cell_order_cmp_impl(
    const Dimension*, const UntypedDatumView a, const UntypedDatumView b) {
  auto ca = a.template value_as<T>();
  auto cb = b.template value_as<T>();
  if (ca < cb)
    return -1;
  if (ca > cb)
    return 1;
  return 0;
}

int Domain::cell_order_cmp(
    unsigned dim_idx, UntypedDatumView a, UntypedDatumView b) const {
  // Handle variable-sized dimensions
  if (dimension_ptrs_[dim_idx]->var_size()) {
    std::string_view s_a{static_cast<const char*>(a.content()), a.size()};
    std::string_view s_b{static_cast<const char*>(b.content()), b.size()};

    if (s_a == s_b)
      return 0;
    if (s_a < s_b)
      return -1;
    return 1;
  }

  if (cell_order_cmp_func_2_[dim_idx] == nullptr) {
    throw std::logic_error("comparison function not initialized");
  }
  return cell_order_cmp_func_2_[dim_idx](a.content(), b.content());
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
    const type::DomainDataRef& left, const type::DomainDataRef& right) const {
  if (cell_order_ == Layout::ROW_MAJOR || cell_order_ == Layout::HILBERT) {
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto res = cell_order_cmp_func_[d](
          dimension_ptr(d),
          left.dimension_datum_view(d),
          right.dimension_datum_view(d));

      if (res == 1 || res == -1)
        return res;
      // else same tile on dimension d --> continue
    }
  } else {  // COL_MAJOR
    for (unsigned d = dim_num_ - 1;; --d) {
      auto res = cell_order_cmp_func_[d](
          dimension_ptr(d),
          left.dimension_datum_view(d),
          right.dimension_datum_view(d));

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
  for (unsigned d = 0; d < dim_num_; ++d) {
    auto type = dimension_ptrs_[d]->type();
    auto g = [&](auto T) {
      if constexpr (tiledb::type::TileDBIntegral<decltype(T)>) {
        tiledb::type::crop_range<decltype(T)>(
            dimension_ptrs_[d]->domain(), (*ndrange)[d]);
      } else {
        throw std::invalid_argument(
            "Unsupported dimension datatype " + datatype_str(type));
      }
    };
    apply_with_type(g, type);
  }
}

shared_ptr<Domain> Domain::deserialize(
    Deserializer& deserializer,
    uint32_t version,
    Layout cell_order,
    Layout tile_order,
    FilterPipeline& coords_filters,
    shared_ptr<MemoryTracker> memory_tracker) {
  Status st;
  // Load type
  Datatype type = Datatype::INT32;
  if (version < 5) {
    auto type_c = deserializer.read<uint8_t>();
    type = static_cast<Datatype>(type_c);
  }

  std::vector<shared_ptr<Dimension>> dimensions;
  auto dim_num = deserializer.read<uint32_t>();
  for (uint32_t i = 0; i < dim_num; ++i) {
    auto dim{Dimension::deserialize(
        deserializer, version, type, coords_filters, memory_tracker)};
    dimensions.emplace_back(std::move(dim));
  }

  return tiledb::common::make_shared<Domain>(
      HERE(), cell_order, dimensions, tile_order, memory_tracker);
}

const Range& Domain::domain(unsigned i) const {
  iassert(i < dim_num_, "i = {}, dim_num_ = {}", i, dim_num_);
  return dimension_ptrs_[i]->domain();
}

NDRange Domain::domain() const {
  NDRange ret(dim_num_);
  for (unsigned d = 0; d < dim_num_; ++d)
    ret[d] = dimension_ptrs_[d]->domain();

  return ret;
}

const Dimension* Domain::dimension_ptr(const std::string& name) const {
  return shared_dimension(name).get();
}

shared_ptr<Dimension> Domain::shared_dimension(const std::string& name) const {
  for (dimension_size_type i = 0; i < dim_num_; i++) {
    const auto dim = dimension_ptrs_[i];
    if (dim->name() == name) {
      return dimensions_[i];
    }
  }
  return {nullptr};
}

void Domain::expand_ndrange(const NDRange& r1, NDRange* r2) const {
  iassert(r2 != nullptr);

  // Assign r1 to r2 if r2 is empty
  if (r2->empty()) {
    *r2 = r1;
    return;
  }

  // Expand r2 along all dimensions
  for (unsigned d = 0; d < dim_num_; ++d) {
    const auto dim = dimension_ptrs_[d];
    if (!dim->var_size())
      dim->expand_range(r1[d], &(*r2)[d]);
    else
      dim->expand_range_var(r1[d], &(*r2)[d]);
  }
}

void Domain::expand_to_tiles_when_no_current_domain(
    NDRange& query_ndrange) const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    const auto dim = dimension_ptrs_[d];
    // Applicable only to fixed-sized dimensions
    if (!dim->var_size()) {
      dim->expand_to_tile(&(query_ndrange)[d]);
    }
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
void Domain::get_next_tile_coords(const T* domain, T* tile_coords) const {
  passert(
      tile_order_ == Layout::ROW_MAJOR || tile_order_ == Layout::COL_MAJOR,
      "tile_order = {}",
      layout_str(tile_order_));
  if (tile_order_ == Layout::ROW_MAJOR) {
    get_next_tile_coords_row(domain, tile_coords);
  } else {
    get_next_tile_coords_col(domain, tile_coords);
  }
}

template <class T>
void Domain::get_next_tile_coords(
    const T* domain, T* tile_coords, bool* in) const {
  passert(
      tile_order_ == Layout::ROW_MAJOR || tile_order_ == Layout::COL_MAJOR,
      "tile_order = {}",
      layout_str(tile_order_));
  if (tile_order_ == Layout::ROW_MAJOR) {
    get_next_tile_coords_row(domain, tile_coords, in);
  } else {
    get_next_tile_coords_col(domain, tile_coords, in);
  }
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

bool Domain::has_dimension(const std::string& name) const {
  for (const auto dim : dimension_ptrs_) {
    if (name == dim->name()) {
      return true;
    }
  }
  return false;
}

std::optional<unsigned> Domain::dimension_index(const std::string& name) const {
  for (unsigned d = 0; d < dim_num_; ++d) {
    if (dimension_ptrs_[d]->name() == name) {
      return d;
    }
  }

  return std::nullopt;
}

unsigned Domain::get_dimension_index(const std::string& name) const {
  const auto maybe = dimension_index(name);
  if (maybe.has_value()) {
    return maybe.value();
  } else {
    throw std::invalid_argument(
        "Cannot get dimension index; Invalid dimension name");
  }
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
void Domain::serialize(Serializer& serializer, uint32_t version) const {
  // Write dimensions
  serializer.write<uint32_t>(dim_num_);
  for (const auto& dim : dimensions_) {
    dim->serialize(serializer, version);
  }
}

void Domain::set_null_tile_extents_to_range() {
  for (auto& d : dimensions_) {
    d->set_null_tile_extent_to_range();
  }
}

template <class T>
uint64_t Domain::stride(Layout subarray_layout) const {
  if (dim_num_ == 1 || subarray_layout == Layout::GLOBAL_ORDER ||
      subarray_layout == cell_order_) {
    return UINT64_MAX;
  }

  if (cell_order_ == Layout::HILBERT) {
    throw std::logic_error("Stride cannot be computed for Hilbert cell order");
  }

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
  iassert(i < dim_num_, "i = {}, dim_num_ = {}", i, dim_num_);
  return dimension_ptrs_[i]->tile_extent();
}

std::vector<ByteVecValue> Domain::tile_extents() const {
  std::vector<ByteVecValue> ret(dim_num_);
  for (unsigned d = 0; d < dim_num_; ++d) {
    ret[d] = tile_extent(d);
  }

  return ret;
}

uint64_t Domain::tile_num(const NDRange& ndrange) const {
  uint64_t ret = 1;
  for (unsigned d = 0; d < dim_num_; ++d)
    ret *= dimension_ptrs_[d]->tile_num(ndrange[d]);

  return ret;
}

uint64_t Domain::cell_num(const NDRange& ndrange) const {
  iassert(!ndrange.empty());
  uint64_t cell_num = 1, range;
  for (unsigned d = 0; d < dim_num_; ++d) {
    range = dimension_ptrs_[d]->domain_range(ndrange[d]);
    if (range == std::numeric_limits<uint64_t>::max())  // Overflow
      return range;

    cell_num = utils::math::safe_mul(range, cell_num);
    if (cell_num == std::numeric_limits<uint64_t>::max())  // Overflow
      return cell_num;
  }

  return cell_num;
}

bool Domain::covered(const NDRange& r1, const NDRange& r2) const {
  iassert(r1.size() == dim_num_);
  iassert(r2.size() == dim_num_);

  for (unsigned d = 0; d < dim_num_; ++d) {
    if (!dimension_ptrs_[d]->covered(r1[d], r2[d]))
      return false;
  }

  return true;
}

bool Domain::overlap(const NDRange& r1, const NDRange& r2) const {
  iassert(r1.size() == dim_num_);
  iassert(r2.size() == dim_num_);

  for (unsigned d = 0; d < dim_num_; ++d) {
    if (!dimension_ptrs_[d]->overlap(r1[d], r2[d]))
      return false;
  }

  return true;
}

double Domain::overlap_ratio(
    const NDRange& r1,
    const std::vector<bool>& r1_default,
    const NDRange& r2) const {
  double ratio = 1.0;
  iassert(dim_num_ == r1.size());
  iassert(dim_num_ == r2.size());

  for (unsigned d = 0; d < dim_num_; ++d) {
    if (r1_default[d])
      continue;

    if (!dimension_ptrs_[d]->overlap(r1[d], r2[d]))
      return 0.0;

    ratio *= dimension_ptrs_[d]->overlap_ratio(r1[d], r2[d]);

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
int Domain::tile_order_cmp_impl(
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
    const type::DomainDataRef& left, const type::DomainDataRef& right) const {
  if (tile_order_ == Layout::ROW_MAJOR) {
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto dim{dimension_ptr(d)};

      // Inapplicable to var-sized dimensions or absent tile extents
      if (dim->var_size() || !dim->tile_extent())
        continue;

      auto res = tile_order_cmp_func_[d](
          dim,
          left.dimension_datum_view(d).content(),
          right.dimension_datum_view(d).content());

      if (res == 1 || res == -1)
        return res;
      // else same tile on dimension d --> continue
    }
  } else {  // COL_MAJOR
    for (unsigned d = dim_num_ - 1;; --d) {
      auto dim{dimension_ptr(d)};

      // Inapplicable to var-sized dimensions or absent tile extents
      if (!dim->var_size() && dim->tile_extent()) {
        auto res = tile_order_cmp_func_[d](
            dim,
            left.dimension_datum_view(d).content(),
            right.dimension_datum_view(d).content());

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
    unsigned d, const void* coord_a, const void* coord_b) const {
  return tile_order_cmp_func_[d](dimension_ptr(d), coord_a, coord_b);
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void Domain::compute_cell_num_per_tile() {
  // Applicable to dimensions that have the same type
  if (!all_dims_same_type())
    return;

  // Invoke the proper templated function
  auto type{dimension_ptrs_[0]->type()};
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
    auto type{dimension_ptrs_[d]->type()};
    switch (type) {
      case Datatype::INT32:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<int32_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<int32_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int32_t>;
        break;
      case Datatype::INT64:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<int64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<int64_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int64_t>;
        break;
      case Datatype::INT8:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<int8_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<int8_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int8_t>;
        break;
      case Datatype::UINT8:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<uint8_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<uint8_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<uint8_t>;
        break;
      case Datatype::INT16:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<int16_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<int16_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int16_t>;
        break;
      case Datatype::UINT16:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<uint16_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<uint16_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<uint16_t>;
        break;
      case Datatype::UINT32:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<uint32_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<uint32_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<uint32_t>;
        break;
      case Datatype::UINT64:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<uint64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<uint64_t>;
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
        tile_order_cmp_func_[d] = tile_order_cmp_impl<int64_t>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<int64_t>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<int64_t>;
        break;
      case Datatype::FLOAT32:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<float>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<float>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<float>;
        break;
      case Datatype::FLOAT64:
        tile_order_cmp_func_[d] = tile_order_cmp_impl<double>;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<double>;
        cell_order_cmp_func_2_[d] = cell_order_cmp_2<double>;
        break;
      case Datatype::STRING_ASCII:
        tile_order_cmp_func_[d] = nullptr;
        cell_order_cmp_func_[d] = cell_order_cmp_impl<char>;
        cell_order_cmp_func_2_[d] = nullptr;
        break;
      case Datatype::BLOB:
      case Datatype::GEOM_WKB:
      case Datatype::GEOM_WKT:
      case Datatype::CHAR:
      case Datatype::BOOL:
      case Datatype::STRING_UTF8:
      case Datatype::STRING_UTF16:
      case Datatype::STRING_UTF32:
      case Datatype::STRING_UCS2:
      case Datatype::STRING_UCS4:
      case Datatype::ANY:
        throw std::invalid_argument(
            "Unsupported dimension datatype " + datatype_str(type));
    }
  }
}

template <class T>
void Domain::get_next_tile_coords_col(const T* domain, T* tile_coords) const {
  dimension_size_type i = 0;
  ++tile_coords[i];

  while (i < dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[++i];
  }
}

template <class T>
void Domain::get_next_tile_coords_col(
    const T* domain, T* tile_coords, bool* in) const {
  dimension_size_type i = 0;
  ++tile_coords[i];

  while (i < dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[++i];
  }

  *in = !(i == dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]);
}

template <class T>
void Domain::get_next_tile_coords_row(const T* domain, T* tile_coords) const {
  dimension_size_type i = dim_num_ - 1;
  ++tile_coords[i];

  while (i > 0 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[--i];
  }
}

template <class T>
void Domain::get_next_tile_coords_row(
    const T* domain, T* tile_coords, bool* in) const {
  dimension_size_type i = dim_num_ - 1;
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
  for (dimension_size_type i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets[i];

  // Return
  return pos;
}

// Explicit template instantiations
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

}  // namespace tiledb::sm

std::ostream& operator<<(std::ostream& os, const tiledb::sm::Domain& domain) {
  for (unsigned i = 0; i < domain.dim_num(); i++) {
    os << std::endl;
    os << *domain.dimension_ptr(i);
  }

  return os;
}
