/**
 * @file   dimension.cc
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
 * This file implements class Dimension.
 */

#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <iostream>
#include <sstream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Dimension::Dimension() {
  domain_ = nullptr;
  tile_extent_ = nullptr;
  type_ = Datatype::INT32;
  set_compute_mbr_func();
  set_crop_range_func();
  set_expand_range_func();
  set_expand_range_v_func();
  set_expand_to_tile_func();
  set_oob_func();
  set_covered_func();
  set_overlap_func();
  set_overlap_ratio_func();
  set_tile_num_func();
  set_value_in_range_func();
}

Dimension::Dimension(const std::string& name, Datatype type)
    : name_(name)
    , type_(type) {
  domain_ = nullptr;
  tile_extent_ = nullptr;
  set_compute_mbr_func();
  set_crop_range_func();
  set_expand_range_func();
  set_expand_range_v_func();
  set_expand_to_tile_func();
  set_oob_func();
  set_covered_func();
  set_overlap_func();
  set_overlap_ratio_func();
  set_tile_num_func();
  set_value_in_range_func();
}

Dimension::Dimension(const Dimension* dim) {
  assert(dim != nullptr);

  name_ = dim->name();
  type_ = dim->type_;
  oob_func_ = dim->oob_func_;
  uint64_t type_size = datatype_size(type_);
  domain_ = std::malloc(2 * type_size);
  std::memcpy(domain_, dim->domain(), 2 * type_size);

  const void* tile_extent = dim->tile_extent();
  if (tile_extent == nullptr) {
    tile_extent_ = nullptr;
  } else {
    tile_extent_ = std::malloc(type_size);
    std::memcpy(tile_extent_, tile_extent, type_size);
  }
}

Dimension::~Dimension() {
  // Clean up
  std::free(domain_);
  std::free(tile_extent_);
}

/* ********************************* */
/*                API                */
/* ********************************* */

unsigned int Dimension::cell_val_num() const {
  // TODO: in a future PR the user will be able to set this value
  return 1;
}

uint64_t Dimension::coord_size() const {
  return datatype_size(type_);
}

std::string Dimension::coord_to_str(const void* coord) const {
  std::stringstream ss;
  assert(coord != nullptr);

  switch (type_) {
    case Datatype::INT32:
      ss << *((int32_t*)coord);
      break;
    case Datatype::INT64:
      ss << *((int64_t*)coord);
      break;
    case Datatype::INT8:
      ss << *((int8_t*)coord);
      break;
    case Datatype::UINT8:
      ss << *((uint8_t*)coord);
      break;
    case Datatype::INT16:
      ss << *((int16_t*)coord);
      break;
    case Datatype::UINT16:
      ss << *((uint16_t*)coord);
      break;
    case Datatype::UINT32:
      ss << *((uint32_t*)coord);
      break;
    case Datatype::UINT64:
      ss << *((uint64_t*)coord);
      break;
    case Datatype::FLOAT32:
      ss << *((float*)coord);
      break;
    case Datatype::FLOAT64:
      ss << *((double*)coord);
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
      ss << *((int64_t*)coord);
      break;
    default:
      break;
  }

  return ss.str();
}

// ===== FORMAT =====
// dimension_name_size (uint32_t)
// dimension_name (string)
// domain (void* - 2*type_size)
// null_tile_extent (uint8_t)
// tile_extent (void* - type_size)
Status Dimension::deserialize(ConstBuffer* buff, Datatype type) {
  // Set type
  type_ = type;

  // Load dimension name
  uint32_t dimension_name_size;
  RETURN_NOT_OK(buff->read(&dimension_name_size, sizeof(uint32_t)));
  name_.resize(dimension_name_size);
  RETURN_NOT_OK(buff->read(&name_[0], dimension_name_size));

  // Load domain
  uint64_t domain_size = 2 * datatype_size(type_);
  std::free(domain_);
  domain_ = std::malloc(domain_size);
  if (domain_ == nullptr)
    return LOG_STATUS(
        Status::DimensionError("Cannot deserialize; Memory allocation failed"));
  RETURN_NOT_OK(buff->read(domain_, domain_size));

  // Load tile extent
  std::free(tile_extent_);
  tile_extent_ = nullptr;
  uint8_t null_tile_extent;
  RETURN_NOT_OK(buff->read(&null_tile_extent, sizeof(uint8_t)));
  if (null_tile_extent == 0) {
    tile_extent_ = std::malloc(datatype_size(type_));
    if (tile_extent_ == nullptr) {
      return LOG_STATUS(Status::DimensionError(
          "Cannot deserialize; Memory allocation failed"));
    }
    RETURN_NOT_OK(buff->read(tile_extent_, datatype_size(type_)));
  }

  set_compute_mbr_func();
  set_crop_range_func();
  set_expand_range_func();
  set_expand_range_v_func();
  set_expand_to_tile_func();
  set_oob_func();
  set_covered_func();
  set_overlap_func();
  set_overlap_ratio_func();
  set_tile_num_func();
  set_value_in_range_func();

  return Status::Ok();
}

void* Dimension::domain() const {
  return domain_;
}

void Dimension::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  // Retrieve domain and tile extent strings
  std::string domain_s = utils::parse::domain_str(domain_, type_);
  std::string tile_extent_s =
      utils::parse::tile_extent_str(tile_extent_, type_);

  // Dump
  fprintf(out, "### Dimension ###\n");
  fprintf(out, "- Name: %s\n", is_anonymous() ? "<anonymous>" : name_.c_str());
  fprintf(out, "- Domain: %s\n", domain_s.c_str());
  fprintf(out, "- Tile extent: %s\n", tile_extent_s.c_str());
}

const FilterPipeline* Dimension::filters() const {
  // TODO: in a future PR, the user will be able to set separate
  // TODO: filters for each dimension
  return nullptr;
}

const std::string& Dimension::name() const {
  return name_;
}

bool Dimension::is_anonymous() const {
  return name_.empty() ||
         utils::parse::starts_with(name_, constants::default_dim_name);
}

template <class T>
void Dimension::compute_mbr(
    const Dimension* dim, const Tile& tile, Range* mbr) {
  assert(dim != nullptr);
  assert(mbr != nullptr);
  auto data = (const T*)(tile.internal_data());
  assert(data != nullptr);
  auto cell_num = tile.cell_num();
  assert(cell_num > 0);

  // Initialize MBR with the first tile values
  T res[] = {data[0], data[0]};
  mbr->set_range(res, sizeof(res));

  // Expand the MBR with the rest tile values
  for (uint64_t c = 1; c < cell_num; ++c)
    dim->expand_range_v(&data[c], mbr);
}

void Dimension::compute_mbr(const Tile& tile, Range* mbr) const {
  assert(compute_mbr_func_ != nullptr);
  compute_mbr_func_(this, tile, mbr);
}

template <class T>
void Dimension::crop_range(const Dimension* dim, Range* range) {
  assert(dim != nullptr);
  assert(!range->empty());
  auto dim_dom = (const T*)dim->domain();
  auto r = (const T*)range->data();
  T res[2] = {std::max(r[0], dim_dom[0]), std::min(r[1], dim_dom[1])};
  range->set_range(res, sizeof(res));
}

void Dimension::crop_range(Range* range) const {
  assert(crop_range_func_ != nullptr);
  crop_range_func_(this, range);
}

template <class T>
void Dimension::expand_range_v(const Dimension* dim, const void* v, Range* r) {
  assert(dim != nullptr);
  assert(v != nullptr);
  assert(!r->empty());
  (void)dim;  // Not used here
  auto rt = (const T*)r->data();
  auto vt = (const T*)v;
  T res[2] = {std::min(rt[0], *vt), std::max(rt[1], *vt)};
  r->set_range(res, sizeof(res));
}

void Dimension::expand_range_v(const void* v, Range* r) const {
  assert(expand_range_v_func_ != nullptr);
  expand_range_v_func_(this, v, r);
}

template <class T>
void Dimension::expand_range(const Dimension* dim, const Range& r1, Range* r2) {
  assert(dim != nullptr);
  assert(!r1.empty());
  assert(!r2->empty());
  (void)dim;  // Not used here
  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2->data();
  T res[2] = {std::min(d1[0], d2[0]), std::max(d1[1], d2[1])};
  r2->set_range(res, sizeof(res));
}

void Dimension::expand_range(const Range& r1, Range* r2) const {
  assert(expand_range_func_ != nullptr);
  expand_range_func_(this, r1, r2);
}

template <class T>
void Dimension::expand_to_tile(const Dimension* dim, Range* range) {
  assert(dim != nullptr);
  assert(!range->empty());

  // Applicable only to regular tiles and integral domains
  if (dim->tile_extent() == nullptr || !std::is_integral<T>::value)
    return;

  auto tile_extent = *(const T*)dim->tile_extent();
  auto dim_dom = (const T*)dim->domain();
  auto r = (const T*)range->data();
  T res[2];

  res[0] = ((r[0] - dim_dom[0]) / tile_extent * tile_extent) + dim_dom[0];
  res[1] =
      ((r[1] - dim_dom[0]) / tile_extent + 1) * tile_extent - 1 + dim_dom[0];

  range->set_range(res, sizeof(res));
}

void Dimension::expand_to_tile(Range* range) const {
  assert(expand_to_tile_func_ != nullptr);
  expand_to_tile_func_(this, range);
}

template <class T>
bool Dimension::oob(
    const Dimension* dim, const void* coord, std::string* err_msg) {
  auto domain = (const T*)dim->domain();
  auto coord_t = (const T*)coord;
  if (*coord_t < domain[0] || *coord_t > domain[1]) {
    std::stringstream ss;
    ss << "Coordinate " << *coord_t << " is out of domain bounds [" << domain[0]
       << ", " << domain[1] << "] on dimension '" << dim->name() << "'";
    *err_msg = ss.str();
    return true;
  }

  return false;
}

bool Dimension::oob(const void* coord, std::string* err_msg) const {
  assert(oob_func_ != nullptr);
  return oob_func_(this, coord, err_msg);
}

template <class T>
bool Dimension::covered(
    const Dimension* dim, const Range& r1, const Range& r2) {
  assert(dim != nullptr);
  assert(!r1.empty());
  assert(!r2.empty());
  (void)dim;  // Not used here

  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2.data();
  assert(d1[0] <= d1[1]);
  assert(d2[0] <= d2[1]);

  return d1[0] >= d2[0] && d1[1] <= d2[1];
}

bool Dimension::covered(const Range& r1, const Range& r2) const {
  assert(covered_func_ != nullptr);
  return covered_func_(this, r1, r2);
}

template <class T>
bool Dimension::overlap(
    const Dimension* dim, const Range& r1, const Range& r2) {
  assert(dim != nullptr);
  assert(!r1.empty());
  assert(!r2.empty());
  (void)dim;  // Not used here

  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2.data();
  return !(d1[0] > d2[1] || d1[1] < d2[0]);
}

bool Dimension::overlap(const Range& r1, const Range& r2) const {
  assert(overlap_func_ != nullptr);
  return overlap_func_(this, r1, r2);
}

template <class T>
double Dimension::overlap_ratio(
    const Dimension* dim, const Range& r1, const Range& r2) {
  assert(dim != nullptr);
  assert(!r1.empty());
  assert(!r2.empty());
  (void)dim;  // Not used here

  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2.data();
  assert(d1[0] <= d1[1]);
  assert(d2[0] <= d2[1]);

  // No overlap
  if (d1[0] > d2[1] || d1[1] < d2[0])
    return 0.0;

  // Compute ratio
  auto overlap_start = std::max(d1[0], d2[0]);
  auto overlap_end = std::min(d1[1], d2[1]);
  auto overlap_range = overlap_end - overlap_start;
  auto mbr_range = d2[1] - d2[0];
  auto max = std::numeric_limits<double>::max();
  if (std::numeric_limits<T>::is_integer) {
    overlap_range += 1;
    mbr_range += 1;
  } else {
    if (overlap_range == 0)
      overlap_range = std::nextafter(overlap_range, max);
    if (mbr_range == 0)
      mbr_range = std::nextafter(mbr_range, max);
  }
  return (double)overlap_range / mbr_range;
}

double Dimension::overlap_ratio(const Range& r1, const Range& r2) const {
  assert(overlap_ratio_func_ != nullptr);
  return overlap_ratio_func_(this, r1, r2);
}

template <class T>
uint64_t Dimension::tile_num(const Dimension* dim, const Range& range) {
  assert(dim != nullptr);
  assert(!range.empty());

  // Trivial cases
  if (dim->tile_extent() == nullptr)
    return 1;
  if (!std::is_integral<T>::value)
    return 0;

  auto tile_extent = *(const T*)dim->tile_extent();
  auto dim_dom = (const T*)dim->domain();
  auto r = (const T*)range.data();
  uint64_t start = (r[0] - dim_dom[0]) / tile_extent;
  uint64_t end = (r[1] - dim_dom[0]) / tile_extent;
  return end - start + 1;
}

uint64_t Dimension::tile_num(const Range& range) const {
  assert(tile_num_func_ != nullptr);
  return tile_num_func_(this, range);
}

template <class T>
bool Dimension::value_in_range(
    const Dimension* dim, const void* value, const Range& range) {
  (void)*dim;  // Not used here
  assert(value != nullptr);
  assert(!range.empty());
  auto v = (const T*)value;
  auto r = (const T*)(range.data());
  return *v >= r[0] && *v <= r[1];
}

bool Dimension::value_in_range(const void* value, const Range& range) const {
  assert(value_in_range_func_ != nullptr);
  return value_in_range_func_(this, value, range);
}

// ===== FORMAT =====
// dimension_name_size (uint32_t)
// dimension_name (string)
// domain (void* - 2*type_size)
// null_tile_extent (uint8_t)
// tile_extent (void* - type_size)
Status Dimension::serialize(Buffer* buff) {
  // Sanity check
  if (domain_ == nullptr) {
    return LOG_STATUS(
        Status::DimensionError("Cannot serialize dimension; Domain not set"));
  }

  // Write dimension name
  auto dimension_name_size = (uint32_t)name_.size();
  RETURN_NOT_OK(buff->write(&dimension_name_size, sizeof(uint32_t)));
  RETURN_NOT_OK(buff->write(name_.c_str(), dimension_name_size));

  // Write domain and tile extent
  uint64_t domain_size = 2 * datatype_size(type_);
  RETURN_NOT_OK(buff->write(domain_, domain_size));

  auto null_tile_extent = (uint8_t)((tile_extent_ == nullptr) ? 1 : 0);
  RETURN_NOT_OK(buff->write(&null_tile_extent, sizeof(uint8_t)));
  if (tile_extent_ != nullptr)
    RETURN_NOT_OK(buff->write(tile_extent_, datatype_size(type_)));

  return Status::Ok();
}

Status Dimension::set_domain(const void* domain) {
  std::free(domain_);

  if (domain == nullptr) {
    domain_ = nullptr;
    return Status::Ok();
  }

  uint64_t domain_size = 2 * datatype_size(type_);
  domain_ = std::malloc(domain_size);
  if (domain_ == nullptr) {
    return LOG_STATUS(
        Status::DimensionError("Cannot set domain; Memory allocation error"));
  }
  std::memcpy(domain_, domain, domain_size);

  auto st = check_domain();
  if (!st.ok()) {
    std::free(domain_);
    domain_ = nullptr;
  }

  return st;
}

Status Dimension::set_tile_extent(const void* tile_extent) {
  if (domain_ == nullptr)
    return LOG_STATUS(Status::DimensionError(
        "Cannot set tile extent; Domain must be set first"));

  // Note: this check was added in release 1.6.0. Older arrays may have been
  // serialized with a null extent, and so it is still supported internally.
  // But users can not construct dimension objects with null tile extents.
  if (tile_extent == nullptr)
    return LOG_STATUS(Status::DimensionError(
        "Cannot set tile extent; tile extent cannot be null"));

  std::free(tile_extent_);
  if (tile_extent == nullptr) {
    tile_extent_ = nullptr;
    return Status::Ok();
  }

  uint64_t type_size = datatype_size(type_);
  tile_extent_ = std::malloc(type_size);
  if (tile_extent_ == nullptr) {
    return LOG_STATUS(Status::DimensionError(
        "Cannot set tile extent; Memory allocation error"));
  }
  std::memcpy(tile_extent_, tile_extent, type_size);

  auto st = check_tile_extent();
  if (!st.ok()) {
    std::free(domain_);
    domain_ = nullptr;
  }

  return st;
}

Status Dimension::set_null_tile_extent_to_range() {
  // Applicable only to null extents
  if (tile_extent_ != nullptr)
    return Status::Ok();

  if (domain_ == nullptr)
    return LOG_STATUS(Status::DimensionError(
        "Cannot set tile extent to domain range; Domain not set"));

  // Note: this is applicable only to dense array, which are allowed
  // only integer domains

  switch (type_) {
    case Datatype::INT32:
      return set_null_tile_extent_to_range<int>();
    case Datatype::INT64:
      return set_null_tile_extent_to_range<int64_t>();
    case Datatype::INT8:
      return set_null_tile_extent_to_range<int8_t>();
    case Datatype::UINT8:
      return set_null_tile_extent_to_range<uint8_t>();
    case Datatype::INT16:
      return set_null_tile_extent_to_range<int16_t>();
    case Datatype::UINT16:
      return set_null_tile_extent_to_range<uint16_t>();
    case Datatype::UINT32:
      return set_null_tile_extent_to_range<uint32_t>();
    case Datatype::UINT64:
      return set_null_tile_extent_to_range<uint64_t>();
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
      return set_null_tile_extent_to_range<int64_t>();
    default:
      return LOG_STATUS(
          Status::DimensionError("Cannot set null tile extent to domain range; "
                                 "Invalid dimension domain type"));
  }

  assert(false);
  return LOG_STATUS(
      Status::DimensionError("Cannot set null tile extent to domain range; "
                             "Unsupported dimension type"));
}

template <class T>
Status Dimension::set_null_tile_extent_to_range() {
  // Applicable only to null extents
  if (tile_extent_ != nullptr)
    return Status::Ok();

  // Calculate new tile extent equal to domain range
  auto domain = (T*)domain_;
  T tile_extent = domain[1] - domain[0];

  // Check overflow before adding 1
  if (tile_extent == std::numeric_limits<T>::max())
    return LOG_STATUS(Status::DimensionError(
        "Cannot set null tile extent to domain range; "
        "Domain range exceeds domain type max numeric limit"));
  ++tile_extent;
  // After this, tile_extent = domain[1] - domain[0] + 1, which is the correct
  // domain range

  // Allocate space
  uint64_t type_size = sizeof(T);
  tile_extent_ = std::malloc(type_size);
  if (tile_extent_ == nullptr) {
    return LOG_STATUS(
        Status::DimensionError("Cannot set null tile extent to domain range; "
                               "Memory allocation error"));
  }
  std::memcpy(tile_extent_, &tile_extent, type_size);

  return Status::Ok();
}

void* Dimension::tile_extent() const {
  return tile_extent_;
}

Datatype Dimension::type() const {
  return type_;
}

bool Dimension::var_size() const {
  // TODO: to fix when adding var-sized support to dimensions
  return false;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

Status Dimension::check_domain() const {
  switch (type_) {
    case Datatype::INT32:
      return check_domain<int>();
    case Datatype::INT64:
      return check_domain<int64_t>();
    case Datatype::INT8:
      return check_domain<int8_t>();
    case Datatype::UINT8:
      return check_domain<uint8_t>();
    case Datatype::INT16:
      return check_domain<int16_t>();
    case Datatype::UINT16:
      return check_domain<uint16_t>();
    case Datatype::UINT32:
      return check_domain<uint32_t>();
    case Datatype::UINT64:
      return check_domain<uint64_t>();
    case Datatype::FLOAT32:
      return check_domain<float>();
    case Datatype::FLOAT64:
      return check_domain<double>();
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
      return check_domain<int64_t>();
    default:
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Invalid dimension domain type"));
  }
}

Status Dimension::check_tile_extent() const {
  switch (type_) {
    case Datatype::INT32:
      return check_tile_extent<int>();
    case Datatype::INT64:
      return check_tile_extent<int64_t>();
    case Datatype::INT8:
      return check_tile_extent<int8_t>();
    case Datatype::UINT8:
      return check_tile_extent<uint8_t>();
    case Datatype::INT16:
      return check_tile_extent<int16_t>();
    case Datatype::UINT16:
      return check_tile_extent<uint16_t>();
    case Datatype::UINT32:
      return check_tile_extent<uint32_t>();
    case Datatype::UINT64:
      return check_tile_extent<uint64_t>();
    case Datatype::FLOAT32:
      return check_tile_extent<float>();
    case Datatype::FLOAT64:
      return check_tile_extent<double>();
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
      return check_tile_extent<int64_t>();
    default:
      return LOG_STATUS(Status::DimensionError(
          "Tile extent check failed; Invalid dimension domain type"));
  }
}

template <class T>
Status Dimension::check_tile_extent() const {
  if (domain_ == nullptr)
    return LOG_STATUS(
        Status::DimensionError("Tile extent check failed; Domain not set"));

  auto tile_extent = static_cast<T*>(tile_extent_);
  auto domain = static_cast<T*>(domain_);
  bool is_int = std::is_integral<T>::value;

  // Check if tile extent is negative or 0
  if (*tile_extent <= 0)
    return LOG_STATUS(Status::DimensionError(
        "Tile extent check failed; Tile extent must be greater than 0"));

  // Check if tile extent exceeds domain
  if (!is_int) {
    if (*tile_extent > (domain[1] - domain[0] + 1))
      return LOG_STATUS(
          Status::DimensionError("Tile extent check failed; Tile extent "
                                 "exceeds dimension domain range"));
  } else {
    // Check if tile extent exceeds domain
    uint64_t range = domain[1] - domain[0] + 1;
    if (uint64_t(*tile_extent) > range)
      return LOG_STATUS(
          Status::DimensionError("Tile extent check failed; Tile extent "
                                 "exceeds dimension domain range"));

    // In the worst case one tile extent will be added to the upper domain
    // for the dense case, so check if the expanded domain will exceed type
    // T's max limit.
    if (range % uint64_t(*tile_extent)) {
      auto upper_floor =
          ((range - 1) / (*tile_extent)) * (*tile_extent) + domain[0];
      bool exceeds =
          (upper_floor >
           std::numeric_limits<uint64_t>::max() - (*tile_extent - 1));
      exceeds =
          (exceeds ||
           uint64_t(upper_floor) > uint64_t(std::numeric_limits<T>::max()));
      if (exceeds)
        return LOG_STATUS(Status::DimensionError(
            "Tile extent check failed; domain max expanded to multiple of tile "
            "extent exceeds max value representable by domain type. Reduce "
            "domain max by 1 tile extent to allow for expansion."));
    }
  }

  return Status::Ok();
}

void Dimension::set_crop_range_func() {
  switch (type_) {
    case Datatype::INT32:
      crop_range_func_ = crop_range<int32_t>;
      break;
    case Datatype::INT64:
      crop_range_func_ = crop_range<int64_t>;
      break;
    case Datatype::INT8:
      crop_range_func_ = crop_range<int8_t>;
      break;
    case Datatype::UINT8:
      crop_range_func_ = crop_range<uint8_t>;
      break;
    case Datatype::INT16:
      crop_range_func_ = crop_range<int16_t>;
      break;
    case Datatype::UINT16:
      crop_range_func_ = crop_range<uint16_t>;
      break;
    case Datatype::UINT32:
      crop_range_func_ = crop_range<uint32_t>;
      break;
    case Datatype::UINT64:
      crop_range_func_ = crop_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      crop_range_func_ = crop_range<float>;
      break;
    case Datatype::FLOAT64:
      crop_range_func_ = crop_range<double>;
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
      crop_range_func_ = crop_range<int64_t>;
      break;
    default:
      crop_range_func_ = nullptr;
      break;
  }
}

void Dimension::set_compute_mbr_func() {
  switch (type_) {
    case Datatype::INT32:
      compute_mbr_func_ = compute_mbr<int32_t>;
      break;
    case Datatype::INT64:
      compute_mbr_func_ = compute_mbr<int64_t>;
      break;
    case Datatype::INT8:
      compute_mbr_func_ = compute_mbr<int8_t>;
      break;
    case Datatype::UINT8:
      compute_mbr_func_ = compute_mbr<uint8_t>;
      break;
    case Datatype::INT16:
      compute_mbr_func_ = compute_mbr<int16_t>;
      break;
    case Datatype::UINT16:
      compute_mbr_func_ = compute_mbr<uint16_t>;
      break;
    case Datatype::UINT32:
      compute_mbr_func_ = compute_mbr<uint32_t>;
      break;
    case Datatype::UINT64:
      compute_mbr_func_ = compute_mbr<uint64_t>;
      break;
    case Datatype::FLOAT32:
      compute_mbr_func_ = compute_mbr<float>;
      break;
    case Datatype::FLOAT64:
      compute_mbr_func_ = compute_mbr<double>;
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
      compute_mbr_func_ = compute_mbr<int64_t>;
      break;
    default:
      compute_mbr_func_ = nullptr;
      break;
  }
}

void Dimension::set_expand_range_func() {
  switch (type_) {
    case Datatype::INT32:
      expand_range_func_ = expand_range<int32_t>;
      break;
    case Datatype::INT64:
      expand_range_func_ = expand_range<int64_t>;
      break;
    case Datatype::INT8:
      expand_range_func_ = expand_range<int8_t>;
      break;
    case Datatype::UINT8:
      expand_range_func_ = expand_range<uint8_t>;
      break;
    case Datatype::INT16:
      expand_range_func_ = expand_range<int16_t>;
      break;
    case Datatype::UINT16:
      expand_range_func_ = expand_range<uint16_t>;
      break;
    case Datatype::UINT32:
      expand_range_func_ = expand_range<uint32_t>;
      break;
    case Datatype::UINT64:
      expand_range_func_ = expand_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      expand_range_func_ = expand_range<float>;
      break;
    case Datatype::FLOAT64:
      expand_range_func_ = expand_range<double>;
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
      expand_range_func_ = expand_range<int64_t>;
      break;
    default:
      expand_range_func_ = nullptr;
      break;
  }
}

void Dimension::set_expand_range_v_func() {
  switch (type_) {
    case Datatype::INT32:
      expand_range_v_func_ = expand_range_v<int32_t>;
      break;
    case Datatype::INT64:
      expand_range_v_func_ = expand_range_v<int64_t>;
      break;
    case Datatype::INT8:
      expand_range_v_func_ = expand_range_v<int8_t>;
      break;
    case Datatype::UINT8:
      expand_range_v_func_ = expand_range_v<uint8_t>;
      break;
    case Datatype::INT16:
      expand_range_v_func_ = expand_range_v<int16_t>;
      break;
    case Datatype::UINT16:
      expand_range_v_func_ = expand_range_v<uint16_t>;
      break;
    case Datatype::UINT32:
      expand_range_v_func_ = expand_range_v<uint32_t>;
      break;
    case Datatype::UINT64:
      expand_range_v_func_ = expand_range_v<uint64_t>;
      break;
    case Datatype::FLOAT32:
      expand_range_v_func_ = expand_range_v<float>;
      break;
    case Datatype::FLOAT64:
      expand_range_v_func_ = expand_range_v<double>;
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
      expand_range_v_func_ = expand_range_v<int64_t>;
      break;
    default:
      expand_range_v_func_ = nullptr;
      break;
  }
}

void Dimension::set_expand_to_tile_func() {
  switch (type_) {
    case Datatype::INT32:
      expand_to_tile_func_ = expand_to_tile<int32_t>;
      break;
    case Datatype::INT64:
      expand_to_tile_func_ = expand_to_tile<int64_t>;
      break;
    case Datatype::INT8:
      expand_to_tile_func_ = expand_to_tile<int8_t>;
      break;
    case Datatype::UINT8:
      expand_to_tile_func_ = expand_to_tile<uint8_t>;
      break;
    case Datatype::INT16:
      expand_to_tile_func_ = expand_to_tile<int16_t>;
      break;
    case Datatype::UINT16:
      expand_to_tile_func_ = expand_to_tile<uint16_t>;
      break;
    case Datatype::UINT32:
      expand_to_tile_func_ = expand_to_tile<uint32_t>;
      break;
    case Datatype::UINT64:
      expand_to_tile_func_ = expand_to_tile<uint64_t>;
      break;
    case Datatype::FLOAT32:
      expand_to_tile_func_ = expand_to_tile<float>;
      break;
    case Datatype::FLOAT64:
      expand_to_tile_func_ = expand_to_tile<double>;
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
      expand_to_tile_func_ = expand_to_tile<int64_t>;
      break;
    default:
      expand_to_tile_func_ = nullptr;
      break;
  }
}

void Dimension::set_oob_func() {
  switch (type_) {
    case Datatype::INT32:
      oob_func_ = oob<int32_t>;
      break;
    case Datatype::INT64:
      oob_func_ = oob<int64_t>;
      break;
    case Datatype::INT8:
      oob_func_ = oob<int8_t>;
      break;
    case Datatype::UINT8:
      oob_func_ = oob<uint8_t>;
      break;
    case Datatype::INT16:
      oob_func_ = oob<int16_t>;
      break;
    case Datatype::UINT16:
      oob_func_ = oob<uint16_t>;
      break;
    case Datatype::UINT32:
      oob_func_ = oob<uint32_t>;
      break;
    case Datatype::UINT64:
      oob_func_ = oob<uint64_t>;
      break;
    case Datatype::FLOAT32:
      oob_func_ = oob<float>;
      break;
    case Datatype::FLOAT64:
      oob_func_ = oob<double>;
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
      oob_func_ = oob<int64_t>;
      break;
    default:
      oob_func_ = nullptr;
      break;
  }
}

void Dimension::set_covered_func() {
  switch (type_) {
    case Datatype::INT32:
      covered_func_ = covered<int32_t>;
      break;
    case Datatype::INT64:
      covered_func_ = covered<int64_t>;
      break;
    case Datatype::INT8:
      covered_func_ = covered<int8_t>;
      break;
    case Datatype::UINT8:
      covered_func_ = covered<uint8_t>;
      break;
    case Datatype::INT16:
      covered_func_ = covered<int16_t>;
      break;
    case Datatype::UINT16:
      covered_func_ = covered<uint16_t>;
      break;
    case Datatype::UINT32:
      covered_func_ = covered<uint32_t>;
      break;
    case Datatype::UINT64:
      covered_func_ = covered<uint64_t>;
      break;
    case Datatype::FLOAT32:
      covered_func_ = covered<float>;
      break;
    case Datatype::FLOAT64:
      covered_func_ = covered<double>;
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
      covered_func_ = covered<int64_t>;
      break;
    default:
      covered_func_ = nullptr;
      break;
  }
}

void Dimension::set_overlap_func() {
  switch (type_) {
    case Datatype::INT32:
      overlap_func_ = overlap<int32_t>;
      break;
    case Datatype::INT64:
      overlap_func_ = overlap<int64_t>;
      break;
    case Datatype::INT8:
      overlap_func_ = overlap<int8_t>;
      break;
    case Datatype::UINT8:
      overlap_func_ = overlap<uint8_t>;
      break;
    case Datatype::INT16:
      overlap_func_ = overlap<int16_t>;
      break;
    case Datatype::UINT16:
      overlap_func_ = overlap<uint16_t>;
      break;
    case Datatype::UINT32:
      overlap_func_ = overlap<uint32_t>;
      break;
    case Datatype::UINT64:
      overlap_func_ = overlap<uint64_t>;
      break;
    case Datatype::FLOAT32:
      overlap_func_ = overlap<float>;
      break;
    case Datatype::FLOAT64:
      overlap_func_ = overlap<double>;
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
      overlap_func_ = overlap<int64_t>;
      break;
    default:
      overlap_func_ = nullptr;
      break;
  }
}

void Dimension::set_overlap_ratio_func() {
  switch (type_) {
    case Datatype::INT32:
      overlap_ratio_func_ = overlap_ratio<int32_t>;
      break;
    case Datatype::INT64:
      overlap_ratio_func_ = overlap_ratio<int64_t>;
      break;
    case Datatype::INT8:
      overlap_ratio_func_ = overlap_ratio<int8_t>;
      break;
    case Datatype::UINT8:
      overlap_ratio_func_ = overlap_ratio<uint8_t>;
      break;
    case Datatype::INT16:
      overlap_ratio_func_ = overlap_ratio<int16_t>;
      break;
    case Datatype::UINT16:
      overlap_ratio_func_ = overlap_ratio<uint16_t>;
      break;
    case Datatype::UINT32:
      overlap_ratio_func_ = overlap_ratio<uint32_t>;
      break;
    case Datatype::UINT64:
      overlap_ratio_func_ = overlap_ratio<uint64_t>;
      break;
    case Datatype::FLOAT32:
      overlap_ratio_func_ = overlap_ratio<float>;
      break;
    case Datatype::FLOAT64:
      overlap_ratio_func_ = overlap_ratio<double>;
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
      overlap_ratio_func_ = overlap_ratio<int64_t>;
      break;
    default:
      overlap_ratio_func_ = nullptr;
      break;
  }
}

void Dimension::set_tile_num_func() {
  switch (type_) {
    case Datatype::INT32:
      tile_num_func_ = tile_num<int32_t>;
      break;
    case Datatype::INT64:
      tile_num_func_ = tile_num<int64_t>;
      break;
    case Datatype::INT8:
      tile_num_func_ = tile_num<int8_t>;
      break;
    case Datatype::UINT8:
      tile_num_func_ = tile_num<uint8_t>;
      break;
    case Datatype::INT16:
      tile_num_func_ = tile_num<int16_t>;
      break;
    case Datatype::UINT16:
      tile_num_func_ = tile_num<uint16_t>;
      break;
    case Datatype::UINT32:
      tile_num_func_ = tile_num<uint32_t>;
      break;
    case Datatype::UINT64:
      tile_num_func_ = tile_num<uint64_t>;
      break;
    case Datatype::FLOAT32:
      tile_num_func_ = tile_num<float>;
      break;
    case Datatype::FLOAT64:
      tile_num_func_ = tile_num<double>;
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
      tile_num_func_ = tile_num<int64_t>;
      break;
    default:
      tile_num_func_ = nullptr;
      break;
  }
}

void Dimension::set_value_in_range_func() {
  switch (type_) {
    case Datatype::INT32:
      value_in_range_func_ = value_in_range<int32_t>;
      break;
    case Datatype::INT64:
      value_in_range_func_ = value_in_range<int64_t>;
      break;
    case Datatype::INT8:
      value_in_range_func_ = value_in_range<int8_t>;
      break;
    case Datatype::UINT8:
      value_in_range_func_ = value_in_range<uint8_t>;
      break;
    case Datatype::INT16:
      value_in_range_func_ = value_in_range<int16_t>;
      break;
    case Datatype::UINT16:
      value_in_range_func_ = value_in_range<uint16_t>;
      break;
    case Datatype::UINT32:
      value_in_range_func_ = value_in_range<uint32_t>;
      break;
    case Datatype::UINT64:
      value_in_range_func_ = value_in_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      value_in_range_func_ = value_in_range<float>;
      break;
    case Datatype::FLOAT64:
      value_in_range_func_ = value_in_range<double>;
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
      value_in_range_func_ = value_in_range<int64_t>;
      break;
    default:
      value_in_range_func_ = nullptr;
      break;
  }
}

}  // namespace sm

}  // namespace tiledb
