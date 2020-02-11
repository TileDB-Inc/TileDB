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
#include "tiledb/sm/buffer/const_buffer.h"
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
  type_ = Datatype::INT32;
  set_oob_func();
  set_value_in_range_func();
  set_overlap_func();
  set_crop_func();
  set_expand_range_func();
  set_expand_range_to_tile_func();
  set_tile_num_func();
  set_tile_domain_func();
  set_range_start_func();
  set_mul_func();
  set_add_func();
  set_value_after_range_func();
  set_set_value_to_range_start_func();
  set_tile_coverage_func();
  set_coverage_func();
}

Dimension::Dimension(const std::string& name, Datatype type)
    : name_(name)
    , type_(type) {
  set_oob_func();
  set_value_in_range_func();
  set_overlap_func();
  set_crop_func();
  set_expand_range_func();
  set_expand_range_to_tile_func();
  set_tile_num_func();
  set_tile_domain_func();
  set_range_start_func();
  set_mul_func();
  set_add_func();
  set_value_after_range_func();
  set_set_value_to_range_start_func();
  set_tile_coverage_func();
  set_coverage_func();
}

Dimension::Dimension(const Dimension* dim) {
  assert(dim != nullptr);

  name_ = dim->name();
  type_ = dim->type_;
  oob_func_ = dim->oob_func_;
  domain_ = dim->domain_;
  tile_extent_ = dim->tile_extent_;
}

/* ********************************* */
/*                API                */
/* ********************************* */

unsigned Dimension::cell_val_num() const {
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
  domain_.resize(domain_size);
  RETURN_NOT_OK(buff->read(&domain_[0], domain_size));

  // Load tile extent
  tile_extent_.clear();
  uint8_t null_tile_extent;
  RETURN_NOT_OK(buff->read(&null_tile_extent, sizeof(uint8_t)));
  if (null_tile_extent == 0) {
    tile_extent_.resize(datatype_size(type_));
    RETURN_NOT_OK(buff->read(&tile_extent_[0], datatype_size(type_)));
  }

  set_oob_func();
  set_value_in_range_func();
  set_overlap_func();
  set_crop_func();
  set_expand_range_func();
  set_expand_range_to_tile_func();
  set_tile_num_func();
  set_tile_domain_func();
  set_range_start_func();
  set_mul_func();
  set_add_func();
  set_value_after_range_func();
  set_set_value_to_range_start_func();
  set_tile_coverage_func();
  set_coverage_func();

  return Status::Ok();
}

const Range& Dimension::domain() const {
  return domain_;
}

void Dimension::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  // Retrieve domain and tile extent strings
  std::string domain_s = utils::parse::domain_str(&domain_[0], type_);
  std::string tile_extent_s =
      utils::parse::tile_extent_str(&tile_extent_[0], type_);

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
bool Dimension::oob(
    const Dimension* dim, const void* coord, std::string* err_msg) {
  auto domain = (const T*)&dim->domain()[0];
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
bool Dimension::value_in_range(
    const Dimension* dim, const Value& value, const Range& range) {
  (void)*dim;  // Will be used in the future
  assert(!value.empty());
  assert(!range.empty());
  auto v = (const T*)&value[0];
  auto r = (const T*)&range[0];
  return *v >= r[0] && *v <= r[1];
}

bool Dimension::value_in_range(const Value& value, const Range& range) const {
  assert(value_in_range_func_ != nullptr);
  return value_in_range_func_(this, value, range);
}

template <class T>
bool Dimension::overlap(const Dimension* dim, const Range& a, const Range& b) {
  (void)*dim;  // Will be used in the future
  assert(!a.empty());
  assert(!b.empty());
  auto a_t = (const T*)&a[0];
  auto b_t = (const T*)&b[0];
  return a_t[0] <= b_t[1] && a_t[1] >= b_t[0];
}

bool Dimension::overlap(const Range& a, const Range& b) const {
  assert(overlap_func_ != nullptr);
  return overlap_func_(this, a, b);
}

template <class T>
void Dimension::crop(const Dimension* dim, Range* a, const Range& b) {
  // Applicable only to integral domains
  if (!std::is_integral<T>::value)
    return;

  (void)*dim;  // Will be used in the future
  assert(a != nullptr);
  assert(!a->empty());
  assert(!b.empty());
  auto a_t = (T*)&(*a)[0];
  auto b_t = (const T*)&b[0];

  if (a_t[0] < b_t[0])
    a_t[0] = b_t[0];
  if (a_t[1] > b_t[1])
    a_t[1] = b_t[1];
}

void Dimension::crop(Range* a, const Range& b) const {
  assert(crop_func_ != nullptr);
  crop_func_(this, a, b);
}

template <class T>
void Dimension::expand_range(const Dimension* dim, Range* a, const Range& b) {
  (void)*dim;  // Will be used in the future
  assert(a != nullptr);
  assert(!a->empty());
  assert(!b.empty());
  auto a_t = (T*)&(*a)[0];
  auto b_t = (const T*)&b[0];

  if (a_t[0] > b_t[0])
    a_t[0] = b_t[0];
  if (a_t[1] < b_t[1])
    a_t[1] = b_t[1];
}

void Dimension::expand_range(Range* a, const Range& b) const {
  assert(expand_range_func_ != nullptr);
  expand_range_func_(this, a, b);
}

template <class T>
void Dimension::expand_range_to_tile(const Dimension* dim, Range* range) {
  (void)*dim;  // Will be used in the future
  assert(range != nullptr);
  assert(!range->empty());

  auto tile_extent = dim->tile_extent();
  if (!std::is_integral<T>() || tile_extent == nullptr)
    return;

  auto tile_extent_v = *(const T*)tile_extent;
  auto dim_domain_low = *(const T*)&dim->domain()[0];
  auto r = (T*)&(*range)[0];

  r[0] = ((r[0] - dim_domain_low) / tile_extent_v * tile_extent_v) +
         dim_domain_low;

  r[1] = ((r[1] - dim_domain_low) / tile_extent_v + 1) * tile_extent_v - 1 +
         dim_domain_low;
}

void Dimension::expand_range_to_tile(Range* range) const {
  assert(expand_range_to_tile_func_ != nullptr);
  expand_range_to_tile_func_(this, range);
}

template <class T>
uint64_t Dimension::tile_num(const Dimension* dim, const Range& range) {
  (void)*dim;  // Will be used in the future
  assert(!range.empty());

  auto tile_extent_v = *(const T*)dim->tile_extent();
  auto dim_domain_low = *(const T*)&dim->domain()[0];
  auto r = (const T*)&range[0];

  uint64_t start, end;
  if (std::is_integral<T>()) {  // Integers
    start = (r[0] - dim_domain_low) / tile_extent_v;
    end = (r[1] - dim_domain_low) / tile_extent_v;
  } else {  // Reals
    start = std::floor((r[0] - dim_domain_low) / tile_extent_v);
    end = std::floor((r[1] - dim_domain_low) / tile_extent_v);
  }

  return end - start + 1;
}

uint64_t Dimension::tile_num(const Range& range) const {
  assert(tile_num_func_ != nullptr);
  return tile_num_func_(this, range);
}

template <class T>
Range Dimension::tile_domain(
    const Dimension* dim, const Range& range, const Range& domain) {
  (void)*dim;  // Will be used in the future
  assert(!range.empty());
  assert(!domain.empty());

  Range ret(2 * sizeof(T));
  auto range_v = (const T*)&range[0];
  auto domain_v = (const T*)&domain[0];
  auto ret_v = (T*)&ret[0];
  auto tile_extent_v = *(const T*)dim->tile_extent();

  auto overlap = std::max(range_v[0], domain_v[0]);
  ret_v[0] = std::floor((overlap - domain_v[0]) / tile_extent_v);

  overlap = std::min(range_v[1], domain_v[1]);
  ret_v[1] = std::floor((overlap - domain[0]) / tile_extent_v);

  return ret;
}

Range Dimension::tile_domain(const Range& range, const Range& domain) const {
  assert(tile_domain_func_ != nullptr);
  return tile_domain_func_(this, range, domain);
}

template <class T>
Value Dimension::range_start(const Dimension* dim, const Range& range) {
  (void)*dim;  // Will be used in the future
  assert(!range.empty());

  Value ret(sizeof(T));
  std::memcpy(&ret[0], &range[0], sizeof(T));
  return ret;
}

Value Dimension::range_start(const Range& range) const {
  assert(range_start_func_ != nullptr);
  return range_start_func_(this, range);
}

template <class T>
uint64_t Dimension::mul(
    const tiledb::sm::Dimension* dim, const Value& a, uint64_t b) {
  (void)*dim;  // Will be used in the future
  assert(!a.empty());

  auto a_v = *(const T*)&a[0];
  return (uint64_t)a_v * b;
}

uint64_t Dimension::mul(const Value& a, uint64_t b) const {
  assert(mul_func_ != nullptr);
  return mul_func_(this, a, b);
}

template <class T>
void Dimension::add(const Dimension* dim, Value* a, uint64_t b) {
  (void)*dim;  // Will be used in the future
  assert(!a->empty());

  auto a_v = (T*)&a[0];
  *a_v = *a_v + b;
}

void Dimension::add(Value* a, uint64_t b) const {
  assert(add_func_ != nullptr);
  add_func_(this, a, b);
}

template <class T>
bool Dimension::value_after_range(
    const tiledb::sm::Dimension* dim, const Value& value, const Range& range) {
  (void)*dim;  // Will be used in the future
  assert(!value.empty());
  assert(!range.empty());

  auto v = (const T*)&value[0];
  auto r = (const T*)&range[0];
  return *v >= r[1];
}

bool Dimension::value_after_range(
    const Value& value, const Range& range) const {
  assert(value_after_range_func_ != nullptr);
  return value_after_range_func_(this, value, range);
}

template <class T>
void Dimension::set_value_to_range_start(
    const Dimension* dim, Value* value, const Range& range) {
  (void)*dim;  // Will be used in the future
  assert(!value->empty());
  assert(!range.empty());

  auto v = (T*)&value[0];
  auto r = (const T*)&range[0];
  *v = r[0];
}

void Dimension::set_value_to_range_start(
    Value* value, const Range& range) const {
  assert(set_value_to_range_start_func_ != nullptr);
  set_value_to_range_start_func_(this, value, range);
}

template <class T>
double Dimension::tile_coverage(
    const Dimension* dim,
    const Range& domain,
    const Value& tile_coords,
    const Range& range) {
  (void)*dim;  // Will be used in the future
  assert(!domain.empty());
  assert(!tile_coords.empty());
  assert(!range.empty());
  assert(dim->tile_extent() != nullptr);

  auto tile_coords_v = *(const T*)&tile_coords[0];
  auto tile_extent_v = *(const T*)dim->tile_extent();
  auto domain_v = (const T*)&domain[0];
  auto range_v = (const T*)&range[0];

  // Get the range of the tile indicated by `tile_coords`
  T tile_range[2];
  tile_range[0] = tile_coords_v * tile_extent_v + domain_v[0];
  tile_range[1] = (tile_coords_v + 1) * tile_extent_v - 1 + domain_v[0];

  // Get overlap between tile range and `range`
  T overlap[2];
  overlap[0] = std::max(tile_range[0], range_v[0]);
  overlap[1] = std::min(tile_range[1], range_v[1]);
  if (overlap[0] > range_v[1] || overlap[1] < range_v[0])
    return 0.0;

  // Get coverage between the overlap and the range
  auto add = int(std::is_integral<T>::value);
  auto overlap_len = double(overlap[1]) - overlap[0] + add;
  auto range_len = double(range_v[1]) - range_v[0] + add;
  if (!std::numeric_limits<T>::is_integer) {
    auto max = std::numeric_limits<T>::max();
    if (overlap_len == 0)
      overlap_len = std::nextafter(overlap_len, max);
    if (range_len == 0)
      range_len = std::nextafter(range_len, max);
  }
  double cov = overlap_len / range_len;

  // At this point, we know that the tile coverage should not be 0.
  // If cov goes to 0, then it is because it is too small, so we should set cov
  // to epsilon.
  return (cov != 0) ? cov :
                      std::nextafter(0, std::numeric_limits<double>::max());
}

double Dimension::tile_coverage(
    const Range& domain, const Value& tile_coords, const Range& range) const {
  assert(tile_coverage_func_ != nullptr);
  return tile_coverage_func_(this, domain, tile_coords, range);
}

template <class T>
double Dimension::coverage(
    const Dimension* dim, const Range& a, const Range& b) {
  (void)*dim;  // Will be used in the future
  assert(!a.empty());
  assert(!b.empty());

  auto a_v = (const T*)&a[0];
  auto b_v = (const T*)&b[0];

  // Get overlap between the two ranges
  T overlap[2];
  overlap[0] = std::max(a_v[0], b_v[0]);
  overlap[1] = std::min(a_v[1], b_v[1]);
  if (overlap[0] > a_v[1] || overlap[1] < a_v[0])
    return 0.0;

  // Get coverage between the overlap and `b`
  auto add = int(std::is_integral<T>::value);
  auto overlap_len = double(overlap[1]) - overlap[0] + add;
  auto b_len = double(b_v[1]) - b_v[0] + add;
  if (!std::numeric_limits<T>::is_integer) {
    auto max = std::numeric_limits<T>::max();
    if (overlap_len == 0)
      overlap_len = std::nextafter(overlap_len, max);
    if (b_len == 0)
      b_len = std::nextafter(b_len, max);
  }
  double cov = overlap_len / b_len;

  // At this point, we know that the tile coverage should not be 0.
  // If cov goes to 0, then it is because it is too small, so we should set cov
  // to epsilon.
  return (cov != 0) ? cov :
                      std::nextafter(0, std::numeric_limits<double>::max());
}

double Dimension::coverage(const Range& a, const Range& b) const {
  assert(coverage_func_ != nullptr);
  return coverage_func_(this, a, b);
}

// ===== FORMAT =====
// dimension_name_size (uint32_t)
// dimension_name (string)
// domain (void* - 2*type_size)
// null_tile_extent (uint8_t)
// tile_extent (void* - type_size)
Status Dimension::serialize(Buffer* buff) {
  // Sanity check
  if (domain_.empty()) {
    return LOG_STATUS(
        Status::DimensionError("Cannot serialize dimension; Domain not set"));
  }

  // Write dimension name
  auto dimension_name_size = (uint32_t)name_.size();
  RETURN_NOT_OK(buff->write(&dimension_name_size, sizeof(uint32_t)));
  RETURN_NOT_OK(buff->write(name_.c_str(), dimension_name_size));

  // Write domain and tile extent
  uint64_t domain_size = 2 * datatype_size(type_);
  RETURN_NOT_OK(buff->write(&domain_[0], domain_size));

  auto null_tile_extent = (uint8_t)((tile_extent_.empty()) ? 1 : 0);
  RETURN_NOT_OK(buff->write(&null_tile_extent, sizeof(uint8_t)));
  if (!tile_extent_.empty())
    RETURN_NOT_OK(buff->write(&tile_extent_[0], datatype_size(type_)));

  return Status::Ok();
}

Status Dimension::set_domain(const void* domain) {
  domain_.clear();
  if (domain == nullptr)
    return Status::Ok();

  uint64_t domain_size = 2 * datatype_size(type_);
  domain_.resize(domain_size);
  std::memcpy(&domain_[0], domain, domain_size);

  RETURN_NOT_OK_ELSE(check_domain(), domain_.clear());

  return Status::Ok();
}

Status Dimension::set_domain(const Range& domain) {
  domain_ = domain;
  if (domain.empty())
    return Status::Ok();

  RETURN_NOT_OK_ELSE(check_domain(), domain_.clear());

  return Status::Ok();
}

Status Dimension::set_tile_extent(const void* tile_extent) {
  if (domain_.empty())
    return Status::DimensionError(
        "Cannot set tile extent; Domain must be set first");

  // Note: this check was added in release 1.6.0. Older arrays may have been
  // serialized with a null extent, and so it is still supported internally.
  // But users can not construct dimension objects with null tile extents.
  if (tile_extent == nullptr)
    return LOG_STATUS(Status::DimensionError(
        "Cannot set tile extent; tile extent cannot be null"));

  tile_extent_.clear();
  uint64_t type_size = datatype_size(type_);
  tile_extent_.resize(type_size);
  std::memcpy(&tile_extent_[0], tile_extent, type_size);

  RETURN_NOT_OK_ELSE(check_tile_extent(), tile_extent_.clear());

  return Status::Ok();
}

Status Dimension::set_null_tile_extent_to_range() {
  // Applicable only to null extents
  if (!tile_extent_.empty())
    return Status::Ok();

  if (domain_.empty())
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
  if (!tile_extent_.empty())
    return Status::Ok();

  // Calculate new tile extent equal to domain range
  auto domain = (T*)&domain_[0];
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
  tile_extent_.resize(type_size);
  std::memcpy(&tile_extent_[0], &tile_extent, type_size);

  return Status::Ok();
}

const void* Dimension::tile_extent() const {
  return tile_extent_.empty() ? nullptr : &tile_extent_[0];
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
  if (domain_.empty())
    return LOG_STATUS(
        Status::DimensionError("Tile extent check failed; Domain not set"));

  assert(!tile_extent_.empty());
  auto tile_extent = (const T*)&tile_extent_[0];
  auto domain = (const T*)&domain_[0];
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

void Dimension::set_crop_func() {
  switch (type_) {
    case Datatype::INT32:
      crop_func_ = crop<int32_t>;
      break;
    case Datatype::INT64:
      crop_func_ = crop<int64_t>;
      break;
    case Datatype::INT8:
      crop_func_ = crop<int8_t>;
      break;
    case Datatype::UINT8:
      crop_func_ = crop<uint8_t>;
      break;
    case Datatype::INT16:
      crop_func_ = crop<int16_t>;
      break;
    case Datatype::UINT16:
      crop_func_ = crop<uint16_t>;
      break;
    case Datatype::UINT32:
      crop_func_ = crop<uint32_t>;
      break;
    case Datatype::UINT64:
      crop_func_ = crop<uint64_t>;
      break;
    case Datatype::FLOAT32:
      crop_func_ = crop<float>;
      break;
    case Datatype::FLOAT64:
      crop_func_ = crop<double>;
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
      crop_func_ = crop<int64_t>;
      break;
    default:
      crop_func_ = nullptr;
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

void Dimension::set_expand_range_to_tile_func() {
  switch (type_) {
    case Datatype::INT32:
      expand_range_to_tile_func_ = expand_range_to_tile<int32_t>;
      break;
    case Datatype::INT64:
      expand_range_to_tile_func_ = expand_range_to_tile<int64_t>;
      break;
    case Datatype::INT8:
      expand_range_to_tile_func_ = expand_range_to_tile<int8_t>;
      break;
    case Datatype::UINT8:
      expand_range_to_tile_func_ = expand_range_to_tile<uint8_t>;
      break;
    case Datatype::INT16:
      expand_range_to_tile_func_ = expand_range_to_tile<int16_t>;
      break;
    case Datatype::UINT16:
      expand_range_to_tile_func_ = expand_range_to_tile<uint16_t>;
      break;
    case Datatype::UINT32:
      expand_range_to_tile_func_ = expand_range_to_tile<uint32_t>;
      break;
    case Datatype::UINT64:
      expand_range_to_tile_func_ = expand_range_to_tile<uint64_t>;
      break;
    case Datatype::FLOAT32:
      expand_range_to_tile_func_ = expand_range_to_tile<float>;
      break;
    case Datatype::FLOAT64:
      expand_range_to_tile_func_ = expand_range_to_tile<double>;
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
      expand_range_to_tile_func_ = expand_range_to_tile<int64_t>;
      break;
    default:
      expand_range_to_tile_func_ = nullptr;
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

void Dimension::set_tile_domain_func() {
  switch (type_) {
    case Datatype::INT32:
      tile_domain_func_ = tile_domain<int32_t>;
      break;
    case Datatype::INT64:
      tile_domain_func_ = tile_domain<int64_t>;
      break;
    case Datatype::INT8:
      tile_domain_func_ = tile_domain<int8_t>;
      break;
    case Datatype::UINT8:
      tile_domain_func_ = tile_domain<uint8_t>;
      break;
    case Datatype::INT16:
      tile_domain_func_ = tile_domain<int16_t>;
      break;
    case Datatype::UINT16:
      tile_domain_func_ = tile_domain<uint16_t>;
      break;
    case Datatype::UINT32:
      tile_domain_func_ = tile_domain<uint32_t>;
      break;
    case Datatype::UINT64:
      tile_domain_func_ = tile_domain<uint64_t>;
      break;
    case Datatype::FLOAT32:
      tile_domain_func_ = tile_domain<float>;
      break;
    case Datatype::FLOAT64:
      tile_domain_func_ = tile_domain<double>;
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
      tile_domain_func_ = tile_domain<int64_t>;
      break;
    default:
      tile_domain_func_ = nullptr;
      break;
  }
}

void Dimension::set_range_start_func() {
  switch (type_) {
    case Datatype::INT32:
      range_start_func_ = range_start<int32_t>;
      break;
    case Datatype::INT64:
      range_start_func_ = range_start<int64_t>;
      break;
    case Datatype::INT8:
      range_start_func_ = range_start<int8_t>;
      break;
    case Datatype::UINT8:
      range_start_func_ = range_start<uint8_t>;
      break;
    case Datatype::INT16:
      range_start_func_ = range_start<int16_t>;
      break;
    case Datatype::UINT16:
      range_start_func_ = range_start<uint16_t>;
      break;
    case Datatype::UINT32:
      range_start_func_ = range_start<uint32_t>;
      break;
    case Datatype::UINT64:
      range_start_func_ = range_start<uint64_t>;
      break;
    case Datatype::FLOAT32:
      range_start_func_ = range_start<float>;
      break;
    case Datatype::FLOAT64:
      range_start_func_ = range_start<double>;
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
      range_start_func_ = range_start<int64_t>;
      break;
    default:
      range_start_func_ = nullptr;
      break;
  }
}

void Dimension::set_mul_func() {
  switch (type_) {
    case Datatype::INT32:
      mul_func_ = mul<int32_t>;
      break;
    case Datatype::INT64:
      mul_func_ = mul<int64_t>;
      break;
    case Datatype::INT8:
      mul_func_ = mul<int8_t>;
      break;
    case Datatype::UINT8:
      mul_func_ = mul<uint8_t>;
      break;
    case Datatype::INT16:
      mul_func_ = mul<int16_t>;
      break;
    case Datatype::UINT16:
      mul_func_ = mul<uint16_t>;
      break;
    case Datatype::UINT32:
      mul_func_ = mul<uint32_t>;
      break;
    case Datatype::UINT64:
      mul_func_ = mul<uint64_t>;
      break;
    case Datatype::FLOAT32:
      mul_func_ = mul<float>;
      break;
    case Datatype::FLOAT64:
      mul_func_ = mul<double>;
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
      mul_func_ = mul<int64_t>;
      break;
    default:
      mul_func_ = nullptr;
      break;
  }
}

void Dimension::set_add_func() {
  switch (type_) {
    case Datatype::INT32:
      add_func_ = add<int32_t>;
      break;
    case Datatype::INT64:
      add_func_ = add<int64_t>;
      break;
    case Datatype::INT8:
      add_func_ = add<int8_t>;
      break;
    case Datatype::UINT8:
      add_func_ = add<uint8_t>;
      break;
    case Datatype::INT16:
      add_func_ = add<int16_t>;
      break;
    case Datatype::UINT16:
      add_func_ = add<uint16_t>;
      break;
    case Datatype::UINT32:
      add_func_ = add<uint32_t>;
      break;
    case Datatype::UINT64:
      add_func_ = add<uint64_t>;
      break;
    case Datatype::FLOAT32:
      add_func_ = add<float>;
      break;
    case Datatype::FLOAT64:
      add_func_ = add<double>;
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
      add_func_ = add<int64_t>;
      break;
    default:
      add_func_ = nullptr;
      break;
  }
}

void Dimension::set_value_after_range_func() {
  switch (type_) {
    case Datatype::INT32:
      value_after_range_func_ = value_after_range<int32_t>;
      break;
    case Datatype::INT64:
      value_after_range_func_ = value_after_range<int64_t>;
      break;
    case Datatype::INT8:
      value_after_range_func_ = value_after_range<int8_t>;
      break;
    case Datatype::UINT8:
      value_after_range_func_ = value_after_range<uint8_t>;
      break;
    case Datatype::INT16:
      value_after_range_func_ = value_after_range<int16_t>;
      break;
    case Datatype::UINT16:
      value_after_range_func_ = value_after_range<uint16_t>;
      break;
    case Datatype::UINT32:
      value_after_range_func_ = value_after_range<uint32_t>;
      break;
    case Datatype::UINT64:
      value_after_range_func_ = value_after_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      value_after_range_func_ = value_after_range<float>;
      break;
    case Datatype::FLOAT64:
      value_after_range_func_ = value_after_range<double>;
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
      value_after_range_func_ = value_after_range<int64_t>;
      break;
    default:
      value_after_range_func_ = nullptr;
      break;
  }
}

void Dimension::set_set_value_to_range_start_func() {
  switch (type_) {
    case Datatype::INT32:
      set_value_to_range_start_func_ = set_value_to_range_start<int32_t>;
      break;
    case Datatype::INT64:
      set_value_to_range_start_func_ = set_value_to_range_start<int64_t>;
      break;
    case Datatype::INT8:
      set_value_to_range_start_func_ = set_value_to_range_start<int8_t>;
      break;
    case Datatype::UINT8:
      set_value_to_range_start_func_ = set_value_to_range_start<uint8_t>;
      break;
    case Datatype::INT16:
      set_value_to_range_start_func_ = set_value_to_range_start<int16_t>;
      break;
    case Datatype::UINT16:
      set_value_to_range_start_func_ = set_value_to_range_start<uint16_t>;
      break;
    case Datatype::UINT32:
      set_value_to_range_start_func_ = set_value_to_range_start<uint32_t>;
      break;
    case Datatype::UINT64:
      set_value_to_range_start_func_ = set_value_to_range_start<uint64_t>;
      break;
    case Datatype::FLOAT32:
      set_value_to_range_start_func_ = set_value_to_range_start<float>;
      break;
    case Datatype::FLOAT64:
      set_value_to_range_start_func_ = set_value_to_range_start<double>;
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
      set_value_to_range_start_func_ = set_value_to_range_start<int64_t>;
      break;
    default:
      set_value_to_range_start_func_ = nullptr;
      break;
  }
}

void Dimension::set_tile_coverage_func() {
  switch (type_) {
    case Datatype::INT32:
      tile_coverage_func_ = tile_coverage<int32_t>;
      break;
    case Datatype::INT64:
      tile_coverage_func_ = tile_coverage<int64_t>;
      break;
    case Datatype::INT8:
      tile_coverage_func_ = tile_coverage<int8_t>;
      break;
    case Datatype::UINT8:
      tile_coverage_func_ = tile_coverage<uint8_t>;
      break;
    case Datatype::INT16:
      tile_coverage_func_ = tile_coverage<int16_t>;
      break;
    case Datatype::UINT16:
      tile_coverage_func_ = tile_coverage<uint16_t>;
      break;
    case Datatype::UINT32:
      tile_coverage_func_ = tile_coverage<uint32_t>;
      break;
    case Datatype::UINT64:
      tile_coverage_func_ = tile_coverage<uint64_t>;
      break;
    case Datatype::FLOAT32:
      tile_coverage_func_ = tile_coverage<float>;
      break;
    case Datatype::FLOAT64:
      tile_coverage_func_ = tile_coverage<double>;
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
      tile_coverage_func_ = tile_coverage<int64_t>;
      break;
    default:
      tile_coverage_func_ = nullptr;
      break;
  }
}

void Dimension::set_coverage_func() {
  switch (type_) {
    case Datatype::INT32:
      coverage_func_ = coverage<int32_t>;
      break;
    case Datatype::INT64:
      coverage_func_ = coverage<int64_t>;
      break;
    case Datatype::INT8:
      coverage_func_ = coverage<int8_t>;
      break;
    case Datatype::UINT8:
      coverage_func_ = coverage<uint8_t>;
      break;
    case Datatype::INT16:
      coverage_func_ = coverage<int16_t>;
      break;
    case Datatype::UINT16:
      coverage_func_ = coverage<uint16_t>;
      break;
    case Datatype::UINT32:
      coverage_func_ = coverage<uint32_t>;
      break;
    case Datatype::UINT64:
      coverage_func_ = coverage<uint64_t>;
      break;
    case Datatype::FLOAT32:
      coverage_func_ = coverage<float>;
      break;
    case Datatype::FLOAT64:
      coverage_func_ = coverage<double>;
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
      coverage_func_ = coverage<int64_t>;
      break;
    default:
      coverage_func_ = nullptr;
      break;
  }
}

}  // namespace sm

}  // namespace tiledb
