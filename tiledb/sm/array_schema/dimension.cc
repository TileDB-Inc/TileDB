/**
 * @file   dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <iostream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Dimension::Dimension() {
  domain_ = nullptr;
  tile_extent_ = nullptr;
  type_ = Datatype::INT32;
}

Dimension::Dimension(const std::string& name, Datatype type)
    : name_(name)
    , type_(type) {
  domain_ = nullptr;
  tile_extent_ = nullptr;
}

Dimension::Dimension(const Dimension* dim) {
  assert(dim != nullptr);

  name_ = dim->name();
  type_ = dim->type_;
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

// ===== FORMAT =====
// dimension_name_size (unsigned int)
// dimension_name (string)
// domain (void* - 2*type_size)
// null_tile_extent (bool)
// tile_extent (void* - type_size)
Status Dimension::deserialize(ConstBuffer* buff, Datatype type) {
  // Set type
  type_ = type;

  // Load dimension name
  unsigned int dimension_name_size;
  RETURN_NOT_OK(buff->read(&dimension_name_size, sizeof(unsigned int)));
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
  bool null_tile_extent;
  RETURN_NOT_OK(buff->read(&null_tile_extent, sizeof(bool)));
  if (!null_tile_extent) {
    tile_extent_ = std::malloc(datatype_size(type_));
    if (tile_extent_ == nullptr) {
      return LOG_STATUS(Status::DimensionError(
          "Cannot deserialize; Memory allocation failed"));
    }
    RETURN_NOT_OK(buff->read(tile_extent_, datatype_size(type_)));
  }

  return Status::Ok();
}

void* Dimension::domain() const {
  return domain_;
}

void Dimension::dump(FILE* out) const {
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

const std::string& Dimension::name() const {
  return name_;
}

bool Dimension::is_anonymous() const {
  return name_.empty() ||
         utils::parse::starts_with(name_, constants::default_dim_name);
}

// ===== FORMAT =====
// dimension_name_size (unsigned int)
// dimension_name (string)
// domain (void* - 2*type_size)
// null_tile_extent (bool)
// tile_extent (void* - type_size)
Status Dimension::serialize(Buffer* buff) {
  // Sanity check
  if (domain_ == nullptr) {
    return LOG_STATUS(
        Status::DimensionError("Cannot serialize dimension; Domain not set"));
  }

  // Write dimension name
  auto dimension_name_size = (unsigned int)name_.size();
  RETURN_NOT_OK(buff->write(&dimension_name_size, sizeof(unsigned int)));
  RETURN_NOT_OK(buff->write(name_.c_str(), dimension_name_size));

  // Write domain and tile extent
  uint64_t domain_size = 2 * datatype_size(type_);
  RETURN_NOT_OK(buff->write(domain_, domain_size));

  bool null_tile_extent = (tile_extent_ == nullptr);
  RETURN_NOT_OK(buff->write(&null_tile_extent, sizeof(bool)));
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
    return Status::DimensionError(
        "Cannot set tile extent; Domain must be set first");

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
    default:
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Invalid dimension domain type"));
  }
}

template <class T>
Status Dimension::check_domain() const {
  assert(domain_ != nullptr);

  // Upper bound should not be smaller than lower
  auto domain = static_cast<const T*>(domain_);
  if (domain[1] < domain[0])
    return LOG_STATUS(
        Status::DimensionError("Domain check failed; Upper domain bound should "
                               "not be smaller than the lower one"));

  // Domain range must not exceed the maximum uint64_t number
  // for integer domains
  if (std::is_integral<T>::value) {
    uint64_t diff = domain[1] - domain[0];
    if (diff == std::numeric_limits<uint64_t>::max())
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Domain range (upper + lower + 1) is larger "
          "than the maximum uint64 number"));
  }

  return Status::Ok();
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
    if (*tile_extent > domain[1] - domain[0])
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

}  // namespace sm

}  // namespace tiledb
