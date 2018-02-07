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

#include "dimension.h"
#include "const_buffer.h"
#include "logger.h"
#include "utils.h"

#include <cassert>
#include <iostream>

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Dimension::Dimension() {
  domain_ = nullptr;
  tile_extent_ = nullptr;
}

Dimension::Dimension(const char* name, Datatype type) {
  // Set name
  if (name != nullptr)
    name_ = name;

  // Set type, domain and tile extent
  type_ = type;
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
  if (domain_ != nullptr)
    std::free(domain_);
  if (tile_extent_ != nullptr)
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
  if (domain_ != nullptr)
    std::free(domain_);
  domain_ = std::malloc(domain_size);
  if (domain_ == nullptr)
    return LOG_STATUS(
        Status::DimensionError("Cannot deserialize; Memory allocation failed"));
  RETURN_NOT_OK(buff->read(domain_, domain_size));

  // Load tile extent
  if (tile_extent_ != nullptr)
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
  std::string domain_s = utils::domain_str(domain_, type_);
  std::string tile_extent_s = utils::tile_extent_str(tile_extent_, type_);

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
         utils::starts_with(name_, constants::default_dim_name);
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
  if (domain_ != nullptr)
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

  return Status::Ok();
}

Status Dimension::set_tile_extent(const void* tile_extent) {
  if (tile_extent_ != nullptr)
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

  RETURN_NOT_OK(check_tile_extent());

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
      assert(0);
      return LOG_STATUS(Status::DimensionError(
          "Tile extent check failed; Invalid dimension domain type"));
  }
}

template <class T>
Status Dimension::check_tile_extent() const {
  auto tile_extent = static_cast<T*>(tile_extent_);
  auto domain = static_cast<T*>(domain_);

  if (&typeid(T) == &typeid(float) || &typeid(T) == &typeid(double)) {
    if (*tile_extent > domain[1] - domain[0])
      return LOG_STATUS(
          Status::DimensionError("Tile extent check failed; Tile extent "
                                 "exceeds dimension domain range"));
  } else {
    if (*tile_extent > domain[1] - domain[0] + 1)
      return LOG_STATUS(
          Status::DimensionError("Tile extent check failed; Tile extent "
                                 "exceeds dimension domain range"));
  }

  return Status::Ok();
}

}  // namespace tiledb
