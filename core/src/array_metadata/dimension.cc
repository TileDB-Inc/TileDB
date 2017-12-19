/**
 * @file   dimension.cc
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
  fprintf(out, "- Name: %s\n", name_.c_str());
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

template <class T>
Status Dimension::set_domain(const T* domain) {
  void* new_domain = nullptr;

  switch (type_) {
    case Datatype::CHAR:
      new_domain = std::malloc(2 * sizeof(char));
      std::copy(domain, domain + 2, (char*)new_domain);
      break;
    case Datatype::INT8:
      new_domain = std::malloc(2 * sizeof(int8_t));
      std::copy(domain, domain + 2, (int8_t*)new_domain);
      break;
    case Datatype::UINT8:
      new_domain = std::malloc(2 * sizeof(uint8_t));
      std::copy(domain, domain + 2, (uint8_t*)new_domain);
      break;
    case Datatype::INT16:
      new_domain = std::malloc(2 * sizeof(int16_t));
      std::copy(domain, domain + 2, (int16_t*)new_domain);
      break;
    case Datatype::UINT16:
      new_domain = std::malloc(2 * sizeof(uint16_t));
      std::copy(domain, domain + 2, (uint16_t*)new_domain);
      break;
    case Datatype::INT32:
      new_domain = std::malloc(2 * sizeof(int));
      std::copy(domain, domain + 2, (int*)new_domain);
      break;
    case Datatype::UINT32:
      new_domain = std::malloc(2 * sizeof(unsigned int));
      std::copy(domain, domain + 2, (unsigned int*)new_domain);
      break;
    case Datatype::INT64:
      new_domain = std::malloc(2 * sizeof(int64_t));
      std::copy(domain, domain + 2, (int64_t*)new_domain);
      break;
    case Datatype::UINT64:
      new_domain = std::malloc(2 * sizeof(uint64_t));
      std::copy(domain, domain + 2, (uint64_t*)new_domain);
      break;
    case Datatype::FLOAT32:
      new_domain = std::malloc(2 * sizeof(float));
      std::copy(domain, domain + 2, (float*)new_domain);
      break;
    case Datatype::FLOAT64:
      new_domain = std::malloc(2 * sizeof(double));
      std::copy(domain, domain + 2, (double*)new_domain);
      break;
  }

  // Set new domain
  assert(new_domain != nullptr);
  Status st = set_domain(new_domain);

  // Clean up and return
  std::free(new_domain);
  return st;
}

Status Dimension::set_domain(const void* domain, Datatype type) {
  // Sanity check
  if (domain == nullptr)
    return LOG_STATUS(
        Status::DimensionError("Cannot set domain; Input domain is nullptr"));

  // Type match
  if (type == type_)
    return set_domain(domain);

  // Type mismatch - convert
  switch (type) {
    case Datatype::CHAR:
      return set_domain<char>((const char*)domain);
    case Datatype::INT8:
      return set_domain<int8_t>((const int8_t*)domain);
    case Datatype::UINT8:
      return set_domain<uint8_t>((const uint8_t*)domain);
    case Datatype::INT16:
      return set_domain<int16_t>((const int16_t*)domain);
    case Datatype::UINT16:
      return set_domain<uint16_t>((const uint16_t*)domain);
    case Datatype::INT32:
      return set_domain<int>((const int*)domain);
    case Datatype::UINT32:
      return set_domain<unsigned int>((const unsigned int*)domain);
    case Datatype::INT64:
      return set_domain<int64_t>((const int64_t*)domain);
    case Datatype::UINT64:
      return set_domain<uint64_t>((const uint64_t*)domain);
    case Datatype::FLOAT32:
      return set_domain<float>((const float*)domain);
    case Datatype::FLOAT64:
      return set_domain<double>((const double*)domain);
    default:
      return Status::Error("unknown domain Datatype");
  }
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

  return Status::Ok();
}

template <class T>
Status Dimension::set_tile_extent(const T* tile_extent) {
  void* new_tile_extent = nullptr;

  switch (type_) {
    case Datatype::CHAR:
      new_tile_extent = std::malloc(sizeof(char));
      std::copy(tile_extent, tile_extent + 1, (char*)new_tile_extent);
      break;
    case Datatype::INT8:
      new_tile_extent = std::malloc(sizeof(int8_t));
      std::copy(tile_extent, tile_extent + 1, (int8_t*)new_tile_extent);
      break;
    case Datatype::UINT8:
      new_tile_extent = std::malloc(sizeof(uint8_t));
      std::copy(tile_extent, tile_extent + 1, (uint8_t*)new_tile_extent);
      break;
    case Datatype::INT16:
      new_tile_extent = std::malloc(sizeof(int16_t));
      std::copy(tile_extent, tile_extent + 1, (int16_t*)new_tile_extent);
      break;
    case Datatype::UINT16:
      new_tile_extent = std::malloc(sizeof(uint16_t));
      std::copy(tile_extent, tile_extent + 1, (uint16_t*)new_tile_extent);
      break;
    case Datatype::INT32:
      new_tile_extent = std::malloc(sizeof(int));
      std::copy(tile_extent, tile_extent + 1, (int*)new_tile_extent);
      break;
    case Datatype::UINT32:
      new_tile_extent = std::malloc(sizeof(unsigned int));
      std::copy(tile_extent, tile_extent + 1, (unsigned int*)new_tile_extent);
      break;
    case Datatype::INT64:
      new_tile_extent = std::malloc(sizeof(int64_t));
      std::copy(tile_extent, tile_extent + 1, (int64_t*)new_tile_extent);
      break;
    case Datatype::UINT64:
      new_tile_extent = std::malloc(sizeof(uint64_t));
      std::copy(tile_extent, tile_extent + 1, (uint64_t*)new_tile_extent);
      break;
    case Datatype::FLOAT32:
      new_tile_extent = std::malloc(sizeof(float));
      std::copy(tile_extent, tile_extent + 1, (float*)new_tile_extent);
      break;
    case Datatype::FLOAT64:
      new_tile_extent = std::malloc(sizeof(double));
      std::copy(tile_extent, tile_extent + 1, (double*)new_tile_extent);
      break;
  }

  // Set new tile extent
  assert(new_tile_extent != nullptr);
  Status st = set_tile_extent(new_tile_extent);

  // Clean up and return
  std::free(new_tile_extent);
  return st;
}

Status Dimension::set_tile_extent(const void* tile_extent, Datatype type) {
  // Sanity check
  if (tile_extent == nullptr) {
    tile_extent_ = nullptr;
    return Status::Ok();
  }

  // Type match
  if (type == type_)
    return set_tile_extent(tile_extent);

  // Type mismatch - convert
  switch (type) {
    case Datatype::CHAR:
      return set_tile_extent<char>((const char*)tile_extent);
    case Datatype::INT8:
      return set_tile_extent<int8_t>((const int8_t*)tile_extent);
    case Datatype::UINT8:
      return set_tile_extent<uint8_t>((const uint8_t*)tile_extent);
    case Datatype::INT16:
      return set_tile_extent<int16_t>((const int16_t*)tile_extent);
    case Datatype::UINT16:
      return set_tile_extent<uint16_t>((const uint16_t*)tile_extent);
    case Datatype::INT32:
      return set_tile_extent<int>((const int*)tile_extent);
    case Datatype::UINT32:
      return set_tile_extent<unsigned int>((const unsigned int*)tile_extent);
    case Datatype::INT64:
      return set_tile_extent<int64_t>((const int64_t*)tile_extent);
    case Datatype::UINT64:
      return set_tile_extent<uint64_t>((const uint64_t*)tile_extent);
    case Datatype::FLOAT32:
      return set_tile_extent<float>((const float*)tile_extent);
    case Datatype::FLOAT64:
      return set_tile_extent<double>((const double*)tile_extent);
    default:
      return Status::Error("unknown tile extent Datatype");
  }
}

void* Dimension::tile_extent() const {
  return tile_extent_;
}

Datatype Dimension::type() const {
  return type_;
}

}  // namespace tiledb
