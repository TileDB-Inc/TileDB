/**
 * @file shape.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file implements class Shape.
 */

#include <iostream>
#include <sstream>

#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/random/random_label.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/storage_format/serialization/serializers.h"

#include "shape.h"

namespace tiledb::sm {

Shape::Shape(shared_ptr<MemoryTracker> memory_tracker, format_version_t version)
    : memory_tracker_(memory_tracker)
    , empty_(true)
    , version_(version) {
}

shared_ptr<const Shape> Shape::deserialize(
    Deserializer& deserializer,
    shared_ptr<MemoryTracker> memory_tracker,
    shared_ptr<Domain> domain) {
  auto disk_version = deserializer.read<uint32_t>();
  if (disk_version > constants::shape_version) {
    throw std::runtime_error(
        "Invalid shape API version on disk. '" + std::to_string(disk_version) +
        "' is newer than your current library shape version '" +
        std::to_string(constants::shape_version) + "'");
  }

  auto empty = deserializer.read<bool>();

  auto shape = make_shared<Shape>(memory_tracker, disk_version);
  if (empty) {
    return shape;
  }

  ShapeType type = static_cast<ShapeType>(deserializer.read<uint8_t>());

  switch (type) {
    case ShapeType::NDRECTANGLE: {
      auto ndrectangle =
          NDRectangle::deserialize(deserializer, memory_tracker, domain);
      shape->set_ndrectangle(ndrectangle);
      break;
    }
    default: {
      throw std::runtime_error(
          "We found an unsupported " + shape_type_str(type) +
          "array shape type on disk.");
    }
  }

  return shape;
}

void Shape::serialize(Serializer& serializer) const {
  serializer.write<uint32_t>(constants::shape_version);
  serializer.write<bool>(empty_);

  if (empty_) {
    return;
  }

  serializer.write<uint8_t>(static_cast<uint8_t>(type_));

  switch (type_) {
    case ShapeType::NDRECTANGLE: {
      ndrectangle_->serialize(serializer);
      break;
    }
    default: {
      throw std::runtime_error(
          "The shape you're trying to serialize has an unsupported type " +
          shape_type_str(type_));
    }
  }
}

void Shape::dump(FILE* out) const {
  if (out == nullptr) {
    out = stdout;
  }
  std::stringstream ss;
  ss << "### Shape ###" << std::endl;
  ss << "- Version: " << version_ << std::endl;
  ss << "- Empty: " << empty_ << std::endl;
  if (empty_) {
    fprintf(out, "%s", ss.str().c_str());
    return;
  }

  ss << "- Type: " << shape_type_str(type_) << std::endl;

  fprintf(out, "%s", ss.str().c_str());

  switch (type_) {
    case ShapeType::NDRECTANGLE: {
      ndrectangle_->dump(out);
      break;
    }
    default: {
      throw std::runtime_error(
          "The shape you're trying to dump as string has an unsupported " +
          std::string("type ") + shape_type_str(type_));
    }
  }
}

void Shape::set_ndrectangle(std::shared_ptr<const NDRectangle> ndr) {
  if (!empty_) {
    throw std::logic_error(
        "Setting a rectangle on a non-empty Shape object is not allowed.");
  }
  ndrectangle_ = ndr;
  type_ = ShapeType::NDRECTANGLE;
  empty_ = false;
}

shared_ptr<const NDRectangle> Shape::ndrectangle() const {
  if (empty_ || type_ != ShapeType::NDRECTANGLE) {
    throw std::logic_error(
        "It's not possible to get the ndrectangle from this shape if one isn't "
        "set.");
  }

  return ndrectangle_;
}

bool Shape::covered(shared_ptr<const Shape> expanded_shape) const {
  return covered(expanded_shape->ndrectangle()->get_ndranges());
}

bool Shape::covered(const NDRange& ndranges) const {
  switch (type_) {
    case ShapeType::NDRECTANGLE: {
      for (unsigned d = 0; d < ndranges.size(); ++d) {
        auto dim = ndrectangle_->domain()->dimension_ptr(d);
        if (dim->var_size() && ndranges[d].empty()) {
          // This is a free pass for array schema var size dimensions for
          // which we don't support specifying a domain.
          continue;
        }

        if (!dim->covered(ndrectangle_->get_range(d), ndranges[d])) {
          return false;
        }
      }
      return true;
    }
    default: {
      throw std::runtime_error(
          "Unable to execute this shape operation because one of the shapes " +
          std::string("passed has an unsupported") + "type " +
          shape_type_str(type_));
    }
  }
}

void Shape::check_schema_sanity(const ArraySchema& schema) const {
  switch (type_) {
    case ShapeType::NDRECTANGLE: {
      auto& ndranges = ndrectangle_->get_ndranges();

      // Dim nums match
      if (schema.dim_num() != ndranges.size()) {
        throw std::logic_error(
            "The array shape and the array schema have a non-equal "
            "number of dimensions");
      }

      // Bounds are set for all dimensions
      for (uint32_t i = 0; i < ndranges.size(); ++i) {
        if (ndranges[i].empty()) {
          throw std::logic_error(
              "This shape has no range specified for dimension idx: " +
              std::to_string(i));
        }
      }

      // Nothing is out of bounds
      if (!this->covered(schema.domain().domain())) {
        throw std::logic_error(
            "This array shape has ranges past the boundaries of the array "
            "schema domain");
      }

      break;
    }
    default: {
      throw std::runtime_error(
          "You used a Shape object which has " + std::string("an unsupported") +
          "type: " + shape_type_str(type_));
    }
  }
}

}  // namespace tiledb::sm
