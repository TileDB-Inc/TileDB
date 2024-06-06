/**
 * @file current_domain.cc
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
 * This file implements class CurrentDomain.
 */

#include <iostream>
#include <sstream>

#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/random/random_label.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/storage_format/serialization/serializers.h"

#include "current_domain.h"

namespace tiledb::sm {

CurrentDomain::CurrentDomain(
    shared_ptr<MemoryTracker> memory_tracker, format_version_t version)
    : memory_tracker_(memory_tracker)
    , empty_(true)
    , version_(version) {
}

CurrentDomain::CurrentDomain(
    shared_ptr<MemoryTracker> memory_tracker,
    format_version_t version,
    shared_ptr<const NDRectangle> ndr)
    : CurrentDomain(memory_tracker, version) {
  set_ndrectangle(ndr);
}

shared_ptr<const CurrentDomain> CurrentDomain::deserialize(
    Deserializer& deserializer,
    shared_ptr<MemoryTracker> memory_tracker,
    shared_ptr<Domain> domain) {
  auto disk_version = deserializer.read<uint32_t>();
  if (disk_version > constants::current_domain_version) {
    throw std::runtime_error(
        "Invalid current_domain API version on disk. '" +
        std::to_string(disk_version) +
        "' is newer than your current library current_domain version '" +
        std::to_string(constants::current_domain_version) + "'");
  }

  auto empty = deserializer.read<bool>();

  if (empty) {
    return make_shared<CurrentDomain>(memory_tracker, disk_version);
  }

  CurrentDomainType type =
      static_cast<CurrentDomainType>(deserializer.read<uint8_t>());

  switch (type) {
    case CurrentDomainType::NDRECTANGLE: {
      auto ndrectangle =
          NDRectangle::deserialize(deserializer, memory_tracker, domain);
      return make_shared<CurrentDomain>(
          memory_tracker, disk_version, ndrectangle);
    }
    default: {
      throw std::runtime_error(
          "We found an unsupported " + current_domain_type_str(type) +
          "array current_domain type on disk.");
    }
  }
}

void CurrentDomain::serialize(Serializer& serializer) const {
  serializer.write<uint32_t>(constants::current_domain_version);
  serializer.write<bool>(empty_);

  if (empty_) {
    return;
  }

  serializer.write<uint8_t>(static_cast<uint8_t>(type_));

  switch (type_) {
    case CurrentDomainType::NDRECTANGLE: {
      ndrectangle_->serialize(serializer);
      break;
    }
    default: {
      throw std::runtime_error(
          "The current_domain you're trying to serialize has an unsupported "
          "type " +
          current_domain_type_str(type_));
    }
  }
}

void CurrentDomain::dump(FILE* out) const {
  if (out == nullptr) {
    out = stdout;
  }
  std::stringstream ss;
  ss << "### CurrentDomain ###" << std::endl;
  ss << "- Version: " << version_ << std::endl;
  ss << "- Empty: " << empty_ << std::endl;
  if (empty_) {
    fprintf(out, "%s", ss.str().c_str());
    return;
  }

  ss << "- Type: " << current_domain_type_str(type_) << std::endl;

  fprintf(out, "%s", ss.str().c_str());

  switch (type_) {
    case CurrentDomainType::NDRECTANGLE: {
      ndrectangle_->dump(out);
      break;
    }
    default: {
      throw std::runtime_error(
          "The current_domain you're trying to dump as string has an "
          "unsupported " +
          std::string("type ") + current_domain_type_str(type_));
    }
  }
}

void CurrentDomain::set_ndrectangle(std::shared_ptr<const NDRectangle> ndr) {
  if (!empty_) {
    throw std::logic_error(
        "Setting a rectangle on a non-empty CurrentDomain object is not "
        "allowed.");
  }
  ndrectangle_ = ndr;
  type_ = CurrentDomainType::NDRECTANGLE;
  empty_ = false;
}

shared_ptr<const NDRectangle> CurrentDomain::ndrectangle() const {
  if (empty_ || type_ != CurrentDomainType::NDRECTANGLE) {
    throw std::logic_error(
        "It's not possible to get the ndrectangle from this current_domain if "
        "one isn't "
        "set.");
  }

  return ndrectangle_;
}

bool CurrentDomain::covered(
    shared_ptr<const CurrentDomain> expanded_current_domain) const {
  return covered(expanded_current_domain->ndrectangle()->get_ndranges());
}

bool CurrentDomain::covered(const NDRange& ndranges) const {
  switch (type_) {
    case CurrentDomainType::NDRECTANGLE: {
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
          "Unable to execute this current_domain operation because one of the "
          "current_domains " +
          std::string("passed has an unsupported") + "type " +
          current_domain_type_str(type_));
    }
  }
}

void CurrentDomain::check_schema_sanity(const ArraySchema& schema) const {
  switch (type_) {
    case CurrentDomainType::NDRECTANGLE: {
      auto& ndranges = ndrectangle_->get_ndranges();

      // Dim nums match
      if (schema.dim_num() != ndranges.size()) {
        throw std::logic_error(
            "The array current_domain and the array schema have a non-equal "
            "number of dimensions");
      }

      // Bounds are set for all dimensions
      for (uint32_t i = 0; i < ndranges.size(); ++i) {
        if (ndranges[i].empty()) {
          throw std::logic_error(
              "This current_domain has no range specified for dimension idx: " +
              std::to_string(i));
        }
      }

      // Nothing is out of bounds
      if (!this->covered(schema.domain().domain())) {
        throw std::logic_error(
            "This array current_domain has ranges past the boundaries of the "
            "array "
            "schema domain");
      }

      break;
    }
    default: {
      throw std::runtime_error(
          "You used a CurrentDomain object which has " +
          std::string("an unsupported") +
          "type: " + current_domain_type_str(type_));
    }
  }
}

}  // namespace tiledb::sm
