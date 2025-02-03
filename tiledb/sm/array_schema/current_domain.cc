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
    shared_ptr<NDRectangle> ndr)
    : CurrentDomain(memory_tracker, version) {
  set_ndrectangle(ndr);
}

shared_ptr<CurrentDomain> CurrentDomain::deserialize(
    Deserializer& deserializer,
    shared_ptr<MemoryTracker> memory_tracker,
    shared_ptr<Domain> domain) {
  auto disk_version = deserializer.read<uint32_t>();
  if (disk_version > constants::current_domain_version) {
    throw std::runtime_error(
        "Invalid current domain API version on disk. '" +
        std::to_string(disk_version) +
        "' is newer than the current library current domain version '" +
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
          "array current domain type on disk.");
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
          "The current domain to serialize has an unsupported type " +
          current_domain_type_str(type_));
    }
  }
}

void CurrentDomain::set_ndrectangle(std::shared_ptr<NDRectangle> ndr) {
  if (!empty_) {
    throw std::logic_error(
        "Setting a rectangle on a non-empty CurrentDomain object is not "
        "allowed.");
  }
  ndrectangle_ = ndr;
  type_ = CurrentDomainType::NDRECTANGLE;
  empty_ = false;
}

shared_ptr<NDRectangle> CurrentDomain::ndrectangle() const {
  if (empty_ || type_ != CurrentDomainType::NDRECTANGLE) {
    throw std::logic_error(
        "It's not possible to get the ndrectangle from this current domain if "
        "one isn't set.");
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
          "Unable to execute this current domain operation because one of the "
          "current domains passed has an unsupported type " +
          current_domain_type_str(type_));
    }
  }
}

bool CurrentDomain::includes(const NDRange& non_empty_domain) const {
  for (dimension_size_type d = 0; d < non_empty_domain.size(); ++d) {
    if (!includes(d, non_empty_domain[d])) {
      return false;
    }
  }

  return true;
}

bool CurrentDomain::includes(
    const dimension_size_type d, const Range& range) const {
  switch (type_) {
    case CurrentDomainType::NDRECTANGLE: {
      auto dim = ndrectangle_->domain()->dimension_ptr(d);
      if (!dim->covered(range, ndrectangle_->get_range(d))) {
        return false;
      }
      return true;
    }
    default: {
      throw std::runtime_error(
          "Unable to execute this current domain operation because one of "
          "the "
          "current domains passed has an unsupported type " +
          current_domain_type_str(type_));
    }
  }
}

void CurrentDomain::check_schema_sanity(
    shared_ptr<Domain> schema_domain) const {
  switch (type_) {
    case CurrentDomainType::NDRECTANGLE: {
      auto& ndranges = ndrectangle_->get_ndranges();

      // Dim nums match
      if (schema_domain->dim_num() != ndranges.size()) {
        throw std::logic_error(
            "The array current domain and the array schema have a non-equal "
            "number of dimensions");
      }

      // Bounds are set for all dimensions
      for (uint32_t i = 0; i < ndranges.size(); ++i) {
        if (ndranges[i].empty()) {
          throw std::logic_error(
              "This current domain has no range specified for dimension idx: " +
              std::to_string(i));
        }
      }

      // Nothing is out of bounds
      if (!this->covered(schema_domain->domain())) {
        throw std::logic_error(
            "This array current domain has ranges past the boundaries of the "
            "array schema domain");
      }

      break;
    }
    default: {
      throw std::runtime_error(
          "The CurrentDomain object has " + std::string("an unsupported") +
          "type: " + current_domain_type_str(type_));
    }
  }
}

/*
 * This is a templatized auxiliary for expand_to_tiles,
 * dispatched on the (necessarily integral) type of a given domain slot.
 */
template <typename T>
void expand_to_tiles_aux(
    CurrentDomain::dimension_size_type dimidx,
    const Dimension* dimptr,
    std::shared_ptr<NDRectangle> cur_dom_ndrect,
    NDRange& query_ndrange) {
  // No extents for string dims, etc.
  if constexpr (!std::is_integral_v<T>) {
    return;
  }

  // Find the initial lo/hi for the query range on this dimension.
  auto query_slot = query_ndrange[dimidx];
  auto query_slot_range = (const T*)query_slot.data();

  // Find the lo/hi for the current domain on this dimension.
  auto cur_dom_slot_range = (const T*)cur_dom_ndrect->get_range(dimidx).data();

  // Find the lo/hi for the core domain (max domain) on this dimension.
  auto dim_dom = (const T*)dimptr->domain().data();

  // Find the tile extent.
  auto tile_extent = *(const T*)dimptr->tile_extent().data();

  // Compute tile indices: e.g. if the extent is 512 and the query lo is
  // 1027, that's tile 2.
  T domain_low = dim_dom[0];
  auto tile_idx0 =
      Dimension::tile_idx(query_slot_range[0], domain_low, tile_extent);
  auto tile_idx1 =
      Dimension::tile_idx(query_slot_range[1], domain_low, tile_extent);

  // Round up to a multiple of the tile coords. E.g. if the query range
  // starts out as (3,4) but the tile extent is 512, that will become (0,511).
  T result[2];
  result[0] = Dimension::tile_coord_low(tile_idx0, domain_low, tile_extent);
  result[1] = Dimension::tile_coord_high(tile_idx1, domain_low, tile_extent);

  /*
   * Since there is a current domain, though (we assume our caller checks this),
   * rounding up to a multiple of the tile extent will lead to an out-of-bounds
   * read. Make the query range lo no smaller than current domain lo on this
   * dimension, and make the query range hi no larger than current domain hi on
   * this dimension.
   */
  result[0] = std::max(result[0], cur_dom_slot_range[0]);
  result[1] = std::min(result[1], cur_dom_slot_range[1]);

  // Update the query range
  query_slot.set_range(result, sizeof(result));
}

/* The job here is, for a given domain slot:
 * Given query ranges (nominally, for dense consolidation)
 * Given the core current domain (which may be empty)
 * Given the core (max) domain
 * Given initial query bounds
 * If the current domain is empty:
 * o round the query to tile boundaries
 * Else:
 * o round the query to tile boundaries, but not outside the current domain
 *
 * Example on one dim slot:
 * - Say non-empty domain is (3,4)
 * - Say tile extent is 512
 * - Say domain is (0,99999)
 * - If current domain is empty: send (3,4) to (0,511)
 * - If current domain is (2, 63): send (3,4) to (2,63)
 */
void CurrentDomain::expand_to_tiles(
    const Domain& domain, NDRange& query_ndrange) const {
  if (query_ndrange.empty()) {
    throw std::invalid_argument("Query range is empty");
  }

  if (this->empty()) {
    domain.expand_to_tiles_when_no_current_domain(query_ndrange);
    return;
  }

  if (this->type() != CurrentDomainType::NDRECTANGLE) {
    return;
  }

  auto cur_dom_ndrect = this->ndrectangle();

  if (query_ndrange.size() != domain.dim_num()) {
    throw std::invalid_argument(
        "Query range size does not match domain dimension size");
  }

  for (CurrentDomain::dimension_size_type dimidx = 0; dimidx < domain.dim_num();
       dimidx++) {
    const auto dimptr = domain.dimension_ptr(dimidx);

    if (dimptr->var_size()) {
      continue;
    }

    if (!dimptr->tile_extent()) {
      continue;
    }

    switch (dimptr->type()) {
      case Datatype::INT64:
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
        expand_to_tiles_aux<int64_t>(
            dimidx, dimptr, cur_dom_ndrect, query_ndrange);
        break;
      case Datatype::UINT64:
        expand_to_tiles_aux<uint64_t>(
            dimidx, dimptr, cur_dom_ndrect, query_ndrange);
        break;
      case Datatype::INT32:
        expand_to_tiles_aux<int32_t>(
            dimidx, dimptr, cur_dom_ndrect, query_ndrange);
        break;
      case Datatype::UINT32:
        expand_to_tiles_aux<uint32_t>(
            dimidx, dimptr, cur_dom_ndrect, query_ndrange);
        break;
      case Datatype::INT16:
        expand_to_tiles_aux<int16_t>(
            dimidx, dimptr, cur_dom_ndrect, query_ndrange);
        break;
      case Datatype::UINT16:
        expand_to_tiles_aux<uint16_t>(
            dimidx, dimptr, cur_dom_ndrect, query_ndrange);
        break;
      case Datatype::INT8:
        expand_to_tiles_aux<int8_t>(
            dimidx, dimptr, cur_dom_ndrect, query_ndrange);
        break;
      case Datatype::UINT8:
        expand_to_tiles_aux<uint8_t>(
            dimidx, dimptr, cur_dom_ndrect, query_ndrange);
        break;
      default:
        break;
    }
  }
}

std::string CurrentDomain::as_string() const {
  if (type_ == CurrentDomainType::NDRECTANGLE) {
    return ndrectangle_->as_string();
  } else {
    // As of 2025-01-09 there is no other such type. When/if we do make such a
    // type, we'd need to configure it to return a description of itself.
    throw std::runtime_error(
        "CurrentDomain::as_string of non-NDRectangle type is not implemented");
  }
}

}  // namespace tiledb::sm

std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::CurrentDomain& current_domain) {
  os << "### Current domain ###" << std::endl;
  os << "- Version: " << current_domain.version() << std::endl;
  os << "- Empty: " << current_domain.empty() << std::endl;
  if (current_domain.empty())
    return os;

  os << "- Type: " << tiledb::sm::current_domain_type_str(current_domain.type())
     << std::endl;

  switch (current_domain.type()) {
    case tiledb::sm::CurrentDomainType::NDRECTANGLE: {
      os << current_domain.ndrectangle();
      break;
    }
    default: {
      throw std::runtime_error(
          "The current domain to dump as string has an unsupported type " +
          tiledb::sm::current_domain_type_str(current_domain.type()));
    }
  }

  return os;
}
