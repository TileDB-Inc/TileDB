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
 * This file declares serialization functions for CurrentDomain.
 */

#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/serialization/current_domain.h"
#include "tiledb/sm/serialization/query.h"

namespace tiledb::sm::serialization {

#ifdef TILEDB_SERIALIZATION

void current_domain_to_capnp(
    shared_ptr<CurrentDomain> crd,
    capnp::CurrentDomain::Builder& current_domain_builder) {
  current_domain_builder.setVersion(crd->version());
  if (crd->empty()) {
    current_domain_builder.setEmptyCurrentDomain();
    return;
  }

  switch (crd->type()) {
    case CurrentDomainType::NDRECTANGLE: {
      current_domain_builder.setType(current_domain_type_str(crd->type()));
      auto ndr_builder = current_domain_builder.initNdRectangle();
      ndrectangle_to_capnp(crd->ndrectangle(), ndr_builder);
      break;
    }
    default: {
      throw std::runtime_error(
          "The current domain to serialize has an unsupported type " +
          current_domain_type_str(crd->type()));
    }
  }
}

void ndrectangle_to_capnp(
    shared_ptr<NDRectangle> ndr,
    capnp::NDRectangle::Builder& ndrectangle_builder) {
  auto& ranges = ndr->get_ndranges();
  if (!ranges.empty()) {
    auto ranges_builder = ndrectangle_builder.initNdranges(ranges.size());
    for (uint32_t i = 0; i < ranges.size(); ++i) {
      auto range_builder = ranges_builder[i];
      auto dtype = ndr->domain()->dimension_ptr(i)->type();
      range_builder.setType(datatype_str(dtype));

      // This function takes a list of ranges per dimension, a NDRectangle has
      // only one range per dimension
      range_buffers_to_capnp({ranges[i]}, range_builder);
    }
    return;
  }

  throw std::logic_error(
      "NDRectangle serialization failed. The NDRectangle on the array current "
      "domain has no ranges set");
}

shared_ptr<CurrentDomain> current_domain_from_capnp(
    const capnp::CurrentDomain::Reader& current_domain_reader,
    shared_ptr<Domain> domain,
    shared_ptr<MemoryTracker> memory_tracker) {
  auto version = current_domain_reader.getVersion();
  if (current_domain_reader.isEmptyCurrentDomain()) {
    return make_shared<CurrentDomain>(HERE(), memory_tracker, version);
  }

  auto type = current_domain_type_enum(current_domain_reader.getType().cStr());
  switch (type) {
    case CurrentDomainType::NDRECTANGLE: {
      if (!current_domain_reader.isNdRectangle()) {
        throw std::runtime_error(
            "The current domain to deserialize has an unexpected type field "
            "given the union type");
      }
      auto ndr = ndrectangle_from_capnp(
          current_domain_reader.getNdRectangle(), domain, memory_tracker);
      return make_shared<CurrentDomain>(HERE(), memory_tracker, version, ndr);
    }
    default: {
      throw std::runtime_error(
          "The current domain to serialize has an unsupported type " +
          current_domain_type_str(type));
    }
  }
}

shared_ptr<NDRectangle> ndrectangle_from_capnp(
    const capnp::NDRectangle::Reader& ndrectangle_reader,
    shared_ptr<Domain> domain,
    shared_ptr<MemoryTracker> memory_tracker) {
  auto ranges_reader = ndrectangle_reader.getNdranges();
  uint32_t dim_num = ranges_reader.size();

  NDRange ndranges;
  for (uint32_t i = 0; i < dim_num; ++i) {
    auto range_reader = ranges_reader[i];
    auto ranges = range_buffers_from_capnp(range_reader);
    if (ranges.size() != 1) {
      throw std::runtime_error(
          "There are un unexpected number of ranges per dimension in the capnp "
          "message");
    }
    ndranges.push_back(ranges[0]);
  }
  return make_shared<NDRectangle>(HERE(), memory_tracker, domain, ndranges);
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm::serialization
