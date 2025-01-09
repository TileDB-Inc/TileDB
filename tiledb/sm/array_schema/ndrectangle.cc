/**
 * @file ndrectangle.cc
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
 * This file implements class ndrectangle.
 *
 * I am debugging a mysterious CI fail on
 * https://github.com/TileDB-Inc/TileDB/pull/5421
 */

#include <iostream>
#include <sstream>

#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"

#include "ndrectangle.h"

namespace tiledb::sm {

NDRectangle::NDRectangle(
    shared_ptr<MemoryTracker> memory_tracker,
    shared_ptr<Domain> dom,
    const NDRange& nd)
    : memory_tracker_(memory_tracker)
    , range_data_(nd)
    , domain_(dom) {
  if (range_data_.empty()) {
    throw std::logic_error("The passed ND ranges vector is empty.");
  }
  if (dom != nullptr &&
      dom->dim_num() != static_cast<Domain::dimension_size_type>(nd.size()))
    if (range_data_.empty()) {
      throw std::logic_error(
          "The array current domain and the array schema have a non-equal "
          "number of dimensions");
    }
}

NDRectangle::NDRectangle(
    shared_ptr<MemoryTracker> memory_tracker, shared_ptr<Domain> dom)
    : memory_tracker_(memory_tracker)
    , domain_(dom) {
  if (dom->dim_num() == 0) {
    throw std::logic_error(
        "The TileDB domain used to create the NDRectangle has no dimensions.");
  }
  range_data_.resize(dom->dim_num());
}

NDRange NDRectangle::deserialize_ndranges(
    Deserializer& deserializer, shared_ptr<Domain> domain) {
  NDRange nd;
  auto dim_num = domain->dim_num();
  nd.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim{domain->dimension_ptr(d)};
    if (!dim->var_size()) {  // Fixed-sized
      auto r_size = 2 * dim->coord_size();
      nd[d] = Range(deserializer.get_ptr<char>(r_size), r_size);
    } else {  // Var-sized
      uint64_t r_size, start_size;
      r_size = deserializer.read<uint64_t>();
      start_size = deserializer.read<uint64_t>();
      nd[d] = Range(deserializer.get_ptr<char>(r_size), r_size, start_size);
    }
  }
  return nd;
}

shared_ptr<NDRectangle> NDRectangle::deserialize(
    Deserializer& deserializer,
    shared_ptr<MemoryTracker> memory_tracker,
    shared_ptr<Domain> domain) {
  return make_shared<NDRectangle>(
      memory_tracker, domain, deserialize_ndranges(deserializer, domain));
}

void NDRectangle::serialize(Serializer& serializer) const {
  for (unsigned d = 0; d < range_data_.size(); ++d) {
    auto dim{domain()->dimension_ptr(d)};
    const auto& r = range_data_[d];
    if (!dim->var_size()) {  // Fixed-sized
      serializer.write(r.data(), r.size());
    } else {  // Var-sized
      auto r_size = r.size();
      auto r_start_size = r.start_size();
      serializer.write<uint64_t>(r_size);
      serializer.write<uint64_t>(r_start_size);
      serializer.write(r.data(), r_size);
    }
  }
}

void NDRectangle::dump(FILE* out) const {
  if (out == nullptr) {
    out = stdout;
  }

  std::stringstream ss;
  ss << " - NDRectangle ###" << std::endl;
  for (uint32_t i = 0; i < range_data_.size(); ++i) {
    auto dtype = domain()->dimension_ptr(i)->type();
    ss << "  - " << range_str(range_data_[i], dtype) << std::endl;
  }

  fprintf(out, "%s", ss.str().c_str());
}

void NDRectangle::set_domain(shared_ptr<Domain> domain) {
  if (domain->dim_num() !=
      static_cast<Domain::dimension_size_type>(range_data_.size())) {
    throw std::logic_error(
        "The array current domain and the array schema have a non-equal "
        "number of dimensions");
  }
  domain_ = domain;
}

void NDRectangle::set_range(const Range& r, uint32_t idx) {
  if (idx >= range_data_.size()) {
    throw std::logic_error(
        "Trying to set a range for an index out of bounds is not possible.");
  }
  check_range_is_valid(r, domain_->dimension_ptr(idx)->type());
  range_data_[idx] = r;
}

void NDRectangle::set_range_for_name(const Range& r, const std::string& name) {
  auto idx = domain()->get_dimension_index(name);
  set_range(r, idx);
}

const Range& NDRectangle::get_range(uint32_t idx) const {
  if (idx >= range_data_.size()) {
    throw std::logic_error(
        "Trying to get a range for an index out of bounds is not possible.");
  }
  return range_data_[idx];
}

const Range& NDRectangle::get_range_for_name(const std::string& name) const {
  auto idx = domain()->get_dimension_index(name);
  return get_range(idx);
}

Datatype NDRectangle::range_dtype(uint32_t idx) const {
  if (idx >= range_data_.size()) {
    throw std::logic_error(
        "The index does not correspond to a valid dimension in the "
        "NDRectangle");
  }
  return domain()->dimension_ptr(idx)->type();
}

Datatype NDRectangle::range_dtype_for_name(const std::string& name) const {
  auto idx = domain()->get_dimension_index(name);
  return range_dtype(idx);
}

}  // namespace tiledb::sm

std::ostream& operator<<(std::ostream& os, const tiledb::sm::NDRectangle& ndr) {
  os << " - NDRectangle ###" << std::endl;
  for (uint32_t i = 0; i < ndr.get_ndranges().size(); ++i) {
    auto dtype = ndr.domain()->dimension_ptr(i)->type();
    os << "  - " << range_str(ndr.get_range(i), dtype) << std::endl;
  }

  return os;
}
