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
 */

#include <iostream>
#include <sstream>

#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/random/random_label.h"

#include "ndrectangle.h"

namespace tiledb::sm {

NDRectangle::NDRectangle(
    shared_ptr<MemoryTracker> memory_tracker,
    shared_ptr<Domain> dom,
    const NDRange& nd)
    : memory_tracker_(memory_tracker)
    , domain_(dom)
    , range_data_(nd) {
}

NDRectangle::NDRectangle(
    shared_ptr<MemoryTracker> memory_tracker, shared_ptr<Domain> dom)
    : memory_tracker_(memory_tracker)
    , domain_(dom) {
  range_data.resize(dom->dim_num());
}

shared_ptr<const NDRectangle> NDRectangle::deserialize(
    Deserializer& deserializer,
    shared_ptr<MemoryTracker> memory_tracker,
    shared_ptr<Domain> domain) {
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

  return make_shared<NDRectangle>(memory_tracker, domain, nd);
}

void NDRectangle::serialize(Serializer& serializer) const {
  for (unsigned d = 0; d < range_data_.size(); ++d) {
    auto dim{domain_->dimension_ptr(d)};
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
  for (dimension_size_type i = 0; i < range_data_.size(); ++i) {
    auto dtype = domain_->dimension_ptr(i)->type();
    ss << "  - " << range_str(range_data_[i], dtype) << std::endl;
  }

  fprintf(out, "%s", ss.str().c_str());
}

void NDRectangle::set_range(const Range& r, dimension_size_type idx) {
  if (idx >= range_data_.size()) {
    throw std::logic_error(
        "You are trying to set a range for an index out of bounds");
  }
  range_data_[idx] = r;
}

void set_range_for_name(const Range& r, const std::string& name) {
  auto idx = domain_->get_dimension_index(name);
  set_range(r, idx);
}

const Range& get_range(dimension_size_type idx) const {
  if (idx >= range_data_.size()) {
    throw std::logic_error(
        "You are trying to get a range for an index out of bounds");
  }
  return range_data_[idx];
}

const Range& get_range_for_name(const std::string& name) const {
  auto idx = domain_->get_dimension_index(name);
  return get_range(idx);
}

}  // namespace tiledb::sm
