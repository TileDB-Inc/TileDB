/**
 * @file   hyperspace.cc
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
 * This file implements class Hyperspace.
 */

#include "hyperspace.h"

#include <iostream>

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Hyperspace::Hyperspace() {
  dim_num_ = 0;
  type_ = Datatype::INT32;
}

Hyperspace::Hyperspace(Datatype type)
    : type_(type) {
  dim_num_ = 0;
}

Hyperspace::Hyperspace(const Hyperspace* hyperspace) {
  dim_num_ = hyperspace->dim_num_;
  type_ = hyperspace->type_;

  for (auto dim : hyperspace->dimensions_)
    dimensions_.emplace_back(new Dimension(dim));
}

Hyperspace::~Hyperspace() {
  for (auto dim : dimensions_)
    delete dim;
}

/* ********************************* */
/*                API                */
/* ********************************* */

Status Hyperspace::add_dimension(
    const char* name, const void* domain, const void* tile_extent) {
  auto dim = new Dimension(name, type_);
  RETURN_NOT_OK_ELSE(dim->set_domain(domain), delete dim);
  RETURN_NOT_OK_ELSE(dim->set_tile_extent(tile_extent), delete dim);

  dimensions_.emplace_back(dim);
  ++dim_num_;

  return Status::Ok();
}

// ===== FORMAT =====
// type (char)
// dim_num (unsigned int)
// dimension #1
// dimension #2
// ...
Status Hyperspace::deserialize(ConstBuffer* buff) {
  // Load type
  char type;
  RETURN_NOT_OK(buff->read(&type, sizeof(char)));
  type_ = static_cast<Datatype>(type);

  // Load dimensions
  RETURN_NOT_OK(buff->read(&dim_num_, sizeof(unsigned int)));
  for (unsigned int i = 0; i < dim_num_; ++i) {
    auto dim = new Dimension();
    dim->deserialize(buff, type_);
    dimensions_.emplace_back(dim);
  }

  return Status::Ok();
}

unsigned int Hyperspace::dim_num() const {
  return dim_num_;
}

const void* Hyperspace::domain(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;

  return dimensions_[i]->domain();
}

const Dimension* Hyperspace::dimension(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;

  return dimensions_[i];
}

void Hyperspace::dump(FILE* out) const {
  const char* type_s = datatype_str(type_);

  fprintf(out, "=== Hyperspace ===\n");
  fprintf(out, "- Dimensions type: %s\n", type_s);

  for (auto& dim : dimensions_) {
    fprintf(out, "\n");
    dim->dump(out);
  }
}

// ===== FORMAT =====
// type (char)
// dim_num (unsigned int)
// dimension #1
// dimension #2
// ...
Status Hyperspace::serialize(Buffer* buff) {
  // Write type
  auto type = static_cast<char>(type_);
  RETURN_NOT_OK(buff->write(&type, sizeof(char)));

  // Write dimensions
  RETURN_NOT_OK(buff->write(&dim_num_, sizeof(unsigned int)));
  for (auto dim : dimensions_)
    dim->serialize(buff);

  return Status::Ok();
}

const void* Hyperspace::tile_extent(unsigned int i) const {
  if (i > dim_num_)
    return nullptr;

  return dimensions_[i]->tile_extent();
}

Datatype Hyperspace::type() const {
  return type_;
}

}  // namespace tiledb
