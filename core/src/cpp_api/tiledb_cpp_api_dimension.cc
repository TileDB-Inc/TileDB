/**
 * @file   tiledb_cpp_api_dimension.cc
 *
 * @author Ravi Gaddipati
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
 * This file defines the C++ API for the TileDB Dimension object.
 */

#include "tiledb_cpp_api_dimension.h"
#include "tiledb_cpp_api_context.h"

void tdb::Dimension::_init(tiledb_dimension_t *dim) {
  _dim = std::shared_ptr<tiledb_dimension_t>(dim, _deleter);
}

void tdb::Dimension::_create(
    const std::string &name,
    tiledb_datatype_t type,
    const void *domain,
    const void *extent) {
  auto &ctx = _ctx.get();
  tiledb_dimension_t *d;
  ctx.handle_error(tiledb_dimension_create(
      ctx.ptr(), &d, name.c_str(), type, domain, extent));
  _init(d);
}

const std::string tdb::Dimension::name() const {
  const char *name;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_dimension_get_name(ctx.ptr(), _dim.get(), &name));
  return name;
}

tiledb_datatype_t tdb::Dimension::type() const {
  tiledb_datatype_t type;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_dimension_get_type(ctx.ptr(), _dim.get(), &type));
  return type;
}

void *tdb::Dimension::_domain() const {
  auto &ctx = _ctx.get();
  void *domain;
  ctx.handle_error(tiledb_dimension_get_domain(ctx.ptr(), _dim.get(), &domain));
  return domain;
}

void *tdb::Dimension::_extent() const {
  void *extent;
  auto &ctx = _ctx.get();
  ctx.handle_error(
      tiledb_dimension_get_tile_extent(ctx.ptr(), _dim.get(), &extent));
  return extent;
}

void tdb::Dimension::load(tiledb_dimension_t **dim) {
  if (dim != nullptr && *dim != nullptr) {
    _init(*dim);
    *dim = nullptr;
  }
}

std::ostream &tdb::operator<<(std::ostream &os, const tdb::Dimension &dim) {
  os << "Dim<" << dim.name() << '>';
  return os;
}
