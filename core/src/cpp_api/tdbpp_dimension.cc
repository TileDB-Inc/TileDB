/**
 * @file   tiledb.h
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
 * This file declares the C++ API for TileDB.
 */

#include "tdbpp_dimension.h"

void tdb::Dimension::_init(tiledb_dimension_t *dim) {
  const char* name;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_dimension_get_name(ctx, dim, &name));
  _name = std::string(name);
  ctx.handle_error(tiledb_dimension_get_domain(ctx, dim, &_domain));
  ctx.handle_error(tiledb_dimension_get_tile_extent(ctx, dim, &_tile_extent));
  ctx.handle_error(tiledb_dimension_get_type(ctx, dim, &_type));
}

tdb::Dimension &tdb::Dimension::operator=(tdb::Dimension &&o) {
  _ctx = o._ctx;
  _name = std::move(o._name);
  _domain = o._domain;
  _tile_extent = o._tile_extent;
  _type = o._type;
  o._domain = nullptr;
  o._tile_extent = nullptr;
  return *this;
}

std::ostream &operator<<(std::ostream &os, const tdb::Dimension &dim) {
  os << "Dim<" << dim.name() << '>';
  return os;
}
