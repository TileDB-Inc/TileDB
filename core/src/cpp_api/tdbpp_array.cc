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
#include "tdbpp_array.h"


void tdb::ArrayConfig::_init(tiledb_array_metadata_t *meta) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_get_array_type(ctx, meta, &type));
  ctx.handle_error(tiledb_array_metadata_get_capacity(ctx, meta, &capacity));
  ctx.handle_error(tiledb_array_metadata_get_tile_order(ctx, meta, &tile_layout));
  ctx.handle_error(tiledb_array_metadata_get_cell_order(ctx, meta, &cell_layout));
  ctx.handle_error(tiledb_array_metadata_get_coords_compressor(ctx, meta, &coords.compressor, &coords.level));

  tiledb_domain_t *domain;
  ctx.handle_error(tiledb_array_metadata_get_domain(ctx, meta, &domain));
  this->domain = Domain(ctx, domain);
  const char *name;
  tiledb_array_metadata_get_array_name(ctx, meta, &name);
  uri = std::string(name);
}

void tdb::ArrayConfig::_init(const std::string &uri) {
  tiledb_array_metadata_t *meta;
  _ctx.get().handle_error(tiledb_array_metadata_load(_ctx.get(), &meta, uri.c_str()));
  _init(meta);
}

std::ostream &operator<<(std::ostream &os, const tdb::Array &array) {
  os << array.uri() << "\tARRAY";
  return os;
}

std::ostream &operator<<(std::ostream &os, const tdb::ArrayConfig &config) {
  os << config.to_str();
  return os;
}