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


void tdb::ArrayMetadata::_init(tiledb_array_metadata_t *meta) {
  _meta = meta;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_get_array_type(ctx, meta, &_type));
  ctx.handle_error(tiledb_array_metadata_get_capacity(ctx, meta, &_capacity));
  ctx.handle_error(tiledb_array_metadata_get_tile_order(ctx, meta, &_tile_layout));
  ctx.handle_error(tiledb_array_metadata_get_cell_order(ctx, meta, &_cell_layout));
  ctx.handle_error(tiledb_array_metadata_get_coords_compressor(ctx, meta, &_coords.compressor, &_coords.level));

  tiledb_domain_t *domain;
  ctx.handle_error(tiledb_array_metadata_get_domain(ctx, meta, &domain));
  this->_domain = Domain(ctx, domain);
  const char *name;
  ctx.handle_error(tiledb_array_metadata_get_array_name(ctx, meta, &name));
  _uri = std::string(name);

  tiledb_attribute_t *attrptr;
  unsigned int nattr;
  ctx.handle_error(tiledb_array_metadata_get_num_attributes(ctx, meta, &nattr));
  for (unsigned int i = 0; i < nattr; ++i) {
    ctx.handle_error(tiledb_attribute_from_index(ctx, meta, i, &attrptr));
    auto attr = Attribute(_ctx, attrptr);
    _attrs.emplace(std::pair<std::string, Attribute>(attr.name(), std::move(attr)));
  }
}

void tdb::ArrayMetadata::_init(const std::string &uri) {
  tiledb_array_metadata_t *meta;
  _ctx.get().handle_error(tiledb_array_metadata_load(_ctx.get(), &meta, uri.c_str()));
  _init(meta);
}

tdb::ArrayMetadata::~ArrayMetadata() {
  if (_meta) {
    _ctx.get().handle_error(tiledb_array_metadata_free(_ctx.get(), _meta));
  }
}

std::string tdb::ArrayMetadata::to_str() const {
  std::stringstream ss;
  ss << "ArrayMetadata<";
  ss << (_type == TILEDB_DENSE ? "DENSE" : "SPARSE");
  ss << ' ' << _domain;
  for (const auto &a : _attrs) {
    ss << ' ' << a.second;
  }
  ss << '>';
  return ss.str();
}

tdb::ArrayMetadata::ArrayMetadata(tdb::ArrayMetadata &&o) : _domain(std::move(o._domain)), _ctx(o._ctx) {
  _meta = o._meta;
  o._meta = nullptr;
  _type = o._type;
  _tile_layout = o._tile_layout;
  _cell_layout = o._cell_layout;
  _capacity = o._capacity;
  _coords = o._coords;
  _uri = o._uri;
  _attrs = std::move(o._attrs);
}

tdb::ArrayMetadata &tdb::ArrayMetadata::operator=(tdb::ArrayMetadata &&o) {
  _ctx = o._ctx;
  _meta = o._meta;
  o._meta = nullptr;
  _type = o._type;
  _tile_layout = o._tile_layout;
  _cell_layout = o._cell_layout;
  _capacity = o._capacity;
  _coords = o._coords;
  _uri = o._uri;
  _attrs = std::move(o._attrs);
  _domain = std::move(o._domain);
  return *this;
}

std::ostream &operator<<(std::ostream &os, const tdb::Array &array) {
  os << "Array<" << array.context() << ' ' << array.meta() << ">";
  return os;
}

std::ostream &operator<<(std::ostream &os, const tdb::ArrayMetadata &meta) {
  os << meta.to_str();
  return os;
}
