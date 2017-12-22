/**
 * @file  tdbpp_arraymeta.cc
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

#include "tdbpp_arraymeta.h"
#include "tdbpp_context.h"
#include "tdbpp_domain.h"
#include "tdbpp_attribute.h"

#include <sstream>

void tdb::ArrayMetadata::_init(tiledb_array_metadata_t *meta) {
  _meta = std::shared_ptr<tiledb_array_metadata_t>(meta, _deleter);
}

void tdb::ArrayMetadata::_init(const std::string &uri) {
  tiledb_array_metadata_t *meta;
  _ctx.get().handle_error(tiledb_array_metadata_load(_ctx.get(), &meta, uri.c_str()));
  _init(meta);
}

std::string tdb::ArrayMetadata::to_str() const {
  std::stringstream ss;
  ss << "ArrayMetadata<";
  ss << from_tiledb(type());
  ss << ' ' << domain();
  for (const auto &a : attributes()) {
    ss << ' ' << a.second;
  }
  ss << '>';
  return ss.str();
}

tiledb_array_type_t tdb::ArrayMetadata::type() const {
  auto &ctx = _ctx.get();
  tiledb_array_type_t type;
  ctx.handle_error(tiledb_array_metadata_get_array_type(ctx, _meta.get(), &type));
  return type;
}

tdb::Compressor tdb::ArrayMetadata::coord_compressor() const {
  auto &ctx = _ctx.get();
  Compressor cmp;
  ctx.handle_error(tiledb_array_metadata_get_coords_compressor(ctx, _meta.get(), &cmp.compressor, &cmp.level));
  return std::move(cmp);
}

tdb::ArrayMetadata &tdb::ArrayMetadata::set_coord_compressor(const tdb::Compressor c) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_set_coords_compressor(ctx, _meta.get(), c.compressor, c.level));
  return *this;
}

tdb::Compressor tdb::ArrayMetadata::offset_compressor() const {
  auto &ctx = _ctx.get();
  Compressor c;
  ctx.handle_error(tiledb_array_metadata_get_offsets_compressor(ctx, _meta.get(), &c.compressor, &c.level));
  return c;
}

tdb::ArrayMetadata &tdb::ArrayMetadata::set_offset_compressor(const tdb::Compressor c) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_set_offsets_compressor(ctx, _meta.get(), c.compressor, c.level));
  return *this;
}

std::string tdb::ArrayMetadata::name() const {
  auto &ctx = _ctx.get();
  const char *n;
  ctx.handle_error(tiledb_array_metadata_get_array_name(ctx, _meta.get(), &n));
  return n;
}

tdb::Domain tdb::ArrayMetadata::domain() const {
  auto &ctx = _ctx.get();
  tiledb_domain_t *domain;
  ctx.handle_error(tiledb_array_metadata_get_domain(ctx, _meta.get(), &domain));
  return Domain(ctx, &domain);
}

tdb::ArrayMetadata &tdb::ArrayMetadata::set_domain(const Domain &domain) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_set_domain(ctx, _meta.get(), domain.ptr().get()));
  return *this;
}
tdb::ArrayMetadata &tdb::ArrayMetadata::add_attribute(const tdb::Attribute &attr) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_add_attribute(ctx, _meta.get(), attr.ptr().get()));
  return *this;
}

void tdb::ArrayMetadata::check() const {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_check(ctx, _meta.get()));
}

const std::unordered_map<std::string, tdb::Attribute> tdb::ArrayMetadata::attributes() const {
  auto &ctx = _ctx.get();
  tiledb_attribute_t *attrptr;
  unsigned int nattr;
  std::unordered_map<std::string, Attribute> attrs;
  ctx.handle_error(tiledb_array_metadata_get_num_attributes(ctx, _meta.get(), &nattr));
  for (unsigned int i = 0; i < nattr; ++i) {
    ctx.handle_error(tiledb_attribute_from_index(ctx, _meta.get(), i, &attrptr));
    auto attr = Attribute(_ctx, &attrptr);
    attrs.emplace(std::pair<std::string, Attribute>(attr.name(), std::move(attr)));
  }
  return std::move(attrs);
}

tdb::ArrayMetadata &tdb::ArrayMetadata::create(const std::string &uri) {
  auto &ctx = _ctx.get();
  tiledb_array_metadata_t *meta;
  ctx.handle_error(tiledb_array_metadata_create(ctx, &meta, uri.c_str()));
  _meta = std::shared_ptr<tiledb_array_metadata_t>(meta, _deleter);
  return *this;
}

tdb::ArrayMetadata &tdb::ArrayMetadata::set_cell_order(tiledb_layout_t layout) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_set_cell_order(ctx, _meta.get(), layout));
  return *this;
}

tdb::ArrayMetadata &tdb::ArrayMetadata::set_tile_order(tiledb_layout_t layout) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_set_tile_order(ctx, _meta.get(), layout));
  return *this;
}

tiledb_layout_t tdb::ArrayMetadata::cell_order() const {
  auto &ctx = _ctx.get();
  tiledb_layout_t layout;
  ctx.handle_error(tiledb_array_metadata_get_cell_order(ctx, _meta.get(), &layout));
  return layout;
}

tiledb_layout_t tdb::ArrayMetadata::tile_order() const {
  auto &ctx = _ctx.get();
  tiledb_layout_t layout;
  ctx.handle_error(tiledb_array_metadata_get_tile_order(ctx, _meta.get(), &layout));
  return layout;
}

uint64_t tdb::ArrayMetadata::capacity() const {
  auto &ctx = _ctx.get();
  uint64_t capacity;
  ctx.handle_error(tiledb_array_metadata_get_capacity(ctx, _meta.get(), &capacity));
  return capacity;
}

tdb::ArrayMetadata &tdb::ArrayMetadata::set_capacity(uint64_t capacity) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_set_capacity(ctx, _meta.get(), capacity));
  return *this;
}

tdb::ArrayMetadata &tdb::ArrayMetadata::set_kv() {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_array_metadata_set_as_kv(ctx, _meta.get()));
  return *this;
}

bool tdb::ArrayMetadata::is_kv() const {
  auto &ctx = _ctx.get();
  int kv;
  ctx.handle_error(tiledb_array_metadata_get_as_kv(ctx, _meta.get(), &kv));
  return kv != 0;
}

std::shared_ptr<tiledb_array_metadata_t> tdb::ArrayMetadata::ptr() const {
  return _meta;
}

bool tdb::ArrayMetadata::good() const {
  return _meta == nullptr;
}

std::reference_wrapper<tdb::Context> tdb::ArrayMetadata::context() {
  return _ctx;
}

void tdb::ArrayMetadata::_Deleter::operator()(tiledb_array_metadata_t *p) {
  _ctx.get().handle_error(tiledb_array_metadata_free(_ctx.get(), p));
}

std::ostream &operator<<(std::ostream &os, const tdb::ArrayMetadata &meta) {
  os << meta.to_str();
  return os;
}

tdb::ArrayMetadata &operator<<(tdb::ArrayMetadata &meta, const tdb::Domain &d) {
  meta.set_domain(d);
  return meta;
}

tdb::ArrayMetadata &operator<<(tdb::ArrayMetadata &meta, const tdb::Attribute &a) {
  meta.add_attribute(a);
  return meta;
}

tdb::ArrayMetadata &operator<<(tdb::ArrayMetadata &meta, const tiledb_array_type_t type) {
  meta.set_type(type);
  return meta;
}