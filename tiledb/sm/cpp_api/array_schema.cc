/**
 * @file  tiledb_cpp_api_array_schema.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines the C++ API for the TileDB ArraySchema object.
 */

#include "array_schema.h"

#include <array>
#include <sstream>

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

ArraySchema::ArraySchema(const Context& ctx, tiledb_array_type_t type)
    : Schema(ctx) {
  tiledb_array_schema_t* schema;
  ctx.handle_error(tiledb_array_schema_create(ctx, &schema, type));
  schema_ = std::shared_ptr<tiledb_array_schema_t>(schema, deleter_);
};

ArraySchema::ArraySchema(const Context& ctx, const std::string& uri)
    : Schema(ctx) {
  tiledb_array_schema_t* schema;
  ctx.handle_error(tiledb_array_schema_load(ctx, &schema, uri.c_str()));
  schema_ = std::shared_ptr<tiledb_array_schema_t>(schema, deleter_);
};

/* ********************************* */
/*                API                */
/* ********************************* */

void ArraySchema::dump(FILE* out) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_array_schema_dump(ctx, schema_.get(), out));
}

tiledb_array_type_t ArraySchema::array_type() const {
  auto& ctx = ctx_.get();
  tiledb_array_type_t type;
  ctx.handle_error(
      tiledb_array_schema_get_array_type(ctx, schema_.get(), &type));
  return type;
}

Compressor ArraySchema::coords_compressor() const {
  auto& ctx = ctx_.get();
  tiledb_compressor_t compressor;
  int level;
  ctx.handle_error(tiledb_array_schema_get_coords_compressor(
      ctx, schema_.get(), &compressor, &level));
  Compressor cmp(compressor, level);
  return cmp;
}

ArraySchema& ArraySchema::set_coords_compressor(const Compressor& c) {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_array_schema_set_coords_compressor(
      ctx, schema_.get(), c.compressor(), c.level()));
  return *this;
}

Compressor ArraySchema::offsets_compressor() const {
  auto& ctx = ctx_.get();
  tiledb_compressor_t compressor;
  int level;
  ctx.handle_error(tiledb_array_schema_get_offsets_compressor(
      ctx, schema_.get(), &compressor, &level));
  Compressor cmp(compressor, level);
  return cmp;
}

ArraySchema& ArraySchema::set_offsets_compressor(const Compressor& c) {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_array_schema_set_offsets_compressor(
      ctx, schema_.get(), c.compressor(), c.level()));
  return *this;
}

Domain ArraySchema::domain() const {
  auto& ctx = ctx_.get();
  tiledb_domain_t* domain;
  ctx.handle_error(tiledb_array_schema_get_domain(ctx, schema_.get(), &domain));
  return Domain(ctx, domain);
}

ArraySchema& ArraySchema::set_domain(const Domain& domain) {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_array_schema_set_domain(ctx, schema_.get(), domain.ptr().get()));
  return *this;
}

ArraySchema& ArraySchema::add_attribute(const Attribute& attr) {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_array_schema_add_attribute(ctx, schema_.get(), attr));
  return *this;
}

std::shared_ptr<tiledb_array_schema_t> ArraySchema::ptr() const {
  return schema_;
}

ArraySchema::operator tiledb_array_schema_t*() const {
  return schema_.get();
}

void ArraySchema::check() const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_array_schema_check(ctx, schema_.get()));
}

const std::unordered_map<std::string, Attribute>
tiledb::ArraySchema::attributes() const {
  auto& ctx = ctx_.get();
  tiledb_attribute_t* attrptr;
  unsigned int nattr;
  std::unordered_map<std::string, Attribute> attrs;
  ctx.handle_error(
      tiledb_array_schema_get_attribute_num(ctx, schema_.get(), &nattr));
  for (unsigned int i = 0; i < nattr; ++i) {
    ctx.handle_error(tiledb_array_schema_get_attribute_from_index(
        ctx, schema_.get(), i, &attrptr));
    auto attr = Attribute(ctx_, attrptr);
    attrs.emplace(
        std::pair<std::string, Attribute>(attr.name(), std::move(attr)));
  }
  return attrs;
}

ArraySchema& ArraySchema::set_cell_order(tiledb_layout_t layout) {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_array_schema_set_cell_order(ctx, schema_.get(), layout));
  return *this;
}

ArraySchema& ArraySchema::set_tile_order(tiledb_layout_t layout) {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_array_schema_set_tile_order(ctx, schema_.get(), layout));
  return *this;
}

ArraySchema& ArraySchema::set_order(const std::array<tiledb_layout_t, 2>& p) {
  set_tile_order(p[0]);
  set_cell_order(p[1]);
  return *this;
}

tiledb_layout_t ArraySchema::cell_order() const {
  auto& ctx = ctx_.get();
  tiledb_layout_t layout;
  ctx.handle_error(
      tiledb_array_schema_get_cell_order(ctx, schema_.get(), &layout));
  return layout;
}

tiledb_layout_t ArraySchema::tile_order() const {
  auto& ctx = ctx_.get();
  tiledb_layout_t layout;
  ctx.handle_error(
      tiledb_array_schema_get_tile_order(ctx, schema_.get(), &layout));
  return layout;
}

uint64_t ArraySchema::capacity() const {
  auto& ctx = ctx_.get();
  uint64_t capacity;
  ctx.handle_error(
      tiledb_array_schema_get_capacity(ctx, schema_.get(), &capacity));
  return capacity;
}

ArraySchema& ArraySchema::set_capacity(uint64_t capacity) {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_array_schema_set_capacity(ctx, schema_.get(), capacity));
  return *this;
}

Attribute ArraySchema::attribute(const std::string& name) const {
  auto& ctx = ctx_.get();
  tiledb_attribute_t* attr;
  ctx.handle_error(tiledb_array_schema_get_attribute_from_name(
      ctx, schema_.get(), name.c_str(), &attr));
  return Attribute(ctx, attr);
}

unsigned ArraySchema::attribute_num() const {
  auto& ctx = ctx_.get();
  unsigned num;
  ctx.handle_error(
      tiledb_array_schema_get_attribute_num(ctx, schema_.get(), &num));
  return num;
}

Attribute ArraySchema::attribute(unsigned int i) const {
  auto& ctx = ctx_.get();
  tiledb_attribute_t* attr;
  ctx.handle_error(tiledb_array_schema_get_attribute_from_index(
      ctx, schema_.get(), i, &attr));
  return Attribute(ctx, attr);
}

/* ********************************* */
/*         STATIC FUNCTIONS          */
/* ********************************* */

std::string ArraySchema::to_str(tiledb_layout_t layout) {
  switch (layout) {
    case TILEDB_GLOBAL_ORDER:
      return "GLOBAL";
    case TILEDB_ROW_MAJOR:
      return "ROW-MAJOR";
    case TILEDB_COL_MAJOR:
      return "COL-MAJOR";
    case TILEDB_UNORDERED:
      return "UNORDERED";
  }
  return "";
}

std::string ArraySchema::to_str(tiledb_array_type_t type) {
  return type == TILEDB_DENSE ? "DENSE" : "SPARSE";
}

/* ********************************* */
/*               MISC                */
/* ********************************* */

std::ostream& operator<<(std::ostream& os, const ArraySchema& schema) {
  os << "ArraySchema<";
  os << tiledb::ArraySchema::to_str(schema.array_type());
  os << ' ' << schema.domain();
  for (const auto& a : schema.attributes()) {
    os << ' ' << a.second;
  }
  os << '>';
  return os;
}

}  // namespace tiledb
