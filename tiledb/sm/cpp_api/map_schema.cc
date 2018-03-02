/**
 * @file  tiledb_cpp_api_map_schema.cc
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
 * This file declares the C++ API for the TileDB MapSchema object.
 */

#include "map_schema.h"

tiledb::MapSchema::MapSchema(const tiledb::Context& ctx)
    : Schema(ctx) {
  tiledb_kv_schema_t* schema;
  ctx.handle_error(tiledb_kv_schema_create(ctx, &schema));
  schema_ = std::shared_ptr<tiledb_kv_schema_t>(schema, deleter_);
}

tiledb::MapSchema::MapSchema(const tiledb::Context& ctx, const std::string& uri)
    : Schema(ctx) {
  tiledb_kv_schema_t* schema;
  ctx.handle_error(tiledb_kv_schema_load(ctx, &schema, uri.c_str()));
  schema_ = std::shared_ptr<tiledb_kv_schema_t>(schema, deleter_);
}

void tiledb::MapSchema::dump(FILE* out) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_kv_schema_dump(ctx, schema_.get(), out));
}

tiledb::MapSchema& tiledb::MapSchema::add_attribute(
    const tiledb::Attribute& attr) {
  auto& ctx = ctx_.get();
  ctx.handle_error(
      tiledb_kv_schema_add_attribute(ctx, schema_.get(), attr.ptr().get()));
  return *this;
}

void tiledb::MapSchema::check() const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_kv_schema_check(ctx, schema_.get()));
}

const std::unordered_map<std::string, tiledb::Attribute>
tiledb::MapSchema::attributes() const {
  auto& ctx = ctx_.get();
  tiledb_attribute_t* attrptr;
  unsigned int nattr;
  std::unordered_map<std::string, Attribute> attrs;
  ctx.handle_error(
      tiledb_kv_schema_get_attribute_num(ctx, schema_.get(), &nattr));
  for (unsigned int i = 0; i < nattr; ++i) {
    ctx.handle_error(tiledb_kv_schema_get_attribute_from_index(
        ctx, schema_.get(), i, &attrptr));
    auto attr = Attribute(ctx_, attrptr);
    attrs.emplace(
        std::pair<std::string, Attribute>(attr.name(), std::move(attr)));
  }
  return attrs;
}

tiledb::Attribute tiledb::MapSchema::attribute(unsigned int i) const {
  auto& ctx = ctx_.get();
  tiledb_attribute_t* attr;
  ctx.handle_error(
      tiledb_kv_schema_get_attribute_from_index(ctx, schema_.get(), i, &attr));
  return {ctx, attr};
}

tiledb::Attribute tiledb::MapSchema::attribute(const std::string& name) const {
  auto& ctx = ctx_.get();
  tiledb_attribute_t* attr;
  ctx.handle_error(tiledb_kv_schema_get_attribute_from_name(
      ctx, schema_.get(), name.c_str(), &attr));
  return {ctx, attr};
}

unsigned tiledb::MapSchema::attribute_num() const {
  auto& ctx = context();
  unsigned num;
  ctx.handle_error(
      tiledb_kv_schema_get_attribute_num(ctx, schema_.get(), &num));
  return num;
}

std::ostream& ::tiledb::operator<<(
    std::ostream& os, const tiledb::MapSchema& schema) {
  os << "MapSchema<Attributes:";
  for (const auto& a : schema.attributes()) {
    os << ' ' << a.second;
  }
  os << '>';
  return os;
}
