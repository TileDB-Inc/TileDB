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

#include "tdbpp_attribute.h"
#include "tdbpp_context.h"
#include "tdbpp_type.h"

void tdb::Attribute::_init(tiledb_attribute_t *attr) {
  _attr = std::shared_ptr<tiledb_attribute_t>(attr, _deleter);
}

std::string tdb::Attribute::name() const {
  auto &ctx = _ctx.get();
  const char *name;
  ctx.handle_error(tiledb_attribute_get_name(ctx, _attr.get(), &name));
  return name;
}

tiledb_datatype_t tdb::Attribute::type() const {
  auto &ctx = _ctx.get();
  tiledb_datatype_t type;
  ctx.handle_error(tiledb_attribute_get_type(ctx, _attr.get(), &type));
  return type;
}

unsigned tdb::Attribute::num() const {
  auto &ctx = _ctx.get();
  unsigned num;
  ctx.handle_error(tiledb_attribute_get_cell_val_num(ctx, _attr.get(), &num));
  return num;
}

tdb::Attribute &tdb::Attribute::set_num(unsigned num) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_attribute_set_cell_val_num(ctx, _attr.get(), num));
  return *this;
}

tdb::Compressor tdb::Attribute::compressor() const {
  auto &ctx = _ctx.get();
  Compressor cmp;
  ctx.handle_error(tiledb_attribute_get_compressor(
      ctx, _attr.get(), &(cmp.compressor), &(cmp.level)));
  return cmp;
}

tdb::Attribute &tdb::Attribute::set_compressor(tdb::Compressor c) {
  auto &ctx = _ctx.get();
  ctx.handle_error(
      tiledb_attribute_set_compressor(ctx, _attr.get(), c.compressor, c.level));
  return *this;
}

void tdb::Attribute::_create(const std::string &name, tiledb_datatype_t type) {
  auto &ctx = _ctx.get();
  tiledb_attribute_t *attr;
  ctx.handle_error(tiledb_attribute_create(ctx, &attr, name.c_str(), type));
  _init(attr);
}

std::ostream &tdb::operator<<(std::ostream &os, const Attribute &a) {
  os << "Attr<" << a.name() << ',' << tdb::type::from_tiledb(a.type()) << ','
     << (a.num() == TILEDB_VAR_NUM ? "VAR" : std::to_string(a.num())) << '>';
  return os;
}

tdb::Attribute &tdb::operator<<(Attribute &attr, const Compressor &c) {
  attr.set_compressor(c);
  return attr;
}

tdb::Attribute &tdb::operator<<(Attribute &attr, unsigned num) {
  attr.set_num(num);
  return attr;
}