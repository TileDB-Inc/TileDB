/**
 * @file   tiledb_cpp_api_attribute.cc
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
 * This file defines the C++ API for the TileDB Attribute object.
 */

#include "attribute.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Attribute::Attribute(const Context& ctx, tiledb_attribute_t* attr)
    : ctx_(ctx)
    , deleter_(ctx) {
  attr_ = std::shared_ptr<tiledb_attribute_t>(attr, deleter_);
}

/* ********************************* */
/*                API                */
/* ********************************* */

std::string Attribute::name() const {
  auto& ctx = ctx_.get();
  const char* name;
  ctx.handle_error(tiledb_attribute_get_name(ctx, attr_.get(), &name));
  return name;
}

tiledb_datatype_t Attribute::type() const {
  auto& ctx = ctx_.get();
  tiledb_datatype_t type;
  ctx.handle_error(tiledb_attribute_get_type(ctx, attr_.get(), &type));
  return type;
}

uint64_t Attribute::cell_size() const {
  auto& ctx = ctx_.get();
  uint64_t cell_size;
  ctx.handle_error(
      tiledb_attribute_get_cell_size(ctx, attr_.get(), &cell_size));
  return cell_size;
}

unsigned Attribute::cell_val_num() const {
  auto& ctx = ctx_.get();
  unsigned num;
  ctx.handle_error(tiledb_attribute_get_cell_val_num(ctx, attr_.get(), &num));
  return num;
}

Attribute& Attribute::set_cell_val_num(unsigned num) {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_attribute_set_cell_val_num(ctx, attr_.get(), num));
  return *this;
}

Compressor Attribute::compressor() const {
  auto& ctx = ctx_.get();
  tiledb_compressor_t compressor;
  int level;
  ctx.handle_error(
      tiledb_attribute_get_compressor(ctx, attr_.get(), &compressor, &level));
  Compressor cmp(compressor, level);
  return cmp;
}

Attribute& Attribute::set_compressor(Compressor c) {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_attribute_set_compressor(
      ctx, attr_.get(), c.compressor(), c.level()));
  return *this;
}

std::shared_ptr<tiledb_attribute_t> Attribute::ptr() const {
  return attr_;
}

Attribute::operator tiledb_attribute_t*() const {
  return attr_.get();
}

/* ********************************* */
/*     PRIVATE STATIC FUNCTIONS      */
/* ********************************* */

Attribute Attribute::create(
    const Context& ctx, const std::string& name, tiledb_datatype_t type) {
  tiledb_attribute_t* attr;
  ctx.handle_error(tiledb_attribute_create(ctx, &attr, name.c_str(), type));
  return Attribute(ctx, attr);
}

/* ********************************* */
/*               MISC                */
/* ********************************* */

std::ostream& operator<<(std::ostream& os, const Attribute& a) {
  os << "Attr<" << a.name() << ',' << tiledb::impl::to_str(a.type()) << ','
     << (a.cell_val_num() == TILEDB_VAR_NUM ? "VAR" :
                                              std::to_string(a.cell_val_num()))
     << '>';
  return os;
}

}  // namespace tiledb
