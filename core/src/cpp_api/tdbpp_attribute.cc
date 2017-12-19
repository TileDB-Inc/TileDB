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

#include "tdbpp_type.h"
#include "tdbpp_attribute.h"
#include "tdbpp_context.h"

void tdb::Attribute::_init(tiledb_attribute_t *attr) {
  _attr = attr;
  auto &ctx = _ctx.get();
  const char *name;
  ctx.handle_error(tiledb_attribute_get_name(ctx, attr, &name));
  _name = std::string(name);
  ctx.handle_error(tiledb_attribute_get_cell_val_num(ctx, attr, &_num));
  ctx.handle_error(tiledb_attribute_get_type(ctx, attr, &_type));
  ctx.handle_error(tiledb_attribute_get_compressor(ctx, attr, &(_compressor.compressor), &(_compressor.level)));
}

tdb::Attribute::~Attribute() {
  if (_attr != nullptr) _ctx.get().handle_error(tiledb_attribute_free(_ctx.get(), _attr));
}

std::ostream &operator<<(std::ostream &os, const tdb::Attribute &a) {
  os << "Attr<" << a.name() << ',' << tdb::type::from_tiledb(a.type()) << '>';
  return os;
}
