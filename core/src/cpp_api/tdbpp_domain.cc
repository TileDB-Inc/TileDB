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

#include "tdbpp_context.h"


void tdb::Domain::_init(tiledb_domain_t *domain) {
  _domain = domain;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_domain_get_type(ctx, domain, &_type));

  unsigned int ndim;
  tiledb_dimension_t *dimptr;
  ctx.handle_error(tiledb_domain_get_rank(ctx, domain, &ndim));
  for (unsigned int i = 0; i < ndim; ++i) {
    ctx.handle_error(tiledb_dimension_from_index(ctx, domain, i, &dimptr));
    Dimension *dim = new Dimension(_ctx, dimptr);
    _dims[dim->name()] = dim;
  }

}

tdb::Domain::~Domain() {
  for (auto &d : _dims) {
    delete d.second;
  }
  if (_domain != nullptr) {
    _ctx.get().handle_error(tiledb_domain_free(_ctx.get(), _domain));
  }
}

std::ostream &operator<<(std::ostream &os, const tdb::Domain &d) {
  os << "Domain<(" << tdb::type::from_tiledb(d.type()) << ")";
  auto dims = d.dimension_names();
  for (const auto &n : dims) {
    const auto &dimension = d.get_dimension(n);
    os << " " << dimension;
  }
  os << '>';
  return os;
}
