/**
 * @file   tiledb_cpp_api_domain.cc
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
 * This file define the C++ API for the TileDB Domain object.
 */

#include "tiledb_cpp_api_domain.h"
#include "tiledb_cpp_api_context.h"

void tdb::Domain::_init(tiledb_domain_t *domain) {
  _domain = std::shared_ptr<tiledb_domain_t>(domain, _deleter);
}

const std::vector<tdb::Dimension> tdb::Domain::dimensions() const {
  auto &ctx = _ctx.get();
  unsigned int ndim;
  tiledb_dimension_t *dimptr;
  std::vector<Dimension> dims;
  ctx.handle_error(tiledb_domain_get_rank(ctx.ptr(), _domain.get(), &ndim));
  for (unsigned int i = 0; i < ndim; ++i) {
    ctx.handle_error(
        tiledb_dimension_from_index(ctx.ptr(), _domain.get(), i, &dimptr));
    dims.emplace_back(_ctx, &dimptr);
  }
  return dims;
}

tiledb_datatype_t tdb::Domain::type() const {
  auto &ctx = _ctx.get();
  tiledb_datatype_t type;
  ctx.handle_error(tiledb_domain_get_type(ctx.ptr(), _domain.get(), &type));
  return type;
}

unsigned tdb::Domain::size() const {
  unsigned rank;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_domain_get_rank(ctx.ptr(), _domain.get(), &rank));
  return rank;
}

void tdb::Domain::_create() {
  auto &ctx = _ctx.get();
  tiledb_domain_t *d;
  ctx.handle_error(tiledb_domain_create(ctx.ptr(), &d));
  _init(d);
}

tdb::Domain &tdb::Domain::add_dimension(const tdb::Dimension &d) {
  auto &ctx = _ctx.get();
  ctx.handle_error(
      tiledb_domain_add_dimension(ctx.ptr(), _domain.get(), d.ptr().get()));
  return *this;
}

void tdb::Domain::load(tiledb_domain_t **domain) {
  if (domain && *domain) {
    _init(*domain);
    *domain = nullptr;
  }
}

std::ostream &tdb::operator<<(std::ostream &os, const tdb::Domain &d) {
  os << "Domain<(" << tdb::type::from_tiledb(d.type()) << ")";
  for (const auto &dimension : d.dimensions()) {
    os << " " << dimension;
  }
  os << '>';
  return os;
}

tdb::Domain &tdb::operator<<(tdb::Domain &d, const tdb::Dimension &dim) {
  d.add_dimension(dim);
  return d;
}