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

namespace tdb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Domain::Domain(const Context &ctx)
    : ctx_(ctx)
    , deleter_(ctx) {
  tiledb_domain_t *domain;
  ctx.handle_error(tiledb_domain_create(ctx, &domain));
  domain_ = std::shared_ptr<tiledb_domain_t>(domain, deleter_);
}

Domain::Domain(const Context &ctx, tiledb_domain_t *domain)
    : ctx_(ctx)
    , deleter_(ctx) {
  domain_ = std::shared_ptr<tiledb_domain_t>(domain, deleter_);
}

/* ********************************* */
/*                API                */
/* ********************************* */

std::shared_ptr<tiledb_domain_t> Domain::ptr() const {
  return domain_;
}

Domain::operator tiledb_domain_t *() const {
  return domain_.get();
}

const std::vector<tdb::Dimension> Domain::dimensions() const {
  auto &ctx = ctx_.get();
  unsigned int ndim;
  tiledb_dimension_t *dimptr;
  std::vector<Dimension> dims;
  ctx.handle_error(tiledb_domain_get_rank(ctx, domain_.get(), &ndim));
  for (unsigned int i = 0; i < ndim; ++i) {
    ctx.handle_error(
        tiledb_dimension_from_index(ctx, domain_.get(), i, &dimptr));
    dims.emplace_back(Dimension(ctx, dimptr));
  }
  return dims;
}

tiledb_datatype_t Domain::type() const {
  auto &ctx = ctx_.get();
  tiledb_datatype_t type;
  ctx.handle_error(tiledb_domain_get_type(ctx, domain_.get(), &type));
  return type;
}

unsigned Domain::dim_num() const {
  unsigned rank;
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_domain_get_rank(ctx, domain_.get(), &rank));
  return rank;
}

Domain &Domain::add_dimension(const Dimension &d) {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_domain_add_dimension(ctx, domain_.get(), d));
  return *this;
}

/* ********************************* */
/*               MISC                */
/* ********************************* */

Domain &operator<<(Domain &d, const Dimension &dim) {
  d.add_dimension(dim);
  return d;
}

std::ostream &operator<<(std::ostream &os, const Domain &d) {
  os << "Domain<(" << impl::to_str(d.type()) << ")";
  for (const auto &dimension : d.dimensions()) {
    os << " " << dimension;
  }
  os << '>';
  return os;
}

}  // namespace tdb
