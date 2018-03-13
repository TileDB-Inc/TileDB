/**
 * @file   tiledb_cpp_api_domain.cc
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
 * This file define the C++ API for the TileDB Domain object.
 */

#include "domain.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Domain::Domain(const Context& ctx)
    : ctx_(ctx)
    , deleter_(ctx) {
  tiledb_domain_t* domain;
  ctx.handle_error(tiledb_domain_create(ctx, &domain));
  domain_ = std::shared_ptr<tiledb_domain_t>(domain, deleter_);
}

Domain::Domain(const Context& ctx, tiledb_domain_t* domain)
    : ctx_(ctx)
    , deleter_(ctx) {
  domain_ = std::shared_ptr<tiledb_domain_t>(domain, deleter_);
}

/* ********************************* */
/*                API                */
/* ********************************* */

uint64_t Domain::cell_num() const {
  auto type = this->type();
  if (type == TILEDB_FLOAT32 || type == TILEDB_FLOAT64)
    throw TileDBError(
        "[TileDB::C++API::Domain] Cannot compute number of cells for a "
        "non-integer domain");

  switch (type) {
    case TILEDB_INT8:
      return cell_num<int8_t>();
    case TILEDB_UINT8:
      return cell_num<uint8_t>();
    case TILEDB_INT16:
      return cell_num<int16_t>();
    case TILEDB_UINT16:
      return cell_num<uint16_t>();
    case TILEDB_INT32:
      return cell_num<int32_t>();
    case TILEDB_UINT32:
      return cell_num<uint32_t>();
    case TILEDB_INT64:
      return cell_num<int64_t>();
    case TILEDB_UINT64:
      return cell_num<uint64_t>();
    default:
      throw TileDBError(
          "[TileDB::C++API::Domain] Cannot compute number of cells; Unknown "
          "domain type");
  }

  return 0;
}

template <class T>
uint64_t Domain::cell_num() const {
  uint64_t ret = 1;
  auto dimensions = this->dimensions();
  for (const auto& dim : dimensions) {
    const auto& d = dim.domain<T>();
    ret *= (d.second - d.first + 1);
  }

  return ret;
}

void Domain::dump(FILE* out) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_domain_dump(ctx, domain_.get(), out));
}

std::shared_ptr<tiledb_domain_t> Domain::ptr() const {
  return domain_;
}

Domain::operator tiledb_domain_t*() const {
  return domain_.get();
}

unsigned Domain::rank() const {
  auto& ctx = ctx_.get();
  unsigned n;
  ctx.handle_error(tiledb_domain_get_rank(ctx, domain_.get(), &n));
  return n;
}

const std::vector<tiledb::Dimension> Domain::dimensions() const {
  auto& ctx = ctx_.get();
  unsigned int ndim;
  tiledb_dimension_t* dimptr;
  std::vector<Dimension> dims;
  ctx.handle_error(tiledb_domain_get_rank(ctx, domain_.get(), &ndim));
  for (unsigned int i = 0; i < ndim; ++i) {
    ctx.handle_error(
        tiledb_domain_get_dimension_from_index(ctx, domain_.get(), i, &dimptr));
    dims.emplace_back(Dimension(ctx, dimptr));
  }
  return dims;
}

tiledb_datatype_t Domain::type() const {
  auto& ctx = ctx_.get();
  tiledb_datatype_t type;
  ctx.handle_error(tiledb_domain_get_type(ctx, domain_.get(), &type));
  return type;
}

Domain& Domain::add_dimension(const Dimension& d) {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_domain_add_dimension(ctx, domain_.get(), d));
  return *this;
}

/* ********************************* */
/*               MISC                */
/* ********************************* */

std::ostream& operator<<(std::ostream& os, const Domain& d) {
  os << "Domain<(" << impl::to_str(d.type()) << ")";
  for (const auto& dimension : d.dimensions()) {
    os << " " << dimension;
  }
  os << '>';
  return os;
}

}  // namespace tiledb
