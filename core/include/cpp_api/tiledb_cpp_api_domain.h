/**
 * @file   tiledb_cpp_api_domain.h
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
 * This file declares the C++ API for the TileDB Domain.
 */

#ifndef TILEDB_CPP_API_DOMAIN_H
#define TILEDB_CPP_API_DOMAIN_H

#include "tiledb.h"
#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_deleter.h"
#include "tiledb_cpp_api_dimension.h"
#include "tiledb_cpp_api_type.h"

#include <functional>
#include <memory>
#include <type_traits>

namespace tdb {

/** Implements the domain functionality. */
class Domain {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  explicit Domain(const Context &ctx);
  Domain(const Context &ctx, tiledb_domain_t *domain);
  Domain(const Domain &domain) = default;
  Domain(Domain &&domain) = default;
  Domain &operator=(const Domain &domain) = default;
  Domain &operator=(Domain &&domain) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the domain type. */
  tiledb_datatype_t type() const;

  /** Returns the current set of dimensions in domain. */
  const std::vector<Dimension> dimensions() const;

  /** Adds a new dimension to the domain. */
  Domain &add_dimension(const Dimension &d);

  /** Returns the number of dimensions in the domain. */
  unsigned dim_num() const;

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_domain_t> ptr() const;

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_domain_t *() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_domain_t> domain_;
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Operator tha add a dimension to the domain. */
Domain &operator<<(Domain &d, const Dimension &dim);

/** Get a string representation of an attribute for an output stream. */
std::ostream &operator<<(std::ostream &os, const Domain &d);

}  // namespace tdb

#endif  // TILEDB_CPP_API_DOMAIN_H
