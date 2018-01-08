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
#include "tiledb_cpp_api_deleter.h"
#include "tiledb_cpp_api_dimension.h"
#include "tiledb_cpp_api_type.h"

#include <functional>
#include <memory>
#include <type_traits>

namespace tdb {

class Context;

class Domain {
 public:
  Domain(Context &ctx)
      : _ctx(ctx)
      , _deleter(ctx) {
    _create();
  }
  Domain(Context &ctx, tiledb_domain_t **domain)
      : Domain(ctx) {
    load(domain);
  }
  Domain(const Domain &o) = default;
  Domain(Domain &&o) = default;
  Domain &operator=(const Domain &) = default;
  Domain &operator=(Domain &&o) = default;

  /**
   * Load and take ownership of a domain.
   *
   * @param domain
   */
  void load(tiledb_domain_t **domain);

  tiledb_datatype_t type() const;

  /**
   * @return Current set of dimensions in domain.
   */
  const std::vector<Dimension> dimensions() const;

  /**
   * Add a new dimension to the domain.
   *
   * @param d dimension to add
   */
  Domain &add_dimension(const Dimension &d);

  /**
   * @return Number of dimensions in the domain.
   */
  unsigned size() const;

  std::shared_ptr<tiledb_domain_t> ptr() const {
    return _domain;
  }

 private:
  void _init(tiledb_domain_t *domain);
  void _create();

  std::reference_wrapper<Context> _ctx;
  impl::Deleter _deleter;
  std::shared_ptr<tiledb_domain_t> _domain;
};

std::ostream &operator<<(std::ostream &os, const Domain &d);
tdb::Domain &operator<<(tdb::Domain &d, const Dimension &dim);

}  // namespace tdb

#endif  // TILEDB_CPP_API_DOMAIN_H
