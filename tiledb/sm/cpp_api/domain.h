/**
 * @file   tiledb_cpp_api_domain.h
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
 * This file declares the C++ API for the TileDB Domain.
 */

#ifndef TILEDB_CPP_API_DOMAIN_H
#define TILEDB_CPP_API_DOMAIN_H

#include "context.h"
#include "deleter.h"
#include "dimension.h"
#include "tiledb.h"
#include "type.h"

#include <functional>
#include <memory>
#include <type_traits>

namespace tiledb {

/**
 * Represents the domain of an array.
 *
 * @details
 * A Domain defines the set of Dimension objects for a given array. The
 * properties of a Domain derive from the underlying dimensions. A
 * Domain is a component of an ArraySchema.
 *
 * @note The dimension can only be signed or unsigned integral types,
 * as well as floating point for sparse array domains.
 *
 * **Example:**
 *
 * @code{.cpp}
 *
 * tiledb::Context ctx;
 * tiledb::Domain domain;
 *
 * // Note the dimension bounds are inclusive.
 * auto d1 = tiledb::Dimension::create<int>(ctx, "d1", {-10, 10});
 * auto d2 = tiledb::Dimension::create<uint64_t>(ctx, "d2", {1, 10});
 * auto d3 = tiledb::Dimension::create<int>(ctx, "d3", {-100, 100});
 *
 * domain.add_dimension(d1);
 * domain.add_dimension(d2);
 * domain.add_dimension(d3); // Invalid, all dims must be same type
 *
 * domain.cell_num(); // (10 - -10 + 1) * (10 - 1 + 1) = 210 max cells
 * domain.type(); // TILEDB_UINT64, determined from the dimensions
 * domain.rank(); // 2, d1 and d2
 *
 * tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
 * schema.set_domain(domain); // Set the array's domain
 *
 * @endcode
 *
 **/
class TILEDB_EXPORT Domain {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  explicit Domain(const Context& ctx);
  Domain(const Context& ctx, tiledb_domain_t* domain);
  Domain(const Domain& domain) = default;
  Domain(Domain&& domain) = default;
  Domain& operator=(const Domain& domain) = default;
  Domain& operator=(Domain&& domain) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Returns the total number of cells in the domain. Throws an exception
   * if the domain type is `float32` or `float64`.
   */
  uint64_t cell_num() const;

  /** Dumps the domain in an ASCII representation to an output. */
  void dump(FILE* out = stdout) const;

  /** Returns the domain type. */
  tiledb_datatype_t type() const;

  /** Get the rank (number of dimensions) **/
  unsigned rank() const;

  /** Returns the current set of dimensions in domain. */
  const std::vector<Dimension> dimensions() const;

  /** Adds a new dimension to the domain. */
  Domain& add_dimension(const Dimension& d);

  /** Add multiple Dimension's. **/
  template <typename... Args>
  Domain& add_dimensions(Args... dims) {
    for (const auto& attr : {dims...}) {
      add_dimension(attr);
    }
    return *this;
  }

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_domain_t> ptr() const;

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_domain_t*() const;

 private:
  /**
   * Returns the total number of cells in the domain.
   *
   * @tparam T The domain datatype.
   */
  template <class T>
  uint64_t cell_num() const;

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

/** Get a string representation of an attribute for an output stream. */
TILEDB_EXPORT std::ostream& operator<<(std::ostream& os, const Domain& d);

}  // namespace tiledb

#endif  // TILEDB_CPP_API_DOMAIN_H
