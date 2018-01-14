/**
 * @file   tiledb_cpp_api_dimension.h
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
 * This file declares the C++ API for the TileDB Dimension object.
 */

#ifndef TILEDB_CPP_API_DIMENSION_H
#define TILEDB_CPP_API_DIMENSION_H

#include "tiledb.h"
#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_deleter.h"
#include "tiledb_cpp_api_exception.h"
#include "tiledb_cpp_api_object.h"
#include "tiledb_cpp_api_type.h"

#include <functional>
#include <memory>

namespace tdb {

/** Implements the dimension functionality. */
class Dimension {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Dimension(const Context &ctx, tiledb_dimension_t *dim);
  Dimension(const Dimension &) = default;
  Dimension(Dimension &&o) = default;
  Dimension &operator=(const Dimension &) = default;
  Dimension &operator=(Dimension &&o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the name of the dimension. */
  const std::string name() const;

  /** Returns the dimension datatype. */
  tiledb_datatype_t type() const;

  /** Returns the dimension domain. */
  template <typename T>
  typename std::enable_if<std::is_fundamental<T>::value, std::pair<T, T>>::type
  domain() const {
    return domain<typename impl::type_from_native<T>::type>();
  }

  /** Returns the dimension domain. */
  template <typename T>
  std::pair<typename T::type, typename T::type> domain() const {
    auto tdbtype = type();
    if (T::tiledb_datatype != tdbtype) {
      throw TypeError::create<T>(tdbtype);
    }
    typename T::type *d = static_cast<typename T::type *>(_domain());
    return std::pair<typename T::type, typename T::type>(d[0], d[1]);
  }

  /** Returns a string representation of the domain. */
  std::string domain_to_str() const;

  /** Returns the tile extent of the dimension. */
  template <typename T>
  typename std::enable_if<std::is_fundamental<T>::value, T>::type extent() {
    extent<typename impl::type_from_native<T>::type>();
  }

  /** Returns a string representation of the extent. */
  std::string extent_to_str() const;

  /** Returns a shared pointer to the C TileDB dimension object. */
  std::shared_ptr<tiledb_dimension_t> ptr() const;

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_dimension_t *() const;

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Factory function for creating a new dimension with datatype T.
   *
   * @tparam T int, char, etc...
   * @param ctx The TileDB context.
   * @param name The dimension name.
   * @param domain The dimension domain.
   * @return A new `Attribute` object.
   */
  template <typename T>
  typename std::enable_if<std::is_fundamental<T>::value, Dimension>::
      type static create(
          const Context &ctx,
          const std::string &name,
          const std::array<T, 2> &domain,
          T extent) {
    return create<typename impl::type_from_native<T>::type>(
        ctx, name, domain, extent);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** A deleter wrapper. */
  impl::Deleter deleter_;

  /** The C TileDB dimension object. */
  std::shared_ptr<tiledb_dimension_t> dim_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Returns the tile extent of the dimension. */
  template <typename T>
  typename T::type extent() const {
    auto tdbtype = type();
    if (T::tiledb_datatype != tdbtype) {
      throw TypeError::create<T>(tdbtype);
    }
    return *static_cast<typename T::type *>(_extent());
  };

  /** Returns the binary representation of the dimension domain. */
  void *_domain() const;

  /** Returns the binary representation of the dimension extent. */
  void *_extent() const;

  /* ********************************* */
  /*     PRIVATE STATIC FUNCTIONS      */
  /* ********************************* */

  /**
   * Create a new dimension with a domain datatype of DataT.
   *
   * @tparam DataT tdb::type::*
   * @param name name of dimension
   * @param domain bounds of dimension, inclusive coordinates
   * @param extent Tile length in this dimension
   */
  template <typename DataT>
  static Dimension create(
      const Context &ctx,
      const std::string &name,
      const std::array<typename DataT::type, 2> &domain,
      typename DataT::type extent) {
    return create(ctx, name, DataT::tiledb_datatype, domain.data(), &extent);
  }

  /** Creates a dimension with the input name, datatype, domain and extent. */
  static Dimension create(
      const Context &ctx,
      const std::string &name,
      tiledb_datatype_t type,
      const void *domain,
      const void *extent);
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of a dimension for an output stream. */
std::ostream &operator<<(std::ostream &os, const Dimension &dim);

}  // namespace tdb

#endif  // TILEDB_CPP_API_DIMENSION_H
