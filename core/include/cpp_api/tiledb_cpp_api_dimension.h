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
#include "tiledb_cpp_api_deleter.h"
#include "tiledb_cpp_api_object.h"
#include "tiledb_cpp_api_type.h"

#include <functional>
#include <memory>

namespace tdb {

class Context;

class Dimension {
 public:
  Dimension(Context &ctx)
      : _ctx(ctx)
      , _deleter(ctx) {
  }
  Dimension(Context &ctx, tiledb_dimension_t **dim)
      : Dimension(ctx) {
    load(dim);
  }
  Dimension(const Dimension &) = default;
  Dimension(Dimension &&o) = default;
  Dimension &operator=(const Dimension &) = default;
  Dimension &operator=(Dimension &&o) = default;

  /**
   * Load and take ownership of a tiledb_dimension
   *
   * @param dim
   */
  void load(tiledb_dimension_t **dim);

  /**
   * Create a new dimension with a domain datatype of DataT.
   *
   * @tparam DataT tdb::type::*
   * @param name name of dimension
   * @param domain bounds of dimension, inclusive coordinates
   * @param extent Tile length in this dimension
   */
  template <typename DataT>
  Dimension &create(
      const std::string &name,
      std::array<typename DataT::type, 2> domain,
      typename DataT::type extent) {
    void *p = domain.data();
    p = (typename DataT::type *)p;
    _create(name, DataT::tiledb_datatype, domain.data(), &extent);
    return *this;
  }

  /**
   * Create a dimension with a native datatype
   *
   * @tparam T int, char,...
   * @param name name of dimension
   * @param domain bounds of dimension, inclusive coordinates
   * @param extent Tile length in this dimension
   */
  template <typename T>
  typename std::enable_if<std::is_fundamental<T>::value, Dimension &>::type
  create(const std::string &name, std::array<T, 2> domain, T extent) {
    return create<typename type::type_from_native<T>::type>(
        name, domain, extent);
  }

  /**
   * @return Name of the dimension
   */
  const std::string name() const;

  /**
   * @return Dimension datatype
   */
  tiledb_datatype_t type() const;

  /**
   * Get the bounds of the dimension.
   *
   * @tparam T Type of underlying dimension.
   * @return
   */
  template <typename T>
  std::pair<typename T::type, typename T::type> domain() const {
    auto tdbtype = type();
    if (T::tiledb_datatype != tdbtype) {
      throw std::invalid_argument(
          "Attempting to use type of " + std::string(T::name) +
          " for dimension of type " + type::from_tiledb(tdbtype));
    }
    typename T::type *d = static_cast<typename T::type *>(_domain());
    return std::pair<typename T::type, typename T::type>(d[0], d[1]);
  };

  template <typename T>
  typename std::enable_if<std::is_fundamental<T>::value, std::pair<T, T>>::type
  domain() {
    domain<typename type::type_from_native<T>::type>();
  }

  /**
   * Get the tile extent of the dimension.
   *
   * @tparam T datatype of the extent.
   */
  template <typename T>
  typename T::type extent() const {
    auto tdbtype = type();
    if (T::tiledb_datatype != tdbtype) {
      throw std::invalid_argument(
          "Attempting to use extent of type " + std::string(T::name) +
          " for dimension of type " + type::from_tiledb(tdbtype));
    }
    return *static_cast<typename T::type *>(_extent());
  };

  template <typename T>
  typename std::enable_if<std::is_fundamental<T>::value, T>::type extent() {
    extent<typename type::type_from_native<T>::type>();
  }

  const tiledb_dimension_t &dim() const {
    return *_dim;
  }

  tiledb_dimension_t &dim() {
    return *_dim;
  }

  std::shared_ptr<tiledb_dimension_t> ptr() const {
    return _dim;
  }

 private:
  std::reference_wrapper<Context> _ctx;
  impl::Deleter _deleter;
  std::shared_ptr<tiledb_dimension_t> _dim;

  void _init(tiledb_dimension_t *dim);
  void _create(
      const std::string &name,
      tiledb_datatype_t type,
      const void *domain,
      const void *extent);
  void *_domain() const;
  void *_extent() const;
};

std::ostream &operator<<(std::ostream &os, const Dimension &dim);

}  // namespace tdb

#endif  // TILEDB_CPP_API_DIMENSION_H
