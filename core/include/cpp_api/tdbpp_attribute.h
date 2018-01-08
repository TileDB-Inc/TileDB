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

#ifndef TILEDB_GENOMICS_ATTRIBUTE_H
#define TILEDB_GENOMICS_ATTRIBUTE_H

#include "tdbpp_context.h"
#include "tdbpp_object.h"
#include "tdbpp_type.h"
#include "tiledb.h"

#include <functional>
#include <memory>

namespace tdb {

class Attribute {
 public:
  Attribute(Context &ctx)
      : _ctx(ctx)
      , _deleter(ctx) {
  }
  Attribute(Context &ctx, tiledb_attribute_t **attr)
      : Attribute(ctx) {
    load(attr);
  }
  Attribute(const Attribute &attr) = default;
  Attribute(Attribute &&o) = default;
  Attribute &operator=(const Attribute &) = default;
  Attribute &operator=(Attribute &&o) = default;

  /**
   * Load an array from tiledb, and take ownership.
   *
   * @param attr attribute to load
   */
  void load(tiledb_attribute_t **attr) {
    if (attr && *attr) {
      _init(*attr);
      *attr = nullptr;
    }
  }

  /**
   * Create a new attribute of type DataT
   *
   * @tparam DataT tdb::type::*
   * @param name Attribute name
   * @return *this
   */
  template <typename DataT, typename = typename DataT::type>
  void create(const std::string &name) {
    _create(name, DataT::tiledb_datatype);
  }

  /**
   * Create a new attribute with datatype T. Reverse lookup for DataT.
   *
   * @tparam T int,char, etc...
   * @param name
   */
  template <typename T>
  typename std::enable_if<std::is_fundamental<T>::value, void>::type create(
      const std::string &name) {
    create<typename type::type_from_native<T>::type>(name);
  }

  /**
   * Create an attribute by naming the type at runtime.
   *
   * @param name
   * @param type
   */
  void create(const std::string &name, tiledb_datatype_t type) {
    _create(name, type);
  }

  /**
   * @return name of attribute
   */
  std::string name() const;

  /**
   * @return Attribute datatype
   */
  tiledb_datatype_t type() const;

  /**
   * @return The number of elements in each cell.
   */
  unsigned num() const;

  /**
   * Set the number of attribute elements per cell.
   *
   * @param num
   */
  Attribute &set_num(unsigned num);

  /**
   * @return Get the current compressor.
   */
  Compressor compressor() const;

  /**
   * Set the attribute compressor.
   *
   * @param c
   */
  Attribute &set_compressor(Compressor c);

  std::shared_ptr<tiledb_attribute_t> ptr() const {
    return _attr;
  }

 protected:
  void _init(tiledb_attribute_t *attr);
  void _create(const std::string &name, tiledb_datatype_t type);

  std::reference_wrapper<Context> _ctx;
  _Deleter _deleter;
  std::shared_ptr<tiledb_attribute_t> _attr;
};

/**
 * Get a string repr of the attribute.
 *
 * @param os
 * @param a
 */
std::ostream &operator<<(std::ostream &os, const Attribute &a);

/**
 * Set the attribute compressor.
 *
 * @param c
 */
Attribute &operator<<(Attribute &attr, const Compressor &c);

/**
 * Set the number of attribute elements per cell.
 *
 * @param num
 */
Attribute &operator<<(Attribute &attr, unsigned num);

}  // namespace tdb

#endif  // TILEDB_GENOMICS_ATTRIBUTE_H
