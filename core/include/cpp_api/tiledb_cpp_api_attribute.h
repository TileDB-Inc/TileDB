/**
 * @file   tiledb_cpp_api_attribute.h
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
 * This file declares the C++ API for the TileDB Attribute object.
 */

#ifndef TILEDB_CPP_API_ATTRIBUTE_H
#define TILEDB_CPP_API_ATTRIBUTE_H

#include "tiledb.h"
#include "tiledb_cpp_api_compressor.h"
#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_deleter.h"
#include "tiledb_cpp_api_object.h"
#include "tiledb_cpp_api_type.h"

#include <functional>
#include <memory>

namespace tdb {

/** Implements the attribute functionality. */
class Attribute {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Attribute(const Context &ctx, tiledb_attribute_t *attr);
  Attribute(const Attribute &attr) = default;
  Attribute(Attribute &&o) = default;
  Attribute &operator=(const Attribute &) = default;
  Attribute &operator=(Attribute &&o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the name of the attribute. */
  std::string name() const;

  /** Returns the attribute datatype. */
  tiledb_datatype_t type() const;

  /** Returns the number of values stored in each cell. */
  unsigned cell_val_num() const;

  /** Sets the number of attribute values per cell. */
  Attribute &set_cell_val_num(unsigned num);

  /** Returns the attribute compressor. */
  Compressor compressor() const;

  /** Sets the attribute compressor. */
  Attribute &set_compressor(Compressor c);

  /** Returns the C TileDB attribute object pointer. */
  tiledb_attribute_t *ptr() const;

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Factory function for creating a new attribute with datatype T.
   *
   * @tparam T int, char, etc...
   * @param ctx The TileDB context.
   * @param name The attribute name.
   * @return A new `Attribute` object.
   */
  template <typename T>
  static typename std::enable_if<std::is_fundamental<T>::value, Attribute>::type
  create(const Context &ctx, const std::string &name) {
    return create<typename type::type_from_native<T>::type>(ctx, name);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;

  /** The pointer to the C TileDB attribute object. */
  std::shared_ptr<tiledb_attribute_t> attr_;

  /* ********************************* */
  /*     PRIVATE STATIC FUNCTIONS      */
  /* ********************************* */

  /**
   * Auxiliary function that converts a basic datatype to a TileDB C
   * datatype.
   */
  template <typename DataT, typename = typename DataT::type>
  static Attribute create(const Context &ctx, const std::string &name) {
    return create(ctx, name, DataT::tiledb_datatype);
  }

  /** Creates an attribute with the input name and datatype. */
  static Attribute create(
      const Context &ctx, const std::string &name, tiledb_datatype_t type);
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of an attribute for an output stream. */
std::ostream &operator<<(std::ostream &os, const Attribute &a);

}  // namespace tdb

#endif  // TILEDB_CPP_API_ATTRIBUTE_H
