/**
 * @file  tiledb_cpp_api_schema_base.h
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
 * This file declares the C++ API for the TileDB Schema base class.
 */

#ifndef TILEDB_TILEDB_CPP_API_SCHEMA_BASE_H
#define TILEDB_TILEDB_CPP_API_SCHEMA_BASE_H

#include "tiledb_cpp_api_deleter.h"

#include <unordered_map>

namespace tdb {

  class Attribute;
  class Context;
  class Compressor;

  /**
   * Base class for TileDB schemas. This is intended
   * for all array-backed stores.
   */
  class Schema {
  public:
    const Context &context() const {
      return ctx_.get();
    }

  protected:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    explicit Schema(const Context &ctx) : ctx_(ctx), deleter_(ctx) {}
    Schema(const Schema &) = default;
    Schema(Schema &&o) = default;
    Schema &operator=(const Schema &) = default;
    Schema &operator=(Schema &&o) = default;

    /* ********************************* */
    /*          VIRTUAL INTERFACE        */
    /* ********************************* */

    /** Dumps the array schema in an ASCII representation to an output. */
    virtual void dump(FILE *out) const = 0;

    /** Adds an attribute to the array. */
    virtual Schema &add_attribute(const Attribute &attr) = 0;

    /** Validates the schema. */
    virtual void check() const  = 0;

    /** Gets all attributes in the array. */
    virtual const std::unordered_map<std::string, Attribute> attributes() const = 0;

    /** Get an attribute by name. **/
    virtual Attribute attribute(const std::string &name) const = 0;

    /** Get an attribute by index **/
    virtual Attribute attribute(unsigned int i) const = 0;

    /** The TileDB context. */
    std::reference_wrapper<const Context> ctx_;

    /** Deleter wrapper. */
    impl::Deleter deleter_;

  };

}

#endif //TILEDB_TILEDB_CPP_API_SCHEMA_BASE_H
