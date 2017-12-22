/**
 * @file   tdbpp_array.h
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

#ifndef TILEDB_GENOMICS_ARRAY_H
#define TILEDB_GENOMICS_ARRAY_H

#include "tiledb.h"
#include "tdbpp_arraymeta.h"
#include "tdbpp_context.h"

#include <sstream>
#include <memory>
#include <unordered_map>

namespace tdb {
  /**
   * Open, create, and manage an Array. Arrays are read using tdb::Query's
   */
  class Array {
  public:
    /**
     * Init an array within an context
     * @param ctx context
     */
    Array(Context &ctx) : _ctx(ctx), _meta(_ctx) {}
    /**
     * Init an array using a metadata configuration
     * @param meta metadata
     */
    Array(const ArrayMetadata &meta);
    /**
     * @param ctx context
     * @param uri Array to open
     */
    Array(Context &ctx, const std::string &uri) : _ctx(ctx), _meta(_ctx, uri) {}
    Array(const Array&) = delete;
    Array(Array&&) = default;
    Array &operator=(const Array&) = delete;
    Array &operator=(Array&&) = default;

    /**
     * @return Name of the current array
     */
    const std::string name() const;

    /**
     * @return True if the underlying metadata is initilized.
     */
    bool good() const;

    /**
     * @param uri Array path to open.
     */
    void load(const std::string &uri);

    /**
     * @param meta Create a new array with given metadata
     */
    void create(const ArrayMetadata &meta);

    Context &context();

    const Context &context() const;

    ArrayMetadata &meta();

    const ArrayMetadata &meta() const;

  private:
    std::reference_wrapper<Context> _ctx;
    ArrayMetadata _meta;
  };


}

std::ostream &operator<<(std::ostream &os, const tdb::Array &array);


#endif //TILEDB_GENOMICS_ARRAY_H
