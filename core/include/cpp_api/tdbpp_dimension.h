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

#ifndef TILEDB_GENOMICS_DOMENSION_H
#define TILEDB_GENOMICS_DOMENSION_H

#include "tdbpp_context.h"

#include <functional>

namespace tdb {

  class Context;

  class Dimension {
  public:
    Dimension(Context &ctx) : _ctx(ctx) {}
    Dimension(Context &ctx, const tiledb_dimension_t *dim) : _ctx(ctx){
        if (dim != nullptr) _init(dim);
    }
    Dimension(const Dimension&) = default;
    Dimension(Dimension&&) = default;
    Dimension &operator=(const Dimension&) = default;
    Dimension &operator=(Dimension&&) = default;

  private:
    std::reference_wrapper<Context> _ctx;
    std::string _name;
    void *_domain, *_tile_extent;


    void _init(const tiledb_dimension_t *dim);
  };

}

#endif //TILEDB_GENOMICS_DOMENSION_H
