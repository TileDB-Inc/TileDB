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
#include "tdbpp_type.h"

#include <functional>

namespace tdb {

  class Context;

  class Dimension {
  public:
    Dimension(Context &ctx) : _ctx(ctx) {}
    Dimension(Context &ctx, tiledb_dimension_t *dim) : _ctx(ctx){
        if (dim != nullptr) _init(dim);
    }
    Dimension(const Dimension&) = delete;
    Dimension(Dimension&& o) : _ctx(o._ctx) {
      *this = std::move(o);
    }
    Dimension &operator=(const Dimension&) = delete;
    Dimension &operator=(Dimension&& o);

    const std::string &name() const {
      return _name;
    }

    tiledb_datatype_t type() const {
      return _type;
    }

    template<typename T, typename NativeT=typename T::type>
    std::pair<NativeT, NativeT> domain() const {
      if (T::tiledb_datatype != _type) {
        throw std::invalid_argument("Attempting to use domain of type " + std::string(T::name) +
                                    " for attribute of type " + type::from_tiledb(_type));
      }
      NativeT* d = static_cast<NativeT*>(_domain);
      return std::pair<NativeT, NativeT>(d[0], d[1]);
    };

    template<typename T, typename NativeT=typename T::type>
    std::pair<NativeT, NativeT> extent() const {
      if (T::tiledb_datatype != _type) {
        throw std::invalid_argument("Attempting to use extent of type " + std::string(T::name) +
                                    " for attribute of type " + type::from_tiledb(_type));
      }
      NativeT* e = static_cast<NativeT*>(_tile_extent);
      return std::make_pair<NativeT, NativeT>(e[0], e[1]);
    };

  private:
    std::reference_wrapper<Context> _ctx;
    std::string _name;
    tiledb_datatype_t _type;
    void *_domain, *_tile_extent;


    void _init(tiledb_dimension_t *dim);
  };

}

std::ostream &operator<<(std::ostream &os, const tdb::Dimension &dim);

#endif //TILEDB_GENOMICS_DOMENSION_H
