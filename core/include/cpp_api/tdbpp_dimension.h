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

#include "tiledb.h"
#include "tdbpp_type.h"

#include <functional>
#include <memory>

namespace tdb {

  class Context;

  class Dimension {
  public:
    Dimension(Context &ctx) : _ctx(ctx), _deleter(ctx) {}
    Dimension(Context &ctx, tiledb_dimension_t **dim) : Dimension(ctx) {
      load(dim);
    }
    Dimension(const Dimension &) = default;
    Dimension(Dimension &&o) = default;
    Dimension &operator=(const Dimension &) = default;
    Dimension &operator=(Dimension &&o) = default;

    void load(tiledb_dimension_t **dim) {
      if (dim != nullptr && *dim != nullptr) {
        _init(*dim);
        *dim = nullptr;
      }
    }

    template<typename DataT, typename NativeT=typename DataT::type>
    Dimension &create(const std::string &name, std::pair<NativeT, NativeT> domain, NativeT extent) {
      _create(name, DataT::tiledb_datatype, &domain, &extent);
      return *this;
    }

    const std::string name() const;

    tiledb_datatype_t type() const;

    template<typename T, typename NativeT=typename T::type>
    std::pair<NativeT, NativeT> domain() const {
      auto tdbtype = type();
      if (T::tiledb_datatype != tdbtype) {
        throw std::invalid_argument("Attempting to use domain of type " + std::string(T::name) +
                                    " for attribute of type " + type::from_tiledb(tdbtype));
      }
      NativeT *d = static_cast<NativeT *>(_domain());
      return std::pair<NativeT, NativeT>(d[0], d[1]);
    };

    template<typename T, typename NativeT=typename T::type>
    std::pair<NativeT, NativeT> extent() const {
      auto tdbtype = type();
      if (T::tiledb_datatype != tdbtype) {
        throw std::invalid_argument("Attempting to use extent of type " + std::string(T::name) +
                                    " for attribute of type " + type::from_tiledb(tdbtype));
      }
      NativeT *e = static_cast<NativeT *>(_extent());
      return std::make_pair<NativeT, NativeT>(e[0], e[1]);
    };

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
    struct _Deleter {
      _Deleter(Context &ctx) : _ctx(ctx) {}

      _Deleter(const _Deleter &) = default;

      void operator()(tiledb_dimension_t *p);

    private:
      std::reference_wrapper<Context> _ctx;
    };

    std::reference_wrapper<Context> _ctx;
    _Deleter _deleter;
    std::shared_ptr<tiledb_dimension_t> _dim;

    void _init(tiledb_dimension_t *dim);
    void _create(const std::string &name, tiledb_datatype_t type, const void *domain, const void *extent);
    void *_domain() const;
    void *_extent() const;
  };

}

std::ostream &operator<<(std::ostream &os, const tdb::Dimension &dim);

#endif //TILEDB_GENOMICS_DOMENSION_H
