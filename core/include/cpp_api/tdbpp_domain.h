/**
 * @file   tdbpp_domain.h
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

#ifndef TILEDB_GENOMICS_DOMAIN_H
#define TILEDB_GENOMICS_DOMAIN_H

#include "tiledb.h"
#include "tdbpp_dimension.h"
#include "tdbpp_type.h"

#include <functional>
#include <memory>

namespace tdb {

  class Context;
  class Dimension;

  class Domain {
  public:
    Domain(Context &ctx) : _ctx(ctx), _deleter(ctx) {}
    Domain(Context &ctx, tiledb_domain_t **domain) : Domain(ctx) {
      load(domain);
    }
    Domain(Context &ctx, tiledb_datatype_t type) : Domain(ctx) {
      create(type);
    }
    Domain(const Domain& o) = default;
    Domain(Domain&& o) = default;
    Domain &operator=(const Domain&) = default;
    Domain &operator=(Domain&& o) = default;

    void load(tiledb_domain_t **domain) {
      if (domain && *domain) {
        _init(*domain);
        *domain = nullptr;
      }
    }

    template<typename DataT>
    void create() {
      _create(DataT::tiledb_datatype);
    }

    void create(tiledb_datatype_t type) {
      _create(type);
    }

    tiledb_datatype_t type() const;

    const std::vector<Dimension> dimensions() const;

    Domain &add_dimension(const Dimension &d);

    unsigned size() const;

    std::shared_ptr<tiledb_domain_t> ptr() const {
      return _domain;
    }

  private:
    void _init(tiledb_domain_t *domain);
    void _create(tiledb_datatype_t type);

    struct _Deleter {
      _Deleter(Context& ctx) : _ctx(ctx) {}
      _Deleter(const _Deleter&) = default;
      void operator()(tiledb_domain_t *p);
    private:
      std::reference_wrapper<Context> _ctx;
    };

    std::reference_wrapper<Context> _ctx;
    _Deleter _deleter;
    std::shared_ptr<tiledb_domain_t> _domain;
  };
}

std::ostream &operator<<(std::ostream &os, const tdb::Domain &d);
tdb::Domain &operator<<(tdb::Domain &d, const tdb::Dimension &dim);

#endif //TILEDB_GENOMICS_DOMAIN_H
