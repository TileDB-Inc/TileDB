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

  class Array {
  public:
    Array(Context &ctx) : _ctx(ctx), _meta(_ctx) {}
    Array(const ArrayMetadata &meta) : _ctx(meta._ctx), _meta(meta) {
      create(meta);
    }
    Array(Context &ctx, const std::string &uri) : _ctx(ctx), _meta(_ctx, uri) {}
    Array(const Array&) = delete;
    Array(Array&&) = default;
    Array &operator=(const Array&) = delete;
    Array &operator=(Array&&) = default;

    const std::string name() const {
      return _meta.name();
    }

    bool good() const {
      return _meta.good();
    }

    void load(const std::string &uri) {
      _meta.load(uri);
    }

    void create(const ArrayMetadata &meta) {
      meta.check();
      auto &ctx = _ctx.get();
      _meta = meta;
      ctx.handle_error(tiledb_array_create(ctx, meta._meta.get()));
    }

    Context &context() {
      return _ctx.get();
    }

    const Context &context() const {
      return _ctx.get();
    }

    ArrayMetadata &meta() {
      return _meta;
    }

    const ArrayMetadata &meta() const {
      return _meta;
    }

  private:
    std::reference_wrapper<Context> _ctx;
    ArrayMetadata _meta;
  };

}

std::ostream &operator<<(std::ostream &os, const tdb::Array &array);


#endif //TILEDB_GENOMICS_ARRAY_H
