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

#ifndef TILEDB_GENOMICS_ARRAY_H
#define TILEDB_GENOMICS_ARRAY_H

#include "tiledb.h"
#include "tdbpp_domain.h"
#include "tdbpp_context.h"
#include "tdbpp_attribute.h"

#include <sstream>
#include <memory>

namespace tdb {

  class Context;

  struct ArrayConfig {
  public:
    ArrayConfig(Context &ctx) : domain(ctx), _ctx(ctx) {};
    ArrayConfig(Context &ctx, tiledb_array_metadata_t *meta) : domain(ctx), _ctx(ctx) {
      _init(meta);
    };
    ArrayConfig(Context &ctx, const std::string &uri) : domain(ctx), _ctx(ctx) {
      _init(uri);
    }
    ~ArrayConfig();

    Domain domain;
    tiledb_array_type_t type;
    tiledb_layout_t tile_layout;
    tiledb_layout_t cell_layout;
    uint64_t capacity;
    Compressor coords;
    std::string uri;

    std::string to_str() const {
      std::stringstream ss;
      ss << (type == TILEDB_DENSE ? "Dense" : "Sparse") << " array: " << uri;
      return ss.str();
    }

  private:
    friend class Array;

    void _init(tiledb_array_metadata_t* meta);
    void _init(const std::string &uri);

    std::reference_wrapper<Context> _ctx;
    std::vector<Attribute> _attrs;
    tiledb_array_metadata_t *_meta = nullptr;


  };


  class Array {
  public:
    Array(ArrayConfig &config) : _ctx(config._ctx), _meta(config) {}
    Array(Context &ctx, const std::string &uri) : _ctx(ctx), _meta(_ctx, uri) {}
    Array(const Array&) = default;
    Array(Array&&) = default;
    Array &operator=(const Array&) = default;
    Array &operator=(Array&&) = default;

    const std::string &uri() const {
      return _meta.uri;
    }

    bool good() const {
      return !uri().empty();
    }

  private:
    std::reference_wrapper<Context> _ctx;
    ArrayConfig _meta;
  };



}

std::ostream &operator<<(std::ostream &os, const tdb::Array &array);
std::ostream &operator<<(std::ostream &os, const tdb::ArrayConfig &config);


#endif //TILEDB_GENOMICS_ARRAY_H
