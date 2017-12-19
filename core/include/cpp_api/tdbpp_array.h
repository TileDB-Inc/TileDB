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
#include <unordered_map>

namespace tdb {

  class Context;

  class ArrayMetadata {
  public:
    ArrayMetadata(Context &ctx) : _domain(ctx), _ctx(ctx) {};
    ArrayMetadata(Context &ctx, tiledb_array_metadata_t *meta) : _domain(ctx), _ctx(ctx) {
      _init(meta);
    };
    ArrayMetadata(Context &ctx, const std::string &uri) : _domain(ctx), _ctx(ctx) {
      _init(uri);
    }
    ArrayMetadata(const ArrayMetadata&) = delete;
    ArrayMetadata(ArrayMetadata&& o);
    ArrayMetadata &operator=(ArrayMetadata&) = delete;
    ArrayMetadata &operator=(ArrayMetadata &&o);
    ~ArrayMetadata();
    std::string to_str() const;

    const std::string &uri() const {
      return _uri;
    }

    void load(const std::string &uri) {
      _init(uri);
    }

    const Domain &domain() const {
      return _domain;
    }

    const Compressor &coord_compressor() const {
      return _coords;
    }

    const std::unordered_map<std::string, Attribute> &attributes() const {
      return _attrs;
    };

    std::vector<std::string> attribute_names() const {
      std::vector<std::string> ret;
      ret.reserve(_attrs.size());
      for (const auto& a : _attrs) ret.push_back(a.first);
      return ret;
    }

  private:
    friend class Array;

    void _init(tiledb_array_metadata_t* meta);
    void _init(const std::string &uri);

    Domain _domain;
    std::reference_wrapper<Context> _ctx;
    std::unordered_map<std::string, Attribute> _attrs;
    tiledb_array_metadata_t *_meta = nullptr;
    tiledb_array_type_t _type;
    tiledb_layout_t _tile_layout;
    tiledb_layout_t _cell_layout;
    uint64_t _capacity;
    Compressor _coords;
    std::string _uri;
  };


  class Array {
  public:
    Array(ArrayMetadata &&meta) : _ctx(meta._ctx), _meta(std::move(meta)) {}
    Array(Context &ctx) : _ctx(ctx), _meta(_ctx) {}
    Array(Context &ctx, const std::string &uri) : _ctx(ctx), _meta(_ctx, uri) {}
    Array(const Array&) = delete;
    Array(Array&&) = default;
    Array &operator=(const Array&) = delete;
    Array &operator=(Array&&) = default;

    const std::string &uri() const {
      return _meta._uri;
    }

    bool good() const {
      return !uri().empty();
    }

    void load(const std::string &uri) {
      _meta.load(uri);
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
std::ostream &operator<<(std::ostream &os, const tdb::ArrayMetadata &);


#endif //TILEDB_GENOMICS_ARRAY_H
