/**
 * @file  tdbpp_arraymeta.h
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

#ifndef TILEDB_TDBPP_ARRAYMETA_H
#define TILEDB_TDBPP_ARRAYMETA_H

#include "tiledb.h"
#include "tdbpp_object.h"
#include "tdbpp_attribute.h"
#include "tdbpp_domain.h"
#include "tdbpp_context.h"

#include <memory>
#include <unordered_map>
#include <functional>
#include <vector>

namespace tdb {

  /**
   * Specifies the configuration that defines an Array.
   */
  class ArrayMetadata {
  public:
    ArrayMetadata(Context &ctx) : _ctx(ctx), _deleter(ctx) {};
    /**
     * Load metadata given a C API pointer. The class takes ownership of the pointer.
     * @param ctx context
     * @param meta metadata pointer
     */
    ArrayMetadata(Context &ctx, tiledb_array_metadata_t **meta) : ArrayMetadata(ctx) {
      if (meta && *meta) {
        _init(*meta);
        *meta = nullptr;
      }
    };
    /**
     * @param ctx context
     * @param uri Name of array to load the metadata for.
     */
    ArrayMetadata(Context &ctx, const std::string &uri) : ArrayMetadata(ctx) {
      _init(uri);
    }
    ArrayMetadata(const ArrayMetadata&) = default;
    ArrayMetadata(ArrayMetadata&& o) = default;
    ArrayMetadata &operator=(const ArrayMetadata&) = default;
    ArrayMetadata &operator=(ArrayMetadata &&o) = default;

    /**
     * Load array metadata given an array path.
     * @param uri
     */
    void load(const std::string &uri) {
      _init(uri);
    }

    /**
     * Create new metadata for an array with name uri
     * @param uri
     * @return *this
     */
    ArrayMetadata &create(const std::string &uri);

    std::string to_str() const;

    /**
     * @return Array type (ex. Dense, Sparse, kv_
     */
    tiledb_array_type_t type() const;

    ArrayMetadata &set_type(tiledb_array_type_t type) {
      auto &ctx = _ctx.get();
      ctx.handle_error(tiledb_array_metadata_set_array_type(ctx, _meta.get(), type));
      return *this;
    }

    uint64_t capacity() const;

    ArrayMetadata &set_capacity(uint64_t capacity);

    tiledb_layout_t tile_order() const;

    ArrayMetadata &set_tile_order(tiledb_layout_t layout);

    ArrayMetadata &set_order(const std::array<tiledb_layout_t, 2> &p);

    tiledb_layout_t cell_order() const;

    ArrayMetadata &set_cell_order(tiledb_layout_t layout);

    Compressor coord_compressor() const;

    ArrayMetadata &set_coord_compressor(const Compressor c);

    Compressor offset_compressor() const;

    ArrayMetadata &set_offset_compressor(const Compressor c);

    std::string name() const;

    Domain domain() const;

    ArrayMetadata &set_domain(const Domain &domain);

    ArrayMetadata &add_attribute(const Attribute &attr);

    ArrayMetadata &set_kv();

    tiledb_array_metadata_t *get() {
      return _meta.get();
    }

    bool is_kv() const;

    /**
     * Validate metadata. The context error handler will be triggered on failure.
     */
    void check() const;

    const std::unordered_map<std::string, Attribute> attributes() const;

    bool good() const;
    std::shared_ptr<tiledb_array_metadata_t> ptr() const;

    std::reference_wrapper<Context> context();

  private:
    friend class Array;

    void _init(tiledb_array_metadata_t* meta);
    void _init(const std::string &uri);

    struct _Deleter {
      _Deleter(Context& ctx) : _ctx(ctx) {}
      _Deleter(const _Deleter&) = default;
      void operator()(tiledb_array_metadata_t *p);
    private:
      std::reference_wrapper<Context> _ctx;
    };

    std::reference_wrapper<Context> _ctx;
    _Deleter _deleter;
    std::shared_ptr<tiledb_array_metadata_t> _meta;
  };
}

std::ostream &operator<<(std::ostream &os, const tdb::ArrayMetadata &meta);
tdb::ArrayMetadata &operator<<(tdb::ArrayMetadata &meta, const tdb::Domain &dim);
tdb::ArrayMetadata &operator<<(tdb::ArrayMetadata &meta, const tdb::Attribute &dim);
tdb::ArrayMetadata &operator<<(tdb::ArrayMetadata &meta, const tiledb_array_type_t type);
tdb::ArrayMetadata &operator<<(tdb::ArrayMetadata &meta, const std::array<tiledb_layout_t, 2> p);
tdb::ArrayMetadata &operator<<(tdb::ArrayMetadata &meta, uint64_t capacity);

#endif //TILEDB_TDBPP_ARRAYMETA_H
