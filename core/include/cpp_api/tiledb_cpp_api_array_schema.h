/**
 * @file  tiledb_cpp_api_array_schema.h
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
 * This file declares the C++ API for the TileDB ArraySchema object.
 */

#ifndef TILEDB_CPP_API_ARRAY_SCHEMA_H
#define TILEDB_CPP_API_ARRAY_SCHEMA_H

#include "tiledb.h"
#include "tiledb_cpp_api_attribute.h"
#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_deleter.h"
#include "tiledb_cpp_api_domain.h"
#include "tiledb_cpp_api_object.h"

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

namespace tdb {

/**
 * Specifies a schema for an Array.
 */
class ArraySchema {
 public:
  ArraySchema(Context &ctx)
      : _ctx(ctx)
      , _deleter(ctx) {
    create();
  };

  /**
   * Load schema given a C API pointer. The class takes ownership of the
   * pointer.
   *
   * @param ctx context
   * @param schema schema pointer
   */
  ArraySchema(Context &ctx, tiledb_array_schema_t **schema)
      : ArraySchema(ctx) {
    if (schema && *schema) {
      _init(*schema);
      *schema = nullptr;
    }
  };

  /**
   * Load the schema of an existing array.
   *
   * @param ctx context
   * @param uri Name of array to load the schema for.
   */
  ArraySchema(Context &ctx, const std::string &uri)
      : ArraySchema(ctx) {
    _init(uri);
  }
  ArraySchema(const ArraySchema &) = default;
  ArraySchema(ArraySchema &&o) = default;
  ArraySchema &operator=(const ArraySchema &) = default;
  ArraySchema &operator=(ArraySchema &&o) = default;

  /**
   * Load array schema given an array path.
   *
   * @param uri
   */
  void load(const std::string &uri) {
    _init(uri);
  }

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_array_schema_t *() const;

  /** Returns the input array type in string format. */
  static std::string to_str(tiledb_array_type_t type);
  static std::string to_str(tiledb_layout_t layout);

  /**
   * Create new schema for an array with name uri
   *
   * @param uri
   * @return *this
   */
  ArraySchema &create();

  /**
   * Dump an ASCII repr of the array.
   */
  void dump(FILE *out = stdout) const;

  /**
   * @return Array type (Dense, Sparse)
   */
  tiledb_array_type_t type() const;

  /**
   * Set the type of array.
   *
   * @param type DENSE or SPARSE array
   */
  ArraySchema &set_type(tiledb_array_type_t type) {
    auto &ctx = _ctx.get();
    ctx.handle_error(
        tiledb_array_schema_set_array_type(ctx, _schema.get(), type));
    return *this;
  }

  /**
   * For a sparse array, get the number of cells per tile.
   *
   * @return Cells per tile
   */
  uint64_t capacity() const;

  /**
   * For a sparse array, set the number of cells per tile.
   *
   * @param capacity
   */
  ArraySchema &set_capacity(uint64_t capacity);

  /**
   * @return Current tile order
   */
  tiledb_layout_t tile_order() const;

  /**
   * Set order of tile compression.
   *
   * @param layout Tile layout
   */
  ArraySchema &set_tile_order(tiledb_layout_t layout);

  /**
   * Set both the tile and cell layouts
   *
   * @param {Tile layout, Cell layout}
   */
  ArraySchema &set_order(const std::array<tiledb_layout_t, 2> &p);

  /**
   * @return Current cell layout.
   */
  tiledb_layout_t cell_order() const;

  /**
   * @param layout Cell layout
   */
  ArraySchema &set_cell_order(tiledb_layout_t layout);

  /**
   * @return Current compression scheme and level
   */
  Compressor coord_compressor() const;

  /**
   * Set the compressor for coordinates
   * @param c Compressor scheme and level
   */
  ArraySchema &set_coord_compressor(const Compressor c);

  /**
   * @return Compressor scheme for offsets
   */
  Compressor offset_compressor() const;

  /**
   * @param c Compressor scheme for offsets
   */
  ArraySchema &set_offset_compressor(const Compressor c);

  /**
   * Domain of array. This is a set of dimensions.
   * @return Domain
   */
  Domain domain() const;

  /**
   * @param domain Configure array with the given domain.
   */
  ArraySchema &set_domain(const Domain &domain);

  /**
   * Add a cell attribute to the array.
   * @param attr
   */
  ArraySchema &add_attribute(const Attribute &attr);

  /**
   * Mark the array as a key value store.
   */
  ArraySchema &set_kv();

  tiledb_array_schema_t *get() {
    return _schema.get();
  }

  /**
   * @return Check if the array is a key value store.
   */
  bool is_kv() const;

  /**
   * Validate schema. The context error handler will be triggered on failure.
   */
  void check() const;

  /**
   * Get all attributes in the array.
   * @return map of attr names to Attributes
   */
  const std::unordered_map<std::string, Attribute> attributes() const;

  /**
   * @return True if an underlying tiledb_schema object exists.
   */
  bool good() const;

  /**
   * @return shared_ptr to underlying schema.
   */
  std::shared_ptr<tiledb_array_schema_t> ptr() const;

  /**
   * @return The underlying context
   */
  std::reference_wrapper<Context> context();

 private:
  void _init(tiledb_array_schema_t *schema);
  void _init(const std::string &uri);

  std::reference_wrapper<Context> _ctx;
  impl::Deleter _deleter;
  std::shared_ptr<tiledb_array_schema_t> _schema;
};

/**
 * Convert the schema into a str repr
 *
 * @param os output stream
 * @param schema array schema
 * @return os
 */
std::ostream &operator<<(std::ostream &os, const ArraySchema &schema);

/**
 * Add a domain to the array schema.
 *
 * @param schema
 * @param dim
 */
tdb::ArraySchema &operator<<(ArraySchema &schema, const Domain &domain);

/**
 * Add a attribute to the array schema.
 *
 * @param schema
 * @param attr
 */
tdb::ArraySchema &operator<<(ArraySchema &schema, const Attribute &attr);

/**
 * Set the array type.
 *
 * @param schema
 * @param type TILEDB_DENSE or TILEDB_SPARSE
 */
tdb::ArraySchema &operator<<(
    ArraySchema &schema, const tiledb_array_type_t type);

/**
 * Set the layout of the array.
 *
 * @param schema
 * @param p {Tile layout, Cell layout}
 */
tdb::ArraySchema &operator<<(
    ArraySchema &schema, const std::array<tiledb_layout_t, 2> p);

/**
 * Set the cell capacity for a sparse array.
 *
 * @param schema
 * @param capacity
 */
tdb::ArraySchema &operator<<(ArraySchema &schema, uint64_t capacity);

}  // namespace tdb

#endif  // TILEDB_CPP_API_ARRAY_SCHEMA_H
