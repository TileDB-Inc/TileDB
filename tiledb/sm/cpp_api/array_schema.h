/**
 * @file  tiledb_cpp_api_array_schema.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "attribute.h"
#include "domain.h"
#include "object.h"
#include "schema_base.h"
#include "tiledb.h"

#include <array>
#include <memory>
#include <string>
#include <unordered_map>

namespace tiledb {

/**
 * Schema describing an array.
 *
 * @details
 * The schema is an independent description of an array. A schema can be
 * used to create multiple array's, and stores information about its
 * domain, cell types, and compression details. An array schema is composed of:
 *
 * - A Domain
 * - A set of Attributes
 * - Memory layout definitions: tile and cell
 * - Compression details for Array level factors like offsets and coordinates
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::ArraySchema schema(ctx, TILEDB_SPARSE); // Or TILEDB_DENSE
 *
 * // Create a Domain
 * tiledb::Domain domain(...);
 *
 * // Create Attributes
 * auto a1 = tiledb::Attribute::create(...);
 *
 * schema.set_domain(domain);
 * schema.add_attribute(a1);
 *
 * // Specify tile memory layout
 * schema.set_tile_order(TILEDB_ROW_MAJOR);
 * // Specify cell memory layout within each tile
 * schema.set_cell_order(TILEDB_ROW_MAJOR);
 * schema.set_capacity(10); // For sparse, set capacity of each tile
 *
 * tiledb::Array::create(schema, "my_array"); // Make array with schema
 * auto s = tiledb::ArraySchema(ctx, "my_array"); // Load schema from array
 *
 * @endcode
 */
class ArraySchema : public Schema {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Creates a new array schema. */
  explicit ArraySchema(const Context& ctx, tiledb_array_type_t type)
      : Schema(ctx) {
    tiledb_array_schema_t* schema;
    ctx.handle_error(tiledb_array_schema_alloc(ctx, type, &schema));
    schema_ = std::shared_ptr<tiledb_array_schema_t>(schema, deleter_);
  };

  /** Loads the schema of an existing array with the input URI. */
  ArraySchema(const Context& ctx, const std::string& uri)
      : Schema(ctx) {
    tiledb_array_schema_t* schema;
    ctx.handle_error(tiledb_array_schema_load(ctx, uri.c_str(), &schema));
    schema_ = std::shared_ptr<tiledb_array_schema_t>(schema, deleter_);
  };

  /**
   * Loads the schema of an existing array with the input C array
   * schema object.
   */
  ArraySchema(const Context& ctx, tiledb_array_schema_t* schema)
      : Schema(ctx) {
    schema_ = std::shared_ptr<tiledb_array_schema_t>(schema, deleter_);
  };

  ArraySchema() = default;
  ArraySchema(const ArraySchema&) = default;
  ArraySchema(ArraySchema&& o) = default;
  ArraySchema& operator=(const ArraySchema&) = default;
  ArraySchema& operator=(ArraySchema&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_array_schema_t*() const {
    return schema_.get();
  }

  /** Dumps the array schema in an ASCII representation to an output. */
  void dump(FILE* out = stdout) const override {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_dump(ctx, schema_.get(), out));
  }

  /** Returns the array type. */
  tiledb_array_type_t array_type() const {
    auto& ctx = ctx_.get();
    tiledb_array_type_t type;
    ctx.handle_error(
        tiledb_array_schema_get_array_type(ctx, schema_.get(), &type));
    return type;
  }

  /** Returns the tile capacity. */
  uint64_t capacity() const {
    auto& ctx = ctx_.get();
    uint64_t capacity;
    ctx.handle_error(
        tiledb_array_schema_get_capacity(ctx, schema_.get(), &capacity));
    return capacity;
  }

  /** Sets the tile capacity. */
  ArraySchema& set_capacity(uint64_t capacity) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_array_schema_set_capacity(ctx, schema_.get(), capacity));
    return *this;
  }

  /** Returns the tile order. */
  tiledb_layout_t tile_order() const {
    auto& ctx = ctx_.get();
    tiledb_layout_t layout;
    ctx.handle_error(
        tiledb_array_schema_get_tile_order(ctx, schema_.get(), &layout));
    return layout;
  }

  /** Sets the tile order. */
  ArraySchema& set_tile_order(tiledb_layout_t layout) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_array_schema_set_tile_order(ctx, schema_.get(), layout));
    return *this;
  }

  /**
   * Sets both the tile and cell layouts
   *
   * @param layout {Tile layout, Cell layout}
   */
  ArraySchema& set_order(const std::array<tiledb_layout_t, 2>& p) {
    set_tile_order(p[0]);
    set_cell_order(p[1]);
    return *this;
  }

  /** Returns the cell order. */
  tiledb_layout_t cell_order() const {
    auto& ctx = ctx_.get();
    tiledb_layout_t layout;
    ctx.handle_error(
        tiledb_array_schema_get_cell_order(ctx, schema_.get(), &layout));
    return layout;
  }

  /** Sets the cell order. */
  ArraySchema& set_cell_order(tiledb_layout_t layout) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_array_schema_set_cell_order(ctx, schema_.get(), layout));
    return *this;
  }

  /** Returns the compressor of the coordinates. */
  Compressor coords_compressor() const {
    auto& ctx = ctx_.get();
    tiledb_compressor_t compressor;
    int level;
    ctx.handle_error(tiledb_array_schema_get_coords_compressor(
        ctx, schema_.get(), &compressor, &level));
    Compressor cmp(compressor, level);
    return cmp;
  }

  /** Sets the compressor for the coordinates. */
  ArraySchema& set_coords_compressor(const Compressor& c) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_set_coords_compressor(
        ctx, schema_.get(), c.compressor(), c.level()));
    return *this;
  }

  /** Returns the compressor of the offsets. */
  Compressor offsets_compressor() const {
    auto& ctx = ctx_.get();
    tiledb_compressor_t compressor;
    int level;
    ctx.handle_error(tiledb_array_schema_get_offsets_compressor(
        ctx, schema_.get(), &compressor, &level));
    Compressor cmp(compressor, level);
    return cmp;
  }

  /** Sets the compressor for the offsets. */
  ArraySchema& set_offsets_compressor(const Compressor& c) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_set_offsets_compressor(
        ctx, schema_.get(), c.compressor(), c.level()));
    return *this;
  }

  /** Retruns the array domain of array. */
  Domain domain() const {
    auto& ctx = ctx_.get();
    tiledb_domain_t* domain;
    ctx.handle_error(
        tiledb_array_schema_get_domain(ctx, schema_.get(), &domain));
    return Domain(ctx, domain);
  }

  /** Sets the array domain. */
  ArraySchema& set_domain(const Domain& domain) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_array_schema_set_domain(ctx, schema_.get(), domain.ptr().get()));
    return *this;
  }

  /** Adds an attribute to the array. */
  ArraySchema& add_attribute(const Attribute& attr) override {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_array_schema_add_attribute(ctx, schema_.get(), attr));
    return *this;
  }

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_array_schema_t> ptr() const {
    return schema_;
  }

  /** Validates the schema. */
  void check() const override {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_schema_check(ctx, schema_.get()));
  }

  /** Gets all attributes in the array. */
  std::unordered_map<std::string, Attribute> attributes() const override {
    auto& ctx = ctx_.get();
    tiledb_attribute_t* attrptr;
    unsigned int nattr;
    std::unordered_map<std::string, Attribute> attrs;
    ctx.handle_error(
        tiledb_array_schema_get_attribute_num(ctx, schema_.get(), &nattr));
    for (unsigned int i = 0; i < nattr; ++i) {
      ctx.handle_error(tiledb_array_schema_get_attribute_from_index(
          ctx, schema_.get(), i, &attrptr));
      auto attr = Attribute(ctx_, attrptr);
      attrs.emplace(
          std::pair<std::string, Attribute>(attr.name(), std::move(attr)));
    }
    return attrs;
  }

  /** Gets an attribute by name. **/
  Attribute attribute(const std::string& name) const override {
    auto& ctx = ctx_.get();
    tiledb_attribute_t* attr;
    ctx.handle_error(tiledb_array_schema_get_attribute_from_name(
        ctx, schema_.get(), name.c_str(), &attr));
    return Attribute(ctx, attr);
  }

  /** Number of attributes. **/
  unsigned attribute_num() const override {
    auto& ctx = ctx_.get();
    unsigned num;
    ctx.handle_error(
        tiledb_array_schema_get_attribute_num(ctx, schema_.get(), &num));
    return num;
  }

  /** Get an attribute by index **/
  Attribute attribute(unsigned int i) const override {
    auto& ctx = ctx_.get();
    tiledb_attribute_t* attr;
    ctx.handle_error(tiledb_array_schema_get_attribute_from_index(
        ctx, schema_.get(), i, &attr));
    return Attribute(ctx, attr);
  }

  /* ********************************* */
  /*         STATIC FUNCTIONS          */
  /* ********************************* */

  /** Returns the input array type in string format. */
  static std::string to_str(tiledb_array_type_t type) {
    return type == TILEDB_DENSE ? "DENSE" : "SPARSE";
  }

  /** Returns the input layout in string format. */
  static std::string to_str(tiledb_layout_t layout) {
    switch (layout) {
      case TILEDB_GLOBAL_ORDER:
        return "GLOBAL";
      case TILEDB_ROW_MAJOR:
        return "ROW-MAJOR";
      case TILEDB_COL_MAJOR:
        return "COL-MAJOR";
      case TILEDB_UNORDERED:
        return "UNORDERED";
    }
    return "";
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The pointer to the C TileDB array schema object. */
  std::shared_ptr<tiledb_array_schema_t> schema_;
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Converts the array schema into a string representation. */
inline std::ostream& operator<<(std::ostream& os, const ArraySchema& schema) {
  os << "ArraySchema<";
  os << tiledb::ArraySchema::to_str(schema.array_type());
  os << ' ' << schema.domain();
  for (const auto& a : schema.attributes()) {
    os << ' ' << a.second;
  }
  os << '>';
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ARRAY_SCHEMA_H
