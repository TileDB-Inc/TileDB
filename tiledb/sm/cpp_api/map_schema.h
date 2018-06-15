/**
 * @file  tiledb_cpp_api_map_schema.h
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
 * This file declares the C++ API for the TileDB MapSchema object.
 */

#ifndef TILEDB_CPP_API_MAP_SCHEMA_H
#define TILEDB_CPP_API_MAP_SCHEMA_H

#include "attribute.h"
#include "context.h"
#include "domain.h"
#include "object.h"
#include "schema_base.h"
#include "tiledb.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace tiledb {

/**
 * Represents the schema of a Map (key-value store).
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::MapSchema schema(ctx);
 * schema.add_attribute(Attribute::create<int>(ctx, "a1"));
 * schema.add_attribute(Attribute::create<std::string>(ctx, "a2"));
 * schema.add_attribute(Attribute::create<std::array<float, 2>>(ctx, "a3"));
 * // Create an empty map with the above schema.
 * Map::create("my_map", schema);
 * @endcode
 *
 */
class MapSchema : public Schema {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates a new empty map schema.
   *
   * @param ctx TileDB context
   */
  explicit MapSchema(const Context& ctx)
      : Schema(ctx) {
    tiledb_kv_schema_t* schema;
    ctx.handle_error(tiledb_kv_schema_alloc(ctx, &schema));
    schema_ = std::shared_ptr<tiledb_kv_schema_t>(schema, deleter_);
  }

  /**
   * Loads the schema of an existing array with the given URI.
   *
   * @param ctx TileDB context
   * @param uri URI of map to load
   */
  MapSchema(const Context& ctx, const std::string& uri)
      : Schema(ctx) {
    tiledb_kv_schema_t* schema;
    ctx.handle_error(tiledb_kv_schema_load(ctx, uri.c_str(), &schema));
    schema_ = std::shared_ptr<tiledb_kv_schema_t>(schema, deleter_);
  }

  /**
   * Loads the schema of an existing kv with the input C array
   * schema object.
   */
  MapSchema(const Context& ctx, tiledb_kv_schema_t* schema)
      : Schema(ctx) {
    schema_ = std::shared_ptr<tiledb_kv_schema_t>(schema, deleter_);
  }

  MapSchema(const MapSchema&) = default;
  MapSchema(MapSchema&& o) = default;
  MapSchema& operator=(const MapSchema&) = default;
  MapSchema& operator=(MapSchema&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_kv_schema_t*() const {
    return schema_.get();
  }

  /**
   * Dumps the map schema in an ASCII representation to an output.
   *
   * @param out (Optional) File to dump output to. Defaults to `stdout`.
   */
  void dump(FILE* out = stdout) const override {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_kv_schema_dump(ctx, schema_.get(), out));
  }

  /**
   * Adds an attribute to the schema.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::MapSchema schema(ctx);
   * schema.add_attribute(Attribute::create<int>(ctx, "a1"));
   * @endcode
   *
   * @param attr Attribute to add
   * @return Reference to this MapSchema.
   */
  MapSchema& add_attribute(const Attribute& attr) override {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_kv_schema_add_attribute(ctx, schema_.get(), attr.ptr().get()));
    return *this;
  }

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_kv_schema_t> ptr() const {
    return schema_;
  }

  /**
   * Validates the schema.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::MapSchema schema(ctx);
   * // Add attributes etc...
   *
   * try {
   *   schema.check();
   * } catch (const tiledb::TileDBError& e) {
   *   std::cout << e.what() << "\n";
   *   exit(1);
   * }
   * @endcode
   *
   * @throws TileDBError if the schema is incorrect or invalid.
   */
  void check() const override {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_kv_schema_check(ctx, schema_.get()));
  }

  /**
   * Gets all attributes in the array.
   *
   * @return Map of attribute name to copy of Attribute instance.
   */
  std::unordered_map<std::string, Attribute> attributes() const override {
    auto& ctx = ctx_.get();
    tiledb_attribute_t* attrptr;
    unsigned int nattr;
    std::unordered_map<std::string, Attribute> attrs;
    ctx.handle_error(
        tiledb_kv_schema_get_attribute_num(ctx, schema_.get(), &nattr));
    for (unsigned int i = 0; i < nattr; ++i) {
      ctx.handle_error(tiledb_kv_schema_get_attribute_from_index(
          ctx, schema_.get(), i, &attrptr));
      auto attr = Attribute(ctx_, attrptr);
      attrs.emplace(
          std::pair<std::string, Attribute>(attr.name(), std::move(attr)));
    }
    return attrs;
  }

  /** Returns the number of attributes in the schema. **/
  unsigned attribute_num() const override {
    auto& ctx = context();
    unsigned num;
    ctx.handle_error(
        tiledb_kv_schema_get_attribute_num(ctx, schema_.get(), &num));
    return num;
  }

  /**
   * Get a copy of an Attribute in the schema by name.
   *
   * @param name Name of attribute
   * @return Attribute
   */
  Attribute attribute(const std::string& name) const override {
    auto& ctx = ctx_.get();
    tiledb_attribute_t* attr;
    ctx.handle_error(tiledb_kv_schema_get_attribute_from_name(
        ctx, schema_.get(), name.c_str(), &attr));
    return {ctx, attr};
  }
  /**
   * Get a copy of an Attribute in the schema by index.
   * Attributes are ordered the same way they were defined
   * when constructing the schema.
   *
   * @param i Index of attribute
   * @return Attribute
   */
  Attribute attribute(unsigned int i) const override {
    auto& ctx = ctx_.get();
    tiledb_attribute_t* attr;
    ctx.handle_error(tiledb_kv_schema_get_attribute_from_index(
        ctx, schema_.get(), i, &attr));
    return {ctx, attr};
  }

  /**
   * Sets the tile capacity.
   *
   * @param capacity Capacity value to set.
   * @return Reference to this MapSchema.
   */
  MapSchema& set_capacity(uint64_t capacity) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_kv_schema_set_capacity(ctx, schema_.get(), capacity));
    return *this;
  }

  /** Returns the tile capacity. */
  uint64_t capacity() const {
    auto& ctx = ctx_.get();
    uint64_t capacity;
    ctx.handle_error(
        tiledb_kv_schema_get_capacity(ctx, schema_.get(), &capacity));
    return capacity;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The pointer to the C TileDB KV schema object. */
  std::shared_ptr<tiledb_kv_schema_t> schema_;
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Converts the array schema into a string representation. */
inline std::ostream& operator<<(std::ostream& os, const MapSchema& schema) {
  os << "MapSchema<Attributes:";
  for (const auto& a : schema.attributes()) {
    os << ' ' << a.second;
  }
  os << '>';
  return os;
}
}  // namespace tiledb

#endif  // TILEDB_CPP_API_MAP_SCHEMA_H
