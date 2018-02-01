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
#include "tiledb_cpp_api_domain.h"
#include "tiledb_cpp_api_object.h"
#include "tiledb_cpp_api_schema_base.h"

#include <memory>
#include <string>
#include <array>
#include <unordered_map>

namespace tdb {

/**
 * Schema describing an array. The schema describes all information
 * about an array including memory ordering, types, and compression
 * details.
 */
class ArraySchema : public Schema {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Creates a new array schema. */
  explicit ArraySchema(const Context &ctx, tiledb_array_type_t type);

  /** Loads the schema of an existing array with the input URI. */
  ArraySchema(const Context &ctx, const std::string &uri);

  ArraySchema(const ArraySchema &) = default;
  ArraySchema(ArraySchema &&o) = default;
  ArraySchema &operator=(const ArraySchema &) = default;
  ArraySchema &operator=(ArraySchema &&o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_array_schema_t *() const;

  /** Returns the input array type in string format. */
  static std::string to_str(tiledb_array_type_t type);

  /** Returns the input layout in string format. */
  static std::string to_str(tiledb_layout_t layout);

  /** Dumps the array schema in an ASCII representation to an output. */
  void dump(FILE *out = stdout) const override;

  /** Returns the array type. */
  tiledb_array_type_t array_type() const;

  /** Returns the tile capacity. */
  uint64_t capacity() const;

  /** Sets the tile capacity. */
  ArraySchema &set_capacity(uint64_t capacity);

  /** Returns the tile order. */
  tiledb_layout_t tile_order() const;

  /** Sets the tile order. */
  ArraySchema &set_tile_order(tiledb_layout_t layout);

  /**
   * Sets both the tile and cell layouts
   *
   * @param layout {Tile layout, Cell layout}
   */
  ArraySchema &set_order(const std::array<tiledb_layout_t, 2> &layout);

  /** Returns the cell order. */
  tiledb_layout_t cell_order() const;

  /** Sets the cell order. */
  ArraySchema &set_cell_order(tiledb_layout_t layout);

  /** Returns the compressor of the coordinates. */
  Compressor coord_compressor() const;

  /** Sets the compressor for the coordinates. */
  ArraySchema &set_coord_compressor(const Compressor &c);

  /** Returns the compressor of the offsets. */
  Compressor offset_compressor() const;

  /** Sets the compressor for the offsets. */
  ArraySchema &set_offset_compressor(const Compressor &c);

  /** Retruns the array domain of array. */
  Domain domain() const;

  /** Sets the array domain. */
  ArraySchema &set_domain(const Domain &domain);

  /** Adds an attribute to the array. */
  ArraySchema &add_attribute(const Attribute &attr) override;

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_array_schema_t> ptr() const;

  /** Validates the schema. */
  void check() const override;

  /** Gets all attributes in the array. */
  const std::unordered_map<std::string, Attribute> attributes() const override;

  /** Get an attribute by name. **/
  Attribute attribute(const std::string &name) const override;

  /** Number of attributes. **/
  unsigned num_attributes() const override;

  /** Get an attribute by index **/
  Attribute attribute(unsigned int i) const override;

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
std::ostream &operator<<(std::ostream &os, const ArraySchema &schema);

/** Adds a domain to the array schema. */
ArraySchema &operator<<(ArraySchema &schema, const Domain &domain);

/** Adds an attribute to the array schema. */
ArraySchema &operator<<(ArraySchema &schema, const Attribute &attr);

/**
 * Sets the tile and cell order of the array.
 * The input should be in the form: {Tile layout, Cell layout}.
 */
ArraySchema &operator<<(
    ArraySchema &schema, const std::array<tiledb_layout_t, 2> p);

/** Sets the tile capacity of the array. */
ArraySchema &operator<<(ArraySchema &schema, uint64_t capacity);

}  // namespace tdb

#endif  // TILEDB_CPP_API_ARRAY_SCHEMA_H
