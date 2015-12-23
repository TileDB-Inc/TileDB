/**
 * @file   array_schema.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file defines class ArraySchema. 
 */

#ifndef ARRAY_SCHEMA_H
#define ARRAY_SCHEMA_H

#include "array_schema_c.h"
#include <limits>
#include <string>
#include <typeinfo>
#include <vector>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

// Return codes.
#define TILEDB_AS_OK                                             0
#define TILEDB_AS_ERR                                           -1

// Default parameters.
#define TILEDB_AS_CAPACITY                                   10000
#define TILEDB_AS_CONSOLIDATION_STEP                             1
#define TILEDB_AS_CELL_ORDER                TILEDB_AS_CO_ROW_MAJOR
#define TILEDB_AS_TILE_ORDER                TILEDB_AS_TO_ROW_MAJOR

// Special value that indicates a variable-sized attribute.
#define TILEDB_AS_VAR_SIZE          std::numeric_limits<int>::max()

/** Specifies the array schema. */
class ArraySchema {
 public:
  // TYPE DEFINITIONS
  /** Supported compression types. */
  enum Compression {
      TILEDB_AS_CMP_NONE,
      TILEDB_AS_CMP_GZIP
  };
  /** Supported cell orders. */
  enum CellOrder {
      TILEDB_AS_CO_COLUMN_MAJOR, 
      TILEDB_AS_CO_HILBERT, 
      TILEDB_AS_CO_ROW_MAJOR
  };
  /** Supported tile orders (applicable only to regular tiles). */
  enum TileOrder {
      TILEDB_AS_TO_COLUMN_MAJOR, 
      TILEDB_AS_TO_HILBERT, 
      TILEDB_AS_TO_ROW_MAJOR
  };

  // CONSTRUCTORS & DESTRUCTORS

  /** Constructor. */
  ArraySchema();
  
  /** Destructor. */
  ~ArraySchema();  

  // ACCESSORS

  /** Returns the array name. */
  const std::string& array_name() const;

  /** Returns the name of the attribute with the input id. */
  const std::string& attribute(int attribute_id) const;

  /** 
   * Returns the id of the input attribute. 
   *
   * @param attribute The attribute name whose id will be retrieved.
   * @return The attribute id if the input attribute exists, and TILEDB_AS_ERR
   *     otherwise.
   */
  int attribute_id(const std::string& attribute) const;
  
  /** Returns the number of attributes. */
  int attribute_num() const;

  /** Returns the attributes. */
  const std::vector<std::string>& attributes() const;

  /** 
   * Returns the number of cells per tile. Meaningful only for the dense case.
   */
  int64_t cell_num_per_tile() const;

  /** Returns the size of cell on the input attribute. */
  size_t cell_size(int attribute_id) const;

  /** Returns the compression type of the attribute with the input id. */
  Compression compression(int attribute_id) const;

  /** Returns the coordinates size. */
  size_t coords_size() const;

  /** True if the array is dense. */
  bool dense() const;

  /**
   * Gets the ids of the input attributes.
   *
   * @param attributes The name of the attributes whose ids will be retrieved.
   * @param attribute_ids The ids that are retrieved by the function.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int get_attribute_ids(
      const std::vector<std::string>& attributes,
      std::vector<int>& attribute_ids) const;

  /** Prints information about the array schema to stdout. */
  void print() const;

  /*
   * Serializes the array schema object into a binary buffer.
   *
   * @param array_schema_bin The binary buffer to be created and populated
   *     by the function with the object data. Note that the caller is
   *     responsible for releasing this buffer afterwards.
   * @param array_schema_bin_size The size of the created binary buffer.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int serialize(void*& array_schema_bin, size_t& array_schema_bin_size) const;

  /** Returns the numver of attributes with variable-sized values. */
  int var_attribute_num() const;

  /** Returns true if the indicated attribute has variable-sized values. */
  bool var_size(int attribute_id) const;

  // MUTATORS

  /** 
   * It assigns values to the members of the object from the input buffer. 
   *
   * @param array_schema_bin The input binary buffer with the object data. 
   * @param array_schema_bin_size The size of the input binary buffer.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int deserialize(const void* array_schema_bin, size_t array_schema_bin_size);
 
  /** 
   * Initializes the ArraySchema object using the information provided in the
   * inpur C-style ArraySchemaC struct.
   *
   * @param array_schema_c The array schema in a C-style struct.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */ 
  int init(const ArraySchemaC* array_schema_c);  

  /** Sets the array name. */
  void set_array_name(const char* array_name);
 
  /** 
   * Sets attribute names. There should not be any duplicate names. Moreover,
   * there should not be an attribute with the same name as a dimension.
   *
   * @param attributes The attribute names.
   * @param attribute_num The number of attributes.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int set_attributes(const char** attributes, int attribute_num);

  /** Sets the tile capacity. */
  void set_capacity(int capacity);

  /** 
   * Sets the cell order. supported cell orders: "row-major", "column-major",
   * "hilbert". 
   *
   * @param cell_order The cell order.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int set_cell_order(const char* cell_order);

  /** Sets the compression types. */
  int set_compression(const char** compression);

  /** Sets the consolidation step. */
  void set_consolidation_step(int consolidation_step);

  /** Sets the proper flag to indicate if the array is dense. */
  void set_dense(int dense);

  /** 
   * Sets dimension names. There should not be any duplicate names. Moreover,
   * there should not be a dimension with the same name as an attribute.
   *
   * @param dimensions The dimension names.
   * @param dim_num The number of dimensions.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int set_dimensions(const char** dimensions, int dim_num);

  /**
   * Sets the domain. 
   *
   * @param domain The domain. It should contain one [lower, upper] pair per
   *     dimension. The type  of the values stored in this buffer should match
   *     the coordinates type.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   * 
   * @note The dimensions and types must already have been set before calling
   *     this function. 
   */
  int set_domain(const void* domain);

  /**
   * Sets the tile extents.
   *
   * @param tile_extents The tile extents (only applicable to regular tiles).
   *     There should be one value for each dimension. The type of the values
   *     stored in this buffer should match the coordinates type. If it is NULL,
   *     then it means that the array has irregular tiles (and, hence, it is
   *     sparse).
   *
   * @note The dimensions and types must already have been set prior to calling
   *     this function. Moreover, dense arrays must always have tile extents,
   *     whereas arrays defined as key-value stores must not have tile extents. 
   */
  int set_tile_extents(const void* tile_extents);

  /** 
   * Sets the tile order. supported tile orders: "row-major", "column-major",
   * "hilbert". 
   *
   * @param tile_order The tile order.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int set_tile_order(const char* tile_order);

  /** 
   * Sets the types. There should be one type per attribute plus one (the last
   * one) for the coordinates. The supported types for the attributes are 
   * **char**, **int32**, **int64**, **float32**, and **float64**. If one
   * attribute takes a fixed number N of values, string <b>":N"</b> must be
   * appended to the type. If an attribute takes a variable number of values,
   * string <b>":var"</b> must be appended to the type. The supported types
   * for the coordinates are <b>char:var</b>, **int32**, **int64**, **float32**,
   * and **float64**. If the array is dense, then only **int32** and **int64**
   * are admissible types. 
   *
   * @param types The types.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   * 
   * @note The attributes, dimensions and the dense flag must have already been
   *     set before calling this function.
   */
  int set_types(const char** types);

 private:
  // PRIVATE ATTRIBUTES

  /** 
   * The array name. It is a path to a directory, whose parent is a TileDB 
   * workspace, group or array.
   */
  std::string array_name_;
  /** The attribute names. */
  std::vector<std::string> attributes_;
  /** The number of attributes. */
  int attribute_num_;
  /** 
   * The tile capacity (only applicable to irregular tiles). If it is <=0,
   * TileDB will use its default.
   */
  int64_t capacity_;
  /** The number of cells per tile. Meaningful only for the dense case. */
  int64_t cell_num_per_tile_;
  /**
   * The cell order. The supported orders are TILEDB_AS_CO_ROW_MAJOR, 
   * TILEDB_AS_CO_COLUMN_MAJOR, and TILEDB_AS_CO_HILBERT.
   */
  CellOrder cell_order_;
  /** Stores the size of every attribute (plus coordinates in the end). */
  std::vector<size_t> cell_sizes_;
  /** 
   * The type of compression. The supported compression types are 
   * TILEDB_AS_CMP_NONE and TILEDB_AS_CMP_GZIP. The number of compression 
   * types given should be equal to the number of attributes, plus one (the 
   * last one) for the coordinates. 
   */
  std::vector<Compression> compression_;
  /** The consolidation step. */
  int consolidation_step_;
  /** 
   * True if the array is in dense format; false if the array is sparse. If the
   * array is dense, then the user must specify tile extents for regular tiles.
   */
  bool dense_;
  /** The dimension names. */
  std::vector<std::string> dimensions_;
  /** The number of dimensions. */
  int dim_num_;
  /**  
   * The domain. It should contain one [lower, upper] pair per dimension. The
   * type  of the values stored in this buffer should match the coordinates
   * type. 
   */
  void* domain_;
  /** 
   * True if the array is a key-value store, in which case the actual
   * coordinates type is <b>char:var</b>, but in fact the coordinates are
   * materialized as integers.
   */
  bool key_value_;
  /** 
   * The tile extents (only applicable to regular tiles). There should be one 
   * value for each dimension. The type of the values stored in this buffer
   * should match the coordinates type. If it is NULL, then it means that the
   * array has irregular tiles (and, hence, it is sparse).
   */
  void* tile_extents_;
  /**
   * The tile order. The supported orders are TILEDB_AS_TO_ROW_MAJOR, 
   * TILEDB_AS_TO_COLUMN_MAJOR, and TILEDB_AS_TO_HILBERT.
   */
  TileOrder tile_order_;
  /** 
   * The attribute and coordinate types. There should be one type per 
   * attribute plus one (the last one) for the coordinates. The supported types
   * for the attributes are **char**, **int**, **int64_t**, **float**, and 
   * **double**. The supported types for the coordinates are **int**, 
   * **int64_t**, **float**, and **double**.
   */
  std::vector<const std::type_info*> types_;
  /** Stores the size of every attribute type (plus coordinates in the end). */
  std::vector<size_t> type_sizes_;
  /** 
   * The list of number of values per attribute per cell. Specifically, each
   * attribute may store more than one values of the specified type.
   * Moreover, a cell may store a variable number of values for some 
   * attribute. This is indicated by special value TILEDB_AS_VAR_SIZE. 
   */
  std::vector<int> val_num_;
  /** Number of attributes with variable-sized values. */
  int var_attribute_num_;

  // PRIVATE METHODS

  /** 
   * Computes and returns the size of the binary representation of the object. 
   */  
  size_t compute_bin_size() const;

  /** 
   * Compute the number of cells per tile. Meaningful only for the dense case.
   */
  void compute_cell_num_per_tile();

  /** Computes and returns the size of an attribute (or coordinates). */
  size_t compute_cell_size(int attribute_id) const;

  /** Computes and returns the size of a type. */
  size_t compute_type_size(int attribute_id) const;
};

#endif
