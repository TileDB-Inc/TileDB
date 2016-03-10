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
#include "metadata_schema_c.h"
#include "hilbert_curve.h"
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

/** Specifies the array schema. */
class ArraySchema {
 public:
  // CONSTRUCTORS & DESTRUCTORS

  /** Constructor. */
  ArraySchema();
  
  /** Destructor. */
  ~ArraySchema();  

  // ACCESSORS

  // TODO
  void array_schema_export(ArraySchemaC* array_schema_c) const;

  // TODO
  void array_schema_export(MetadataSchemaC* metadata_schema_c) const;

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

  /** Returns the capacity. */
  int64_t capacity() const;

  /** 
   * Returns the number of cells per tile. Meaningful only for the dense case.
   */
  int64_t cell_num_per_tile() const;

  /** Returns the cell order. */
  int cell_order() const;

  /** Returns the size of cell on the input attribute. */
  size_t cell_size(int attribute_id) const;

  /** Returns the compression type of the attribute with the input id. */
  int compression(int attribute_id) const;

  /** Returns the coordinates size. */
  size_t coords_size() const;

  /** Returns the type of the coordinates. */
  int coords_type() const;

  /** True if the array is dense. */
  bool dense() const;

  /** Returns the number of dimensions. */
  int dim_num() const;

  /** Returns the domain. */
  const void* domain() const;

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

  /** Returns the tile domain. */
  const void* tile_domain() const;

  /** Returns the tile extents. */
  const void* tile_extents() const;

  /** Returns the number of tiles - applicable only to dense arrays. */
  int64_t tile_num() const;

  /** Returns the number of tiles - applicable only to dense arrays. */
  template<class T>
  int64_t tile_num() const;

  // TODO
  int64_t tile_num(const void* domain) const;

  // TODO
  template<class T>
  int64_t tile_num(const T* domain) const;


  /** Returns the type of the i-th attribute, or NULL if 'i' is invalid. */
  int type(int i) const;

  /** Returns true if the indicated attribute has variable-sized values. */
  bool var_size(int attribute_id) const;

  // MUTATORS

  /** Computes the number of bits required by the Hilbert curve. */
  template<class T>
  void compute_hilbert_bits();

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

  // TODO
  int init(const MetadataSchemaC* metadata_schema_c);  

  /** Initializes a Hilbert curve. */
  void init_hilbert_curve();

  /** Sets the array name. */
  void set_array_name(const char* array_name);
  
  // TODO
  void set_cell_val_num(const int* cell_val_num);
 
  /** 
   * Sets attribute names. There should not be any duplicate names. Moreover,
   * there should not be an attribute with the same name as a dimension.
   *
   * @param attributes The attribute names.
   * @param attribute_num The number of attributes.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int set_attributes(char** attributes, int attribute_num);

  /** Sets the tile capacity. */
  void set_capacity(int64_t capacity);

  /** 
   * Sets the cell order. supported cell orders: "row-major", "column-major",
   * "hilbert". 
   *
   * @param cell_order The cell order.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int set_cell_order(int cell_order);

  /** Sets the compression types. */
  int set_compression(int* compression);

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
  int set_dimensions(char** dimensions, int dim_num);

  /**
   * Sets the domain. 
   *
   * @param domain The domain. It should contain one [lower, upper] pair per
   *     dimension. Thie type  of the values stored in this buffer should match
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
  int set_tile_order(int tile_order);

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
  int set_types(int* types);

  // MISC

  // TODO
  template<class T>
  int cell_order_cmp_2(const T* coords_a, const T* coords_b) const;

  // TODO
  template<class T>
  int cell_order_cmp(const T* coords_a, const T* coords_b) const;

  // TODO
  template<class T>
  int tile_order_cmp(const T* coords_a, const T* coords_b) const;

  // TODO
  template<class T>
  T cell_num_in_range_slab(const T* range) const;

  // TODO
  template<class T>
  T cell_num_in_range_slab_col(const T* range) const;

  // TODO
  template<class T>
  T cell_num_in_range_slab_row(const T* range) const;

  // TODO
  template<class T>
  T cell_num_in_tile_slab() const;

  // TODO
  template<class T>
  T cell_num_in_tile_slab_col() const;

  // TODO
  template<class T>
  T cell_num_in_tile_slab_row() const;

  // TODO
  void expand_domain(void* domain) const;

  // TODO
  template<class T>
  void expand_domain(T* domain) const;

  // TODO
  template<class T>
  int64_t get_cell_pos(const T* range) const;

  // TODO
  template<class T>
  int64_t get_cell_pos_col(const T* range) const;

  // TODO
  template<class T>
  int64_t get_cell_pos_row(const T* range) const;

  // TODO
  template<class T> 
  void get_next_cell_coords(const T* domain, T* cell_coords) const;

  // TODO
  template<class T> 
  void get_next_cell_coords_col(const T* domain, T* cell_coords) const;

  // TODO
  template<class T> 
  void get_next_cell_coords_row(const T* domain, T* cell_coords) const;

  // TODO
  template<class T> 
  void get_previous_cell_coords(const T* domain, T* cell_coords) const;

  // TODO
  template<class T> 
  void get_previous_cell_coords_col(const T* domain, T* cell_coords) const;

  // TODO
  template<class T> 
  void get_previous_cell_coords_row(const T* domain, T* cell_coords) const;

  // TODO
  template<class T> 
  void get_next_tile_coords(const T* domain, T* tile_coords) const;

  // TODO
  template<class T> 
  void get_next_tile_coords_col(const T* domain, T* tile_coords) const;

  // TODO
  template<class T> 
  void get_next_tile_coords_row(const T* domain, T* tile_coords) const;

  // TODO
  template<class T> 
  int64_t get_tile_pos(
      const T* domain,
      const T* tile_coords) const;

  // TODO
  template<class T> 
  int64_t get_tile_pos_col(
      const T* domain,
      const T* tile_coords) const;

  // TODO
  template<class T> 
  int64_t get_tile_pos_row(
      const T* domain,
      const T* tile_coords) const;

  // TODO
  // Overlap value meaning:
  // 0: NONE
  // 1: FULL
  // 2: PARTIAL_NON_CONTIG
  // 3: PARTILA_CONTIG
  template<class T> 
  void compute_mbr_range_overlap(
      const T* range,
      const T* mbr,
      T* overlap_range,
      int& overlap) const;

  // TODO
  // Overlap value meaning:
  // 0: NONE
  // 1: FULL
  // 2: PARTIAL_NON_CONTIG
  // 3: PARTILA_CONTIG
  template<class T> 
  void compute_tile_range_overlap(
      const T* domain,
      const T* non_empty_domain,
      const T* range,
      const T* tile_coords,
      T* overlap_range,
      int& overlap) const;

  /** Returns the Hilbert id of the input coordinates. */
  template<class T>
  int64_t hilbert_id(const T* coords) const;

  /** Returns the id of the tile the coordinates fall into. */
  template<class T>
  int64_t tile_id(const T* cell_coords) const;

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
  int cell_order_;
  /** Stores the size of every attribute (plus coordinates in the end). */
  std::vector<size_t> cell_sizes_;
  // TODO
  std::vector<int> compression_;
  /** To be used when calculating Hilbert ids. */
  int* coords_for_hilbert_;
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
   * Number of bits used for the calculation of cell ids with the 
   * Hilbert curve. 
   */
  int hilbert_bits_;
  /** A Hilbert curve object for finding cell ids. */
  HilbertCurve* hilbert_curve_;
  /** 
   * The domain where each tile is a distinct set of coordinates. For instance,
   * if the array is 2D, the domain is (1,10), (1,10) and the tile extents are 2
   * and 5, then the tile domain is (0,4), (0,1), as there are 5 tiles across
   * the row dimension, and 2 tiles across the column dimension. Then, the tile
   * in the upper left corner has coordinates (0,0), the one on its right has
   * coordinates (0,1), and so on. Applicable only to arrays with regular tiles.
   */
  void* tile_domain_;
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
  int tile_order_;
  // TODO
  std::vector<int> types_;
  /** Stores the size of every attribute type (plus coordinates in the end). */
  std::vector<size_t> type_sizes_;
  /** 
   * The list of number of values per attribute per cell. Specifically, each
   * attribute may store more than one values of the specified type.
   * Moreover, a cell may store a variable number of values for some 
   * attribute. This is indicated by special value TILEDB_AS_VAR_SIZE. 
   */
  std::vector<int> val_num_;

  // PRIVATE METHODS

  /** 
   * Computes and returns the size of the binary representation of the object. 
   */  
  size_t compute_bin_size() const;

  /** 
   * Compute the number of cells per tile. Meaningful only for the dense case.
   */
  void compute_cell_num_per_tile();

  /** 
   * Compute the number of cells per tile. Meaningful only for the dense case.
   *
   * @template T The coordinates type.
   */
  template<class T>
  void compute_cell_num_per_tile();

  /** Computes and returns the size of an attribute (or coordinates). */
  size_t compute_cell_size(int attribute_id) const;

  /** Computes the tile domain. Applicable only to arrays with regular tiles. */
  void compute_tile_domain();

  /** 
   * Computes the tile domain. Applicable only to arrays with regular tiles.
   *
   * @template T The coordinates type.
   */
  template<class T>
  void compute_tile_domain();

  /** Computes and returns the size of a type. */
  size_t compute_type_size(int attribute_id) const;
};

#endif
