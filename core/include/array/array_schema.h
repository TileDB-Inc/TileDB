/**
 * @file   array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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

#ifndef __ARRAY_SCHEMA_H__
#define __ARRAY_SCHEMA_H__

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

/**@{*/
/** Special key dimension name. */
#define TILEDB_AS_KEY_DIM1_NAME "__key_dim_1"
#define TILEDB_AS_KEY_DIM2_NAME "__key_dim_2"
#define TILEDB_AS_KEY_DIM3_NAME "__key_dim_3"
#define TILEDB_AS_KEY_DIM4_NAME "__key_dim_4"
/**@}*/

/**@{*/
/** Return code. */
#define TILEDB_AS_OK                       0
#define TILEDB_AS_ERR                     -1
/**@}*/

/** Default parameters. */
#define TILEDB_AS_CAPACITY             10000

/** Default error message. */
#define TILEDB_AS_ERRMSG std::string("[TileDB::ArraySchema] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_as_errmsg;




/** Specifies the array schema. */
class ArraySchema {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ArraySchema();
  
  /** Destructor. */
  ~ArraySchema();  




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Returns the array name. */
  const std::string& array_name() const;

  /** Exports the array schema into the input array schema struct. */
  void array_schema_export(ArraySchemaC* array_schema_c) const;

  /** Exports the array schema into the input metadata schema struct. */
  void array_schema_export(MetadataSchemaC* metadata_schema_c) const;

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

  /** Returns the number of values per cell of the input attribute. */
  int cell_val_num(int attribute_id) const;

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

  /** 
   * Returns true if the input range is contained fully in a single
   * column of tiles. 
   */
  bool is_contained_in_tile_slab_col(const void* range) const;

  /** 
   * Returns true if the input range is contained fully in a single
   * column of tiles. 
   *
   * @tparam T The coordinates type.
   * @param range The input range.
   * @return True if the input range is contained fully in a single
   *     column of tiles. 
   */
  template<class T>
  bool is_contained_in_tile_slab_col(const T* range) const;

  /** 
   * Returns true if the input range is contained fully in a single
   * row of tiles. 
   */
  bool is_contained_in_tile_slab_row(const void* range) const;

  /** 
   * Returns true if the input range is contained fully in a single
   * row of tiles. 
   *
   * @tparam T The coordinates type.
   * @param range The input range.
   * @return True if the input range is contained fully in a single
   *     row of tiles. 
   */
  template<class T>
  bool is_contained_in_tile_slab_row(const T* range) const;

  /** Prints information about the array schema to stdout. */
  void print() const;

  /**
   * Serializes the array schema object into a binary buffer.
   *
   * @param array_schema_bin The binary buffer to be created and populated
   *     by the function with the object data. Note that the caller is
   *     responsible for releasing this buffer afterwards.
   * @param array_schema_bin_size The size of the created binary buffer.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int serialize(void*& array_schema_bin, size_t& array_schema_bin_size) const;

  /**
   * Returns the type of overlap of the input subarrays.
   *
   * @tparam T The types of the subarrays.
   * @param subarray_a The first input subarray.
   * @param subarray_b The second input subarray.
   * @param overlap_subarray The overlap area between *subarray_a* and
   *     *subarray_b*.
   * @return The type of overlap, which can be one of the following:
   *    - 0: No overlap
   *    - 1: *subarray_a* fully covers *subarray_b*
   *    - 2: Partial overlap (non-contig)
   *    - 3: Partial overlap (contig)
   */
  template<class T>
  int subarray_overlap(
      const T* subarray_a, 
      const T* subarray_b, 
      T* overlap_subarray) const;

  /** Returns the tile domain. */
  const void* tile_domain() const;

  /** Returns the tile extents. */
  const void* tile_extents() const;

  /** 
   * Returns the number of tiles in the array domain (applicable only to dense
   * arrays). 
   */
  int64_t tile_num() const;

  /** 
   * Returns the number of tiles in the array domain (applicable only to dense
   * arrays). 
   *
   * @tparam T The coordinates type.
   * @return The number of tiles.
   */
  template<class T>
  int64_t tile_num() const;

  /** 
   * Returns the number of tiles overlapping with the input range 
   * (applicable only to dense arrays). 
   */
  int64_t tile_num(const void* range) const;

  /** 
   * Returns the number of tiles in the input domain (applicable only to dense
   * arrays). 
   *
   * @tparam T The coordinates type.
   * @param domain The input domain.
   * @return The number of tiles.
   */
  template<class T>
  int64_t tile_num(const T* domain) const;

  /** Returns the tile order. */
  int tile_order() const;

  /** Return the number of cells in a column tile slab of an input subarray. */
  int64_t tile_slab_col_cell_num(const void* subarray) const;

  /** Return the number of cells in a row tile slab of an input subarray. */
  int64_t tile_slab_row_cell_num(const void* subarray) const;

  /** Returns the type of the i-th attribute, or NULL if 'i' is invalid. */
  int type(int i) const;

  /** Returns the type size of the i-th attribute. */
  size_t type_size(int i) const;

  /** Returns the number of attributes with variable-sized values. */
  int var_attribute_num() const;

  /** Returns *true* if the indicated attribute has variable-sized values. */
  bool var_size(int attribute_id) const;




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

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
   * input C-style ArraySchemaC struct.
   *
   * @param array_schema_c The array schema in a C-style struct.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */ 
  int init(const ArraySchemaC* array_schema_c);  

  /** 
   * Initializes the ArraySchema object using the information provided in the
   * inpur C-style MetadataSchemaC struct.
   *
   * @param metadata_schema_c The metadata schema in a C-style struct.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int init(const MetadataSchemaC* metadata_schema_c);  

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
  int set_attributes(char** attributes, int attribute_num);

  /** Sets the tile capacity. */
  void set_capacity(int64_t capacity);

  /** Sets the number of cell values per attribute. */
  void set_cell_val_num(const int* cell_val_num);

  /** 
   * Sets the cell order. Supported cell orders: 
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   *    - TILEDB_HILBSERT
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
   * Sets the tile order. Supported tile orders. 
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   *    - TILEDB_HILBERT
   *
   * @param tile_order The tile order.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   */
  int set_tile_order(int tile_order);

  /** 
   * Sets the types. There should be one type per attribute plus one (the last
   * one) for the coordinates. 
   * The supported types for the attributes are:
   *     - TILEDB_CHAR
   *     - TILEDB_INT32
   *     - TILEDB_INT64
   *     - TILEDB_FLOAT32
   *     - TILEDB_FLOAT64
   *     - TILEDB_INT8
   *     - TILEDB_UINT8
   *     - TILEDB_INT16
   *     - TILEDB_UINT16
   *     - TILEDB_UINT32
   *     - TILEDB_UINT64
   *
   * The supported types for the coordinates are:
   *     - TILEDB_INT32
   *     - TILEDB_INT64
   *     - TILEDB_FLOAT32
   *     - TILEDB_FLOAT64
   *     - TILEDB_INT8
   *     - TILEDB_UINT8
   *     - TILEDB_INT16
   *     - TILEDB_UINT16
   *     - TILEDB_UINT32
   *     - TILEDB_UINT64
   *
   * @param types The types.
   * @return TILEDB_AS_OK for success, and TILEDB_AS_ERR for error.
   * 
   * @note The attributes, dimensions and the dense flag must have already been
   *     set before calling this function.
   */
  int set_types(const int* types);




  /* ********************************* */
  /*               MISC                */
  /* ********************************* */

  /**
   * Checks the cell order of the input coordinates. Note that, in the presence
   * of a regular tile grid, this function assumes that the cells are in the
   * same regular tile.
   *
   * @tparam T The coordinates type.
   * @param coords_a The first input coordinates.
   * @param coords_b The second input coordinates.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second
   *    -  0 if the two coordinates are identical
   *    - +1 if the first coordinates succeed the second
   */
  template<class T>
  int cell_order_cmp(const T* coords_a, const T* coords_b) const;

  /** 
   * Expands the input domain such that it coincides with the boundaries of
   * the array's regular tiles (i.e., it maps it on the regular tile grid).
   * If the array has no regular tile grid, the function does not do anything.
   *
   * @param domain The domain to be expanded.
   * @return void
   */
  void expand_domain(void* domain) const;

  /** 
   * Expands the input domain such that it coincides with the boundaries of
   * the array's regular tiles (i.e., it maps it on the regular tile grid).
   * If the array has no regular tile grid, the function does not do anything.
   *
   * @tparam The domain type.
   * @param domain The domain to be expanded.
   * @return void
   */
  template<class T>
  void expand_domain(T* domain) const;

  /**
   * Returns the position of the input coordinates inside its corresponding
   * tile, based on the array cell order. Applicable only to **dense** arrays. 
   * 
   * @tparam T The coordinates type.
   * @param coords The input coordindates, which are expressed as global 
   *     coordinates in the array domain.
   * @return The position of the cell coordinates in the array cell order
   *     within its corresponding tile. In case of error, the function returns
   *     TILEDB_AS_ERR.
   */
  template<class T>
  int64_t get_cell_pos(const T* coords) const;

  /**
   * Retrieves the next coordinates along the array cell order within a given
   * domain (desregarding whether the domain is split into tiles or not). 
   * Applicable only to **dense** arrays.  
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the next coordinates at termination.
   * @param coords_retrieved Will store true if the retrieved coordinates are
   *     inside the domain, and false otherwise.
   * @return void
   */
  template<class T> 
  void get_next_cell_coords(
      const T* domain, 
      T* cell_coords, 
      bool& coords_retrieved) const;

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays.  
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @return void
   */
  template<class T> 
  void get_next_tile_coords(const T* domain, T* tile_coords) const;

  /**
   * Retrieves the previous coordinates along the array cell order within a 
   * given domain (desregarding whether the domain is split into tiles or not). 
   * Applicable only to **dense** arrays.  
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the previous coordinates at termination.
   * @return void
   */
  template<class T> 
  void get_previous_cell_coords(const T* domain, T* cell_coords) const;

  /**
   * Gets a subarray of tile coordinates for the input (cell) subarray
   * over the input array domain. Retrieves also the tile domain of
   * the array..
   *
   * @tparam T The domain type.
   * @param subarray The input (cell) subarray.
   * @param tile_domain The array tile domain to be retrieved. 
   * @param subarray_in_tile_domain The output (tile) subarray.
   * @return void
   */
  template<class T>
  void get_subarray_tile_domain(
      const T* subarray,
      T* tile_domain,
      T* subarray_in_tile_domain) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays.
   * 
   * @tparam T The domain type.
   * @param tile_coords The tile coordinates. 
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the array domain, or TILEDB_AS_ERR on error.
   */
  template<class T> 
  int64_t get_tile_pos(const T* tile_coords) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays.
   * 
   * @tparam T The domain type.
   * @param domain The input domain, which is a cell domain partitioned into 
   *     regular tiles in the same manner as that of the array domain (however 
   *     *domain* may be a sub-domain of the array domain).
   * @param tile_coords The tile coordinates. 
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the input domain, or TILEDB_AS_ERR on error.
   */
  template<class T> 
  int64_t get_tile_pos(
      const T* domain,
      const T* tile_coords) const;

  /**
   * Gets the tile subarray for the input tile coordinates.
   * 
   * @tparam T The coordinates type.
   * @param tile_coords The input tile coordinates.
   * @param tile_subarray The output tile subarray.
   * @return void.
   */
  template<class T>
  void get_tile_subarray(const T* tile_coords, T* tile_subarray) const;

  /** 
   * Returns the Hilbert id of the input coordinates. 
   *
   * @tparam T The coordinates type.
   * @param coords The coordinates for which the Hilbert id is computed.
   * @return The computed Hilbert id.
   */
  template<class T>
  int64_t hilbert_id(const T* coords) const;

  /**
   * Checks the order of the input coordinates. First the tile order is checked
   * (which, in case of non-regular tiles, is always the same), breaking the
   * tie by checking the cell order. 
   *
   * @tparam T The coordinates type.
   * @param coords_a The first input coordinates.
   * @param coords_b The second input coordinates.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second
   *    -  0 if the two coordinates are identical
   *    - +1 if the first coordinates succeed the second
   */
  template<class T>
  int tile_cell_order_cmp(const T* coords_a, const T* coords_b) const;

  /** 
   * Returns the id of the tile the input coordinates fall into. 
   * 
   * @tparam T The coordinates type.
   * @param cell_coords The input coordinates.
   * @return The computed tile id.
   */
  template<class T>
  int64_t tile_id(const T* cell_coords) const;

  /**
   * Checks the tile order of the input coordinates.  
   *
   * @tparam T The coordinates type.
   * @param coords_a The first input coordinates.
   * @param coords_b The second input coordinates.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinates succeed the second on the tile order
   */
  template<class T>
  int tile_order_cmp(const T* coords_a, const T* coords_b) const;



  /* ********************************* */
  /*        AUXILIARY ATTRIBUTES       */
  /* ********************************* */

  /** 
   * Auxiliary attribute used in the computation of tile ids, in order to avoid
   * repeated allocations and deallocations that impact performance.
   */
  void* tile_coords_aux_;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** 
   * The array name. It is a directory, whose parent must be a TileDB workspace,
   * or group.
   */
  std::string array_name_;
  /** The attribute names. */
  std::vector<std::string> attributes_;
  /** The number of attributes. */
  int attribute_num_;
  /** 
   * The tile capacity for the case of sparse fragments.
   */
  int64_t capacity_;
  /** The number of cells per tile. Meaningful only for the **dense** case. */
  int64_t cell_num_per_tile_;
  /** 
   * The cell order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   *    - TILEDB_HILBERT 
   */
  int cell_order_;
  /** Stores the size of every attribute (plus coordinates in the end). */
  std::vector<size_t> cell_sizes_;
  /**
   * Specifies the number of values per attribute for a cell. If it is NULL,
   * then each attribute has a single value per cell. If for some attribute
   * the number of values is variable (e.g., in the case off strings), then
   * TILEDB_VAR_NUM must be used.
   */
  std::vector<int> cell_val_num_;
  /** 
   * The compression type for each attribute (plus one extra at the end for the
   * coordinates. It can be one of the following: 
   *    - TILEDB_NO_COMPRESSION
   *    - TILEDB_GZIP
   *    - TILEDB_ZSTD 
   *    - TILEDB_LZ4 
   *    - TILEDB_BLOSC 
   *    - TILEDB_BLOSC_LZ4 
   *    - TILEDB_BLOSC_LZ4HC 
   *    - TILEDB_BLOSC_SNAPPY 
   *    - TILEDB_BLOSC_ZLIB 
   *    - TILEDB_BLOSC_ZSTD 
   *    - TILEDB_RLE 
   *    - TILEDB_BZIP2 
   */
  std::vector<int> compression_;
  /** Auxiliary variable used when calculating Hilbert ids. */
  int* coords_for_hilbert_;
  /** The size (in bytes) of the coordinates. */
  size_t coords_size_;
  /** 
   * Specifies if the array is dense or sparse. If the array is dense, 
   * then the user must specify tile extents (see below).
   */
  bool dense_;
  /** The dimension names. */
  std::vector<std::string> dimensions_;
  /** The number of dimensions. */
  int dim_num_;
  /**  
   * The array domain. It should contain one [lower, upper] pair per dimension. 
   * The type of the values stored in this buffer should match the coordinates
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
   * The array domain. It should contain one [lower, upper] pair per dimension. 
   * The type of the values stored in this buffer should match the coordinates
   * type.
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
   * Offsets for calculating tile positions and ids for the column-major 
   * tile order. 
   */
  std::vector<int64_t> tile_offsets_col_;
  /**
   * Offsets for calculating tile positions and ids for the row-major 
   * tile order. 
   */
  std::vector<int64_t> tile_offsets_row_;
  /** 
   * The tile order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR 
   */
  int tile_order_;
  /** 
   * The attribute types, plus an extra one in the end for the coordinates.
   * The attribute type can be one of the following: 
   *    - TILEDB_INT32
   *    - TILEDB_INT64
   *    - TILEDB_FLOAT32
   *    - TILEDB_FLOAT64
   *    - TILEDB_CHAR 
   *    - TILEDB_INT8
   *    - TILEDB_UINT8
   *    - TILEDB_INT16
   *    - TILEDB_UINT16
   *    - TILEDB_UINT32
   *    - TILEDB_UINT64
   *
   * The coordinate type can be one of the following: 
   *    - TILEDB_INT32
   *    - TILEDB_INT64
   *    - TILEDB_FLOAT32
   *    - TILEDB_FLOAT64
   *    - TILEDB_INT8
   *    - TILEDB_UINT8
   *    - TILEDB_INT16
   *    - TILEDB_UINT16
   *    - TILEDB_UINT32
   *    - TILEDB_UINT64
   */
  std::vector<int> types_;
  /** Stores the size of every attribute type (plus coordinates in the end). */
  std::vector<size_t> type_sizes_;




  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** 
   * Computes and returns the size of the binary representation of the
   * ArraySchema object. 
   */  
  size_t compute_bin_size() const;

  /** 
   * Compute the number of cells per tile. Meaningful only for the **dense**
   * case.
   *
   * @return void
   */
  void compute_cell_num_per_tile();

  /** 
   * Compute the number of cells per tile. Meaningful only for the **dense**
   * case.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template<class T>
  void compute_cell_num_per_tile();

  /** Computes and returns the size of an attribute (or coordinates). */
  size_t compute_cell_size(int attribute_id) const;

  /** 
   * Computes the number of bits per dimension required by the Hilbert curve. 
   *
   * @tparam T The domain type.
   * @return void
   */
  template<class T>
  void compute_hilbert_bits();

  /**
   * Computes the tile domain. Applicable only to arrays with regular tiles. 
   *
   * @return void
   */
  void compute_tile_domain();

  /**
   * Computes tile offsets neccessary when computing tile positions and ids.
   *
   * @return void 
   */
  void compute_tile_offsets();

  /**
   * Computes tile offsets neccessary when computing tile positions and ids.
   *
   * @tparam T The coordinates type.
   * @return void 
   */
  template<class T>
  void compute_tile_offsets();

  /** 
   * Computes the tile domain. Applicable only to arrays with regular tiles.
   *
   * @tparam T The domain type.
   * @return void
   */
  template<class T>
  void compute_tile_domain();

  /** Computes and returns the size of a type. */
  size_t compute_type_size(int attribute_id) const;

  /**
   * Returns the position of the input coordinates inside its corresponding
   * tile, based on the array cell order. Applicable only to **dense** arrays,
   * and focusing on the **column-major** cell order. 
   * 
   * @tparam T The coordinates type.
   * @param coords The input coordindates, which are expressed as global 
   *     coordinates in the array domain.
   * @return The position of the cell coordinates in the array cell order
   *     within its corresponding tile. In case of error, the function returns
   *     TILEDB_AS_ERR.
   */
  template<class T>
  int64_t get_cell_pos_col(const T* coords) const;

  /**
   * Returns the position of the input coordinates inside its corresponding
   * tile, based on the array cell order. Applicable only to **dense** arrays,
   * and focusing on the **row-major** cell order. 
   * 
   * @tparam T The coordinates type.
   * @param coords The input coordindates, which are expressed as global 
   *     coordinates in the array domain.
   * @return The position of the cell coordinates in the array cell order
   *     within its corresponding tile. In case of error, the function returns
   *     TILEDB_AS_ERR.
   */
  template<class T>
  int64_t get_cell_pos_row(const T* coords) const;

  /**
   * Retrieves the next coordinates along the array cell order within a given
   * domain (desregarding whether the domain is split into tiles or not). 
   * Applicable only to **dense** arrays, and focusing on **column-major** 
   * cell order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the next coordinates at termination.
   * @param coords_retrieved Will store true if the retrieved coordinates are
   *     inside the domain, and false otherwise.
   * @return void
   */
  template<class T> 
  void get_next_cell_coords_col(
      const T* domain, 
      T* cell_coords, 
      bool& coords_retrieved) const;

  /**
   * Retrieves the next coordinates along the array cell order within a given
   * domain (desregarding whether the domain is split into tiles or not). 
   * Applicable only to **dense** arrays, and focusing on **row-major** 
   * cell order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the next coordinates at termination.
   * @param coords_retrieved Will store true if the retrieved coordinates are
   *     inside the domain, and false otherwise.
   * @return void
   */
  template<class T> 
  void get_next_cell_coords_row(
      const T* domain, 
      T* cell_coords, 
      bool& coords_retrieved) const;

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays, and focusing on
   * the **column-major** tile order. 
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @return void
   */
  template<class T> 
  void get_next_tile_coords_col(const T* domain, T* tile_coords) const;

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays, and focusing on
   * the **row-major** tile order. 
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @return void
   */
  template<class T> 
  void get_next_tile_coords_row(const T* domain, T* tile_coords) const;

  /**
   * Retrieves the previous coordinates along the array cell order within a 
   * given domain (desregarding whether the domain is split into tiles or not). 
   * Applicable only to **dense** arrays, and focusing on the **column-major**
   * cell order. 
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the previous coordinates at termination.
   * @return void
   */
  template<class T> 
  void get_previous_cell_coords_col(const T* domain, T* cell_coords) const;

  /**
   * Retrieves the previous coordinates along the array cell order within a 
   * given domain (desregarding whether the domain is split into tiles or not). 
   * Applicable only to **dense** arrays, and focusing on the **row-major**
   * cell order. 
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the previous coordinates at termination.
   * @return void
   */
  template<class T> 
  void get_previous_cell_coords_row(const T* domain, T* cell_coords) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays, and focusing on the 
   * **column-major** tile order.
   * 
   * @tparam T The domain type.
   * @param tile_coords The tile coordinates. 
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the array domain.
   */
  template<class T> 
  int64_t get_tile_pos_col(const T* tile_coords) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays, and focusing on the 
   * **column-major** tile order.
   * 
   * @tparam T The domain type.
   * @param domain The input domain, which is a cell domain partitioned into 
   *     regular tiles in the same manner as that of the array domain 
   *     (however *domain* may be a sub-domain of the array domain).
   * @param tile_coords The tile coordinates. 
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the input domain.
   */
  template<class T> 
  int64_t get_tile_pos_col(
      const T* domain,
      const T* tile_coords) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays, and focusing on the 
   * **row-major** tile order.
   * 
   * @tparam T The domain type.
   * @param tile_coords The tile coordinates. 
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the array domain.
   */
  template<class T> 
  int64_t get_tile_pos_row(const T* tile_coords) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays, and focusing on the 
   * **row-major** tile order.
   * 
   * @tparam T The domain type.
   * @param domain The input domain, which is a cell domain partitioned into 
   *     regular tiles in the same manner as that of the array domain (however 
   *     *domain* may be a sub-domain of the array domain).
   * @param tile_coords The tile coordinates. 
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the input domain.
   */
  template<class T> 
  int64_t get_tile_pos_row(
      const T* domain,
      const T* tile_coords) const;

  /** Initializes a Hilbert curve. */
  void init_hilbert_curve();

  /** Return the number of cells in a column tile slab of an input subarray. */
  template<class T>
  int64_t tile_slab_col_cell_num(const T* subarray) const;

  /** Return the number of cells in a row tile slab of an input subarray. */
  template<class T>
  int64_t tile_slab_row_cell_num(const T* subarray) const;
};

#endif
