/**
 * @file   array_schema.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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

#include <vector>
#include <set>
#include <string>
#include <inttypes.h>
#include <typeinfo>

/** Default value for ArraySchema::capacity_. */
#define AS_CAPACITY 10000
/** Default value for ArraySchema::consolidation_step_. */
#define AS_CONSOLIDATION_STEP 1
/** Name for the extra attribute representing the array coordinates. */
#define AS_COORDINATE_TILE_NAME "__coords"

/**
 * Objects of this class store information about the schema of an array, and
 * derive information based on the schema. 
 *
 * An array consists of a set of cells. The location of a cell in the array is
 * determined by its coordinates in a muti-dimensional space. This space is
 * determined by the dimensions and their domains. Each cell can store
 * a set of attribute values. 
 *
 * The attributes and dimensions may have variable data types. The dimensions
 * collectively comprise the coordinates, which are treated as an extra 
 * attribute called AS_COORDINATE_TILE_NAME (whose value is specified 
 * at the top of this header file). If there are m attributes,
 * in the sequel we consider the coordinates as an extra (m+1)-th attribute.
 *
 * The cells are grouped into tiles. An array may have regular or irregular 
 * tiles. If the array has regular tiles, the (common and non-zero) extent 
 * of each tile on each dimension is stored in ArraySchema::tile_extents_. 
 * If the array has irregular tiles, ArraySchema::tile_extents_ is empty.
 */
class ArraySchema {
 public:
  // TYPE DEFINITIONS
  /** A vector of attribute ids. */
  typedef std::vector<int> AttributeIds;
  /** The cell data types (CHAR is currently not supported for coordinates). */ 
  enum CellType {CHAR, INT, INT64_T, FLOAT, DOUBLE};
  /** The cell order. */
  enum CellOrder {CO_COLUMN_MAJOR, CO_HILBERT, CO_ROW_MAJOR, CO_NONE};
  /** The compression type. */
  enum CompressionType {RLE, ZIP, LZ, NONE};
  /** The tile order (applicable only to regular tiles). */
  enum TileOrder {TO_COLUMN_MAJOR, TO_HILBERT, TO_ROW_MAJOR, TO_NONE};

  // CONSTRUCTORS
  /** Empty constructor. */
  ArraySchema() {}
  /** 
   * Simple constructor, used to create a schema with irregular tiles. 
   * If there are m attributes, types must have size m+1,
   * and include the type of (all) the dimensions in types[m].
   * Recall that the dimensions are collectively regarded as an
   * extra attribute.
   */	
  ArraySchema(const std::string& array_name,
              const std::vector<std::string>& attribute_names,
              const std::vector<std::string>& dim_names,
              const std::vector<std::pair<double, double> >& dim_domains,
              const std::vector<const std::type_info*>& types,
              CellOrder cell_order,
              int consolidation_step = AS_CONSOLIDATION_STEP,
              int64_t capacity = AS_CAPACITY);
  /**
   * Simple constructor, used to create a schema with regular tiles.
   * If there are m attributes, argument cell types must have size m+1,
   * and include the type of (all) the dimensions in types[m].
   * Recall that the dimensions are collectively regarded as an
   * extra attribute.
   */
  ArraySchema(const std::string& array_name,
              const std::vector<std::string>& attribute_names,
              const std::vector<std::string>& dim_names,
              const std::vector<std::pair< double, double> >& dim_domains,
              const std::vector<const std::type_info*>& types,
              TileOrder tile_order,
              const std::vector<double>& tile_extents,
              int consolidation_step = AS_CONSOLIDATION_STEP,
              int64_t capacity = AS_CAPACITY,
              CellOrder cell_order = CO_ROW_MAJOR);
  /** Empty destructor. */
  ~ArraySchema() {}

  // ACCESSORS
  /** Returns the array name. */
  const std::string& array_name() const { return array_name_; }
  /** Returns the name of the i-th attribute. */
  int attribute_id(const std::string& attribute_name) const;
  /** Returns the name of the i-th attribute. */
  const std::string& attribute_name(int i) const;
  /** 
   * Returns the number of attributes (excluding the extra coordinate 
   * attribute. 
   */
  int attribute_num() const { return attribute_num_; }
  /** Returns the tile capacity. */
  int64_t capacity() const { return capacity_; }
  /** Returns the tile order.  */
  CellOrder cell_order() const { return cell_order_; }
  /** 
   * Returns the size of an entire logical cell (coordinates and
   * and attributes).
   */
  size_t cell_size() const { return cell_size_; }
  /** Returns the cell size of the i-th attribute. */
  size_t cell_size(int i) const { return cell_sizes_[i]; }
  /** Returns the consolidation step. */
  int consolidation_step() const { return consolidation_step_; }
  /** Returns the number of dimensions. */
  int dim_num() const { return dim_num_; }
  /** Returns the domains. */
  const std::vector<std::pair< double, double> >& dim_domains() const 
      { return dim_domains_; } 
  /** 
   * It serializes the object into a buffer of bytes. It returns a pointer
   * to the buffer it creates, along with the size of the buffer.
   */
  std::pair<const char*, size_t> serialize() const;
  /** Returns the tile extents. */
  const std::vector<double>& tile_extents() const 
      { return tile_extents_; } 
  /** Returns the tile order.  */
  TileOrder tile_order() const { return tile_order_; }
  /** Returns the type of the i-th attribute. */
  const std::type_info* type(int i) const;
  
  // MUTATORS
  /** It assigns values to the members of the object from the input buffer. */
  void deserialize(const char* buffer, size_t buffer_size);
	
  // MISC
  /** 
   * Returns the cell id of the input coordinates, along the Hilbert 
   * space-filling curve.
   */
  int64_t cell_id_hilbert(const void* coords) const;
  /** 
   * Returns the cell id of the input coordinates, along the Hilbert 
   * space-filling curve.
   */
  template<typename T>
  int64_t cell_id_hilbert(const T* coords) const;
  /** Returns an identical schema assigning the input to the array name. */
  ArraySchema* clone(const std::string& array_name) const;
  /** Returns an identical schema with the input array name and cell order. */
  ArraySchema* clone(const std::string& array_name, CellOrder cell_order) const;
  /** Returns an identical schema assigning the input to the capacity. */
  ArraySchema* clone(int64_t capacity) const;
  /** 
   * Returns the schema of the result when joining the arrays with the
   * input schemas. The result array name is given in the third argument. 
   * Let the joining arrays be A,B and the result be C.
   * 
   * 1. C has the same number of dimensions as A,B and the union of their
   * attributes.
   *
   * 2. C gets the dimension names of A.
   *
   * 3. If A and B have an attribute with the same name, say 'attr', B's
   * attribute in C will be renamed to 'attr_2'.
   *
   * 4. C gets the cell capacity of A. 
   */
  static ArraySchema create_join_result_schema(
      const ArraySchema& array_schema_A, 
      const ArraySchema& array_schema_B,
      const std::string& result_array_name);
  /** 
   * Returns a pair of vectors of attribute ids. The first contains the 
   * attribute ids corresponding to the input names. The second includes the 
   * attribute ids that do NOT correspond to the input names.
   */
  std::pair<AttributeIds, AttributeIds> get_attribute_ids(
      const std::set<std::string>& expr_attribute_names) const;
  /** 
   * Returns true if the array has irregular tiles (i.e., 
   * ArraySchema::tile_extents_ is empty), and false otherwise. 
   */
  bool has_irregular_tiles() const;
  /** 
   * Returns true if the array has regular tiles (i.e., if 
   * ArraySchema::tile_extents_ is not empty), and false otherwise. 
   */
  bool has_regular_tiles() const;
  /** 
   * Returns true if the input array schemas correspond to arrays that can be 
   * joined. Otherwise, it returns false along with an error message. 
   *
   * 1. If one array is regular and the other irregular, they cannot be joined.
   *
   * 2. If the arrays have irregular tiles, then they are join-compatible if 
   * they have (i) the same number of dimensions, (ii) the same dimension type,
   * (iii) the same domains, and (iv) the same cell order.
   *
   * 3. If the arrays have regular tiles, then they are join compatible if they 
   * have (i) the same number of dimensions, (ii) the same dimension type, 
   * (iii) the same domains, (iv) the same tile and cell order, and, 
   * (v) the same tile extents.
   */
  static std::pair<bool,std::string> join_compatible(
      const ArraySchema& array_schema_A,
      const ArraySchema& array_schema_B);
  /** 
   * Returns true if the first cell precedes the second along the cell 
   * order of the schema. 
   */
  bool precedes(const void* coords_A, const void* coords_B) const;
  /** 
   * Returns true if the first cell precedes the second along the cell 
   * order of the schema. 
   */
  template<class T>
  bool precedes(const T* coords_A, const T* coords_B) const;
  /** Prints the array schema info. */
  void print() const;
  /** 
   * Returns true if the first cell succeeds the second along the cell 
   * order of the schema. 
   */
  bool succeeds(const void* coords_A, const void* coords_B) const;
  /** 
   * Returns true if the first cell succeeds the second along the cell 
   * order of the schema. 
   */
  template<class T>
  bool succeeds(const T* coords_A, const T* coords_B) const;
  /** Returns a tile id following a column major order. */
  int64_t tile_id_column_major(const void* coords) const;
  /** Returns a tile id following a column major order. */
  template<typename T>
  int64_t tile_id_column_major(const T* coords) const;
   /** 
   * Returns the tile id of the input coordinates, along the Hilbert 
   * space-filling curve.
   */
  int64_t tile_id_hilbert(const void* coords) const;
   /** 
   * Returns the tile id of the input coordinates, along the Hilbert 
   * space-filling curve.
   */
  template<typename T>
  int64_t tile_id_hilbert(const T* coords) const;
  /** Returns a tile id following a row major order. */
  int64_t tile_id_row_major(const void* coords) const;
  /** Returns a tile id following a row major order. */
  template<typename T>
  int64_t tile_id_row_major(const T* coords) const;
  /**  
   * Creates a new array schema which is identical as the caller object,
   * but has a different name (given in the input), and a transposed 2D domain
   * (i.e., the rows become columns, and vice versa). This is applicable only 
   * to matrices (i.e., 2D arrays).
   */
  const ArraySchema* transpose(const std::string& new_array_name) const;

 private:
  // PRIVATE ATTRIBUTES
  /** The array name. */
  std::string array_name_;
  /** The list with the attribute names.*/
  std::vector<std::string> attribute_names_;
  /** The number of attributes (excluding the extra coordinate attribute).*/
  int attribute_num_;
  /** 
   * The expected number of cells in a tile. This does not impose any constraint
   * on the actual number of cells per tile. It only reserves space in memory
   * for this number of cells for each tile. It is useful mainly in 
   * arrays with irregular tiles, where the capacity of each tile is fixed to 
   * ArraySchema::capacity_.
   */
  int64_t capacity_;
  /** The cell order. */
  CellOrder cell_order_;
  /** The size of an entire logical cell (i.e., coordinates plus attributes). */
  size_t cell_size_;
  /** Stores the size of every attribute (plus coordinates in the end). */
  std::vector<size_t> cell_sizes_;
  /** 
   * Indicates the compression type of each attribute (where the coordinates
   * are treated as an extra (m+1)-th attribute).
   */
  std::vector<CompressionType> compression_; 
  /** 
   * The consolidation step indicates the number of batch updates that will
   * materialize into separate array fragments, before a consolidation of
   * fragments takes place (more details are included in class Consolidator). 
   */
  int consolidation_step_;
  /** The list with the dimension domains.*/
  std::vector< std::pair< double, double > > dim_domains_;
  /** The list with the dimension names.*/
  std::vector<std::string> dim_names_;
  /** The number of dimensions.*/
  int dim_num_;
  /** 
   * Number of bits used for the calculation of cell ids with the 
   * Hilbert curve, via ArraySchema::cell_id_hilbert. 
   */
  int hilbert_cell_bits_;
  /** 
   * Number of bits used for the calculation of tile ids with the 
   * Hilbert curve, via ArraySchema::tile_id_hilbert. 
   */
  int hilbert_tile_bits_;
  /** 
   * Offsets needed for calculating tile ids with 
   * ArraySchema::tile_id_column_major.
   */
  std::vector<int64_t> tile_id_offsets_column_major_;
  /** 
   * Offsets needed for calculating tile ids with 
   * ArraySchema::tile_id_row_major.
   */
  std::vector<int64_t> tile_id_offsets_row_major_;
  /** 
   * The list with the tile extents. A tile extent is the size of the tile
   * along some dimension.
   */
  std::vector<double> tile_extents_;
  /** The tile order for regular tiles. */
  TileOrder tile_order_;
  /** The list with the attribute types. */
  std::vector<const std::type_info*> types_;

  // PRIVATE METHODS
  /** Performs appropriate checks upon a tile id request. */
  template<typename T>
  bool check_on_tile_id_request(const T* coordinates) const;
  /** Returns the size of an attribute (or coordinates). */
  size_t compute_cell_size(int attribute_id) const;
  /** 
   * Initializes the ArraySchema::hilbert_cell_bits_ value, which is 
   * necessary for calulcating cell ids with the Hilbert curve via 
   * ArraySchema::cell_id_hilbert.
   */
  void compute_hilbert_cell_bits();
  /** 
   * Initializes the ArraySchema::hilbert_tile_bits_ value, which is 
   * necessary for calulcating tile ids with the Hilbert curve via 
   * ArraySchema::tile_id_hilbert.
   */
  void compute_hilbert_tile_bits();
  /**
   * Calculates ArraySchema::tile_id_offsets_column_major_ and
   * ArraySchema::tile_id_offsets_row_major_ needed for calculating tile ids
   * with ArraySchema::tile_id_column_major and ArraySchema::tile_id_row_major,
   * respectively.
   */
  void compute_tile_id_offsets();
};

#endif
