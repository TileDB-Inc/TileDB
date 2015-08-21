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

#include "csv_line.h"
#include "special_values.h"
#include <vector>
#include <set>
#include <string>
#include <inttypes.h>
#include <typeinfo>

/** Default value for ArraySchema::capacity_. */
#define AS_CAPACITY 10000
/** Default value for ArraySchema::cell_order_. */
#define AS_CELL_ORDER ArraySchema::CO_ROW_MAJOR
/** Default value for ArraySchema::consolidation_step_. */
#define AS_CONSOLIDATION_STEP 1
/** Name for the extra attribute representing the array coordinates. */
#define AS_COORDINATES_NAME "__coords"
/** Default value for ArraySchema::tile_order_. */
#define AS_TILE_ORDER ArraySchema::TO_ROW_MAJOR

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
  /** Dimension domains. */
  typedef std::vector<std::pair<double, double> > DimDomains;
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
  ArraySchema();
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
              const std::vector<int>& val_num, 
              CellOrder cell_order = AS_CELL_ORDER,
              int64_t capacity = AS_CAPACITY,
              int consolidation_step = AS_CONSOLIDATION_STEP);
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
              const std::vector<int>& val_num, 
              const std::vector<double>& tile_extents,
              CellOrder cell_order = AS_CELL_ORDER,
              TileOrder tile_order = AS_TILE_ORDER,
              int consolidation_step = AS_CONSOLIDATION_STEP);
  /** Empty destructor. */
  ~ArraySchema();

  // ACCESSORS
  /** Returns the array name. */
  const std::string& array_name() const;
  /** Returns the id of the i-th attribute (-1 if the name is not found). */
  int attribute_id(const std::string& attribute_name) const;
  /** Returns the ids of all attributes (plus coordinates in the end). */
  std::vector<int> attribute_ids() const;
  /** Returns the name of the i-th attribute. */
  const std::string& attribute_name(int i) const;
  /** 
   * Returns the number of attributes (excluding the extra coordinate 
   * attribute. 
   */
  int attribute_num() const;
  /** Returns the tile capacity. */
  int64_t capacity() const;
  /** Returns the tile order.  */
  CellOrder cell_order() const;
  /** 
   * Returns the size of an entire logical cell (coordinates and
   * and attributes).
   */
  size_t cell_size() const;
  /** Returns the cell size of the i-th attribute. */
  size_t cell_size(int i) const;
  /** Returns the sum of the cell sizes of the input attributes. */
  size_t cell_size(const std::vector<int>& attribute_ids) const;
  /** Returns the coordinates size. */
  size_t coords_size() const;
  /** Returns the type of the coordinates. */
  const std::type_info* coords_type() const;
  /** Returns the consolidation step. */
  int consolidation_step() const;
  /** Returns the domains. */
  const std::vector<std::pair< double, double> >& dim_domains() const; 
  /** Returns the id of the i-th dimension (-1 if the name is not found). */
  int dim_id(const std::string& dim_name) const;
  /** Returns the number of dimensions. */
  int dim_num() const;
  /** 
   * It serializes the object into a buffer of bytes. It returns a pointer
   * to the buffer it creates, along with the size of the buffer.
   */
  std::pair<const char*, size_t> serialize() const;
  /**
   * It serializes the object into a ArraySchema CSV string description that can
   * be deserialized with ArraySchema::deserialize_csv();
   */
  std::string serialize_csv() const;
  /**
   * Returns the id of the attribute with the smallest size of values. If
   * all attributes are variable-sized, it returns the id of the attribute
   * with the smallest type. 
   */
  int smallest_attribute() const;
  /** Returns the tile extents. */
  const std::vector<double>& tile_extents() const; 
  /** Returns the tile order.  */
  TileOrder tile_order() const;
  /** Returns the type of the i-th attribute. */
  const std::type_info* type(int i) const;
  /** Returns the size of the i-th attribute type. */
  size_t type_size(int i) const;
  /** Returns the number of values per attribute cell. */
  int val_num(int attribute_id) const;
  /** True if the cells are of variable size. */
  bool var_size() const;
  
  // MUTATORS
  /** It assigns values to the members of the object from the input buffer. */
  void deserialize(const char* buffer, size_t buffer_size);
  /** 
   * Creates an array schema (setting its members) from the serialized info
   * in the input string. Returns 0 on success, and -1 on error.
   */
  int deserialize_csv(std::string array_schema_str);
  /** Sets the array name. Returns 0 on success and -1 on error. */
  int set_array_name(const std::string& array_name);
  /** Sets the attribute names. Returns 0 on success and -1 on error. */
  int set_attribute_names(const std::vector<std::string>& attribute_names);
  /** Sets the capacity. Returns 0 on success and -1 on error. */
  int set_capacity(int64_t capacity);
  /** Sets the cell order. Returns 0 on success and -1 on error. */
  int set_cell_order(CellOrder cell_order);
  /** Sets the compression. Returns 0 on success and -1 on error. */
  int set_compression(const std::vector<CompressionType>& compression);
  /** Sets the consolidation steo. Returns 0 on success and -1 on error. */
  int set_consolidation_step(int consolidation_step);
  /** Sets the dimension domains. Returns 0 on success and -1 on error. */
  int set_dim_domains(
      const std::vector<std::pair<double, double> >& dim_domains);
  /** Sets the dimension names. Returns 0 on success and -1 on error. */
  int set_dim_names(const std::vector<std::string>& dim_names);
  /** Sets the tile extents. Returns 0 on success and -1 on error. */
  int set_tile_extents(const std::vector<double>& tile_extents);
  /** Sets the tile order. Returns 0 on success and -1 on error. */
  int set_tile_order(TileOrder tile_order);
  /** Sets the types. Returns 0 on success and -1 on error. */
  int set_types(const std::vector<const std::type_info*>& types);
  /** 
   * Sets the number of values per attribute. 
   * Returns 0 on success and -1 on error. 
   */
  int set_val_num(const std::vector<int>& val_num);

	
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
  /** 
   * Returns an identical schema assigning the input to the array name, and
   * including only the attributes with the input ids. 
   */
  ArraySchema* clone(const std::string& array_name, 
                     const std::vector<int>& attribute_ids) const;
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
  /** Converts a cell from a CSV line format to a binary cell format. */
  void csv_line_to_cell(
      CSVLine& csv_line, void*& cell, size_t& cell_size) const;
  /** 
   * Returns a pair of vectors of attribute ids. The first contains the 
   * attribute ids corresponding to the input names. The second includes the 
   * attribute ids that do NOT correspond to the input names.
   */
  std::pair<AttributeIds, AttributeIds> get_attribute_ids(
      const std::set<std::string>& expr_attribute_names) const;
  /** Returns the attribute ids of the input attribute names. */
  int get_attribute_ids(
      const std::vector<std::string>& attribute_names,
      std::vector<int>& attribute_ids) const;
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
  /** Returns the tile id, based on the stored tile order. */
  template<class T>
  int64_t tile_id(const T* coords) const;
  /** Returns a tile id following a column major order. */
  int64_t tile_id_column_major(const void* coords) const;
  /** Returns a tile id following a column major order. */
  template<class T>
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
  template<class T>
  int64_t tile_id_hilbert(const T* coords) const;
  /** Returns a tile id following a row major order. */
  int64_t tile_id_row_major(const void* coords) const;
  /** Returns a tile id following a row major order. */
  template<class T>
  int64_t tile_id_row_major(const T* coords) const;
  /**  
   * Creates a new array schema which is identical as the caller object,
   * but has a different name (given in the input), and a transposed 2D domain
   * (i.e., the rows become columns, and vice versa). This is applicable only 
   * to matrices (i.e., 2D arrays).
   */
  const ArraySchema* transpose(const std::string& new_array_name) const;
  /** Returns true if the input attribute ids are valid. */
  bool valid_attribute_ids(const std::vector<int>& attribute_ids) const;

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
  /** Holds space for browsing coordinates. */
  void* coords_;
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
  /** Stores the size of every attribute type (plus coordinates in the end). */
  std::vector<size_t> type_sizes_;
  /** 
   * The list of number of attribute values per cell. Specifically, each
   * attribute may store more than one values of the specified type.
   * Moreover, the cells may store a variable number of values per 
   * attribute. This is indicated by special value VAR_SIZE. 
   */
  std::vector<int> val_num_;

  // PRIVATE METHODS
  /** Appends the attribute values from a CSV line to a cell. */
  void append_attributes(
      CSVLine& csv_line, void*& cell, size_t& cell_size, size_t& offset) const;
  /** Appends an attribute value from a CSV line to a cell. */
  template<class T>
  void append_attribute(
      CSVLine& csv_line, int val_num, void*& cell, size_t& cell_size, 
      size_t& offset) const;
  /** Appends coordinates from a CSV line to a cell. */
  void append_coordinates(
      CSVLine& csv_line, void*& cell, size_t& cell_size, size_t& offset) const;
  /** Appends coordinates from a CSV line to a cell. */
  template<class T>
  void append_coordinates(
      CSVLine& csv_line, void*& cell, size_t& cell_size, size_t& offset) const;
  /** Calculates the binary size of a CSV line. */
  ssize_t calculate_cell_size(CSVLine& csv_line) const;
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
  /** Returns the size of an attribute (or coordinates) type. */
  size_t compute_type_size(int attribute_id) const;
};

#endif
