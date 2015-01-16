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

/** Default value for ArraySchema::capacity_. */
#define AS_CAPACITY 10000
/** Name for the extra attribute representing the array coordinates. */
#define AS_COORDINATE_TILE_NAME "__coords"

#include <vector>
#include <string>
#include <inttypes.h>
#include <typeinfo>
#include <tile.h>

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
  /** The supported data types for the attributes and dimensions. */
  enum CellType {CHAR, INT, INT64_T, FLOAT, DOUBLE};
  /** 
   * The tile order in regular tiles, or the cell order in irregular tiles. 
   * Note that the cell order in regular tiles (i.e., within the tiles) is
   * by default fixed to ROW_MAJOR.
   */
  enum Order {COLUMN_MAJOR, HILBERT, ROW_MAJOR};

  // CONSTRUCTORS
  /** Empty constructor. */
  ArraySchema() {}
  /** 
   * Simple constructor, used to create a schema with irregular tiles. 
   * If there are m attributes, argument types must have size m+1,
   * and include the type of (all) the dimensions in types[m].
   * Recall that the dimensions are collectively regarded as an
   * extra attribute.
   */	
  ArraySchema(const std::string& array_name,
              const std::vector<std::string>& attribute_names,
              const std::vector<std::string>& dim_names,
              const std::vector<std::pair<double, double> >& dim_domains,
              const std::vector<const std::type_info*>& types,
              Order order,
              uint64_t capacity = AS_CAPACITY);
  /**
   * Simple constructor, used to create a schema with regular tiles.
   * If there are m attributes, argument types must have size m+1,
   * and include the type of (all) the dimensions in types[m].
   * Recall that the dimensions are collectively regarded as an
   * extra attribute.
   */
  ArraySchema(const std::string& array_name,
              const std::vector<std::string>& attribute_names,
              const std::vector<std::string>& dim_names,
              const std::vector<std::pair< double, double> >& dim_domains,
              const std::vector<const std::type_info*>& types,
              Order order,
              const std::vector<double>& tile_extents,
              uint64_t capacity = AS_CAPACITY);
  /** Empty destructor. */
  ~ArraySchema() {}

  // ACCESSORS
  /** Returns the array name. */
  const std::string& array_name() const { return array_name_; }
  /** Returns the name of the i-th attribute. */
  const std::string& attribute_name(unsigned int i) const;
  /** 
   * Returns the number of attributes (excluding the extra coordinate 
   * attribute. 
   */
  unsigned int attribute_num() const { return attribute_num_; }
  /** Returns the tile capacity. */
  uint64_t capacity() const { return capacity_; }
  /** Returns the cell size of the i-th attribute. */
  uint64_t cell_size(unsigned int i) const;
  /** Returns the number of dimensions. */
  unsigned int dim_num() const { return dim_num_; }
  /** Returns the domains. */
  const std::vector<std::pair< double, double> >& dim_domains() const 
      { return dim_domains_; } 
  /** Returns the (tile/cell) order.  */
  Order order() const { return order_; }
  /** Returns the maximum cell size across all attributes. */
  uint64_t max_cell_size() const;
  /** 
   * It serializes the object into a buffer of bytes. It returns a pointer
   * to the buffer it creates, along with the size of the buffer.
   */
  std::pair<char*, uint64_t> serialize() const;
  /** Returns the type of the i-th attribute. */
  const std::type_info* type(unsigned int i) const;
  
  // MUTATORS
  /** It assigns values to the members of the object from the input buffer. */
  void deserialize(const char* buffer, uint64_t buffer_size);
	
  // MISC
  /** 
   * Returns the cell id of the input coordinates, along the Hilbert 
   * space-filling curve.
   */
  template<typename T>
  uint64_t cell_id_hilbert(const std::vector<T>& coordinates) const;
  /** Returns an identical schema assigning the input to the array name. */
  ArraySchema clone(const std::string& array_name) const;
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
   * (iii) the same domains, and (iv) the same order.
   *
   * 3. If the arrays have regular tiles, then they are join compatible if they 
   * have (i) the same number of dimensions, (ii) the same dimension type, 
   * (iii) the same domains, (iv) the same order, and, 
   * (v) the same tile extents.
   */
  static std::pair<bool,std::string> join_compatible(
      const ArraySchema& array_schema_A,
      const ArraySchema& array_schema_B);
  /** 
   * Returns true if the first cell precedes the second along the cell 
   * order of the schema. 
   */
  template<class T>
  bool precedes(const std::vector<T>& coord_A, 
                const std::vector<T>& coord_B) const;
  /** 
   * Returns true if the first cell precedes the second along the cell 
   * order of the schema. 
   */
  bool precedes(const Tile::const_iterator& cell_it_A, 
                const Tile::const_iterator& cell_it_B) const;
  /** Prints the array schema info. */
  void print() const;
  /** 
   * Returns true if the first cell succeeds the second along the cell 
   * order of the schema. 
   */
  template<class T>
  bool succeeds(const std::vector<T>& coord_A, 
                const std::vector<T>& coord_B) const;
  /** 
   * Returns true if the first cell succeeds the second along the cell 
   * order of the schema. 
   */
  bool succeeds(const Tile::const_iterator& cell_it_A, 
                const Tile::const_iterator& cell_it_B) const;

  /** Returns a tile id following a column major order. */
  template<typename T>
  uint64_t tile_id_column_major(const std::vector<T>& coordinates) const;
   /** 
   * Returns the tile id of the input coordinates, along the Hilbert 
   * space-filling curve.
   */
  template<typename T>
  uint64_t tile_id_hilbert(const std::vector<T>& coordinates) const;
  /** Returns a tile id following a row major order. */
  template<typename T>
  uint64_t tile_id_row_major(const std::vector<T>& coordinates) const;

 private:
  // PRIVATE ATTRIBUTES
  /** The array name. */
  std::string array_name_;
  /** The list with the attribute names.*/
  std::vector<std::string> attribute_names_;
  /** The number of attributes (excluding the extra coordinate attribute).*/
  unsigned int attribute_num_;
  /** 
   * The expected number of cells in a tile. This does not impose any constraint 
   * on the actual number of cells per tile. It only reserves space in memory
   * for this number of cells for each tile. It is useful mainly in 
   * arrays with irregular tiles, where the capacity of each tile is fixed to 
   * ArraySchema::capacity_.
   */
  uint64_t capacity_;
  /** The list with the dimension domains.*/
  std::vector< std::pair< double, double > > dim_domains_;
  /** The list with the dimension names.*/
  std::vector<std::string> dim_names_;
  /** The number of dimensions.*/
  unsigned int dim_num_;
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
   * The tile order for regular tiles, or the cell order for irregular tiles.
   */
  Order order_;
  /** 
   * Offsets needed for calculating tile ids with 
   * ArraySchema::tile_id_column_major.
   */
  std::vector<uint64_t> tile_id_offsets_column_major_;
  /** 
   * Offsets needed for calculating tile ids with 
   * ArraySchema::tile_id_row_major.
   */
  std::vector<uint64_t> tile_id_offsets_row_major_;
  /** 
   * The list with the tile extents. A tile extent is the size of the tile
   * along some dimension.
   */
  std::vector<double> tile_extents_;
  /** The list with the attribute types. */
  std::vector<const std::type_info*> types_;

  // PRIVATE METHODS
  /** Performs appropriate checks upon a tile id request. */
  template<typename T>
  bool check_on_tile_id_request(const std::vector<T>& coordinates) const;
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
