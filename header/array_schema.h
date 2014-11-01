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
 * This file defines class ArraySchema and implements ArraySchemaException. 
 */

#ifndef ARRAYSCHEMA_H
#define ARRAYSCHEMA_H

#include <vector>
#include <string>
#include <inttypes.h>

/**
 * Objects of this class store information about the schema of an array, and
 * derive information based on the schema (e.g., cell and tile ids). 
 *
 * An array may have regular or irregular tiles. If the array has regular 
 * tiles, the (common and non-zero) extents of each tile on each dimension 
 * are stored in ArraySchema::tile_extents_. If the array has irregular tiles, 
 * ArraySchema::tile_extents_ is empty.
 */
class ArraySchema {
 public:
  /** The data types of the array attributes and dimensions.*/
  enum DataType {INT, INT64_T, FLOAT, DOUBLE};
  
  // CONSTRUCTORS
  ArraySchema() {}
  /** Simple constructor, used to create an array with irregular tiles. */	
  ArraySchema(const std::string& array_name,
              const std::vector<std::string>& attribute_names,
              const std::vector<DataType>& attribute_types,
              const std::vector<std::pair<double, double> >& dim_domains,
              const std::vector<std::string>& dim_names,
              DataType dim_type);
  /** Simple constructor, used to create an array with regular tiles. */	
  ArraySchema(const std::string& array_name,
              const std::vector<std::string>& attribute_names,
              const std::vector<DataType>& attribute_types,
              const std::vector<std::pair< double, double> >& dim_domains,
              const std::vector<std::string>& dim_names,
              DataType dim_type,
              const std::vector<double>& tile_extents);

  // ACCESSORS
  /** Returns the array name. */
  const std::string& array_name() const { return array_name_; }
  /** Returns the cell size of the i-th attribute. */
  uint64_t attribute_cell_size(unsigned int i) const;
  /** Returns the name of the i-th attribute. */
  const std::string& attribute_name(unsigned int i) const;
  /** Returns the list of attribute names. */
  const std::vector<std::string>& attribute_names() const 
      { return attribute_names_; }
  /** Returns the number of attributes. */
  unsigned int attribute_num() const { return attribute_num_; }
  /** Returns the type of the i-th attribute. */
  const DataType& attribute_type(unsigned int i) const;
  /** Returns the list of attribute types. */
  const std::vector<DataType>& attribute_types() const 
      { return attribute_types_; }
  /** 
   * Returns the coordinates cell size 
   * (where a coordinate is a value on some dimension). 
   */
  uint64_t coordinates_cell_size() const;
  /** Returns the list of dimension domains. */
  const std::vector<std::pair< double, double> >& dim_domains() const 
      { return dim_domains_; }
  /** Returns the list of dimension names. */
  const std::vector<std::string>& dim_names() const { return dim_names_; }
  /** Returns the number of dimensions. */
  unsigned int dim_num() const { return dim_num_; }
  /** Returns the type of the dimension coordinates. */
  DataType dim_type() const { return dim_type_; }
  /** 
   * Returns the maximum cell size across all attributes and the coordinates. 
   */
  uint64_t max_cell_size() const;

  /** Returns the list of tile extents. */
  const std::vector<double>& tile_extents() const;
  	
  // MISC
  /** 
   * Returns the cell id of the input coordinates, along the Hilbert 
   * space-filling curve.
   */
  template<typename T>
  uint64_t cell_id_hilbert(const std::vector<T>& coordinates) const;
  /** 
   * Returns true if the array has irregular tiles (i.e., 
   * ArraySchema::tile_extents_ is empty), and false otherwise. 
   */
  bool has_irregular_tiles() const;

  /** 
   * Returns true if the array has regular tiles (i.e., 
   * ArraySchema::tile_extents_ is not empty), and false otherwise. 
   */
  bool has_regular_tiles() const;
  /** 
   * Returns true if the array is aligned with the input array, and false
   * otherwise. Two arrays are aligned if (i) they both have regular tiles
   * (alignment does not apply for irregular tiles), (ii) they have the same 
   * domains, and (iii) they have the same tile extents.
   */
  bool is_aligned_with(const ArraySchema& array_schema) const;
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
  /** The number of attributes.*/
  unsigned int attribute_num_;
  /** The list with the attribute types.*/
  std::vector<DataType> attribute_types_;
  /** The list with the dimension domains.*/
  std::vector< std::pair< double, double > > dim_domains_;
  /** The list with the dimension names.*/
  std::vector<std::string> dim_names_;
  /** The number of dimensions.*/
  unsigned int dim_num_;
  /** The type of the dimension coordinates.*/
  DataType dim_type_;
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

  // PRIVATE METHODS
  /** Performs appropriate checks upon a tile id request. */
  template<typename T>
  void check_on_tile_id_request(const std::vector<T>& coordinates) const;
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

/** This exception is thrown by ArraySchema. */
class ArraySchemaException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** 
   * Takes as input the exception message and the name of the array 
   * where the exception occured. 
   */
  ArraySchemaException(const std::string& msg, const std::string& array_name) 
      : msg_(msg), array_name_(array_name) {}
  ~ArraySchemaException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }
  /** Returns the array name where the exception occured. */
  const std::string& where() const { return array_name_; }

 private:
  /** The exception message. */
  std::string msg_;
  /** The array name where the exception occured. */
  std::string array_name_;
};


#endif
