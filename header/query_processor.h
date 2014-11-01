/**
 * @file   query_processor.h
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
 * This file defines class QueryProcessor. It also defines 
 * QueryProcessorException, which is thrown by QueryProcessor.
 */

#ifndef QUERYPROCESSOR_H
#define QUERYPROCESSOR_H

#include "array_schema.h"
#include "csv_file.h"
#include "storage_manager.h"

/** The maximum size of a physical tile in the case of irregular tiles. */
#define QP_MAX_TILE_SIZE 1000000 // 1MB 

/** 
 * This class implements the query processor module, which is responsible
 * for processing the various queries. 
 */
class QueryProcessor {
 public:
  // TYPE DEFINITIONS
  /** 
   * A multidimensional range (hyper-rectangle) in the loginal 
   * coordinate space. Format: <dim#1_lower, dim#1_upper, dim#2_lower, ...>. 
   */
  typedef std::vector<double> Range; 

  // CONSTRUCTORS AND DESTRUCTORS
  /** 
   * Takes as input the storage manager object the query processor will
   * be interfacing with, and optinally the maximum tile size. 
   */
  QueryProcessor(StorageManager& storage_manager, 
                 uint64_t max_tile_size = QP_MAX_TILE_SIZE);

  // MUTATORS
  /** Simple setter for QueryProcessor::max_tile_size_. */
  void set_max_tile_size(uint64_t max_tile_size) 
      { max_tile_size_ = max_tile_size; }

  // QUERY FUNCTIONS
  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the array schema.
   */
  void export_to_CSV(const ArraySchema& array_schema,
                     const std::string& filename) const;
  /** 
   * A subarray query creates a new array from the array with the input schema, 
   * containing only the cells whose coordinates fall into the input range, 
   * which will have the input result name.
   */
  void subarray(const ArraySchema& array_schema,
                const Range& range,
                const std::string& result_array_name) const;

 private:
  // PRIVATE ATTRIBUTES
  /** The StorageManager object the QueryProcessor will be interfacing with. */
  StorageManager& storage_manager_;
  /** The maximum size of a physical tile in the case of irregular tiles. */
  uint64_t max_tile_size_;

  // PRIVATE METHODS
  /** 
   * Appends the attribute value held by the input cell iterator into input 
   * the CSV line. 
   */
  template<class T>
  void append_attribute_value(const Tile::const_iterator& cell_it,
                              CSVLine* csv_line) const;
  /** 
   * Appends the attribute value held by the input cell iterator into the 
   * input tile. 
   */
  template<class T>
  void append_attribute_value(const Tile::const_iterator& cell_it,
                              Tile* tile) const;
  /** 
   * Appends a logical cell of an array into the input CSV line. The cell is 
   * comprised of all coordinates and attribute values (described in the input 
   * array schema), which are contained in the input array of cell iterators.
   */
  void append_cell(const ArraySchema& array_schema, 
                   const Tile::const_iterator* cell_its,
                   CSVLine* csv_line) const;
  /** 
   * Appends a logical cell of an array (comprised of attribute values and 
   * coordinates held in the input cell iterators) into
   * another array (in the corresponding tiles held in input 'tiles' variable).
   */
  void append_cell(const ArraySchema& array_schema, 
                   const Tile::const_iterator* cell_its,
                   Tile** tiles) const;
  /** 
   * Appends the coordinates held by the input cell iterator into the input
   * CSV line. 
   */
  template<class T>
  void append_coordinates(unsigned int dim_num,
                          const Tile::const_iterator& cell_it,
                          CSVLine* csv_line) const;
  /** 
   * Appends the coordinates held by the input cell iterator into the 
   * input tile. 
   */
  template<class T>
  void append_coordinates(unsigned int dim_num,
                          const Tile::const_iterator& cell_it,
                          Tile* tile) const;
  /** 
   * Creates an array of Tile objects with the input tile id based on the input 
   * array schema. 
   */
  void create_new_tiles(const ArraySchema& array_schema,
                        uint64_t tile_id, Tile** tiles) const;
  /** 
   * Gets from the storage manager all the (attribute and coordinate) tiles
   * of the input array having the input id. 
   */
  void get_tiles(const ArraySchema& array_schema,
                 uint64_t tile_id, const Tile** tiles) const;
  /** 
   * Initializes tile iterators based on the input array schema. 
   * tiles_its[0], ..., tile_its[attribute_num-1] will correspond to 
   * begin iterators over the attribute tiles.
   * tiles_its[attribute_num] and tile_it_end will correspond to begin and end
   * iterators over the coordinate tiles, respectively.
   */
  void init_tile_iterators(const ArraySchema& array_schema,
                           StorageManager::const_iterator* tile_its,
                           StorageManager::const_iterator* tile_it_end) const;
  /** Returns true if the point falls inside the inside range. */
  template<class T>
  bool inside_range(const std::vector<T>& point,
                    const Range& range) const;
  /** 
   * Returns true if the coordinates held by cell iterator cell_it fall 
   * inside the input range. 
   */
  bool inside_range(const ArraySchema& array_schema,
                    const Tile::const_iterator& cell_it, 
                    const Range& range) const;
  /** Sends the input tiles to the storage manager. */
  void store_tiles(const ArraySchema& array_schema, 
                   const std::string& result_array_name,
                   Tile** tiles) const;
  /** Implements QueryProcessor::subarray for arrays with irregular tiles. */
  void subarray_irregular(const ArraySchema& array_schema,
                          const Range& range,
                          const std::string& result_array_name) const;
  /** Implements QueryProcessor::subarray for arrays with regular tiles. */
  void subarray_regular(const ArraySchema& array_schema,
                        const Range& range,
                        const std::string& result_array_name) const;
};

/** This exception is thrown by QueryProcessor. */
class QueryProcessorException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  QueryProcessorException(const std::string& msg) 
      : msg_(msg), array_name_("") {}
  /** 
   * Takes as input the exception message and the name of the array 
   * where the exception occured. 
   */
  QueryProcessorException(const std::string& msg, const std::string& array_name) 
      : msg_(msg), array_name_(array_name) {}
  ~QueryProcessorException() {}

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
