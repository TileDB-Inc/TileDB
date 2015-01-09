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
 * This file defines class QueryProcessor. 
 */

#ifndef QUERY_PROCESSOR_H
#define QUERY_PROCESSOR_H

#include "array_schema.h"
#include "csv_file.h"
#include "storage_manager.h"

/** 
 * This class implements the query processor module, which is responsible
 * for processing the various queries. 
 */
class QueryProcessor {
 public:
  // TYPE DEFINITIONS

  // CONSTRUCTORS AND DESTRUCTORS
  /** 
   * Simple constructor. The workspace is where the query processor will create 
   * its data. The storage manager is the module the query processor interefaces 
   * with.
   */
  QueryProcessor(const std::string& workspace, StorageManager& storage_manager);

  // QUERY FUNCTIONS
  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the schema of the array.
   */
  void export_to_CSV(const StorageManager::ArrayDescriptor* array_descriptor,
                     const std::string& filename) const;
  /** 
   * A subarray query creates a new array from the input array descriptor, 
   * containing only the cells whose coordinates fall into the input range. 
   * The new array will have the input result name.
   */
  void subarray(const StorageManager::ArrayDescriptor* array_descriptor,
                const Tile::Range& range,
                const std::string& result_array_name) const;

 private:
  // PRIVATE ATTRIBUTES
  /** The StorageManager object the QueryProcessor will be interfacing with. */
  StorageManager& storage_manager_;
  /** A folder in the disk where the query processor creates all its data. */
  std::string workspace_;

  // PRIVATE METHODS
  /** Advances the cell iterators. */
  void advance_cell_its(unsigned int attribute_num,
                        Tile::const_iterator* cell_its) const; 
  /** Advances the tile iterators. */
  void advance_tile_its(unsigned int attribute_num,
                        StorageManager::const_iterator* tile_its) const; 
  /** 
   * Appends a logical cell of an array (comprised of attribute values and 
   * coordinates held in the input cell iterators) into
   * another array (in the corresponding tiles held in input variable 'tiles').
   */
  void append_cell(const Tile::const_iterator* cell_its,
                   Tile** tiles,
                   unsigned int attribute_num) const;
  /** 
   * Converts a logical cell of an array into a CSV line. The cell is 
   * comprised of all coordinates and attribute values, which are contained 
   * in the input array of cell iterators.
   */
  CSVLine cell_to_csv_line(const Tile::const_iterator* cell_its,
                           unsigned int attribute_num) const;
  /** Creates the workspace folder. */
  void create_workspace() const;
  /** 
   * Gets from the storage manager all the (attribute and coordinate) tiles
   * of the input array having the input id. 
   */
  void get_tiles(const StorageManager::ArrayDescriptor* array_descriptor,
                 uint64_t tile_id, const Tile** tiles) const;
  /** Initializes cell iterators. */
  void initialize_cell_its(const Tile** tiles,
                           unsigned int attribute_num,
                           Tile::const_iterator* cell_its, 
                           Tile::const_iterator& cell_it_end) const; 
  /** Initializes cell iterators. */
  void initialize_cell_its(const StorageManager::const_iterator* tile_its,
                           unsigned int attribute_num,
                           Tile::const_iterator* cell_its, 
                           Tile::const_iterator& cell_it_end) const; 
  /** Initializes tile iterators. */
  void initialize_tile_its(const StorageManager::ArrayDescriptor* ad,
                           StorageManager::const_iterator* tile_its,
                           StorageManager::const_iterator& tile_it_end) const;
  /** 
   * Creates an array of Tile objects with the input tile id based on the input 
   * array schema. 
   */
  void new_tiles(const ArraySchema& array_schema,
                 uint64_t tile_id, Tile** tiles) const;
  /** Returns true if the input path is an existing directory. */
  bool path_exists(const std::string& path) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
  /** Sends the input tiles to the storage manager. */
  void store_tiles(const StorageManager::ArrayDescriptor* ad, 
                   Tile** tiles) const;
  /** Implements QueryProcessor::subarray for arrays with irregular tiles. */
  void subarray_irregular(
      const StorageManager::ArrayDescriptor* array_descriptor,
      const Tile::Range& range, const std::string& result_array_name) const;
  /** Implements QueryProcessor::subarray for arrays with regular tiles. */
  void subarray_regular(
      const StorageManager::ArrayDescriptor* array_descriptor,
      const Tile::Range& range, const std::string& result_array_name) const;
};

#endif
