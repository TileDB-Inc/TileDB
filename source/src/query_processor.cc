/**
 * @file   query_processor.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the QueryProcessor class.
 */
  
#include "query_processor.h"
#include <stdio.h>
#include <typeinfo>
#include <iostream>
#include <assert.h>

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

QueryProcessor::QueryProcessor(StorageManager& storage_manager,
                               uint64_t max_tile_size) 
    : storage_manager_(storage_manager), max_tile_size_(max_tile_size) {
}

void QueryProcessor::export_to_CSV(const ArraySchema& array_schema,
                                   const std::string& filename) const { 
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  const unsigned int attribute_num = array_schema.attribute_num();
  const unsigned int dim_num = array_schema.dim_num();

  // Initialize iterators
  StorageManager::const_iterator* tile_its = 
      new StorageManager::const_iterator[attribute_num+1];
  StorageManager::const_iterator tile_it_end; 
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  try { 
    // Prepare array and CSV file
    CSVFile csv_file(filename, CSVFile::WRITE);
    CSVLine csv_line;
    storage_manager_.open_array(array_name, StorageManager::READ);
    if(storage_manager_.is_empty(array_name))  {
      storage_manager_.close_array(array_name);
      delete [] tile_its;
      delete [] cell_its;
      throw QueryProcessorException("Cannot export to CSV file: "
                                    "array '" + array_name + "' is empty.");
    }
    init_tile_iterators(array_schema, tile_its, &tile_it_end);

    // Iterate over all tiles
    // Note that We use only the coordinate tile and cell iterators to check 
    // the stopping conditions when iterating over tiles and cell, 
    // since (i) the number of tiles is equal across all attributes, and
    // (ii) the number of cells is equal across all attribute and coordinate
    // tiles with the same id.
    while(tile_its[attribute_num] != tile_it_end) {
      // Iterate over all cells of each tile
      for(unsigned int i=0; i<attribute_num+1; i++) 
        cell_its[i] = (*tile_its[i]).begin();
   
      cell_it_end = (*tile_its[attribute_num]).end();
      while(cell_its[attribute_num] != cell_it_end) { 
        append_cell(array_schema, cell_its, &csv_line);
        csv_file << csv_line;
        csv_line.clear();
        for(unsigned int i=0; i<attribute_num+1; i++) 
          ++cell_its[i];
      }

      for(unsigned int i=0; i<attribute_num+1; i++) 
        ++tile_its[i];
    }

    // Clean up 
    storage_manager_.close_array(array_name);
    delete [] tile_its;
    delete [] cell_its;

  } catch(CSVFileException& cfe) {
    delete [] tile_its;
    delete [] cell_its;
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name());
    remove(filename.c_str());
    throw QueryProcessorException("CSVFileException caught by QueryProcessor: " 
                                  + cfe.what(), array_schema.array_name());
  } catch(TileException& te) {
    delete [] tile_its;
    delete [] cell_its;
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name());
    remove(filename.c_str());
    throw QueryProcessorException("TileException caught by QueryProcessor: " + 
                                  te.what(), array_schema.array_name());
  } catch(StorageManagerException& sme) {
    delete [] tile_its;
    delete [] cell_its;
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name());
    remove(filename.c_str());
    throw QueryProcessorException("StorageManagerException caught by "
                                  "QueryProcessor: " + sme.what(), 
                                   array_schema.array_name());
  } 
}
 
void QueryProcessor::subarray(const ArraySchema& array_schema,
                              const Range& range,
                              const std::string& result_array_name) const { 
  if(array_schema.has_regular_tiles())
    subarray_regular(array_schema, range, result_array_name);
  else 
    subarray_irregular(array_schema, range, result_array_name);
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

template<class T>
inline
void QueryProcessor::append_attribute_value(
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const {
  const T& v = *cell_it;
  *csv_line << v;
}

template<class T>
inline
void QueryProcessor::append_attribute_value(
    const Tile::const_iterator& cell_it,
    Tile* tile) const {
  const T& v = *cell_it;
  *tile << v;
}

inline
void QueryProcessor::append_cell(const ArraySchema& array_schema,
                                 const Tile::const_iterator* cell_its,
                                 CSVLine* csv_line) const {
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int dim_num = array_schema.dim_num();

  // Append coordinates
  if(array_schema.dim_type() == ArraySchema::INT) 
    append_coordinates<int>(dim_num, cell_its[attribute_num], csv_line);
  else if(array_schema.dim_type() == ArraySchema::INT64_T)
    append_coordinates<int64_t>(dim_num, cell_its[attribute_num], csv_line);
  else if(array_schema.dim_type() == ArraySchema::FLOAT)
    append_coordinates<float>(dim_num, cell_its[attribute_num], csv_line);
  else if(array_schema.dim_type() == ArraySchema::DOUBLE)
    append_coordinates<double>(dim_num, cell_its[attribute_num], csv_line);

  // Append attribute values
  for(unsigned int i=0; i<attribute_num; ++i) {
    if(array_schema.attribute_type(i) == ArraySchema::INT)
      append_attribute_value<int>(cell_its[i], csv_line); 
    else if(array_schema.attribute_type(i) == ArraySchema::INT64_T)
      append_attribute_value<int64_t>(cell_its[i], csv_line); 
    else if(array_schema.attribute_type(i) == ArraySchema::FLOAT)
      append_attribute_value<float>(cell_its[i], csv_line); 
    else if(array_schema.attribute_type(i) == ArraySchema::DOUBLE)
      append_attribute_value<double>(cell_its[i], csv_line); 
  }
}

inline
void QueryProcessor::append_cell(const ArraySchema& array_schema,
                                 const Tile::const_iterator* cell_its,
                                 Tile** tiles) const {
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int dim_num = array_schema.dim_num();

  // Append coordinates
  if(array_schema.dim_type() == ArraySchema::INT) 
    append_coordinates<int>(
        dim_num, cell_its[attribute_num], tiles[attribute_num]);
  else if(array_schema.dim_type() == ArraySchema::INT64_T)
    append_coordinates<int64_t>(
        dim_num, cell_its[attribute_num], tiles[attribute_num]);
  else if(array_schema.dim_type() == ArraySchema::FLOAT)
    append_coordinates<float>(
        dim_num, cell_its[attribute_num], tiles[attribute_num]);
  else if(array_schema.dim_type() == ArraySchema::DOUBLE)
    append_coordinates<double>(
        dim_num, cell_its[attribute_num], tiles[attribute_num]);

  // Append attribute values
  for(unsigned int i=0; i<attribute_num; ++i) {
    if(array_schema.attribute_type(i) == ArraySchema::INT)
      append_attribute_value<int>(cell_its[i], tiles[i]); 
    else if(array_schema.attribute_type(i) == ArraySchema::INT64_T)
      append_attribute_value<int64_t>(cell_its[i], tiles[i]); 
    else if(array_schema.attribute_type(i) == ArraySchema::FLOAT)
      append_attribute_value<float>(cell_its[i], tiles[i]); 
    else if(array_schema.attribute_type(i) == ArraySchema::DOUBLE)
      append_attribute_value<double>(cell_its[i], tiles[i]); 
  }
}

template<class T>
inline
void QueryProcessor::append_coordinates(unsigned int dim_num,
                                        const Tile::const_iterator& cell_it,
                                        CSVLine* csv_line) const {
  const std::vector<T>& coordinates = *cell_it;
  for(unsigned int i=0; i<dim_num; ++i) 
    *csv_line << coordinates[i];
}

template<class T>
inline
void QueryProcessor::append_coordinates(unsigned int dim_num,
                                        const Tile::const_iterator& cell_it,
                                        Tile* tile) const {
  const std::vector<T>& coordinates = *cell_it;
  *tile << coordinates;
}

inline
void QueryProcessor::create_new_tiles(const ArraySchema& array_schema,   
                                      uint64_t tile_id,
                                      Tile** tiles) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int dim_num = array_schema.dim_num();

  // Create attribute tiles
  for(unsigned int i=0; i<attribute_num; i++) {
    if(array_schema.attribute_type(i) == ArraySchema::INT)
      tiles[i] = new AttributeTile<int>(tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::INT64_T)
      tiles[i] = new AttributeTile<int64_t>(tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::FLOAT)
      tiles[i] = new AttributeTile<float>(tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::DOUBLE)
      tiles[i] = new AttributeTile<double>(tile_id);
  }

  // Create coordinate tile
  if(array_schema.dim_type() == ArraySchema::INT)
    tiles[attribute_num] = new CoordinateTile<int>(tile_id, dim_num);
  else if(array_schema.dim_type() == ArraySchema::INT64_T)
    tiles[attribute_num] = new CoordinateTile<int64_t>(tile_id, dim_num);
  else if(array_schema.dim_type() == ArraySchema::FLOAT)
    tiles[attribute_num] = new CoordinateTile<float>(tile_id, dim_num);
  else if(array_schema.dim_type() == ArraySchema::DOUBLE)
    tiles[attribute_num] = new CoordinateTile<double>(tile_id, dim_num);
}

inline
void QueryProcessor::get_tiles(const ArraySchema& array_schema, 
                               uint64_t tile_id,
                               const Tile** tiles) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int dim_num = array_schema.dim_num();

  // Get attribute tiles
  for(unsigned int i=0; i<attribute_num; i++) {
    if(array_schema.attribute_type(i) == ArraySchema::INT)
      tiles[i] = storage_manager_.get_tile<int>(
                     array_name, array_schema.attribute_name(i), tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::INT64_T)
      tiles[i] = storage_manager_.get_tile<int64_t>(
                     array_name, array_schema.attribute_name(i), tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::FLOAT)
      tiles[i] = storage_manager_.get_tile<float>(
                     array_name, array_schema.attribute_name(i), tile_id);
    else if(array_schema.attribute_type(i) == ArraySchema::DOUBLE)
      tiles[i] = storage_manager_.get_tile<double>(
                     array_name, array_schema.attribute_name(i), tile_id);
  }

  // Get coordinate tile
  if(array_schema.dim_type() == ArraySchema::INT)
    tiles[attribute_num] = storage_manager_.get_tile<int>(
                               array_name, tile_id);
  else if(array_schema.dim_type() == ArraySchema::INT64_T)
    tiles[attribute_num] = storage_manager_.get_tile<int64_t>(
                               array_name, tile_id);
  else if(array_schema.dim_type() == ArraySchema::FLOAT)
    tiles[attribute_num] = storage_manager_.get_tile<float>(
                               array_name, tile_id);
  else if(array_schema.dim_type() == ArraySchema::DOUBLE)
    tiles[attribute_num] = storage_manager_.get_tile<double>(
                               array_name, tile_id);
}

void QueryProcessor::init_tile_iterators(const ArraySchema& array_schema,
    StorageManager::const_iterator* tile_its, 
    StorageManager::const_iterator* tile_it_end) const {
  unsigned int attribute_num = array_schema.attribute_num();
  const std::string& array_name = array_schema.array_name();  

  for(unsigned int i=0; i<attribute_num; ++i) {
    if(array_schema.attribute_type(i) == ArraySchema::INT)
      tile_its[i] = storage_manager_.begin(array_name, 
                                           array_schema.attribute_name(i),
                                           typeid(int));
    else if(array_schema.attribute_type(i) == ArraySchema::INT64_T)
      tile_its[i] = storage_manager_.begin(array_name, 
                                           array_schema.attribute_name(i),
                                           typeid(int64_t));
    else if(array_schema.attribute_type(i) == ArraySchema::FLOAT)
      tile_its[i] = storage_manager_.begin(array_name, 
                                           array_schema.attribute_name(i),
                                           typeid(float));
    else if(array_schema.attribute_type(i) == ArraySchema::DOUBLE)
      tile_its[i] = storage_manager_.begin(array_name, 
                                           array_schema.attribute_name(i),
                                           typeid(double));
  }

  if(array_schema.dim_type() == ArraySchema::INT) {
    tile_its[attribute_num] = storage_manager_.begin(array_name, typeid(int));
    *tile_it_end = storage_manager_.end(array_name, typeid(int));
  } else if(array_schema.dim_type() == ArraySchema::INT64_T) {
    tile_its[attribute_num] = storage_manager_.begin(array_name, 
                                                     typeid(int64_t));
    *tile_it_end = storage_manager_.end(array_name, typeid(int64_t));
  } else if(array_schema.dim_type() == ArraySchema::FLOAT) {
    tile_its[attribute_num] = storage_manager_.begin(array_name, typeid(float));
    *tile_it_end = storage_manager_.end(array_name, typeid(float));
  } else if(array_schema.dim_type() == ArraySchema::DOUBLE) {
    tile_its[attribute_num] = storage_manager_.begin(array_name, 
                                                     typeid(double));
    *tile_it_end = storage_manager_.end(array_name, typeid(double));
  }
}

template<class T>
inline
bool QueryProcessor::inside_range(const std::vector<T>& point, 
                                  const Range& range) const {
  unsigned int dim_num = point.size();
  assert(range.size()/2 == dim_num);

  for(unsigned int i=0; i<dim_num; i++)
    if(point[i] < range[2*i] || point[i] > range[2*i+1])
      return false;
  return true;
}

inline
bool QueryProcessor::inside_range(const ArraySchema& array_schema,
                                  const Tile::const_iterator& cell_it,
                                  const Range& range) const {
  unsigned int dim_num = array_schema.dim_num();
  assert(range.size()/2 == dim_num); 

  if(array_schema.dim_type() == ArraySchema::INT) {
    const std::vector<int>& point = *cell_it;
    return inside_range<int>(point, range);
  } else if(array_schema.dim_type() == ArraySchema::INT64_T) {
    const std::vector<int64_t>& point = *cell_it;
    return inside_range<int64_t>(point, range);
  } else if(array_schema.dim_type() == ArraySchema::FLOAT) {
    const std::vector<float>& point = *cell_it;
    return inside_range<float>(point, range);
  } else if(array_schema.dim_type() == ArraySchema::DOUBLE) {
    const std::vector<double>& point = *cell_it;
    return inside_range<double>(point, range);
  }
}

inline
void QueryProcessor::store_tiles(const ArraySchema& array_schema,
                                 const std::string& result_array_name,
                                 Tile** tiles) const {
  unsigned int attribute_num = array_schema.attribute_num();
  for(unsigned int i=0; i<attribute_num; i++) 
    storage_manager_.append_tile(tiles[i], result_array_name, 
                                 array_schema.attribute_name(i));

  storage_manager_.append_tile(tiles[attribute_num], result_array_name); 
} 

void QueryProcessor::subarray_irregular(
    const ArraySchema& array_schema,
    const Range& range,
    const std::string& result_array_name) const { 
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  const unsigned int attribute_num = array_schema.attribute_num();
  const unsigned int dim_num = array_schema.dim_num(); 
  uint64_t max_cell_num = QP_MAX_TILE_SIZE / array_schema.max_cell_size();

  // Create tiles and iterators
  const Tile** tiles = new const Tile*[attribute_num+1];
  Tile** new_tiles = new Tile*[attribute_num+1];
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  try {
    // Prepare arrays
    storage_manager_.open_array(array_name, StorageManager::READ);
    if(storage_manager_.is_empty(array_name)) { 
      delete [] tiles;
      delete [] new_tiles;
      delete [] cell_its;
      throw QueryProcessorException("Cannot process subarray query: "
                                    "array '" + array_name + "' is empty.");
    }
    storage_manager_.open_array(result_array_name, StorageManager::CREATE);
    
    // Get the tile ids that overlap with the range
    std::vector<std::pair<uint64_t, bool> > overlapping_tile_ids;
    storage_manager_.get_overlapping_tile_ids(array_name, range, 
                                              &overlapping_tile_ids);

    // Initialize iterators
    std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it = 
        overlapping_tile_ids.begin();
    std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it_end =
        overlapping_tile_ids.end();
      
    // Create new tiles and initialize iterators 
    uint64_t tile_id = 0;
    create_new_tiles(array_schema, 0, new_tiles); 

    // Iterate over all tiles
    for(; tile_id_it != tile_id_it_end; ++tile_id_it) {
      get_tiles(array_schema, tile_id_it->first, tiles);         
      for(unsigned int i=0; i<attribute_num+1; i++)
        cell_its[i] = tiles[i]->begin();
      cell_it_end = tiles[attribute_num]->end();

      if(tile_id_it->second) { // Full overlap
        while(cell_its[attribute_num] != cell_it_end) {
          if(new_tiles[0]->cell_num() == max_cell_num) {
            store_tiles(array_schema, result_array_name, new_tiles);
            create_new_tiles(array_schema, ++tile_id, new_tiles); 
           }
          append_cell(array_schema, cell_its, new_tiles);
          for(unsigned int i=0; i<attribute_num+1; i++) 
            ++cell_its[i];
        }
      } else { // Partial overlap
        while(cell_its[attribute_num] != cell_it_end) {
          if(inside_range(array_schema, cell_its[attribute_num], range)) {
            if(new_tiles[0]->cell_num() == max_cell_num) {
              store_tiles(array_schema, result_array_name, new_tiles);
              create_new_tiles(array_schema, ++tile_id, new_tiles); 
           }
            append_cell(array_schema, cell_its, new_tiles);
          }
          for(unsigned int i=0; i<attribute_num+1; i++) 
            ++cell_its[i];
        }
      }
      
      // Send the lastly created tiles to storage manager
      store_tiles(array_schema, result_array_name, new_tiles);
    } 

    // Clean up 
    storage_manager_.close_array(array_name);
    storage_manager_.close_array(result_array_name);
    delete [] tiles;
    delete [] new_tiles;
    delete [] cell_its;

  } catch(CSVFileException& cfe) {
    delete [] tiles;
    delete [] new_tiles;
    delete [] cell_its;
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name());
    storage_manager_.delete_array(result_array_name);
    throw QueryProcessorException("CSVFileException caught by QueryProcessor: " 
                                  + cfe.what(), array_schema.array_name());
  } catch(TileException& te) {
    delete [] tiles;
    delete [] new_tiles;
    delete [] cell_its; 
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name()); 
    storage_manager_.delete_array(result_array_name);
    throw QueryProcessorException("TileException caught by QueryProcessor: " + 
                                  te.what(), array_schema.array_name());
  } catch(StorageManagerException& sme) {
    delete [] tiles;
    delete [] new_tiles;
    delete [] cell_its;
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name());
    storage_manager_.delete_array(result_array_name);
    throw QueryProcessorException("StorageManagerException caught by "
                                  "QueryProcessor: " + sme.what(), 
                                   array_schema.array_name());
  }
}

void QueryProcessor::subarray_regular(
    const ArraySchema& array_schema,
    const Range& range,
    const std::string& result_array_name) const { 
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  const unsigned int attribute_num = array_schema.attribute_num();
  const unsigned int dim_num = array_schema.dim_num(); 

  // Create tiles and iterators
  const Tile** tiles = new const Tile*[attribute_num+1];
  Tile** new_tiles = new Tile*[attribute_num+1];
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  try {
    // Prepare arrays
    storage_manager_.open_array(array_name, StorageManager::READ);
    if(storage_manager_.is_empty(array_name)) { 
      delete [] tiles;
      delete [] new_tiles;
      delete [] cell_its;
      throw QueryProcessorException("Cannot process subarray query: "
                                    "array '" + array_name + "' is empty.");
    }
    storage_manager_.open_array(result_array_name, StorageManager::CREATE);
    
    // Get the tile ids that overlap with the range
    std::vector<std::pair<uint64_t, bool> > overlapping_tile_ids;
    storage_manager_.get_overlapping_tile_ids(array_name, range, 
                                              &overlapping_tile_ids);

    // Initialize iterators
    std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it = 
        overlapping_tile_ids.begin();
    std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it_end =
        overlapping_tile_ids.end();

    // Iterate over all tiles
    for(; tile_id_it != tile_id_it_end; ++tile_id_it) {
      // Create new tiles and initialize iterators 
      create_new_tiles(array_schema, tile_id_it->first, new_tiles); 
      get_tiles(array_schema, tile_id_it->first, tiles);         
      for(unsigned int i=0; i<attribute_num+1; i++)
        cell_its[i] = tiles[i]->begin();
      cell_it_end = tiles[attribute_num]->end();

      if(tile_id_it->second) { // Full overlap
        while(cell_its[attribute_num] != cell_it_end) {
          append_cell(array_schema, cell_its, new_tiles);
          for(unsigned int i=0; i<attribute_num+1; i++) 
            ++cell_its[i];
        }
      } else { // Partial overlap
        while(cell_its[attribute_num] != cell_it_end) {
          if(inside_range(array_schema, cell_its[attribute_num], range))
            append_cell(array_schema, cell_its, new_tiles);
          for(unsigned int i=0; i<attribute_num+1; i++) 
            ++cell_its[i];
        }
      }
      
      // Send new tiles to storage manager
      store_tiles(array_schema, result_array_name, new_tiles);
    } 

    // Clean up 
    storage_manager_.close_array(array_name);
    storage_manager_.close_array(result_array_name);
    delete [] tiles;
    delete [] new_tiles;
    delete [] cell_its;

  } catch(CSVFileException& cfe) {
    delete [] tiles;
    delete [] new_tiles;
    delete [] cell_its;
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name());
    storage_manager_.delete_array(result_array_name);
    throw QueryProcessorException("CSVFileException caught by QueryProcessor: " 
                                  + cfe.what(), array_schema.array_name());
  } catch(TileException& te) {
    delete [] tiles;
    delete [] new_tiles;
    delete [] cell_its; 
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name()); 
    storage_manager_.delete_array(result_array_name);
    throw QueryProcessorException("TileException caught by QueryProcessor: " + 
                                  te.what(), array_schema.array_name());
  } catch(StorageManagerException& sme) {
    delete [] tiles;
    delete [] new_tiles;
    delete [] cell_its;
    if(storage_manager_.is_open(array_schema.array_name())) 
      storage_manager_.close_array(array_schema.array_name());
    storage_manager_.delete_array(result_array_name);
    throw QueryProcessorException("StorageManagerException caught by "
                                  "QueryProcessor: " + sme.what(), 
                                   array_schema.array_name());
  }
}

// Explicit template instantiations
template void QueryProcessor::append_attribute_value<int>(
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const;
template void QueryProcessor::append_attribute_value<int64_t>(
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const;
template void QueryProcessor::append_attribute_value<float>(
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const;
template void QueryProcessor::append_attribute_value<double>(
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const;
template void QueryProcessor::append_coordinates<int>(
    unsigned int dim_num,
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const;
template void QueryProcessor::append_coordinates<int64_t>(
    unsigned int dim_num,
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const;
template void QueryProcessor::append_coordinates<float>(
    unsigned int dim_num,
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const;
template void QueryProcessor::append_coordinates<double>(
    unsigned int dim_num,
    const Tile::const_iterator& cell_it,
    CSVLine* csv_line) const;










/*

#include <limits>
#include <stdlib.h>
#include <algorithm>


void Executor::delete_array(const std::string& array_name)
{
//	std::string cmd = "rm TileDB_Data/" + array_name + "-* -rf";
	std::string cmd = "rm " BASE_DIR + array_name + "-* -rf";
	system(cmd.c_str());	
//	remove(("TileDB_Data/" + array_name + "_Version.idx").c_str());
	remove((BASE_DIR + array_name + "_Version.idx").c_str());
}


void Executor::execute_load(const char* plan)
{
	std::cout << "Processing 'load' query...\n";
	fflush(stdout);

	std::string filename;
	DBArray* db_array = NULL;
	plan_parser.parse_load_plan(plan, filename, db_array);
	
	delete_array(db_array->get_array_name());

	try{
		storage_manager->init_versions(*db_array);
		storage_manager->init_load_session(*db_array);
		loader.load(filename, *db_array);
		storage_manager->commit_load_session(*db_array);
		storage_manager->commit_versions(*db_array);
	}
	catch(LoaderException& le)
	{
		if(db_array != NULL) 
			delete db_array;
		throw ExecutorException(le.what());
	}
	catch(StorageManagerException& sme)
	{
		if(db_array != NULL) 
			delete db_array;
		throw ExecutorException(sme.what());
	}

	if(db_array != NULL) 
		delete db_array;
	
	std::cout << "'load' query processed successfully!\n";
	fflush(stdout);
}

void Executor::execute_insert(const char* plan)
{
	std::cout << "Processing 'insert' query...\n";
	fflush(stdout);

	std::string filename;
	DBArray* db_array = NULL;
	plan_parser.parse_insert_plan(plan, filename, db_array);

	try{
		storage_manager->init_time_Store();             
                storage_manager->init_time_write(); 
		storage_manager->init_read_segments();


                struct timeval tim;      
                gettimeofday(&tim, NULL);
                double start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::clock_t start_cpu = std::clock();
	
		std::clock_t end_cpu;
		double cpu_time;
		double end_total;

		int new_version = storage_manager->load_versions(*db_array);
	//	std::cout << "new_version = " << new_version << "\n";
	//	fflush(stdout); 

 
//		storage_manager->set_version(*db_array, new_version);
//		storage_manager->init_load_session(*db_array);

  //              std::clock_t end_cpu = std::clock();
    //            double cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
     //           std::cout << "Insert init: CPU seconds elapsed :" << cpu_time << "\n";
      //          gettimeofday(&tim, NULL);
      //          double end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
       //         std::cout << "Insert init: Total seconds elapsed:" << end_total-start_total << "\n";
       //         fflush(stdout);




            //    gettimeofday(&tim, NULL);
           //     start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
          //      start_cpu = std::clock();



	//	loader.load(filename, *db_array);


          //      end_cpu = std::clock();
          //      cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
          //      std::cout << "Insert - load: CPU seconds elapsed :" << cpu_time << "\n";
         //       gettimeofday(&tim, NULL);
        //        end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
       //         std::cout << "Insert - load: Total seconds elapsed:" << end_total-start_total << "\n";
      //          fflush(stdout);


    //            gettimeofday(&tim, NULL);
   //             start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
  //              start_cpu = std::clock();



//		storage_manager->commit_load_session(*db_array);


    //            end_cpu = std::clock();
    //            cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
//                std::cout << "Insert - commit load: CPU seconds elapsed :" << cpu_time << "\n";
  //              gettimeofday(&tim, NULL);
    //            end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
    //            std::cout << "Insert - commit load: Total seconds elapsed:" << end_total-start_total << "\n";
   //             fflush(stdout);



		
                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();



		db_array->set_version(storage_manager->get_consolidation_step()-1);
		storage_manager->consolidate(*db_array);


                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Insert - consolidate: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Insert - consolidate: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);


                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();



		storage_manager->commit_versions(*db_array);


                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Insert - commit versions: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Insert - commit versions: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);




                std::cout << "Insert: Total CPU time for STORE seconds elapsed :" << storage_manager->get_time_Store() << "\n";
                std::cout << "Insert: Total time for write seconds elapsed :" << storage_manager->get_time_write() << "\n";

		std::cout <<"Read segments = " << storage_manager->get_read_segments() << "\n";
                fflush(stdout);


	}
	catch(LoaderException& le)
	{
		if(db_array != NULL) 
			delete db_array;
		throw ExecutorException(le.what());
	}
	catch(StorageManagerException& sme)
	{
		if(db_array != NULL) 
			delete db_array;
		throw ExecutorException(sme.what());
	}

	if(db_array != NULL) 
		delete db_array;
	
	std::cout << "'insert' query processed successfully!\n";
	fflush(stdout);
}

void Executor::execute_export(const char* plan)
{
	std::cout << "Processing 'export' query...\n";
	fflush(stdout);

	std::string filename;
	DBArray* db_array = NULL;
	plan_parser.parse_export_plan(plan, filename, db_array);
	
	try{
		storage_manager->load_versions(*db_array);
		std::vector<std::string> versioned_array_names = storage_manager->init_query_session(*db_array);
		export_to_CSV(filename, *db_array, versioned_array_names, true);
		storage_manager->commit_query_session(*db_array, versioned_array_names);
		storage_manager->delete_versions(*db_array);
	}
	catch(StorageManagerException& sme)
	{
		if(db_array != NULL) 
			delete db_array;
		throw ExecutorException(sme.what());
	}

	if(db_array != NULL) 
		delete db_array;
	
	std::cout << "'export' query processed successfully!\n";
	fflush(stdout);
}


void Executor::execute_filter(const char* plan)
{
	std::cout << "Processing 'filter' query...\n";
	fflush(stdout);

	std::string result_array_name, query_attr_name, value;
	PlanParser::CMP_OPERATOR cmp_operator;
	DBArray* db_array = NULL;
	plan_parser.parse_filter_plan(plan, result_array_name, query_attr_name, cmp_operator, value, db_array);
	DBArray new_db_array = *db_array;
	new_db_array.set_array_name(result_array_name);

	std::cout << result_array_name << "\n";
	fflush(stdout);

	try{

		storage_manager->init_time_Store();             
                storage_manager->init_time_write(); 


                struct timeval tim;      
                gettimeofday(&tim, NULL);
                double start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::clock_t start_cpu = std::clock();


		storage_manager->load_versions(*db_array);
		std::vector<std::string> versioned_array_names = storage_manager->init_query_session(*db_array);
		storage_manager->init_versions(new_db_array);
		storage_manager->init_load_session(new_db_array);

                std::clock_t end_cpu = std::clock();
                double cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Filter 1: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                double end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Filter 1: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);




                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();



		filter(new_db_array.get_array_name(), query_attr_name, cmp_operator, value, 
			*db_array, versioned_array_names);



                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Filter 2: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Filter 2: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);




                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();





		storage_manager->commit_query_session(*db_array, versioned_array_names);
		storage_manager->delete_versions(*db_array);
		storage_manager->commit_load_session(new_db_array);
		storage_manager->commit_versions(new_db_array);


                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Filter 3: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Filter 3: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);




                std::cout << "Filter: Total CPU time for STORE seconds elapsed :" << storage_manager->get_time_Store() << "\n";
                std::cout << "Filter: Total IO time for STORE seconds elapsed :" << storage_manager->get_time_write() << "\n";



	}
	catch(StorageManagerException& sme)
	{
		if(db_array != NULL) 
			delete db_array;
		throw ExecutorException(sme.what());
	}

	if(db_array != NULL) 
		delete db_array;
	
	std::cout << "'filter' query processed successfully!\n";
	fflush(stdout);
}


void Executor::execute_subarray(const char* plan)
{
	std::cout << "Processing 'subarray' query...\n";
	fflush(stdout);

	std::string result_array_name;
	std::vector<double> range;
//	PlanParser::CMP_OPERATOR cmpsoperator;
	DBArray* db_array = NULL;
	plan_parser.parse_subarray_plan(plan, result_array_name, range, db_array);
	DBArray new_db_array = *db_array;
	new_db_array.set_array_name(result_array_name);

        std::cout << result_array_name << "\n";
        fflush(stdout);

	try{


		storage_manager->init_time_Store();
                storage_manager->init_time_write();
		storage_manager->init_read_segments();


                struct timeval tim;
                gettimeofday(&tim, NULL);
                double start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::clock_t start_cpu = std::clock();




		storage_manager->load_versions(*db_array);


		std::vector<std::string> versioned_array_names = storage_manager->init_query_session(*db_array);


		
		storage_manager->init_versions(new_db_array);
		// TODO: SET VERSIONS (copy index)


		storage_manager->init_load_session(new_db_array);


                std::clock_t end_cpu = std::clock();
                double cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Subarray 1: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                double end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Subarray 1: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);


                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();

		std::cout << "new_db_array.get_array_name() = " << new_db_array.get_array_name() << "\n";
		fflush(stdout);


//		subarray(new_db_array.get_array_name(), range, *db_array, versioned_array_names);
		// TODO: Change first input
		subarray(new_db_array.get_array_name(), range, *db_array, versioned_array_names[0]);


                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Subarray 2: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Subarray 2: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);

		std::cout <<"Read segments = " << storage_manager->get_read_segments() << "\n";
		fflush(stdout);


                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();



		storage_manager->commit_query_session(*db_array, versioned_array_names);
		storage_manager->delete_versions(*db_array);
		// TODO: for loop for all new versions
		storage_manager->commit_load_session(new_db_array);
		storage_manager->commit_versions(new_db_array);

                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Subarray 3: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Subarray 3: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);




                std::cout << "Subarray: Total CPU time for STORE seconds elapsed :" << storage_manager->get_time_Store() << "\n";
                std::cout << "Subarray: Total IO time for STORE seconds elapsed :" << storage_manager->get_time_write() << "\n";


	}
	catch(StorageManagerException& sme)
	{
		if(db_array != NULL) 
			delete db_array;
		throw ExecutorException(sme.what());
	}

	if(db_array != NULL) 
		delete db_array;
	
	std::cout << "'subarray' query processed successfully!\n";
	fflush(stdout);
}


void Executor::execute_join(const char* plan)
{
	std::cout << "Processing 'join' query...\n";
	fflush(stdout);

	std::string result_array_name;
	DBArray *db_array_1 = NULL, *db_array_2 = NULL, *new_db_array = NULL;
	plan_parser.parse_join_plan(plan, result_array_name, db_array_1, db_array_2);
	join_db_arrays(db_array_1, db_array_2, new_db_array, result_array_name);

	try{
		bool aligned = db_array_2->is_aligned_with(*db_array_1);

		std::cout << "Join: " << db_array_1->get_array_name() << " x " << db_array_2->get_array_name() << "\n";
		fflush(stdout);

		storage_manager->init_time_Store();
                storage_manager->init_time_write();
		storage_manager->init_read_segments();


                struct timeval tim;
                gettimeofday(&tim, NULL);
                double start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::clock_t start_cpu = std::clock();


		
		if(!aligned)
			redimension(*db_array_1, *db_array_2);



                std::clock_t end_cpu = std::clock();
                double cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Redimension: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                double end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Redimension: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);


                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();

	
		storage_manager->load_versions(*db_array_1);
		storage_manager->load_versions(*db_array_2);
		std::vector<std::string> versioned_array_names_1 = storage_manager->init_query_session(*db_array_1);
		std::vector<std::string> versioned_array_names_2 = storage_manager->init_query_session(*db_array_2);
		storage_manager->init_versions(*new_db_array);
		storage_manager->init_load_session(*new_db_array);


                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Join Init: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Join Init: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);


                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();
		
		join(*db_array_1, *db_array_2, *new_db_array, versioned_array_names_1, versioned_array_names_2);
		

                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Join execution: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Join execution: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);



                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();


		storage_manager->commit_query_session(*db_array_1, versioned_array_names_1);
		storage_manager->commit_query_session(*db_array_2, versioned_array_names_2);
		storage_manager->delete_versions(*db_array_1);
		storage_manager->delete_versions(*db_array_2);
		storage_manager->commit_load_session(*new_db_array);
		storage_manager->commit_versions(*new_db_array);



                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Join commit: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Join commit: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);

		std::cout <<"Read segments = " << storage_manager->get_read_segments() << "\n";
		fflush(stdout);


		
		if(!aligned)
			delete_array(db_array_2->get_array_name());


	}
	catch(StorageManagerException& sme)
	{
		if(db_array_1 != NULL) delete db_array_1;
		if(db_array_2 != NULL) delete db_array_2;
		if(new_db_array != NULL) delete new_db_array;
		throw ExecutorException(sme.what());
	}
	catch(LoaderException& le)
	{
		if(db_array_1 != NULL) delete db_array_1;
		if(db_array_2 != NULL) delete db_array_2;
		if(new_db_array != NULL) delete new_db_array;
		throw ExecutorException(le.what());
	}
	
	if(db_array_1 != NULL) delete db_array_1;
	if(db_array_2 != NULL) delete db_array_2;
	if(new_db_array != NULL) delete new_db_array;
	
	std::cout << "'join' query processed successfully!\n";
	fflush(stdout);
}

// Redimension db_array_2 to match the tile sizes of db_array_1
void Executor::redimension(const DBArray& db_array_1, DBArray& db_array_2)
{
	std::string temp_name = TEMP_REDIMENSION_NAME;


                struct timeval tim;
                gettimeofday(&tim, NULL);
                double start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::clock_t start_cpu = std::clock();



	// Exprort to CSV without the IDs
	storage_manager->load_versions(db_array_2);
	std::vector<std::string> versioned_array_names_2 = storage_manager->init_query_session(db_array_2);
	export_to_CSV(temp_name + ".csv", db_array_2, versioned_array_names_2, false);
	storage_manager->commit_query_session(db_array_2, versioned_array_names_2);
	storage_manager->delete_versions(db_array_2);



                std::clock_t end_cpu = std::clock();
                double cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Redimension - export: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                double end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Redimension - export: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);


                gettimeofday(&tim, NULL);
                start_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                start_cpu = std::clock();




	
	// Change the tile sizes of db_array_2 to match those of db_array_1, and give a temp name
	db_array_2.set_tile_sizes(db_array_1.get_tile_sizes());
	db_array_2.set_array_name(temp_name);

	// Re-load db_array_2
	DBArray temp_db_array_2 = db_array_2;
	storage_manager->init_versions(temp_db_array_2);
	storage_manager->init_load_session(temp_db_array_2);
	loader.load(temp_name + ".csv", temp_db_array_2);
	storage_manager->commit_load_session(temp_db_array_2);
	storage_manager->commit_versions(temp_db_array_2);



                end_cpu = std::clock();
                cpu_time = ( end_cpu - start_cpu ) / (double) CLOCKS_PER_SEC;
                std::cout << "Redimension - re-load: CPU seconds elapsed :" << cpu_time << "\n";
                gettimeofday(&tim, NULL);
                end_total = tim.tv_sec+(tim.tv_usec/1000000.0);
                std::cout << "Redimension - re-load: Total seconds elapsed:" << end_total-start_total << "\n";
                fflush(stdout);


	
	// delete the temp CSV file
	remove(temp_name.c_str());


}


void Executor::join_db_arrays(const DBArray* const  db_array_1, const DBArray* const db_array_2, DBArray*& new_db_array, 
	const std::string& result_array_name)
{
	std::vector<std::string> attr_names = db_array_1->get_attr_names();
	std::vector<std::string> attr_names_2 = db_array_2->get_attr_names();
	for(int i=0; i<attr_names_2.size(); i++)
		attr_names.push_back(attr_names_2[i]);
	std::vector<DBArray::DATA_TYPE> attr_types = db_array_1->get_attr_types();
	std::vector<DBArray::DATA_TYPE> attr_types_2 = db_array_2->get_attr_types();
	for(int i=0; i<attr_types_2.size(); i++)
		attr_types.push_back(attr_types_2[i]);

	new_db_array = new DBArray(result_array_name, db_array_1->get_dim_num(), attr_names.size(), 
		db_array_1->get_dim_domains(), db_array_1->get_dim_names(), db_array_1->get_dim_types(), 
		attr_names, attr_types, db_array_1->get_tile_sizes());
}


void Executor::join(const DBArray& db_array_1, const DBArray& db_array_2, const DBArray& new_db_array,
	const std::vector<std::string>& versioned_array_names_1, const std::vector<std::string>& versioned_array_names_2)
{
	if(db_array_1.fixed_logical_tiles())
		join_logical(db_array_1, db_array_2, new_db_array, versioned_array_names_1, versioned_array_names_2);
	else
		join_physical(db_array_1, db_array_2, new_db_array, versioned_array_names_1, versioned_array_names_2);
}


// Note: The current implementation assumes unique coordinates in each array!
void Executor::join_physical(const DBArray& db_array_1, const DBArray& db_array_2, const DBArray& new_db_array,
	const std::vector<std::string>& versioned_array_names_1, const std::vector<std::string>& versioned_array_names_2)
{
	int attr_num_1 = db_array_1.get_attr_num();
	int attr_num_2 = db_array_2.get_attr_num();
	int new_attr_num = new_db_array.get_attr_num();
	int dim_num = db_array_1.get_dim_num();
	int64_t min_cell_ID_1, min_cell_ID_2;
	int version_num_1 = versioned_array_names_1.size();
	int version_num_2 = versioned_array_names_2.size();
	int64_t segment_size = storage_manager->get_segment_size();	

	int attr_with_max_cell_size;
	int64_t max_cell_size = std::numeric_limits<int64_t>::min();
	for(int j=0; j<new_attr_num+1; j++)
		if(new_db_array.get_attr_size(j) > max_cell_size)
		{
			max_cell_size = new_db_array.get_attr_size(j);
			attr_with_max_cell_size = j;
		}

	
	// Prepare the tile ID iterators
	std::vector<int64_t>::const_iterator* tile_id_index_its_1 = new std::vector<int64_t>::const_iterator[version_num_1];
	for(int i=0; i<version_num_1; i++)
		tile_id_index_its_1[i] = storage_manager->get_tile_id_index_it_begin(versioned_array_names_1[i]);
	std::vector<int64_t>::const_iterator* tile_id_index_its_2 = new std::vector<int64_t>::const_iterator[version_num_2];
	for(int i=0; i<version_num_2; i++)
		tile_id_index_its_2[i] = storage_manager->get_tile_id_index_it_begin(versioned_array_names_2[i]);

	// Prepare the output tiles	
	int64_t tile_ID = 0;
	Tile** output_tiles = new Tile*[new_attr_num+1];
	for(int j=0; j<new_attr_num+1; j++)
	{
		output_tiles[j] = new Tile(tile_ID, Tile::OUT, 0, &new_db_array, j);
//		output_tiles[j] = new Tile(tile_ID, new_db_array.get_array_name(), new_db_array.get_attr_name(j), 
//			Tile::OUT, 0, new_db_array.get_attr_size(j), new_db_array.get_dim_num());
		output_tiles[j]->set_buffer_expansion(tile_size);	
	}
	
	// Prepare the input tiles
	Tile*** tiles_1 = new Tile**[version_num_1];
	for(int i=0; i<version_num_1; i++)
	{
		tiles_1[i] = new Tile*[attr_num_1+1];
		for(int j=0; j<attr_num_1+1; j++)
			tiles_1[i][j] = storage_manager->get_tile(versioned_array_names_1[i], db_array_1.get_attr_name(j),
				*(tile_id_index_its_1[i]), db_array_1);
	}
	Tile*** tiles_2 = new Tile**[version_num_2];
	for(int i=0; i<version_num_2; i++)
	{
		tiles_2[i] = new Tile*[attr_num_2+1];
		for(int j=0; j<attr_num_2+1; j++)
			tiles_2[i][j] = storage_manager->get_tile(versioned_array_names_2[i], db_array_2.get_attr_name(j),
				*(tile_id_index_its_2[i]), db_array_2);
	}
		
	// Prepare the cell IDs
	char** coordinate_cells_1 = new char*[version_num_1];
	int64_t* cell_IDs_1 = new int64_t[version_num_1];
	for(int i=0; i<version_num_1; i++)
	{
		coordinate_cells_1[i] = new char[db_array_1.get_attr_size(attr_num_1)];
		tiles_1[i][attr_num_1]->get_next_cell(coordinate_cells_1[i]);
		cell_IDs_1[i] = get_cell_ID(db_array_1, coordinate_cells_1[i], tiles_1[i][attr_num_1]->get_tile_ID());
	}
	char** coordinate_cells_2 = new char*[version_num_2];
	int64_t* cell_IDs_2 = new int64_t[version_num_2];
	for(int i=0; i<version_num_2; i++)
	{
		coordinate_cells_2[i] = new char[db_array_2.get_attr_size(attr_num_1)];
		tiles_2[i][attr_num_2]->get_next_cell(coordinate_cells_2[i]);
		cell_IDs_2[i] = get_cell_ID(db_array_2, coordinate_cells_2[i], tiles_2[i][attr_num_2]->get_tile_ID());
	}



	int times = -1;

	do
	{

		times++;

//		std::cout << "While times = " << times << " start \n";
//		fflush(stdout);


		// Get the minimum cell IDs
		min_cell_ID_1 = std::numeric_limits<int64_t>::max();
		int version_1;
		for(int i=0; i<version_num_1; i++)
			if(cell_IDs_1[i] < min_cell_ID_1)
			{
				min_cell_ID_1 = cell_IDs_1[i];
				version_1 = i;
			}
		min_cell_ID_2 = std::numeric_limits<int64_t>::max();
		int version_2;
		for(int i=0; i<version_num_2; i++)
			if(cell_IDs_2[i] < min_cell_ID_2)
			{
				min_cell_ID_2 = cell_IDs_2[i];
				version_2 = i;
			}

		// No more cells in one of the two arrays, we must break
		if(min_cell_ID_1 == std::numeric_limits<int64_t>::max() &&
		   min_cell_ID_2 == std::numeric_limits<int64_t>::max())
			break;


//		std::cout << "continue times = " << times << "\n";
//		fflush(stdout);


		// Skip entire tiles if they are guaranteed NOT to join, and continue
		if(min_cell_ID_1 < min_cell_ID_2)
		{

//			std::cout << "Skip join times = " << times << "\n";
//			fflush(stdout);


			if(storage_manager->get_cell_id_range(versioned_array_names_1[version_1], 
		 	   tiles_1[version_1][0]->get_tile_ID()).second < min_cell_ID_2)
			{
				tile_id_index_its_1[version_1]++;

				if(tile_id_index_its_1[version_1] == 
				   storage_manager->get_tile_id_index_it_end(versioned_array_names_1[version_1]))
				{	
					cell_IDs_1[version_1] = std::numeric_limits<int64_t>::max();
					for(int j=0; j<attr_num_1+1; j++)
						delete tiles_1[version_1][j];
				}
				else
				{
					for(int j=0; j<attr_num_1+1; j++)
					{
						delete tiles_1[version_1][j];
						tiles_1[version_1][j] = 	
							storage_manager->get_tile(versioned_array_names_1[version_1], 
							db_array_1.get_attr_name(j), *(tile_id_index_its_1[version_1]), db_array_1);
					}
					tiles_1[version_1][attr_num_1]->get_next_cell(coordinate_cells_1[version_1]);
					cell_IDs_1[version_1] = get_cell_ID(db_array_1, coordinate_cells_1[version_1],
						tiles_1[version_1][attr_num_1]->get_tile_ID());
				}
 				continue;
			}		


		}
		else if (min_cell_ID_2 < min_cell_ID_1)
		{
//			std::cout << "perform join times = " << times << "\n";
//			fflush(stdout);


			if(storage_manager->get_cell_id_range(versioned_array_names_2[version_2], 
		 	  tiles_2[version_2][0]->get_tile_ID()).second < min_cell_ID_1)
			{
				tile_id_index_its_2[version_2]++;
				
				if(tile_id_index_its_2[version_2] == 
				   storage_manager->get_tile_id_index_it_end(versioned_array_names_2[version_2]))
				{	
					cell_IDs_2[version_2] = std::numeric_limits<int64_t>::max();
					for(int j=0; j<attr_num_2+1; j++)
						delete tiles_2[version_2][j];
				}
				else
				{
					for(int j=0; j<attr_num_2+1; j++)
					{
						delete tiles_2[version_2][j];
						tiles_2[version_2][j] = 	
							storage_manager->get_tile(versioned_array_names_2[version_2], 
							db_array_2.get_attr_name(j), *(tile_id_index_its_2[version_2]), db_array_2);
					}
					tiles_2[version_2][attr_num_2]->get_next_cell(coordinate_cells_2[version_2]);
					cell_IDs_2[version_2] = get_cell_ID(db_array_2, coordinate_cells_2[version_2], 
						tiles_2[version_2][attr_num_2]->get_tile_ID());
				}
 				continue;
			}		 
		}


//			std::cout << "after check finished times = " << times << "\n";
//			fflush(stdout);

	
		// Continue if the two min_cell_IDs do not match
		if(min_cell_ID_1 < min_cell_ID_2)
		{
			// Skip the first cell of each attribute
			for(int j=0; j<attr_num_1; j++)	
			{
				char* cell = new char[db_array_1.get_attr_size(j)];
				tiles_1[version_1][j]->get_next_cell(cell);
				delete cell;
			}
			
			// Get the next cell ID
			if(tiles_1[version_1][attr_num_1]->get_next_cell(coordinate_cells_1[version_1]))
				cell_IDs_1[version_1] = get_cell_ID(db_array_1, coordinate_cells_1[version_1],
					tiles_1[version_1][attr_num_1]->get_tile_ID());
			else // We must retrieve a new tile
			{
				tile_id_index_its_1[version_1]++;
					
				// End of tiles
				if(tile_id_index_its_1[version_1] == 
				    storage_manager->get_tile_id_index_it_end(versioned_array_names_1[version_1]))
				{
					cell_IDs_1[version_1] = std::numeric_limits<int64_t>::max();
					for(int j=0; j<attr_num_1+1; j++)
						delete tiles_1[version_1][j];
				}
				else
				{
					// Delete input tiles and bring new ones
					for(int j=0; j<attr_num_1+1; j++)
					{
						delete tiles_1[version_1][j];
						tiles_1[version_1][j] = 
							storage_manager->get_tile(versioned_array_names_1[version_1], 
							db_array_1.get_attr_name(j), *(tile_id_index_its_1[version_1]), db_array_1);
					}

					// Get the first cell and its ID
					tiles_1[version_1][attr_num_1]->get_next_cell(coordinate_cells_1[version_1]);
					cell_IDs_1[version_1] = get_cell_ID(db_array_1, coordinate_cells_1[version_1],
						tiles_1[version_1][attr_num_1]->get_tile_ID());
				}
			}

			continue;
		}
		else if(min_cell_ID_2 < min_cell_ID_1)
		{
			// Skip the first cell of each attribute
			for(int j=0; j<attr_num_2; j++)	
			{
				char* cell = new char[db_array_2.get_attr_size(j)];
				tiles_2[version_2][j]->get_next_cell(cell);
				delete cell;
			}
			
			// Get the next cell ID
			if(tiles_2[version_2][attr_num_2]->get_next_cell(coordinate_cells_2[version_2]))
				cell_IDs_2[version_2] = get_cell_ID(db_array_2, coordinate_cells_2[version_2],
					tiles_2[version_2][attr_num_2]->get_tile_ID());
			else // We must retrieve a new tile
			{
				tile_id_index_its_2[version_2]++;
					
				// End of tiles
				if(tile_id_index_its_2[version_2] == 
				    storage_manager->get_tile_id_index_it_end(versioned_array_names_2[version_2]))
				{
					cell_IDs_2[version_2] = std::numeric_limits<int64_t>::max();
					for(int j=0; j<attr_num_2+1; j++)
						delete tiles_2[version_2][j];
				}
				else
				{
					// Delete input tiles and bring new ones
					for(int j=0; j<attr_num_2+1; j++)
					{
						delete tiles_2[version_2][j];
						tiles_2[version_2][j] = 
							storage_manager->get_tile(versioned_array_names_2[version_2], 
							db_array_2.get_attr_name(j), *(tile_id_index_its_2[version_2]), db_array_2);
					}

					// Get the first cell and its ID
					tiles_2[version_2][attr_num_2]->get_next_cell(coordinate_cells_2[version_2]);
					cell_IDs_2[version_2] = get_cell_ID(db_array_2, coordinate_cells_2[version_2],
						tiles_2[version_2][attr_num_2]->get_tile_ID());
				}
			}

			continue;
		}

//			std::cout << "min cell id match start times = " << times << "\n";
//			fflush(stdout);


		// At this point, the min cell IDs match
		
		// We must append, but we first check if the output tile is full
		if(output_tiles[attr_with_max_cell_size]->get_tile_size() + max_cell_size > tile_size)
		{
			tile_ID++;
			// Store and delete the output tiles, and initialize new output tiles
			for(int j=0; j<new_attr_num+1; j++)
			{
				storage_manager->store(output_tiles[j], new_db_array);
				delete output_tiles[j];
				output_tiles[j] = new Tile(tile_ID, Tile::OUT, 0, &new_db_array, j);
//				output_tiles[j] = new Tile(tile_ID, new_db_array.get_array_name(), 
//					new_db_array.get_attr_name(j), Tile::OUT, 0, new_db_array.get_attr_size(j), new_db_array.get_dim_num());
				output_tiles[j]->set_buffer_expansion(tile_size);
			}
		}

//			std::cout << "before append times = " << times << "\n";
//			fflush(stdout);


		// Append
		output_tiles[new_attr_num]->append_cell(coordinate_cells_1[version_1], min_cell_ID_1);
		char* cell;	
		for(int j=0; j<attr_num_1; j++)
		{
			cell = new char[db_array_1.get_attr_size(j)];
			tiles_1[version_1][j]->get_next_cell(cell);
			output_tiles[j]->append_cell(cell, min_cell_ID_1);
			delete [] cell;
		}
		for(int j=0; j<attr_num_2; j++)
		{
			cell = new char[db_array_2.get_attr_size(j)];
			tiles_2[version_2][j]->get_next_cell(cell);
			output_tiles[j+attr_num_1]->append_cell(cell, min_cell_ID_2);
			delete [] cell;
		}
		
//			std::cout << "after append times = " << times << "\n";
//			fflush(stdout);


		// Get the next cell ID
		if(tiles_1[version_1][attr_num_1]->get_next_cell(coordinate_cells_1[version_1]))
			cell_IDs_1[version_1] = get_cell_ID(db_array_1, coordinate_cells_1[version_1],
				tiles_1[version_1][attr_num_1]->get_tile_ID());
		else // We must retrieve a new tile
		{
			tile_id_index_its_1[version_1]++;
					
			// End of tiles
			if(tile_id_index_its_1[version_1] == 
			    storage_manager->get_tile_id_index_it_end(versioned_array_names_1[version_1]))
			{
				cell_IDs_1[version_1] = std::numeric_limits<int64_t>::max();
				for(int j=0; j<attr_num_1+1; j++)
					delete tiles_1[version_1][j];
			}
			else
			{
				// Delete input tiles and bring new ones
				for(int j=0; j<attr_num_1+1; j++)
				{
					delete tiles_1[version_1][j];
					tiles_1[version_1][j] = 
						storage_manager->get_tile(versioned_array_names_1[version_1], 
						db_array_1.get_attr_name(j), *(tile_id_index_its_1[version_1]), db_array_1);
				}

				// Get the first cell and its ID
				tiles_1[version_1][attr_num_1]->get_next_cell(coordinate_cells_1[version_1]);
				cell_IDs_1[version_1] = get_cell_ID(db_array_1, coordinate_cells_1[version_1],
					tiles_1[version_1][attr_num_1]->get_tile_ID());
			}
		}

//			std::cout << "after get next cell id if 1 times = " << times << "\n";
//			fflush(stdout);


		if(tiles_2[version_2][attr_num_2]->get_next_cell(coordinate_cells_2[version_2]))
		{
//			std::cout << "in if start times = " << times << "\n";
//			fflush(stdout);


			cell_IDs_2[version_2] = get_cell_ID(db_array_2, coordinate_cells_2[version_2],
				tiles_2[version_2][attr_num_2]->get_tile_ID());

//			std::cout << "in if end times = " << times << "\n";
//			fflush(stdout);


		}
		else // We must retrieve a new tile
		{

//			std::cout << "in else start times = " << times << "\n";
//			fflush(stdout);

			tile_id_index_its_2[version_2]++;
				
			// End of tiles
			if(tile_id_index_its_2[version_2] == 
			    storage_manager->get_tile_id_index_it_end(versioned_array_names_2[version_2]))
			{
				cell_IDs_2[version_2] = std::numeric_limits<int64_t>::max();
				for(int j=0; j<attr_num_2+1; j++)
					delete tiles_2[version_2][j];
			}
			else
			{
				// Delete input tiles and bring new ones
				for(int j=0; j<attr_num_2+1; j++)
				{
					delete tiles_2[version_2][j];
					tiles_2[version_2][j] = 
						storage_manager->get_tile(versioned_array_names_2[version_2], 
						db_array_2.get_attr_name(j), *(tile_id_index_its_2[version_2]), db_array_2);
				}

				// Get the first cell and its ID
				tiles_2[version_2][attr_num_2]->get_next_cell(coordinate_cells_2[version_2]);
				cell_IDs_2[version_2] = get_cell_ID(db_array_2, coordinate_cells_2[version_2],
					tiles_2[version_2][attr_num_2]->get_tile_ID());
			}
//			std::cout << "in else finish times = " << times << "\n";
//			fflush(stdout);

		}

//			std::cout << "after get next cell id if 2 = " << times << "\n";
//			fflush(stdout);


	
	}while(true);


//	std::cout << "While finished \n";
//	fflush(stdout);



	// Store the lastly created output tiles and delete them from the memory
	for(int j=0; j<new_attr_num+1; j++)
	{
		storage_manager->store(output_tiles[j], new_db_array);
		delete output_tiles[j];
	}
	delete [] output_tiles;
	
	// Clear the rest of the memory
	delete [] tile_id_index_its_1;
	delete [] tile_id_index_its_2;
	delete [] cell_IDs_1;
	delete [] cell_IDs_2;
	for(int i=0; i<version_num_1; i++)
	{
		delete [] coordinate_cells_1[i];
		delete [] tiles_1[i];
	}
	for(int i=0; i<version_num_2; i++)
	{
		delete [] coordinate_cells_2[i];
		delete [] tiles_2[i];
	}
	delete [] coordinate_cells_1;
	delete [] coordinate_cells_2;
	delete [] tiles_1;
	delete [] tiles_2;
}

int64_t Executor::get_cell_ID(const DBArray& db_array, char* cell, int64_t tile_ID)
{
	if(db_array.fixed_logical_tiles())
	{
	
		if(storage_manager->get_cell_order() == "hilbert")        
                        return db_array.get_local_cell_ID_hilbert_2(cell);
                else if(storage_manager->get_cell_order() == "column")
                        return db_array.get_local_cell_ID_column_2(cell);		
	}
	else
	{
		if(storage_manager->get_cell_order() == "hilbert")
			return db_array.get_cell_ID_hilbert_2(cell);
		else if(storage_manager->get_cell_order() == "column")
			return db_array.get_cell_ID_column_2(cell);
	}
}

// Note: The current implementation assumes unique coordinates in each array!
void Executor::join_logical(const DBArray& db_array_1, const DBArray& db_array_2, const DBArray& new_db_array,
	const std::vector<std::string>& versioned_array_names_1, const std::vector<std::string>& versioned_array_names_2)
{
	int attr_num_1 = db_array_1.get_attr_num();
	int attr_num_2 = db_array_2.get_attr_num();
	int new_attr_num = new_db_array.get_attr_num();
	int dim_num = db_array_1.get_dim_num();
	int64_t min_tile_ID_1, min_tile_ID_2;

	// Get the tile iterators for both input arrays
	int version_num_1 = versioned_array_names_1.size();
	std::vector<int64_t>::const_iterator* tile_id_its_1 = new std::vector<int64_t>::const_iterator[version_num_1];
	for(int i=0; i<version_num_1; i++)
		tile_id_its_1[i] = storage_manager->get_tile_id_index_it_begin(versioned_array_names_1[i]);	

	int version_num_2 = versioned_array_names_2.size();
	std::vector<int64_t>::const_iterator* tile_id_its_2 = new std::vector<int64_t>::const_iterator[version_num_2];
	for(int i=0; i<version_num_2; i++)
		tile_id_its_2[i] = storage_manager->get_tile_id_index_it_begin(versioned_array_names_2[i]);	

	Tile** output_tiles = new Tile*[new_attr_num+1];

	int examined_cells = 0;
	int joined_tiles = 0;

	do
	{

//		joined_tiles++;

//		if(joined_tiles%100 == 0)
//		{
//			std::cout << "joined_tiles = " << joined_tiles << "\n";	
//			fflush(stdout);
//		}

		// Find the minimum tile IDs
		min_tile_ID_1 = std::numeric_limits<int64_t>::max();
		for(int i=0; i<version_num_1; i++)
			if( tile_id_its_1[i] != storage_manager->get_tile_id_index_it_end(versioned_array_names_1[i]) &&
			    min_tile_ID_1 > *tile_id_its_1[i])
				min_tile_ID_1 = *tile_id_its_1[i];

		min_tile_ID_2 = std::numeric_limits<int64_t>::max();
		for(int i=0; i<version_num_2; i++)
			if( tile_id_its_2[i] != storage_manager->get_tile_id_index_it_end(versioned_array_names_2[i]) &&
			    min_tile_ID_2 > *tile_id_its_2[i])
				min_tile_ID_2 = *tile_id_its_2[i];
	
		// Break if any of the min tile IDs is max (no more tiles for one of the arrays)
		if(min_tile_ID_1 == std::numeric_limits<int64_t>::max() || 
		   min_tile_ID_2 == std::numeric_limits<int64_t>::max())
			break;

		// Continue if the two min_tile_IDs do not match
		if(min_tile_ID_1 != min_tile_ID_2)
		{
			if(min_tile_ID_1 < min_tile_ID_2)
			{
				for(int i=0; i<version_num_1; i++)
					if(*tile_id_its_1[i] == min_tile_ID_1)
						tile_id_its_1[i]++;
			}
			else
			{
				for(int i=0; i<version_num_2; i++)
					if(*tile_id_its_2[i] == min_tile_ID_2)
						tile_id_its_2[i]++;
			}
			continue;
		}

		// Now min_tile_ID_1 = min_tile_ID_2
	
		// Initialize the output tiles 	
		for(int j=0; j<new_attr_num+1; j++)
			output_tiles[j] = new Tile(min_tile_ID_1, Tile::OUT, 0, &new_db_array, j);
//			output_tiles[j] = new Tile(min_tile_ID_1, new_db_array.get_array_name(), 
//				new_db_array.get_attr_name(j), Tile::OUT, 0, new_db_array.get_attr_size(j), new_db_array.get_dim_num());

		// Gather the tiles with min_ID from both arrays
		std::vector<Tile**> tiles_with_min_ID_1;
		for(int i=0; i<version_num_1; i++)
			if(*tile_id_its_1[i] == min_tile_ID_1)
			{
				Tile** tiles_1 = new Tile*[attr_num_1+1];
				for(int j=0; j<attr_num_1+1; j++)
					tiles_1[j] = storage_manager->get_tile(versioned_array_names_1[i], 
							db_array_1.get_attr_name(j), min_tile_ID_1, db_array_1);
				tiles_with_min_ID_1.push_back(tiles_1);
				tile_id_its_1[i]++;	
			}

		std::vector<Tile**> tiles_with_min_ID_2;
		for(int i=0; i<version_num_2; i++)
			if(*tile_id_its_2[i] == min_tile_ID_2)
			{
				Tile** tiles_2 = new Tile*[attr_num_2+1];
				for(int j=0; j<attr_num_2+1; j++)
					tiles_2[j] = storage_manager->get_tile(versioned_array_names_2[i], 
							db_array_2.get_attr_name(j), min_tile_ID_2, db_array_2);
				tiles_with_min_ID_2.push_back(tiles_2);
				tile_id_its_2[i]++;	
			}

		// Find the sorted orders on cell_ID
		std::vector< std::vector<std::pair<int64_t, int64_t> > > cell_orders_1 = 
									get_cell_orders(tiles_with_min_ID_1, db_array_1);  
		std::vector< std::vector<std::pair<int64_t, int64_t> > > cell_orders_2 = 
									get_cell_orders(tiles_with_min_ID_2, db_array_2);  
		join_tiles(tiles_with_min_ID_1, tiles_with_min_ID_2, cell_orders_1, cell_orders_2, output_tiles,
				db_array_1, db_array_2, new_db_array);

		cell_orders_1.clear();
		cell_orders_2.clear();

		// Clean memory
		for(int i=0; i<tiles_with_min_ID_1.size(); i++)
		{
			for(int j=0; j<attr_num_1+1; j++)
				delete tiles_with_min_ID_1[i][j];
			delete tiles_with_min_ID_1[i];
		}
		
		for(int i=0; i<tiles_with_min_ID_2.size(); i++)
		{
			for(int j=0; j<attr_num_2+1; j++)
				delete tiles_with_min_ID_2[i][j];
			delete tiles_with_min_ID_2[i];
		}

		for(int j=0; j < new_attr_num+1; j++)
		{
			storage_manager->store(output_tiles[j], new_db_array);
			delete output_tiles[j];
		}

	}while(true);

	delete [] tile_id_its_1;
	delete [] tile_id_its_2;
	delete [] output_tiles;
}

std::vector< std::vector<std::pair<int64_t, int64_t> > > Executor::get_cell_orders(const std::vector<Tile**>& tiles, 
	const DBArray& db_array)
{
	std::vector< std::vector<std::pair<int64_t, int64_t> > > cell_orders;
	char* cell = new char[tiles[0][db_array.get_attr_num()]->get_cell_size()];
	bool logical = db_array.fixed_logical_tiles();

	int64_t cell_ID;

//	std::cout << "Get cell orders: Tiles size = " << tiles.size() << "\n";
//	fflush(stdout);

	for(int i=0; i<tiles.size(); i++)
	{
		
//		std::cout << "Get cell orders: Array = " << db_array.get_array_name() << "\t Tile ID = " << tiles[i][0]->get_tile_ID() << " Cell num = " << tiles[i][0]->get_cell_num() << "\n";
//		fflush(stdout);

		std::vector<std::pair<int64_t, int64_t> > cell_order;
		for(int64_t j=0; j<tiles[i][0]->get_cell_num(); j++)
		{
			tiles[i][db_array.get_attr_num()]->get_next_cell(cell);
			

			if(logical && storage_manager->get_cell_order() == "hilbert")
				//cell_ID = db_array.get_cell_ID_hilbert_logical_iterative_2(cell, tiles[i][db_array.get_attr_num()]->get_tile_ID());
				cell_ID = db_array.get_local_cell_ID_hilbert_2(cell);
			else if(logical && storage_manager->get_cell_order() == "column")
				//cell_ID = db_array.get_cell_ID_column_logical_iterative_2(cell, tiles[i][db_array.get_attr_num()]->get_tile_ID());
				cell_ID = db_array.get_local_cell_ID_column_2(cell);
			else if(!logical && storage_manager->get_cell_order() == "hilbert")
				cell_ID = db_array.get_cell_ID_hilbert_2(cell);
			else if(!logical && storage_manager->get_cell_order() == "column")
				cell_ID = db_array.get_cell_ID_column_2(cell);
			
			cell_order.push_back(std::pair<int64_t, int64_t>(cell_ID, j));
		}
	
		cell_orders.push_back(cell_order);
	}

	delete [] cell;

	return cell_orders;
}

void Executor::join_tiles(const std::vector<Tile**>& tiles_1, const std::vector<Tile**>& tiles_2, 
	const std::vector< std::vector<std::pair<int64_t, int64_t> > >& cell_orders_1, 
	const std::vector< std::vector<std::pair<int64_t, int64_t> > >& cell_orders_2,	
	Tile**& output_tiles, const DBArray& db_array_1, const DBArray& db_array_2, const DBArray& new_db_array)
{
	int attr_num_1 = db_array_1.get_attr_num();
	int attr_num_2 = db_array_2.get_attr_num();
	int new_attr_num = new_db_array.get_attr_num();
	// int dim_num = db_array_1.get_dim_num();
	int64_t min_cell_ID_1, min_cell_ID_2;
	char *cell_1, *cell_2, *cell, *output_cell;
	
	// Get the tile iterators for both input cell orders
	int tiles_num_1 = tiles_1.size();
	std::vector<std::pair<int64_t, int64_t> >::const_iterator* cell_orders_its_1 = 
		new std::vector<std::pair<int64_t, int64_t> >::const_iterator[tiles_num_1];	
	for(int i=0; i<tiles_num_1; i++)
		cell_orders_its_1[i] = cell_orders_1[i].begin();	

	int tiles_num_2 = tiles_2.size();
	std::vector<std::pair<int64_t, int64_t> >::const_iterator* cell_orders_its_2 = 
		new std::vector<std::pair<int64_t, int64_t> >::const_iterator[tiles_num_2];	
	for(int i=0; i<tiles_num_2; i++)
		cell_orders_its_2[i] = cell_orders_2[i].begin();	


	int joined_cells = 0;
	int times = 0;

	


	do
	{
//		if(cell_orders_its_1[0] == cell_orders_1[0].end() || cell_orders_its_2[0] != cell_orders_2[0].end())
//		{
//				std::cout << "Cell orders are empty. After " << times << " loops";
//				fflush(stdout);
//		}

		// Find the minimum cell IDs
		min_cell_ID_1 = std::numeric_limits<int64_t>::max();
		for(int i=0; i<tiles_1.size(); i++)
			if( cell_orders_its_1[i] != cell_orders_1[i].end() && 
			    min_cell_ID_1 > (*(cell_orders_its_1[i])).first)
				min_cell_ID_1 = (*(cell_orders_its_1[i])).first;

		min_cell_ID_2 = std::numeric_limits<int64_t>::max();
		for(int i=0; i<tiles_2.size(); i++)
			if( cell_orders_its_2[i] != cell_orders_2[i].end() && 
			    min_cell_ID_2 > (*(cell_orders_its_2[i])).first)
				min_cell_ID_2 = (*(cell_orders_its_2[i])).first;
	
		// Break if any of the min tile IDs is max (no more tiles for one of the arrays)
		if(min_cell_ID_1 == std::numeric_limits<int64_t>::max() || 
		   min_cell_ID_2 == std::numeric_limits<int64_t>::max())
			{
//				std::cout << "Break: min cell id = numeric max. After " << times << " loops";
//				fflush(stdout);
				break;
			}
		// Continue if the two min_tile_IDs do not match
		if(min_cell_ID_1 != min_cell_ID_2)
		{
			if(min_cell_ID_1 < min_cell_ID_2)
			{
				for(int i=0; i<tiles_num_1; i++)
					if((*cell_orders_its_1[i]).first == min_cell_ID_1)
						cell_orders_its_1[i]++;
			}
			else
			{
				for(int i=0; i<tiles_num_2; i++)
					if((*cell_orders_its_2[i]).first == min_cell_ID_2)
						cell_orders_its_2[i]++;
			}
//			std::cout << "Break: min cell ids do not much. After " << times << " loops";
//			fflush(stdout);


			continue;
		}
		
		// Now min_cell_ID_1 = min_cell_ID_2: we must merge the two cells
		
		// Find the positions in the tiles of the joining cells
		int64_t pos_1, pos_2;
		int version_1, version_2;
		for(int i=0; i<tiles_num_1; i++)
			if((*cell_orders_its_1[i]).first == min_cell_ID_1)
			{
				version_1 = i;
				pos_1 = (*cell_orders_its_1[i]).second;
				cell_orders_its_1[i]++;
				break;
			}
		
		for(int i=0; i<tiles_num_2; i++)
			if((*cell_orders_its_2[i]).first == min_cell_ID_2)
			{
				version_2 = i;
				pos_2 = (*cell_orders_its_2[i]).second;
				cell_orders_its_2[i]++;
				break;
			}

		cell_1 = new char[db_array_1.get_attr_size(attr_num_1)];		
		tiles_1[version_1][attr_num_1]->get_cell(cell_1, pos_1);
		cell_2 = new char[db_array_1.get_attr_size(attr_num_1)];		
		tiles_2[version_2][attr_num_1]->get_cell(cell_2, pos_2);


//		int x1, x2, y1, y2, time1, time2;
//		int64_t id1, id2;

//		memcpy(&x1, cell_1, sizeof(int));
//		memcpy(&y1, cell_1+sizeof(int), sizeof(int));
//		memcpy(&time1, cell_1+2*sizeof(int), sizeof(int));
//		memcpy(&id1, cell_1+3*sizeof(int), sizeof(int64_t));

//		memcpy(&x2, cell_2, sizeof(int));
//		memcpy(&y2, cell_2+sizeof(int), sizeof(int));
//		memcpy(&time2, cell_2+2*sizeof(int), sizeof(int));
//		memcpy(&id2, cell_2+3*sizeof(int), sizeof(int64_t));

//		if(times%1000 == 0)
//		{
//			std::cout << "x1 = " << x1 << "\t y1 = " << y1 << "\t x2 = " << x2 << "\t y2 = " << y2 << "\t time1 = " << time1 << "\t time2 = " << time2 << "\t id1 = " << id1 << "\t id2 = " << id2 << "\n";
//			fflush(stdout);
//		}

		// Join the cells
		bool cells_must_join = !memcmp(cell_1, cell_2, db_array_1.get_attr_size(attr_num_1));

		
		



		if(cells_must_join)
		{
			joined_cells++;
			output_tiles[new_attr_num]->append_cell(cell_1);
		}

			
		for(int i=0; i<attr_num_1; i++)
		{
			cell = new char[db_array_1.get_attr_size(i)];
			tiles_1[version_1][i]->get_cell(cell, pos_1);
			if(cells_must_join)
				output_tiles[i]->append_cell(cell);
			delete [] cell;
		}
			
		for(int i=0; i<attr_num_2; i++)
		{
			cell = new char[db_array_2.get_attr_size(i)];
			tiles_2[version_2][i]->get_cell(cell, pos_2);
			if(cells_must_join)
				output_tiles[i+attr_num_1]->append_cell(cell);
			delete [] cell;
		}
		
		delete [] cell_1;
		delete [] cell_2;

		times++;
			
	}while(true);

	delete [] cell_orders_its_1;
	delete [] cell_orders_its_2;
	
//	std::cout << "joined_cells = " << joined_cells << "\n";
//	fflush(stdout);	
}


//void Executor::subarray(const std::string& result_array_name, const std::vector<double>& range, const DBArray& db_array, 
//	const std::vector<std::string>& versioned_array_names)
void Executor::subarray(const std::string& result_array_name, const std::vector<double>& range, const DBArray& db_array, 
	const std::string& versioned_array_name)
{
	if(db_array.fixed_logical_tiles())
		subarray_logical(result_array_name, range, db_array, versioned_array_name);
	else
		subarray_physical(result_array_name, range, db_array, versioned_array_name);
}


//void Executor::subarray_logical(const std::string& result_array_name, const std::vector<double>& range, 
//	const DBArray& db_array, const std::vector<std::string>& versioned_array_names)
void Executor::subarray_logical(const std::string& result_array_name, const std::vector<double>& range, 
	const DBArray& db_array, const std::string& versioned_array_name)
{
	int attr_num = db_array.get_attr_num();
	int dim_num = db_array.get_dim_num();
	DBArray result_db_array = db_array;
	result_db_array.set_array_name(result_array_name);


	// Get overlapping tile ids
	std::vector<std::pair<int64_t, Overlap> > overlapping_tile_ids;
	std::vector<std::pair<int64_t, Overlap> >::const_iterator tile_id_it;
	storage_manager->get_overlapping_tile_ids(range, versioned_array_name, overlapping_tile_ids);

	if(overlapping_tile_ids.size() == 0)
		return;

	tile_id_it = overlapping_tile_ids.begin();

	int64_t cells_in_subarray = 0;
	int64_t investigated_cells = 0;

	// Prepare output tiles
	Tile** output_tiles = new Tile*[attr_num+1];
	
	char** cell = new char*[attr_num+1];
	for(int j=0; j<attr_num+1; j++)	
		cell[j] = new char[db_array.get_attr_size(j)];

	// Prepare the input tiles
	Tile** tiles = new Tile*[attr_num+1];

	int64_t tile_ID;


	while(tile_id_it != overlapping_tile_ids.end())
	{

		// prepare input tiles
		for(int j=0; j<attr_num+1; j++)
			tiles[j] = storage_manager->get_tile(versioned_array_name, db_array.get_attr_name(j),
					(*(tile_id_it)).first, db_array);

		
		// prepare output tiles
		tile_ID = tiles[0]->get_tile_ID();                     
                for(int j=0; j<attr_num+1; j++)
                        output_tiles[j] = new Tile(tile_ID, Tile::OUT, 0, &result_db_array, j);


		int64_t cell_ID;

		for(int i = 0; i < tiles[attr_num]->get_cell_num(); i++)
		{
			// Get all cells
        	        for(int j=0; j<attr_num+1; j++)
                	        tiles[j]->get_cell(cell[j], i);

			if((*(tile_id_it)).second == FULL)
			{
				cells_in_subarray++;
 				cell_ID = get_cell_ID(db_array, cell[attr_num], (*(tile_id_it)).first);

                                for(int j=0; j<attr_num+1; j++)
                                        output_tiles[j]->append_cell(cell[j], cell_ID);				
			}
			else
			{

     		        	// Get cell of coordinates
                		std::vector<double> point;
              			double p;
                		int pos = 0;
                		for(int j=0; j<dim_num; j++)
                		{
                        		if(db_array.get_dim_type(j) == DBArray::INT)
                        		{
                                		int c;
                                		memcpy(&c, cell[attr_num]+pos, sizeof(int));
                                		pos += sizeof(int);
                                		p = c;
                        		}
                        		else if(db_array.get_dim_type(j) == DBArray::INT64_T)
                        		{
                                		int64_t c;
                                		memcpy(&c, cell[attr_num]+pos, sizeof(int64_t));
                                		pos += sizeof(int64_t);
                                		p = c;
                        		}
                        		else if(db_array.get_dim_type(j) == DBArray::FLOAT)
                        		{
                                		float c;
                                		memcpy(&c, cell[attr_num]+pos, sizeof(float));
                                		pos += sizeof(float);
                                		p = c;
                        		}
                        		else if(db_array.get_dim_type(j) == DBArray::DOUBLE)
                        		{
                                		double c;
                                		memcpy(&c, cell[attr_num]+pos, sizeof(double));
                                		pos += sizeof(double);
                                		p = c;
                        		}

                        		point.push_back(p);
                		}
			

				// Check if it is in the range and append
	                	if(inside_range(point, range))       
        	        	{
                	        	cells_in_subarray++;
	 				cell_ID = get_cell_ID(db_array, cell[attr_num], (*(tile_id_it)).first);

                        		for(int j=0; j<attr_num+1; j++)
                                		output_tiles[j]->append_cell(cell[j], cell_ID);
                		}
			} // end of else



		}	// end of for loop
	
		investigated_cells+=tiles[attr_num]->get_cell_num();


		for(int j=0; j<attr_num+1; j++)
                {
                	storage_manager->store(output_tiles[j], result_db_array);
                        delete output_tiles[j];
                }


		// Delete input tiles
                for(int j=0; j<attr_num+1; j++)
                	delete tiles[j];

		tile_id_it++;
	} // end of while loop

	// Clear the rest of the memory

	delete [] output_tiles;

	for(int j=0; j<attr_num; j++)	
		delete [] cell[j];
	delete [] cell;

	delete [] tiles;


	


	std::cout << "cells in subarray = " << cells_in_subarray << "\n";
	fflush(stdout);

	std::cout << "investigated cells = " << investigated_cells << "\n";
	fflush(stdout);

}


//void Executor::subarray_physical(const std::string& result_array_name, const std::vector<double>& range, 
//	const DBArray& db_array, const std::vector<std::string>& versioned_array_names)
void Executor::subarray_physical(const std::string& result_array_name, const std::vector<double>& range, 
	const DBArray& db_array, const std::string& versioned_array_name)
{

	int attr_num = db_array.get_attr_num();
	int dim_num = db_array.get_dim_num();
	DBArray result_db_array = db_array;
	result_db_array.set_array_name(result_array_name);
//	int version_num = versioned_array_names.size();
	int64_t segment_size = storage_manager->get_segment_size();

	// Get overlapping tile ids
	std::vector<std::pair<int64_t, Overlap> > overlapping_tile_ids; 
	std::vector<std::pair<int64_t, Overlap> >::const_iterator tile_id_it; 

	storage_manager->get_overlapping_tile_ids(range, versioned_array_name, overlapping_tile_ids);

	if(overlapping_tile_ids.size() == 0)
		return;

	tile_id_it = overlapping_tile_ids.begin();


	int64_t max_cell_size = std::numeric_limits<int64_t>::min();
	int attr_with_max_cell_size;
	for(int j=0; j<attr_num+1; j++)
		if(db_array.get_attr_size(j) > max_cell_size)
		{
			max_cell_size = db_array.get_attr_size(j);
			attr_with_max_cell_size = j;
		}
	
	// Prepare output tiles
	int64_t tile_ID = 0;
	Tile** output_tiles = new Tile*[attr_num+1];
	for(int j=0; j<attr_num+1; j++)
	{
		output_tiles[j] = new Tile(tile_ID, Tile::OUT, 0, &result_db_array, j);
		output_tiles[j]->set_buffer_expansion(tile_size);
	}

	char** cell = new char*[attr_num+1];
	for(int j=0; j<attr_num+1; j++)	
		cell[j] = new char[db_array.get_attr_size(j)];

	// Prepare the input tiles
	Tile** tiles = new Tile*[attr_num+1];

	int64_t cells_in_subarray = 0;
	int64_t investigated_cells = 0;

	while(tile_id_it != overlapping_tile_ids.end())
        {

                // prepare input tiles
                for(int j=0; j<attr_num+1; j++)
                        tiles[j] = storage_manager->get_tile(versioned_array_name, db_array.get_attr_name(j),
                                        (*(tile_id_it)).first, db_array);


		int64_t cell_ID;

                for(int i = 0; i < tiles[attr_num]->get_cell_num(); i++)
                {
                        // Get all cells
                        for(int j=0; j<attr_num+1; j++)
                                tiles[j]->get_cell(cell[j], i);

                        if((*(tile_id_it)).second == FULL)
                        {
                                cells_in_subarray++;
 				cell_ID = get_cell_ID(db_array, cell[attr_num], (*(tile_id_it)).first);

                                for(int j=0; j<attr_num+1; j++)
                                        output_tiles[j]->append_cell(cell[j], cell_ID);                          
                        }
                        else
                        {

                                // Get cell of coordinates
                                std::vector<double> point;
                                double p;
                                int pos = 0;
                                for(int j=0; j<dim_num; j++)
                                {
                                        if(db_array.get_dim_type(j) == DBArray::INT)
                                        {
                                                int c;
                                                memcpy(&c, cell[attr_num]+pos, sizeof(int));
                                                pos += sizeof(int);
                                                p = c;
                                        }
                                        else if(db_array.get_dim_type(j) == DBArray::INT64_T)
                                        {
                                                int64_t c;
                                                memcpy(&c, cell[attr_num]+pos, sizeof(int64_t));
                                                pos += sizeof(int64_t);
                                                p = c;
                                        }
                                        else if(db_array.get_dim_type(j) == DBArray::FLOAT)
                                        {
                                                float c;
                                                memcpy(&c, cell[attr_num]+pos, sizeof(float));
                                                pos += sizeof(float);
                                                p = c;
                                        }
                                        else if(db_array.get_dim_type(j) == DBArray::DOUBLE)
                                        {
                                                double c;
                                                memcpy(&c, cell[attr_num]+pos, sizeof(double));
                                                pos += sizeof(double);
                                                p = c;
                                        }

                                        point.push_back(p);
                                }


                                // Check if it is in the range and append
                                if(inside_range(point, range))       
                                {
					// We must append, but we first check if the output tile is full
                        		if(output_tiles[attr_with_max_cell_size]->get_tile_size() + max_cell_size > tile_size)
                        		{
                                		tile_ID++;
                                		// Store and delete the output tiles, and initialize new output tiles
                                		for(int j=0; j<attr_num+1; j++)             
                                		{
                                        		storage_manager->store(output_tiles[j], result_db_array);
                                        		delete output_tiles[j];
                                        		output_tiles[j] = new Tile(tile_ID, Tile::OUT, 0, &db_array, j);
		                                        output_tiles[j]->set_array_name(result_array_name);
                		                        output_tiles[j]->set_buffer_expansion(tile_size);
                                		}
                        		}


					cells_in_subarray++;
	 				cell_ID = get_cell_ID(db_array, cell[attr_num], (*(tile_id_it)).first);

                       	 		for(int j=0; j<attr_num+1; j++)
                                		output_tiles[j]->append_cell(cell[j], cell_ID);

                                }
                        } // end of else



                }       // end of for loop
        
                investigated_cells+=tiles[attr_num]->get_cell_num();


                // Delete input tiles
                for(int j=0; j<attr_num+1; j++)
                        delete tiles[j];

                tile_id_it++;
        } // end of while loop



	// Store the lastly created output tiles and delete them
	for(int j=0; j<attr_num+1; j++)
	{
		storage_manager->store(output_tiles[j], result_db_array);
		delete output_tiles[j];
	}

	delete [] output_tiles;

	delete [] tiles;

	
	for(int j=0; j<attr_num; j++)	
		delete [] cell[j];
	delete [] cell;


	std::cout << "cells in subarray = " << cells_in_subarray << "\n";
	fflush(stdout);

	std::cout << "investigated cells = " << investigated_cells << "\n";
	fflush(stdout);

}





void Executor::filter(const std::string& result_array_name, const std::string& query_attr_name, 
	PlanParser::CMP_OPERATOR& cmp_operator, const std::string& value, const DBArray& db_array, 
	const std::vector<std::string>& versioned_array_names)
{
	if(db_array.fixed_logical_tiles())
		filter_logical(result_array_name, query_attr_name, cmp_operator, value, db_array, versioned_array_names);
	else
		filter_physical(result_array_name, query_attr_name, cmp_operator, value, db_array, versioned_array_names);
}


void Executor::filter_logical(const std::string& result_array_name, const std::string& query_attr_name, 
	PlanParser::CMP_OPERATOR& cmp_operator, const std::string& value, const DBArray& db_array, 
	const std::vector<std::string>& versioned_array_names)
{
	int attr_num = db_array.get_attr_num();
	int dim_num = db_array.get_dim_num();
	int query_attr_id = db_array.get_attr_id(query_attr_name);
	DBArray result_db_array = db_array;
	result_db_array.set_array_name(result_array_name);
	int version_num = versioned_array_names.size();

	Tile** output_tiles = new Tile*[attr_num+1];
	
	char** cell = new char*[attr_num];
	for(int j=0; j<attr_num; j++)	
		cell[j] = new char[db_array.get_attr_size(j)];

	// FIX ME: This should change to support the 'string' data type
	std::stringstream ss(value);
	double expr_value;
	ss >> expr_value;

	// Prepare the tile ID index iterators
	std::vector<int64_t>::const_iterator* tile_id_index_its = new std::vector<int64_t>::const_iterator[version_num];
	for(int i=0; i<version_num; i++)
		tile_id_index_its[i] = storage_manager->get_tile_id_index_it_begin(versioned_array_names[i]);	
		
	// Prepare the input tiles
	Tile*** tiles = new Tile**[version_num];
	for(int i=0; i<version_num; i++)
	{
		tiles[i] = new Tile*[attr_num+1];
		for(int j=0; j<attr_num+1; j++)
			tiles[i][j] = storage_manager->get_tile(versioned_array_names[i], db_array.get_attr_name(j),
				*(tile_id_index_its[i]), db_array);
	}

	// Prepare the cell IDs
	char** coordinate_cells = new char*[version_num];
	int64_t* cell_IDs = new int64_t[version_num];
	for(int i=0; i<version_num; i++)
	{
		coordinate_cells[i] = new char[db_array.get_attr_size(attr_num)];
		tiles[i][attr_num]->get_next_cell(coordinate_cells[i]);
		cell_IDs[i] = get_cell_ID(db_array, coordinate_cells[i],
			tiles[i][attr_num]->get_tile_ID());
	}

	int64_t tile_ID = std::numeric_limits<int64_t>::min();

	int succeed = 0;

	do
	{
		// Get the minimum cell_ID
		int64_t min_cell_ID = std::numeric_limits<int64_t>::max();
		int version;
		for(int i=0; i<version_num; i++)
			if(cell_IDs[i] < min_cell_ID)
			{
				min_cell_ID = cell_IDs[i];
				version = i;
			}

		// No more cells, we must break
		if(min_cell_ID == std::numeric_limits<int64_t>::max())
			break;

		// Store the output tiles and prepare new ones
		if(tile_ID != tiles[version][0]->get_tile_ID())
		{
			if(tile_ID != std::numeric_limits<int64_t>::min())
			{
				for(int j=0; j<attr_num+1; j++)
				{
					storage_manager->store(output_tiles[j], result_db_array);
					delete output_tiles[j];
				}
			}

			tile_ID = tiles[version][0]->get_tile_ID();			
			for(int j=0; j<attr_num+1; j++)
				output_tiles[j] = new Tile(tile_ID, Tile::OUT, 0, &result_db_array, j);
//				output_tiles[j] = new Tile(tile_ID, result_db_array.get_array_name(), 
//					result_db_array.get_attr_name(j), Tile::OUT, 0, result_db_array.get_attr_size(j), result_db_array.get_dim_num());
		}

		// Append the cells that satisfy the query condition

		// Get all cells
		for(int j=0; j<attr_num; j++)
			tiles[version][j]->get_next_cell(cell[j]);

		// Check if the query attribute cell satisfies the condition
		bool satisfies_expression;
		if(db_array.get_attr_type(query_attr_id) == DBArray::INT)
		{
			int v;
			memcpy(&v, cell[query_attr_id], sizeof(int));
			satisfies_expression = evaluate_expression(v, cmp_operator, expr_value);
		}
		else if(db_array.get_attr_type(query_attr_id) == DBArray::INT64_T)
		{
			int64_t v;
			memcpy(&v, cell[query_attr_id], sizeof(int64_t));
			satisfies_expression = evaluate_expression(v, cmp_operator, expr_value);
		}
		else if(db_array.get_attr_type(query_attr_id) == DBArray::FLOAT)
		{
			float v;
			memcpy(&v, cell[query_attr_id], sizeof(float));
			satisfies_expression = evaluate_expression(v, cmp_operator, expr_value);
		}
		else if(db_array.get_attr_type(query_attr_id) == DBArray::DOUBLE)
		{
			double v;
			memcpy(&v, cell[query_attr_id], sizeof(double));
			satisfies_expression = evaluate_expression(v, cmp_operator, expr_value);
		}

		// Append			
		if(satisfies_expression)
		{
			output_tiles[attr_num]->append_cell(coordinate_cells[version]);
			for(int j=0; j<attr_num; j++)
				output_tiles[j]->append_cell(cell[j]);
		
			succeed++;
		}

		// Get next coordinate cell and determine its cell ID
		if(tiles[version][attr_num]->get_next_cell(coordinate_cells[version]))
			cell_IDs[version] = get_cell_ID(db_array, coordinate_cells[version],
				tiles[version][attr_num]->get_tile_ID());
		else // We must retrieve a new tile
		{
			tile_id_index_its[version]++;
				
			// End of tiles
			if(tile_id_index_its[version] == 
			    storage_manager->get_tile_id_index_it_end(versioned_array_names[version]))
			{
				cell_IDs[version] = std::numeric_limits<int64_t>::max();
				for(int j=0; j<attr_num+1; j++)
					delete tiles[version][j];
			}
			else
			{
				// Delete input tiles and bring new ones
				for(int j=0; j<attr_num+1; j++)
				{
					delete tiles[version][j];
					tiles[version][j] = storage_manager->get_tile(versioned_array_names[version], 
						db_array.get_attr_name(j), *(tile_id_index_its[version]), db_array);
				}

				// Get the first cell and its ID
				tiles[version][attr_num]->get_next_cell(coordinate_cells[version]);
				cell_IDs[version] = get_cell_ID(db_array, coordinate_cells[version],
					tiles[version][attr_num]->get_tile_ID());
			}
		}
		
	}while(true);
	
	// Store the lastly created output tiles and delete them
	for(int j=0; j<attr_num+1; j++)
	{
		storage_manager->store(output_tiles[j], result_db_array);
		delete output_tiles[j];
	}
	delete [] output_tiles;

	// Clear the rest of the memory
	delete [] tile_id_index_its;
	delete [] cell_IDs;
	for(int i=0; i<version_num; i++)
	{
		delete [] coordinate_cells[i];
		delete [] tiles[i];
	}
	delete [] tiles;
	delete [] coordinate_cells;
	
	for(int j=0; j<attr_num; j++)	
		delete [] cell[j];
	delete [] cell;

        std::cout << "Succeed cells = " << succeed << "\n";
        fflush(stdout);

}


void Executor::filter_physical(const std::string& result_array_name, const std::string& query_attr_name, 
	PlanParser::CMP_OPERATOR& cmp_operator, const std::string& value, const DBArray& db_array, 
	const std::vector<std::string>& versioned_array_names)
{
	int attr_num = db_array.get_attr_num();
	int dim_num = db_array.get_dim_num();
	int query_attr_id = db_array.get_attr_id(query_attr_name);
	DBArray result_db_array = db_array;
	result_db_array.set_array_name(result_array_name);
	int version_num = versioned_array_names.size();
	int64_t segment_size = storage_manager->get_segment_size();

	int64_t max_cell_size = std::numeric_limits<int64_t>::min();
	int attr_with_max_cell_size;
	for(int j=0; j<attr_num+1; j++)
		if(db_array.get_attr_size(j) > max_cell_size)
		{
			max_cell_size = db_array.get_attr_size(j);
			attr_with_max_cell_size = j;
		}
	
	int64_t tile_ID = 0;
	Tile** output_tiles = new Tile*[attr_num+1];
	for(int j=0; j<attr_num+1; j++)
	{
		output_tiles[j] = new Tile(tile_ID, Tile::OUT, 0, &result_db_array, j);
//		output_tiles[j] = new Tile(tile_ID, result_db_array.get_array_name(), 
//			result_db_array.get_attr_name(j), Tile::OUT, 0, result_db_array.get_attr_size(j), result_db_array.get_dim_num());
		output_tiles[j]->set_buffer_expansion(tile_size);
	}

	char** cell = new char*[attr_num];
	for(int j=0; j<attr_num; j++)	
		cell[j] = new char[db_array.get_attr_size(j)];

	// FIX ME: This should change to support the 'string' data type
	std::stringstream ss(value);
	double expr_value;
	ss >> expr_value;

	// Prepare the tile ID index iterators
	std::vector<int64_t>::const_iterator* tile_id_index_its = new std::vector<int64_t>::const_iterator[version_num];
	for(int i=0; i<version_num; i++)
		tile_id_index_its[i] = storage_manager->get_tile_id_index_it_begin(versioned_array_names[i]);	
		
	// Prepare the input tiles
	Tile*** tiles = new Tile**[version_num];
	for(int i=0; i<version_num; i++)
	{
		tiles[i] = new Tile*[attr_num+1];
		for(int j=0; j<attr_num+1; j++)
			tiles[i][j] = storage_manager->get_tile(versioned_array_names[i], db_array.get_attr_name(j),
				*(tile_id_index_its[i]), db_array);
	}

	// Prepare the cell IDs
	char** coordinate_cells = new char*[version_num];
	int64_t* cell_IDs = new int64_t[version_num];
	for(int i=0; i<version_num; i++)
	{
		coordinate_cells[i] = new char[db_array.get_attr_size(attr_num)];
		tiles[i][attr_num]->get_next_cell(coordinate_cells[i]);
		cell_IDs[i] = get_cell_ID(db_array, coordinate_cells[i],
			tiles[i][attr_num]->get_tile_ID());
	}

	int succeed = 0;

	do
	{
		// Get the minimum cell_ID
		int64_t min_cell_ID = std::numeric_limits<int64_t>::max();
		int version;
		for(int i=0; i<version_num; i++)
			if(cell_IDs[i] < min_cell_ID)
			{
				min_cell_ID = cell_IDs[i];
				version = i;
			}

		// No more cells, we must break
		if(min_cell_ID == std::numeric_limits<int64_t>::max())
			break;

		// Append the cells that satisfy the query condition

		// Get all cells
		for(int j=0; j<attr_num; j++)
			tiles[version][j]->get_next_cell(cell[j]);

		// Check if the query attribute cell satisfies the condition
		bool satisfies_expression;
		if(db_array.get_attr_type(query_attr_id) == DBArray::INT)
		{
			int v;
			memcpy(&v, cell[query_attr_id], sizeof(int));
			satisfies_expression = evaluate_expression(v, cmp_operator, expr_value);
		}
		else if(db_array.get_attr_type(query_attr_id) == DBArray::INT64_T)
		{
			int64_t v;
			memcpy(&v, cell[query_attr_id], sizeof(int64_t));
			satisfies_expression = evaluate_expression(v, cmp_operator, expr_value);
		}
		else if(db_array.get_attr_type(query_attr_id) == DBArray::FLOAT)
		{
			float v;
			memcpy(&v, cell[query_attr_id], sizeof(float));
			satisfies_expression = evaluate_expression(v, cmp_operator, expr_value);
		}
		else if(db_array.get_attr_type(query_attr_id) == DBArray::DOUBLE)
		{
			double v;
			memcpy(&v, cell[query_attr_id], sizeof(double));
			satisfies_expression = evaluate_expression(v, cmp_operator, expr_value);
		}

		// Append			
		if(satisfies_expression)
		{
			succeed++;
			// We must append, but we first check if the output tile is full
			if(output_tiles[attr_with_max_cell_size]->get_tile_size() + max_cell_size > tile_size)
			{
				tile_ID++;
				// Store and delete the output tiles, and initialize new output tiles
				for(int j=0; j<attr_num+1; j++)
				{
					storage_manager->store(output_tiles[j], result_db_array);
					delete output_tiles[j];
					output_tiles[j] = new Tile(tile_ID, Tile::OUT, 0, &db_array, j);
					output_tiles[j]->set_array_name(result_array_name);
//					output_tiles[j] = new Tile(tile_ID, result_array_name, 
//						db_array.get_attr_name(j), Tile::OUT, 0, db_array.get_attr_size(j), db_array.get_dim_num());
					output_tiles[j]->set_array_name(result_array_name);
					output_tiles[j]->set_buffer_expansion(tile_size);
				}
			}

			output_tiles[attr_num]->append_cell(coordinate_cells[version], cell_IDs[version]);
			for(int j=0; j<attr_num; j++)
				output_tiles[j]->append_cell(cell[j], cell_IDs[version]);
		}

		// Get next coordinate cell and determine its cell ID
		if(tiles[version][attr_num]->get_next_cell(coordinate_cells[version]))
			cell_IDs[version] = get_cell_ID(db_array, coordinate_cells[version],
				tiles[version][attr_num]->get_tile_ID());
		else // We must retrieve a new tile
		{
			tile_id_index_its[version]++;
				
			// End of tiles
			if(tile_id_index_its[version] == 
			    storage_manager->get_tile_id_index_it_end(versioned_array_names[version]))
			{
				cell_IDs[version] = std::numeric_limits<int64_t>::max();
				for(int j=0; j<attr_num+1; j++)
					delete tiles[version][j];
			}
			else
			{
				// Delete input tiles and bring new ones
				for(int j=0; j<attr_num+1; j++)
				{
					delete tiles[version][j];
					tiles[version][j] = storage_manager->get_tile(versioned_array_names[version], 
						db_array.get_attr_name(j), *(tile_id_index_its[version]), db_array);
				}

				// Get the first cell and its ID
				tiles[version][attr_num]->get_next_cell(coordinate_cells[version]);
				cell_IDs[version] = get_cell_ID(db_array, coordinate_cells[version],
					tiles[version][attr_num]->get_tile_ID());
			}
		}
		
	}while(true);
	
	// Store the lastly created output tiles and delete them
	for(int j=0; j<attr_num+1; j++)
	{
		storage_manager->store(output_tiles[j], result_db_array);
		delete output_tiles[j];
	}
	delete [] output_tiles;

	// Clear the rest of the memory
	delete [] tile_id_index_its;
	delete [] cell_IDs;
	for(int i=0; i<version_num; i++)
	{
		delete [] coordinate_cells[i];
		delete [] tiles[i];
	}
	delete [] tiles;
	delete [] coordinate_cells;
	
	for(int j=0; j<attr_num; j++)	
		delete [] cell[j];
	delete [] cell;

	std::cout << "Succeed = " << succeed << "\n";
	fflush(stdout);

}

bool Executor::evaluate_expression(const double& v1, const PlanParser::CMP_OPERATOR& cmp_operator, const double& v2)
{
	if(cmp_operator == PlanParser::STEQ && v1 <= v2)
		return true;	
	else if(cmp_operator == PlanParser::GTEQ && v1 >= v2)
		return true;	
	else if(cmp_operator == PlanParser::EQ && v1 == v2)
		return true;	
	else if(cmp_operator == PlanParser::INEQ && v1 != v2)
		return true;	
	else if(cmp_operator == PlanParser::GT && v1 > v2)
		return true;	
	else if(cmp_operator == PlanParser::ST && v1 < v2)
		return true;
	else 
		return false;	
}

*/
