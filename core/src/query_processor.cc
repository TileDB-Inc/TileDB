/**
 * @file   query_processor.cc
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
 * This file implements the QueryProcessor class.
 */
  
#include "query_processor.h"
#include <stdio.h>
#include <typeinfo>
#include <stdlib.h>
#include <iostream>
#include <sys/stat.h>
#include <assert.h>

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

QueryProcessor::QueryProcessor(const std::string& workspace, 
                               StorageManager& storage_manager) 
    : storage_manager_(storage_manager) {
  set_workspace(workspace);
  create_workspace(); 
}

void QueryProcessor::export_to_CSV(const StorageManager::ArrayDescriptor* ad,
                                   const std::string& filename) const { 
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  const std::string& array_name = array_schema.array_name();
  const unsigned int attribute_num = array_schema.attribute_num();
  const unsigned int dim_num = array_schema.dim_num();
  
  // Prepare CSV file
  CSVFile csv_file(filename, CSVFile::WRITE);

  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its = 
      new StorageManager::const_iterator[attribute_num+1];
  StorageManager::const_iterator tile_it_end;
  initialize_tile_its(ad, tile_its, tile_it_end);

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Iterate over all tiles
  while(tile_its[0] != tile_it_end) {
    // Iterate over all cells of each tile
    initialize_cell_its(tile_its, attribute_num, cell_its, cell_it_end);

    while(cell_its[0] != cell_it_end) { 
      csv_file << cell_to_csv_line(cell_its, attribute_num);
      advance_cell_its(attribute_num, cell_its);
    }
 
    advance_tile_its(attribute_num, tile_its);
  }

  // Clean up 
  delete [] tile_its;
  delete [] cell_its;
}

void QueryProcessor::subarray(const StorageManager::ArrayDescriptor* ad,
                              const Tile::Range& range,
                              const std::string& result_array_name) const { 
  if(ad->array_schema().has_regular_tiles())
    subarray_regular(ad, range, result_array_name);
  else 
    subarray_irregular(ad, range, result_array_name);
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

inline
void QueryProcessor::advance_cell_its(unsigned int attribute_num,
                                      Tile::const_iterator* cell_its) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
      ++cell_its[i];
}

inline
void QueryProcessor::advance_tile_its(
    unsigned int attribute_num, 
    StorageManager::const_iterator* tile_its) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
      ++tile_its[i];
}

inline
void QueryProcessor::append_cell(const Tile::const_iterator* cell_its,
                                 Tile** tiles,
                                 unsigned int attribute_num) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
    *tiles[i] << cell_its[i]; 
}

inline
CSVLine QueryProcessor::cell_to_csv_line(const Tile::const_iterator* cell_its,
                                         unsigned int attribute_num) const {
  CSVLine csv_line;

  // Append coordinates first
  cell_its[attribute_num] >> csv_line;
  // Append attribute values next
  for(unsigned int i=0; i<attribute_num; i++)
    cell_its[i] >> csv_line;

  return csv_line;
}

void QueryProcessor::create_workspace() const {
  struct stat st;
  stat(workspace_.c_str(), &st);

  // If the workspace does not exist, create it
  if(!S_ISDIR(st.st_mode)) { 
    int dir_flag = mkdir(workspace_.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }
}

inline
void QueryProcessor::get_tiles(
    const StorageManager::ArrayDescriptor* ad, 
    uint64_t tile_id, const Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  // Get attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++) 
   tiles[i] = storage_manager_.get_tile(ad, i, tile_id);
}

bool QueryProcessor::path_exists(const std::string& path) const {
  struct stat st;
  stat(path.c_str(), &st);
  return S_ISDIR(st.st_mode);
}

inline
void QueryProcessor::initialize_cell_its(
    const Tile** tiles, unsigned int attribute_num,
    Tile::const_iterator* cell_its, Tile::const_iterator& cell_it_end) const {
  for(unsigned int i=0; i<=attribute_num; i++)
    cell_its[i] = tiles[i]->begin();
  // Tiles with the same id have the same # of cells.
  // Thus it suffices to keep track of the end of the first cell iterator.
  cell_it_end = tiles[0]->end();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, unsigned int attribute_num,
    Tile::const_iterator* cell_its, Tile::const_iterator& cell_it_end) const {
  for(unsigned int i=0; i<=attribute_num; i++)
    cell_its[i] = (*tile_its[i]).begin();
  // Tiles with the same id have the same # of cells.
  // Thus it suffices to keep track of the end of the first cell iterator.
  cell_it_end = (*tile_its[0]).end();
}

inline
void QueryProcessor::initialize_tile_its(
    const StorageManager::ArrayDescriptor* ad,
    StorageManager::const_iterator* tile_its, 
    StorageManager::const_iterator& tile_it_end) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  for(unsigned int i=0; i<=attribute_num; i++)
    tile_its[i] = storage_manager_.begin(ad, i);
  // Every attribute has the same # of tiles.
  // Thus it suffices to keep track of the end of the first tile iterator.
  tile_it_end = storage_manager_.end(ad, 0);
}

inline
void QueryProcessor::new_tiles(const ArraySchema& array_schema, 
                               uint64_t tile_id, Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  for(unsigned int i=0; i<=attribute_num; i++)
    tiles[i] = storage_manager_.new_tile(array_schema, i, tile_id, capacity);
}

inline
void QueryProcessor::set_workspace(const std::string& path) {
  workspace_ = path;
  
  // Replace '~' with the absolute path
  if(workspace_[0] == '~') {
    workspace_ = std::string(getenv("HOME")) +
                 workspace_.substr(1, workspace_.size()-1);
  }

  // Check if the input path is an existing directory 
  assert(path_exists(workspace_));
 
  workspace_ += "/QueryProcessor";
}

inline
void QueryProcessor::store_tiles(const StorageManager::ArrayDescriptor* ad,
                                 Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  // Append attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++)
    storage_manager_.append_tile(tiles[i], ad, i);
} 

void QueryProcessor::subarray_irregular(
    const StorageManager::ArrayDescriptor* ad,
    const Tile::Range& range, const std::string& result_array_name) const { 
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  const unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  // Create tiles and tile iterators
  const Tile** tiles = new const Tile*[attribute_num+1];
  Tile** result_tiles = new Tile*[attribute_num+1];
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Prepare result array
  ArraySchema result_array_schema = array_schema.clone(result_array_name);
  const StorageManager::ArrayDescriptor* result_ad = 
      storage_manager_.open_array(result_array_schema);
  
  // Get the tile ids that overlap with the range
  std::vector<std::pair<uint64_t, bool> > overlapping_tile_ids;
  storage_manager_.get_overlapping_tile_ids(ad, range, &overlapping_tile_ids);

  // Initialize tile iterators
  std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it = 
      overlapping_tile_ids.begin();
  std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it_end =
      overlapping_tile_ids.end();
      
  // Create result tiles and load input array tiles 
  uint64_t tile_id = 0;
  new_tiles(result_array_schema, tile_id, result_tiles); 

  // Iterate over all tiles
  for(; tile_id_it != tile_id_it_end; ++tile_id_it) {
    get_tiles(ad, tile_id_it->first, tiles);
    initialize_cell_its(tiles, attribute_num, cell_its, cell_it_end); 

    if(tile_id_it->second) { // Full overlap
      while(cell_its[0] != cell_it_end) {
        if(result_tiles[0]->cell_num() == capacity) {
          store_tiles(result_ad, result_tiles);
          new_tiles(array_schema, ++tile_id, result_tiles); 
        }
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      }
    } else { // Partial overlap
      while(cell_its[0] != cell_it_end) {
        if(cell_its[attribute_num].cell_inside_range(range)) {
          if(result_tiles[0]->cell_num() == capacity) {
            store_tiles(result_ad, result_tiles);
            new_tiles(array_schema, ++tile_id, result_tiles); 
          }
          append_cell(cell_its, result_tiles, attribute_num);
        }
        advance_cell_its(attribute_num, cell_its);
      }
    }
      
    // Send the lastly created tiles to storage manager
    store_tiles(result_ad, result_tiles);
  } 

  // Clean up 
  storage_manager_.close_array(result_ad);
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
}

void QueryProcessor::subarray_regular(
    const StorageManager::ArrayDescriptor* ad,
    const Tile::Range& range, const std::string& result_array_name) const { 
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  const unsigned int attribute_num = array_schema.attribute_num();

  // Create tiles and tile iterators
  const Tile** tiles = new const Tile*[attribute_num+1];
  Tile** result_tiles = new Tile*[attribute_num+1];
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Prepare result array
  ArraySchema result_array_schema = array_schema.clone(result_array_name);
  const StorageManager::ArrayDescriptor* result_ad = 
      storage_manager_.open_array(result_array_schema);
    
  // Get the tile ids that overlap with the range
  std::vector<std::pair<uint64_t, bool> > overlapping_tile_ids;
  storage_manager_.get_overlapping_tile_ids(ad, range, &overlapping_tile_ids);

  // Initialize tile iterators
  std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it = 
      overlapping_tile_ids.begin();
  std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it_end =
      overlapping_tile_ids.end();

  // Iterate over all overlapping tiles
  for(; tile_id_it != tile_id_it_end; ++tile_id_it) {
    // Create result tiles and load input array tiles 
    new_tiles(result_array_schema, tile_id_it->first, result_tiles); 
    get_tiles(ad, tile_id_it->first, tiles); 
    initialize_cell_its(tiles, attribute_num, cell_its, cell_it_end); 
 
    if(tile_id_it->second) { // Full overlap
      while(cell_its[0] != cell_it_end) {
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      }
    } else { // Partial overlap
      while(cell_its[0] != cell_it_end) {
        if(cell_its[attribute_num].cell_inside_range(range))
          append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      }
    }
      
    // Send new tiles to storage manager
    store_tiles(result_ad, result_tiles);
  } 

  // Clean up 
  storage_manager_.close_array(result_ad);
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
}

