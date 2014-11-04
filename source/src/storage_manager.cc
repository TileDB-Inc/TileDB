/**
 * @file   storage_manager.cc
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
 * This file implements the StorageManager class.
 */

#include "storage_manager.h"
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <typeinfo>
#include <iostream>

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

StorageManager::StorageManager(const std::string& path, 
                               uint64_t segment_size) 
                              : segment_size_(segment_size){
  set_workspace(path); 
  create_workspace();
}

StorageManager::~StorageManager() {
  OpenArraysIndex::const_iterator array_it = open_arrays_index_.begin();
  OpenArraysIndex::const_iterator array_it_end = open_arrays_index_.end();

  // Close all open arrays
  for(; array_it != array_it_end; array_it++)
    close_array(array_it->first);
}

/******************************************************
******************* ARRAY FUNCTIONS *******************
******************************************************/

StorageManager::ArrayMode StorageManager::array_mode(
    const std::string& array_name) const {
  OpenArraysIndex::const_iterator array_it =
      open_arrays_index_.find(array_name);

  if(array_it == open_arrays_index_.end())
    return NOT_OPEN;
  else 
    return array_it->second;
}

void StorageManager::close_array(const std::string& array_name) {
  // Perform check
  check_array_on_close(array_name);  

  // Flush tiles and indices 
  if(open_arrays_index_[array_name] == CREATE) {
    flush_tiles(array_name);
    delete_tiles(array_name);
    check_array_correctness_on_close(array_name);
    flush_indices(array_name);
    delete_indices(array_name);
  } else {
    delete_tiles(array_name);
    delete_indices(array_name);
  }
}

void StorageManager::delete_array(const std::string& array_name) {
  delete_tiles(array_name);
  delete_indices(array_name);
  delete_directory(array_name);
}

bool StorageManager::is_empty(const std::string& array_name) const {
  return (attribute_name_index_.find(array_name) ==  
              attribute_name_index_.end());
}

bool StorageManager::is_open(const std::string& array_name) const {
  return (open_arrays_index_.find(array_name) != open_arrays_index_.end());
}

void StorageManager::open_array(const std::string& array_name, ArrayMode mode) {
  // Perform check
  check_array_on_open(array_name, mode);
  
  // Prepare indices and directories
  if(mode == READ) { 
    try {
      load_indices(array_name);
    } catch(StorageManagerException& sme) {
      delete_indices(array_name);
      throw sme;
    }
  } else if(mode == CREATE) {
    create_array_directory(array_name);
  }
  
  // Update index
  open_arrays_index_[array_name] = mode;
}

/******************************************************
******************** TILE FUNCTIONS *******************
******************************************************/

void StorageManager::append_tile(const Tile* tile, 
                                 const std::string& array_name,
                                 const std::string& attribute_name) {
  // Do nothing if tile is empty
  if(tile->cell_num() == 0)
    return;

  // Perform checks
  check_array_on_append_tile(array_name);
  check_tile_id_on_append_tile(array_name, attribute_name, tile->tile_id());
 
  // Update indices
  update_attribute_name_index_on_append_tile(array_name, attribute_name);
  update_tile_id_index_on_append_tile(array_name, attribute_name, 
                                      tile->tile_id());
  update_tile_index_on_append_tile(array_name, attribute_name, tile);
  
  bool exceeded_segment_size = 
      update_payload_size_index_on_append_tile(
          array_name, attribute_name, tile->tile_size());

  // Flush tiles to disk if the sum of payloads exceeds the segment size
  if(exceeded_segment_size) { 
    flush_tiles(array_name, attribute_name);
    delete_tiles(array_name, attribute_name);
  }
} 

void StorageManager::append_tile(const Tile* tile, 
                                 const std::string& array_name) {
  // Do nothing if tile is empty
  if(tile->cell_num() == 0)
    return;

  // Preform checks
  check_dim_num_on_append_tile(array_name, tile->mbr());

  
  // Update the indices relevant only to coordinate tiles
  update_MBR_index_on_append_tile(array_name, tile->mbr());
  update_bounding_coordinates_index_on_append_tile(
      array_name, tile->bounding_coordinates());

  // Invoke the same commands as in the case of appending an attribute tile
  append_tile(tile, array_name, SM_COORDINATE_TILE_NAME);  
}

template<class T>
const Tile* StorageManager::get_tile(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id) {
  check_array_on_get_tile(array_name);
  check_tile_id_on_get_tile(array_name, attribute_name, tile_id);
  
  const Tile* tile = get_tile_from_tile_index(array_name, 
                                              attribute_name, tile_id);
  
  if(tile == NULL) {
    load_tiles<T>(array_name, attribute_name, tile_id);
    tile = get_tile_from_tile_index(array_name, attribute_name, tile_id);
  }

  return tile;
}

template<class T>
const Tile* StorageManager::get_tile(
    const std::string& array_name, uint64_t tile_id) {
  check_array_on_get_tile(array_name);
  check_tile_id_on_get_tile(array_name, SM_COORDINATE_TILE_NAME, tile_id);

  const Tile* tile = get_tile_from_tile_index(array_name, 
                                              SM_COORDINATE_TILE_NAME,
                                              tile_id);

  if(tile == NULL) {
    load_tiles<T>(array_name, tile_id);
    tile = get_tile_from_tile_index(array_name, SM_COORDINATE_TILE_NAME, 
                                    tile_id);
  }

  return tile;
}

/******************************************************
******************** TILE ITERATORS *******************
******************************************************/

StorageManager::const_iterator::const_iterator() 
    : storage_manager_(NULL), cell_type_(NULL) {
}

StorageManager::const_iterator::const_iterator(
    StorageManager* storage_manager,
    const std::string& array_name,
    const std::string& attribute_name,
    const TileIDToRank::const_iterator& tile_id_it,
    const std::type_info* cell_type)
    : storage_manager_(storage_manager), 
      array_name_(array_name), attribute_name_(attribute_name),
      tile_id_it_(tile_id_it), cell_type_(cell_type) {
}

void StorageManager::const_iterator::operator=(
    const StorageManager::const_iterator& rhs) {
  array_name_ = rhs.array_name_;
  attribute_name_ = rhs.attribute_name_;
  storage_manager_ = rhs.storage_manager_;
  tile_id_it_ = rhs.tile_id_it_;
  cell_type_ = rhs.cell_type_;
}

StorageManager::const_iterator StorageManager::const_iterator::operator++() {
  ++tile_id_it_;
  return *this;
}

StorageManager::const_iterator StorageManager::const_iterator::operator++(
    int junk) {
  const_iterator it = *this;
  ++tile_id_it_;
  return it;
}

bool StorageManager::const_iterator::operator==(
    const StorageManager::const_iterator& rhs) const {
  return tile_id_it_ == rhs.tile_id_it_; 
}

bool StorageManager::const_iterator::operator!=(
    const StorageManager::const_iterator& rhs) const {
  return tile_id_it_ != rhs.tile_id_it_; 
}

const Tile& StorageManager::const_iterator::operator*() const {
  if(attribute_name_ != SM_COORDINATE_TILE_NAME) { // Return attribute tile
    if(*cell_type_ == typeid(int)) 
      return *storage_manager_->get_tile<int>(array_name_, 
                                              attribute_name_, 
                                              tile_id_it_->first);
    else if(*cell_type_ == typeid(int64_t)) 
      return *storage_manager_->get_tile<int64_t>(array_name_, 
                                                  attribute_name_, 
                                                  tile_id_it_->first);
    else if(*cell_type_ == typeid(float)) 
      return *storage_manager_->get_tile<float>(array_name_, 
                                                attribute_name_, 
                                                tile_id_it_->first);
    else if(*cell_type_ == typeid(double)) 
      return *storage_manager_->get_tile<double>(array_name_, 
                                                 attribute_name_, 
                                                 tile_id_it_->first);
  } else { // Return coordinate tile
    if(*cell_type_ == typeid(int)) 
      return *storage_manager_->get_tile<int>(array_name_, 
                                              tile_id_it_->first);
    else if(*cell_type_ == typeid(int64_t)) 
      return *storage_manager_->get_tile<int64_t>(array_name_, 
                                                  tile_id_it_->first);
    else if(*cell_type_ == typeid(float)) 
      return *storage_manager_->get_tile<float>(array_name_, 
                                                tile_id_it_->first);
    else if(*cell_type_ == typeid(double)) 
      return *storage_manager_->get_tile<double>(array_name_, 
                                                 tile_id_it_->first);
  }
}

StorageManager::const_iterator StorageManager::begin(
    const std::string& array_name,
    const std::type_info& cell_type) {
  TileIDIndex::const_iterator array_it = tile_id_index_.find(array_name);
  if(array_it == tile_id_index_.end())
    throw StorageManagerException("Cannot return begin iterator: cannot find" 
                                  " tile id list for array '" + array_name +
                                  "'."); 

  // When in READ mode, there exists a single tile id list/map in 
  // StorageManager::tile_id_index_[array_name]
  AttributeToTileIDToRank::const_iterator attribute_it = 
      array_it->second.begin();
  if(attribute_it == array_it->second.end())
    throw StorageManagerException("Cannot return begin iterator: cannot find" 
                                  " tile id list for array '" + array_name +
                                  "'."); 
  
  return const_iterator(this, array_name, SM_COORDINATE_TILE_NAME,
                         attribute_it->second.begin(), &cell_type);
}

StorageManager::const_iterator StorageManager::begin(
    const std::string& array_name, const std::string& attribute_name,
    const std::type_info& cell_type) {
  TileIDIndex::const_iterator array_it = tile_id_index_.find(array_name);
  if(array_it == tile_id_index_.end())
    throw StorageManagerException("Cannot return begin iterator: cannot find" 
                                  " tile id list for array '" + array_name +
                                  "'."); 

  // When in READ mode, there exists a single tile id list/map in 
  // StorageManager::tile_id_index_[array_name]
  AttributeToTileIDToRank::const_iterator attribute_it = 
      array_it->second.begin();
  if(attribute_it == array_it->second.end())
    throw StorageManagerException("Cannot return begin iterator: cannot find" 
                                  " tile id list for array '" + array_name +
                                  "'."); 
  
  return  const_iterator(this, array_name, attribute_name,
                         attribute_it->second.begin(), &cell_type);
}

StorageManager::const_iterator StorageManager::end(
    const std::string& array_name,
    const std::type_info& cell_type) {
  TileIDIndex::const_iterator array_it = tile_id_index_.find(array_name);
  if(array_it == tile_id_index_.end())
    throw StorageManagerException("Cannot return end iterator: cannot find" 
                                  " tile id list for array '" + array_name +
                                  "'."); 

  // When in READ mode, there exists a single tile id list/map in 
  // StorageManager::tile_id_index_[array_name]
  AttributeToTileIDToRank::const_iterator attribute_it = 
      array_it->second.begin();
  if(attribute_it == array_it->second.end())
    throw StorageManagerException("Cannot return end iterator: cannot find" 
                                  " tile id list for array '" + array_name +
                                  "'."); 
  
  return  const_iterator(this, array_name, SM_COORDINATE_TILE_NAME,
                         attribute_it->second.end(), &cell_type);
}

StorageManager::const_iterator StorageManager::end(
    const std::string& array_name, const std::string& attribute_name,
    const std::type_info& cell_type) { 
  TileIDIndex::const_iterator array_it = tile_id_index_.find(array_name);
  if(array_it == tile_id_index_.end())
    throw StorageManagerException("Cannot return end iterator: cannot find" 
                                  " tile id list for array '" + array_name +
                                  "'."); 

  // When in READ mode, there exists a single tile id list/map in 
  // StorageManager::tile_id_index_[array_name]
  AttributeToTileIDToRank::const_iterator attribute_it = 
      array_it->second.begin();
  if(attribute_it == array_it->second.end())
    throw StorageManagerException("Cannot return end iterator: cannot find" 
                                  " tile id list for array '" + array_name +
                                  "'."); 
  
  return  const_iterator(this, array_name, attribute_name,
                         attribute_it->second.end(), &cell_type);
}


StorageManager::MBRList::const_iterator StorageManager::MBR_begin(
    const std::string& array_name) const {
  MBRIndex::const_iterator array_it = MBR_index_.find(array_name);
  if(array_it ==  MBR_index_.end())
    throw StorageManagerException("Cannot return MBR begin iterator: cannot find" 
                                  " MBR list for array '" + array_name +
                                  "'."); 
  return array_it->second.begin(); 
}

StorageManager::MBRList::const_iterator StorageManager::MBR_end(
    const std::string& array_name) const {
  MBRIndex::const_iterator array_it = MBR_index_.find(array_name);
  if(array_it ==  MBR_index_.end())
    throw StorageManagerException("Cannot return MBR begin iterator: cannot find" 
                                  " MBR list for array '" + array_name +
                                  "'."); 
  return array_it->second.end(); 
}

/******************************************************
*********************** MISC **************************
******************************************************/

void StorageManager::get_overlapping_tile_ids(
    const std::string& array_name, const Range& range,
    std::vector<std::pair<uint64_t, bool> >* overlapping_tile_ids) const {
  // Initialization
  assert(is_open(array_name));
  MBRIndex::const_iterator MBR_array_it = MBR_index_.find(array_name);
  assert(MBR_array_it != MBR_index_.end());
  MBRList::const_iterator MBR_it = MBR_array_it->second.begin();
  assert(MBR_it != MBR_array_it->second.end());
  TileIDIndex::const_iterator TII_array_it = tile_id_index_.find(array_name);
  assert(TII_array_it != tile_id_index_.end());
  AttributeToTileIDToRank::const_iterator TII_attribute_it = 
      TII_array_it->second.begin();
  assert(MBR_array_it->second.size() == TII_attribute_it->second.size());
  TileIDToRank::const_iterator tile_id_it = TII_attribute_it->second.begin();
  TileIDToRank::const_iterator tile_id_it_end = TII_attribute_it->second.end();

  std::pair<uint64_t, bool> overlapping_tile_id;
  unsigned int dim_num = range.size() / 2;

  // Overlap type per dimension
  bool* partial = new bool[dim_num];
  bool* full = new bool[dim_num];

  // True if an MBR overlaps (partially or fully) the range
  bool overlap;

  for(; tile_id_it != tile_id_it_end; tile_id_it++, MBR_it++) {
    const MBR& mbr = *MBR_it;
    overlap = true;
    overlapping_tile_id.second = true;

    // Determine overlap per dimension
    for(unsigned int i=0; i<dim_num; i++) {
      partial[i] = false;
      full[i] = false;
      if(mbr[2*i] >= range[2*i] && mbr[2*i+1] <= range[2*i+1])
        full[i] = true;
      else if(range[2*i] >= mbr[2*i] && range[2*i] <= mbr[2*i+1] || 
              range[2*i+1] >= mbr[2*i] && range[2*i+1] <= mbr[2*i+1])
        partial[i] = true;
    }

    // Determine MBR overlap
    for(unsigned int i=0; i<dim_num; i++) {
      if(!partial[i] && !full[i]) {
        overlap = false;
        break;
      }
  
      if(partial[i])
        overlapping_tile_id.second = false;
    }

    // Insert overlapping tile id into the result list
    if(overlap) {
      overlapping_tile_id.first = tile_id_it->first;
      overlapping_tile_ids->push_back(overlapping_tile_id);
    }
  }

  delete [] partial;
  delete [] full;
}

/******************************************************
***************** PRIVATE FUNCTIONS *******************
******************************************************/

bool StorageManager::array_is_correct(const std::string& array_name) const {
  // Rule#1: If an array is 'empty', it is in 'correct' form.
  if(array_is_empty(array_name))
    return true;
 
  assert_attribute_entries(array_name);

  // Rule#2: All tile entries must be correct
  if(!tile_entries_are_correct(array_name))
    return false;

  return true;
}

bool StorageManager::array_is_empty(const std::string& array_name) const {
  if(attribute_name_index_.find(array_name) != attribute_name_index_.end()) {
    return false;
  } else { 
    // Proper assertions: if there is no entry for array_name in
    // attribute_name_index_, then there should be no entry for array_name in 
    // any other index.
    assert(tile_id_index_.find(array_name) == tile_id_index_.end());
    assert(offset_index_.find(array_name) == offset_index_.end());
    assert(MBR_index_.find(array_name) == MBR_index_.end());
    assert(bounding_coordinates_index_.find(array_name) == 
           bounding_coordinates_index_.end());

    return true;
  }
}

void StorageManager::assert_attribute_entries(
    const std::string& array_name) const {
  AttributeNameIndex::const_iterator ANI_array_it = 
      attribute_name_index_.find(array_name);
  TileIDIndex::const_iterator TII_array_it = 
      tile_id_index_.find(array_name);
  OffsetIndex::const_iterator OI_array_it = 
      offset_index_.find(array_name);
  MBRIndex::const_iterator MBRI_array_it = 
      MBR_index_.find(array_name);
  BoundingCoordinatesIndex::const_iterator BCI_array_it = 
      bounding_coordinates_index_.find(array_name);

  // Assert that no attribute set/map is empty  
  assert(ANI_array_it != attribute_name_index_.end() &&
     TII_array_it != tile_id_index_.end() &&
     OI_array_it != offset_index_.end());

  // Assert that (i) when a coordinate tile name is present in
  // AttributeNameIndex, entries exist in the MBRIndex and CellIDRangeIndex, 
  // or (ii) no entry exists in any of the three indices
  AttributeSet::const_iterator ANI_attribute_it = 
      ANI_array_it->second.find(SM_COORDINATE_TILE_NAME);
  assert((ANI_attribute_it != ANI_array_it->second.end() &&
         MBRI_array_it != MBR_index_.end() &&
         BCI_array_it != bounding_coordinates_index_.end()) 
         ||
         (ANI_attribute_it == ANI_array_it->second.end() &&
         MBRI_array_it == MBR_index_.end() &&
         BCI_array_it == bounding_coordinates_index_.end())); 

  // All indices must have the same attribute entries
  ANI_attribute_it = ANI_array_it->second.begin();
  AttributeSet::const_iterator ANI_attribute_it_end = 
      ANI_array_it->second.end();
  AttributeToTileIDToRank::const_iterator TII_attribute_it =
      TII_array_it->second.begin();
  AttributeToOffsetList::const_iterator OI_attribute_it =
      OI_array_it->second.begin();
  for(; ANI_attribute_it != ANI_attribute_it_end; ++ANI_attribute_it) {
    // Assert that no attribute list is shorter
    assert(TII_attribute_it != TII_array_it->second.end() &&
           OI_attribute_it != OI_array_it->second.end());
    // Assert that all attributes in all indices are the same
    assert(TII_attribute_it->first == *ANI_attribute_it &&
           OI_attribute_it->first == *ANI_attribute_it);
    TII_attribute_it++;
    OI_attribute_it++;
  }
  // Assert that no attribute list is longer_
  assert(TII_attribute_it == TII_array_it->second.end() &&
         OI_attribute_it == OI_array_it->second.end());
}

void StorageManager::check_array_correctness_on_close(
    const std::string& array_name) const {
  if(!array_is_correct(array_name))
    throw StorageManagerException("Cannot close array: array is not in "
                                  "'correct form'.", array_name); 
}

inline
void StorageManager::check_array_on_append_tile(
    const std::string& array_name) const {
  OpenArraysIndex::const_iterator array_it = 
      open_arrays_index_.find(array_name);

  if(array_it == open_arrays_index_.end())
    throw StorageManagerException("Cannot append tile to array" 
                                  ": array is not open.", array_name);
  else if(array_it->second == READ)
    throw StorageManagerException("Cannot append tile to array"
                                  ": array is open in READ mode.", array_name);
}

inline
void StorageManager::check_array_on_close(const std::string& array_name) const {
  OpenArraysIndex::const_iterator array_it = 
      open_arrays_index_.find(array_name);
  if(array_it == open_arrays_index_.end())
    throw StorageManagerException("Cannot close array: array is not open.", 
                                  array_name);
}

inline
void StorageManager::check_array_on_open(const std::string& array_name, 
                                         ArrayMode mode) const {
  OpenArraysIndex::const_iterator array_it = 
      open_arrays_index_.find(array_name);
 
  // Check if the array is already open
  if(array_it != open_arrays_index_.end())
    throw StorageManagerException("Cannot open array: array is already open "
                                  "in " + std::string(
                                              ((array_it->second == READ) 
                                                ? "READ" : "CREATE")) +
                                  " mode.", array_name);

  // Check if the directory of the array exists in the workspace
  std::string dir_name = workspace_ + "/" + array_name;
  struct stat st;
  stat(dir_name.c_str(), &st);
  if(mode == CREATE && S_ISDIR(st.st_mode))
    throw StorageManagerException("Cannot open array in CREATE mode: "
                                  "array directory '" + dir_name + 
                                  "' already exists.", array_name);
  else if(mode == READ && !S_ISDIR(st.st_mode))
    throw StorageManagerException("Cannot open array: array directory '" +
                                  dir_name + "' not found.", 
                                  array_name);
  else if(mode != READ && mode != CREATE) // Invalid mode
    throw StorageManagerException("Cannot open array: invalid mode.", 
                                  array_name);
}

inline
void StorageManager::check_array_on_get_tile(
    const std::string& array_name) const {
  OpenArraysIndex::const_iterator array_it = 
      open_arrays_index_.find(array_name);

  // Check if the array is properly opened
  if(array_it == open_arrays_index_.end())
    throw StorageManagerException("Cannot get tile from array"
                                  ": array is not open.", array_name);
  else if(array_it->second == CREATE)
    throw StorageManagerException("Cannot get tile from array" 
                                  ": array is open in CREATE mode.", array_name);

}

inline
void StorageManager::check_dim_num_on_append_tile(
    const std::string& array_name, const MBR& mbr) const {
  MBRIndex::const_iterator array_it = MBR_index_.find(array_name);

  if(array_it != MBR_index_.end() && array_it->second.size() != 0 && 
     array_it->second.begin()->size() != mbr.size())
    throw StorageManagerException("Cannot append coordinate tile: the number "
                                  "of dimensions of the input tile must be "
                                  "the same as that of the stored one(s).", 
                                  array_name);
}

inline
void StorageManager::check_tile_id_on_append_tile(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id) const {
  TileIDIndex::const_iterator array_it = tile_id_index_.find(array_name);
  if(array_it != tile_id_index_.end()) {
    AttributeToTileIDToRank::const_iterator attribute_it =
        array_it->second.find(attribute_name);

    if(attribute_it != array_it->second.end() &&
       attribute_it->second.size() != 0 &&
       attribute_it->second.rbegin()->first >= tile_id)
      throw StorageManagerException("Cannot append tile: tiles must be "
                                     "appended in increasing tile id order.", 
                                     array_name, attribute_name, tile_id);
  }
}

// Note: When the array is opened in READ mode, the tile
// ids are loaded only for a single attribute, in order
// to avoid redundancy.
void StorageManager::check_tile_id_on_get_tile(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id) const {
  TileIDIndex::const_iterator array_it = tile_id_index_.find(array_name);
  if(array_it == tile_id_index_.end())
    throw StorageManagerException("Cannot find requested tile id: array "
                                  "not in the tile id index.", array_name,
                                  attribute_name, tile_id);

  AttributeToTileIDToRank::const_iterator attribute_it = 
      array_it->second.begin();
  if(attribute_it == array_it->second.end())
    throw StorageManagerException("Cannot find requested tile: tile id list "
                                  "is empty.", array_name,
                                  attribute_name, tile_id);
 
  TileIDToRank::const_iterator tile_id_it = attribute_it->second.find(tile_id);
  if(tile_id_it == attribute_it->second.end())
    throw StorageManagerException("Cannot find requested tile id: tile id not "
                                  "in the tile id index.", array_name,
                                  attribute_name, tile_id);
}

void StorageManager::create_array_directory(const std::string& array_name) {
  std::string dir_name = workspace_ + "/" + array_name;

  if(mkdir(dir_name.c_str(), S_IRWXU) != 0)
    throw StorageManagerException("Cannot create array directory '" + 
                                   dir_name + "'.", array_name); 
}

void StorageManager::create_workspace() {
  // If the workspace does not exist, create it
  struct stat st;
  stat(workspace_.c_str(), &st);
  if(!S_ISDIR(st.st_mode)) 
    if(mkdir(workspace_.c_str(), S_IRWXU) != 0)
      throw StorageManagerException("Cannot create workspace.");
}

void StorageManager::delete_directory(const std::string& array_name) {
  std::string dirname = workspace_ + "/" + array_name + "/";
  std::string filename; 

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0)
      continue;
    filename = dirname + next_file->d_name;
    remove(filename.c_str());
  } 
  
  closedir(dir);
  rmdir(dirname.c_str());
}

void StorageManager::delete_indices(const std::string& array_name) {
  attribute_name_index_.erase(array_name);
  open_arrays_index_.erase(array_name);
  tile_id_index_.erase(array_name);
  tile_index_.erase(array_name);
  offset_index_.erase(array_name);
  payload_size_index_.erase(array_name);
  MBR_index_.erase(array_name);
  bounding_coordinates_index_.erase(array_name);
}

void StorageManager::delete_tiles(const std::string& array_name) {
  TileIndex::const_iterator array_it = tile_index_.begin();
  if(array_it != tile_index_.end()) {
    AttributeToTileList::const_iterator attribute_it = 
        array_it->second.begin();
    AttributeToTileList::const_iterator attribute_it_end = 
        array_it->second.end();

    for(; attribute_it != attribute_it_end; attribute_it++)
      delete_tiles(array_name, attribute_it->first);
  }
}

void StorageManager::delete_tiles(const std::string& array_name,
                                  const std::string& attribute_name) {
  // Delete tiles
  TileIndex::iterator TI_array_it = tile_index_.find(array_name);
  if(TI_array_it != tile_index_.end()) {
    AttributeToTileList::iterator TI_attribute_it = 
        TI_array_it->second.find(attribute_name);
    if(TI_attribute_it != TI_array_it->second.end()) {
      TileList::iterator tile_it = TI_attribute_it->second.begin();
      TileList::iterator tile_it_end = TI_attribute_it->second.end();
      for(; tile_it != tile_it_end; tile_it++)
        delete *tile_it;
    }
  }

  // Update indices 
  TI_array_it->second.erase(attribute_name);
  PayloadSizeIndex::iterator PSI_array_it = 
      payload_size_index_.find(array_name);
  if(PSI_array_it != payload_size_index_.end())
    PSI_array_it->second.erase(attribute_name);
}

// FILE FORMAT:
// attribute_num(unsigned int) 
//   attribute#1_name_size(unsigned int) attribute#1_name(string) 
//   attribute#2_name_size(unsigned int) attribute#2_name(string) 
//   ...
void StorageManager::flush_attribute_name_index(const std::string& array_name) {
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_ATTRIBUTE_NAME_INDEX_NAME + SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1)
    throw StorageManagerException("Cannot create index file '" + 
                                  filename + "'.", array_name);

  AttributeNameIndex::const_iterator array_it = 
      attribute_name_index_.find(array_name);

  if(array_it != attribute_name_index_.end()) {
    // Prepare the buffer
    AttributeSet::const_iterator attribute_it = array_it->second.begin();
    AttributeSet::const_iterator attribute_it_end = array_it->second.end();
    unsigned int attribute_num = array_it->second.size();
    uint64_t buffer_size = sizeof(unsigned int);
    for(; attribute_it != attribute_it_end; attribute_it++)
      buffer_size += sizeof(unsigned int) + attribute_it->size();
    char* buffer = new char[buffer_size];

    // Populate buffer 
    memcpy(buffer, &attribute_num, sizeof(unsigned int));
    uint64_t offset = sizeof(unsigned int);
    unsigned int attribute_name_size;
    attribute_it = array_it->second.begin();
    for(; attribute_it != attribute_it_end; attribute_it++) {
      attribute_name_size = attribute_it->size();
      memcpy(buffer+offset, &(attribute_name_size), sizeof(unsigned int));
      offset += sizeof(unsigned int);
      memcpy(buffer+offset, attribute_it->c_str(), attribute_name_size);
      offset += attribute_name_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete buffer;
  }

  close(fd);
}

// FILE FORMAT:
// tile#1_lower_dim#1(double) tile#1_lower_dim#2(double) ... 
// tile#1_upper_dim#1(double) tile#1_upper_dim#2(double) ... 
// tile#2_lower_dim#1(double) tile#2_lower_dim#2(double) ... 
// tile#2_upper_dim#1(double) tile#2_upper_dim#2(double) ...
// ... 
void StorageManager::flush_bounding_coordinates_index(
    const std::string& array_name) {
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_BOUNDING_COORDINATES_INDEX_NAME + 
                         SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1)
    throw StorageManagerException("Cannot create index file '" + 
                                  filename + "'.", array_name);
 
  BoundingCoordinatesIndex::const_iterator array_it =
      bounding_coordinates_index_.find(array_name);
  
  if(array_it != bounding_coordinates_index_.end()) {
    // Prepare the buffer
    unsigned int dim_num = array_it->second[0].first.size();
    uint64_t tile_num = array_it->second.size();
    uint64_t buffer_size = 2 * tile_num * dim_num * sizeof(double);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    BoundingCoordinatesList::const_iterator bound_it = 
        array_it->second.begin();
    BoundingCoordinatesList::const_iterator bound_it_end = 
        array_it->second.end();
    uint64_t offset = 0;
    for(; bound_it != bound_it_end; bound_it++) {
      for(unsigned int i=0; i<dim_num; i++) {
        memcpy(buffer+offset, &(bound_it->first[i]), sizeof(double));
        offset += sizeof(double);
      }
      
      for(unsigned int i=0; i<dim_num; i++) {
        memcpy(buffer+offset, &(bound_it->second[i]), sizeof(double));
        offset += sizeof(double);
      }
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete buffer;
  }  

  close(fd);
}

void StorageManager::flush_indices(const std::string& array_name) {
  flush_attribute_name_index(array_name);
  flush_tile_id_index(array_name);
  flush_offset_index(array_name);
  flush_MBR_index(array_name);
  flush_bounding_coordinates_index(array_name);
}

// FILE FORMAT:
// MBR#1_bound#1(double) MBR#1_bound#2(double) ...
// MBR#2_bound#1(double) MBR#2_bound#2(double) ...
// ...
void StorageManager::flush_MBR_index(const std::string& array_name) {
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_MBR_INDEX_NAME + SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1)
    throw StorageManagerException("Cannot create index file '" + 
                                  filename + "'.", array_name);
  
  MBRIndex::const_iterator array_it = MBR_index_.find(array_name);

  if(array_it != MBR_index_.end()) {
    // Prepare the buffer
    MBRList::const_iterator MBR_it = array_it->second.begin();
    MBRList::const_iterator MBR_it_end = array_it->second.end();
    uint64_t tile_num = array_it->second.size();
    // Note that care has been taken already so that each MBR in the index
    // has the same number of elements/bounds. Therefore, we can extract
    // bound_num only from the first MBR in the list
    unsigned int bound_num = MBR_it->size();
    uint64_t buffer_size = tile_num * bound_num * sizeof(double);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    uint64_t offset = 0;
    for(; MBR_it != MBR_it_end; MBR_it++) {
      for(unsigned int i=0; i<bound_num; i++) {
        memcpy(buffer+offset, &((*MBR_it)[i]), sizeof(double));
        offset += sizeof(double);
      }
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete buffer;
  }

  close(fd);
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(uint64_t)
// tile#2_of_attribute#1_offset(uint64_t)
// ...
// tile#1_of_attribute#2_offset(uint64_t)
// tile#2_of_attribute#2_offset(uint64_t)
// ...
void StorageManager::flush_offset_index(const std::string& array_name) {
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_OFFSET_INDEX_NAME + SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1)
    throw StorageManagerException("Cannot create index file '" + 
                                  filename + "'.", array_name);

  OffsetIndex::const_iterator array_it = offset_index_.find(array_name);

  if(array_it != offset_index_.end()) {
    AttributeToOffsetList::const_iterator attribute_it = 
        array_it->second.begin();
    AttributeToOffsetList::const_iterator attribute_it_end =
        array_it->second.end();

    // Prepare the buffer
    uint64_t attribute_num = array_it->second.size();
    // Since the array is assumed to be correct we can focus on the tile 
    // number of the first attribute only.
    uint64_t tile_num = attribute_it->second.size();
    uint64_t buffer_size = attribute_num * tile_num * sizeof(uint64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    OffsetList::const_iterator offset_it;
    OffsetList::const_iterator offset_it_end;
    uint64_t offset = 0;
    for(; attribute_it != attribute_it_end; attribute_it++) {
      offset_it = attribute_it->second.begin();
      offset_it_end = attribute_it->second.end();
      for(; offset_it != offset_it_end; offset_it++) {
        memcpy(buffer+offset, &(*offset_it), sizeof(uint64_t));
        offset += sizeof(uint64_t);
      }
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete buffer;
  }
  
  close(fd);
}

// FILE FORMAT:
// tile_num(uint64_t)
//   tile_id#1(uint64_t) tile_id#2(uint64_t)  ...
void StorageManager::flush_tile_id_index(const std::string& array_name) {
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_TILE_ID_INDEX_NAME + SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1)
    throw StorageManagerException("Cannot create index file '" + 
                                  filename + "'.", array_name);
 
  TileIDIndex::const_iterator array_it = tile_id_index_.find(array_name);

  if(array_it != tile_id_index_.end()) {
    AttributeToTileIDToRank::const_iterator attribute_it = 
        array_it->second.begin();

    // Prepare the buffer
    // Since the array is assumed to be correct, we can focus on the tile ids 
    // of the first attribute only.
    uint64_t tile_num = attribute_it->second.size();
    uint64_t buffer_size = (tile_num+1) * sizeof(uint64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    memcpy(buffer, &tile_num, sizeof(uint64_t));
    TileIDToRank::const_iterator tile_id_it = attribute_it->second.begin();
    TileIDToRank::const_iterator tile_id_it_end = attribute_it->second.end();
    uint64_t offset = sizeof(uint64_t);
    for(; tile_id_it != tile_id_it_end; tile_id_it++) {
      memcpy(buffer+offset, &(tile_id_it->first), sizeof(uint64_t));
      offset += sizeof(uint64_t);
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);  
    delete buffer;
  }

  close(fd);
}

void StorageManager::flush_tiles(const std::string& array_name) {
  TileIndex::const_iterator array_it = tile_index_.find(array_name);
  if(array_it != tile_index_.end()) {  
    AttributeToTileList::const_iterator attribute_it =
        array_it->second.begin();
    AttributeToTileList::const_iterator attribute_it_end = 
        array_it->second.end();
  
    for(; attribute_it != attribute_it_end; attribute_it++)
      flush_tiles(array_name, attribute_it->first);
  }
}

void StorageManager::flush_tiles(const std::string& array_name,
                                 const std::string& attribute_name) {
  // Assert that entry payload_size_index_[array_name][attribute_name] exists,
  // and that it is non-zero
  PayloadSizeIndex::iterator PSI_array_it =
    payload_size_index_.find(array_name);
  assert(PSI_array_it != payload_size_index_.end());
  AttributeToPayloadSize::iterator PSI_attribute_it =
    PSI_array_it->second.find(attribute_name);
  assert(PSI_attribute_it != PSI_array_it->second.end());
  assert(PSI_attribute_it->second != 0);

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + attribute_name + 
                         SM_TILE_DATA_FILE_SUFFIX;		
  int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC,  
                S_IRWXU);
  if(fd == -1)
    throw StorageManagerException("Cannot open tile data file '" + 
                                  filename + "'.", array_name, attribute_name);

  // Retrieve the current file offset (equal to the file size)
  struct stat st;
  fstat(fd, &st);
  uint64_t file_offset = st.st_size;

  // Prepare a segment and append it to the file
  uint64_t segment_size = PSI_attribute_it->second;
  char *segment = new char[segment_size];
  prepare_segment(array_name, attribute_name, file_offset,
                  segment_size, segment);
  write(fd, segment, segment_size);
  
  // Clean up 
  delete segment;
  close(fd);
}

const Tile* StorageManager::get_tile_from_tile_index(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id) const {
  TileIndex::const_iterator array_it = tile_index_.find(array_name);
  if(array_it == tile_index_.end())
    return NULL;

  AttributeToTileList::const_iterator attribute_it = 
      array_it->second.find(attribute_name);
  if(attribute_it == array_it->second.end() ||
     attribute_it->second.size() == 0)
    return NULL;
 
  // For easy reference
  const TileList& tile_list = attribute_it->second;

  // Perform binary search to find the tile 
  uint64_t min = 0;
  uint64_t max = tile_list.size()-1;
  uint64_t mid;
  const Tile* tile;
  while(min <= max) {
    mid = min + ((max - min) / 2);
    tile = tile_list[mid];
    if(tile->tile_id() == tile_id)  // Tile found!
      return tile;
    else if(tile->tile_id() > tile_id)
      max = mid - 1;
    else // (tile->tile_id() < tile_id)
      min = mid + 1;
  }

  // Tile not found
  return NULL;
}

// FILE FORMAT:
// attribute_num(unsigned int) 
//   attribute#1_name_size(unsigned int) attribute#1_name(string) 
//   attribute#2_name_size(unsigned int) attribute#2_name(string) 
void StorageManager::load_attribute_name_index(const std::string& array_name) {
  // Assert that there is no entry for array_name in the index already 
  AttributeNameIndex::iterator array_it = 
      attribute_name_index_.find(array_name);
  assert(array_it == attribute_name_index_.end());
   
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_ATTRIBUTE_NAME_INDEX_NAME + SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1)
    throw StorageManagerException("Cannot open index file '" + 
                                  filename + "'.", array_name);

  // A buffer will store all the contents of the file
  char* buffer;
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;

  if(buffer_size != 0) { // The file is not empty
    // Create an entry for array_name in the index
    std::pair<AttributeNameIndex::iterator, bool> ret =
        attribute_name_index_.insert(
          std::pair<std::string, AttributeSet>
              (array_name, AttributeSet()));
    array_it = ret.first;

    // Load the file contents into the buffer
    buffer = new char[buffer_size];
    read(fd, buffer, buffer_size);

    // Load attribute names into the index
    // Load the number of attributes
    unsigned int attribute_num;
    if(buffer_size < sizeof(unsigned int)) {
      delete buffer;
      close(fd);
      throw StorageManagerException("Format error in index file '" + 
                                    filename + "': cannot read number of "
                                     "attributes.", array_name);
    }
    memcpy(&attribute_num, buffer, sizeof(unsigned int));
    uint64_t offset = sizeof(unsigned int);
    // Load the attribute names one by one
    unsigned int attribute_name_size;
    char* attribute_name;
    for(unsigned int i=0; i<attribute_num; i++) {
      // Load the attribute name size
      if(buffer_size < offset + sizeof(unsigned int)) {
        delete buffer;
        close(fd);
        throw StorageManagerException("Format error in index file '" + 
                                      filename + "': cannot read attribute " 
                                      "name size.", array_name);
      }
      memcpy(&attribute_name_size, buffer+offset, sizeof(unsigned int));
      offset += sizeof(unsigned int);
      // Load the attribute name
      if(buffer_size < offset + attribute_name_size) {
        delete buffer;
        close(fd);
        throw StorageManagerException("Format error in index file '" + 
                                      filename + "': cannot read attribute " 
                                      "name.", array_name);
      }
      attribute_name = new char[attribute_name_size+1];
      memcpy(attribute_name, buffer+offset, attribute_name_size);
      offset += attribute_name_size;
      attribute_name[attribute_name_size] = '\0';
      array_it->second.insert(attribute_name);
      delete attribute_name;
    }  
    // Check for redundant data in the file
    if(offset != buffer_size) {
      delete buffer;
      close(fd);
      throw StorageManagerException("Format error in index file '" + 
                                    filename + "': redundant data.",
                                    array_name);
    }

    // Clean up
    delete buffer;
    close(fd);
  } else { // The file is empty
  // The array is empty, do nothing
    close(fd);
  }
}

// FILE FORMAT:
// tile#1_lower_dim#1(double) tile#1_lower_dim#2(double) ... 
// tile#1_upper_dim#1(double) tile#1_upper_dim#2(double) ... 
// tile#2_lower_dim#1(double) tile#2_lower_dim#2(double) ... 
// tile#2_upper_dim#1(double) tile#2_upper_dim#2(double) ... 
// ...
void StorageManager::load_bounding_coordinates_index(
    const std::string& array_name) {
  AttributeNameIndex::const_iterator ANI_array_it = 
      attribute_name_index_.find(array_name);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_BOUNDING_COORDINATES_INDEX_NAME + 
                         SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1)
    throw StorageManagerException("Cannot open index file '" + 
                                  filename + "'.", array_name);

  // A buffer will store all the contents of the file
  char* buffer;
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;

  // Note that it is assumed that attribute_name_index_ has been already loaded.
  if(ANI_array_it == attribute_name_index_.end() && buffer_size != 0) {
    // If there is no entry array_name in attribute_name_index_, then the 
    // file must be empty (i.e., the array should be empty).     
    close(fd);
    throw StorageManagerException("Format error in index file '" + 
                                  filename + "': file should be empty.", 
                                  array_name);
  } else if(ANI_array_it != attribute_name_index_.end() && buffer_size == 0) {
    // If there is an entry array_name in attribute_name_index_, then the 
    // file must not be empty.
    close(fd);
    throw StorageManagerException("Format error in index file '" + 
                                  filename + "': file should not be empty.",
                                  array_name);
  } else if(ANI_array_it != attribute_name_index_.end() && buffer_size != 0) {
    // We can load the cell id ranges

    // Necessary assertions
    BoundingCoordinatesIndex::iterator BCI_array_it = 
        bounding_coordinates_index_.find(array_name);
    assert(BCI_array_it == bounding_coordinates_index_.end());
    TileIDIndex::const_iterator TII_array_it = tile_id_index_.find(array_name);
    // It is assumed that tile_id_index_ has been already loaded.
    assert(TII_array_it != tile_id_index_.end());
    uint64_t tile_num = TII_array_it->second.begin()->second.size();
    assert(tile_num != 0);

    // Create an entry for array_name in the bounding_coordinates_index
    std::pair<BoundingCoordinatesIndex::iterator, bool> ret =
        bounding_coordinates_index_.insert(
          std::pair<std::string, BoundingCoordinatesList>
              (array_name, BoundingCoordinatesList()));
    BCI_array_it = ret.first;

    // Load the file contents into the buffer
    buffer = new char[buffer_size];
    read(fd, buffer, buffer_size);

    // Load bounding coordinates
    assert(buffer_size % (tile_num * sizeof(double) * 2) == 0);
    unsigned int dim_num = buffer_size / (tile_num * sizeof(double) * 2);
    uint64_t offset = 0;
    std::vector<double> lower, upper;
    double coordinate;
    for(uint64_t i=0; i<tile_num; i++) {
      // Load lower bound
      for(unsigned int j=0; j<dim_num; j++) {
        if(buffer_size < offset + sizeof(double)) {
          delete buffer;
          close(fd);
          throw StorageManagerException("Format error in index file '" + 
                                        filename + "': cannot read lower "
                                        "bounding coordinate.", array_name);
        }
        memcpy(&coordinate, buffer+offset, sizeof(double));
        offset += sizeof(double);
        lower.push_back(coordinate);
      }

      // Load upper bound
      for(unsigned int j=0; j<dim_num; j++) {
        if(buffer_size < offset + sizeof(double)) {
          delete buffer;
          close(fd);
          throw StorageManagerException("Format error in index file '" + 
                                        filename + "': cannot read upper "
                                        "bounding coordinate.", array_name);
        }
        memcpy(&coordinate, buffer+offset, sizeof(double));
        offset += sizeof(double);
        upper.push_back(coordinate);
      }  

      // Insert new entry into the index
      BCI_array_it->second.push_back(BoundingCoordinates(lower, upper));
      lower.clear();
      upper.clear();
    }

    // Check for redundant data in the file
    if(offset != buffer_size) {
      delete buffer;
      close(fd);
      throw StorageManagerException("Format error in index file '" + 
                                    filename + "': redundant data.",
                                    array_name);
    }
    
    // Clean up
    delete buffer;
    close(fd);
  } else { // (ANI_array_it == attribute_name_index_.end() && buffer_size == 0)
    // The array is empty, do nothing
    close(fd);
  }    
}

void StorageManager::load_indices(const std::string& array_name) {
  load_attribute_name_index(array_name);
  load_tile_id_index(array_name);
  load_offset_index(array_name);
  load_MBR_index(array_name);
  load_bounding_coordinates_index(array_name);
}

// FILE FORMAT:
// MBR#1_bound#1(double) MBR#1_bound#2(double) ...
// MBR#2_bound#1(double) MBR#2_bound#2(double) ...
// ...
void StorageManager::load_MBR_index(const std::string& array_name) {
  AttributeNameIndex::const_iterator ANI_array_it = 
      attribute_name_index_.find(array_name);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_MBR_INDEX_NAME + SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1)
    throw StorageManagerException("Cannot open index file '" + 
                                  filename + "'.", array_name);
  
  // A buffer will store all the contents of the file
  char* buffer;
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;

  // Note that it is assumed that attribute_name_index_ has been already loaded.
  if(ANI_array_it == attribute_name_index_.end() && buffer_size != 0) {
    // If there is no entry array_name in attribute_name_index_, then the 
    // file must be empty (i.e., the array should be empty).
    close(fd);
    throw StorageManagerException("Index file '" + 
                                  filename + "' should have been empty.",
                                  array_name);
  } else if(ANI_array_it != attribute_name_index_.end() && buffer_size == 0) {
    // If there is an entry array_name in attribute_name_index_, then the 
    // file must not be empty.
    close(fd);
    throw StorageManagerException("Index file '" + 
                                  filename + "' must not be empty.",
                                  array_name);
  } else if(ANI_array_it != attribute_name_index_.end() && buffer_size != 0) {
    // We can load the MBRs

    // Necessary assertions
    MBRIndex::iterator MBR_array_it = MBR_index_.find(array_name);
    assert(MBR_array_it == MBR_index_.end());
    TileIDIndex::const_iterator TII_array_it = tile_id_index_.find(array_name);
    // It is assumed that tile_id_index_ has been already loaded
    assert(TII_array_it != tile_id_index_.end()); 
    assert(TII_array_it->second.size() != 0);
    // Extract the tile id number from tile_id_index_[array_name][0]
    uint64_t tile_num = TII_array_it->second.begin()->second.size();
    assert(tile_num != 0);

    // Create an entry for array_name in MBR_index_
    std::pair<MBRIndex::iterator, bool> ret =
        MBR_index_.insert(std::pair<std::string, MBRList>
            (array_name, MBRList()));
    MBR_array_it = ret.first;

    // Load the file contents into the buffer
    buffer = new char[buffer_size];
    read(fd, buffer, buffer_size);

    // Load MBRs
    assert(buffer_size % (tile_num * sizeof(double)) == 0);
    unsigned int bound_num = buffer_size / (tile_num * sizeof(double));
    uint64_t offset = 0;
    double bound;
    for(uint64_t i=0; i<tile_num; i++) {
      // Append a new MBR entry
      MBR_array_it->second.push_back(MBR());
      // For easy reference
      MBR& mbr = MBR_array_it->second.back();
      // Load an MBR
      for(unsigned int j=0; j<bound_num; j++) {
        if(buffer_size < offset + sizeof(double)) {
          delete buffer;
          close(fd);
          throw StorageManagerException("Format error in index file '" + 
                                        filename + "': cannot read MBR.",
                                        array_name);
        }
        memcpy(&bound, buffer+offset, sizeof(double));
        offset += sizeof(double);
        mbr.push_back(bound);
      }
    }
    // Check for redundant data in the file
    if(offset != buffer_size) {
      delete buffer;
      close(fd);
      throw StorageManagerException("Format error in index file '" + 
                                     filename + "': redundant data.",
                                     array_name);
    }
    // Clean up
    close(fd);
    delete buffer;
  } else { // (ANI_array_it == attribute_name_index_.end() && buffer_size == 0)
    // The array is empty, do nothing
    close(fd);
  }    
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(uint64_t)
// tile#2_of_attribute#1_offset(uint64_t)
// ...
// tile#1_of_attribute#2_offset(uint64_t)
// tile#2_of_attribute#2_offset(uint64_t)
// ...
void StorageManager::load_offset_index(const std::string& array_name) {
  AttributeNameIndex::const_iterator ANI_array_it = 
      attribute_name_index_.find(array_name);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_OFFSET_INDEX_NAME + SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1)
    throw StorageManagerException("Cannot open index file '" + 
                                  filename + "'.", array_name);
  // A buffer will store all the contents of the file
  char* buffer;
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;

  // Note that it is assumed that attribute_name_index_ has been already loaded.
  if(ANI_array_it == attribute_name_index_.end() && buffer_size != 0) {
    // If there is no entry array_name in attribute_name_index_, then the 
    // file must be empty (i.e., the array should be empty).
    close(fd);
    throw StorageManagerException("Index file '" + 
                                  filename + "' should have been empty.",
                                  array_name);
  } else if(ANI_array_it != attribute_name_index_.end() && buffer_size == 0) {
    // If there is an entry array_name in attribute_name_index_, then the 
    // file must not be empty.
    close(fd);
    throw StorageManagerException("Index file '" + 
                                  filename + "' must not be empty.",
                                  array_name);
  } else if(ANI_array_it != attribute_name_index_.end() && buffer_size != 0) {
    // We can load the offsets

    // Necessary assertions
    assert(ANI_array_it->second.size() != 0);
    OffsetIndex::iterator OI_array_it = 
        offset_index_.find(array_name);
    assert(OI_array_it == offset_index_.end());
    // It is assumed that tile_id_index_ has been already loaded
    TileIDIndex::const_iterator TII_array_it = tile_id_index_.find(array_name);
    assert(TII_array_it != tile_id_index_.end());
    assert(TII_array_it->second.size() != 0);
    // Extract the tile id number from tile_id_index_[array_name][0]
    uint64_t tile_num = TII_array_it->second.begin()->second.size(); 
    assert(tile_num != 0);

    // Create an entry for array_name in offset_index_
    std::pair<OffsetIndex::iterator, bool> ret =
        offset_index_.insert(
          std::pair<std::string, AttributeToOffsetList>
              (array_name, AttributeToOffsetList()));
    OI_array_it = ret.first;

    // Load the file contents into the buffer
    buffer = new char[buffer_size];
    read(fd, buffer, buffer_size);

    // Load offsets
    AttributeSet::const_iterator ANI_attribute_it = 
        ANI_array_it->second.begin();
    AttributeSet::const_iterator ANI_attribute_it_end = 
        ANI_array_it->second.end();
    uint64_t offset = 0;
    uint64_t tile_offset;
    AttributeToOffsetList::iterator OI_attribute_it;
    for(; ANI_attribute_it != ANI_attribute_it_end; ANI_attribute_it++) {
      // Create an entry for every attribute_name in offset_index_
      std::pair<AttributeToOffsetList::iterator, bool> ret =
          OI_array_it->second.insert(std::pair<std::string, OffsetList>
              (*ANI_attribute_it, OffsetList()));
      OI_attribute_it = ret.first;
      // Load tile offsets for the current attribute
      for(uint64_t i=0; i<tile_num; i++) {
        if(buffer_size < offset + sizeof(uint64_t)) {
          delete buffer;
          close(fd);
          throw StorageManagerException("Format error in index file '" + 
                                        filename + "': cannot read tile "
                                        "offset.", array_name);
        }  
        memcpy(&tile_offset, buffer+offset, sizeof(uint64_t));
        OI_attribute_it->second.push_back(tile_offset);
        offset += sizeof(uint64_t);
      }
    }
    // Check for redundant data in the file
    if(offset != buffer_size) {
      delete buffer;
      close(fd);
      throw StorageManagerException("Format error in index file '" + 
                                    filename + "': redundant data.",
                                    array_name);
    }
    // Clean up
    close(fd);
    delete buffer;
  } else { // (ANI_array_it == attribute_name_index_.end() && buffer_size == 0)
    // The array is empty, do nothing
    close(fd);
  }    
}

std::pair<uint64_t, uint64_t> StorageManager::load_payloads_into_buffer(
    const std::string& array_name, const std::string& attribute_name, 
    uint64_t start_tile_id, char*& buffer) const {
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + attribute_name + 
                         SM_TILE_DATA_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1)
    throw StorageManagerException("Cannot open tile data file '" + 
                                  filename + "'.", array_name, attribute_name);
  struct stat st;
  fstat(fd, &st);
 
  // Necessary assertions on offset_index_ and tile_id_index
  OffsetIndex::const_iterator OI_array_it = offset_index_.find(array_name);
  assert(OI_array_it != offset_index_.end());
  AttributeToOffsetList::const_iterator OI_attribute_it = 
      OI_array_it->second.find(attribute_name);
  assert(OI_attribute_it != OI_array_it->second.end());
  TileIDIndex::const_iterator TII_array_it = tile_id_index_.find(array_name);
  assert(TII_array_it != tile_id_index_.end());
  AttributeToTileIDToRank::const_iterator TII_attribute_it =
      TII_array_it->second.begin();
  assert(TII_attribute_it != TII_array_it->second.end());
  TileIDToRank::const_iterator TII_tile_id_it =
      TII_attribute_it->second.find(start_tile_id);
  assert(TII_tile_id_it != TII_attribute_it->second.end());
  assert(OI_attribute_it->second.size() > TII_tile_id_it->second);

  // For easy reference
  const OffsetList& offset_list = OI_attribute_it->second;
  const uint64_t& start_rank = TII_tile_id_it->second;

  // Calculate buffer size (smallest size larger than or equal to segment_size_)
  uint64_t buffer_size = 0;
  uint64_t tile_num = 0;
  uint64_t max_rank = start_rank;
  while(max_rank < offset_list.size() && buffer_size < segment_size_) {
    if(max_rank == offset_list.size()-1)
      buffer_size += st.st_size - offset_list[max_rank];
    else
      buffer_size += offset_list[max_rank+1] - offset_list[max_rank];
    max_rank++;
    tile_num++;
  }

  // Necessary assertions on buffer_size
  assert(buffer_size != 0);
  if(offset_list[start_rank] + buffer_size > st.st_size) {
    close(fd);
    throw StorageManagerException("Format error in tile data file '" + 
                                  filename + "': file size too short "
                                  "to load the tile payloads.", array_name,
                                  attribute_name);
  }

  // Read payloads into buffer
  buffer = new char[buffer_size];
  lseek(fd, offset_list[start_rank], SEEK_SET);
  read(fd, buffer, buffer_size); 
  close(fd);

  return std::pair<uint64_t, uint64_t>(buffer_size, tile_num);
}

// FILE FORMAT:
// tile_num(uint64_t)
//   tile_id#1(uint64_t) tile_id#2(uint64_t)  ...
void StorageManager::load_tile_id_index(const std::string& array_name) {
  AttributeNameIndex::const_iterator ANI_array_it = 
      attribute_name_index_.find(array_name);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_TILE_ID_INDEX_NAME + SM_INDEX_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1)
    throw StorageManagerException("Cannot open index file '" + 
                                  filename + "'.", array_name);
 
  // A buffer will store all the contents of the file
  char* buffer;
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;

  // Note that it is assumed that attribute_name_index_ has been already loaded.
  if(ANI_array_it == attribute_name_index_.end() && buffer_size != 0) {
    // If there is no entry array_name in attribute_name_index_, then the 
    // file must be empty (i.e., the array should be empty).
    close(fd);
    throw StorageManagerException("Index file '" + 
                                  filename + "' should have been empty.",
                                  array_name);
  } else if(ANI_array_it != attribute_name_index_.end() && buffer_size == 0) {
    // If there is an entry array_name in attribute_name_index_, then the 
    // file must not be empty.
    close(fd);
    throw StorageManagerException("Index file '" + 
                                  filename + "' must not be empty.",
                                  array_name);
  } else if(ANI_array_it != attribute_name_index_.end() && buffer_size != 0) {
    // We can load the tile ids
    // Necessary assertions
    assert(ANI_array_it->second.size() != 0);
    TileIDIndex::iterator TII_array_it = tile_id_index_.find(array_name);
    assert(TII_array_it == tile_id_index_.end());

    // Create an entry for array_name in the tile_id_index_
    std::pair<TileIDIndex::iterator, bool> ret_1 =
        tile_id_index_.insert(
          std::pair<std::string, AttributeToTileIDToRank>
              (array_name, AttributeToTileIDToRank()));
    TII_array_it = ret_1.first;
    // Create an entry for some attribute_name in the tile_id_index_
    // (the name of the attribute does not matter when in READ mode, 
    // since we will always check entry tile_id_index_[array_name][0],
    // when reading tile ids).
    std::pair<AttributeToTileIDToRank::iterator, bool> ret_2 =
        TII_array_it->second.insert(
          std::pair<std::string, TileIDToRank>
              (*(ANI_array_it->second.begin()), TileIDToRank()));
    AttributeToTileIDToRank::iterator TII_attribute_it = ret_2.first;

    // Load the file contents into the buffer
    buffer = new char[buffer_size];
    read(fd, buffer, buffer_size);

    // Load tile ids
    // Load the number of tiles
    uint64_t tile_num;
    if(buffer_size < sizeof(uint64_t)) {
      delete buffer;
      close(fd);
      throw StorageManagerException("Format error in index file '" + 
                                    filename + "': cannot read number of "
                                    " tiles.", array_name);
    }
    memcpy(&tile_num, buffer, sizeof(uint64_t));
    uint64_t offset = sizeof(uint64_t);
    // Load the tile id list
    uint64_t tile_id;
    uint64_t last_read_tile_id;
    bool checking_first_tile = true;
    for(uint64_t i=0; i<tile_num; i++) {
      // Load a tile id and perform checks
      if(buffer_size < offset + sizeof(uint64_t)) {  
        delete buffer;
        close(fd);
        throw StorageManagerException("Format error in index file '" + 
                                      filename + ": cannot read tile id'.",
                                      array_name);
      }
      memcpy(&tile_id, buffer+offset, sizeof(uint64_t));
      // Check the tile id increasing order
      if(tile_id <= last_read_tile_id) {
        if(checking_first_tile) {
          checking_first_tile = false;
        } else {
          delete buffer;
          close(fd);
          throw StorageManagerException("Format error in index file '" + 
                                        filename + "': tile ids must appear "
                                        "in ascending order.", array_name);
        }
      }
      last_read_tile_id = tile_id;
      TII_attribute_it->second[tile_id] = i;
      offset += sizeof(uint64_t);
    }
    // Check for redundant data in the file
    if(offset != buffer_size) {
      delete buffer;
      close(fd);
      throw StorageManagerException("Format error in index file '" + 
                                    filename + "': redundant data.", 
                                    array_name);
    }

    // Clean up
    delete buffer;
    close(fd);
  } else { // (ANI_array_it == attribute_name_index_.end() && buffer_size == 0)
    // The array is empty, do nothing
    close(fd);
  }
}

template<class T>
void StorageManager::load_tiles(const std::string& array_name,
                                const std::string& attribute_name,
                                uint64_t start_tile_id) { 
  // Load the tile payloads from the disk into a buffer
  char* buffer;
  std::pair<uint64_t, uint64_t> ret = 
      load_payloads_into_buffer(
          array_name, attribute_name, start_tile_id, buffer);
  // NOTE: load_payloads_into_buffer creates a new bufer, which must be cleaned
  // up later.

  // For clarity
  uint64_t& buffer_size = ret.first;
  uint64_t& tile_num = ret.second;

  // Delete previous tiles from main memory
  delete_tiles(array_name, attribute_name);
  
  // Create the tiles form the payloads in the buffer and load them
  // into the tile index
  try {
    load_tiles_from_buffer<T>(
        array_name, attribute_name, start_tile_id, buffer, buffer_size, tile_num);
  } catch(StorageManagerException& sme) {
    delete buffer;
    throw(sme);
  }
 
  // Clean up
  delete [] buffer;
}

template<class T>
void StorageManager::load_tiles(const std::string& array_name,
                                uint64_t start_tile_id) {
  // Load the tile payloads from the disk into a buffer
  char* buffer;
  std::pair<uint64_t, uint64_t> ret = 
      load_payloads_into_buffer(
          array_name, SM_COORDINATE_TILE_NAME, start_tile_id, buffer);
  // NOTE: load_payloads_into_buffer creates a new bufer, which must be cleaned
  // up later.

  // For easy clarity
  uint64_t& buffer_size = ret.first;
  uint64_t& tile_num = ret.second;

  // Delete previous tiles from main memory
  delete_tiles(array_name, SM_COORDINATE_TILE_NAME);

  // Create the tiles form the payloads in the buffer and load them
  // into the tile index
  try {
    load_tiles_from_buffer<T>(
        array_name, start_tile_id, buffer, buffer_size, tile_num);
  } catch(StorageManagerException& sme) {
    delete buffer;
    throw(sme);
  }
 
  // Clean up
  delete buffer;
}

template<class T>
void StorageManager::load_tiles_from_buffer(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t start_tile_id, char* buffer, uint64_t buffer_size, 
    uint64_t tile_num) {
  TileIDIndex::iterator TII_array_it = tile_id_index_.find(array_name);
  AttributeToTileIDToRank::const_iterator TII_attribute_it =
      TII_array_it->second.begin();
  TileIDToRank::const_iterator TII_tile_id_it =
      TII_attribute_it->second.find(start_tile_id);

  // Create new entries in tile index for array_name, attribute_name
  std::pair<TileIndex::iterator, bool> ret_1 =
      tile_index_.insert(
        std::pair<std::string, AttributeToTileList>
            (array_name, AttributeToTileList()));
  TileIndex::iterator TI_array_it = ret_1.first;
  std::pair<AttributeToTileList::iterator, bool> ret_2 =
      TI_array_it->second.insert(
        std::pair<std::string, TileList>
            (attribute_name, TileList()));
  AttributeToTileList::iterator TI_attribute_it = ret_2.first;

  // For easy reference
  const OffsetList& offset_list = offset_index_[array_name][attribute_name];
  TileList& tile_list = TI_attribute_it->second;

  // Load tiles
  uint64_t buffer_offset = 0;
  uint64_t tile_size, cell_num;
  std::vector<T> payload;
  for(uint64_t i=0; i<tile_num; i++, TII_tile_id_it++) {
    assert(TII_tile_id_it != TII_attribute_it->second.end());
    const uint64_t& tile_id = TII_tile_id_it->first;
    const uint64_t& rank = TII_tile_id_it->second;
    assert(rank < offset_list.size());
     
    if(rank == offset_list.size()-1)
      tile_size = buffer_size - buffer_offset;
    else
      tile_size = offset_list[rank+1] - offset_list[rank];
  
    assert(buffer_offset + tile_size <= buffer_size);
    assert(tile_size % sizeof(T) == 0);

    cell_num = tile_size / sizeof(T);
    payload.resize(cell_num);
    memcpy(&payload[0], buffer + buffer_offset, tile_size);
    buffer_offset += tile_size;
   
    AttributeTile<T>* tile = new AttributeTile<T>(tile_id);
    tile->set_payload(payload);
    tile_list.push_back(tile);
    
    payload.clear();
  }
}

template<class T>
void StorageManager::load_tiles_from_buffer(
    const std::string& array_name, uint64_t start_tile_id, char* buffer, 
    uint64_t buffer_size, uint64_t tile_num) {
  TileIDIndex::iterator TII_array_it = tile_id_index_.find(array_name);
  AttributeToTileIDToRank::const_iterator TII_attribute_it =
      TII_array_it->second.begin();
  TileIDToRank::const_iterator TII_tile_id_it =
      TII_attribute_it->second.find(start_tile_id);

  // Create new entries in tile index for array_name, attribute_name
  std::pair<TileIndex::iterator, bool> ret_1 =
      tile_index_.insert(
        std::pair<std::string, AttributeToTileList>
            (array_name, AttributeToTileList()));
  TileIndex::iterator TI_array_it = ret_1.first;
  std::pair<AttributeToTileList::iterator, bool> ret_2 =
      TI_array_it->second.insert(
        std::pair<std::string, TileList>
            (SM_COORDINATE_TILE_NAME, TileList()));
  AttributeToTileList::iterator TI_attribute_it = ret_2.first;

  // For easy reference
  const OffsetList& offset_list = 
      offset_index_[array_name][SM_COORDINATE_TILE_NAME];
  TileList& tile_list = TI_attribute_it->second;
  const MBRList& MBR_list = MBR_index_[array_name];

  // Load tiles
  uint64_t buffer_offset = 0;
  uint64_t tile_size, cell_num;
  unsigned int dim_num = MBR_list[0].size() / 2;
  uint64_t cell_size = dim_num * sizeof(T);
  std::vector<T> coordinates;
  coordinates.resize(dim_num);
  std::vector<std::vector<T> > payload;
  for(uint64_t i=0; i<tile_num; i++, TII_tile_id_it++) {
    assert(TII_tile_id_it != TII_attribute_it->second.end());
    const uint64_t& tile_id = TII_tile_id_it->first;
    const uint64_t& rank = TII_tile_id_it->second;
    assert(rank < offset_list.size());
    assert(rank < MBR_list.size());
    const MBR& mbr = MBR_list[rank];
     
    if(rank == offset_list.size()-1)
      tile_size = buffer_size - buffer_offset;
    else
      tile_size = offset_list[rank+1] - offset_list[rank];
  
    assert(buffer_offset + tile_size <= buffer_size);
    assert(tile_size % cell_size == 0);

    cell_num = tile_size / cell_size;
    payload.resize(cell_num);

    for(uint64_t j=0; j<cell_num; j++) {
      memcpy(&coordinates[0], buffer + buffer_offset, cell_size);
      buffer_offset += cell_size;
      payload[j] = coordinates;
    }   

    CoordinateTile<T>* tile = new CoordinateTile<T>(tile_id, dim_num);
    tile->set_payload(payload);
    tile->set_mbr(mbr);
    tile_list.push_back(tile);
    
    payload.clear();
  }
}

void StorageManager::prepare_segment( 
    const std::string& array_name, const std::string& attribute_name,
    uint64_t file_offset, uint64_t segment_size, char* segment) {
  // Assert that entry tile_index_[array_name][attribute_name] exists,
  // and that the stored tile list is not empty
  TileIndex::const_iterator TI_array_it = tile_index_.find(array_name);
  assert(TI_array_it != tile_index_.end());
  AttributeToTileList::const_iterator TI_attribute_it = 
      TI_array_it->second.find(attribute_name);
  assert(TI_attribute_it != TI_array_it->second.end());
  TileList::const_iterator tile_it = TI_attribute_it->second.begin();
  TileList::const_iterator tile_it_end = TI_attribute_it->second.end();
  assert(tile_it != tile_it_end);

  // Create entry offset_index_[array_name][attribute_name]
  OffsetIndex::iterator OI_array_it = offset_index_.find(array_name);
  if(OI_array_it == offset_index_.end()) {
    std::pair<OffsetIndex::iterator, bool> ret = offset_index_.insert(
        std::pair<std::string, AttributeToOffsetList>
            (array_name, AttributeToOffsetList()));
    OI_array_it = ret.first;
  }
  AttributeToOffsetList::iterator OI_attribute_it = 
      OI_array_it->second.find(attribute_name);
  if(OI_attribute_it == OI_array_it->second.end()) {
    std::pair<AttributeToOffsetList::iterator, bool> ret = 
        OI_array_it->second.insert(std::pair<std::string, OffsetList>
            (attribute_name, OffsetList()));
    OI_attribute_it = ret.first;
  }

  // Copy payloads to segment and update offset index
  uint64_t segment_offset = 0;
  for(; tile_it != tile_it_end; tile_it++) {
    // Assert that the sum of payloads can never exceed the segment size
    assert((*tile_it)->tile_size() + segment_offset <= segment_size);
    (*tile_it)->copy_payload(segment+segment_offset);
    OI_attribute_it->second.push_back(file_offset + segment_offset);
    segment_offset += (*tile_it)->tile_size();
  }
}

inline
void StorageManager::set_workspace(const std::string& path) {
  workspace_ = path + "/StorageManager";
  
  // Replace '~' with the absolute path (mkdir does not recogize '~')
  if(workspace_[0] == '~') {
    workspace_ = std::string(getenv("HOME")) +
                 workspace_.substr(1, workspace_.size()-1);
  }
}

bool StorageManager::tile_entries_are_correct(
    const std::string& array_name) const {
  TileIDIndex::const_iterator TII_array_it = 
      tile_id_index_.find(array_name);
  OffsetIndex::const_iterator OI_array_it = 
      offset_index_.find(array_name);
  MBRIndex::const_iterator MBRI_array_it = 
      MBR_index_.find(array_name);
  BoundingCoordinatesIndex::const_iterator BCI_array_it = 
      bounding_coordinates_index_.find(array_name);

  AttributeToTileIDToRank::const_iterator TII_attribute_it =
      TII_array_it->second.begin();
  AttributeToOffsetList::const_iterator OI_attribute_it =
      OI_array_it->second.begin();
 
  // All ids must be the same in tile_id_index_[array_name][attribute_name]
  // for all attribute_name values.
  // Make sure that they have the same number of tile entries.
  uint64_t tile_num = TII_attribute_it->second.size();
  if(tile_num == 0)
    return false;
  std::vector<TileIDToRank::const_iterator> tile_id_it_vec;
  AttributeToTileIDToRank::const_iterator TII_attribute_it_end =
      TII_array_it->second.end();
  for(; TII_attribute_it != TII_attribute_it_end; ++TII_attribute_it) {
    if(TII_attribute_it->second.size() != tile_num)
      return false;
    tile_id_it_vec.push_back(TII_attribute_it->second.begin());
  }
  // Make sure that all tile ids are the same across all attributes 
  if(tile_id_it_vec.size() > 1) { 
    for(uint64_t i=0; i<tile_num; i++) {
      for(uint64_t j=1; j<tile_id_it_vec.size(); j++) {
        if(tile_id_it_vec[j]->first != tile_id_it_vec[0]->first)
          return false;
        tile_id_it_vec[j]++;
      }
      tile_id_it_vec[0]++;
    }
  }

  // Some further assertions
  AttributeToOffsetList::const_iterator OI_attribute_it_end =
      OI_array_it->second.end();
  for(; OI_attribute_it != OI_attribute_it_end; OI_attribute_it++) 
    assert(OI_attribute_it->second.size() == tile_num);
  assert(MBRI_array_it == MBR_index_.end() || 
         MBRI_array_it->second.size() == tile_num);
  assert(BCI_array_it == bounding_coordinates_index_.end() || 
         BCI_array_it->second.size() == tile_num);
  
  return true;
}

inline
void StorageManager::update_attribute_name_index_on_append_tile(
    const std::string& array_name, const std::string& attribute_name) {
  AttributeNameIndex::iterator array_it = 
      attribute_name_index_.find(array_name);
  
  // Create an array entry if it does not exist
  if(array_it == attribute_name_index_.end()) {
    std::pair<AttributeNameIndex::iterator, bool> ret = 
        attribute_name_index_.insert(
            std::pair<std::string, AttributeSet>(array_name, AttributeSet()));
    array_it = ret.first;
  }
  
  // Create attribute entry 
  array_it->second.insert(attribute_name);
}

inline
void StorageManager::update_bounding_coordinates_index_on_append_tile(
    const std::string& array_name, 
    const BoundingCoordinates& bounding_coordinates) {
  BoundingCoordinatesIndex::iterator array_it = 
      bounding_coordinates_index_.find(array_name);

  // Create an array entry if it does not exist
  if(array_it == bounding_coordinates_index_.end()) {
    std::pair<BoundingCoordinatesIndex::iterator, bool> ret = 
        bounding_coordinates_index_.insert(
            std::pair<std::string, BoundingCoordinatesList>
                (array_name, BoundingCoordinatesList()));
    array_it = ret.first;
  }

  array_it->second.push_back(bounding_coordinates);
}

inline 
void StorageManager::update_MBR_index_on_append_tile(
    const std::string& array_name, const MBR& mbr) {

  MBRIndex::iterator array_it = MBR_index_.find(array_name);

  // Create an array entry if it does not exist
  if(array_it == MBR_index_.end()) {
    std::pair<MBRIndex::iterator, bool> ret = 
        MBR_index_.insert(
            std::pair<std::string, MBRList>(array_name, MBRList()));
    array_it = ret.first;
  }
  
  array_it->second.push_back(mbr);
}

inline
bool StorageManager::update_payload_size_index_on_append_tile(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_size) {
  PayloadSizeIndex::iterator array_it = payload_size_index_.find(array_name);

  // Create an array entry if it does not exist
  if(array_it == payload_size_index_.end()) {
    std::pair<PayloadSizeIndex::iterator, bool> ret = 
        payload_size_index_.insert(
            std::pair<std::string, AttributeToPayloadSize>
                (array_name, AttributeToPayloadSize()));
    array_it = ret.first;
  }

  AttributeToPayloadSize::iterator attribute_it =
      array_it->second.find(attribute_name);

  // Create an attribute entry if it does not exist
  if(attribute_it == array_it->second.end()) {
    std::pair<AttributeToPayloadSize::iterator, bool> ret = 
        array_it->second.insert(
            std::pair<std::string, uint64_t>(attribute_name, tile_size));
    attribute_it = ret.first;
    attribute_it->second = 0;
  }
  
  attribute_it->second += tile_size;

  return (attribute_it->second >= segment_size_);
}

inline
void StorageManager::update_tile_id_index_on_append_tile(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id) {
  TileIDIndex::iterator array_it = tile_id_index_.find(array_name);

  // Create an array entry if it does not exist
  if(array_it == tile_id_index_.end()) {
    std::pair<TileIDIndex::iterator, bool> ret = 
        tile_id_index_.insert(
            std::pair<std::string, AttributeToTileIDToRank>
                (array_name, AttributeToTileIDToRank()));
    array_it = ret.first;
  }

  AttributeToTileIDToRank::iterator attribute_it =
      array_it->second.find(attribute_name);

  // Create an attribute entry if it does not exist
  if(attribute_it == array_it->second.end()) {
    std::pair<AttributeToTileIDToRank::iterator, bool> ret = 
        array_it->second.insert(std::pair<std::string, TileIDToRank>
            (attribute_name, TileIDToRank()));
    attribute_it = ret.first;
  }

  // The new rank is equivalent to the old one + 1 
  attribute_it->second[tile_id] = attribute_it->second.size();
}

inline
void StorageManager::update_tile_index_on_append_tile(
    const std::string& array_name, const std::string& attribute_name,
    const Tile* tile) {
  TileIndex::iterator array_it = tile_index_.find(array_name);

  // Create an array entry if it does not exist
  if(array_it == tile_index_.end()) {
    std::pair<TileIndex::iterator, bool> ret = 
        tile_index_.insert(std::pair<std::string, AttributeToTileList>
            (array_name, AttributeToTileList()));
    array_it = ret.first;
  }

  AttributeToTileList::iterator attribute_it =
      array_it->second.find(attribute_name);

  // Create an attribute entry if it does not exist
  if(attribute_it == array_it->second.end()) {
    std::pair<AttributeToTileList::iterator, bool> ret = 
        array_it->second.insert(
            std::pair<std::string, TileList>(attribute_name, TileList()));
    attribute_it = ret.first;
  }

  attribute_it->second.push_back(tile);
}

// Explicit template instantiations
template const Tile* StorageManager::get_tile<int>(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id);
template const Tile* StorageManager::get_tile<int64_t>(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id);
template const Tile* StorageManager::get_tile<float>(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id);
template const Tile* StorageManager::get_tile<double>(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t tile_id);
template const Tile* StorageManager::get_tile<int>(
    const std::string& array_name, uint64_t tile_id);
template const Tile* StorageManager::get_tile<int64_t>(
    const std::string& array_name, uint64_t tile_id);
template const Tile* StorageManager::get_tile<float>(
    const std::string& array_name, uint64_t tile_id);
template const Tile* StorageManager::get_tile<double>(
    const std::string& array_name, uint64_t tile_id);
template void StorageManager::load_tiles<int>(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t start_tile_id);
template void StorageManager::load_tiles<int64_t>(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t start_tile_id);
template void StorageManager::load_tiles<float>(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t start_tile_id);
template void StorageManager::load_tiles<double>(
    const std::string& array_name, const std::string& attribute_name,
    uint64_t start_tile_id);
template void StorageManager::load_tiles<int>(
    const std::string& array_name, uint64_t start_tile_id);
template void StorageManager::load_tiles<int64_t>(
    const std::string& array_name, uint64_t start_tile_id);
template void StorageManager::load_tiles<float>(
    const std::string& array_name, uint64_t start_tile_id);
template void StorageManager::load_tiles<double>(
    const std::string& array_name, uint64_t start_tile_id);
template void StorageManager::load_tiles_from_buffer<int>(
    const std::string& array_name, const std::string& attribute_name, 
    uint64_t start_tile_id,
    char* buffer, uint64_t buffer_size, uint64_t tile_num);
template void StorageManager::load_tiles_from_buffer<int64_t>(
    const std::string& array_name, const std::string& attribute_name, 
    uint64_t start_tile_id,
    char* buffer, uint64_t buffer_size, uint64_t tile_num);
template void StorageManager::load_tiles_from_buffer<float>(
    const std::string& array_name, const std::string& attribute_name, 
    uint64_t start_tile_id,
    char* buffer, uint64_t buffer_size, uint64_t tile_num);
template void StorageManager::load_tiles_from_buffer<double>(
    const std::string& array_name, const std::string& attribute_name, 
    uint64_t start_tile_id,
    char* buffer, uint64_t buffer_size, uint64_t tile_num);
template void StorageManager::load_tiles_from_buffer<int>(
    const std::string& array_name, uint64_t start_tile_id,
    char* buffer, uint64_t buffer_size, uint64_t tile_num);
template void StorageManager::load_tiles_from_buffer<int64_t>(
    const std::string& array_name, uint64_t start_tile_id,
    char* buffer, uint64_t buffer_size, uint64_t tile_num);
template void StorageManager::load_tiles_from_buffer<float>(
    const std::string& array_name, uint64_t start_tile_id,
    char* buffer, uint64_t buffer_size, uint64_t tile_num);
template void StorageManager::load_tiles_from_buffer<double>(
    const std::string& array_name, uint64_t start_tile_id,
    char* buffer, uint64_t buffer_size, uint64_t tile_num);
