/**
 * @file   storage_manager.cc
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
************ STATIC VARIABLE DEFINITIONS **************
******************************************************/

uint64_t StorageManager::array_info_id_ = 0;

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

StorageManager::StorageManager(
    const std::string& path, uint64_t segment_size) 
    : segment_size_(segment_size){
  set_workspace(path);
  create_workspace();
}

StorageManager::~StorageManager() {
  OpenArrays::iterator array_it;

  // Close all open arrays
  while( (array_it = open_arrays_.begin()) != open_arrays_.end() ) {
    flush_array_info(array_it->second); 
    open_arrays_.erase(array_it); 
  }
}

/******************************************************
******************* ARRAY FUNCTIONS *******************
******************************************************/

void StorageManager::close_array(const ArrayDescriptor* array_descriptor) {
  assert(check_on_close_array(*array_descriptor)); 
 
  // For easy reference
  ArrayInfo& array_info = *(array_descriptor->array_info_);
  const std::string& array_name = array_info.array_schema_.array_name();
  
  flush_array_info(array_info); 
  open_arrays_.erase(array_name); 

  delete array_descriptor;
}

void StorageManager::delete_array(const std::string& array_name) {
  OpenArrays::iterator array_it = open_arrays_.find(array_name);

  // If the array is open
  if(array_it != open_arrays_.end()) {
    delete_tiles(array_it->second);
    open_arrays_.erase(array_it); 
  }

  // Regardless of whether the array is open or not
  delete_directory(array_name);
}

bool StorageManager::is_empty(const ArrayDescriptor* array_descriptor) const {
  assert(check_array_descriptor(*array_descriptor));

  return array_descriptor->array_info_->tile_ids_.size() == 0;
}

// Opens an array in READ mode
StorageManager::ArrayDescriptor* StorageManager::open_array(
    const std::string& array_name) {
  assert(check_on_open_array(array_name, READ));
  
  // Create a new entry in open_arrays_ 
  std::pair<OpenArrays::iterator, bool> ret = open_arrays_.insert(
      std::pair<std::string, ArrayInfo>(array_name, ArrayInfo()));

  // For easy reference
  ArrayInfo& array_info = ret.first->second;
  
  // Load array schema
  ArraySchema array_schema;
  load_array_schema(array_name, array_schema);

  // Initialize array info
  init_array_info(array_info, READ, array_schema);

  // Load book-keeping structures
  load_tile_ids(array_info);      
  if(array_info.tile_ids_.size() != 0) { // If the array is not empty
    load_bounding_coordinates(array_info);
    load_MBRs(array_info);  
    load_offsets(array_info);
  }
  
  return new ArrayDescriptor(&array_info); 
}

// Opens an array in CREATE mode
StorageManager::ArrayDescriptor* StorageManager::open_array(
    const ArraySchema& array_schema) {
  assert(check_on_open_array(array_schema.array_name(), CREATE));

  // For easy reference
  const std::string& array_name = array_schema.array_name();

  // Create array directory
  create_array_directory(array_name);

  // Create an array info entry in open_arrays_
  std::pair<OpenArrays::iterator, bool> ret = open_arrays_.insert(
      std::pair<std::string, ArrayInfo>(array_name, ArrayInfo()));
  ArrayInfo& array_info = ret.first->second;
  
  // Initialize array info
  init_array_info(array_info, CREATE, array_schema);

  return new ArrayDescriptor(&array_info); 
}

/******************************************************
******************** TILE FUNCTIONS *******************
******************************************************/

void StorageManager::append_tile(const Tile* tile, 
                                 const ArrayDescriptor* array_descriptor,
                                 unsigned int attribute_id) {
  // If tile is empty, delete it and exit
  if(tile->cell_num() == 0) {
    delete tile;
    return;
  }

  assert(check_on_append_tile(*array_descriptor, attribute_id, tile));

  // For easy reference
  ArrayInfo& array_info = *(array_descriptor->array_info_);

  // Update indices
  // For both attribute and coordinate tiles
  if(is_empty(array_descriptor) || 
     array_info.tile_ids_.back() != tile->tile_id())
    array_info.tile_ids_.push_back(tile->tile_id());
  array_info.tiles_[attribute_id].push_back(tile);
  array_info.payload_sizes_[attribute_id] += tile->tile_size();
  // Only for coordinate tiles
  if(tile->tile_type() == Tile::COORDINATE) {
    array_info.mbrs_.push_back(tile->mbr());
    array_info.bounding_coordinates_.push_back(tile->bounding_coordinates());
  }

  // Flush tiles to disk if the sum of payloads exceeds the segment size
  if(array_info.payload_sizes_[attribute_id] >= segment_size_) { 
    flush_tiles(array_info, attribute_id);
    delete_tiles(array_info, attribute_id);
  }
} 

const Tile* StorageManager::get_tile( 
    const ArrayDescriptor* array_descriptor,
    unsigned int attribute_id, uint64_t tile_id) {
  assert(check_on_get_tile(*array_descriptor, attribute_id, tile_id));

  // For easy reference
  ArrayInfo& array_info = *(array_descriptor->array_info_);

  uint64_t rank = tile_rank(array_info, tile_id); 

  return get_tile_by_rank(array_info, attribute_id, rank);
}

Tile* StorageManager::new_tile(const ArraySchema& array_schema, 
    unsigned int attribute_id, uint64_t tile_id, 
    uint64_t cell_num) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();
  assert(attribute_id <= attribute_num);
  unsigned int dim_num = array_schema.dim_num();
  const std::type_info* cell_type = array_schema.type(attribute_id);
  Tile::TileType tile_type = 
      (attribute_id < attribute_num) ? Tile::ATTRIBUTE : Tile::COORDINATE;
  Tile* tile;

  // Create an attribute tile
  if(tile_type == Tile::ATTRIBUTE) {
    if(*cell_type == typeid(char))
      tile = new AttributeTile<char>(tile_id, cell_num);
    else if(*cell_type == typeid(int))
      tile = new AttributeTile<int>(tile_id, cell_num);
    else if(*cell_type == typeid(int64_t))
      tile = new AttributeTile<int64_t>(tile_id, cell_num);
    else if(*cell_type == typeid(float))
      tile = new AttributeTile<float>(tile_id, cell_num);
    else if(*cell_type == typeid(double)) 
      tile = new AttributeTile<double>(tile_id, cell_num);
  } else { // Create a coordinate tile
    if(*cell_type == typeid(int))
      tile = new CoordinateTile<int>(tile_id, dim_num, cell_num);
    else if(*cell_type == typeid(int64_t))
      tile = new CoordinateTile<int64_t>(tile_id, dim_num, cell_num);
    else if(*cell_type == typeid(float))
      tile = new CoordinateTile<float>(tile_id, dim_num, cell_num);
    else if(*cell_type == typeid(double))
      tile = new CoordinateTile<double>(tile_id, dim_num, cell_num);
  }

  return tile; 
}


/******************************************************
******************** TILE ITERATORS *******************
******************************************************/

StorageManager::const_iterator::const_iterator() 
    : storage_manager_(NULL), array_descriptor_(NULL) {
}

StorageManager::const_iterator::const_iterator(
    StorageManager* storage_manager,
    const ArrayDescriptor* array_descriptor,
    unsigned int attribute_id,
    uint64_t rank)
    : storage_manager_(storage_manager), array_descriptor_(array_descriptor), 
      attribute_id_(attribute_id), rank_(rank) {
}

void StorageManager::const_iterator::operator=(
    const StorageManager::const_iterator& rhs) {
  array_descriptor_ = rhs.array_descriptor_;
  attribute_id_ = rhs.attribute_id_;
  storage_manager_ = rhs.storage_manager_;
  rank_ = rhs.rank_;
}

void StorageManager::const_iterator::operator+=(int64_t step) {
  rank_ += step;
}

StorageManager::const_iterator StorageManager::const_iterator::operator++() {
  ++rank_;
  return *this;
}

StorageManager::const_iterator StorageManager::const_iterator::operator++(
    int junk) {
  const_iterator it = *this;
  ++rank_;
  return it;
}

bool StorageManager::const_iterator::operator==(
    const StorageManager::const_iterator& rhs) const {
  return (rank_ == rhs.rank_ && attribute_id_ == rhs.attribute_id_ &&
          array_descriptor_ == rhs.array_descriptor_ &&
          storage_manager_ == rhs.storage_manager_); 
}

bool StorageManager::const_iterator::operator!=(
    const StorageManager::const_iterator& rhs) const {
  return (!(rank_ == rhs.rank_ && attribute_id_ == rhs.attribute_id_ &&
          array_descriptor_ == rhs.array_descriptor_ &&
          storage_manager_ == rhs.storage_manager_)); 
}

const Tile& StorageManager::const_iterator::operator*() const {
  assert(rank_ < array_descriptor_->array_info_->tile_ids_.size());
  assert(storage_manager_->check_on_get_tile(*array_descriptor_, 
             attribute_id_, array_descriptor_->array_info_->tile_ids_[rank_]));
  
  // For easy reference
  ArrayInfo& array_info = *(array_descriptor_->array_info_);

  return *storage_manager_->get_tile_by_rank(array_info, attribute_id_, rank_);
}

bool StorageManager::const_iterator::operator<(const const_iterator& it_R) const {
  // For easy reference
  const ArraySchema& array_schema_L = array_schema();
  const ArraySchema& array_schema_R = it_R.array_schema(); 
  BoundingCoordinatesPair bounding_coordinates_L = bounding_coordinates(); 
  BoundingCoordinatesPair bounding_coordinates_R = it_R.bounding_coordinates();

  // If both tiles belong to the same array
  if(array_schema_L.array_name() == array_schema_R.array_name())
    return rank() < it_R.rank();
  
  assert(ArraySchema::join_compatible(array_schema_L, array_schema_R).first);

  // The tiles belong to different arrays - check precedence along the 
  // global cell order using the upper bounding coordinates
  if(array_schema_L.precedes(bounding_coordinates_L.second,
                             bounding_coordinates_R.second))
    return true;
  else
    return false;
}


const ArraySchema& StorageManager::const_iterator::array_schema() const {
  return array_descriptor_->array_schema();
}

StorageManager::BoundingCoordinatesPair StorageManager::const_iterator::
    bounding_coordinates() const {
  return array_descriptor_->array_info()->bounding_coordinates_[rank_];
}

StorageManager::MBR StorageManager::const_iterator::mbr() const {
  return array_descriptor_->array_info()->mbrs_[rank_];
}

uint64_t StorageManager::const_iterator::tile_id() const {
  return array_descriptor_->array_info()->tile_ids_[rank_];
}

StorageManager::const_iterator StorageManager::begin(
    const ArrayDescriptor* array_descriptor,
    unsigned int attribute_id) {
  // Check array descriptor
  assert(check_array_descriptor(*array_descriptor)); 

  // For easy reference
  const ArrayInfo& array_info = *(array_descriptor->array_info_);

  // The array should be open in READ mode
  assert(array_info.array_mode_ == READ);

  // Check attribute id
  assert(attribute_id <= array_info.array_schema_.attribute_num());

  return const_iterator(this, array_descriptor, attribute_id, 0);
}

StorageManager::const_iterator StorageManager::end(
    const ArrayDescriptor* array_descriptor,
    unsigned int attribute_id) {
  // Check array descriptor
  assert(check_array_descriptor(*array_descriptor));
 
  // For easy reference
  const ArrayInfo& array_info = *(array_descriptor->array_info_);
  uint64_t tile_num = array_info.tile_ids_.size();

  // The array should be open in READ mode
  assert(array_info.array_mode_ == READ);

  // Check attribute id
  assert(attribute_id <= array_info.array_schema_.attribute_num());

  return const_iterator(this, array_descriptor, attribute_id, tile_num);
}

StorageManager::MBRs::const_iterator StorageManager::MBR_begin(
    const ArrayDescriptor* array_descriptor) const {
  // Check array descriptor
  assert(check_array_descriptor(*array_descriptor)); 

  // For easy reference
  const ArrayInfo& array_info = *(array_descriptor->array_info_);

  // The array must be open in READ mode
  assert(array_info.array_mode_ == READ);

  return array_info.mbrs_.begin(); 
}

StorageManager::MBRs::const_iterator StorageManager::MBR_end(
    const ArrayDescriptor* array_descriptor) const {
  // Check array descriptor
  assert(check_array_descriptor(*array_descriptor)); 
  
  // For easy reference
  const ArrayInfo& array_info = *(array_descriptor->array_info_);

  // The array must be open in READ mode
  assert(array_info.array_mode_ == READ);

  return array_info.mbrs_.end(); 
}

/******************************************************
*********************** MISC **************************
******************************************************/

void StorageManager::get_overlapping_tile_ids(
    const ArrayDescriptor* array_descriptor, const Tile::Range& range,
    std::vector<std::pair<uint64_t, bool> >* overlapping_tile_ids) const {
  // Check array descriptor
  assert(check_array_descriptor(*array_descriptor)); 

  // The array must be open in READ mode
  assert(array_descriptor->array_info_->array_mode_ == READ);
 
  // For easy reference
  const ArrayInfo& array_info = *( array_descriptor->array_info_);
  const ArraySchema& array_schema = array_info.array_schema_;
  const unsigned int dim_num = array_schema.dim_num();
  const uint64_t tile_num = array_info.tile_ids_.size();

  assert(array_info.mbrs_.size() == tile_num);
  assert(range.size() == 2 * dim_num);

  // Overlap type per dimension
  bool* partial = new bool[dim_num];
  bool* full = new bool[dim_num];

  bool overlap; // True if an MBR overlaps (partially or fully) the range
  std::pair<uint64_t, bool> overlapping_tile_id; // (tile_id, full)
  
  for(uint64_t i=0; i<tile_num; i++) {
    const MBR& mbr = array_info.mbrs_[i];
    overlap = true;
    overlapping_tile_id.second = true;

    // Determine overlap per dimension
    for(unsigned int j=0; j<dim_num; j++) {
      partial[j] = false;
      full[j] = false;
      if(mbr[2*j] >= range[2*j] && mbr[2*j+1] <= range[2*j+1])
        full[j] = true;
      else if(range[2*j] >= mbr[2*j] && range[2*j] <= mbr[2*j+1] || 
              range[2*j+1] >= mbr[2*j] && range[2*j+1] <= mbr[2*j+1])
        partial[j] = true;
    }

    // Determine MBR overlap
    for(unsigned int j=0; j<dim_num; j++) {
      if(!partial[j] && !full[j]) {
        overlap = false;
        break;
      }
  
      if(partial[j])
        overlapping_tile_id.second = false;
    }

    // Insert overlapping tile id into the result list
    if(overlap) {
      overlapping_tile_id.first = array_info.tile_ids_[i];
      overlapping_tile_ids->push_back(overlapping_tile_id);
    }
  }

  delete [] partial;
  delete [] full;
}

/******************************************************
***************** PRIVATE FUNCTIONS *******************
******************************************************/

bool StorageManager::check_array_descriptor(
    const ArrayDescriptor& array_descriptor) const {
  // Check if the array is open
  OpenArrays::const_iterator array_it = 
      open_arrays_.find(array_descriptor.array_name_);
  if(array_it == open_arrays_.end())
    return false;

  // Check if the descriptor is obsolete
  if(array_descriptor.array_info_id_ != array_it->second.id_)
    return false;

  return true;
}

bool StorageManager::check_on_append_tile(
    const ArrayDescriptor& array_descriptor,
    unsigned int attribute_id, const Tile* tile) const {
  // Check descriptor
  if(!check_array_descriptor(array_descriptor))
    return false;

  // For easy reference
  ArrayInfo& array_info = *(array_descriptor.array_info_);
  const ArraySchema& array_schema = array_info.array_schema_;
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int dim_num = array_schema.dim_num();

  // Check if the array is open in CREATE mode
  if(array_info.array_mode_ != CREATE)
    return false;

  // Check attribute id
  if(attribute_id > attribute_num)
    return false;

  // Check if tile type
  if(attribute_id < attribute_num && tile->tile_type() != Tile::ATTRIBUTE)
    return false;
  if(attribute_id == attribute_num && tile->tile_type() != Tile::COORDINATE)
    return false;

  // Check the cell type
  if (*(tile->cell_type()) != *(array_schema.type(attribute_id)))
    return false;

  // Check the dimensions of the tile  
  if(tile->tile_type() == Tile::COORDINATE &&
     tile->dim_num() != array_schema.dim_num())
    return false;

  // Check tile id
  // If this is not the first tile to be appended for this attribute...
  if(array_info.lastly_appended_tile_ids_[attribute_id] != SM_INVALID_TILE_ID) {
    // The tile ids must follow a strictly ascending order
    if(array_info.lastly_appended_tile_ids_[attribute_id] >= tile->tile_id()) {
      return false;
    } else {
    // The lastly inserted id of every other attribute must be either equal
    // to the currently inserted one, or equal to the last id of the attribute
    // of the currently inserted one.
    uint64_t last_tile_id = array_info.lastly_appended_tile_ids_[attribute_id]; 
    for(unsigned int i=0; i<=attribute_num; i++)
      if(i != attribute_id && 
         array_info.lastly_appended_tile_ids_[i] != last_tile_id &&
         array_info.lastly_appended_tile_ids_[i] != tile->tile_id())
        return false;
    }
  }

  // Check MBR to verify that the coordinates are inside the domain
  // Only for coordinate tiles
  if(tile->tile_type() == Tile::COORDINATE) {
    const MBR& mbr = tile->mbr();
    const std::vector<std::pair<double, double> >& dim_domains = 
        array_schema.dim_domains();
    for(unsigned int i=0; i<dim_num; i++)
      if(mbr[2*i] < dim_domains[i].first || mbr[2*i] > dim_domains[i].second ||
         mbr[2*i+1] < dim_domains[i].first || mbr[2*i+1] > dim_domains[i].second)
        return false;
  }

  array_info.lastly_appended_tile_ids_[attribute_id] = tile->tile_id();
  
  return true;
}

bool StorageManager::check_on_close_array(
    const ArrayDescriptor& array_descriptor) const {
  // Check descriptor
  if(!check_array_descriptor(array_descriptor))
    return false;

  // For easy reference
  const ArrayInfo& array_info = *(array_descriptor.array_info_);
  unsigned int attribute_num = array_info.array_schema_.attribute_num();

  // The array must either be empty, or all the lastly inserted tile ids must
  // be the same
  if(!is_empty(&array_descriptor)) {
    uint64_t last_tile_id = array_info.lastly_appended_tile_ids_[0];
    for(unsigned int i=1; i<=attribute_num; i++)
      if(array_info.lastly_appended_tile_ids_[i] != last_tile_id)
        return false;
  }

  return true;
}

bool StorageManager::check_on_get_tile(
    const ArrayDescriptor& array_descriptor,
    unsigned int attribute_id, uint64_t tile_id) const {
  // Check descriptor
  if(!check_array_descriptor(array_descriptor))
    return false;

  // For easy reference
  const ArrayInfo& array_info = *(array_descriptor.array_info_);

  // Check if the array is opened in READ mode
  if(array_info.array_mode_ != READ)
    return false;

  // Check attribute id
  if(attribute_id > array_info.array_schema_.attribute_num())
    return false;

  // Check if tile id exists
  if(tile_rank(array_info, tile_id) == SM_INVALID_RANK)
    return false;

  return true;
}

bool StorageManager::check_on_open_array(const std::string& array_name, 
                                         ArrayMode array_mode) const {
  OpenArrays::const_iterator array_it = open_arrays_.find(array_name);
 
  // The array must not be already open
  if(array_it != open_arrays_.end())
    return false;

  std::string dir_name = workspace_ + "/" + array_name;
  struct stat st;
  stat(dir_name.c_str(), &st);
  // If the array is opened in CREATE mode, the array directory must not exist
  if(array_mode == CREATE && S_ISDIR(st.st_mode))
    return false;
  // If the array is opened in READ mode, the array directory must exist
  if(array_mode == READ && !S_ISDIR(st.st_mode))
    return false;

  return true;
}

void StorageManager::create_array_directory(
    const std::string& array_name) const {
  std::string dir_name = workspace_ + "/" + array_name;

  int dir_flag = mkdir(dir_name.c_str(), S_IRWXU);
  assert(dir_flag == 0);
}

void StorageManager::create_workspace() {
  struct stat st;
  stat(workspace_.c_str(), &st);

  // If the workspace does not exist, create it
  if(!S_ISDIR(st.st_mode)) { 
    int dir_flag = mkdir(workspace_.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }
}

void StorageManager::delete_directory(const std::string& array_name) const {
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

void StorageManager::delete_tiles(ArrayInfo& array_info) const {
  for(unsigned int i=0; i<=array_info.array_schema_.attribute_num(); i++)
    delete_tiles(array_info, i);
}

void StorageManager::delete_tiles(ArrayInfo& array_info,
                                  unsigned int attribute_id) const {
  // Delete the tiles
  TileList::const_iterator tile_it = 
      array_info.tiles_[attribute_id].begin(); 
  TileList::const_iterator tile_it_end = 
      array_info.tiles_[attribute_id].end(); 
  for(; tile_it != tile_it_end; tile_it++)
    delete *tile_it;

  // Update the indices
  array_info.tiles_[attribute_id].clear(); 
  array_info.payload_sizes_[attribute_id] = 0; 
}

void StorageManager::flush_array_info(ArrayInfo& array_info) const {
  if(array_info.array_mode_ == CREATE) {
    flush_tiles(array_info);  
    flush_bounding_coordinates(array_info);
    flush_MBRs(array_info);   
    flush_offsets(array_info);
    flush_tile_ids(array_info);
    flush_array_schema(array_info);
  } 
  
  delete_tiles(array_info); 
}

void StorageManager::flush_array_schema(const ArrayInfo& array_info) const {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  std::pair<char*, uint64_t> ret = array_schema.serialize();

  // For easy reference
  char* buffer = ret.first;
  uint64_t buffer_size = ret.second; 

  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// tile#1_lower_dim#1(double) tile#1_lower_dim#2(double) ... 
// tile#1_upper_dim#1(double) tile#1_upper_dim#2(double) ... 
// tile#2_lower_dim#1(double) tile#2_lower_dim#2(double) ... 
// tile#2_upper_dim#1(double) tile#2_upper_dim#2(double) ...
// ... 
void StorageManager::flush_bounding_coordinates(
    const ArrayInfo& array_info) const {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  uint64_t tile_num = array_info.tile_ids_.size();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_BOUNDING_COORDINATES_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    unsigned int dim_num = array_schema.dim_num();
    uint64_t buffer_size = 2 * tile_num * dim_num * sizeof(double);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    uint64_t offset = 0;
    for(uint64_t i=0; i<tile_num; i++) {
      // Lower bounding coordinates
      memcpy(buffer+offset, 
             &array_info.bounding_coordinates_[i].first[0], 
             dim_num * sizeof(double));
      offset += dim_num * sizeof(double);
      // Upper bounding coordinates
      memcpy(buffer+offset, 
             &array_info.bounding_coordinates_[i].second[0], 
             dim_num * sizeof(double));
      offset += dim_num * sizeof(double);
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }  

  close(fd);
}

// FILE FORMAT:
// MBR#1_dim#1_low(double) MBR#1_dim#1_high(double) ...
// MBR#1_dim#2_low(double) MBR#1_dim#2_high(double) ...
// ...
// MBR#2_dim#1_low(double) MBR#2_dim#1_high(double) ...
// ...
void StorageManager::flush_MBRs(const ArrayInfo& array_info) const {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  uint64_t tile_num = array_info.tile_ids_.size();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_MBRS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    unsigned int dim_num = array_schema.dim_num();
    uint64_t buffer_size = tile_num * 2 * dim_num * sizeof(double);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    uint64_t offset = 0;
    for(uint64_t i=0; i<tile_num; i++) {
      memcpy(buffer+offset, &array_info.mbrs_[i][0], 2 * dim_num * sizeof(double));
      offset += 2 * dim_num * sizeof(double);
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
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
// NOTE: Do not forget the extra coordinate attribute
void StorageManager::flush_offsets(const ArrayInfo& array_info) const {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  uint64_t tile_num = array_info.tile_ids_.size();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_OFFSETS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    unsigned int attribute_num = array_schema.attribute_num();
    uint64_t buffer_size = (attribute_num+1) * tile_num * sizeof(uint64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    uint64_t offset = 0;
    for(unsigned int i=0; i<=attribute_num; i++) {
      memcpy(buffer+offset, 
             &array_info.offsets_[i][0], tile_num * sizeof(uint64_t));
      offset += tile_num * sizeof(uint64_t);
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }
  
  close(fd);
}

// FILE FORMAT:
// tile_num(uint64_t)
//   tile_id#1(uint64_t) tile_id#2(uint64_t)  ...
void StorageManager::flush_tile_ids(const ArrayInfo& array_info) const {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  uint64_t tile_num = array_info.tile_ids_.size();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_TILE_IDS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);
 
  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    uint64_t buffer_size = (tile_num+1) * sizeof(uint64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    memcpy(buffer, &tile_num, sizeof(uint64_t));
    memcpy(buffer+sizeof(uint64_t), 
           &array_info.tile_ids_[0], tile_num * sizeof(uint64_t));

    // Write buffer and clean up
    write(fd, buffer, buffer_size);  
    delete [] buffer;
  }

  close(fd);
}

void StorageManager::flush_tiles(ArrayInfo& array_info) const {
  for(unsigned int i=0; i<array_info.array_schema_.attribute_num()+1; i++)
    flush_tiles(array_info, i);
}

void StorageManager::flush_tiles(ArrayInfo& array_info,
                                 unsigned int attribute_id) const {
  // For easy reference
  uint64_t segment_size = array_info.payload_sizes_[attribute_id];
  const ArraySchema& array_schema = array_info.array_schema_; 

  if(segment_size > 0) {
    // Open file
    std::string filename = workspace_ + "/" + 
                           array_schema.array_name() + "/" + 
                           array_schema.attribute_name(attribute_id) +
                           SM_TILE_DATA_FILE_SUFFIX;		
    int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC,  
                  S_IRWXU);
    assert(fd != -1);

    // Retrieve the current file offset (equal to the file size)
    struct stat st;
    fstat(fd, &st);
    uint64_t file_offset = st.st_size;

    // Prepare a segment and append it to the file
    char *segment = new char[segment_size];
    prepare_segment(array_info, attribute_id, file_offset,
                    segment_size, segment);
    write(fd, segment, segment_size);
  
    // Clean up 
    delete segment;
    close(fd);
  }
}

const Tile* StorageManager::get_tile_by_rank(ArrayInfo& array_info, 
                                             unsigned int attribute_id,
                                             uint64_t rank) const {
  // For easy reference
  const uint64_t& rank_low = array_info.rank_ranges_[attribute_id].first;
  const uint64_t& rank_high = array_info.rank_ranges_[attribute_id].second;

  // Fetch from the disk if the tile is not in main memory
  if(array_info.tiles_[attribute_id].size() == 0 ||
     (rank < rank_low || rank > rank_high)) 
    // The following updates rank_low and rank_high
    load_tiles_from_disk(array_info, attribute_id, rank);

  assert(rank >= rank_low && rank <= rank_high);
  assert(rank - rank_low <= array_info.tiles_[attribute_id].size());

  return array_info.tiles_[attribute_id][rank-rank_low];
}

inline
void StorageManager::init_array_info(ArrayInfo& array_info, 
                                     const ArrayMode array_mode, 
                                     const ArraySchema& array_schema) const {
  // For easy reference
  const unsigned int attribute_num = array_schema.attribute_num();

  array_info.array_mode_ = array_mode;
  array_info.array_schema_ = array_schema;
  array_info.id_ = array_info_id_++;
  array_info.offsets_.resize(attribute_num+1);
  array_info.payload_sizes_.resize(attribute_num+1);
  array_info.tiles_.resize(attribute_num+1);
  array_info.rank_ranges_.resize(attribute_num+1);
  array_info.lastly_appended_tile_ids_.resize(attribute_num+1);
  for(unsigned int i=0; i<=attribute_num; i++)
    array_info.lastly_appended_tile_ids_[i] = SM_INVALID_TILE_ID;
}

void StorageManager::load_array_schema(const std::string& array_name,
                                       ArraySchema& array_schema) const {
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initialzie buffer
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;
  char* buffer = new char[buffer_size];
 
  // Load array schema
  read(fd, buffer, buffer_size);
  array_schema.deserialize(buffer, buffer_size);

  // Clean up
  close(fd);
  delete [] buffer;
}

// FILE FORMAT:
// tile#1_lower_dim#1(double) tile#1_lower_dim#2(double) ... 
// tile#1_upper_dim#1(double) tile#1_upper_dim#2(double) ... 
// tile#2_lower_dim#1(double) tile#2_lower_dim#2(double) ... 
// tile#2_upper_dim#1(double) tile#2_upper_dim#2(double) ...
// ... 
void StorageManager::load_bounding_coordinates(ArrayInfo& array_info) {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  uint64_t tile_num = array_info.tile_ids_.size();
  unsigned int dim_num = array_schema.dim_num();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_BOUNDING_COORDINATES_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;
  assert(buffer_size == tile_num*2*dim_num*sizeof(double));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  uint64_t offset = 0;
  array_info.bounding_coordinates_.resize(tile_num);

  // Load bounding coordinates
  for(uint64_t i=0; i<tile_num; i++) {
    array_info.bounding_coordinates_[i].first.resize(dim_num);
    memcpy(&array_info.bounding_coordinates_[i].first[0], 
           buffer + offset, dim_num * sizeof(double));
    offset += dim_num * sizeof(double);
    array_info.bounding_coordinates_[i].second.resize(dim_num);
    memcpy(&array_info.bounding_coordinates_[i].second[0], 
           buffer + offset, dim_num * sizeof(double));
    offset += dim_num * sizeof(double);
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// MBR#1_dim#1_low(double) MBR#1_dim#1_high(double) ...
// MBR#1_dim#2_low(double) MBR#1_dim#2_high(double) ...
// ...
// MBR#2_dim#1_low(double) MBR#2_dim#1_high(double) ...
// ...
void StorageManager::load_MBRs(ArrayInfo& array_info) {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  unsigned int dim_num = array_schema.dim_num();
  uint64_t tile_num = array_info.tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_MBRS_FILENAME +  
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;
  assert(buffer_size == tile_num*2*dim_num*sizeof(double));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  uint64_t offset = 0;
  array_info.mbrs_.resize(tile_num);

  // Load MBRs
  for(uint64_t i=0; i<tile_num; i++) {
    array_info.mbrs_[i].resize(2*dim_num);
    memcpy(&array_info.mbrs_[i][0], buffer + offset, 2*dim_num*sizeof(double));
    offset += 2 * dim_num * sizeof(double);
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(uint64_t)
// tile#2_of_attribute#1_offset(uint64_t)
// ...
// tile#1_of_attribute#2_offset(uint64_t)
// tile#2_of_attribute#2_offset(uint64_t)
// ...
// NOTE: Do not forget the extra coordinate attribute
void StorageManager::load_offsets(ArrayInfo& array_info) {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t tile_num = array_info.tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_OFFSETS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;
  assert(buffer_size == (attribute_num+1)*tile_num*sizeof(uint64_t));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  uint64_t offset = 0;

  // Load offsets
  for(unsigned int i=0; i<=attribute_num; i++) {
    array_info.offsets_[i].resize(tile_num);
    memcpy(&array_info.offsets_[i][0], buffer + offset, tile_num*sizeof(uint64_t));
    offset += tile_num * sizeof(uint64_t);
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

inline
std::pair<uint64_t, uint64_t> StorageManager::load_payloads_into_buffer(
    const ArrayInfo& array_info, unsigned int attribute_id, 
    uint64_t start_rank, char*& buffer) const {
  // For easy reference
  const std::string& array_name = array_info.array_schema_.array_name();
  const std::string& attribute_name = 
      array_info.array_schema_.attribute_name(attribute_id);
  const OffsetList& offsets = array_info.offsets_[attribute_id];
  const uint64_t tile_num = offsets.size();
  assert(tile_num == array_info.tile_ids_.size());

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + attribute_name + 
                         SM_TILE_DATA_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initilizations
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = 0;
  uint64_t tiles_in_buffer = 0;
  int64_t rank = start_rank;

  // Calculate buffer size (smallest size larger than or equal to segment_size_)
  while(rank < tile_num && buffer_size < segment_size_) {
    if(rank == tile_num-1)
      buffer_size += st.st_size - offsets[rank];
    else
      buffer_size += offsets[rank+1] - offsets[rank];
    rank++;
    tiles_in_buffer++;
  }

  assert(buffer_size != 0);
  assert(offsets[start_rank] + buffer_size <= st.st_size);

  // Read payloads into buffer
  buffer = new char[buffer_size];
  lseek(fd, offsets[start_rank], SEEK_SET);
  read(fd, buffer, buffer_size); 
  close(fd);

  return std::pair<uint64_t, uint64_t>(buffer_size, tiles_in_buffer);
}

// FILE FORMAT:
// tile_num(uint64_t)
//   tile_id#1(uint64_t) tile_id#2(uint64_t)  ...
void StorageManager::load_tile_ids(ArrayInfo& array_info) const {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_TILE_IDS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;

  if(buffer_size == 0) // Empty array
    return;

  assert(buffer_size > sizeof(uint64_t));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  uint64_t tile_num;
  memcpy(&tile_num, buffer, sizeof(uint64_t));
  assert(buffer_size == (tile_num+1)*sizeof(uint64_t));
  array_info.tile_ids_.resize(tile_num);

  // Load tile_ids
  memcpy(&array_info.tile_ids_[0], 
         buffer+sizeof(uint64_t), tile_num*sizeof(uint64_t));

  // Clean up
  delete [] buffer;
  close(fd);
}

void StorageManager::load_tiles_from_disk(ArrayInfo& array_info,
                                          unsigned int attribute_id,
                                          uint64_t start_rank) const { 
  char* buffer;

  // Load the tile payloads from the disk into a buffer
  std::pair<uint64_t, uint64_t> ret = load_payloads_into_buffer(
      array_info, attribute_id, start_rank, buffer); 

  // For easy reference
  uint64_t buffer_size = ret.first;
  uint64_t tiles_in_buffer = ret.second;

  // Delete previous tiles from main memory
  delete_tiles(array_info, attribute_id);
  
  // Create the tiles from the payloads in the buffer and load them
  // into the tile book-keeping structure
  load_tiles_from_buffer(array_info, attribute_id, start_rank, 
                         buffer, buffer_size, tiles_in_buffer);         
 
  // Update rank range in main memory
  array_info.rank_ranges_[attribute_id].first = start_rank;
  array_info.rank_ranges_[attribute_id].second = 
      start_rank + tiles_in_buffer - 1;

  // Clean up
  delete [] buffer;
}

void StorageManager::load_tiles_from_buffer(ArrayInfo& array_info, 
    unsigned int attribute_id, uint64_t start_rank, char* buffer, 
    uint64_t buffer_size, uint64_t tiles_in_buffer) const {
  // For easy reference
  const ArraySchema& array_schema = array_info.array_schema_;
  const OffsetList& offsets = array_info.offsets_[attribute_id];
  const TileIds& tile_ids = array_info.tile_ids_;
  const MBRs& mbrs = array_info.mbrs_;
  const BoundingCoordinates& bounding_coordinates = 
      array_info.bounding_coordinates_;
  assert(offsets.size() == tile_ids.size());
  TileList& tiles = array_info.tiles_[attribute_id];
  const unsigned int attribute_num = array_schema.attribute_num();

  // Initializations
  uint64_t buffer_offset = 0;
  uint64_t tile_id, tile_size;
  uint64_t rank = start_rank;
  tiles.resize(tiles_in_buffer);
  char* payload;
  
  // Load tiles
  for(uint64_t i=0; i<tiles_in_buffer; i++, rank++) {
    assert(rank < tile_ids.size());
    tile_id = tile_ids[rank];

    if(rank == offsets.size()-1)
      tile_size = buffer_size - buffer_offset;
    else
      tile_size = offsets[rank+1] - offsets[rank];

    assert(buffer_offset + tile_size <= buffer_size);
    payload = new char[tile_size];
    memcpy(payload, buffer + buffer_offset, tile_size);
    
    uint64_t cell_num = tile_size / array_schema.cell_size(attribute_id);
    Tile* tile = new_tile(array_schema, attribute_id, tile_id, cell_num);
    tile->set_payload(payload, tile_size);
    if(tile->tile_type() == Tile::COORDINATE) 
      tile->set_mbr(mbrs[rank]);
    
    tiles[i] = tile;
    buffer_offset += tile_size;

    // Clean up
    delete [] payload;
  }
}

bool StorageManager::path_exists(const std::string& path) const {
  struct stat st;
  stat(path.c_str(), &st);
  return S_ISDIR(st.st_mode);
}

inline
void StorageManager::prepare_segment( 
    ArrayInfo& array_info, unsigned int attribute_id,
    uint64_t file_offset, uint64_t segment_size, 
    char* segment) const {
  uint64_t segment_offset = 0;
  uint64_t tile_size;
  TileList::const_iterator tile_it = 
      array_info.tiles_[attribute_id].begin();
  TileList::const_iterator tile_it_end = 
      array_info.tiles_[attribute_id].end();

  for(; tile_it != tile_it_end; tile_it++) {
    tile_size = (*tile_it)->tile_size();
    assert(tile_size + segment_offset <= segment_size);
    (*tile_it)->copy_payload(segment+segment_offset);
    array_info.offsets_[attribute_id].push_back(
        file_offset + segment_offset);
    segment_offset += tile_size;
  }
}

inline
void StorageManager::set_workspace(const std::string& path) {
  workspace_ = path;
  
  // Replace '~' with the absolute path
  if(workspace_[0] == '~') {
    workspace_ = std::string(getenv("HOME")) +
                 workspace_.substr(1, workspace_.size()-1);
  }

  // Check if the input path is an existing directory 
  assert(path_exists(workspace_));
 
  workspace_ += "/StorageManager";
}

uint64_t StorageManager::tile_rank(const ArrayInfo& array_info, 
                                   uint64_t tile_id) const {
  // Check of the array has not tiles
  if(array_info.tile_ids_.size() == 0)
    return SM_INVALID_RANK;

  // Perform binary search
  uint64_t min = 0;
  uint64_t max = array_info.tile_ids_.size()-1;
  uint64_t mid;
  while(min <= max) {
    mid = min + ((max - min) / 2);
    if(array_info.tile_ids_[mid] == tile_id)  // Tile found!
      return mid;
    else if(array_info.tile_ids_[mid] > tile_id) {
      if(mid == 0)
        return SM_INVALID_RANK;
      max = mid - 1;
    }
    else // (array_info.tile_ids_[mid] < tile_id)
      min = mid + 1;
  }

  // Tile id not found
  return SM_INVALID_RANK;
}

