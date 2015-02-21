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
#include <math.h>
#include <typeinfo>
#include <iostream>

/******************************************************
************ STATIC VARIABLE DEFINITIONS **************
******************************************************/

uint64_t StorageManager::fragment_info_id_ = 0;

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
}

/******************************************************
******************* ARRAY FUNCTIONS *******************
******************************************************/

bool StorageManager::array_defined(const std::string& array_name) const {
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

bool StorageManager::array_loaded(const std::string& array_name) const {
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_FRAGMENTS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

void StorageManager::clear_array(const std::string& array_name) {
  std::string dirname = workspace_ + "/" + array_name + "/";
  std::string filename; 

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0 ||
       strcmp(next_file->d_name, "array_schema.bkp") == 0)
      continue;
    if(strcmp(next_file->d_name, "fragments.bkp") == 0)  {
      filename = dirname + next_file->d_name;
      remove(filename.c_str());
    }
    else {  // It is a fragment folder
      delete_fragment(array_name, next_file->d_name);
    }
  } 
  
  closedir(dir);
}

void StorageManager::close_array(const ArrayDescriptor* ad) {
  for(int i=0; i<ad->fd_.size(); ++i)
    close_fragment(ad->fd_[i]);

  delete ad;
}

void StorageManager::close_fragment(
    const FragmentDescriptor* fd) {
  assert(check_on_close_fragment(*fd)); 
 
  // For easy reference
  FragmentInfo& fragment_info = *(fd->fragment_info_);
  const std::string& array_name = fd->array_name_;
  const std::string& fragment_name = fragment_info.fragment_name_;
  
  flush_fragment_info(fragment_info); 
  open_fragments_.erase(array_name + "_" + fragment_name); 

  delete fd;
}

void StorageManager::define_array(const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();

  // Create array directory
  std::string dir_name = workspace_ + "/" + array_name + "/"; 
  int dir_flag = mkdir(dir_name.c_str(), S_IRWXU);
  assert(dir_flag == 0);

  // Open file
  std::string filename = dir_name + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Serialize array schema
  std::pair<char*, uint64_t> ret = array_schema.serialize();
  char* buffer = ret.first;
  uint64_t buffer_size = ret.second; 

  // Store the array schema
  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);
}

void StorageManager::delete_array(const std::string& array_name) {
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
    if(strcmp(next_file->d_name, "fragments.bkp") == 0 ||
       strcmp(next_file->d_name, "array_schema.bkp") == 0) {
      filename = dirname + next_file->d_name;
      remove(filename.c_str());
    }
    else {  // It is a fragment folder
      delete_fragment(array_name, next_file->d_name);
    }
  } 
  
  closedir(dir);
  rmdir(dirname.c_str());
}

void StorageManager::flush_fragments_bkp(
   const std::string& array_name, 
   const char* buffer, uint64_t buffer_size) const { 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_FRAGMENTS_FILENAME + SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
  int fd = open(filename.c_str(), 
                O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Flush the buffer into the file
  write(fd, buffer, buffer_size);

  // Clean up
  close(fd);
}

bool StorageManager::fragment_empty(const FragmentDescriptor* fd) const {
  assert(check_fragment_descriptor(*fd));

  return fd->fragment_info_->tile_ids_.size() == 0;
}

bool StorageManager::fragment_exists(
    const std::string& array_name,
    const std::string& fragment_name) const {
  std::string fragment_dir = workspace_ + "/" + array_name 
                                           + "/" + fragment_name;
  return path_exists(fragment_dir);   
}

void StorageManager::load_fragments_bkp(const std::string& array_name,
                                        char*& buffer, 
                                        uint64_t& buffer_size) const {
  buffer = NULL;
  buffer_size = 0;

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         SM_FRAGMENTS_FILENAME + SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);

  // If the file does not exist, the tree is empty
  if(fd == -1)
    return;

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  buffer_size = st.st_size;
  buffer = new char[buffer_size];
 
  // Load contents of the file into the buffer
  read(fd, buffer, buffer_size);

  close(fd);
}

void StorageManager::modify_array_schema(
    const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();


  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Serialize array schema
  std::pair<char*, uint64_t> ret = array_schema.serialize();
  char* buffer = ret.first;
  uint64_t buffer_size = ret.second; 

  // Store the array schema
  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);
}

void StorageManager::modify_fragment_bkp(
    const FragmentDescriptor* fd, uint64_t capacity) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd->fragment_info_->array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fd->fragment_info_->fragment_name_;
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t old_capacity = array_schema.capacity();

  // New book-keeping structures
  FragmentInfo fragment_info;
  fragment_info.array_schema_ = &array_schema;
  fragment_info.fragment_name_ = fragment_name;
  BoundingCoordinates& new_bounding_coordinates = 
      fragment_info.bounding_coordinates_;
  MBRs& new_mbrs = fragment_info.mbrs_;
  Offsets& new_offsets = fragment_info.offsets_;
  TileIds& new_tile_ids = fragment_info.tile_ids_;

  // Reserve proper space for the new book-keeping structures
  uint64_t old_tile_num = fd->fragment_info_->tile_ids_.size();
  uint64_t expected_new_tile_num = 
      ceil(double(old_tile_num) * old_capacity / capacity);
  new_bounding_coordinates.reserve(expected_new_tile_num);
  new_mbrs.reserve(expected_new_tile_num);
  new_offsets.resize(attribute_num+1);
  for(unsigned int i=0; i<=attribute_num; ++i)
    new_offsets[i].reserve(expected_new_tile_num);
  new_tile_ids.reserve(expected_new_tile_num);

  // Compute new book-keeping info
  uint64_t new_tile_id = 0;
  uint64_t cell_num = 0;
  const_iterator tile_it = begin(fd, attribute_num); 
  const_iterator tile_it_end = end(fd, attribute_num);
  Tile::const_iterator cell_it, cell_it_end;
  BoundingCoordinatesPair bounding_coordinates;
  MBR mbr;
  std::vector<double> coord;
  for(; tile_it != tile_it_end; ++tile_it) {
    cell_it = (*tile_it).begin();
    cell_it_end = (*tile_it).end();
    for(; cell_it != cell_it_end; ++cell_it) {
      coord = *cell_it;
      if(cell_num % capacity == 0) { // New tile
        if(cell_num != 0) { 
          new_mbrs.push_back(mbr); 
          new_bounding_coordinates.push_back(bounding_coordinates);
          new_tile_ids.push_back(new_tile_id++);
          for(unsigned int i=0; i<=attribute_num; ++i) {
            new_offsets[i].push_back((cell_num - capacity) * 
                                     array_schema.cell_size(i)); 
          }
          mbr.clear();
        }
        bounding_coordinates.first = *cell_it; 
      }
      expand_mbr(coord, mbr);
      bounding_coordinates.second = coord;
      ++cell_num;
    }
  }
  // Take into account the book-keeping info of the last new tile
  if(cell_num % capacity != 0) {
    new_mbrs.push_back(mbr); 
    new_bounding_coordinates.push_back(bounding_coordinates);
    new_tile_ids.push_back(new_tile_id++);
    for(unsigned int i=0; i<=attribute_num; ++i) {
      new_offsets[i].push_back((cell_num - (cell_num % capacity)) * 
                               array_schema.cell_size(i));  
    } 
  }

  // Flush the new book-keeping structures to disk
  flush_bounding_coordinates(fragment_info);
  flush_MBRs(fragment_info);   
  flush_offsets(fragment_info);
  flush_tile_ids(fragment_info);
}

StorageManager::ArrayDescriptor* StorageManager::open_array(
    const std::string& array_name,
    const std::vector<std::string>& fragment_names,
    Mode mode) {

  std::vector<const FragmentDescriptor*> fd;
  fd.reserve(fragment_names.size());

  // Load array schema
  const ArraySchema* array_schema = load_array_schema(array_name);

  // Open fragments
  for(int i=0; i<fragment_names.size(); ++i)
    fd.push_back(open_fragment(array_schema, fragment_names[i], mode));

  return new ArrayDescriptor(array_schema, fd); 
}

StorageManager::FragmentDescriptor* StorageManager::open_fragment(
    const ArraySchema* array_schema, 
    const std::string& fragment_name,
    Mode mode) {
  // For easy reference
  const std::string& array_name = array_schema->array_name();

  assert(check_on_open_fragment(array_name, fragment_name, mode));

  // Create an fragment info entry in open_fragments_
  std::pair<OpenFragments::iterator, bool> ret = open_fragments_.insert(
      std::pair<std::string, FragmentInfo>(array_name + "_" + fragment_name, 
                                           FragmentInfo()));
  FragmentInfo& fragment_info = ret.first->second;

  // Create array directory
  if(mode == CREATE)
    create_fragment_directory(array_name, fragment_name);
 
  // Initialize array info
  init_fragment_info(fragment_name, array_schema, mode, fragment_info);

  // Load book-keeping structures
  if(mode == READ) {
    load_tile_ids(fragment_info);      
    if(fragment_info.tile_ids_.size() != 0) { // If the fragment is not empty
      load_bounding_coordinates(fragment_info);
      load_MBRs(fragment_info);  
      load_offsets(fragment_info);
    }
  }

  return new FragmentDescriptor(&fragment_info); 
}

/******************************************************
******************** TILE FUNCTIONS *******************
******************************************************/

void StorageManager::append_tile(const Tile* tile, 
                                 const FragmentDescriptor* fd,
                                 unsigned int attribute_id) {
  // If tile is empty, delete it and exit
  if(tile->cell_num() == 0) {
    delete tile;
    return;
  }

  assert(check_on_append_tile(*fd, attribute_id, tile));

  // For easy reference
  FragmentInfo& fragment_info = *(fd->fragment_info_);

  // Update indices
  // For both attribute and coordinate tiles
  if(fragment_empty(fd) || 
     fragment_info.tile_ids_.back() != tile->tile_id())
    fragment_info.tile_ids_.push_back(tile->tile_id());
  fragment_info.tiles_[attribute_id].push_back(tile);
  fragment_info.payload_sizes_[attribute_id] += tile->tile_size();
  // Only for coordinate tiles
  if(tile->tile_type() == Tile::COORDINATE) {
    fragment_info.mbrs_.push_back(tile->mbr());
    fragment_info.bounding_coordinates_.push_back(tile->bounding_coordinates());
  }

  // Flush tiles to disk if the sum of payloads exceeds the segment size
  if(fragment_info.payload_sizes_[attribute_id] >= segment_size_) { 
    flush_tiles(fragment_info, attribute_id);
    delete_tiles(fragment_info, attribute_id);
  }
} 

const Tile* StorageManager::get_tile( 
    const FragmentDescriptor* fd,
    unsigned int attribute_id, 
    uint64_t tile_id) {
  assert(check_on_get_tile(*fd, attribute_id, tile_id));

  // For easy reference
  FragmentInfo& fragment_info = *(fd->fragment_info_);

  uint64_t rank = tile_rank(fragment_info, tile_id); 

  return get_tile_by_rank(fragment_info, attribute_id, rank);
}

const Tile* StorageManager::get_tile_by_rank( 
    const FragmentDescriptor* fd,
    unsigned int attribute_id, 
    uint64_t rank) {
  assert(check_on_get_tile_by_rank(*fd, attribute_id, rank));

  // For easy reference
  FragmentInfo& fragment_info = *(fd->fragment_info_);

  return get_tile_by_rank(fragment_info, attribute_id, rank);
}

Tile* StorageManager::new_tile(
    const ArraySchema& array_schema, 
    unsigned int attribute_id, 
    uint64_t tile_id, 
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

uint64_t StorageManager::get_tile_id( 
    const FragmentDescriptor* fd, uint64_t rank) const {
  assert(rank >= 0 && rank < fd->fragment_info_->tile_ids_.size());
  return fd->fragment_info_->tile_ids_[rank];
}

/******************************************************
******************** TILE ITERATORS *******************
******************************************************/

StorageManager::const_iterator::const_iterator() 
    : storage_manager_(NULL), fd_(NULL) {
}

StorageManager::const_iterator::const_iterator(
    const StorageManager* storage_manager,
    const FragmentDescriptor* fd,
    unsigned int attribute_id,
    uint64_t rank)
    : storage_manager_(storage_manager), fd_(fd),
      attribute_id_(attribute_id), rank_(rank) {
}

void StorageManager::const_iterator::operator=(
    const StorageManager::const_iterator& rhs) {
  fd_ = rhs.fd_;
  attribute_id_ = rhs.attribute_id_;
  storage_manager_ = rhs.storage_manager_;
  rank_ = rhs.rank_;
}

StorageManager::const_iterator StorageManager::const_iterator::operator+(
int64_t step) {
  const_iterator it = *this;
  it.rank_ += step;
  return it;
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
          fd_ == rhs.fd_ && storage_manager_ == rhs.storage_manager_); 
}

bool StorageManager::const_iterator::operator!=(
    const StorageManager::const_iterator& rhs) const {
  return (!(rank_ == rhs.rank_ && attribute_id_ == rhs.attribute_id_ &&
          fd_ == rhs.fd_ && storage_manager_ == rhs.storage_manager_)); 
}

const Tile& StorageManager::const_iterator::operator*() const {
  assert(rank_ < fd_->fragment_info_->tile_ids_.size());
  assert(storage_manager_->check_on_get_tile( 
             *fd_, attribute_id_, fd_->fragment_info_->tile_ids_[rank_]));
  // For easy reference
  FragmentInfo& fragment_info = *(fd_->fragment_info_);

  return *storage_manager_->get_tile_by_rank(fragment_info,
                                             attribute_id_, rank_);
}

bool StorageManager::const_iterator::operator<(
    const const_iterator& it_R) const {
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
  return *fd_->fragment_info_->array_schema_;
}

StorageManager::BoundingCoordinatesPair StorageManager::const_iterator::
    bounding_coordinates() const {
  return fd_->fragment_info()->bounding_coordinates_[rank_];
}

StorageManager::MBR StorageManager::const_iterator::mbr() const {
  return fd_->fragment_info()->mbrs_[rank_];
}

uint64_t StorageManager::const_iterator::tile_id() const {
  return fd_->fragment_info()->tile_ids_[rank_];
}

StorageManager::const_iterator StorageManager::begin(
    const FragmentDescriptor* fd,
    unsigned int attribute_id) const {
  // Check array descriptor
  assert(check_fragment_descriptor(*fd)); 

  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // The fragment should be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  // Check attribute id
  assert(attribute_id <= fd->fragment_info_->array_schema_->attribute_num());

  return const_iterator(this, fd, attribute_id, 0);
}

StorageManager::const_iterator StorageManager::end(
    const FragmentDescriptor* fd, 
    unsigned int attribute_id) const {
  // Check array descriptor
  assert(check_fragment_descriptor(*fd));
 
  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);
  uint64_t tile_num = fragment_info.tile_ids_.size();

  // The fragment should be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  // Check attribute id
  assert(attribute_id <= fd->fragment_info_->array_schema_->attribute_num());

  return const_iterator(this, fd, attribute_id, tile_num);
}

StorageManager::MBRs::const_iterator StorageManager::MBR_begin(
    const FragmentDescriptor* fd) const {
  // Check array descriptor
  assert(check_fragment_descriptor(*fd)); 

  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // The fragment must be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  return fragment_info.mbrs_.begin(); 
}

StorageManager::MBRs::const_iterator StorageManager::MBR_end(
    const FragmentDescriptor*fd) const {
  // Check fragment descriptor
  assert(check_fragment_descriptor(*fd)); 
  
  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // The fragment must be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  return fragment_info.mbrs_.end(); 
}

/******************************************************
*********************** MISC **************************
******************************************************/

void StorageManager::get_overlapping_tile_ids(
    const FragmentDescriptor* fd, 
    const Tile::Range& range,
    std::vector<std::pair<uint64_t, bool> >* overlapping_tile_ids) const {
  // Check array descriptor
  assert(check_fragment_descriptor(*fd)); 

  // The fragment must be open in READ mode
  assert(fd->fragment_info_->fragment_mode_ == READ);
 
  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);
  const unsigned int dim_num = fd->array_schema()->dim_num();
  const uint64_t tile_num = fragment_info.tile_ids_.size();

  assert(fragment_info.mbrs_.size() == tile_num);
  assert(range.size() == 2 * dim_num);

  // Overlap type per dimension
  bool* partial = new bool[dim_num];
  bool* full = new bool[dim_num];

  bool overlap; // True if an MBR overlaps (partially or fully) the range
  std::pair<uint64_t, bool> overlapping_tile_id; // (tile_id, full)
  
  for(uint64_t i=0; i<tile_num; i++) {
    const MBR& mbr = fragment_info.mbrs_[i];
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
      overlapping_tile_id.first = fragment_info.tile_ids_[i];
      overlapping_tile_ids->push_back(overlapping_tile_id);
    }
  }

  delete [] partial;
  delete [] full;
}

void StorageManager::get_overlapping_tile_ranks(
    const FragmentDescriptor* fd, 
    const Tile::Range& range,
    std::vector<std::pair<uint64_t, bool> >* overlapping_tile_ranks) const {
  // Check array descriptor
  assert(check_fragment_descriptor(*fd)); 

  // The fragment must be open in READ mode
  assert(fd->fragment_info_->fragment_mode_ == READ);
 
  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);
  const unsigned int dim_num = fd->array_schema()->dim_num();
  const uint64_t tile_num = fragment_info.tile_ids_.size();

  assert(fragment_info.mbrs_.size() == tile_num);
  assert(range.size() == 2 * dim_num);

  // Overlap type per dimension
  bool* partial = new bool[dim_num];
  bool* full = new bool[dim_num];

  bool overlap; // True if an MBR overlaps (partially or fully) the range
  std::pair<uint64_t, bool> overlapping_tile_rank; // (tile_rank, full)
  
  for(uint64_t i=0; i<tile_num; i++) {
    const MBR& mbr = fragment_info.mbrs_[i];
    overlap = true;
    overlapping_tile_rank.second = true;

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
        overlapping_tile_rank.second = false;
    }

    // Insert overlapping tile rank into the result list
    if(overlap) {
      overlapping_tile_rank.first = i;
      overlapping_tile_ranks->push_back(overlapping_tile_rank);
    }
  }

  delete [] partial;
  delete [] full;
}

/******************************************************
***************** PRIVATE FUNCTIONS *******************
******************************************************/

bool StorageManager::check_fragment_descriptor(
    const FragmentDescriptor& fd) const {
  // Check if the fragment is open
  OpenFragments::const_iterator fragment_it = 
      open_fragments_.find(fd.array_name_ + "_" + fd.fragment_name_);
  if(fragment_it == open_fragments_.end())
    return false;

  // Check if the descriptor is obsolete
  if(fd.fragment_info_id_ != fragment_it->second.id_)
    return false;

  return true;
}

bool StorageManager::check_on_append_tile(
    const FragmentDescriptor& fd,
    unsigned int attribute_id, 
    const Tile* tile) const {
  // Check descriptor
  if(!check_fragment_descriptor(fd))
    return false;

  // For easy reference
  const ArraySchema& array_schema = *(fd.fragment_info_->array_schema_);
  FragmentInfo& fragment_info = *(fd.fragment_info_);
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int dim_num = array_schema.dim_num();

  // Check if the fragment is open in CREATE mode
  if(fragment_info.fragment_mode_ != CREATE)
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
  if(fragment_info.lastly_appended_tile_ids_[attribute_id] != 
     SM_INVALID_TILE_ID) {
    // The tile ids must follow a strictly ascending order
    if(fragment_info.lastly_appended_tile_ids_[attribute_id] >= 
       tile->tile_id()) {
      return false;
    } else {
    // The lastly inserted id of every other attribute must be either equal
    // to the currently inserted one, or equal to the last id of the attribute
    // of the currently inserted one.
    uint64_t last_tile_id = 
        fragment_info.lastly_appended_tile_ids_[attribute_id]; 
    for(unsigned int i=0; i<=attribute_num; i++)
      if(i != attribute_id && 
         fragment_info.lastly_appended_tile_ids_[i] != last_tile_id &&
         fragment_info.lastly_appended_tile_ids_[i] != tile->tile_id())
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

  fragment_info.lastly_appended_tile_ids_[attribute_id] = tile->tile_id();
  
  return true;
}

bool StorageManager::check_on_close_fragment(
    const FragmentDescriptor& fd) const {
  // Check descriptor
  if(!check_fragment_descriptor(fd))
    return false;

  // For easy reference
  const ArraySchema& array_schema = *(fd.fragment_info_->array_schema_);
  const FragmentInfo& fragment_info = *(fd.fragment_info_);
  unsigned int attribute_num = array_schema.attribute_num();

  // The fragment must either be empty, or all the lastly inserted tile ids must
  // be the same
  if(!fragment_empty(&fd)) {
    uint64_t last_tile_id = fragment_info.lastly_appended_tile_ids_[0];
    for(unsigned int i=1; i<=attribute_num; i++)
      if(fragment_info.lastly_appended_tile_ids_[i] != last_tile_id)
        return false;
  }

  return true;
}

bool StorageManager::check_on_get_tile(
    const FragmentDescriptor& fd,
    unsigned int attribute_id, 
    uint64_t tile_id) const {
  // Check descriptor
  if(!check_fragment_descriptor(fd))
    return false;

  // For easy reference
  const FragmentInfo& fragment_info = *(fd.fragment_info_);

  // Check if the fragment is opened in READ mode
  if(fragment_info.fragment_mode_ != READ)
    return false;

  // Check attribute id
  if(attribute_id > fragment_info.array_schema_->attribute_num())
    return false;

  // Check if tile id exists
  if(tile_rank(fragment_info, tile_id) == SM_INVALID_RANK)
    return false;

  return true;
}

bool StorageManager::check_on_get_tile_by_rank(
    const FragmentDescriptor& fd,
    unsigned int attribute_id, 
    uint64_t tile_rank) const {
  // Check descriptor
  if(!check_fragment_descriptor(fd))
    return false;

  // For easy reference
  const FragmentInfo& fragment_info = *(fd.fragment_info_);

  // Check if the fragment is opened in READ mode
  if(fragment_info.fragment_mode_ != READ)
    return false;

  // Check attribute id
  if(attribute_id > fragment_info.array_schema_->attribute_num())
    return false;

  // Check if tile id exists
  if(tile_rank >= fragment_info.tile_ids_.size())
    return false;

  return true;
}

bool StorageManager::check_on_open_fragment(const std::string& array_name, 
                                            const std::string& fragment_name,
                                            Mode mode) const {
  OpenFragments::const_iterator fragment_it = 
      open_fragments_.find(array_name + "_" + fragment_name);

  // The fragment must not be already open
  if(fragment_it != open_fragments_.end())
    return false;

  bool exists = fragment_exists(array_name, fragment_name);
  // If the fragment is opened in CREATE mode, 
  // the fragment must not exist
  if(mode == CREATE && exists)
    return false;
  // If the fragment is opened in READ mode, the fragment must exist
  if(mode == READ && !exists)
    return false;

  return true;
}

void StorageManager::create_array_directory(
    const std::string& array_name) const {
  std::string dir_name = workspace_ + "/" + array_name;

  int dir_flag = mkdir(dir_name.c_str(), S_IRWXU);
  assert(dir_flag == 0);
}

void StorageManager::create_fragment_directory(
    const std::string& array_name,
    const std::string& fragment_name) const {
  std::string dir_name = workspace_ + "/" + array_name + "/" +
                         fragment_name;

  int dir_flag = mkdir(dir_name.c_str(), S_IRWXU);
  assert(dir_flag == 0);
}

void StorageManager::create_workspace() {
  struct stat st;
  bool workspace_exists = 
      stat(workspace_.c_str(), &st) == 0 && S_ISDIR(st.st_mode);

  // If the workspace does not exist, create it
  if(!workspace_exists) { 
    int dir_flag = mkdir(workspace_.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }
}

void StorageManager::delete_fragment(
    const std::string& array_name,
    const std::string& fragment_name) {
  OpenFragments::iterator fragment_it = 
      open_fragments_.find(array_name + "_" + fragment_name);

  // If the array is open
  if(fragment_it != open_fragments_.end()) {
    delete_tiles(fragment_it->second);
    open_fragments_.erase(fragment_it); 
  }

  // Regardless of whether the array is open or not
  delete_fragment_directory(array_name, fragment_name);
}

void StorageManager::delete_fragment_directory(
    const std::string& array_name, 
    const std::string& fragment_name) const {
  std::string dirname = workspace_ + "/" + array_name + "/" + 
                        fragment_name + "/";
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

void StorageManager::delete_tiles(FragmentInfo& fragment_info) const {
  for(unsigned int i=0; i<fragment_info.tiles_.size(); i++)
    delete_tiles(fragment_info, i);
}

void StorageManager::delete_tiles(FragmentInfo& fragment_info,
                                  unsigned int attribute_id) const {
  // Delete the tiles
  TileList::const_iterator tile_it = 
      fragment_info.tiles_[attribute_id].begin(); 
  TileList::const_iterator tile_it_end = 
      fragment_info.tiles_[attribute_id].end(); 
  for(; tile_it != tile_it_end; tile_it++)
    delete *tile_it;

  // Update the indices
  fragment_info.tiles_[attribute_id].clear(); 
  fragment_info.payload_sizes_[attribute_id] = 0; 
}

void StorageManager::flush_fragment_info(
    FragmentInfo& fragment_info) const {
  if(fragment_info.fragment_mode_ == CREATE) {
    flush_tiles(fragment_info);  
    flush_bounding_coordinates(fragment_info);
    flush_MBRs(fragment_info);   
    flush_offsets(fragment_info);
    flush_tile_ids(fragment_info);
  } 
  
  delete_tiles(fragment_info); 
}

// FILE FORMAT:
// tile#1_lower_dim#1(double) tile#1_lower_dim#2(double) ... 
// tile#1_upper_dim#1(double) tile#1_upper_dim#2(double) ... 
// tile#2_lower_dim#1(double) tile#2_lower_dim#2(double) ... 
// tile#2_upper_dim#1(double) tile#2_upper_dim#2(double) ...
// ... 
void StorageManager::flush_bounding_coordinates(
    const FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  uint64_t tile_num = fragment_info.tile_ids_.size();

  // Prepare filename
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         fragment_name + "/" +
                         SM_BOUNDING_COORDINATES_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
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
             &fragment_info.bounding_coordinates_[i].first[0], 
             dim_num * sizeof(double));
      offset += dim_num * sizeof(double);
      // Upper bounding coordinates
      memcpy(buffer+offset, 
             &fragment_info.bounding_coordinates_[i].second[0], 
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
void StorageManager::flush_MBRs(
    const FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  uint64_t tile_num = fragment_info.tile_ids_.size();

  // prepare file name
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         fragment_name + "/" +
                         SM_MBRS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
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
      memcpy(buffer+offset, &fragment_info.mbrs_[i][0], 
             2 * dim_num * sizeof(double));
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
void StorageManager::flush_offsets(
    const FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  uint64_t tile_num = fragment_info.tile_ids_.size();

  // Prepare file name
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         fragment_name + "/" +
                         SM_OFFSETS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
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
             &fragment_info.offsets_[i][0], tile_num * sizeof(uint64_t));
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
void StorageManager::flush_tile_ids(
    const FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  uint64_t tile_num = fragment_info.tile_ids_.size();

  // Prepare file name
  std::string filename = workspace_ + "/" + array_name + "/" +
                         fragment_name + "/" +
                         SM_TILE_IDS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);
 
  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    uint64_t buffer_size = (tile_num+1) * sizeof(uint64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    memcpy(buffer, &tile_num, sizeof(uint64_t));
    memcpy(buffer+sizeof(uint64_t), 
           &fragment_info.tile_ids_[0], tile_num * sizeof(uint64_t));

    // Write buffer and clean up
    write(fd, buffer, buffer_size);  
    delete [] buffer;
  }

  close(fd);
}

void StorageManager::flush_tiles(
    FragmentInfo& fragment_info) const {
  for(unsigned int i=0; i<=fragment_info.array_schema_->attribute_num(); i++)
    flush_tiles(fragment_info, i);
}

void StorageManager::flush_tiles(
    FragmentInfo& fragment_info,
    unsigned int attribute_id) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  uint64_t segment_size = fragment_info.payload_sizes_[attribute_id];

  if(segment_size > 0) {
    // Open file
    std::string filename = workspace_ + "/" + 
                           array_schema.array_name() + "/" + 
                           fragment_info.fragment_name_ + "/" + 
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
    prepare_segment(fragment_info, attribute_id, file_offset,
                    segment_size, segment);
    write(fd, segment, segment_size);
  
    // Clean up 
    delete [] segment;
    close(fd);
  }
}

const Tile* StorageManager::get_tile_by_rank(
    FragmentInfo& fragment_info, 
    unsigned int attribute_id,
    uint64_t rank) const {
  // For easy reference
  const uint64_t& rank_low = fragment_info.rank_ranges_[attribute_id].first;
  const uint64_t& rank_high = fragment_info.rank_ranges_[attribute_id].second;

  // Fetch from the disk if the tile is not in main memory
  if(fragment_info.tiles_[attribute_id].size() == 0 ||
     (rank < rank_low || rank > rank_high)) 
    // The following updates rank_low and rank_high
    load_tiles_from_disk(fragment_info, attribute_id, rank);

  assert(rank >= rank_low && rank <= rank_high);
  assert(rank - rank_low <= fragment_info.tiles_[attribute_id].size());

  return fragment_info.tiles_[attribute_id][rank-rank_low];
}

inline
void StorageManager::init_fragment_info(
    const std::string& fragment_name,
    const ArraySchema* array_schema, 
    Mode mode, 
    FragmentInfo& fragment_info) const { 
  // For easy reference
  const unsigned int attribute_num = array_schema->attribute_num();

  fragment_info.fragment_mode_ = mode;
  fragment_info.array_schema_ = array_schema;
  fragment_info.id_ = fragment_info_id_++;
  fragment_info.fragment_name_ = fragment_name;
  fragment_info.offsets_.resize(attribute_num+1);
  fragment_info.payload_sizes_.resize(attribute_num+1);
  fragment_info.tiles_.resize(attribute_num+1);
  fragment_info.rank_ranges_.resize(attribute_num+1);
  fragment_info.lastly_appended_tile_ids_.resize(attribute_num+1);
  for(unsigned int i=0; i<=attribute_num; i++)
    fragment_info.lastly_appended_tile_ids_[i] = SM_INVALID_TILE_ID;
}

const ArraySchema *StorageManager::load_array_schema(
    const std::string& array_name) const {
  // The schema to be returned
  ArraySchema* array_schema = new ArraySchema();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;
  char* buffer = new char[buffer_size];
 
  // Load array schema
  read(fd, buffer, buffer_size);
  array_schema->deserialize(buffer, buffer_size);

  // Clean up
  close(fd);
  delete [] buffer;

  return array_schema;
}

// FILE FORMAT:
// tile#1_lower_dim#1(double) tile#1_lower_dim#2(double) ... 
// tile#1_upper_dim#1(double) tile#1_upper_dim#2(double) ... 
// tile#2_lower_dim#1(double) tile#2_lower_dim#2(double) ... 
// tile#2_upper_dim#1(double) tile#2_upper_dim#2(double) ...
// ... 
void StorageManager::load_bounding_coordinates(
    FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *fragment_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  uint64_t tile_num = fragment_info.tile_ids_.size();
  assert(tile_num != 0);
  unsigned int dim_num = array_schema.dim_num();
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         fragment_name + "/" +
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
  fragment_info.bounding_coordinates_.resize(tile_num);

  // Load bounding coordinates
  for(uint64_t i=0; i<tile_num; i++) {
    fragment_info.bounding_coordinates_[i].first.resize(dim_num);
    memcpy(&fragment_info.bounding_coordinates_[i].first[0], 
           buffer + offset, dim_num * sizeof(double));
    offset += dim_num * sizeof(double);
    fragment_info.bounding_coordinates_[i].second.resize(dim_num);
    memcpy(&fragment_info.bounding_coordinates_[i].second[0], 
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
void StorageManager::load_MBRs(FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *fragment_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  uint64_t tile_num = fragment_info.tile_ids_.size();
  unsigned int dim_num = array_schema.dim_num();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         fragment_name + "/" +
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
  fragment_info.mbrs_.resize(tile_num);

  // Load MBRs
  for(uint64_t i=0; i<tile_num; i++) {
    fragment_info.mbrs_[i].resize(2*dim_num);
    memcpy(&fragment_info.mbrs_[i][0], buffer + offset, 
           2*dim_num*sizeof(double));
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
void StorageManager::load_offsets(FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *fragment_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t tile_num = fragment_info.tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         fragment_name + "/" +
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
    fragment_info.offsets_[i].resize(tile_num);
    memcpy(&fragment_info.offsets_[i][0], buffer + offset, 
           tile_num*sizeof(uint64_t));
    offset += tile_num * sizeof(uint64_t);
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

inline
std::pair<uint64_t, uint64_t> StorageManager::load_payloads_into_buffer(
    const FragmentInfo& fragment_info, 
    unsigned int attribute_id, 
    uint64_t start_rank, char*& buffer) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  const std::string& attribute_name = 
      array_schema.attribute_name(attribute_id);
  const OffsetList& offsets = fragment_info.offsets_[attribute_id];
  const uint64_t tile_num = offsets.size();
  assert(tile_num == fragment_info.tile_ids_.size());

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         fragment_name + "/" +
                         attribute_name + 
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
void StorageManager::load_tile_ids(FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *fragment_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
 
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         fragment_name + "/" +
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
  fragment_info.tile_ids_.resize(tile_num);

  // Load tile_ids
  memcpy(&fragment_info.tile_ids_[0], 
         buffer+sizeof(uint64_t), tile_num*sizeof(uint64_t));

  // Clean up
  delete [] buffer;
  close(fd);
}

void StorageManager::load_tiles_from_disk(
    FragmentInfo& fragment_info, 
    unsigned int attribute_id,
    uint64_t start_rank) const { 
  char* buffer;

  // Load the tile payloads from the disk into a buffer
  std::pair<uint64_t, uint64_t> ret = load_payloads_into_buffer(
      fragment_info, attribute_id, start_rank, buffer); 

  // For easy reference
  uint64_t buffer_size = ret.first;
  uint64_t tiles_in_buffer = ret.second;

  // Delete previous tiles from main memory
  delete_tiles(fragment_info, attribute_id);
  
  // Create the tiles from the payloads in the buffer and load them
  // into the tile book-keeping structure
  load_tiles_from_buffer(fragment_info, 
                         attribute_id, start_rank, 
                         buffer, buffer_size, tiles_in_buffer);         
 
  // Update rank range in main memory
  fragment_info.rank_ranges_[attribute_id].first = start_rank;
  fragment_info.rank_ranges_[attribute_id].second = 
      start_rank + tiles_in_buffer - 1;

  // Clean up
  delete [] buffer;
}

void StorageManager::load_tiles_from_buffer(
    FragmentInfo& fragment_info, 
    unsigned int attribute_id, uint64_t start_rank, char* buffer, 
    uint64_t buffer_size, uint64_t tiles_in_buffer) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const OffsetList& offsets = fragment_info.offsets_[attribute_id];
  const TileIds& tile_ids = fragment_info.tile_ids_;
  const MBRs& mbrs = fragment_info.mbrs_;
  const BoundingCoordinates& bounding_coordinates = 
      fragment_info.bounding_coordinates_;
  assert(offsets.size() == tile_ids.size());
  TileList& tiles = fragment_info.tiles_[attribute_id];
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
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

inline
void StorageManager::prepare_segment( 
    FragmentInfo& fragment_info, unsigned int attribute_id,
    uint64_t file_offset, uint64_t segment_size, 
    char* segment) const {
  uint64_t segment_offset = 0;
  uint64_t tile_size;
  TileList::const_iterator tile_it = 
      fragment_info.tiles_[attribute_id].begin();
  TileList::const_iterator tile_it_end = 
      fragment_info.tiles_[attribute_id].end();

  for(; tile_it != tile_it_end; tile_it++) {
    tile_size = (*tile_it)->tile_size();
    assert(tile_size + segment_offset <= segment_size);
    (*tile_it)->copy_payload(segment+segment_offset);
    fragment_info.offsets_[attribute_id].push_back(
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

uint64_t StorageManager::tile_rank(const FragmentInfo& fragment_info, 
                                   uint64_t tile_id) const {
  // Check of the array has not tiles
  if(fragment_info.tile_ids_.size() == 0)
    return SM_INVALID_RANK;

  // Perform binary search
  uint64_t min = 0;
  uint64_t max = fragment_info.tile_ids_.size()-1;
  uint64_t mid;
  while(min <= max) {
    mid = min + ((max - min) / 2);
    if(fragment_info.tile_ids_[mid] == tile_id)  // Tile found!
      return mid;
    else if(fragment_info.tile_ids_[mid] > tile_id) {
      if(mid == 0)
        return SM_INVALID_RANK;
      max = mid - 1;
    }
    else // (fragment_info.tile_ids_[mid] < tile_id)
      min = mid + 1;
  }

  // Tile id not found
  return SM_INVALID_RANK;
}

void StorageManager::expand_mbr(
    const std::vector<double>& coord, MBR& mbr) const {
  assert(mbr.size() == 0 || mbr.size() == 2 * coord.size());

  // For easy reference
  unsigned int dim_num = coord.size();

  if(mbr.size() == 0) { // Initialization 
    mbr.resize(2*dim_num);
    for(unsigned int i=0; i<dim_num; ++i) {
      mbr[2*i] = coord[i]; 
      mbr[2*i+1] = coord[i]; 
    }
  } else { // Expansion
    for(unsigned int i=0; i<dim_num; ++i) {
      // Update lower bound on dimension i
      if(mbr[2*i] > coord[i])
        mbr[2*i] = coord[i];

      // Update upper bound on dimension i
      if(mbr[2*i+1] < coord[i])
        mbr[2*i+1] = coord[i];   
    }	
  }
}
