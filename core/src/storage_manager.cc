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
#include "utils.h"
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

int64_t StorageManager::fragment_info_id_ = 0;

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

StorageManager::StorageManager(const std::string& path, size_t segment_size) 
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
  std::string fragments_filename = std::string(SM_FRAGMENTS_FILENAME) + 
                                   SM_BOOK_KEEPING_FILE_SUFFIX;
  std::string array_schema_filename = std::string(SM_ARRAY_SCHEMA_FILENAME) + 
                                      SM_BOOK_KEEPING_FILE_SUFFIX;
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0 ||
       strcmp(next_file->d_name, array_schema_filename.c_str()) == 0)
      continue;
    if(strcmp(next_file->d_name, fragments_filename.c_str()) == 0)  {
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

void StorageManager::close_fragment(const FragmentDescriptor* fd) {
  assert(check_on_close_fragment(fd)); 
 
  // For easy reference
  FragmentInfo& fragment_info = *(fd->fragment_info_);
  const std::string& array_name = fd->array_name_;
  const std::string& fragment_name = fragment_info.fragment_name_;
  
  flush_fragment_info(fragment_info); 
  open_fragments_.erase(array_name + "_" + fragment_name); 

  delete fd;
}

void StorageManager::define_array(const ArraySchema* array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema->array_name();

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
  std::pair<const char*, size_t> ret = array_schema->serialize();
  const char* buffer = ret.first;
  size_t buffer_size = ret.second; 

  // Store the array schema
  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);
}

void StorageManager::delete_array(const std::string& array_name) {
  std::string dirname = workspace_ + "/" + array_name + "/";
  std::string filename; 
  std::string fragments_filename = std::string(SM_FRAGMENTS_FILENAME) + 
                                   SM_BOOK_KEEPING_FILE_SUFFIX;
  std::string array_schema_filename = std::string(SM_ARRAY_SCHEMA_FILENAME) + 
                                      SM_BOOK_KEEPING_FILE_SUFFIX;

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0)
      continue;
    if(strcmp(next_file->d_name, fragments_filename.c_str()) == 0 ||
       strcmp(next_file->d_name, array_schema_filename.c_str()) == 0) {
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
   const char* buffer, size_t buffer_size) const { 
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
  assert(check_fragment_descriptor(fd));

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
                                        size_t& buffer_size) const {
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
    const ArraySchema* array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema->array_name();


  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Serialize array schema
  std::pair<const char*, size_t> ret = array_schema->serialize();
  const char* buffer = ret.first;
  size_t buffer_size = ret.second; 

  // Store the array schema
  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);
}

void StorageManager::modify_fragment_bkp(
    const FragmentDescriptor* fd, int64_t capacity) const {
  assert(capacity > 0);

  // For easy reference
  const ArraySchema* array_schema = fd->fragment_info_->array_schema_;
  const std::string& array_name = array_schema->array_name();
  const std::string& fragment_name = fd->fragment_info_->fragment_name_;
  int attribute_num = array_schema->attribute_num();
  int64_t old_capacity = array_schema->capacity();
  size_t coords_cell_size = array_schema->cell_size(attribute_num);

  // New book-keeping structures
  FragmentInfo fragment_info;
  fragment_info.array_schema_ = array_schema;
  fragment_info.fragment_name_ = fragment_name;
  BoundingCoordinates& new_bounding_coordinates = 
      fragment_info.bounding_coordinates_;
  MBRs& new_mbrs = fragment_info.mbrs_;
  Offsets& new_offsets = fragment_info.offsets_;
  TileIds& new_tile_ids = fragment_info.tile_ids_;

  // Reserve proper space for the new book-keeping structures
  int64_t old_tile_num = fd->fragment_info_->tile_ids_.size();
  int64_t expected_new_tile_num = 
      ceil(double(old_tile_num) * old_capacity / capacity);
  new_bounding_coordinates.reserve(expected_new_tile_num);
  new_mbrs.reserve(expected_new_tile_num);
  new_offsets.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    new_offsets[i].reserve(expected_new_tile_num);
  new_tile_ids.reserve(expected_new_tile_num);

  // Compute new book-keeping info
  int64_t new_tile_id = 0;
  int64_t cell_num = 0;
  const_tile_iterator tile_it = begin(fd, attribute_num); 
  const_tile_iterator tile_it_end = end(fd, attribute_num);
  Tile::const_cell_iterator cell_it, cell_it_end;
  BoundingCoordinatesPair bounding_coordinates;
  void* mbr = NULL;
  const void* coords;
  for(; tile_it != tile_it_end; ++tile_it) {
    cell_it = (*tile_it)->begin();
    cell_it_end = (*tile_it)->end();
    for(; cell_it != cell_it_end; ++cell_it) {
      coords = *cell_it;
      if(cell_num % capacity == 0) { // New tile
        if(cell_num != 0) { 
          new_mbrs.push_back(mbr); 
          new_bounding_coordinates.push_back(bounding_coordinates);
          new_tile_ids.push_back(new_tile_id++);
          for(int i=0; i<=attribute_num; ++i) {
            new_offsets[i].push_back((cell_num - capacity) * 
                                     array_schema->cell_size(i)); 
          }
          mbr = NULL;
        }
        bounding_coordinates.first = malloc(coords_cell_size);
        bounding_coordinates.second = malloc(coords_cell_size);
        memcpy(bounding_coordinates.first, coords, coords_cell_size);
      }
      expand_mbr(array_schema, coords, mbr);
      memcpy(bounding_coordinates.second, coords, coords_cell_size);
      ++cell_num;
    }
  }

  // Take into account the book-keeping info of the last new tile
  if(cell_num % capacity != 0) {
    new_mbrs.push_back(mbr); 
    new_bounding_coordinates.push_back(bounding_coordinates);
    new_tile_ids.push_back(new_tile_id++);
    for(int i=0; i<=attribute_num; ++i) {
      new_offsets[i].push_back((cell_num - (cell_num % capacity)) * 
                               array_schema->cell_size(i));  
    } 
  }

  // Flush the new book-keeping structures to disk
  flush_bounding_coordinates(fragment_info);
  flush_MBRs(fragment_info);   
  flush_offsets(fragment_info);
  flush_tile_ids(fragment_info);
}

const StorageManager::ArrayDescriptor* StorageManager::open_array(
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

const StorageManager::ArrayDescriptor* StorageManager::open_array(
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    Mode mode) {

  std::vector<const FragmentDescriptor*> fd;
  fd.reserve(fragment_names.size());

  // Open fragments
  for(int i=0; i<fragment_names.size(); ++i)
    fd.push_back(open_fragment(array_schema, fragment_names[i], mode));

  return new ArrayDescriptor(array_schema, fd); 
}

const StorageManager::FragmentDescriptor* StorageManager::open_fragment(
    const ArraySchema* array_schema, 
    const std::string& fragment_name,
    Mode mode) {
  // For easy reference
  const std::string& array_name = array_schema->array_name();

  assert(check_on_open_fragment(array_name, fragment_name, mode));

  // Create a fragment info entry in open_fragments_
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
                                 int attribute_id) {
  // If tile is empty, delete it and exit
  if(tile->cell_num() == 0) {
    delete tile;
    return;
  }

  assert(check_on_append_tile(fd, attribute_id, tile));

  // For easy reference
  FragmentInfo& fragment_info = *(fd->fragment_info_);
  size_t cell_size = tile->cell_size();

  // Update indices
  // For both attribute and coordinate tiles
  if(fragment_empty(fd) || 
     fragment_info.tile_ids_.back() != tile->tile_id())
    fragment_info.tile_ids_.push_back(tile->tile_id());
  fragment_info.tiles_[attribute_id].push_back(tile);
  fragment_info.payload_sizes_[attribute_id] += tile->tile_size();
  // Only for coordinate tiles
  if(tile->tile_type() == Tile::COORDINATE) {
    // Append MBR
    void* mbr = malloc(2*cell_size);
    memcpy(mbr, tile->mbr(), 2*cell_size);
    fragment_info.mbrs_.push_back(mbr);

    // Append bounding coordinates
    std::pair<const void*, const void*> tile_b_coords = 
        tile->bounding_coordinates();
    std::pair<void*, void*> b_coords;
    b_coords.first = malloc(cell_size);
    memcpy(b_coords.first, tile_b_coords.first, cell_size);
    b_coords.second = malloc(cell_size);
    memcpy(b_coords.second, tile_b_coords.second, cell_size);
    fragment_info.bounding_coordinates_.push_back(b_coords);
  }

  // Flush tiles to disk if the sum of payloads exceeds the segment size
  if(fragment_info.payload_sizes_[attribute_id] >= segment_size_) { 
    flush_tiles(fragment_info, attribute_id);
    delete_tiles(fragment_info, attribute_id);
  }
} 

const Tile* StorageManager::get_tile( 
    const FragmentDescriptor* fd, int attribute_id, int64_t tile_id) {
  assert(check_on_get_tile(fd, attribute_id, tile_id));

  // For easy reference
  FragmentInfo& fragment_info = *(fd->fragment_info_);

  int64_t pos = tile_pos(fragment_info, tile_id); 

  return get_tile_by_pos(fragment_info, attribute_id, pos);
}

const Tile* StorageManager::get_tile_by_pos( 
    const FragmentDescriptor* fd, int attribute_id, int64_t pos) {
  assert(check_on_get_tile_by_pos(fd, attribute_id, pos));

  // For easy reference
  FragmentInfo& fragment_info = *(fd->fragment_info_);

  return get_tile_by_pos(fragment_info, attribute_id, pos);
}

Tile* StorageManager::new_tile(
    const ArraySchema* array_schema, int attribute_id, 
    int64_t tile_id, int64_t capacity) const {
  // For easy reference
  int attribute_num = array_schema->attribute_num();
  assert(attribute_id >=0 && attribute_id <= attribute_num);
  int dim_num = (attribute_id < attribute_num) ? 0 : array_schema->dim_num();
  const std::type_info* cell_type = array_schema->type(attribute_id);

  return new Tile(tile_id, dim_num, cell_type, capacity);
}

int64_t StorageManager::get_tile_id( 
    const FragmentDescriptor* fd, int64_t pos) const {
  assert(pos >= 0 && pos < fd->fragment_info_->tile_ids_.size());
  return fd->fragment_info_->tile_ids_[pos];
}

/******************************************************
******************** TILE ITERATORS *******************
******************************************************/

StorageManager::const_tile_iterator::const_tile_iterator() 
    : storage_manager_(NULL), fd_(NULL) {
}

StorageManager::const_tile_iterator::const_tile_iterator(
    const StorageManager* storage_manager,
    const FragmentDescriptor* fd,
    int attribute_id,
    int64_t pos)
    : storage_manager_(storage_manager), fd_(fd),
      attribute_id_(attribute_id), pos_(pos) {
}

void StorageManager::const_tile_iterator::operator=(
    const StorageManager::const_tile_iterator& rhs) {
  fd_ = rhs.fd_;
  attribute_id_ = rhs.attribute_id_;
  storage_manager_ = rhs.storage_manager_;
  pos_ = rhs.pos_;
}

StorageManager::const_tile_iterator 
StorageManager::const_tile_iterator::operator+(int64_t step) {
  const_tile_iterator it = *this;
  it.pos_ += step;
  return it;
}

void StorageManager::const_tile_iterator::operator+=(int64_t step) {
  pos_ += step;
}

StorageManager::const_tile_iterator 
StorageManager::const_tile_iterator::operator++() {
  ++pos_;
  return *this;
}

StorageManager::const_tile_iterator 
StorageManager::const_tile_iterator::operator++(
    int junk) {
  const_tile_iterator it = *this;
  ++pos_;
  return it;
}

bool StorageManager::const_tile_iterator::operator==(
    const StorageManager::const_tile_iterator& rhs) const {
  return (pos_ == rhs.pos_ && attribute_id_ == rhs.attribute_id_ &&
          fd_ == rhs.fd_ && storage_manager_ == rhs.storage_manager_); 
}

bool StorageManager::const_tile_iterator::operator!=(
    const StorageManager::const_tile_iterator& rhs) const {
  return (!(pos_ == rhs.pos_ && attribute_id_ == rhs.attribute_id_ &&
          fd_ == rhs.fd_ && storage_manager_ == rhs.storage_manager_)); 
}

const Tile* StorageManager::const_tile_iterator::operator*() const {
  assert(pos_ < fd_->fragment_info_->tile_ids_.size());
  assert(storage_manager_->check_on_get_tile( 
             fd_, attribute_id_, fd_->fragment_info_->tile_ids_[pos_]));
  // For easy reference
  FragmentInfo& fragment_info = *(fd_->fragment_info_);

  return storage_manager_->get_tile_by_pos(fragment_info,
                                           attribute_id_, pos_);
}

const ArraySchema* StorageManager::const_tile_iterator::array_schema() const {
  return fd_->fragment_info_->array_schema_;
}

StorageManager::BoundingCoordinatesPair 
StorageManager::const_tile_iterator::bounding_coordinates() const {
  return fd_->fragment_info()->bounding_coordinates_[pos_];
}

StorageManager::MBR StorageManager::const_tile_iterator::mbr() const {
  return fd_->fragment_info()->mbrs_[pos_];
}

int64_t StorageManager::const_tile_iterator::tile_id() const {
  return fd_->fragment_info()->tile_ids_[pos_];
}

StorageManager::const_tile_iterator StorageManager::begin(
    const FragmentDescriptor* fd,
    int attribute_id) const {
  // Check array descriptor
  assert(check_fragment_descriptor(fd)); 

  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // The fragment should be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  // Check attribute id
  assert(attribute_id <= fd->fragment_info_->array_schema_->attribute_num());

  return const_tile_iterator(this, fd, attribute_id, 0);
}

StorageManager::const_tile_iterator StorageManager::end(
    const FragmentDescriptor* fd, 
    int attribute_id) const {
  // Check array descriptor
  assert(check_fragment_descriptor(fd));
 
  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);
  int64_t tile_num = fragment_info.tile_ids_.size();

  // The fragment should be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  // Check attribute id
  assert(attribute_id <= fd->fragment_info_->array_schema_->attribute_num());

  return const_tile_iterator(this, fd, attribute_id, tile_num);
}

StorageManager::MBRs::const_iterator StorageManager::MBR_begin(
    const FragmentDescriptor* fd) const {
  // Check array descriptor
  assert(check_fragment_descriptor(fd)); 

  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // The fragment must be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  return fragment_info.mbrs_.begin(); 
}

StorageManager::MBRs::const_iterator StorageManager::MBR_end(
    const FragmentDescriptor* fd) const {
  // Check fragment descriptor
  assert(check_fragment_descriptor(fd)); 
  
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
    const void* range,
    std::vector<std::pair<int64_t, bool> >* overlapping_tile_ids) const {
  // For easy reference
  const ArraySchema* array_schema = fd->array_schema();
  int attribute_num = array_schema->attribute_num(); 
  const std::type_info* type = array_schema->type(attribute_num);

  if(*type == typeid(int))
    get_overlapping_tile_ids(fd, static_cast<const int*>(range), 
                        overlapping_tile_ids);
  else if(*type == typeid(int64_t))
    get_overlapping_tile_ids(fd, static_cast<const int64_t*>(range), 
                        overlapping_tile_ids);
  else if(*type == typeid(float))
    get_overlapping_tile_ids(fd, static_cast<const float*>(range), 
                        overlapping_tile_ids);
  else if(*type == typeid(double))
    get_overlapping_tile_ids(fd, static_cast<const double*>(range), 
                        overlapping_tile_ids);
}

template<class T>
void StorageManager::get_overlapping_tile_ids(
    const FragmentDescriptor* fd, 
    const T* range,
    std::vector<std::pair<int64_t, bool> >* overlapping_tile_ids) const {
  // Check array descriptor
  assert(check_fragment_descriptor(fd)); 

  // The fragment must be open in READ mode
  assert(fd->fragment_info_->fragment_mode_ == READ);
 
  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);
  int dim_num = fd->array_schema()->dim_num();
  int64_t tile_num = fragment_info.tile_ids_.size();

  assert(fragment_info.mbrs_.size() == tile_num);

  // Overlap type per dimension
  bool* partial = new bool[dim_num];
  bool* full = new bool[dim_num];

  bool overlap; // True if an MBR overlaps (partially or fully) the range
  std::pair<int64_t, bool> overlapping_tile_id; // (tile_id, full)
  
  for(int64_t i=0; i<tile_num; ++i) {
    const T* mbr = static_cast<const T*>(fragment_info.mbrs_[i]);
    overlap = true;
    overlapping_tile_id.second = true;

    // Determine overlap per dimension
    for(int j=0; j<dim_num; ++j) {
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

void StorageManager::get_overlapping_tile_pos(
    const FragmentDescriptor* fd, 
    const void* range,
    std::vector<std::pair<int64_t, bool> >* overlapping_tile_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fd->array_schema();
  int attribute_num = array_schema->attribute_num(); 
  const std::type_info* type = array_schema->type(attribute_num);

  if(*type == typeid(int))
    get_overlapping_tile_pos(fd, static_cast<const int*>(range), 
                        overlapping_tile_pos);
  else if(*type == typeid(int64_t))
    get_overlapping_tile_pos(fd, static_cast<const int64_t*>(range), 
                        overlapping_tile_pos);
  else if(*type == typeid(float))
    get_overlapping_tile_pos(fd, static_cast<const float*>(range), 
                        overlapping_tile_pos);
  else if(*type == typeid(double))
    get_overlapping_tile_pos(fd, static_cast<const double*>(range), 
                        overlapping_tile_pos);
}

template<class T>
void StorageManager::get_overlapping_tile_pos(
    const FragmentDescriptor* fd, 
    const T* range,
    std::vector<std::pair<int64_t, bool> >* overlapping_tile_pos) const {
  // Check array descriptor
  assert(check_fragment_descriptor(fd)); 

  // The fragment must be open in READ mode
  assert(fd->fragment_info_->fragment_mode_ == READ);
 
  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);
  int dim_num = fd->array_schema()->dim_num();
  int64_t tile_num = fragment_info.tile_ids_.size();

  assert(fragment_info.mbrs_.size() == tile_num);

  // Overlap type per dimension
  bool* partial = new bool[dim_num];
  bool* full = new bool[dim_num];

  bool overlap; // True if an MBR overlaps (partially or fully) the range
  std::pair<int64_t, bool> overlapping_tile_pos_pair; // (tile_rank, full)
  
  for(int64_t i=0; i<tile_num; ++i) {
    const T* mbr = static_cast<const T*>(fragment_info.mbrs_[i]);
    overlap = true;
    overlapping_tile_pos_pair.second = true;

    // Determine overlap per dimension
    for(int j=0; j<dim_num; ++j) {
      partial[j] = false;
      full[j] = false;
      if(mbr[2*j] >= range[2*j] && mbr[2*j+1] <= range[2*j+1])
        full[j] = true;
      else if(range[2*j] >= mbr[2*j] && range[2*j] <= mbr[2*j+1] || 
              range[2*j+1] >= mbr[2*j] && range[2*j+1] <= mbr[2*j+1])
        partial[j] = true;
    }

    // Determine MBR overlap
    for(int j=0; j<dim_num; ++j) {
      if(!partial[j] && !full[j]) {
        overlap = false;
        break;
      }
  
      if(partial[j])
        overlapping_tile_pos_pair.second = false;
    }

    // Insert overlapping tile rank into the result list
    if(overlap) {
      overlapping_tile_pos_pair.first = i;
      overlapping_tile_pos->push_back(overlapping_tile_pos_pair);
    }
  }

  delete [] partial;
  delete [] full;
}

/******************************************************
***************** PRIVATE FUNCTIONS *******************
******************************************************/

bool StorageManager::check_fragment_descriptor(
    const FragmentDescriptor* fd) const {
  // Check if the fragment is open
  OpenFragments::const_iterator fragment_it = 
      open_fragments_.find(fd->array_name_ + "_" + fd->fragment_name_);
  if(fragment_it == open_fragments_.end())
    return false;

  // Check if the descriptor is obsolete
  if(fd->fragment_info_id_ != fragment_it->second.id_)
    return false;

  return true;
}

template<class T>
bool StorageManager::check_mbr_on_append_tile(
    const FragmentDescriptor* fd, const Tile* tile) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd->fragment_info_->array_schema_);
  int dim_num = array_schema.dim_num();
  const T* mbr = static_cast<const T*>(tile->mbr());
  const std::vector<std::pair<double, double> >& dim_domains = 
      array_schema.dim_domains();

  for(int i=0; i<dim_num; ++i) {
    if(mbr[2*i] < dim_domains[i].first || mbr[2*i] > dim_domains[i].second ||
       mbr[2*i+1] < dim_domains[i].first || 
       mbr[2*i+1] > dim_domains[i].second)
      return false;
  }

  return true;
}

bool StorageManager::check_on_append_tile(
    const FragmentDescriptor* fd, int attribute_id, const Tile* tile) const {
  // Check descriptor
  if(!check_fragment_descriptor(fd))
    return false;

  // For easy reference
  const ArraySchema& array_schema = *(fd->fragment_info_->array_schema_);
  FragmentInfo& fragment_info = *(fd->fragment_info_);
  int attribute_num = array_schema.attribute_num();
  int dim_num = array_schema.dim_num();

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
    int64_t last_tile_id = 
        fragment_info.lastly_appended_tile_ids_[attribute_id]; 
    for(int i=0; i<=attribute_num; ++i)
      if(i != attribute_id && 
         fragment_info.lastly_appended_tile_ids_[i] != last_tile_id &&
         fragment_info.lastly_appended_tile_ids_[i] != tile->tile_id())
        return false;
    }
  }

  // Check MBR to verify that the coordinates are inside the domain
  bool mbr_check = true;
  if(tile->tile_type() == Tile::COORDINATE) {
    const std::type_info* type = array_schema.type(attribute_num);  
    if(*type == typeid(int))
      mbr_check = check_mbr_on_append_tile<int>(fd, tile);
    else if(*type == typeid(int64_t))
      mbr_check = check_mbr_on_append_tile<int64_t>(fd, tile);
    else if(*type == typeid(float))
      mbr_check = check_mbr_on_append_tile<float>(fd, tile);
    else if(*type == typeid(double))
      mbr_check = check_mbr_on_append_tile<double>(fd, tile); 
  }


  if(!mbr_check) {
    return false;
  } else {
    fragment_info.lastly_appended_tile_ids_[attribute_id] = tile->tile_id();
    return true;
  }
}

bool StorageManager::check_on_close_fragment(
    const FragmentDescriptor* fd) const {
  // Check descriptor
  if(!check_fragment_descriptor(fd))
    return false;

  // For easy reference
  const ArraySchema& array_schema = *(fd->fragment_info_->array_schema_);
  const FragmentInfo& fragment_info = *(fd->fragment_info_);
  int attribute_num = array_schema.attribute_num();

  // The fragment must either be empty, or all the lastly inserted tile ids must
  // be the same
  if(!fragment_empty(fd)) {
    int64_t last_tile_id = fragment_info.lastly_appended_tile_ids_[0];
    for(int i=1; i<=attribute_num; ++i)
      if(fragment_info.lastly_appended_tile_ids_[i] != last_tile_id)
        return false;
  }

  return true;
}

bool StorageManager::check_on_get_tile(
    const FragmentDescriptor* fd, int attribute_id, int64_t tile_id) const {
  // Check descriptor
  if(!check_fragment_descriptor(fd))
    return false;

  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // Check if the fragment is opened in READ mode
  if(fragment_info.fragment_mode_ != READ)
    return false;

  // Check attribute id
  if(attribute_id > fragment_info.array_schema_->attribute_num())
    return false;

  // Check if tile id exists
  if(tile_pos(fragment_info, tile_id) == SM_INVALID_POS)
    return false;

  return true;
}

bool StorageManager::check_on_get_tile_by_pos(
    const FragmentDescriptor* fd, int attribute_id, int64_t pos) const {
  // Check descriptor
  if(!check_fragment_descriptor(fd))
    return false;

  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // Check if the fragment is opened in READ mode
  if(fragment_info.fragment_mode_ != READ)
    return false;

  // Check attribute id
  if(attribute_id > fragment_info.array_schema_->attribute_num())
    return false;

  // Check if tile id exists
  if(pos < 0 || pos >= fragment_info.tile_ids_.size())
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
  FragmentInfo& fragment_info = fragment_it->second;

  // If the array is open
  if(fragment_it != open_fragments_.end()) {
    delete_tiles(fragment_info);

    int64_t tile_num = fragment_info.mbrs_.size();
    for(int i=0; i<tile_num; ++i) {
      free(fragment_info.bounding_coordinates_[i].first);
      free(fragment_info.bounding_coordinates_[i].second);
      free(fragment_info.mbrs_[i]);
    }

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
  for(int i=0; i<fragment_info.tiles_.size(); ++i)
    delete_tiles(fragment_info, i);
}

void StorageManager::delete_tiles(
    FragmentInfo& fragment_info, int attribute_id) const {
  // Delete the tiles
  TileList::const_iterator tile_it = 
      fragment_info.tiles_[attribute_id].begin(); 
  TileList::const_iterator tile_it_end = 
      fragment_info.tiles_[attribute_id].end(); 
  for(; tile_it != tile_it_end; tile_it++)
    delete *tile_it;

  // Delete segment
  delete [] static_cast<char*>(fragment_info.segments_[attribute_id]);

  // Update the indices
  fragment_info.tiles_[attribute_id].clear(); 
  fragment_info.payload_sizes_[attribute_id] = 0; 
  fragment_info.segments_[attribute_id] = NULL;
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
// tile#1_lower_dim#1(T) tile#1_lower_dim#2(T) ... 
// tile#1_upper_dim#1(T) tile#1_upper_dim#2(T) ... 
// tile#2_lower_dim#1(T) tile#2_lower_dim#2(T) ... 
// tile#2_upper_dim#1(T) tile#2_upper_dim#2(T) ...
// ... 
// NOTE: T is the type of the dimensions of this array
void StorageManager::flush_bounding_coordinates(
    const FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  int attribute_num = array_schema.attribute_num();
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  int64_t tile_num = fragment_info.tile_ids_.size();
  size_t cell_size = array_schema.cell_size(attribute_num);

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
    size_t buffer_size = 2 * tile_num * cell_size;
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    size_t offset = 0;
    for(int64_t i=0; i<tile_num; ++i) {
      // Lower bounding coordinates
      memcpy(buffer+offset, 
             fragment_info.bounding_coordinates_[i].first, cell_size);
      offset += cell_size;
      // Upper bounding coordinates
      memcpy(buffer+offset, 
             fragment_info.bounding_coordinates_[i].second, cell_size);
      offset += cell_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }  

  close(fd);
}

// FILE FORMAT:
// MBR#1_dim#1_low(T) MBR#1_dim#1_high(T) ...
// MBR#1_dim#2_low(T) MBR#1_dim#2_high(T) ...
// ...
// MBR#2_dim#1_low(T) MBR#2_dim#1_high(T) ...
// ...
// NOTE: T is the type of the dimensions of this array
void StorageManager::flush_MBRs(
    const FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  int attribute_num = array_schema.attribute_num();
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  int64_t tile_num = fragment_info.tile_ids_.size();
  size_t cell_size = array_schema.cell_size(attribute_num);

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
    size_t buffer_size = tile_num * 2 * cell_size;
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    size_t offset = 0;
    for(int64_t i=0; i<tile_num; ++i) {
      memcpy(buffer+offset, fragment_info.mbrs_[i], 2 * cell_size);
      offset += 2 * cell_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }

  close(fd);
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(int64_t)
// tile#2_of_attribute#1_offset(int64_t)
// ...
// tile#1_of_attribute#2_offset(int64_t)
// tile#2_of_attribute#2_offset(int64_t)
// ...
// NOTE: Do not forget the extra coordinate attribute
void StorageManager::flush_offsets(
    const FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  int64_t tile_num = fragment_info.tile_ids_.size();

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
    int attribute_num = array_schema.attribute_num();
    size_t buffer_size = (attribute_num+1) * tile_num * sizeof(int64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    int64_t offset = 0;
    for(int i=0; i<=attribute_num; ++i) {
      memcpy(buffer+offset, 
             &fragment_info.offsets_[i][0], tile_num * sizeof(int64_t));
      offset += tile_num * sizeof(int64_t);
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }
  
  close(fd);
}

// FILE FORMAT:
// tile_num(int64_t)
//   tile_id#1(int64_t) tile_id#2(int64_t)  ...
void StorageManager::flush_tile_ids(
    const FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  int64_t tile_num = fragment_info.tile_ids_.size();

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
    size_t buffer_size = (tile_num+1) * sizeof(int64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    memcpy(buffer, &tile_num, sizeof(int64_t));
    memcpy(buffer+sizeof(int64_t), 
           &fragment_info.tile_ids_[0], tile_num * sizeof(int64_t));

    // Write buffer and clean up
    write(fd, buffer, buffer_size);  
    delete [] buffer;
  }

  close(fd);
}

void StorageManager::flush_tiles(FragmentInfo& fragment_info) const {
  for(int i=0; i<=fragment_info.array_schema_->attribute_num(); ++i)
    flush_tiles(fragment_info, i);
}

void StorageManager::flush_tiles(
    FragmentInfo& fragment_info, int attribute_id) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  size_t segment_size = fragment_info.payload_sizes_[attribute_id];

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
    int64_t file_offset = st.st_size;

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

const Tile* StorageManager::get_tile_by_pos(
    FragmentInfo& fragment_info, int attribute_id, int64_t pos) const {
  // For easy reference
  const int64_t& pos_lower = fragment_info.pos_ranges_[attribute_id].first;
  const int64_t& pos_upper = fragment_info.pos_ranges_[attribute_id].second;

  // Fetch from the disk if the tile is not in main memory
  if(fragment_info.tiles_[attribute_id].size() == 0 ||
     (pos < pos_lower || pos > pos_upper)) 
    // The following updates pos_lower and pos_upper
    load_tiles_from_disk(fragment_info, attribute_id, pos);

  assert(pos >= pos_lower && pos <= pos_upper);
  assert(pos - pos_lower <= fragment_info.tiles_[attribute_id].size());

fragment_info.tiles_[attribute_id][pos-pos_lower]->print();

  return fragment_info.tiles_[attribute_id][pos-pos_lower];
}

inline
void StorageManager::init_fragment_info(
    const std::string& fragment_name, const ArraySchema* array_schema, 
    Mode mode, FragmentInfo& fragment_info) const { 
  // For easy reference
  int attribute_num = array_schema->attribute_num();

  fragment_info.fragment_mode_ = mode;
  fragment_info.array_schema_ = array_schema;
  fragment_info.id_ = fragment_info_id_++;
  fragment_info.fragment_name_ = fragment_name;
  fragment_info.offsets_.resize(attribute_num+1);
  fragment_info.payload_sizes_.resize(attribute_num+1);
  fragment_info.tiles_.resize(attribute_num+1);
  fragment_info.pos_ranges_.resize(attribute_num+1);
  fragment_info.segments_.resize(attribute_num+1);
  fragment_info.lastly_appended_tile_ids_.resize(attribute_num+1);

  for(int i=0; i<=attribute_num; ++i) {
    fragment_info.segments_[i] = NULL;
    fragment_info.lastly_appended_tile_ids_[i] = SM_INVALID_TILE_ID;
  }
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
  size_t buffer_size = st.st_size;
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
// tile#1_lower_dim#1(T) tile#1_lower_dim#2(T) ... 
// tile#1_upper_dim#1(T) tile#1_upper_dim#2(T) ... 
// tile#2_lower_dim#1(T) tile#2_lower_dim#2(T) ... 
// tile#2_upper_dim#1(T) tile#2_upper_dim#2(T) ...
// ... 
// NOTE: T is the type of the dimensions of this array
void StorageManager::load_bounding_coordinates(
    FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *fragment_info.array_schema_;
  int attribute_num = array_schema.attribute_num();
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  int64_t tile_num = fragment_info.tile_ids_.size();
  assert(tile_num != 0);
  size_t cell_size = array_schema.cell_size(attribute_num);
 
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
  size_t buffer_size = st.st_size;
  assert(buffer_size == tile_num * 2 * cell_size);
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t offset = 0;
  fragment_info.bounding_coordinates_.resize(tile_num);
  void* coord;

  // Load bounding coordinates
  for(int64_t i=0; i<tile_num; ++i) {
    coord = malloc(cell_size);
    memcpy(coord, buffer + offset, cell_size);
    fragment_info.bounding_coordinates_[i].first = coord; 
    offset += cell_size;
    coord = malloc(cell_size);
    memcpy(coord, buffer + offset, cell_size);
    fragment_info.bounding_coordinates_[i].second = coord; 
    offset += cell_size;
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// MBR#1_dim#1_low(T) MBR#1_dim#1_high(T) ...
// MBR#1_dim#2_low(T) MBR#1_dim#2_high(T) ...
// ...
// MBR#2_dim#1_low(T) MBR#2_dim#1_high(T) ...
// ...
// NOTE: T is the type of the dimensions of this array
void StorageManager::load_MBRs(FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *fragment_info.array_schema_;
  int attribute_num = array_schema.attribute_num();
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  size_t cell_size = array_schema.cell_size(attribute_num);
  int64_t tile_num = fragment_info.tile_ids_.size();
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
  size_t buffer_size = st.st_size;
  assert(buffer_size == tile_num * 2 * cell_size);
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  size_t offset = 0;
  fragment_info.mbrs_.resize(tile_num);
  void* mbr;

  // Load MBRs
  for(int64_t i=0; i<tile_num; ++i) {
    mbr = malloc(2 * cell_size);
    memcpy(mbr, buffer + offset, 2 * cell_size);
    fragment_info.mbrs_[i] = mbr;
    offset += 2 * cell_size;
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(int64_t)
// tile#2_of_attribute#1_offset(int64_t)
// ...
// tile#1_of_attribute#2_offset(int64_t)
// tile#2_of_attribute#2_offset(int64_t)
// ...
// NOTE: Do not forget the extra coordinate attribute
void StorageManager::load_offsets(FragmentInfo& fragment_info) const {
  // For easy reference
  const ArraySchema& array_schema = *fragment_info.array_schema_;
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  int attribute_num = array_schema.attribute_num();
  int64_t tile_num = fragment_info.tile_ids_.size();
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
  size_t buffer_size = st.st_size;
  assert(buffer_size == (attribute_num+1)*tile_num*sizeof(int64_t));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t offset = 0;

  // Load offsets
  for(int i=0; i<=attribute_num; ++i) {
    fragment_info.offsets_[i].resize(tile_num);
    memcpy(&fragment_info.offsets_[i][0], buffer + offset, 
           tile_num*sizeof(int64_t));
    offset += tile_num * sizeof(int64_t);
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

inline
std::pair<size_t, int64_t> StorageManager::load_payloads_into_buffer(
    const FragmentInfo& fragment_info, 
    int attribute_id, int64_t start_pos, char*& buffer) const {
  // For easy reference
  const ArraySchema& array_schema = *(fragment_info.array_schema_);
  const std::string& array_name = array_schema.array_name();
  const std::string& fragment_name = fragment_info.fragment_name_;
  const std::string& attribute_name = 
      array_schema.attribute_name(attribute_id);
  const OffsetList& offsets = fragment_info.offsets_[attribute_id];
  int64_t tile_num = offsets.size();
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
  size_t buffer_size = 0;
  int64_t tiles_in_buffer = 0;
  int64_t pos = start_pos;

  // Calculate buffer size (smallest size larger than or equal to segment_size_)
  while(pos < tile_num && buffer_size < segment_size_) {
    if(pos == tile_num-1)
      buffer_size += st.st_size - offsets[pos];
    else
      buffer_size += offsets[pos+1] - offsets[pos];
    pos++;
    tiles_in_buffer++;
  }

  assert(buffer_size != 0);
  assert(offsets[start_pos] + buffer_size <= st.st_size);

  // Read payloads into buffer
  buffer = new char[buffer_size];
  lseek(fd, offsets[start_pos], SEEK_SET);
  read(fd, buffer, buffer_size); 
  close(fd);

  return std::pair<size_t, int64_t>(buffer_size, tiles_in_buffer);
}

// FILE FORMAT:
// tile_num(int64_t)
//   tile_id#1(int64_t) tile_id#2(int64_t)  ...
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
  size_t buffer_size = st.st_size;

  if(buffer_size == 0) // Empty array
    return;

  assert(buffer_size > sizeof(int64_t));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t tile_num;
  memcpy(&tile_num, buffer, sizeof(int64_t));
  assert(buffer_size == (tile_num+1)*sizeof(int64_t));
  fragment_info.tile_ids_.resize(tile_num);

  // Load tile_ids
  memcpy(&fragment_info.tile_ids_[0], 
         buffer+sizeof(int64_t), tile_num*sizeof(int64_t));

  // Clean up
  delete [] buffer;
  close(fd);
}

void StorageManager::load_tiles_from_disk(
    FragmentInfo& fragment_info, int attribute_id, int64_t start_pos) const { 
  char* buffer;

  // Load the tile payloads from the disk into a buffer
  std::pair<size_t, int64_t> ret = load_payloads_into_buffer(
      fragment_info, attribute_id, start_pos, buffer); 

  // For easy reference
  size_t buffer_size = ret.first;
  int64_t tiles_in_buffer = ret.second;

  // Delete previous tiles from main memory
  delete_tiles(fragment_info, attribute_id);
  
  // Create the tiles from the payloads in the buffer and load them
  // into the tile book-keeping structure
  load_tiles_from_buffer(fragment_info, 
                         attribute_id, start_pos, 
                         buffer, buffer_size, tiles_in_buffer);         
 
  // Update rank range in main memory
  fragment_info.pos_ranges_[attribute_id].first = start_pos;
  fragment_info.pos_ranges_[attribute_id].second = 
      start_pos + tiles_in_buffer - 1;

  // Store retrieved segment
  fragment_info.segments_[attribute_id] = buffer;
}

void StorageManager::load_tiles_from_buffer(
    FragmentInfo& fragment_info, 
    int attribute_id, int64_t start_pos, char* buffer, 
    size_t buffer_size, int64_t tiles_in_buffer) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_info.array_schema_;
  const OffsetList& offsets = fragment_info.offsets_[attribute_id];
  const TileIds& tile_ids = fragment_info.tile_ids_;
  const MBRs& mbrs = fragment_info.mbrs_;
  const BoundingCoordinates& bounding_coordinates = 
      fragment_info.bounding_coordinates_;
  assert(offsets.size() == tile_ids.size());
  TileList& tiles = fragment_info.tiles_[attribute_id];
  int attribute_num = array_schema->attribute_num();

  // Initializations
  size_t buffer_offset = 0, payload_size;
  int64_t tile_id;
  int64_t pos = start_pos;
  tiles.resize(tiles_in_buffer);
  void* payload;
  
  // Load tiles
  for(int64_t i=0; i<tiles_in_buffer; ++i, ++pos) {
    assert(pos < tile_ids.size());
    tile_id = tile_ids[pos];

    if(pos == offsets.size()-1)
      payload_size = buffer_size - buffer_offset;
    else
      payload_size = offsets[pos+1] - offsets[pos];

    payload = buffer + buffer_offset;
    
    int64_t cell_num = payload_size / array_schema->cell_size(attribute_id);
    Tile* tile = new_tile(array_schema, attribute_id, tile_id, cell_num);
    tile->set_payload(payload, payload_size);
    if(tile->tile_type() == Tile::COORDINATE) 
      tile->set_mbr(mbrs[pos]);
    
    tiles[i] = tile;
    buffer_offset += payload_size;
  }
}

bool StorageManager::path_exists(const std::string& path) const {
  struct stat st;
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

inline
void StorageManager::prepare_segment( 
    FragmentInfo& fragment_info, int attribute_id,
    int64_t file_offset, size_t segment_size, char* segment) const {
  size_t segment_offset = 0;
  size_t tile_size;
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

int64_t StorageManager::tile_pos(const FragmentInfo& fragment_info, 
                                  int64_t tile_id) const {
  // Check of the array has not tiles
  if(fragment_info.tile_ids_.size() == 0 || tile_id  < 0)
    return SM_INVALID_POS;

  // Perform binary search
  int64_t min = 0;
  int64_t max = fragment_info.tile_ids_.size()-1;
  int64_t mid;
  while(min <= max) {
    mid = min + ((max - min) / 2);
    if(fragment_info.tile_ids_[mid] == tile_id)  // Tile found!
      return mid;
    else if(fragment_info.tile_ids_[mid] > tile_id) {
      if(mid == 0)
        return SM_INVALID_POS;
      max = mid - 1;
    }
    else // (fragment_info.tile_ids_[mid] < tile_id)
      min = mid + 1;
  }

  // Tile id not found
  return SM_INVALID_POS;
}

