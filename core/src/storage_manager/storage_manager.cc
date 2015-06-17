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

#include "const_cell_iterator.h"
#include "storage_manager.h"
#include "special_values.h"
#include <algorithm>
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <typeinfo>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

StorageManager::StorageManager(const std::string& path, 
                               const MPIHandler* mpi_handler,
                               size_t segment_size)
    : segment_size_(segment_size), mpi_handler_(mpi_handler) {
  set_workspace(path);
  create_directory(workspace_);
  create_directory(workspace_ + "/" + TEMP + "/"); 

  write_state_max_size_ = WRITE_STATE_MAX_SIZE;

  arrays_ = new Array*[MAX_OPEN_ARRAYS]; 
  for(int i=0; i<MAX_OPEN_ARRAYS; ++i)
    arrays_[i] = NULL;
}

StorageManager::~StorageManager() {
  for(int i=0; i<MAX_OPEN_ARRAYS; ++i) {
    if(arrays_[i] != NULL)
      close_array(i);
  }

  delete [] arrays_;
}

/******************************************************
********************* MUTATORS ************************
******************************************************/

void StorageManager::set_segment_size(size_t segment_size) {
  segment_size_ = segment_size;
}

/******************************************************
****************** ARRAY FUNCTIONS ********************
******************************************************/

bool StorageManager::array_defined(const std::string& array_name) const {
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         ARRAY_SCHEMA_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

int StorageManager::clear_array(
    const std::string& array_name, std::string& err_msg) {
  // Check if the array is defined
  if(!array_defined(array_name)) {
    err_msg = std::string("Array '") + array_name + "' is not defined.";
    return -1;
  }

  // Close the array if it is open 
  OpenArrays::iterator it = open_arrays_.find(array_name);
  if(it != open_arrays_.end())
    forced_close_array(it->second);

  // Delete the entire array directory
  std::string dirname = workspace_ + "/" + array_name + "/";
  std::string filename; 

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  std::string fragments_filename = std::string(FRAGMENT_TREE_FILENAME) + 
                                   BOOK_KEEPING_FILE_SUFFIX;
  std::string array_schema_filename = std::string(ARRAY_SCHEMA_FILENAME) + 
                                      BOOK_KEEPING_FILE_SUFFIX;
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return 0;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0 ||
       strcmp(next_file->d_name, array_schema_filename.c_str()) == 0)
      continue;
    if(strcmp(next_file->d_name, fragments_filename.c_str()) == 0)  {
      filename = dirname + next_file->d_name;
      remove(filename.c_str());
    }
    else {  // It is a fragment directory
      delete_directory(dirname + next_file->d_name);
    }
  } 
  
  closedir(dir);

  err_msg = "";
  return 0;
}

void StorageManager::close_array(int ad) {
  if(arrays_[ad] == NULL)
    return;

  open_arrays_.erase(arrays_[ad]->array_schema()->array_name());
  delete arrays_[ad];
  arrays_[ad] = NULL;
}

int StorageManager::define_array(
    const ArraySchema* array_schema, std::string& err_msg) {
  // For easy reference
  const std::string& array_name = array_schema->array_name();

  // Delete array if it exists
  if(array_defined(array_name)) 
    delete_array(array_name, err_msg); 

  // Create array directory
  std::string dir_name = workspace_ + "/" + array_name + "/"; 
  create_directory(dir_name);

  // Open file
  std::string filename = dir_name + 
                         ARRAY_SCHEMA_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;
  remove(filename.c_str());
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    err_msg = "Cannot create the array schema file.";
    return -1;
  }

  // Serialize array schema
  std::pair<const char*, size_t> ret = array_schema->serialize();
  const char* buffer = ret.first;
  size_t buffer_size = ret.second; 

  // Store the array schema
  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);

  return 0;
}

int StorageManager::delete_array(
    const std::string& array_name, std::string& err_msg) {
  // Check if the array is defined
  if(!array_defined(array_name)) {
    err_msg = std::string("Array '") + array_name + "' is not defined.";
    return -1;
  }

  // Close the array if it is open 
  OpenArrays::iterator it = open_arrays_.find(array_name);
  if(it != open_arrays_.end())
    forced_close_array(it->second);

  // Delete the entire array directory
  std::string dirname = workspace_ + "/" + array_name + "/";
  std::string filename; 
  std::string fragments_filename = std::string(FRAGMENT_TREE_FILENAME) + 
                                   BOOK_KEEPING_FILE_SUFFIX;
  std::string array_schema_filename = std::string(ARRAY_SCHEMA_FILENAME) + 
                                      BOOK_KEEPING_FILE_SUFFIX;

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return 0;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0)
      continue;
    if(strcmp(next_file->d_name, fragments_filename.c_str()) == 0 ||
       strcmp(next_file->d_name, array_schema_filename.c_str()) == 0) {
      filename = dirname + next_file->d_name;
      remove(filename.c_str());
    }
    else {  // It is a fragment directory
      delete_directory(dirname + next_file->d_name);
    }
  } 
  
  closedir(dir);
  rmdir(dirname.c_str());
}

void StorageManager::forced_close_array(int ad) {
  if(arrays_[ad] == NULL)
    return;

  const std::string& array_name = arrays_[ad]->array_schema()->array_name();

  arrays_[ad]->forced_close();
  open_arrays_.erase(array_name);
  delete arrays_[ad];
  arrays_[ad] = NULL;
}

int StorageManager::get_array_schema(
    int ad, const ArraySchema*& array_schema, std::string& err_msg) const {
  if(arrays_[ad] == NULL) {
    err_msg = "Array is not open.";
    return -1;
  }

  array_schema = arrays_[ad]->array_schema();
  return 0;
}

int StorageManager::get_array_schema(
    const std::string& array_name, ArraySchema*& array_schema,
    std::string& err_msg) const {
  // The schema to be returned
  array_schema = new ArraySchema();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         ARRAY_SCHEMA_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    err_msg = std::string("Cannot access array schema file for array '") +
              array_name + "'.";
    return -1;
  }

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

  return 0;
}

void StorageManager::get_version() const {
  std::cout << "TileDB StorageManager Version 0.1\n";
}

int StorageManager::load_sorted_bin(
    const std::string& dirname, const std::string& array_name,
    std::string& err_msg) {
  assert(is_dir(dirname));

  // Open array
  int ad = open_array(array_name, "w", err_msg);
  if(ad == -1) 
    return -1;

  arrays_[ad]->load_sorted_bin(dirname);
 
  // Close array
  close_array(ad); 

  return 0;
}

int StorageManager::open_array(
    const std::string& array_name, const char* mode, std::string& err_msg) {
  // Proper checks
  int err = check_on_open_array(array_name, mode, err_msg);
  if(err == -1)
    return -1;

  // Get array mode
  Array::Mode array_mode;
  if(strcmp(mode, "w") == 0)
    array_mode = Array::WRITE;
  else if(strcmp(mode, "a") == 0) 
    array_mode = Array::APPEND;
  else if(strcmp(mode, "r") == 0)
    array_mode = Array::READ;

  // If in write mode, delete the array if it exists
  if(array_mode == Array::WRITE) 
    clear_array(array_name, err_msg); 

  // Initialize an Array object
  ArraySchema* array_schema;
  get_array_schema(array_name, array_schema, err_msg);
  Array* array = new Array(workspace_, segment_size_,
                           write_state_max_size_,
                           array_schema, array_mode);

  // If the array is in write or append mode, initialize a new fragment
  if(array_mode == Array::WRITE || array_mode == Array::APPEND)
    array->new_fragment();

  // Stores the Array object and returns an array descriptor
  int ad = store_array(array);

  // Maximum open arrays reached
  if(ad == -1) {
    delete array; 
    err_msg = std::string("Cannot open array' ") + array_name +
              "'. Maximum open arrays reached.";
    return -1;
  }

  // Keep track of the opened array
  open_arrays_[array_name] = ad;

  return ad;
}

/******************************************************
******************* CELL FUNCTIONS ********************
******************************************************/

template<class T>
ArrayConstCellIterator<T> StorageManager::begin(
    int ad) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);
  assert(!arrays_[ad]->empty());

  return ArrayConstCellIterator<T>(arrays_[ad]);
}

template<class T>
ArrayConstCellIterator<T> StorageManager::begin(
    int ad, const std::vector<int>& attribute_ids) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);
  assert(!arrays_[ad]->empty());

  return ArrayConstCellIterator<T>(arrays_[ad], attribute_ids);
}

template<class T>
ArrayConstCellIterator<T> StorageManager::begin(
    int ad, const T* range) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);
  assert(!arrays_[ad]->empty());

  return ArrayConstCellIterator<T>(arrays_[ad], range);
}

template<class T>
ArrayConstCellIterator<T> StorageManager::begin(
    int ad, const T* range, const std::vector<int>& attribute_ids) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);
  assert(!arrays_[ad]->empty());

  return ArrayConstCellIterator<T>(arrays_[ad], range, attribute_ids);
}

template<class T>
ArrayConstReverseCellIterator<T> StorageManager::rbegin(
    int ad) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);
  assert(!arrays_[ad]->empty());

  return ArrayConstReverseCellIterator<T>(arrays_[ad]);
}

template<class T>
ArrayConstReverseCellIterator<T> StorageManager::rbegin(
    int ad, const std::vector<int>& attribute_ids) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);
  assert(!arrays_[ad]->empty());

  return ArrayConstReverseCellIterator<T>(arrays_[ad], attribute_ids);
}

template<class T>
ArrayConstReverseCellIterator<T> StorageManager::rbegin(
    int ad, const T* range) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);
  assert(!arrays_[ad]->empty());

  return ArrayConstReverseCellIterator<T>(arrays_[ad], range);
}

template<class T>
ArrayConstReverseCellIterator<T> StorageManager::rbegin(
    int ad, const T* range, const std::vector<int>& attribute_ids) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);
  assert(!arrays_[ad]->empty());

  return ArrayConstReverseCellIterator<T>(arrays_[ad], range, attribute_ids);
}

void StorageManager::read_cells(int ad, const void* range, 
                                const std::vector<int>& attribute_ids,
                                void*& cells, size_t& cells_size) const {
  // For easy reference
  int attribute_num = arrays_[ad]->array_schema()->attribute_num();
  const std::type_info& coords_type = 
      *(arrays_[ad]->array_schema()->type(attribute_num));

  if(coords_type == typeid(int))
    read_cells<int>(ad, static_cast<const int*>(range), 
                    attribute_ids, cells, cells_size);
  else if(coords_type == typeid(int64_t))
    read_cells<int64_t>(ad, static_cast<const int64_t*>(range), 
                        attribute_ids, cells, cells_size);
  else if(coords_type == typeid(float))
    read_cells<float>(ad, static_cast<const float*>(range), 
                      attribute_ids, cells, cells_size);
  else if(coords_type == typeid(double))
    read_cells<double>(ad, static_cast<const double*>(range), 
                       attribute_ids, cells, cells_size);
}

void StorageManager::read_cells(int ad, const void* range,
                                const std::vector<int>& attribute_ids,
                                void*& cells, size_t& cells_size,
                                int rcv_rank) const {
  // For easy reference
  int attribute_num = arrays_[ad]->array_schema()->attribute_num();
  const std::type_info& coords_type = 
      *(arrays_[ad]->array_schema()->type(attribute_num));

  if(coords_type == typeid(int))
    read_cells<int>(ad, static_cast<const int*>(range), 
                    attribute_ids, cells, cells_size, rcv_rank);
  else if(coords_type == typeid(int64_t))
    read_cells<int64_t>(ad, static_cast<const int64_t*>(range),
                        attribute_ids, cells, cells_size, rcv_rank);
  else if(coords_type == typeid(float))
    read_cells<float>(ad, static_cast<const float*>(range),
                      attribute_ids, cells, cells_size, rcv_rank);
  else if(coords_type == typeid(double))
    read_cells<double>(ad, static_cast<const double*>(range),
                       attribute_ids, cells, cells_size, rcv_rank);
}

template<class T>
void StorageManager::read_cells(int ad, const T* range, 
                                const std::vector<int>& attribute_ids,
                                void*& cells, size_t& cells_size) const {
  // Initialization
  size_t cell_size;          // Single cell
  cells_size = 0;            // All cells collectively
  size_t buffer_size = segment_size_;
  cells = malloc(buffer_size);

  // Prepare cell iterator
  ArrayConstCellIterator<T> cell_it = begin<T>(ad, range, attribute_ids);

  // Write cells into the CSV file
  for(; !cell_it.end(); ++cell_it) { 
    // Expand buffer
    if(cells_size == buffer_size) {
      expand_buffer(cells, buffer_size);
      buffer_size *= 2;
    } 

    // Retrieve cell size
    cell_size = cell_it.cell_size();
    memcpy(static_cast<char*>(cells) + cells_size, *cell_it, cell_size);
    cells_size += cell_size;
  }
}

template<class T>
void StorageManager::read_cells(int ad, const T* range,
                                const std::vector<int>& attribute_ids,
                                void*& cells, size_t& cells_size,
                                int rcv_rank) const {
  assert(mpi_handler_ != NULL);

  // Read local cells in the range
  void* local_cells;
  size_t local_cells_size;
  read_cells(ad, range, attribute_ids, local_cells, local_cells_size);

  // Collect all cells from all processes
  void* all_cells;
  size_t all_cells_size;
  mpi_handler_->gather(local_cells, local_cells_size,
                       all_cells, all_cells_size, 
                       rcv_rank); 

  if(rcv_rank == mpi_handler_->rank()) {
    cells = all_cells;
    cells_size = all_cells_size; 
  }
}

template<class T>
void StorageManager::write_cell(int ad, const void* cell) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);

  arrays_[ad]->write_cell<T>(cell);
}

template<class T>
void StorageManager::write_cell_sorted(int ad, const void* cell) const {
  arrays_[ad]->write_cell_sorted<T>(cell); 
}

void StorageManager::write_cells(
    int ad, const void* cells, size_t cells_size) const {
  // For easy reference
  int attribute_num = arrays_[ad]->array_schema()->attribute_num();
  const std::type_info& coords_type = 
      *(arrays_[ad]->array_schema()->type(attribute_num));

  if(coords_type == typeid(int))
    write_cells<int>(ad, cells, cells_size);
  else if(coords_type == typeid(int64_t))
    write_cells<int64_t>(ad, cells, cells_size);
  else if(coords_type == typeid(float))
    write_cells<float>(ad, cells, cells_size);
  else if(coords_type == typeid(double))
    write_cells<double>(ad, cells, cells_size);
}

template<class T>
void StorageManager::write_cells(
    int ad, const void* cells, size_t cells_size) const {
  ConstCellIterator cell_it(cells, cells_size, arrays_[ad]->array_schema());

  for(; !cell_it.end(); ++cell_it) 
    write_cell<T>(ad, *cell_it);
}

void StorageManager::write_cells_sorted(
    int ad, const void* cells, size_t cells_size) const {
  // For easy reference
  int attribute_num = arrays_[ad]->array_schema()->attribute_num();
  const std::type_info& coords_type = 
      *(arrays_[ad]->array_schema()->type(attribute_num));

  if(coords_type == typeid(int))
    write_cells_sorted<int>(ad, cells, cells_size);
  else if(coords_type == typeid(int64_t))
    write_cells_sorted<int64_t>(ad, cells, cells_size);
  else if(coords_type == typeid(float))
    write_cells_sorted<float>(ad, cells, cells_size);
  else if(coords_type == typeid(double))
    write_cells_sorted<double>(ad, cells, cells_size);
}

template<class T>
void StorageManager::write_cells_sorted(
    int ad, const void* cells, size_t cells_size) const {
  ConstCellIterator cell_it(cells, cells_size, arrays_[ad]->array_schema());

  for(; !cell_it.end(); ++cell_it) 
    write_cell_sorted<T>(ad, *cell_it);
}

/******************************************************
***************** PRIVATE FUNCTIONS *******************
******************************************************/

int StorageManager::check_on_open_array(
    const std::string& array_name, const char* mode,
    std::string& err_msg) const {
  // Check if the array is defined
  if(!array_defined(array_name)) {
    err_msg = std::string("Array '") + array_name + "' is not defined.";
    return -1;
  }

  // Check mode
  if(invalid_array_mode(mode)) {
    err_msg = std::string("Invalid mode '") + mode + "'."; 
    return -1;
  }

  // Check if array is already open
  if(open_arrays_.find(array_name) != open_arrays_.end()) {
    err_msg = std::string("Array '") + array_name + "' already open."; 
    return -1;
  }

  return 0;
}

bool StorageManager::invalid_array_mode(const char* mode) const {
  if(strcmp(mode, "r") && strcmp(mode, "w") && strcmp(mode, "a"))
    return true;
  else 
    return false;
}

inline
void StorageManager::set_workspace(const std::string& path) {
  workspace_ = absolute_path(path);
  assert(is_dir(workspace_));

  if(mpi_handler_ == NULL) {
    workspace_ += "/StorageManager/";
  } else {
    std::stringstream ss;
    ss << workspace_<< "/StorageManager_"  
       << mpi_handler_->rank() << "/"; 
    workspace_ = ss.str();
  }
}

int StorageManager::store_array(Array* array) {
  int ad = -1;

  for(int i=0; i<MAX_OPEN_ARRAYS; ++i) {
    if(arrays_[i] == NULL) {
      ad = i; 
      break;
    }
  }

  if(ad != -1) 
    arrays_[ad] = array;

  return ad;
}

// Explicit template instantiations
template ArrayConstCellIterator<int>
StorageManager::begin<int>(int ad) const;
template ArrayConstCellIterator<int64_t>
StorageManager::begin<int64_t>(int ad) const;
template ArrayConstCellIterator<float>
StorageManager::begin<float>(int ad) const;
template ArrayConstCellIterator<double>
StorageManager::begin<double>(int ad) const;

template ArrayConstCellIterator<int>
StorageManager::begin<int>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<int64_t>
StorageManager::begin<int64_t>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<float>
StorageManager::begin<float>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<double>
StorageManager::begin<double>(
    int ad, const std::vector<int>& attribute_ids) const;

template ArrayConstCellIterator<int>
StorageManager::begin<int>(int ad, const int* range) const;
template ArrayConstCellIterator<int64_t>
StorageManager::begin<int64_t>(int ad, const int64_t* range) const;
template ArrayConstCellIterator<float>
StorageManager::begin<float>(int ad, const float* range) const;
template ArrayConstCellIterator<double>
StorageManager::begin<double>(int ad, const double* range) const;

template ArrayConstCellIterator<int>
StorageManager::begin<int>(
    int ad, const int* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<int64_t>
StorageManager::begin<int64_t>(
    int ad, const int64_t* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<float>
StorageManager::begin<float>(
    int ad, const float* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<double>
StorageManager::begin<double>(
    int ad, const double* range, const std::vector<int>& attribute_ids) const;

template ArrayConstReverseCellIterator<int>
StorageManager::rbegin<int>(int ad) const;
template ArrayConstReverseCellIterator<int64_t>
StorageManager::rbegin<int64_t>(int ad) const;
template ArrayConstReverseCellIterator<float>
StorageManager::rbegin<float>(int ad) const;
template ArrayConstReverseCellIterator<double>
StorageManager::rbegin<double>(int ad) const;

template ArrayConstReverseCellIterator<int>
StorageManager::rbegin<int>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<int64_t>
StorageManager::rbegin<int64_t>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<float>
StorageManager::rbegin<float>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<double>
StorageManager::rbegin<double>(
    int ad, const std::vector<int>& attribute_ids) const;

template ArrayConstReverseCellIterator<int>
StorageManager::rbegin<int>(int ad, const int* range) const;
template ArrayConstReverseCellIterator<int64_t>
StorageManager::rbegin<int64_t>(int ad, const int64_t* range) const;
template ArrayConstReverseCellIterator<float>
StorageManager::rbegin<float>(int ad, const float* range) const;
template ArrayConstReverseCellIterator<double>
StorageManager::rbegin<double>(int ad, const double* range) const;

template ArrayConstReverseCellIterator<int>
StorageManager::rbegin<int>(
    int ad, const int* range, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<int64_t>
StorageManager::rbegin<int64_t>(
    int ad, const int64_t* range, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<float>
StorageManager::rbegin<float>(
    int ad, const float* range, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<double>
StorageManager::rbegin<double>(
    int ad, const double* range, const std::vector<int>& attribute_ids) const;

template void StorageManager::write_cell<int>(
    int ad, const void* cell) const;
template void StorageManager::write_cell<int64_t>(
    int ad, const void* cell) const;
template void StorageManager::write_cell<float>(
    int ad, const void* cell) const;
template void StorageManager::write_cell<double>(
    int ad, const void* cell) const;

template void StorageManager::write_cell_sorted<int>(
    int ad, const void* cell) const;
template void StorageManager::write_cell_sorted<int64_t>(
    int ad, const void* cell) const;
template void StorageManager::write_cell_sorted<float>(
    int ad, const void* cell) const;
template void StorageManager::write_cell_sorted<double>(
    int ad, const void* cell) const;

template void StorageManager::read_cells<int>(
    int ad, const int* range, const std::vector<int>& attribute_ids, 
    void*& cells, size_t& cells_size, int rcv_rank) const;
template void StorageManager::read_cells<int64_t>(
    int ad, const int64_t* range, const std::vector<int>& attribute_ids,
    void*& cells, size_t& cells_size, int rcv_rank) const;
template void StorageManager::read_cells<float>(
    int ad, const float* range, const std::vector<int>& attribute_ids,
    void*& cells, size_t& cells_size, int rcv_rank) const;
template void StorageManager::read_cells<double>(
    int ad, const double* range, const std::vector<int>& attribute_ids,
    void*& cells, size_t& cells_size, int rcv_rank) const;
