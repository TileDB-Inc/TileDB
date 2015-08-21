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
#include "special_values.h"
#include "storage_manager.h"
#include "tiledb_error.h"
#include "utils.h"
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
                               size_t segment_size)
    : segment_size_(segment_size), arrays_(NULL) {
  // Success code
  err_ = TILEDB_OK;

  if(!is_dir(path)) {
    std::cerr << ERROR_MSG_HEADER 
              << " Workspace directory '" << path << "' does not exist.\n";
    err_ = TILEDB_EDNEXIST;
    return;
  }

  // Set workspace
  set_workspace(path);

  // Create directories
  int rc;
  if(!is_dir(workspace_)) {
    rc = create_directory(workspace_);
    if(rc) {
      std::cerr << ERROR_MSG_HEADER
                << " Cannot create directory '" << workspace_ << "'.\n";
      err_ = TILEDB_EDNCREAT;
      return;
    }
  }
  std::string tmpdir = workspace_ + "/" + TEMP + "/";
  if(!is_dir(tmpdir)) {
    rc = create_directory(tmpdir);
    if(rc) {
      std::cerr << ERROR_MSG_HEADER
                << " Cannot create directory '" << tmpdir << "'.\n";
      err_ = TILEDB_EDNCREAT;
      return;
    }
  }

  // Set maximum size for write state
  write_state_max_size_ = WRITE_STATE_MAX_SIZE;

  // Initializes the structure that holds the open arrays
  arrays_ = new Array*[MAX_OPEN_ARRAYS]; 
  for(int i=0; i<MAX_OPEN_ARRAYS; ++i)
    arrays_[i] = NULL;
}

StorageManager::~StorageManager() {
  if (arrays_ != NULL) {
    for(int i=0; i<MAX_OPEN_ARRAYS; ++i) {
      if(arrays_[i] != NULL)
        close_array(i);
    }
    delete [] arrays_;
  }
}

/******************************************************
********************* ACCESSORS ************************
******************************************************/

int StorageManager::err() const {
  return err_;
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

int StorageManager::clear_array(const std::string& array_name) {
// TODO: more error messages

  // Check if the array is defined
  if(!array_defined(array_name)) {
    std::cerr << ERROR_MSG_HEADER 
              << " Array '" + array_name + "' is not defined.\n";
    return TILEDB_ENDEFARR;
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
  if(dir == NULL) {
    std::cerr << ERROR_MSG_HEADER 
              << " Cannot open directory '" + dirname + "'.\n";
    return TILEDB_EFILE;
  }

  while((next_file = readdir(dir))) {
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

  return TILEDB_OK;
}

void StorageManager::close_array(int ad) {
  if(arrays_[ad] == NULL)
    return;

  open_arrays_.erase(arrays_[ad]->array_schema()->array_name());
  delete arrays_[ad];
  arrays_[ad] = NULL;
}

int StorageManager::define_array(const ArraySchema* array_schema) {
  // For easy reference
  const std::string& array_name = array_schema->array_name();
  std::string err_msg;

  // Delete array if it exists
  if(array_defined(array_name)) 
    delete_array(array_name); 

  // Create array directory
  std::string dir_name = workspace_ + "/" + array_name + "/"; 
  create_directory(dir_name);

  // Open file
  std::string filename = dir_name + 
                         ARRAY_SCHEMA_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;
  remove(filename.c_str());
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) 
    return -1;

  // Serialize array schema
  std::pair<const char*, size_t> ret = array_schema->serialize();
  const char* buffer = ret.first;
  size_t buffer_size = ret.second; 

  // Store the array schema
  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);

  return TILEDB_OK;
}

int StorageManager::delete_array(const std::string& array_name) {
  // Check if the array is defined
  if(!array_defined(array_name)) {
    std::cerr << ERROR_MSG_HEADER 
              << " Array '" + array_name + "' is not defined.\n";
    return TILEDB_ENDEFARR;
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
  if(dir == NULL) {
    std::cerr << ERROR_MSG_HEADER 
              << " Cannot open directory '" + dirname + "'.\n";
    return TILEDB_EFILE;
  }

  // TODO: more error handling here
  while((next_file = readdir(dir))) {
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

  return TILEDB_OK;
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
    int ad, const ArraySchema*& array_schema) const {
std::string err_msg;

 // TODO: Remove err_msg

  if(arrays_[ad] == NULL) {
    err_msg = "Array is not open.";
    return -1;
  }

  array_schema = arrays_[ad]->array_schema();
  return TILEDB_OK;
}

int StorageManager::get_array_schema(
    const std::string& array_name, 
    ArraySchema*& array_schema) const {
  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         ARRAY_SCHEMA_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    if(!is_file(filename.c_str())) {
      std::cerr << ERROR_MSG_HEADER 
               << " Undefined array '" << array_name << "'.\n";
      return TILEDB_ENDEFARR;
    } else {
      std::cerr << ERROR_MSG_HEADER 
               << " Cannot open file '" << filename << "'.\n";
      return TILEDB_EFILE;
    }
  }

  // The schema to be returned
  array_schema = new ArraySchema();

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  ssize_t buffer_size = st.st_size;
  if(buffer_size == 0) {
    std::cerr << ERROR_MSG_HEADER 
             << " File '" << filename << "' is empty.\n";
    return TILEDB_EFILE;
  }
  char* buffer = new char[buffer_size];

  // Load array schema
  ssize_t bytes_read = read(fd, buffer, buffer_size);
  if(bytes_read != buffer_size) {
    // Clean up
    delete [] buffer;
    std::cerr << ERROR_MSG_HEADER 
             << " Cannot read schema from file '" << filename << "'.\n";
    return TILEDB_EFILE;
  } 
  array_schema->deserialize(buffer, buffer_size);

  // Clean up
  delete [] buffer;
  int rc = close(fd);
  if(rc != 0) {
    std::cerr << ERROR_MSG_HEADER 
             << " Cannot close file '" << filename << "'.\n";
    return TILEDB_EFILE;
  }

  return TILEDB_OK;
}

void StorageManager::get_version() {
  std::cout << "TileDB StorageManager Version 0.1\n";
}

int StorageManager::open_array(
    const std::string& array_name, const char* mode) {
  // Proper checks
  int err = check_on_open_array(array_name, mode);
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
  else
    return -1; // TODO: better error here

  // If in write mode, delete the array if it exists
  if(array_mode == Array::WRITE) 
    clear_array(array_name); 

  // Initialize an Array object
  ArraySchema* array_schema;
  get_array_schema(array_name, array_schema);
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
// TODO    err_msg = std::string("Cannot open array' ") + array_name +
//              "'. Maximum open arrays reached.";
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
ArrayConstCellIterator<T>* StorageManager::begin(
    int ad) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);

  return new ArrayConstCellIterator<T>(arrays_[ad]);
}

template<class T>
ArrayConstCellIterator<T>* StorageManager::begin(
    int ad, const std::vector<int>& attribute_ids) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);

  return new ArrayConstCellIterator<T>(arrays_[ad], attribute_ids);
}

template<class T>
ArrayConstCellIterator<T>* StorageManager::begin(
    int ad, const T* range) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);

  return new ArrayConstCellIterator<T>(arrays_[ad], range);
}

template<class T>
ArrayConstCellIterator<T>* StorageManager::begin(
    int ad, const T* range, const std::vector<int>& attribute_ids) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);

  return new ArrayConstCellIterator<T>(arrays_[ad], range, attribute_ids);
}

template<class T>
ArrayConstReverseCellIterator<T>* StorageManager::rbegin(
    int ad) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);

  return new ArrayConstReverseCellIterator<T>(arrays_[ad]);
}

template<class T>
ArrayConstReverseCellIterator<T>* StorageManager::rbegin(
    int ad, const std::vector<int>& attribute_ids) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);

  return new ArrayConstReverseCellIterator<T>(arrays_[ad], attribute_ids);
}

template<class T>
ArrayConstReverseCellIterator<T>* StorageManager::rbegin(
    int ad, const T* range) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);

  return new ArrayConstReverseCellIterator<T>(arrays_[ad], range);
}

template<class T>
ArrayConstReverseCellIterator<T>* StorageManager::rbegin(
    int ad, const T* range, const std::vector<int>& attribute_ids) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::READ);

  return new ArrayConstReverseCellIterator<T>(
                 arrays_[ad], range, attribute_ids);
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
  ArrayConstCellIterator<T>* cell_it = begin<T>(ad, range, attribute_ids);

  // Write cells into the CSV file
  for(; !cell_it->end(); ++(*cell_it)) { 
    // Expand buffer
    if(cells_size == buffer_size) {
      expand_buffer(cells, buffer_size);
      buffer_size *= 2;
    } 

    // Retrieve cell size
    cell_size = cell_it->cell_size();
    memcpy(static_cast<char*>(cells) + cells_size, **cell_it, cell_size);
    cells_size += cell_size;
  }
}

template<class T>
void StorageManager::write_cell(int ad, const void* cell) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::WRITE ||
         arrays_[ad]->mode() == Array::APPEND);

  arrays_[ad]->write_cell<T>(cell);
}

template<class T>
void StorageManager::write_cell_sorted(int ad, const void* cell) const {
  assert(ad >= 0 && ad < MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(arrays_[ad]->mode() == Array::WRITE ||
         arrays_[ad]->mode() == Array::APPEND);

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
    const std::string& array_name, const char* mode) const {
  // Check if the array is defined
  if(!array_defined(array_name)) {
    // TODO err_msg = std::string("Array '") + array_name + "' is not defined.";
    return -1;
  }

  // Check mode
  if(invalid_array_mode(mode)) {
    // TODO err_msg = std::string("Invalid mode '") + mode + "'."; 
    return -1;
  }

  // Check if array is already open
  if(open_arrays_.find(array_name) != open_arrays_.end()) {
    // TODO err_msg = std::string("Array '") + array_name + "' already open."; 
    return -1;
  }

  return TILEDB_OK;
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

  if(!ends_with(workspace_, "/"))
    workspace_ += "/";

  workspace_ += "StorageManager";

  return;
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
template ArrayConstCellIterator<int>*
StorageManager::begin<int>(int ad) const;
template ArrayConstCellIterator<int64_t>*
StorageManager::begin<int64_t>(int ad) const;
template ArrayConstCellIterator<float>*
StorageManager::begin<float>(int ad) const;
template ArrayConstCellIterator<double>*
StorageManager::begin<double>(int ad) const;

template ArrayConstCellIterator<int>*
StorageManager::begin<int>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<int64_t>*
StorageManager::begin<int64_t>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<float>*
StorageManager::begin<float>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<double>*
StorageManager::begin<double>(
    int ad, const std::vector<int>& attribute_ids) const;

template ArrayConstCellIterator<int>*
StorageManager::begin<int>(int ad, const int* range) const;
template ArrayConstCellIterator<int64_t>*
StorageManager::begin<int64_t>(int ad, const int64_t* range) const;
template ArrayConstCellIterator<float>*
StorageManager::begin<float>(int ad, const float* range) const;
template ArrayConstCellIterator<double>*
StorageManager::begin<double>(int ad, const double* range) const;

template ArrayConstCellIterator<int>*
StorageManager::begin<int>(
    int ad, const int* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<int64_t>*
StorageManager::begin<int64_t>(
    int ad, const int64_t* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<float>*
StorageManager::begin<float>(
    int ad, const float* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<double>*
StorageManager::begin<double>(
    int ad, const double* range, const std::vector<int>& attribute_ids) const;

template ArrayConstReverseCellIterator<int>*
StorageManager::rbegin<int>(int ad) const;
template ArrayConstReverseCellIterator<int64_t>*
StorageManager::rbegin<int64_t>(int ad) const;
template ArrayConstReverseCellIterator<float>*
StorageManager::rbegin<float>(int ad) const;
template ArrayConstReverseCellIterator<double>*
StorageManager::rbegin<double>(int ad) const;

template ArrayConstReverseCellIterator<int>*
StorageManager::rbegin<int>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<int64_t>*
StorageManager::rbegin<int64_t>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<float>*
StorageManager::rbegin<float>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<double>*
StorageManager::rbegin<double>(
    int ad, const std::vector<int>& attribute_ids) const;

template ArrayConstReverseCellIterator<int>*
StorageManager::rbegin<int>(int ad, const int* range) const;
template ArrayConstReverseCellIterator<int64_t>*
StorageManager::rbegin<int64_t>(int ad, const int64_t* range) const;
template ArrayConstReverseCellIterator<float>*
StorageManager::rbegin<float>(int ad, const float* range) const;
template ArrayConstReverseCellIterator<double>*
StorageManager::rbegin<double>(int ad, const double* range) const;

template ArrayConstReverseCellIterator<int>*
StorageManager::rbegin<int>(
    int ad, const int* range, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<int64_t>*
StorageManager::rbegin<int64_t>(
    int ad, const int64_t* range, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<float>*
StorageManager::rbegin<float>(
    int ad, const float* range, const std::vector<int>& attribute_ids) const;
template ArrayConstReverseCellIterator<double>*
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

