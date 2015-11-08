/**
 * @file   storage_manager.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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

#include "cell.h"
#include "const_cell_iterator.h"
#include "special_values.h"
#include "storage_manager.h"
#include "utils.h"
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <typeinfo>
#include <unistd.h>

// Macro for printing error and warning messages in stderr in VERBOSE mode
#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << "[TileDB::StorageManager] Error: " << x \
                                   << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::StorageManager] Warning: " \
                                     << x \
                                     << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManager::StorageManager() {
  // Initialize the structure that holds the open arrays
  arrays_ = new Array*[SM_OPEN_ARRAYS_MAX]; 
  for(int i=0; i<SM_OPEN_ARRAYS_MAX; ++i)
    arrays_[i] = NULL;

  finalized_ = false;
  created_successfully_ = true;
}

bool StorageManager::created_successfully() const {
  return created_successfully_;
}

int StorageManager::finalize() {
  assert(arrays_ != NULL);

  for(int i=0; i<SM_OPEN_ARRAYS_MAX; ++i) {
    if(arrays_[i] != NULL)
      if(array_close(i))
        return -1;
  }
  delete [] arrays_;

  finalized_ = true;

  return 0;
}

StorageManager::~StorageManager() {
  if(!finalized_) {
    PRINT_WARNING("StorageManager not finalized. Finalizing now");
    finalize();
  }
}

/* ****************************** */
/*         ARRAY FUNCTIONS        */
/* ****************************** */

int StorageManager::array_clear(
    const std::string& workspace,
    const std::string& group,
    const std::string& array_name,
    bool real_paths) {
  // Get real workspace and group paths
  std::string workspace_real, group_real;
  if(real_paths_get(workspace, group, workspace_real, group_real, real_paths)) 
    return -1;

  // Check if the array is defined
  if(!array_is_defined(workspace_real, group_real, array_name, true)) {
    PRINT_ERROR("Array not defined");
    return -1;
  }

  // Calculate the (real) array directory name
  std::string array_dirname = group_real + "/" + array_name;

  // Close the array if it is open 
  OpenArrays::iterator it = open_arrays_.find(array_dirname);
  if(it != open_arrays_.end())
    array_close_forced(it->second);

  // Delete the entire array directory except for the array schema file
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(array_dirname.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open array directory - ") + 
                strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, ARRAY_SCHEMA_FILENAME))
      continue;
    if(is_file(array_dirname + "/" + next_file->d_name, true)) {
      filename = array_dirname + "/" + next_file->d_name;
      if(remove(filename.c_str())) {
        PRINT_ERROR(std::string("Cannot delete file - ") +
                    strerror(errno));
        return -1;
      }
    } else {  // It is a fragment directory
      if(directory_delete(array_dirname + "/" + next_file->d_name, true))
        return -1;
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the array directory - ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::array_close(int ad) {
  // Sanity check
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return -1;
  }

  // Trivial case
  if(arrays_[ad] == NULL)
    return 0;

  // Close array
  open_arrays_.erase(arrays_[ad]->dirname());
  arrays_[ad]->finalize();
  delete arrays_[ad];
  arrays_[ad] = NULL;

  // Success
  return 0;
}

int StorageManager::array_close_forced(int ad) {
  // Sanity check
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return -1;
  }

  // Trivial case
  if(arrays_[ad] == NULL)
    return -1;

  // Close array
  if(arrays_[ad]->close_forced())
    return -1;

  open_arrays_.erase(arrays_[ad]->dirname());
  arrays_[ad]->finalize();
  delete arrays_[ad];
  arrays_[ad] = NULL;

  // Success
  return 0;
}

int StorageManager::array_consolidate(
    const std::string& workspace,
    const std::string& group,
    const std::string& array_name,
    bool real_paths) {
  // Open array
  int ad = array_open(workspace, group, array_name, "c");
  if(ad == -1)
    return -1;

  // Consolidate
  if(arrays_[ad]->consolidate())
    return -1;

  // Clean up
  array_close(ad);

  // Success
  return 0;
}

int StorageManager::array_delete(
    const std::string& workspace,
    const std::string& group,
    const std::string& array_name,
    bool real_paths) {
  // Get real workspace and group paths
  std::string workspace_real, group_real;
  if(real_paths_get(workspace, group, workspace_real, group_real, real_paths))
    return -1;

  // Clear the array (necessary for closing the array and deleting
  // the fragment folders)
  if(array_clear(workspace_real, group_real, array_name, true))
    return -1;

  // Delete the directory (remaining files plus the folder itself)
  std::string array_dirname = group_real + "/" + array_name;
  if(directory_delete(array_dirname, true))
    return -1; 

  // Success
  return 0;
}

bool StorageManager::array_is_defined(
    const std::string& workspace,
    const std::string& group,
    const std::string& array_name,
    bool real_paths) const {
  // Get real workspace path
  std::string workspace_real, group_real;
  if(real_paths)
    workspace_real = workspace;
  else
    workspace_real = real_path(workspace);

  // Check if workspace exists
  if(!workspace_exists(workspace_real))
    return false;

  // Get real group path
  if(real_paths)
    group_real = group;
  else
    group_real = real_path(group, workspace_real,
                           workspace_real, workspace_real);

  // Check if array schema file exists
  std::string filename = group_real + "/" + array_name + "/" +
                         ARRAY_SCHEMA_FILENAME;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

bool StorageManager::array_name_is_valid(const char* array_name) const {
  for(int i=0; array_name[i] != '\0'; ++i) {
    if(!isalnum(array_name[i]) && 
       array_name[i] != '_' && array_name[i] != '-' && array_name[i] != '.')
      return false;
  }

  return true;
}

int StorageManager::array_open(
    const std::string& workspace, 
    const std::string& group, 
    const std::string& array_name, 
    const char* mode,
    bool real_paths) {
  // Get real workspace, group and array paths
  std::string workspace_real, group_real;
  if(real_paths_get(workspace, group, workspace_real, group_real, real_paths))
    return -1;
  std::string array_dirname = group_real + "/" + array_name;

  // Check if the array is defined
  if(!array_is_defined(workspace_real, group_real, array_name, true)) {
    PRINT_ERROR("Undefined array '" + array_name + "'");
    return -1;
  }

  // If the array is already open, return its stored descriptor
  OpenArrays::iterator it = open_arrays_.find(array_dirname);
  if(it != open_arrays_.end())
    return it->second;

  // If in write mode, clear the array if it exists
  if(!strcmp(mode, "w")) 
    if(array_clear(workspace_real, group_real, array_name, true))
      return -1;

  // Get array mode
  Array::Mode array_mode;
  if(!strcmp(mode, "w") || !strcmp(mode, "a")) {
    array_mode = Array::WRITE;
  } else if(!strcmp(mode, "r")) {
    array_mode = Array::READ;
  } else if(!strcmp(mode, "c")) {
    array_mode = Array::CONSOLIDATE;
  } else {
    PRINT_ERROR("Invalid array mode");
    return -1; 
  }

  // Initialize an Array object
  ArraySchema* array_schema;
  if(array_schema_get(workspace_real, group_real, 
                      array_name, array_schema, true))
    return -1;
  Array* array = new Array(array_dirname, array_schema, array_mode);
  if(!array->created_successfully()) {
    array->finalize();
    delete array;
    return -1;
  }

  // Get a new array descriptor
  int ad = -1;
  for(int i=0; i<SM_OPEN_ARRAYS_MAX; ++i) {
    if(arrays_[i] == NULL) {
      ad = i; 
      break;
    }
  }

  // Success
  if(ad != -1) { 
    arrays_[ad] = array;
    open_arrays_[array_dirname] = ad;
    return ad;
  } else { // Error
    PRINT_ERROR("Cannot open array: reached maximum open arrays limit");
    array->finalize();
    delete array; 
    return -1;
  }
}

int StorageManager::array_schema_get(
    const std::string& workspace,
    const std::string& group,
    const std::string& array_name, 
    ArraySchema*& array_schema,
    bool real_paths) const {
  // Get real workspace and group paths
  std::string workspace_real, group_real;
  if(real_paths_get(workspace, group, workspace_real, group_real, real_paths))
    return -1;

  // Check if the array is defined
  if(!array_is_defined(workspace_real, group_real, array_name, true)) {
    PRINT_ERROR("Array not defined");
    return -1;
  }

  // Open array schema file
  std::string filename = group_real + "/" + array_name + "/" + 
                         ARRAY_SCHEMA_FILENAME;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR(std::string( "Cannot open array schema file - ") +
                strerror(errno));
    return -1;
  }

  // The schema to be returned
  array_schema = new ArraySchema();

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  ssize_t buffer_size = st.st_size;
  if(buffer_size == 0) {
    PRINT_ERROR("Array schema file is empty");
    delete array_schema;
    return -1;
  }
  char* buffer = new char[buffer_size];

  // Load array schema
  ssize_t bytes_read = read(fd, buffer, buffer_size);
  if(bytes_read != buffer_size) {
    delete [] buffer;
    delete array_schema;
    if(bytes_read == -1)
      PRINT_ERROR(std::string("Failed to read from array schema file - ") +
                  strerror(errno));
    else
      PRINT_ERROR("Failed to properly read from array schema file");
    return -1;
  } 
  if(array_schema->deserialize(buffer, buffer_size)) {
    delete [] buffer;
    delete array_schema;
    return -1;
  }

  // Clean up
  delete [] buffer;
  if(close(fd)) {
    delete array_schema;
    PRINT_ERROR("Failed to close array schema file");
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::array_schema_get(
    int ad, 
    const ArraySchema*& array_schema) const {
  // Sanity check
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Cannot get array schema - Invalid array descriptor");
    return -1;
  }

  // Check if array is open
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Cannot get array schema - Array not open");
    return -1;
  }

  // Get array schema
  array_schema = arrays_[ad]->array_schema();

  // Success
  return 0;
}

int StorageManager::array_schema_store(
    const std::string& workspace,
    const std::string& group,
    const ArraySchema* array_schema,
    bool real_paths) {
  // Check array schema
  if(array_schema == NULL) {
    PRINT_ERROR("Cannot store array schema - The array schema cannot be empty");
    return -1;
  }

  // Get real workspace and group paths
  std::string workspace_real, group_real;
  if(real_paths_get(workspace, group, workspace_real, group_real, real_paths))
    return -1;

  // Create workspace
  if(workspace_create(workspace_real, true))
    return -1;
 
  // Create group
  if(group_create(workspace_real, group_real, true))
    return -1;

  // Check array name
  const std::string& array_name = array_schema->array_name();
  if(!name_is_valid(array_name.c_str())) { 
    PRINT_ERROR("Cannot store array schema - Invalid array name");
    return -1;
  }

  // Delete array if it exists
  if(array_is_defined(workspace_real, group_real, array_name, true)) 
    if(array_delete(workspace_real, group_real, array_name, true))
      return -1;

  // Create array directory
  std::string array_path = group_real + "/" + array_name;
  if(directory_create(array_path, true)) 
    return -1;
  assert(is_dir(array_path, true));

  // Open array schema file
  std::string filename = array_path + "/" + ARRAY_SCHEMA_FILENAME; 
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Failed to create array schema file - ") + 
                strerror(errno));
    return -1;
  }

  // Serialize array schema
  std::pair<const char*, size_t> ret = array_schema->serialize();
  const char* array_schema_c_str = ret.first;
  ssize_t array_schema_c_strlen = ret.second; 

  // Store the array schema
  ssize_t bytes_written = write(fd, array_schema_c_str, array_schema_c_strlen);
  if(bytes_written != array_schema_c_strlen) {
    if(bytes_written == -1)
      PRINT_ERROR(std::string("Failed to write the array schema to file - ") +
                  strerror(errno));
    else
      PRINT_ERROR("Failed to properly write the array schema to file");
    delete [] array_schema_c_str;
    return -1;
  }

  // Clean up
  delete [] array_schema_c_str;
  if(close(fd)) {
    PRINT_ERROR("Failed to close the array schema file"); 
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::group_create(
    const std::string& workspace,
    const std::string& group,
    bool real_paths) const {
  // Get real workspace and group paths
  std::string workspace_real, group_real;
  if(real_paths_get(workspace, group, workspace_real, group_real, real_paths))
    return -1;

  if(!workspace_exists(workspace_real, true)) {
    PRINT_ERROR(std::string("Invalid workspace '") +
                workspace_real + "'"); 
    return -1;
  }

  // Do nothing if the array exists already
  if(group_exists(workspace_real, group_real, true)) 
    return 0;

  // Return with error if the group directory is inside an array directory
  if(path_inside_array_directory(group_real, true)) { 
    PRINT_ERROR("Cannot create a group inside an array directory");
    return -1;
  }

  // Create group directory path 
  if(path_create(group_real)) { 
    PRINT_ERROR(std::string("Cannot create group '") +
                group_real + "' - " + 
                strerror(errno));
    return -1;
  }
  assert(is_dir(group_real, true));

  // Create a "group file" for each group folder along the path from the
  // workspace path to the full group path
  if(group_files_create(workspace_real, group_real))
    return -1;

  // Success
  return 0;
}

bool StorageManager::group_exists(
    const std::string& workspace,
    const std::string& group,
    bool real_paths) const {
  // Get real workspace path
  std::string workspace_real, group_real;
  if(real_paths)
    workspace_real = workspace;
  else
    workspace_real = real_path(workspace);
  if(workspace_real == "") 
    return false;

  // Get real group path
  if(real_paths)
    group_real = group;
  else
    group_real = real_path(group, workspace_real,
                           workspace_real, workspace_real);
  if(group_real == "") 
    return false;

  // Check workspace
  if(!workspace_exists(workspace_real, true)) 
    return false;

  // Check group
  if(is_dir(group_real, true) && 
     is_file(group_real + "/" + SM_GROUP_FILENAME, true)) 
    return true;
  else
    return false;
}

int StorageManager::real_paths_get(
    const std::string& workspace,
    const std::string& group,
    std::string& workspace_real,
    std::string& group_real,
    bool real_paths) const {
  // Get real workspace path
  if(real_paths)
    workspace_real = workspace;
  else
    workspace_real = real_path(workspace);

  if(workspace_real == "") {
    PRINT_ERROR("Invalid workspace");
    return -1;
  }

  // Get real group path
  if(real_paths)
    group_real = group;
  else
    group_real = real_path(group, workspace_real,
                           workspace_real, workspace_real);

  if(group_real == "") {
    PRINT_ERROR("Invalid group");
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::workspace_create(
    const std::string& workspace, 
    bool real_path) const {
  // Get real path
  std::string workspace_real;
  if(real_path)
    workspace_real = workspace;
  else
    workspace_real = ::real_path(workspace);

  if(workspace_real == "") {
    PRINT_ERROR(std::string("Invalid workspace '") +
                workspace + "'"); 
    return -1;
  }

  // Check if workspace exists
  if(workspace_exists(workspace_real, true))
    return 0;

  // Create directory 
  if(path_create(workspace_real)) { 
    PRINT_ERROR(std::string("Cannot create workspace '") +
                workspace_real + "' - " + 
                strerror(errno));
    return -1;
  }
  assert(is_dir(workspace_real, true));

  // Create (empty) workspace file 
  std::string filename = workspace_real + "/" + SM_WORKSPACE_FILENAME;
  // ----- open -----
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Failed to create workspace file: ") +
                strerror(errno));
    return -1;
  }
  // ----- close -----
  if(close(fd)) {
    PRINT_ERROR(std::string("Failed to close workspace file: ") +
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

bool StorageManager::workspace_exists(
    const std::string& workspace,
    bool real_path) const {
  // Get real path
  std::string workspace_real;
  if(real_path)
    workspace_real = workspace;
  else
    workspace_real = ::real_path(workspace);

  // Check existence
  if(is_dir(workspace_real, true) && 
     is_file(workspace_real + "/" + SM_WORKSPACE_FILENAME, true)) 
    return true;
  else
    return false;
}

/* ****************************** */
/*         ARRAY FUNCTIONS        */
/* ****************************** */

template<class T>
int StorageManager::cell_write(int ad, const void* cell) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return -1;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return -1;
  }
  if(arrays_[ad]->mode() != Array::WRITE) {
    PRINT_ERROR("Invalid array mode");
    return -1;
  }

  // Write cell
  return arrays_[ad]->cell_write<T>(cell);
}

template<class T>
int StorageManager::cell_write_sorted(int ad, const void* cell) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return -1;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return -1;
  }
  if(arrays_[ad]->mode() != Array::WRITE) {
    PRINT_ERROR("Invalid array mode");
    return -1;
  }

  return arrays_[ad]->cell_write_sorted<T>(cell); 
}

int StorageManager::cells_write(
    int ad, 
    const void* cells, 
    size_t cells_size) const {
  // For easy reference
  const std::type_info* coords_type = 
      arrays_[ad]->array_schema()->coords_type();

  if(coords_type == &typeid(int)) {
    return cells_write<int>(ad, cells, cells_size);
  } else if(coords_type == &typeid(int64_t)) {
    return cells_write<int64_t>(ad, cells, cells_size);
  } else if(coords_type == &typeid(float)) {
    return cells_write<float>(ad, cells, cells_size);
  } else if(coords_type == &typeid(double)) {
    return cells_write<double>(ad, cells, cells_size);
  } else {
    PRINT_ERROR("Invalid array coordinates type");
    return -1;
  }
}

template<class T>
int StorageManager::cells_write(
    int ad, 
    const void* cells, 
    size_t cells_size) const {
  // Trivial case
  if(cells == NULL)
    return 0;

  ConstCellIterator cell_it(cells, cells_size, arrays_[ad]->array_schema());

  for(; !cell_it.end(); ++cell_it) 
    if(cell_write<T>(ad, *cell_it))
      return -1;

  // Success
  return 0;
}

int StorageManager::cells_write_sorted(
    int ad, 
    const void* cells, 
    size_t cells_size) const {
  // For easy reference
  const std::type_info* coords_type = 
      arrays_[ad]->array_schema()->coords_type();

  if(coords_type == &typeid(int)) {
    return cells_write_sorted<int>(ad, cells, cells_size);
  } else if(coords_type == &typeid(int64_t)) {
    return cells_write_sorted<int64_t>(ad, cells, cells_size);
  } else if(coords_type == &typeid(float)) {
    return cells_write_sorted<float>(ad, cells, cells_size);
  } else if(coords_type == &typeid(double)) {
    return cells_write_sorted<double>(ad, cells, cells_size);
  } else {
    PRINT_ERROR("Invalid array coordinates type");
    return -1;
  }
}

template<class T>
int StorageManager::cells_write_sorted(
    int ad, const void* cells, size_t cells_size) const {
  // Trivial case
  if(cells == NULL)
    return 0;

  ConstCellIterator cell_it(cells, cells_size, arrays_[ad]->array_schema());

  for(; !cell_it.end(); ++cell_it) 
    if(cell_write_sorted<T>(ad, *cell_it))
      return -1;

  // Success
  return 0;
}

/* ****************************** */
/*          CELL ITERATORS        */
/* ****************************** */

template<class T>
ArrayConstCellIterator<T>* StorageManager::begin(
    int ad) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstCellIterator<T>* cell_it = 
      new ArrayConstCellIterator<T>(arrays_[ad]);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstCellIterator<T>* StorageManager::begin(
    int ad, 
    const std::vector<int>& attribute_ids) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstCellIterator<T>* cell_it = 
      new ArrayConstCellIterator<T>(arrays_[ad], attribute_ids);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstCellIterator<T>* StorageManager::begin(
    int ad, 
    const T* range) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstCellIterator<T>* cell_it = 
      new ArrayConstCellIterator<T>(arrays_[ad], range);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstCellIterator<T>* StorageManager::begin(
    int ad, 
    const T* range, 
    const std::vector<int>& attribute_ids) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstCellIterator<T>* cell_it = 
      new ArrayConstCellIterator<T>(arrays_[ad], range, attribute_ids);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstDenseCellIterator<T>* StorageManager::begin_dense(
    int ad) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstDenseCellIterator<T>* cell_it = 
      new ArrayConstDenseCellIterator<T>(arrays_[ad]);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstDenseCellIterator<T>* StorageManager::begin_dense(
    int ad, 
    const std::vector<int>& attribute_ids) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstDenseCellIterator<T>* cell_it = 
      new ArrayConstDenseCellIterator<T>(arrays_[ad], attribute_ids);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstDenseCellIterator<T>* StorageManager::begin_dense(
    int ad, 
    const T* range) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstDenseCellIterator<T>* cell_it = 
      new ArrayConstDenseCellIterator<T>(arrays_[ad], range);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstDenseCellIterator<T>* StorageManager::begin_dense(
    int ad, 
    const T* range, 
    const std::vector<int>& attribute_ids) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstDenseCellIterator<T>* cell_it = 
      new ArrayConstDenseCellIterator<T>(arrays_[ad], range, attribute_ids);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstReverseCellIterator<T>* StorageManager::rbegin(
    int ad) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstReverseCellIterator<T>* cell_it = 
      new ArrayConstReverseCellIterator<T>(arrays_[ad]);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstReverseCellIterator<T>* StorageManager::rbegin(
    int ad, 
    const std::vector<int>& attribute_ids) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstReverseCellIterator<T>* cell_it = 
      new ArrayConstReverseCellIterator<T>(arrays_[ad], attribute_ids);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstReverseCellIterator<T>* StorageManager::rbegin(
    int ad, 
    const T* multi_D_obj, 
    bool range) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstReverseCellIterator<T>* cell_it = 
      new ArrayConstReverseCellIterator<T>(arrays_[ad], multi_D_obj, range);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

template<class T>
ArrayConstReverseCellIterator<T>* StorageManager::rbegin(
    int ad, 
    const T* multi_D_obj, 
    const std::vector<int>& attribute_ids,
    bool range) const {
  // Checks
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return NULL;
  }
  if(arrays_[ad] == NULL) {
    PRINT_ERROR("Array not open");
    return NULL;
  }
  if(arrays_[ad]->mode() != Array::READ) {
    PRINT_ERROR("Invalid array mode");
    return NULL;
  }

  // Create iterator
  ArrayConstReverseCellIterator<T>* cell_it = 
      new ArrayConstReverseCellIterator<T>(
              arrays_[ad], multi_D_obj, attribute_ids, range);

  // Return iterator, performing a proper check first
  if(cell_it->created_successfully()) {
    return cell_it;
  } else {
    cell_it->finalize();
    delete cell_it;
    return NULL;
  }
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int StorageManager::group_files_create(
    const std::string& workspace,
    const std::string& group) const {
  // Function works only with absolute paths
  assert(workspace != "" && workspace[0] == '/' && workspace.back() != '/'); 
  assert(group != "" && group[0] == '/' && group.back() != '/'); 

  // Auxiliary variables
  char path_tmp[PATH_MAX];
  char* p = NULL;

  // Initialzation
  snprintf(path_tmp, sizeof(path_tmp), "%s", group.c_str());

  // Create the directories in the path one by one 
  for(p = path_tmp + workspace.size(); *p; ++p) {
    if(*p == '/') {
      // Cut path
      *p = '\0';

      // Create group file if it does not exist
      std::string filename = std::string(path_tmp) + "/" + SM_GROUP_FILENAME;
      if(!is_file(filename.c_str(), true)) { 
        // ----- open -----
        int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
        if(fd == -1) {
          PRINT_ERROR(std::string("Failed to create group file - ") +
                      strerror(errno));
          return -1;
        }
        // ----- close -----
        if(close(fd)) {
          PRINT_ERROR(std::string("Failed to close group file - ") +
                      strerror(errno));
          return -1;
        }
      }

      // Stitch path back
      *p = '/';
    }
  }

  // Create last group file 
  std::string filename = group + "/" + SM_GROUP_FILENAME;
  if(!is_file(filename.c_str(), true)) { 
    // ----- open -----
    int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
    if(fd == -1) {
      PRINT_ERROR(std::string("Failed to create group file - ") +
                  strerror(errno));
      return -1;
    }
    // ----- close -----
    if(close(fd)) {
      PRINT_ERROR(std::string("Failed to close group file - ") +
                  strerror(errno));
      return -1;
    }
  }

  return 0;
}

bool StorageManager::path_inside_array_directory(
    const std::string& path, 
    bool real_path) const {
  // Get real path
  std::string path_real;
  if(real_path)
    path_real = path;
  else
    path_real = ::real_path(path);

  if(path_real == "") 
    return false;

  // Initialization
  char path_tmp[PATH_MAX];
  char* p = NULL;
  size_t len;
  std::string filename;
  int fd;
  snprintf(path_tmp, sizeof(path_tmp), "%s", path.c_str());
  len = strlen(path_tmp);
  if(path_tmp[len-1] == '/')
    path_tmp[len-1] = '\0';

  // Check directories on the path one by one
  for(p = path_tmp + 1; *p; ++p) {
    if(*p == '/') {
      // Cut path
      *p = '\0';

      // Check for array schema file
      filename = std::string(path_tmp) + "/" +  ARRAY_SCHEMA_FILENAME;
      fd = open(filename.c_str(), O_RDONLY);
      if(fd != -1) { // File exists - path is an array directory
        close(fd);
        return true;
      } 

      // Stitch path back
      *p = '/';
    }
  }

  // Handle last path
  filename = std::string(path_tmp) + "/" +  ARRAY_SCHEMA_FILENAME;
  fd = open(filename.c_str(), O_RDONLY);
  if(fd != -1) { // File exists - path is an array directory
    close(fd);
    return true;
  }

  return false;
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

template ArrayConstDenseCellIterator<int>*
StorageManager::begin_dense<int>(int ad) const;
template ArrayConstDenseCellIterator<int64_t>*
StorageManager::begin_dense<int64_t>(int ad) const;
template ArrayConstDenseCellIterator<float>*
StorageManager::begin_dense<float>(int ad) const;
template ArrayConstDenseCellIterator<double>*
StorageManager::begin_dense<double>(int ad) const;

template ArrayConstDenseCellIterator<int>*
StorageManager::begin_dense<int>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstDenseCellIterator<int64_t>*
StorageManager::begin_dense<int64_t>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstDenseCellIterator<float>*
StorageManager::begin_dense<float>(
    int ad, const std::vector<int>& attribute_ids) const;
template ArrayConstDenseCellIterator<double>*
StorageManager::begin_dense<double>(
    int ad, const std::vector<int>& attribute_ids) const;

template ArrayConstDenseCellIterator<int>*
StorageManager::begin_dense<int>(int ad, const int* range) const;
template ArrayConstDenseCellIterator<int64_t>*
StorageManager::begin_dense<int64_t>(int ad, const int64_t* range) const;
template ArrayConstDenseCellIterator<float>*
StorageManager::begin_dense<float>(int ad, const float* range) const;
template ArrayConstDenseCellIterator<double>*
StorageManager::begin_dense<double>(int ad, const double* range) const;

template ArrayConstDenseCellIterator<int>*
StorageManager::begin_dense<int>(
    int ad, const int* range, const std::vector<int>& attribute_ids) const;
template ArrayConstDenseCellIterator<int64_t>*
StorageManager::begin_dense<int64_t>(
    int ad, const int64_t* range, const std::vector<int>& attribute_ids) const;
template ArrayConstDenseCellIterator<float>*
StorageManager::begin_dense<float>(
    int ad, const float* range, const std::vector<int>& attribute_ids) const;
template ArrayConstDenseCellIterator<double>*
StorageManager::begin_dense<double>(
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
StorageManager::rbegin<int>(
    int ad, const int* multi_D_obj, bool is_range) const;
template ArrayConstReverseCellIterator<int64_t>*
StorageManager::rbegin<int64_t>(
    int ad, const int64_t* multi_D_obj, bool is_range) const;
template ArrayConstReverseCellIterator<float>*
StorageManager::rbegin<float>(
    int ad, const float* multi_D_obj, bool is_range) const;
template ArrayConstReverseCellIterator<double>*
StorageManager::rbegin<double>(
    int ad, const double* multi_D_obj, bool is_range) const;

template ArrayConstReverseCellIterator<int>*
StorageManager::rbegin<int>(
    int ad, const int* multi_D_obj, 
    const std::vector<int>& attribute_ids,
    bool is_range) const;
template ArrayConstReverseCellIterator<int64_t>*
StorageManager::rbegin<int64_t>(
    int ad, const int64_t* multi_D_obj, 
    const std::vector<int>& attribute_ids,
    bool is_range) const;
template ArrayConstReverseCellIterator<float>*
StorageManager::rbegin<float>(
    int ad, const float* multi_D_obj, 
    const std::vector<int>& attribute_ids,
    bool is_range) const;
template ArrayConstReverseCellIterator<double>*
StorageManager::rbegin<double>(
    int ad, const double* multi_D_obj, 
    const std::vector<int>& attribute_ids,
    bool is_range) const;

template int StorageManager::cell_write<int>(
    int ad, const void* cell) const;
template int StorageManager::cell_write<int64_t>(
    int ad, const void* cell) const;
template int StorageManager::cell_write<float>(
    int ad, const void* cell) const;
template int StorageManager::cell_write<double>(
    int ad, const void* cell) const;

template int StorageManager::cell_write_sorted<int>(
    int ad, const void* cell) const;
template int StorageManager::cell_write_sorted<int64_t>(
    int ad, const void* cell) const;
template int StorageManager::cell_write_sorted<float>(
    int ad, const void* cell) const;
template int StorageManager::cell_write_sorted<double>(
    int ad, const void* cell) const;

