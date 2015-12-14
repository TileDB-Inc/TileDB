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
#include <algorithm>
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <limits.h>
#include <openssl/md5.h>
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

  // Initialize the structure that holds the open metadata
  metadata_ = new Array*[SM_OPEN_METADATA_MAX]; 
  for(int i=0; i<SM_OPEN_METADATA_MAX; ++i)
    metadata_[i] = NULL;

  finalized_ = false;
  created_successfully_ = true;
}

bool StorageManager::created_successfully() const {
  return created_successfully_;
}

int StorageManager::finalize() {
  assert(arrays_ != NULL);
  assert(metadata_ != NULL);

  // Clean up open arrays
  for(int i=0; i<SM_OPEN_ARRAYS_MAX; ++i) {
    if(arrays_[i] != NULL)
      if(array_close(i))
        return -1;
  }
  delete [] arrays_;

  // Clean up open metadata
  for(int i=0; i<SM_OPEN_METADATA_MAX; ++i) {
    if(metadata_[i] != NULL)
      if(metadata_close(i))
        return -1;
  }
  delete [] metadata_;

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
    filename = array_dirname + "/" + next_file->d_name;
    if(is_file(filename, true)) { // File
      if(remove(filename.c_str())) {
        PRINT_ERROR(std::string("Cannot delete file - ") +
                    strerror(errno));
        return -1;
      }
    } else if(metadata_exists(filename, true)) { // Metadata
      if(metadata_delete(filename, true))
        return -1;
    } else {  // Fragment
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
  open_arrays_.erase(arrays_[ad]->array_name());
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

  open_arrays_.erase(arrays_[ad]->array_name());
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

int StorageManager::array_consolidate(
    const std::string& array) {
  // Open metadata
  int ad = array_open(array, "c");
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

int StorageManager::metadata_consolidate(
    const std::string& metadata) {
  // Open metadata
  int md = metadata_open(metadata, "c");
  if(md == -1)
    return -1;

  // Consolidate
  if(metadata_[md]->consolidate())
    return -1;

  // Clean up
  metadata_close(md);

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

bool StorageManager::array_exists(
    const std::string& array_name,
    bool real_path) const {
  // Get real array directory
  std::string array_name_real;
  if(real_path)
    array_name_real = array_name;
  else
    array_name_real = ::real_path(array_name);

  // Check if array schema file exists
  std::string filename = array_name_real + "/" +
                         ARRAY_SCHEMA_FILENAME;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
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
    const ArraySchema* array_schema) {
  // Check array schema
  if(array_schema == NULL) {
    PRINT_ERROR("Cannot store array schema; "
                "the array schema cannot be empty");
    return -1;
  }

  // Get real array directory name
  std::string array_dirname = array_schema->array_name();
  std::string array_group_dirname = array_schema->group_name();

  // Check if directory already exists
  if(is_dir(array_dirname)) {
    PRINT_ERROR(std::string("Directory '") + array_dirname + 
                "' already exists");
    return -1;
  }

  // Check if the array directory are contained in a workspaceo or group
  if(!group_exists(array_group_dirname, true) && 
     !workspace_exists(array_group_dirname, true)) {
    PRINT_ERROR(std::string("Folder '") + array_group_dirname + "' must "
                "be either a workspace or a group");
    return -1;
  }

  // Create array directory
  if(directory_create(array_dirname, true)) 
    return -1;

  // Open metadata schema file
  std::string filename = array_dirname + "/" + 
                         ARRAY_SCHEMA_FILENAME; 
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
      PRINT_ERROR(std::string("Failed to write the array schema to file; ") +
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
  if(workspace_create_2(workspace_real, true))
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

int StorageManager::group_create(const std::string& group) const {
  // Get real group path
  std::string group_real = real_path(group); 

  // Check if the group is inside a workspace or another group
  std::string group_real_parent_folder = parent_folder(group_real);
  if(!workspace_exists(group_real_parent_folder, true) &&
     !group_exists(group_real_parent_folder, true)) {
    PRINT_ERROR("The group must be contained in a worksapce "
                "or another group");
    return -1;
  }

  // Check if directory already exists
  if(is_dir(group_real)) {
    PRINT_ERROR(std::string("Directory '") + group_real + "' already exists");
    return -1;
  }

  // Create group directory path 
  if(directory_create(group_real)) { 
    PRINT_ERROR(std::string("Cannot create group '") +
                group_real + "' - " + strerror(errno));
    return -1;
  }

  // Create a "group file" 
  std::string filename = group_real + "/" + SM_GROUP_FILENAME;
  // ----- open -----
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Failed to create group file; ") +
                strerror(errno));
    return -1;
  }
  // ----- close -----
  if(close(fd)) {
    PRINT_ERROR(std::string("Failed to close group file; ") +
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::group_clear(const std::string& group) {
  // Get real group path
  std::string group_real = real_path(group); 

  // Check if group exists
  if(!group_exists(group_real, true)) {
    PRINT_ERROR(std::string("Group '") + group_real + "' does not exist");
    return -1;
  }

  // Do not clear if it is a workspace
  if(workspace_exists(group_real, true)) {
    PRINT_ERROR(std::string("Group '") + group_real + "' is also a workspace");
    return -1;
  }

  // Delete all groups and arrays inside the group directory
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(group_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open group directory '") + 
                group_real + "'; " + strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, SM_GROUP_FILENAME))
      continue;
    filename = group_real + "/" + next_file->d_name;
    if(group_exists(filename, true)) { // Group
      if(group_delete(filename, true))
        return -1;
    } else if(metadata_exists(filename, true)) { // Metadata
      if(metadata_delete(filename, true))
        return -1;
    } else if(array_exists(filename, true)){  // Array
      if(array_delete(filename, true))
        return -1;
    } else { // Non TileDB related
      PRINT_ERROR(std::string("Cannot delete non TileDB related element '") +
                  filename + "'");
      return -1;
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the group directory; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::group_delete(
    const std::string& group,
    bool real_path) {
  // Get real group path
  std::string group_real;
  if(real_path)
    group_real = group;
  else
    group_real = ::real_path(group); 

  // Check if group exists
  if(!group_exists(group_real, true)) {
    PRINT_ERROR(std::string("Group '") + group_real + "' does not exist");
    return -1;
  }

  // Do not delete if it is a workspace
  if(workspace_exists(group_real, true)) {
    PRINT_ERROR(std::string("Group '") + group_real + "' is also a workspace");
    return -1;
  }

  // Delete all groups and arrays inside the group directory
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(group_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open group directory '") + 
                group_real + "'; " + strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, SM_GROUP_FILENAME))
      continue;
    filename = group_real + "/" + next_file->d_name;
    if(group_exists(filename, true)) { // Group
      if(group_delete(filename, true))
        return -1;
    } else if(metadata_exists(filename, true)) { // Metadata
      if(metadata_delete(filename, true))
        return -1;
    } else if(array_exists(filename, true)){  // Array
      if(array_delete(filename, true))
        return -1;
    } else { // Non TileDB related
      PRINT_ERROR(std::string("Cannot delete non TileDB related element '") +
                  filename + "'");
      return -1;
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the group directory; ") + 
                strerror(errno));
    return -1;
  }

  // Delete the group directory
  if(directory_delete(group_real))
    return -1;

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

int StorageManager::master_catalog_move(
    const std::string& old_master_catalog,
    const std::string& new_master_catalog,
    bool real_paths) {
  // Get real master catalog directory name
  std::string old_master_catalog_real, new_master_catalog_real;
  if(real_paths) {
    old_master_catalog_real = old_master_catalog;
    new_master_catalog_real = new_master_catalog;
  } else {
    old_master_catalog_real = real_path(old_master_catalog);
    new_master_catalog_real = real_path(new_master_catalog);
  }

  // Check if old master catalog exists
  if(!master_catalog_exists(old_master_catalog_real, true)) {
    PRINT_ERROR(std::string("Master catalog '") + 
                old_master_catalog + "' does not exist");
    return -1;
  }

  // Make sure that the new master catalog is not an existing directory
  if(is_dir(new_master_catalog_real)) {
    PRINT_ERROR(std::string("Directory '") + new_master_catalog_real + "' already exists");
    return -1;
  }

  // Rename
  if(rename(old_master_catalog_real.c_str(), new_master_catalog_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move master catalog; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::group_move(
    const std::string& old_group,
    const std::string& new_group,
    bool real_paths) {
  // Get real group directory names
  std::string old_group_real, new_group_real;
  if(real_paths) {
    old_group_real = old_group;
    new_group_real = new_group;
  } else {
    old_group_real = real_path(old_group);
    new_group_real = real_path(new_group);
  }

  // Check if the old group is also a workspace
  if(workspace_exists(old_group_real, true)) {
    PRINT_ERROR(std::string("Group '") + old_group_real + 
                "' is also a workspace");
    return -1;
  }

  // Check if the old group exists
  if(!group_exists(old_group_real, true)) {
    PRINT_ERROR(std::string("Group '") + old_group_real + 
                "' does not exist");
    return -1;
  }

  // Make sure that the new group is not an existing directory
  if(is_dir(new_group_real)) {
    PRINT_ERROR(std::string("Directory '") + new_group_real + "' already exists");
    return -1;
  }

  // Check if the new group is inside a workspace or group
  std::string new_group_parent_folder = parent_folder(new_group_real);

  if(!group_exists(new_group_parent_folder, true) && 
     !workspace_exists(new_group_parent_folder, true)) {
    PRINT_ERROR(std::string("Folder '") + new_group_parent_folder + "' must "
                "be either a workspace or a group");
    return -1;
  }

  // Rename
  if(rename(old_group_real.c_str(), new_group_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move group; ") + 
                strerror(errno));
    return -1;
  }

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

bool StorageManager::group_exists(
    const std::string& group,
    bool real_path) const {
  // Get real group path
  std::string group_real;
  if(real_path)
    group_real = group;
  else
    group_real = ::real_path(group);
  if(group_real == "") 
    return false;

  // Check group
  if(is_dir(group_real, true) && 
     is_file(group_real + "/" + SM_GROUP_FILENAME, true)) 
    return true;
  else
    return false;
}

int StorageManager::array_move(
    const std::string& old_array,
    const std::string& new_array,
    bool real_paths) {
  // Get real array directory name
  std::string old_array_real, new_array_real;
  if(real_paths) {
    old_array_real = old_array;
    new_array_real = new_array;
  } else {
    old_array_real = real_path(old_array);
    new_array_real = real_path(new_array);
  }

  // Check if the old array exists
  if(!array_exists(old_array_real, true)) {
    PRINT_ERROR(std::string("Array '") + old_array_real + 
                "' does not exist");
    return -1;
  }

  // Make sure that the new array is not an existing directory
  if(is_dir(new_array_real)) {
    PRINT_ERROR(std::string("Directory '") + new_array_real + "' already exists");
    return -1;
  }

  // Check if the new array are inside a workspace or group
  std::string new_array_parent_folder = parent_folder(new_array_real);

  if(!group_exists(new_array_parent_folder, true) && 
     !workspace_exists(new_array_parent_folder, true)) {
    PRINT_ERROR(std::string("Folder '") + new_array_parent_folder + "' must "
                "be either a workspace or a group");
    return -1;
  }

  // Close the array if it is open 
  OpenArrays::iterator it = open_arrays_.find(old_array_real);
  if(it != open_arrays_.end())
    array_close(it->second);

  // Rename
  if(rename(old_array_real.c_str(), new_array_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move array; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::metadata_move(
    const std::string& old_metadata,
    const std::string& new_metadata,
    bool real_paths) {
  // Get real metadata directory name
  std::string old_metadata_real, new_metadata_real;
  if(real_paths) {
    old_metadata_real = old_metadata;
    new_metadata_real = new_metadata;
  } else {
    old_metadata_real = real_path(old_metadata);
    new_metadata_real = real_path(new_metadata);
  }

  // Check if the old metadata exists
  if(!metadata_exists(old_metadata_real, true)) {
    PRINT_ERROR(std::string("Metadata '") + old_metadata_real + 
                "' do not exist");
    return -1;
  }

  // Make sure that the new metadata is not an existing directory
  if(is_dir(new_metadata_real)) {
    PRINT_ERROR(std::string("Directory '") + new_metadata_real + "' already exists");
    return -1;
  }

  // Check if the new metadata are inside a workspace, group or array
  std::string new_metadata_parent_folder = parent_folder(new_metadata_real);

  if(!group_exists(new_metadata_parent_folder, true) && 
     !workspace_exists(new_metadata_parent_folder, true) &&
     !array_exists(new_metadata_parent_folder, true)) {
    PRINT_ERROR(std::string("Folder '") + new_metadata_parent_folder + "' must "
                "be either a workspace or a group or an array");
    return -1;
  }

  // Close the array if it is open 
  OpenMetadata::iterator it = open_metadata_.find(old_metadata_real);
  if(it != open_metadata_.end())
    metadata_close(it->second);

  // Rename
  if(rename(old_metadata_real.c_str(), new_metadata_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move metadata; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::array_clear(
    const std::string& array_name,
    bool real_path) {
  // Get real array directory name
  std::string array_name_real;
  if(real_path)
    array_name_real = array_name;
  else
    array_name_real = ::real_path(array_name);

  // Check if the array exists
  if(!array_exists(array_name_real, true)) {
    PRINT_ERROR(std::string("Array '") + array_name_real + 
                "' does not exist");
    return -1;
  }

  // Close the array if it is open 
  OpenArrays::iterator it = open_arrays_.find(array_name_real);
  if(it != open_arrays_.end())
    array_close_forced(it->second);

  // Delete the entire array directory except for the array schema file
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(array_name_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open array directory; ") + 
                strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, ARRAY_SCHEMA_FILENAME))
      continue;
    filename = array_name_real + "/" + next_file->d_name;
    if(is_file(filename, true)) {
      if(remove(filename.c_str())) {
        PRINT_ERROR(std::string("Cannot delete array file; ") +
                    strerror(errno));
        return -1;
      }
    } else if(metadata_exists(filename)) { // It is metadata
      metadata_delete(filename);
    } else {  // It is a fragment directory
      if(directory_delete(filename, true))
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

int StorageManager::master_catalog_clear(
    const std::string& master_catalog,
    bool real_path) {
  // Get real master catalog directory name
  std::string master_catalog_real;
  if(real_path)
    master_catalog_real = master_catalog;
  else
    master_catalog_real = ::real_path(master_catalog);

  // Check if the master_catalog exists
  if(!master_catalog_exists(master_catalog_real, true)) {
    PRINT_ERROR(std::string("Master catalog '") + master_catalog_real + 
                "' does not exist");
    return -1;
  }

  // Clear contents
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(master_catalog_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open master catalog directory; ") + 
                strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, TILEDB_SM_MASTER_CATALOG_FILENAME) ||
       !strcmp(next_file->d_name, TILEDB_SM_METADATA_SCHEMA_FILENAME))
      continue;
    filename = master_catalog_real + "/" + next_file->d_name;
    if(is_file(filename, true)) {
      if(remove(filename.c_str())) {
        PRINT_ERROR(std::string("Cannot delete master catalog file; ") +
                    strerror(errno));
        return -1;
      }
    } else {  // It is a fragment directory
      if(directory_delete(filename, true))
        return -1;
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the master catalog directory - ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::metadata_clear(
    const std::string& metadata_name,
    bool real_path) {
  // Get real metadata directory name
  std::string metadata_name_real;
  if(real_path)
    metadata_name_real = metadata_name;
  else
    metadata_name_real = ::real_path(metadata_name);

  // Check if the metadata exists
  if(!metadata_exists(metadata_name_real, true)) {
    PRINT_ERROR(std::string("Metadata '") + metadata_name_real + 
                "' do not exist");
    return -1;
  }

  // Close the array if it is open 
  OpenMetadata::iterator it = open_metadata_.find(metadata_name_real);
  if(it != open_metadata_.end())
    metadata_close_forced(it->second);

  // Delete the entire array directory except for the array schema file
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(metadata_name_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open metadata directory; ") + 
                strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, TILEDB_SM_METADATA_SCHEMA_FILENAME))
      continue;
    filename = metadata_name_real + "/" + next_file->d_name;
    if(is_file(filename, true)) {
      if(remove(filename.c_str())) {
        PRINT_ERROR(std::string("Cannot delete metadata file; ") +
                    strerror(errno));
        return -1;
      }
    } else {  // It is a fragment directory
      if(directory_delete(metadata_name_real + "/" + next_file->d_name, true))
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

int StorageManager::metadata_close(int md) {
  // Sanity check
  if(md < 0 || md >= SM_OPEN_METADATA_MAX) {
    PRINT_ERROR("Invalid metadata descriptor");
    return -1;
  }

  // Trivial case
  if(metadata_[md] == NULL)
    return 0;

  // Close array
  open_metadata_.erase(metadata_[md]->array_name());
  metadata_[md]->finalize();
  delete metadata_[md];
  metadata_[md] = NULL;

  // Success
  return 0;
}

int StorageManager::metadata_close_forced(int md) {
  // Sanity check
  if(md < 0 || md >= SM_OPEN_METADATA_MAX) {
    PRINT_ERROR("Invalid metadata descriptor");
    return -1;
  }

  // Trivial case
  if(metadata_[md] == NULL)
    return -1;

  // Close metadata
  if(metadata_[md]->close_forced())
    return -1;

  open_metadata_.erase(metadata_[md]->array_name());
  metadata_[md]->finalize();
  delete metadata_[md];
  metadata_[md] = NULL;

  // Success
  return 0;
}

int StorageManager::array_delete(
    const std::string& array_name,
    bool real_path) {
  // Get real metadata directory name
  std::string array_name_real;
  if(real_path)
    array_name_real = array_name;
  else
    array_name_real = ::real_path(array_name);

  // Clear the array
  if(array_clear(array_name_real, true))
    return -1;

  // Delete array directory
  if(directory_delete(array_name_real, true))
    return -1; 

  // Success
  return 0;
}

int StorageManager::master_catalog_delete(
    const std::string& master_catalog,
    bool real_path) {
  return metadata_delete(master_catalog, real_path);
}

int StorageManager::metadata_delete(
    const std::string& metadata_name,
    bool real_path) {
  // Get real metadata directory name
  std::string metadata_name_real;
  if(real_path)
    metadata_name_real = metadata_name;
  else
    metadata_name_real = ::real_path(metadata_name);

  // Clear the metadata
  if(metadata_clear(metadata_name_real, true))
    return -1;

  // Delete metadata directory
  if(directory_delete(metadata_name_real, true))
    return -1; 

  // Success
  return 0;
}

bool StorageManager::metadata_exists(
    const std::string& metadata_name,
    bool real_path) const {
  // Get real metadata directory name
  std::string metadata_name_real;
  if(real_path)
    metadata_name_real = metadata_name;
  else
    metadata_name_real = ::real_path(metadata_name);

  // Check if metadata schema file exists
  std::string filename = metadata_name_real + "/" +
                         TILEDB_SM_METADATA_SCHEMA_FILENAME;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

bool StorageManager::is_array_schema(
    const std::string& filename,
    bool real_path) const {
  // Get real name of file
  std::string filename_real;
  if(real_path)
    filename_real = filename;
  else
    filename_real = ::real_path(filename);

  return ends_with(filename_real, ARRAY_SCHEMA_FILENAME);
}

bool StorageManager::is_metadata_schema(
    const std::string& filename,
    bool real_path) const {
  // Get real name of file
  std::string filename_real;
  if(real_path)
    filename_real = filename;
  else
    filename_real = ::real_path(filename);

  return ends_with(filename_real, TILEDB_SM_METADATA_SCHEMA_FILENAME);
}

int StorageManager::metadata_schema_print(
    const std::string& filename,
    bool real_path) const {
  // Get real name of file
  std::string filename_real;
  if(real_path)
    filename_real = filename;
  else
    filename_real = ::real_path(filename);

  // Get parent folder name
  std::string filename_real_parent = parent_folder(filename_real);

  // Get array schema
  ArraySchema* array_schema;
  metadata_schema_get(filename_real_parent, array_schema, true);

  // Print array schema
  array_schema->print();

  // Clean up
  delete array_schema;
}

int StorageManager::array_schema_print(
    const std::string& filename,
    bool real_path) const {
  // Get real name of file
  std::string filename_real;
  if(real_path)
    filename_real = filename;
  else
    filename_real = ::real_path(filename);

  // Get parent folder name
  std::string filename_real_parent = parent_folder(filename_real);

  // Get array schema
  ArraySchema* array_schema;
  array_schema_get(filename_real_parent, array_schema, true);

  // Print array schema
  array_schema->print();

  // Clean up
  delete array_schema;
}

int StorageManager::list(
    const std::string& item) {
  // Get real name of item
  std::string item_real = real_path(item);

  if(master_catalog_exists(item_real, true)) {
    return master_catalog_list(item_real);
  } else if(workspace_exists(item_real, true)) {
    return workspace_list(item_real);
  } else if(group_exists(item_real, true)) {
    return group_list(item_real);
  } else if(array_exists(item_real, true)) {
    return array_list(item_real);
  } else if(metadata_exists(item_real, true)) {
    return metadata_list(item_real);
  } else if(is_array_schema(item_real, true)) {
     return array_schema_print(item_real, true); 
  } else if(is_metadata_schema(item_real, true)) {
     return metadata_schema_print(item_real, true); 
  } else if(is_fragment(item_real, true)) {
     return fragment_list(item_real); 
  } else {
    PRINT_ERROR(std::string("Invalid item '") + item_real + "'");
    return -1;
  } 
}

int StorageManager::master_catalog_list(
    const std::string& master_catalog) {
  // Get real metadata name
  std::string master_catalog_real = real_path(master_catalog);

  // Open catalog
  int md = metadata_open(master_catalog_real, "r");
  if(md == -1)
    return -1;

  // Get array schema
  ArraySchema* array_schema;
  if(metadata_schema_get(master_catalog_real, array_schema, true))
    return TILEDB_SM_ERR;

  // Create iterator
  ArrayConstCellIterator<int>* cell_it = metadata_begin<int>(md);

  // Retrieve all workspaces
  const void* cell;
  size_t coords_size = array_schema->coords_size(); 
  size_t workspace_size;
  size_t offset;
  std::string workspace;
  std::vector<std::string> workspaces;
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell = **cell_it; 
    offset = coords_size + sizeof(size_t);
    memcpy(&workspace_size, (const char*) cell + offset, sizeof(int)); 
    offset += sizeof(int);
    workspace.resize(workspace_size);
    memcpy(&workspace[0], (const char*) cell + offset, workspace_size);
    workspaces.push_back(workspace);
  }
 
  // Sort and print all workspaces
  // TODO: Print more details
  std::cout << "TileDB Master Catalog: \n";
  std::sort(workspaces.begin(), workspaces.end());
  for(int i=0; i<workspaces.size(); ++i) {
    std::cout << "TileDB workspace\t" << workspaces[i] << "\n";  
  }

  // Clean up
  if(metadata_close(md))
    return -1;
  cell_it->finalize();
  delete cell_it;

  // Success 
  return 0;
}

int StorageManager::master_catalog_create(
    const std::string& master_catalog,
    bool real_path) {
  // Get real master catalog directory name
  std::string master_catalog_real;
  if(real_path)
    master_catalog_real = master_catalog;
  else
    master_catalog_real = ::real_path(master_catalog);

  if(master_catalog_real == "") {
    PRINT_ERROR(std::string("Invalid master catalog '") + 
                master_catalog_real + "'"); 
    return -1;
  }

  // Check if master catalog is a directory
  if(is_dir(master_catalog_real, true)) {
    PRINT_ERROR(std::string("Directory '") +
                master_catalog_real + "' already exists");
    return -1;
  }

  // Create metadata schema
  TileDB_MetadataSchema metadata = {};
  metadata.metadata_name_ = master_catalog_real.c_str();
  metadata.attribute_num_ = 1;
  metadata.attributes_ = new const char*[1]; 
  metadata.attributes_[0] = "workspace";
  metadata.types_ = new const char*[1]; 
  metadata.types_[0] = "char:var";

  // Store metadata schema
  ArraySchema* array_schema = new ArraySchema();
  array_schema->init(&metadata);
  metadata_schema_store(array_schema, true);

  // Clean up
  delete [] metadata.attributes_; 
  delete [] metadata.types_; 
  delete array_schema;

  // Create a "master catalog file" 
  std::string filename = master_catalog_real + "/" + 
                         TILEDB_SM_MASTER_CATALOG_FILENAME;
  // ----- open -----
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Failed to create master catalog file; ") +
                strerror(errno));
    return -1;
  }
  // ----- close -----
  if(close(fd)) {
    PRINT_ERROR(std::string("Failed to close master catalog file; ") +
                strerror(errno));
    return -1;
  }

  // Success 
  return 0;
}

bool StorageManager::master_catalog_exists(
    const std::string& master_catalog,
    bool real_path) const {
  // Get real master catalog directory name
  std::string master_catalog_real;
  if(real_path)
    master_catalog_real = master_catalog;
  else
    master_catalog_real = ::real_path(master_catalog);

  // Check if master_catalog file exists
  std::string filename = master_catalog_real + "/" +
                         TILEDB_SM_MASTER_CATALOG_FILENAME;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

int StorageManager::array_open(
    const std::string& array_name, 
    const char* mode) {
  // Get real array name
  std::string array_name_real = real_path(array_name);

  // Check if the array is defined
  if(!array_exists(array_name_real, true)) {
    PRINT_ERROR("Array '" + array_name_real + "' does not exist");
    return TILEDB_SM_ERR;
  }

  // If the array is already open, return its stored descriptor
  OpenArrays::iterator it = open_arrays_.find(array_name_real);
  if(it != open_arrays_.end())
    return it->second;

  // Get array mode
  Array::Mode array_mode;
  if(!strcmp(mode, "w")) {
    array_mode = Array::WRITE;
  } else if(!strcmp(mode, "r")) {
    array_mode = Array::READ;
  } else if(!strcmp(mode, "c")) {
    array_mode = Array::CONSOLIDATE;
  } else {
    PRINT_ERROR("Invalid array mode");
    return TILEDB_SM_ERR; 
  }

  // Initialize an Array object
  ArraySchema* array_schema;
  if(array_schema_get(array_name_real, array_schema, true))
    return TILEDB_SM_ERR;
  Array* array = new Array(array_name_real, array_schema, array_mode);
  if(!array->created_successfully()) {
    array->finalize();
    delete array;
    return TILEDB_SM_ERR;
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
    open_arrays_[array_name_real] = ad;
    return ad;
  } else { // Error
    PRINT_ERROR("Cannot open array: reached maximum open arrays limit");
    array->finalize();
    delete array; 
    return TILEDB_SM_ERR;
  }
}

int StorageManager::metadata_open(
    const std::string& metadata_name, 
    const char* mode) {
  // Get real metadata name
  std::string metadata_name_real = real_path(metadata_name);

  // Check if the array is defined
  if(!metadata_exists(metadata_name_real, true)) {
    PRINT_ERROR("Metadata '" + metadata_name_real + "' does not exist");
    return TILEDB_SM_ERR;
  }

  // If the array is already open, return its stored descriptor
  OpenMetadata::iterator it = open_metadata_.find(metadata_name_real);
  if(it != open_metadata_.end())
    return it->second;

  // Get array mode
  Array::Mode array_mode;
  if(!strcmp(mode, "w")) {
    array_mode = Array::WRITE;
  } else if(!strcmp(mode, "r")) {
    array_mode = Array::READ;
  } else if(!strcmp(mode, "c")) {
    array_mode = Array::CONSOLIDATE;
  } else {
    PRINT_ERROR("Invalid array mode");
    return TILEDB_SM_ERR; 
  }

  // Initialize an Array object
  ArraySchema* array_schema;
  if(metadata_schema_get(metadata_name_real, array_schema, true))
    return TILEDB_SM_ERR;
  Array* array = new Array(metadata_name_real, array_schema, array_mode);
  if(!array->created_successfully()) {
    array->finalize();
    delete array;
    return TILEDB_SM_ERR;
  }

  // Get a new metadata descriptor
  int md = -1;
  for(int i=0; i<SM_OPEN_METADATA_MAX; ++i) {
    if(metadata_[i] == NULL) {
      md = i; 
      break;
    }
  }

  // Success
  if(md != -1) { 
    metadata_[md] = array;
    open_metadata_[metadata_name_real] = md;
    return md;
  } else { // Error
    PRINT_ERROR("Cannot open metadata: reached maximum open metadata limit");
    array->finalize();
    delete array; 
    return TILEDB_SM_ERR;
  }
}

int StorageManager::array_schema_get(
    const std::string& array_name, 
    ArraySchema*& array_schema,
    bool real_paths) const {
  // Get real array path
  std::string array_name_real;
  if(real_paths)
    array_name_real = array_name;
  else
    array_name_real = real_path(array_name);

  // Check if array exists
  if(!array_exists(array_name_real, true)) {
    PRINT_ERROR("Array '" + array_name_real + "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Open array schema file
  std::string filename = array_name_real + "/" + 
                         ARRAY_SCHEMA_FILENAME;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR(std::string( "Cannot open array schema file; ") +
                strerror(errno));
    return TILEDB_SM_ERR;
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
    return TILEDB_SM_ERR;
  }
  char* buffer = new char[buffer_size];

  // Load metadata schema
  ssize_t bytes_read = read(fd, buffer, buffer_size);
  if(bytes_read != buffer_size) {
    delete [] buffer;
    delete array_schema;
    if(bytes_read == -1)
      PRINT_ERROR(std::string("Failed to read from array schema file; ") +
                  strerror(errno));
    else
      PRINT_ERROR("Failed to properly read from array schema file");
    return TILEDB_SM_ERR;
  } 


  if(array_schema->deserialize(buffer, buffer_size)) {
    delete [] buffer;
    delete array_schema;
    return TILEDB_SM_ERR;
  }

  // Clean up
  delete [] buffer;
  if(close(fd)) {
    delete array_schema;
    PRINT_ERROR("Failed to close array schema file");
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_schema_get(
    const std::string& metadata_name, 
    ArraySchema*& array_schema,
    bool real_paths) const {
  // Get real metadata path
  std::string metadata_name_real;
  if(real_paths)
    metadata_name_real = metadata_name;
  else
    metadata_name_real = real_path(metadata_name);

  // Check if metadata exists
  if(!metadata_exists(metadata_name_real, true)) {
    PRINT_ERROR("Metadata '" + metadata_name_real + "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Open array schema file
  std::string filename = metadata_name_real + "/" + 
                         TILEDB_SM_METADATA_SCHEMA_FILENAME;
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR(std::string( "Cannot open metadata schema file; ") +
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // The schema to be returned
  array_schema = new ArraySchema();

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  ssize_t buffer_size = st.st_size;
  if(buffer_size == 0) {
    PRINT_ERROR("Metadata schema file is empty");
    delete array_schema;
    return TILEDB_SM_ERR;
  }
  char* buffer = new char[buffer_size];

  // Load metadata schema
  ssize_t bytes_read = read(fd, buffer, buffer_size);
  if(bytes_read != buffer_size) {
    delete [] buffer;
    delete array_schema;
    if(bytes_read == -1)
      PRINT_ERROR(std::string("Failed to read from metadata schema file; ") +
                  strerror(errno));
    else
      PRINT_ERROR("Failed to properly read from metadata schema file");
    return TILEDB_SM_ERR;
  } 
  if(array_schema->deserialize(buffer, buffer_size)) {
    delete [] buffer;
    delete array_schema;
    return TILEDB_SM_ERR;
  }

  // Clean up
  delete [] buffer;
  if(close(fd)) {
    delete array_schema;
    PRINT_ERROR("Failed to close metadata schema file");
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_schema_get(
    int md, 
    const ArraySchema*& array_schema) const {
  // Sanity check
  if(md < 0 || md >= SM_OPEN_METADATA_MAX) {
    PRINT_ERROR("Cannot get metadata schema; Invalid metadata descriptor");
    return TILEDB_SM_ERR;
  }

  // Check if metadata is open
  if(metadata_[md] == NULL) {
    PRINT_ERROR("Cannot get metadata schema; Metadata not open");
    return TILEDB_SM_ERR;
  }

  // Get metadata schema
  array_schema = metadata_[md]->array_schema();

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_schema_store(
    const ArraySchema* array_schema,
    bool master_catalog) {
  // Check array schema
  if(array_schema == NULL) {
    PRINT_ERROR("Cannot store metadata schema; "
                "the metadata schema cannot be empty");
    return -1;
  }

  // Get real matadata directory name
  std::string metadata_dirname = array_schema->array_name();
  std::string metadata_group_dirname = array_schema->group_name();

  // Make sure that the new metadata is not an existing directory
  if(is_dir(metadata_dirname)) {
    PRINT_ERROR(std::string("Directory '") + metadata_dirname + "' already exists");
    return -1;
  }

  // Check if the metadata are contained in a workspace, group or array
  if(!master_catalog &&
     !group_exists(metadata_group_dirname, true) && 
     !workspace_exists(metadata_group_dirname, true) &&
     !array_exists(metadata_group_dirname, true)) {
    PRINT_ERROR(std::string("Folder '") + metadata_group_dirname + "' must "
                "be either a workspace or a group or an array");
    return -1;
  }

  // Create metadata directory
  if(directory_create(metadata_dirname, true)) 
    return -1;

  // Open metadata schema file
  std::string filename = metadata_dirname + "/" + 
                         TILEDB_SM_METADATA_SCHEMA_FILENAME; 
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Failed to create metadata schema file - ") + 
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
      PRINT_ERROR(std::string("Failed to write the metadata schema to file - ") +
                  strerror(errno));
    else
      PRINT_ERROR("Failed to properly write the metadata schema to file");
    delete [] array_schema_c_str;
    return -1;
  }

  // Clean up
  delete [] array_schema_c_str;
  if(close(fd)) {
    PRINT_ERROR("Failed to close the metadata schema file"); 
    return -1;
  }

  // Success
  return 0;
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

int StorageManager::workspace_create_2(
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

int StorageManager::metadata_list(const std::string& metadata) {
  // Get real metadata path
  std::string metadata_real = real_path(metadata); 

  // Check if group exists
  if(!metadata_exists(metadata_real, true)) {
    PRINT_ERROR(std::string("Metadata '") + metadata_real + "' do not exist");
    return -1;
  }

  // Print all contents
  std::cout << "TileDB metadata '" << metadata_real << "':\n";
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(metadata_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open metadata directory '") + 
                metadata_real + "'; " + strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, ".."))
      continue;
    filename = metadata_real + "/" + next_file->d_name;
    if(!strcmp(next_file->d_name, TILEDB_SM_METADATA_SCHEMA_FILENAME)) {
      std::cout << "TileDB metadata schema" <<  "\t" << filename << "\n"; 
    } else if(is_fragment(filename, true)) { // Fragment
      std::cout << "TileDB fragment" <<  "\t" << filename << "\n"; 
    } else { // Non TileDB related
      std::cout << "TileDB irrelevant" <<  "\t" << filename << "\n"; 
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the metadata directory; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::fragment_list(const std::string& fragment) {
  // Get real fragment path
  std::string fragment_real = real_path(fragment); 

  // Check if fragment exists
  if(!is_fragment(fragment_real, true)) {
    PRINT_ERROR(std::string("Fragment '") + fragment_real + "' does not exist");
    return -1;
  }

  // Print all files
  std::cout << "TileDB fragment '" << fragment_real << "':\n";
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(fragment_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open fragment directory '") + 
                fragment_real + "'; " + strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, ".."))
      continue;
    filename = fragment_real + "/" + next_file->d_name;
    if(ends_with(filename, std::string(AS_COORDINATES_NAME) + TILE_DATA_FILE_SUFFIX)) {
      std::cout << "TileDB coordinates file" <<  "\t" << filename << "\n"; 
    } else if(ends_with(filename, TILE_DATA_FILE_SUFFIX)) {
      std::cout << "TileDB attribute data file" <<  "\t" << filename << "\n"; 
    } else if(ends_with(filename, BOOK_KEEPING_FILE_SUFFIX)) {
      std::cout << "TileDB book-keeping file" <<  "\t" << filename << "\n"; 
    } else { // Non TileDB related
      std::cout << "TileDB irrelevant" <<  "\t" << filename << "\n"; 
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the array directory; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::array_list(const std::string& array) {
  // Get real array path
  std::string array_real = real_path(array); 

  // Check if group exists
  if(!array_exists(array_real, true)) {
    PRINT_ERROR(std::string("Array '") + array_real + "' does not exist");
    return -1;
  }

  // Print all metadata
  std::cout << "TileDB array '" << array_real << "':\n";
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(array_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open array directory '") + 
                array_real + "'; " + strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, ".."))
      continue;
    filename = array_real + "/" + next_file->d_name;
    if(!strcmp(next_file->d_name, ARRAY_SCHEMA_FILENAME)) {
      std::cout << "TileDB array schema" <<  "\t" << filename << "\n"; 
    } else if(metadata_exists(filename, true)) { // Metadata
      std::cout << "TileDB metadata" <<  "\t" << filename << "\n"; 
    } else if(is_fragment(filename, true)) { // Fragment
      std::cout << "TileDB fragment" <<  "\t" << filename << "\n"; 
    } else { // Non TileDB related
      std::cout << "TileDB irrelevant" <<  "\t" << filename << "\n"; 
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the array directory; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

bool StorageManager::is_fragment(
    const std::string& filename, 
    bool real_path) {
  // Get real filename path
  std::string filename_real;
  if(real_path)
    filename_real = filename;
  else
    filename_real = ::real_path(filename); 

  if(is_file(filename + "/" + OFFSETS_FILENAME + BOOK_KEEPING_FILE_SUFFIX))
    return true;
  else
    return false;
}

int StorageManager::group_list(const std::string& group) {
  // Get real group path
  std::string group_real = real_path(group); 

  // Check if group exists
  if(!group_exists(group_real, true)) {
    PRINT_ERROR(std::string("Group '") + group_real + "' does not exist");
    return -1;
  }

  // Print all groups, arrays and metadata
  std::cout << "TileDB group '" << group_real << "':\n";
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(group_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open group directory '") + 
               group_real + "'; " + strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, SM_WORKSPACE_FILENAME) ||
       !strcmp(next_file->d_name, SM_GROUP_FILENAME))
      continue;
    filename = group_real + "/" + next_file->d_name;
    if(group_exists(filename, true)) { // Group
      std::cout << "TileDB group" <<  "\t" << filename << "\n"; 
    } else if(metadata_exists(filename, true)) { // Metadata
      std::cout << "TileDB metadata" <<  "\t" << filename << "\n"; 
    } else if(array_exists(filename, true)){  // Array
      std::cout << "TileDB array" <<  "\t" << filename << "\n"; 
    } else { // Non TileDB related
      std::cout << "TileDB irrelevant" <<  "\t" << filename << "\n"; 
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the group directory; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::workspace_list(const std::string& workspace) {
  // Get real workspace path
  std::string workspace_real = real_path(workspace); 

  // Check if group exists
  if(!workspace_exists(workspace_real, true)) {
    PRINT_ERROR(std::string("Workspace '") + workspace_real + "' does not exist");
    return -1;
  }

  // Print all groups, workspaces and arrays
  std::cout << "TileDB workspace '" << workspace_real << "':\n";
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(workspace_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open workspace directory '") + 
               workspace_real + "'; " + strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, SM_WORKSPACE_FILENAME) ||
       !strcmp(next_file->d_name, SM_GROUP_FILENAME))
      continue;
    filename = workspace_real + "/" + next_file->d_name;
    if(group_exists(filename, true)) { // Group
      std::cout << "TileDB group" <<  "\t" << filename << "\n"; 
    } else if(metadata_exists(filename, true)) { // Metadata
      std::cout << "TileDB metadata" <<  "\t" << filename << "\n"; 
    } else if(array_exists(filename, true)){  // Array
      std::cout << "TileDB array" <<  "\t" << filename << "\n"; 
    } else { // Non TileDB related
      std::cout << "TileDB irrelevant" <<  "\t" << filename << "\n"; 
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the workspace directory; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::workspace_clear(const std::string& workspace) {
  // Get real workspace path
  std::string workspace_real = real_path(workspace); 

  // Check if group exists
  if(!workspace_exists(workspace_real, true)) {
    PRINT_ERROR(std::string("Workspace '") + workspace_real + "' does not exist");
    return -1;
  }

  // Delete all groups, arrays and metadata inside the group directory
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(workspace_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open group directory '") + 
               workspace_real + "'; " + strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, SM_WORKSPACE_FILENAME) ||
       !strcmp(next_file->d_name, SM_GROUP_FILENAME))
      continue;
    filename = workspace_real + "/" + next_file->d_name;
    if(group_exists(filename, true)) { // Group
      if(group_delete(filename, true))
        return -1;
    } else if(metadata_exists(filename, true)) { // Metadata
      if(metadata_delete(filename, true))
        return -1;
    } else if(array_exists(filename, true)){  // Array
      if(array_delete(filename, true))
        return -1;
    } else { // Non TileDB related
      PRINT_ERROR(std::string("Cannot delete non TileDB related element '") +
                  filename + "'");
      return -1;
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the group directory; ") + 
                strerror(errno));
    return -1;
  }

  // Success
  return 0;
}

int StorageManager::workspace_create(
    const std::string& workspace, 
    const std::string& master_catalog) {
  // Get real paths
  std::string workspace_real, master_catalog_real;
  workspace_real = real_path(workspace);
  master_catalog_real = real_path(master_catalog);

  if(workspace_real == "") {
    PRINT_ERROR(std::string("Invalid workspace '") +
                workspace_real + "'"); 
    return -1;
  }
  if(master_catalog_real == "") {
    PRINT_ERROR(std::string("Invalid master catalog '") +
                master_catalog_real + "'"); 
    return -1;
  }

  // Workspace should not be inside another workspace or group or array or metadata
  std::string workspace_real_parent = parent_folder(workspace_real);
  if(workspace_exists(workspace_real_parent) ||
     group_exists(workspace_real_parent) ||
     array_exists(workspace_real_parent) ||
     metadata_exists(workspace_real_parent)) {
    PRINT_ERROR(std::string("Folder '") + workspace_real_parent + 
                            "' should not be a workspace, group, "
                            "array, or metadata");
    return -1;
  }

  // Master catalog should not be inside another workspace or group or array or metadata
  std::string master_catalog_real_parent = parent_folder(master_catalog_real);
  if(workspace_exists(master_catalog_real_parent) ||
     group_exists(master_catalog_real_parent) ||
     array_exists(master_catalog_real_parent) ||
     metadata_exists(master_catalog_real_parent)) {
    PRINT_ERROR(std::string("Folder '") + master_catalog_real_parent + 
                            "' should not be a workspace, group, "
                            "array, or metadata");
    return -1;
  }  

  // Check if directory already exists
  if(is_dir(workspace_real, true)) {
    PRINT_ERROR(std::string("Directory '") + 
                workspace_real + "' already exists");
    return -1;
  }

  // Check if master catalog directory exists
  if(!master_catalog_exists(master_catalog_real, true) &&
     is_dir(master_catalog_real, true)) {
    PRINT_ERROR(std::string("Directory '") + 
                master_catalog_real + "' already exists");
    return -1;
  }

  // Create directory 
  if(directory_create(workspace_real, true)) { 
    PRINT_ERROR(std::string("Cannot create workspace '") +
                workspace_real + "' - " + 
                strerror(errno));
    return -1;
  }

  // Create (empty) workspace file 
  std::string filename = workspace_real + "/" + SM_WORKSPACE_FILENAME;
  // ----- open -----
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Failed to create workspace file; ") +
                strerror(errno));
    return -1;
  }
  // ----- close -----
  if(close(fd)) {
    PRINT_ERROR(std::string("Failed to close workspace file; ") +
                strerror(errno));
    return -1;
  }

  // Create (empty) group file 
  filename = workspace_real + "/" + SM_GROUP_FILENAME;
  // ----- open -----
  fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Failed to create workspace file; ") +
                strerror(errno));
    return -1;
  }
  // ----- close -----
  if(close(fd)) {
    PRINT_ERROR(std::string("Failed to close workspace file; ") +
                strerror(errno));
    return -1;
  }

  // Create master catalog if it does not exist
  if(!master_catalog_exists(master_catalog_real, true)) {
    if(master_catalog_create(master_catalog_real, true))
      return -1;
  }

  // Update master catalog 
  // ----- open catalog
  int md = metadata_open(master_catalog_real, "w");
  if(md == -1)
    return -1;
  // ----- get schema
  const ArraySchema* array_schema;
  if(metadata_schema_get(md, array_schema))
    return -1;
  // ----- write new entry
  // Get coordinates from key
  int coords[4];
  MD5((const unsigned char*) workspace_real.c_str(), 
       workspace_real.size(), 
       (unsigned char*) &coords);

  // Get cell size
  int workspace_size = workspace_real.size();
  size_t coords_size = array_schema->coords_size();
  size_t cell_size = coords_size + sizeof(size_t) + 
                     sizeof(int) + workspace_size;

  // Create new cell
  size_t offset = 0;
  void* cell = malloc(cell_size);
  memcpy(cell, coords, coords_size);
  offset += coords_size;
  memcpy((char*) cell + coords_size, &cell_size, sizeof(size_t));
  offset += sizeof(size_t);
  memcpy((char*) cell + offset, &workspace_size, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) cell + offset, workspace_real.c_str(), workspace_size);

  // Write cell
  metadata_write<int>(md, cell);
  // ---- close catalog
  if(metadata_close(md))
    return -1;
  free(cell);

  // Consolidate
  if(metadata_consolidate(master_catalog_real)) 
    return -1;

  // Success
  return 0;
}

int StorageManager::workspace_delete(
    const std::string& workspace, 
    const std::string& master_catalog) {
  // Get real paths
  std::string workspace_real, master_catalog_real;
  workspace_real = real_path(workspace);
  master_catalog_real = real_path(master_catalog);

  // Check if workspace exists
  if(!workspace_exists(workspace_real, true)) {
    PRINT_ERROR(std::string("Workspace '") + workspace_real + "' does not exist");
    return -1;
  }  

  // Master catalog should exist
  if(!master_catalog_exists(master_catalog_real, true)) {
    PRINT_ERROR(std::string("Master catalog '") + master_catalog_real + 
                            "' does not exist'");
    return -1;
  }  

  // Clear workspace 
  if(workspace_clear(workspace_real))
    return -1;

  // Delete directory
  if(directory_delete(workspace_real, true))
    return -1;

  // Update master catalog by deleting workspace 
  // ----- open catalog
  int md = metadata_open(master_catalog_real, "w");
  if(md == -1)
    return -1;
  // ----- get schema
  const ArraySchema* array_schema;
  if(metadata_schema_get(md, array_schema))
    return -1;

  int coords[4];
  MD5((const unsigned char*) workspace_real.c_str(), 
       workspace_real.size(), 
       (unsigned char*) &coords);

  // Get cell size
  size_t coords_size = array_schema->coords_size(); 
  size_t cell_size = coords_size + sizeof(size_t) + sizeof(int) + 1;

  // Create new cell 
  size_t offset = 0;
  void* cell = malloc(cell_size);
  memcpy(cell, coords, coords_size);
  offset += coords_size;
  memcpy((char*) cell + coords_size, &cell_size, sizeof(size_t));
  offset += sizeof(size_t);
  int v = 1;
  memcpy((char*) cell + offset, &v, sizeof(int));
  offset += sizeof(int);
  char a = DEL_CHAR; 
  memcpy((char*) cell + offset, &a, 1);

  // Write cell
  metadata_write<int>(md, cell);
  free(cell);

  // ---- close catalog
  if(metadata_close(md))
    return -1;

  // Consolidate
  if(metadata_consolidate(master_catalog_real)) 
    return -1;

  // Success
  return 0;
}

int StorageManager::workspace_move(
    const std::string& old_workspace, 
    const std::string& new_workspace,
    const std::string& master_catalog) {
  // Get real paths
  std::string old_workspace_real, new_workspace_real, master_catalog_real;
  old_workspace_real = real_path(old_workspace);
  new_workspace_real = real_path(new_workspace);
  master_catalog_real = real_path(master_catalog);

  // Check if old workspace exists
  if(!workspace_exists(old_workspace_real, true)) {
    PRINT_ERROR(std::string("Workspace '") + old_workspace_real + "' does not exist");
    return -1;
  }  

  // Check new workspace
  if(new_workspace_real == "") {
    PRINT_ERROR(std::string("Invalid workspace '") +
                new_workspace_real + "'"); 
    return -1;
  }
  if(is_dir(new_workspace_real, true)) {
    PRINT_ERROR(std::string("Directory '") + new_workspace_real + 
                            "' already exists"); 
    return -1;
  }

  // New workspace should not be inside another workspace or group or array or metadata
  std::string new_workspace_real_parent = parent_folder(new_workspace_real);
  if(workspace_exists(new_workspace_real_parent) ||
     group_exists(new_workspace_real_parent) ||
     array_exists(new_workspace_real_parent) ||
     metadata_exists(new_workspace_real_parent)) {
    PRINT_ERROR(std::string("Folder '") + new_workspace_real_parent + 
                            "' should not be a workspace, group, "
                            "array, or metadata");
    return -1;
  }

  // Master catalog should exist
  if(!master_catalog_exists(master_catalog_real, true)) {
    PRINT_ERROR(std::string("Master catalog '") + master_catalog_real + 
                            "' does not exist'");
    return -1;
  }  

  // Rename directory 
  if(rename(old_workspace_real.c_str(), new_workspace_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move group; ") + 
                strerror(errno));
    return -1;
  }

  // Update master catalog by adding new workspace 
  // ----- open catalog
  int md = metadata_open(master_catalog_real, "w");
  if(md == -1)
    return -1;
  // ----- get schema
  const ArraySchema* array_schema;
  if(metadata_schema_get(md, array_schema))
    return -1;
  // ----- write new entry
  // Get coordinates from key
  int coords[4];
  MD5((const unsigned char*) new_workspace_real.c_str(), 
       new_workspace_real.size(), 
       (unsigned char*) &coords);

  // Get cell size
  int workspace_size = new_workspace_real.size();
  size_t coords_size = array_schema->coords_size();
  size_t cell_size = coords_size + sizeof(size_t) + 
                     sizeof(int) + workspace_size;

  // Create new cell 
  size_t offset = 0;
  void* cell = malloc(cell_size);
  memcpy(cell, coords, coords_size);
  offset += coords_size;
  memcpy((char*) cell + coords_size, &cell_size, sizeof(size_t));
  offset += sizeof(size_t);
  memcpy((char*) cell + offset, &workspace_size, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) cell + offset, new_workspace_real.c_str(), workspace_size);

  // Write cell
  metadata_write<int>(md, cell);
  free(cell);

  // Update master catalog by deleting old workspace 
  MD5((const unsigned char*) old_workspace_real.c_str(), 
       old_workspace_real.size(), 
       (unsigned char*) &coords);

  // Get cell size
  cell_size = coords_size + sizeof(size_t) + sizeof(int) + 1;

  // Create new cell 
  offset = 0;
  cell = malloc(cell_size);
  memcpy(cell, coords, coords_size);
  offset += coords_size;
  memcpy((char*) cell + coords_size, &cell_size, sizeof(size_t));
  offset += sizeof(size_t);
  int v = 1;
  memcpy((char*) cell + offset, &v, sizeof(int));
  offset += sizeof(int);
  char a = DEL_CHAR; 
  memcpy((char*) cell + offset, &a, 1);

  // Write cell
  metadata_write<int>(md, cell);
  free(cell);

  // ---- close catalog
  if(metadata_close(md))
    return -1;

  // Consolidate
  if(metadata_consolidate(master_catalog_real)) 
    return -1;

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
int StorageManager::metadata_write(int md, const void* cell) const {
  // Checks
  if(md < 0 || md >= SM_OPEN_METADATA_MAX) {
    PRINT_ERROR("Invalid metadata descriptor");
    return -1;
  }
  if(metadata_[md] == NULL) {
    PRINT_ERROR("Metadata not open");
    return -1;
  }
  if(metadata_[md]->mode() != Array::WRITE) {
    PRINT_ERROR("Invalid metadata mode");
    return -1;
  }

  // Write cell
  return metadata_[md]->cell_write<T>(cell);
}

template<class T>
int StorageManager::cell_write_sorted(int ad, const void* cell, bool without_coords) const {
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

  return arrays_[ad]->cell_write_sorted<T>(cell, without_coords); 
}

template<class T>
int StorageManager::metadata_write_sorted(int md, const void* cell) const {
  // Checks
  if(md < 0 || md >= SM_OPEN_METADATA_MAX) {
    PRINT_ERROR("Invalid metadata descriptor");
    return -1;
  }
  if(metadata_[md] == NULL) {
    PRINT_ERROR("Metadata not open");
    return -1;
  }
  if(metadata_[md]->mode() != Array::WRITE) {
    PRINT_ERROR("Invalid metadata mode");
    return -1;
  }

  return metadata_[md]->cell_write_sorted<T>(cell); 
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

template<class T>
int StorageManager::array_read_dense(
    int ad,
    const T* range,
    const std::vector<int>& attribute_ids,
    void* buffer,
    int* buffer_size) {
  // Sanity check
  if(ad < 0 || ad >= SM_OPEN_ARRAYS_MAX) {
    PRINT_ERROR("Invalid array descriptor");
    return -1;
  }

  // Keep track the offset in the buffer
  int offset = 0;

  // Prepare cell iterator
  ArrayConstCellIterator<T>* cell_it = begin<T>(ad, range, attribute_ids);

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);

  // Prepare C-style cell to hold the actual cell to be written in the buffer
  void* cell_c = NULL;
  size_t cell_c_capacity = 0, cell_c_size = 0;
  std::vector<int> dim_ids;

  // Write cells into the CSV file
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell.set_cell(**cell_it);
    cell.cell<T>(
        dim_ids,
        attribute_ids, 
        cell_c, 
        cell_c_capacity,  
        cell_c_size);

    if(offset + cell_c_size > *buffer_size) {
      PRINT_ERROR("Cannot read from array; buffer overflow");  
      return TILEDB_SM_READ_BUFFER_OVERFLOW;
    }

    memcpy(static_cast<char*>(buffer) + offset, cell_c, cell_c_size);
    offset += cell_c_size;
  }

  // Clean up
  if(cell_c != NULL)
    free(cell_c);
  cell_it->finalize();
  delete cell_it;

  *buffer_size = offset;

  return TILEDB_SM_OK;
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
ArrayConstCellIterator<T>* StorageManager::metadata_begin(
    int md, 
    const T* range, 
    const std::vector<int>& attribute_ids) const {
  // Checks
  if(md < 0 || md >= SM_OPEN_METADATA_MAX) {
    PRINT_ERROR("Invalid metadata descriptor");
    return NULL;
  }
  if(metadata_[md] == NULL) {
    PRINT_ERROR("Metadata not open");
    return NULL;
  }
  if(metadata_[md]->mode() != Array::READ) {
    PRINT_ERROR("Invalid metadata mode");
    return NULL;
  }

  // Create iterator
  ArrayConstCellIterator<T>* cell_it = 
      new ArrayConstCellIterator<T>(metadata_[md], range, attribute_ids);

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
ArrayConstCellIterator<T>* StorageManager::metadata_begin(
    int md) const {
  // Checks
  if(md < 0 || md >= SM_OPEN_METADATA_MAX) {
    PRINT_ERROR("Invalid metadata descriptor");
    return NULL;
  }
  if(metadata_[md] == NULL) {
    PRINT_ERROR("Metadata not open");
    return NULL;
  }
  if(metadata_[md]->mode() != Array::READ) {
    PRINT_ERROR("Invalid metadata mode");
    return NULL;
  }

  // Create iterator
  ArrayConstCellIterator<T>* cell_it = 
      new ArrayConstCellIterator<T>(metadata_[md]);

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

template ArrayConstCellIterator<int>*
StorageManager::metadata_begin<int>(
    int ad, const int* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<int64_t>*
StorageManager::metadata_begin<int64_t>(
    int ad, const int64_t* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<float>*
StorageManager::metadata_begin<float>(
    int ad, const float* range, const std::vector<int>& attribute_ids) const;
template ArrayConstCellIterator<double>*
StorageManager::metadata_begin<double>(
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
    int ad, const void* cell, bool without_coords) const;
template int StorageManager::cell_write_sorted<int64_t>(
    int ad, const void* cell, bool without_coords) const;
template int StorageManager::cell_write_sorted<float>(
    int ad, const void* cell, bool without_coords) const;
template int StorageManager::cell_write_sorted<double>(
    int ad, const void* cell, bool without_coords) const;

template int StorageManager::metadata_write<int>(
    int md, const void* cell) const;
template int StorageManager::metadata_write<int64_t>(
    int md, const void* cell) const;
template int StorageManager::metadata_write<float>(
    int md, const void* cell) const;
template int StorageManager::metadata_write<double>(
    int md, const void* cell) const;

template int StorageManager::metadata_write_sorted<int>(
    int md, const void* cell) const;
template int StorageManager::metadata_write_sorted<int64_t>(
    int md, const void* cell) const;
template int StorageManager::metadata_write_sorted<float>(
    int md, const void* cell) const;
template int StorageManager::metadata_write_sorted<double>(
    int md, const void* cell) const;

template int StorageManager::array_read_dense<int>(
    int ad,
    const int* range,
    const std::vector<int>& attribute_ids,
    void* buffer,
    int* buffer_size);
template int StorageManager::array_read_dense<int64_t>(
    int ad,
    const int64_t* range,
    const std::vector<int>& attribute_ids,
    void* buffer,
    int* buffer_size);
