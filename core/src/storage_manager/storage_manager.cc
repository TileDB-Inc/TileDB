/**
 * @file   storage_manager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * Copyright (c) 2016 MIT and Intel Corp.
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
#include <cassert>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utils.h>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::StorageManager] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::StorageManager] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManager::StorageManager() {
}

StorageManager::~StorageManager() {
}




/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

int StorageManager::init(const char* config_filename) {
  // Set configuration parameters
  if(config_filename == NULL)
    config_set_default();
  else if(config_set(config_filename) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Set the TileDB home directory
  tiledb_home_ = TILEDB_HOME;
  if(tiledb_home_ == "") {
    tiledb_home_ = getenv("HOME");
    if(tiledb_home_ == "") {
      char cwd[1024];
      if(getcwd(cwd, sizeof(cwd)) != NULL) {
        tiledb_home_ = cwd;
      } else {
        PRINT_ERROR("Cannot set TileDB home directory");
        return TILEDB_SM_ERR;
      }
    }
    tiledb_home_ += "/.tiledb";
  }

  // Set the master catalog directory
  master_catalog_dir_ = tiledb_home_ + "/" + TILEDB_SM_MASTER_CATALOG;

  // Create the TileDB home directory if it does not exists, as well
  // as the master catalog.
  if(!is_dir(tiledb_home_)) { 
    if(create_dir(tiledb_home_) != TILEDB_UT_OK)
      return TILEDB_SM_ERR;
    if(master_catalog_create() != TILEDB_SM_OK)
      return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}




/* ****************************** */
/*            WORKSPACE           */
/* ****************************** */

int StorageManager::workspace_create(const std::string& workspace) const {
  // Check if the workspace is inside a workspace or another group
  std::string parent_dir = ::parent_dir(workspace);
  if(is_workspace(parent_dir) || 
     is_group(parent_dir) ||
     is_array(parent_dir) ||
     is_metadata(parent_dir)) {
    PRINT_ERROR("The workspace cannot be contained in another workspace, "
                "group, array or metadata directory");
    return TILEDB_SM_ERR;
  }

  // Create workspace directory
  if(create_dir(workspace) != TILEDB_UT_OK)  
    return TILEDB_SM_ERR;

  // Create workspace file
  if(create_workspace_file(workspace) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Create master catalog entry
  if(create_master_catalog_entry(workspace, TILEDB_SM_MC_INS) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::ls_workspaces(
    char** workspaces,
    int& workspace_num) const {
  // Initialize the master catalog iterator
  const char* attributes[] = { TILEDB_KEY };
  MetadataIterator* metadata_it;
  size_t buffer_key[100];
  char buffer_key_var[1000];
  void* buffers[] = { buffer_key, buffer_key_var };
  size_t buffer_sizes[] = { sizeof(buffer_key), sizeof(buffer_key_var) };
  if(metadata_iterator_init(
       metadata_it,
       (tiledb_home_ + "/" + TILEDB_SM_MASTER_CATALOG).c_str(),
       attributes,
       1,
       buffers,
       buffer_sizes) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Copy workspaces
  int workspace_i = 0;
  const char* key;
  size_t key_size;

  while(!metadata_it->end()) {
    // Get workspace
    if(metadata_it->get_value(0, (const void**) &key, &key_size) != 
       TILEDB_MT_OK) {
      metadata_iterator_finalize(metadata_it);
      return TILEDB_MT_ERR;
    }

    // Copy workspace
    if(key_size != 1 || key[0] != TILEDB_EMPTY_CHAR) {
      strcpy(workspaces[workspace_i], key);
      ++workspace_i;
    }

    // Advance
    if(metadata_it->next() != TILEDB_MT_OK) {
      metadata_iterator_finalize(metadata_it);
      return TILEDB_MT_ERR;
    }
  }

  // Set the workspace number
  workspace_num = workspace_i;

  // Finalize the master catalog iterator
  if(metadata_iterator_finalize(metadata_it) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}





/* ****************************** */
/*             GROUP              */
/* ****************************** */

int StorageManager::group_create(const std::string& group) const {
  // Check if the group is inside a workspace or another group
  std::string parent_dir = ::parent_dir(group);
  if(!is_workspace(parent_dir) && !is_group(parent_dir)) {
    PRINT_ERROR("The group must be contained in a workspace "
                "or another group");
    return TILEDB_SM_ERR;
  }

  // Create group directory
  if(create_dir(group) != TILEDB_UT_OK)  
    return TILEDB_SM_ERR;

  // Create group file
  if(create_group_file(group) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}




/* ****************************** */
/*             ARRAY              */
/* ****************************** */

int StorageManager::array_create(const ArraySchemaC* array_schema_c) const {
  // Initialize array schema
  ArraySchema* array_schema = new ArraySchema();
  if(array_schema->init(array_schema_c) != TILEDB_AS_OK) {
    delete array_schema;
    return TILEDB_SM_ERR;
  }

  // Get real array directory name
  std::string dir = array_schema->array_name();
  std::string parent_dir = ::parent_dir(dir);

  // Check if the array directory is contained in a workspace, group or array
  if(!is_workspace(parent_dir) && 
     !is_group(parent_dir)) {
    PRINT_ERROR(std::string("Cannot create array; Directory '") + parent_dir + 
                "' must be a TileDB workspace or group");
    return TILEDB_SM_ERR;
  }

  // Create array with the new schema
  int rc = array_create(array_schema);
  
  // Clean up
  delete array_schema;

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}

int StorageManager::array_create(const ArraySchema* array_schema) const {
  // Check array schema
  if(array_schema == NULL) {
    PRINT_ERROR("Cannot create array; Empty array schema");
    return TILEDB_SM_ERR;
  }

  // Create array directory
  std::string dir = array_schema->array_name();
  if(create_dir(dir) != TILEDB_UT_OK) 
    return TILEDB_SM_ERR;

  // Open array schema file
  std::string filename = dir + "/" + TILEDB_ARRAY_SCHEMA_FILENAME; 
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Cannot create array; ") + strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Serialize array schema
  void* array_schema_bin;
  size_t array_schema_bin_size;
  if(array_schema->serialize(array_schema_bin, array_schema_bin_size) !=
     TILEDB_AS_OK)
    return TILEDB_SM_ERR;

  // Store the array schema
  ssize_t bytes_written = ::write(fd, array_schema_bin, array_schema_bin_size);
  if(bytes_written != array_schema_bin_size) {
    PRINT_ERROR(std::string("Cannot create array; ") + strerror(errno));
    free(array_schema_bin);
    return TILEDB_SM_ERR;
  }

  // Clean up
  free(array_schema_bin);
  if(::close(fd)) {
    PRINT_ERROR(std::string("Cannot create array; ") + strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_load_schema(
    const char* array_dir,
    ArraySchema*& array_schema) const {
  // Get real array path
  std::string real_array_dir = ::real_dir(array_dir);

  // Check if array exists
  if(!is_array(real_array_dir)) {
    PRINT_ERROR(std::string("Cannot load array schema; Array '") + 
                real_array_dir + "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Open array schema file
  std::string filename = real_array_dir + "/" + TILEDB_ARRAY_SCHEMA_FILENAME;
  int fd = ::open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot load schema; File opening error");
    return TILEDB_SM_ERR;
  }

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  ssize_t buffer_size = st.st_size;
  if(buffer_size == 0) {
    PRINT_ERROR("Cannot load array schema; Empty array schema file");
    return TILEDB_SM_ERR;
  }
  void* buffer = malloc(buffer_size);

  // Load array schema
  ssize_t bytes_read = ::read(fd, buffer, buffer_size);
  if(bytes_read != buffer_size) {
    PRINT_ERROR("Cannot load array schema; File reading error");
    free(buffer);
    return TILEDB_SM_ERR;
  } 

  // Initialize array schema
  array_schema = new ArraySchema();
  if(array_schema->deserialize(buffer, buffer_size) != TILEDB_AS_OK) {
    free(buffer);
    delete array_schema;
    return TILEDB_SM_ERR;
  }

  // Clean up
  free(buffer);
  if(::close(fd)) {
    delete array_schema;
    PRINT_ERROR("Cannot load array schema; File closing error");
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_init(
    Array*& array,
    const char* array_dir,
    int mode,
    const void* subarray,
    const char** attributes,
    int attribute_num)  const {
  // Load array schema
  ArraySchema* array_schema;
  if(array_load_schema(array_dir, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Create Array object
  array = new Array();
  if(array->init(array_schema, mode, attributes, attribute_num, subarray) !=
     TILEDB_AR_OK) {
    delete array;
    array = NULL;
    return TILEDB_SM_ERR;
  } else {
    return TILEDB_SM_OK;
  }
}

int StorageManager::array_finalize(Array* array) const {
  // If the array is NULL, do nothing
  if(array == NULL)
    return TILEDB_SM_OK;

  // Finalize array
  int rc = array->finalize();
  delete array;

  // Return
  if(rc == TILEDB_AR_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}

int StorageManager::array_iterator_init(
    ArrayIterator*& array_it,
    const char* array_dir,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes)  const {
  // Load array schema
  ArraySchema* array_schema;
  if(array_load_schema(array_dir, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Create Array object
  Array* array = new Array();
  if(array->init(
         array_schema, 
         TILEDB_ARRAY_READ, 
         attributes, 
         attribute_num, 
         subarray) != TILEDB_AR_OK) {
    delete array;
    array_it = NULL;
    return TILEDB_SM_ERR;
  } 

  // Create ArrayIterator object
  array_it = new ArrayIterator();
  if(array_it->init(array, buffers, buffer_sizes) != TILEDB_AIT_OK) {
    delete array;
    delete array_it;
    array_it = NULL;
    return TILEDB_SM_ERR;
  } 

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_iterator_finalize(
    ArrayIterator* array_it) const {
  // If the array iterator is NULL, do nothing
  if(array_it == NULL)
    return TILEDB_SM_OK;

  // Finalize array
  int rc = array_it->finalize();
  delete array_it;

  // Return
  if(rc == TILEDB_AIT_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}




/* ****************************** */
/*            METADATA            */
/* ****************************** */

int StorageManager::metadata_create(
    const MetadataSchemaC* metadata_schema_c) const {
  // Initialize array schema
  ArraySchema* array_schema = new ArraySchema();
  if(array_schema->init(metadata_schema_c) != TILEDB_AS_OK) {
    delete array_schema;
    return TILEDB_SM_ERR;
  }

  // Get real array directory name
  std::string dir = array_schema->array_name();
  std::string parent_dir = ::parent_dir(dir);

  // Check if the array directory is contained in a workspace, group or array
  if(!is_workspace(parent_dir) && 
     !is_group(parent_dir) &&
     !is_array(parent_dir)) {
    PRINT_ERROR(std::string("Cannot create metadata; Directory '") + 
                parent_dir + "' must be a TileDB workspace, group, or array");
    return TILEDB_SM_ERR;
  }

  // Create array with the new schema
  int rc = metadata_create(array_schema);
  
  // Clean up
  delete array_schema;

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}

int StorageManager::metadata_create(const ArraySchema* array_schema) const {
  // Check metadata schema
  if(array_schema == NULL) {
    PRINT_ERROR("Cannot create metadata; Empty metadata schema");
    return TILEDB_SM_ERR;
  }

  // Create array directory
  std::string dir = array_schema->array_name();
  if(create_dir(dir) != TILEDB_UT_OK) 
    return TILEDB_SM_ERR;

  // Open metadata schema file
  std::string filename = dir + "/" + TILEDB_METADATA_SCHEMA_FILENAME; 
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Cannot create metadata; ") + strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Serialize metadata schema
  void* array_schema_bin;
  size_t array_schema_bin_size;
  if(array_schema->serialize(array_schema_bin, array_schema_bin_size) !=
     TILEDB_AS_OK)
    return TILEDB_SM_ERR;

  // Store the array schema
  ssize_t bytes_written = ::write(fd, array_schema_bin, array_schema_bin_size);
  if(bytes_written != array_schema_bin_size) {
    PRINT_ERROR(std::string("Cannot create metadata; ") + strerror(errno));
    free(array_schema_bin);
    return TILEDB_SM_ERR;
  }

  // Clean up
  free(array_schema_bin);
  if(::close(fd)) {
    PRINT_ERROR(std::string("Cannot create metadata; ") + strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_load_schema(
    const char* metadata_dir,
    ArraySchema*& array_schema) const {
  // Get real array path
  std::string real_metadata_dir = ::real_dir(metadata_dir);

  // Check if metadata exists
  if(!is_metadata(real_metadata_dir)) {
    PRINT_ERROR(std::string("Cannot load metadata schema; Metadata '") + 
                real_metadata_dir + "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Open array schema file
  std::string filename = 
      real_metadata_dir + "/" + TILEDB_METADATA_SCHEMA_FILENAME;
  int fd = ::open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot load metadata schema; File opening error");
    return TILEDB_SM_ERR;
  }

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  ssize_t buffer_size = st.st_size;
  if(buffer_size == 0) {
    PRINT_ERROR("Cannot load metadata schema; Empty metadata schema file");
    return TILEDB_SM_ERR;
  }
  void* buffer = malloc(buffer_size);

  // Load array schema
  ssize_t bytes_read = ::read(fd, buffer, buffer_size);
  if(bytes_read != buffer_size) {
    PRINT_ERROR("Cannot load metadata schema; File reading error");
    free(buffer);
    return TILEDB_SM_ERR;
  } 

  // Initialize array schema
  array_schema = new ArraySchema();
  if(array_schema->deserialize(buffer, buffer_size) == TILEDB_AS_ERR) {
    free(buffer);
    delete array_schema;
    return TILEDB_SM_ERR;
  }

  // Clean up
  free(buffer);
  if(::close(fd)) {
    delete array_schema;
    PRINT_ERROR("Cannot load metadata schema; File closing error");
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_init(
    Metadata*& metadata,
    const char* metadata_dir,
    int mode,
    const char** attributes,
    int attribute_num)  const {
  // Load metadata schema
  ArraySchema* array_schema;
  if(metadata_load_schema(metadata_dir, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Create metadata object
  metadata = new Metadata();
  int rc = metadata->init(array_schema, mode, attributes, attribute_num);

  // Return
  if(rc != TILEDB_MT_OK) {
    delete metadata;
    metadata = NULL;
    return TILEDB_SM_ERR;
  } else {
    return TILEDB_SM_OK;
  }
}

int StorageManager::metadata_finalize(Metadata* metadata) const {
  // If the metadata is NULL, do nothing
  if(metadata == NULL)
    return TILEDB_SM_OK;

  // Finalize metadata
  int rc = metadata->finalize();
  delete metadata;

  // Return
  if(rc == TILEDB_MT_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}

int StorageManager::metadata_iterator_init(
    MetadataIterator*& metadata_it,
    const char* metadata_dir,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes)  const {
  // Load metadata schema
  ArraySchema* array_schema;
  if(metadata_load_schema(metadata_dir, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Create metadata object
  Metadata* metadata = new Metadata();
  if(metadata->init(
         array_schema, 
         TILEDB_METADATA_READ, 
         attributes, 
         attribute_num) !=
     TILEDB_MT_OK) {
    delete metadata;
    metadata_it = NULL;
    return TILEDB_SM_ERR;
  } 

  // Create MetadataIterator object
  metadata_it = new MetadataIterator();
  if(metadata_it->init(metadata, buffers, buffer_sizes) != TILEDB_MIT_OK) {
    delete metadata;
    delete metadata_it;
    metadata_it = NULL;
    return TILEDB_SM_ERR;
  } 

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_iterator_finalize(
    MetadataIterator* metadata_it) const {
  // If the metadata iterator is NULL, do nothing
  if(metadata_it == NULL)
    return TILEDB_SM_OK;

  // Finalize metadata
  int rc = metadata_it->finalize();
  delete metadata_it;

  // Return
  if(rc == TILEDB_MIT_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}




/* ****************************** */
/*               MISC             */
/* ****************************** */

int StorageManager::ls(
    const char* parent_dir,
    char** dirs, 
    int* dir_types,
    int& dir_num) const {
  // Get real parent directory
  std::string parent_dir_real = ::real_dir(parent_dir); 

  // Initialize directory counter
  int dir_i =0;

  // List all groups and arrays inside the parent directory
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(parent_dir_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open parent directory '") + 
                parent_dir_real + "'; " + strerror(errno));
    return TILEDB_SM_ERR;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, ".."))
      continue;
    filename = parent_dir_real + "/" +  next_file->d_name;
    if(is_group(filename)) {                  // Group
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_GROUP;
      ++dir_i;
    } else if(is_metadata(filename)) {        // Metadata
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_METADATA;
      ++dir_i;
    } else if(is_array(filename)){            // Array
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_ARRAY;
      ++dir_i;
    } else if(is_workspace(filename)){        // Workspace
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_WORKSPACE;
      ++dir_i;
    } 
  } 

  // Close parent directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close parent directory; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Set the number of directories
  dir_num = dir_i;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::clear(const std::string& dir) const {
  if(is_workspace(dir)) {
    return workspace_clear(dir);
  } else if(is_group(dir)) {
    return group_clear(dir);
  } else if(is_array(dir)) {
    return array_clear(dir);
  } else if(is_metadata(dir)) {
    return metadata_clear(dir);
  } else {
    PRINT_ERROR("Clear failed; Invalid directory");
    return TILEDB_SM_ERR;
  }
}

int StorageManager::delete_entire(const std::string& dir) const {
  if(is_workspace(dir)) {
    return workspace_delete(dir);
  } else if(is_group(dir)) {
    return group_delete(dir);
  } else if(is_array(dir)) {
    return array_delete(dir);
  } else if(is_metadata(dir)) {
    return metadata_delete(dir);
  } else {
    PRINT_ERROR("Clear failed; Invalid directory");
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::move(
    const std::string& old_dir,
    const std::string& new_dir) const {
  if(is_workspace(old_dir)) {
    return workspace_move(old_dir, new_dir);
  } else if(is_group(old_dir)) {
    return group_move(old_dir, new_dir);
  } else if(is_array(old_dir)) {
    return array_move(old_dir, new_dir);
  } else if(is_metadata(old_dir)) {
    return metadata_move(old_dir, new_dir);
  } else {
    PRINT_ERROR("Move failed; Invalid source directory");
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}




/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int StorageManager::array_clear(
    const std::string& array) const {
  // Get real array directory name
  std::string array_real = ::real_dir(array);

  // Check if the array exists
  if(!is_array(array_real)) {
    PRINT_ERROR(std::string("Array '") + array_real + 
                "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Delete the entire array directory except for the array schema file
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(array_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open array directory; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, TILEDB_ARRAY_SCHEMA_FILENAME))
      continue;
    filename = array_real + "/" + next_file->d_name;
    if(is_metadata(filename)) {         // Metadata
      metadata_delete(filename);
    } else if(is_fragment(filename)){   // Fragment
      if(delete_dir(filename) != TILEDB_UT_OK)
        return TILEDB_SM_ERR;
    } else {                            // Non TileDB related
      PRINT_ERROR(std::string("Cannot delete non TileDB related element '") +
                  filename + "'");
      return TILEDB_SM_ERR;
    }
  } 

  // Close array directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the array directory; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_delete(
    const std::string& array) const {
  // Clear the array
  if(array_clear(array) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Delete array directory
  if(delete_dir(array) != TILEDB_UT_OK)
    return TILEDB_SM_ERR; 

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_move(
    const std::string& old_array,
    const std::string& new_array) const {
  // Get real array directory name
  std::string old_array_real = real_dir(old_array);
  std::string new_array_real = real_dir(new_array);

  // Check if the old array exists
  if(!is_array(old_array_real)) {
    PRINT_ERROR(std::string("Array '") + old_array_real + 
                "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Make sure that the new array is not an existing directory
  if(is_dir(new_array_real)) {
    PRINT_ERROR(std::string("Directory '") + 
                new_array_real + "' already exists");
    return TILEDB_SM_ERR;
  }

  // Check if the new array are inside a workspace or group
  std::string new_array_parent_folder = parent_dir(new_array_real);
  if(!is_group(new_array_parent_folder) && 
     !is_workspace(new_array_parent_folder)) {
    PRINT_ERROR(std::string("Folder '") + new_array_parent_folder + "' must "
                "be either a workspace or a group");
    return TILEDB_SM_ERR;
  }

  // Rename array
  if(rename(old_array_real.c_str(), new_array_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move array; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::config_set(const char* config_filename) {
  // Success
  return TILEDB_SM_OK;
} 

void StorageManager::config_set_default() {
}

int StorageManager::create_group_file(const std::string& group) const {
  // Create file
  std::string filename = group + "/" + TILEDB_GROUP_FILENAME;
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1 || ::close(fd)) {
    PRINT_ERROR(std::string("Failed to create group file; ") +
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::create_master_catalog_entry(
    const std::string& workspace,
    MasterCatalogOp op) const {
  // Get real workspace path
  std::string real_workspace = ::real_dir(workspace);

  // Initialize master catalog
  Metadata* metadata;
  if(metadata_init(
         metadata, 
         master_catalog_dir_.c_str(), 
         TILEDB_METADATA_WRITE, 
         NULL, 
         0) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Write entry
  const char empty_char = TILEDB_EMPTY_CHAR;
  const char* entry_var = 
      (op == TILEDB_SM_MC_INS) ? real_workspace.c_str() : &empty_char;
  const size_t entry[] = { 0 };
  const void* buffers[] = { entry, entry_var };
  size_t entry_var_size = (op == TILEDB_SM_MC_INS) ? strlen(entry_var)+1 : 1;
  const size_t buffer_sizes[] = { sizeof(entry), entry_var_size };

  if(metadata->write(
         real_workspace.c_str(), 
         real_workspace.size()+1, 
         buffers, 
         buffer_sizes) != TILEDB_MT_OK)
    return TILEDB_SM_ERR;

  // Finalize master catalog
  if(metadata->finalize() != TILEDB_MT_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::create_workspace_file(const std::string& workspace) const {
  // Create file
  std::string filename = workspace + "/" + TILEDB_WORKSPACE_FILENAME;
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1 || ::close(fd)) {
    PRINT_ERROR(std::string("Failed to create workspace file; ") +
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::group_clear(
    const std::string& group) const {
  // Get real group path
  std::string group_real = ::real_dir(group); 

  // Check if group exists
  if(!is_group(group_real)) {
    PRINT_ERROR(std::string("Group '") + group_real + "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Do not delete if it is a workspace
  if(is_workspace(group_real)) {
    PRINT_ERROR(std::string("Group '") + group_real + "' is also a workspace");
    return TILEDB_SM_ERR;
  }

  // Delete all groups, arrays and metadata inside the group directory
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(group_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open group directory '") + 
                group_real + "'; " + strerror(errno));
    return TILEDB_SM_ERR;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, TILEDB_GROUP_FILENAME))
      continue;
    filename = group_real + "/" + next_file->d_name;
    if(is_group(filename)) {            // Group
      if(group_delete(filename))
        return TILEDB_SM_ERR;
    } else if(is_metadata(filename)) {  // Metadata
      if(metadata_delete(filename))
        return TILEDB_SM_ERR;
    } else if(is_array(filename)){      // Array
      if(array_delete(filename))
        return TILEDB_SM_ERR;
    } else {                            // Non TileDB related
      PRINT_ERROR(std::string("Cannot delete non TileDB related element '") +
                  filename + "'");
      return TILEDB_SM_ERR;
    }
  } 

  // Close group directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the group directory; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::group_delete(
    const std::string& group) const {
  // Clear the group
  if(group_clear(group) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Delete group directory
  if(delete_dir(group) != TILEDB_UT_OK)
    return TILEDB_SM_ERR; 

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::group_move(
    const std::string& old_group,
    const std::string& new_group) const {
  // Get real group directory names
  std::string old_group_real = real_dir(old_group);
  std::string new_group_real = real_dir(new_group);

  // Check if the old group is also a workspace
  if(is_workspace(old_group_real)) {
    PRINT_ERROR(std::string("Group '") + old_group_real + 
                "' is also a workspace");
    return TILEDB_SM_ERR;
  }

  // Check if the old group exists
  if(!is_group(old_group_real)) {
    PRINT_ERROR(std::string("Group '") + old_group_real + 
                "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Make sure that the new group is not an existing directory
  if(is_dir(new_group_real)) {
    PRINT_ERROR(std::string("Directory '") + 
                new_group_real + "' already exists");
    return TILEDB_SM_ERR;
  }

  // Check if the new group is inside a workspace or group
  std::string new_group_parent_folder = parent_dir(new_group_real);
  if(!is_group(new_group_parent_folder) && 
     !is_workspace(new_group_parent_folder)) {
    PRINT_ERROR(std::string("Folder '") + new_group_parent_folder + "' must "
                "be either a workspace or a group");
    return TILEDB_SM_ERR;
  }

  // Rename
  if(rename(old_group_real.c_str(), new_group_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move group; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::master_catalog_consolidate() const {
  // Initialize master catalog
  Metadata* metadata;
  if(metadata_init(
         metadata, 
         master_catalog_dir_.c_str(), 
         TILEDB_METADATA_READ, 
         NULL, 
         0) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;
  
  // Consolidate master catalog
  if(metadata->consolidate() != TILEDB_MT_OK)
    return TILEDB_SM_ERR;

  // Finalize master catalog
  if(metadata->finalize() != TILEDB_MT_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::master_catalog_create() const {
  // Create a metadata schema
  MetadataSchemaC metadata_schema_c = {};
  metadata_schema_c.metadata_name_ = 
      (char*) (tiledb_home_ + "/" + TILEDB_SM_MASTER_CATALOG).c_str();

 // Initialize array schema
  ArraySchema* array_schema = new ArraySchema();
  if(array_schema->init(&metadata_schema_c) != TILEDB_AS_OK) {
    delete array_schema;
    return TILEDB_SM_ERR;
  }

  // Create metadata with the new schema
  int rc = metadata_create(array_schema);

  // Clean up
  delete array_schema;

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}

int StorageManager::metadata_clear(
    const std::string& metadata) const {
  // Get real metadata directory name
  std::string metadata_real = ::real_dir(metadata);

  // Check if the metadata exists
  if(!is_metadata(metadata_real)) {
    PRINT_ERROR(std::string("Metadata '") + metadata_real + 
                "' do not exist");
    return TILEDB_SM_ERR;
  }

  // Delete the entire metadata directory except for the array schema file
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(metadata_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open metadata directory; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, TILEDB_METADATA_SCHEMA_FILENAME))
      continue;
    filename = metadata_real + "/" + next_file->d_name;
    if(is_fragment(filename)) {  // Fragment
      if(delete_dir(filename))
        return TILEDB_SM_ERR;
    } else {                     // Non TileDB related
      PRINT_ERROR(std::string("Cannot delete non TileDB related element '") +
                  filename + "'");
      return TILEDB_SM_ERR;
    }
  } 

  // Close metadata directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the metadata directory; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_delete(
    const std::string& metadata) const {
  // Get real metadata directory name
  std::string metadata_real = ::real_dir(metadata);

  // Clear the metadata
  if(metadata_clear(metadata_real))
    return TILEDB_SM_ERR;

  // Delete metadata directory
  if(delete_dir(metadata_real))
    return TILEDB_SM_ERR; 

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_move(
    const std::string& old_metadata,
    const std::string& new_metadata) const {
  // Get real metadata directory name
  std::string old_metadata_real = real_dir(old_metadata);
  std::string new_metadata_real = real_dir(new_metadata);

  // Check if the old metadata exists
  if(!is_metadata(old_metadata_real)) {
    PRINT_ERROR(std::string("Metadata '") + old_metadata_real + 
                "' do not exist");
    return TILEDB_SM_ERR;
  }

  // Make sure that the new metadata is not an existing directory
  if(is_dir(new_metadata_real)) {
    PRINT_ERROR(std::string("Directory '") + 
                new_metadata_real + "' already exists");
    return TILEDB_SM_ERR;
  }

  // Check if the new metadata are inside a workspace, group or array
  std::string new_metadata_parent_folder = parent_dir(new_metadata_real);
  if(!is_group(new_metadata_parent_folder) && 
     !is_workspace(new_metadata_parent_folder) &&
     !is_array(new_metadata_parent_folder)) {
    PRINT_ERROR(std::string("Folder '") + new_metadata_parent_folder + "' must "
                "be workspace, group or array");
    return TILEDB_SM_ERR;
  }

  // Rename metadata
  if(rename(old_metadata_real.c_str(), new_metadata_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move metadata; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::workspace_clear(const std::string& workspace) const {
  // Get real workspace path
  std::string workspace_real = real_dir(workspace); 

  // Delete all groups, arrays and metadata inside the workspace directory
  std::string filename; 
  struct dirent *next_file;
  DIR* dir = opendir(workspace_real.c_str());
  
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open workspace directory '") + 
               workspace_real + "'; " + strerror(errno));
    return TILEDB_SM_ERR;
  }

  while((next_file = readdir(dir))) {
    if(!strcmp(next_file->d_name, ".") ||
       !strcmp(next_file->d_name, "..") ||
       !strcmp(next_file->d_name, TILEDB_WORKSPACE_FILENAME) ||
       !strcmp(next_file->d_name, TILEDB_GROUP_FILENAME))
      continue;
    filename = workspace_real + "/" + next_file->d_name;
    if(is_group(filename)) {            // Group
      if(group_delete(filename))
        return TILEDB_SM_ERR;
    } else if(is_metadata(filename)) {  // Metadata
      if(metadata_delete(filename))
        return TILEDB_SM_ERR;
    } else if(is_array(filename)){      // Array
      if(array_delete(filename))
        return TILEDB_SM_ERR;
    } else {                            // Non TileDB related
      PRINT_ERROR(std::string("Cannot delete non TileDB related element '") +
                  filename + "'");
      return TILEDB_SM_ERR;
    }
  } 

  // Close workspace directory  
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close the workspace directory; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::workspace_delete(
    const std::string& workspace) const { 
  // Get real paths
  std::string workspace_real, master_catalog_real;
  workspace_real = real_dir(workspace);
  master_catalog_real = real_dir(master_catalog_dir_);

  // Check if workspace exists
  if(!is_workspace(workspace_real)) {
    PRINT_ERROR(std::string("Workspace '") + 
                workspace_real + "' does not exist");
    return TILEDB_SM_ERR;
  }  

  // Master catalog should exist
  if(!is_metadata(master_catalog_real)) {
    PRINT_ERROR(std::string("Master catalog '") + master_catalog_real + 
                            "' does not exist'");
    return TILEDB_SM_ERR;
  }  

  // Clear workspace 
  if(workspace_clear(workspace_real) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Delete directory
  if(delete_dir(workspace_real))
    return TILEDB_SM_ERR;

  // Update master catalog by deleting workspace 
  if(create_master_catalog_entry(workspace_real, TILEDB_SM_MC_DEL) != 
      TILEDB_SM_OK)
    return TILEDB_SM_ERR; 

  // Consolidate master catalog
  if(master_catalog_consolidate() != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::workspace_move(
    const std::string& old_workspace, 
    const std::string& new_workspace) const {
  // Get real paths
  std::string old_workspace_real = real_dir(old_workspace);
  std::string new_workspace_real = real_dir(new_workspace);
  std::string master_catalog_real = 
      real_dir(tiledb_home_ + "/" + TILEDB_SM_MASTER_CATALOG);

  // Check if old workspace exists
  if(!is_workspace(old_workspace_real)) {
    PRINT_ERROR(std::string("Workspace '") + 
                old_workspace_real + "' does not exist");
    return TILEDB_SM_ERR;
  }  

  // Check new workspace
  if(new_workspace_real == "") {
    PRINT_ERROR(std::string("Invalid workspace '") +
                new_workspace_real + "'"); 
    return TILEDB_SM_ERR;
  }
  if(is_dir(new_workspace_real)) {
    PRINT_ERROR(std::string("Directory '") + new_workspace_real + 
                            "' already exists"); 
    return TILEDB_SM_ERR;
  }

  // New workspace should not be inside another workspace, group array or 
  // metadata
  std::string new_workspace_real_parent = parent_dir(new_workspace_real);
  if(is_workspace(new_workspace_real_parent) ||
     is_group(new_workspace_real_parent) ||
     is_array(new_workspace_real_parent) ||
     is_metadata(new_workspace_real_parent)) {
    PRINT_ERROR(std::string("Folder '") + new_workspace_real_parent + 
                            "' should not be a workspace, group, "
                            "array, or metadata");
    return TILEDB_SM_ERR;
  }

  // Master catalog should exist
  if(!is_metadata(master_catalog_real)) {
    PRINT_ERROR(std::string("Master catalog '") + master_catalog_real + 
                            "' does not exist'");
    return TILEDB_SM_ERR;
  }  

  // Rename directory 
  if(rename(old_workspace_real.c_str(), new_workspace_real.c_str())) {
    PRINT_ERROR(std::string("Cannot move group; ") + 
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  // Update master catalog by adding new workspace 
  if(create_master_catalog_entry(old_workspace_real, TILEDB_SM_MC_DEL) !=
     TILEDB_SM_OK)
    return TILEDB_SM_ERR;
  if(create_master_catalog_entry(new_workspace_real, TILEDB_SM_MC_INS) !=
     TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Consolidate master catalog
  if(master_catalog_consolidate() != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

