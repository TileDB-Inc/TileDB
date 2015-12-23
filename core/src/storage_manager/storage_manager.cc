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

#include "storage_manager.h"
#include <cstring>
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

StorageManager::StorageManager(const std::string& config_filename) {
  // Set configuration parameters
  if(config_set(config_filename) == TILEDB_SM_ERR)
    config_set_default();
}

StorageManager::~StorageManager() {
}

/* ****************************** */
/*           WORKSPACE            */
/* ****************************** */

bool StorageManager::is_workspace(const std::string& dir) const {
  // Check existence
  if(is_dir(dir) && 
     is_file(dir + "/" + TILEDB_SM_WORKSPACE_FILENAME)) 
    return true;
  else
    return false;
}

/* ****************************** */
/*             GROUP              */
/* ****************************** */

bool StorageManager::is_group(const std::string& dir) const {
  // Check existence
  if(is_dir(dir) && 
     is_file(dir + "/" + TILEDB_SM_GROUP_FILENAME)) 
    return true;
  else
    return false;
}

int StorageManager::group_create(const std::string& dir) const {
  // Check if the group is inside a workspace or another group
  std::string parent_dir = ::parent_dir(dir);
  if(!is_workspace(parent_dir) && !is_group(parent_dir)) {
    PRINT_ERROR("The group must be contained in a workspace "
                "or another group");
    return TILEDB_SM_ERR;
  }

  // Create group directory
  if(create_dir(dir) == TILEDB_UT_ERR)  
    return TILEDB_SM_ERR;

  // Create group file
  if(create_group_file(dir) == TILEDB_SM_ERR)
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

  // Create array with the new schema
  int rc = array_create(array_schema);
  
  // Clean up
  delete array_schema;

  // Return
  if(rc == TILEDB_AS_OK)
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

  // Get real array directory name
  std::string dir = array_schema->array_name();
  std::string parent_dir = ::parent_dir(dir);

  // Check if the array directory is contained in a workspace, group or array
  if(!is_workspace(parent_dir) && 
     !is_group(parent_dir) && 
     !is_array(parent_dir)) {
    PRINT_ERROR(std::string("Cannot create array; Directory '") + parent_dir + 
                "' must be a TileDB workspace, group, or array");
    return TILEDB_SM_ERR;
  }

  // Create array directory
  if(create_dir(dir) == TILEDB_UT_ERR) 
    return TILEDB_SM_ERR;

  // Open array schema file
  std::string filename = dir + "/" + TILEDB_SM_ARRAY_SCHEMA_FILENAME; 
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
  return 0;
}

int StorageManager::array_init(
    Array*& array,
    const char* dir,
    int mode,
    const void* range,
    const char** attributes,
    int attribute_num)  const {
  // Load array schema
  ArraySchema* array_schema;
  if(array_load_schema(dir, array_schema) == TILEDB_SM_ERR)
    return TILEDB_SM_ERR;

  // TODO: debugging
  array_schema->print();

  // Create Array object
  array = new Array();
  int rc = array->init(array_schema, mode, attributes, attribute_num, range);

  // Return
  if(rc == TILEDB_AR_ERR) {
    delete array;
    delete array_schema;
    return TILEDB_SM_ERR;
  } else {
    return TILEDB_SM_OK;
  }
}

int StorageManager::array_finalize(Array* array) const {
  if(array->finalize() == TILEDB_AR_ERR) {
    return TILEDB_SM_ERR;
  } else {
    delete array;
    return TILEDB_SM_OK;
  }
}

int StorageManager::array_load_schema(
    const char* dir,
    ArraySchema*& array_schema) const {
  // Get real array path
  std::string real_dir = ::real_dir(dir);

  // Check if array exists
  if(!is_array(real_dir)) {
    PRINT_ERROR(std::string("Cannot load array schema; Array '") + real_dir + 
                "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Open array schema file
  std::string filename = real_dir + "/" + TILEDB_SM_ARRAY_SCHEMA_FILENAME;
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
  if(array_schema->deserialize(buffer, buffer_size) == TILEDB_AS_ERR) {
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

int StorageManager::array_write(
    Array* array,
    const void** buffers,
    const size_t* buffer_sizes) const {
  if(array->write(buffers, buffer_sizes) == TILEDB_AR_ERR) {
    return TILEDB_SM_ERR;
  } else {
    delete array;
    return TILEDB_SM_OK;
  }
}

bool StorageManager::is_array(const std::string& dir) const {
  // Check existence
  if(is_dir(dir) && 
     is_file(dir + "/" + TILEDB_SM_ARRAY_SCHEMA_FILENAME)) 
    return true;
  else
    return false;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int StorageManager::config_set(const std::string& config_filename) {
  // TODO

  return TILEDB_SM_OK;
} 

void StorageManager::config_set_default() {
  // TODO
} 

int StorageManager::create_group_file(const std::string& dir) const {
  std::string filename = std::string(dir) + "/" + TILEDB_SM_GROUP_FILENAME;
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1 || ::close(fd)) {
    PRINT_ERROR(std::string("Failed to create group file; ") +
                strerror(errno));
    return TILEDB_SM_ERR;
  }

  return TILEDB_SM_OK;
}

