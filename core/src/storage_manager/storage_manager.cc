/**
 * @file   storage_manager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#define PRINT_ERROR(x) std::cerr << TILEDB_SM_ERRMSG << x << ".\n"
#else
#define PRINT_ERROR(x) \
  do {                 \
  } while (0)
#endif

#if defined HAVE_OPENMP && defined USE_PARALLEL_SORT
#include <parallel/algorithm>
#define SORT_LIB __gnu_parallel
#else
#include <algorithm>
#define SORT_LIB std
#endif

#define SORT_2(first, last) SORT_LIB::sort((first), (last))
#define SORT_3(first, last, comp) SORT_LIB::sort((first), (last), (comp))
#define GET_MACRO(_1, _2, _3, NAME, ...) NAME
#define SORT(...) GET_MACRO(__VA_ARGS__, SORT_3, SORT_2)(__VA_ARGS__)

namespace tiledb {

/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_sm_errmsg = "";

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManager::StorageManager() = default;

StorageManager::~StorageManager() = default;

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

int StorageManager::finalize() {
  if (config_ != nullptr)
    delete config_;

  return open_array_mtx_destroy();
}

int StorageManager::init(StorageManagerConfig* config) {
  // Set configuration parameters
  if (config_set(config) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Initialize mutexes and return
  return open_array_mtx_init();
}

/* ****************************** */
/*            WORKSPACE           */
/* ****************************** */

int StorageManager::workspace_create(const std::string& workspace) {
  // Check if the workspace is inside a workspace or another group
  std::string parent_dir = utils::parent_dir(workspace);
  if (utils::is_workspace(parent_dir) || utils::is_group(parent_dir) ||
      utils::is_array(parent_dir) || utils::is_metadata(parent_dir)) {
    std::string errmsg =
        "The workspace cannot be contained in another workspace, "
        "group, array or metadata directory";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Create workspace directory
  if (utils::create_dir(workspace) != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Create workspace file
  if (create_workspace_file(workspace) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::dir_type(const char* dir) {
  // Get real path
  std::string dir_real = utils::real_dir(dir);

  // Return type
  if (utils::is_workspace(dir_real))
    return TILEDB_WORKSPACE;
  else if (utils::is_group(dir_real))
    return TILEDB_GROUP;
  else if (utils::is_array(dir_real))
    return TILEDB_ARRAY;
  else if (utils::is_metadata(dir_real))
    return TILEDB_METADATA;
  else
    return -1;
}

/* ****************************** */
/*             GROUP              */
/* ****************************** */

int StorageManager::group_create(const std::string& group) const {
  // Check if the group is inside a workspace or another group
  std::string parent_dir = utils::parent_dir(group);
  if (!utils::is_workspace(parent_dir) && !utils::is_group(parent_dir)) {
    std::string errmsg =
        "The group must be contained in a workspace "
        "or another group";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Create group directory
  if (utils::create_dir(group) != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Create group file
  if (create_group_file(group) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

/* ****************************** */
/*             ARRAY              */
/* ****************************** */

int StorageManager::array_consolidate(const char* array_dir) {
  // Create an array object
  Array* array;
  if (array_init(array, array_dir, TILEDB_ARRAY_READ, nullptr, nullptr, 0) !=
      TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Consolidate array
  Fragment* new_fragment;
  std::vector<std::string> old_fragment_names;
  int rc_array_consolidate =
      array->consolidate(new_fragment, old_fragment_names);

  // Close the array
  int rc_array_close = array_close(array->array_schema()->array_name());

  // Finalize consolidation
  int rc_consolidation_finalize =
      consolidation_finalize(new_fragment, old_fragment_names);

  // Finalize array
  int rc_array_finalize = array->finalize();
  delete array;

  // Errors
  if (rc_array_consolidate != TILEDB_AR_OK) {
    tiledb_sm_errmsg = tiledb_ar_errmsg;
    return TILEDB_SM_ERR;
  }
  if (rc_array_close != TILEDB_SM_OK || rc_array_finalize != TILEDB_SM_OK ||
      rc_consolidation_finalize != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_create(const ArraySchemaC* array_schema_c) const {
  // Initialize array schema
  ArraySchema* array_schema = new ArraySchema();
  if (array_schema->init(array_schema_c) != TILEDB_AS_OK) {
    delete array_schema;
    tiledb_sm_errmsg = tiledb_as_errmsg;
    return TILEDB_SM_ERR;
  }

  // Get real array directory name
  std::string dir = array_schema->array_name();
  std::string parent_dir = utils::parent_dir(dir);

  // Check if the array directory is contained in a workspace, group or array
  if (!utils::is_workspace(parent_dir) && !utils::is_group(parent_dir)) {
    std::string errmsg = std::string("Cannot create array; Directory '") +
                         parent_dir + "' must be a TileDB workspace or group";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Create array with the new schema
  int rc = array_create(array_schema);

  // Clean up
  delete array_schema;

  // Return
  if (rc == TILEDB_SM_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}

int StorageManager::array_create(const ArraySchema* array_schema) const {
  // Check array schema
  if (array_schema == nullptr) {
    std::string errmsg = "Cannot create array; Empty array schema";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Create array directory
  std::string dir = array_schema->array_name();
  if (utils::create_dir(dir) != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Store array schema
  if (array_store_schema(dir, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Create consolidation filelock
  if (consolidation_filelock_create(dir) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

void StorageManager::array_get_fragment_names(
    const std::string& array, std::vector<std::string>& fragment_names) {
  // Get directory names in the array folder
  fragment_names = utils::get_fragment_dirs(utils::real_dir(array));

  // Sort the fragment names
  sort_fragment_names(fragment_names);
}

int StorageManager::array_load_book_keeping(
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    std::vector<BookKeeping*>& book_keeping,
    int mode) {
  // For easy reference
  int fragment_num = fragment_names.size();

  // Initialization
  book_keeping.resize(fragment_num);

  // Load the book-keeping for each fragment
  for (int i = 0; i < fragment_num; ++i) {
    // For easy reference
    int dense = !utils::is_file(
        fragment_names[i] + "/" + TILEDB_COORDS + TILEDB_FILE_SUFFIX);

    // Create new book-keeping structure for the fragment
    BookKeeping* f_book_keeping =
        new BookKeeping(array_schema, dense, fragment_names[i], mode);

    // Load book-keeping
    if (f_book_keeping->load() != TILEDB_BK_OK) {
      delete f_book_keeping;
      tiledb_sm_errmsg = tiledb_bk_errmsg;
      return TILEDB_SM_ERR;
    }

    // Append to the open array entry
    book_keeping[i] = f_book_keeping;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_load_schema(
    const char* array_dir, ArraySchema*& array_schema) const {
  // Get real array path
  std::string real_array_dir = utils::real_dir(array_dir);

  // Check if array exists
  if (!utils::is_array(real_array_dir)) {
    std::string errmsg = std::string("Cannot load array schema; Array '") +
                         real_array_dir + "' does not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Open array schema file
  std::string filename = real_array_dir + "/" + TILEDB_ARRAY_SCHEMA_FILENAME;
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    std::string errmsg = "Cannot load schema; File opening error";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  ssize_t buffer_size = st.st_size;

  if (buffer_size == 0) {
    std::string errmsg = "Cannot load array schema; Empty array schema file";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }
  void* buffer = malloc(buffer_size);

  // Load array schema
  ssize_t bytes_read = ::read(fd, buffer, buffer_size);
  if (bytes_read != buffer_size) {
    std::string errmsg = "Cannot load array schema; File reading error";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    free(buffer);
    return TILEDB_SM_ERR;
  }

  // Initialize array schema
  array_schema = new ArraySchema();
  if (array_schema->deserialize(buffer, buffer_size) != TILEDB_AS_OK) {
    free(buffer);
    delete array_schema;
    tiledb_sm_errmsg = tiledb_as_errmsg;
    return TILEDB_SM_ERR;
  }

  // Clean up
  free(buffer);
  if (::close(fd)) {
    delete array_schema;
    std::string errmsg = "Cannot load array schema; File closing error";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
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
    int attribute_num) {
  // Check array name length
  if (array_dir == nullptr || strlen(array_dir) > TILEDB_NAME_MAX_LEN) {
    std::string errmsg = "Invalid array name length";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Load array schema
  ArraySchema* array_schema;
  if (array_load_schema(array_dir, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Open the array
  OpenArray* open_array = nullptr;
  if (utils::array_read_mode(mode)) {
    if (array_open(utils::real_dir(array_dir), open_array, mode) !=
        TILEDB_SM_OK)
      return TILEDB_SM_ERR;
  }

  // Create the clone Array object
  Array* array_clone = new Array();
  int rc_clone = array_clone->init(
      array_schema,
      open_array->fragment_names_,
      open_array->book_keeping_,
      mode,
      attributes,
      attribute_num,
      subarray,
      config_);

  // Handle error
  if (rc_clone != TILEDB_AR_OK) {
    delete array_schema;
    delete array_clone;
    array = nullptr;
    if (utils::array_read_mode(mode))
      array_close(array_dir);
    return TILEDB_SM_ERR;
  }

  // Create actual array
  array = new Array();
  int rc = array->init(
      array_schema,
      open_array->fragment_names_,
      open_array->book_keeping_,
      mode,
      attributes,
      attribute_num,
      subarray,
      config_,
      array_clone);

  // Handle error
  if (rc != TILEDB_AR_OK) {
    delete array_schema;
    delete array;
    array = nullptr;
    if (utils::array_read_mode(mode))
      array_close(array_dir);
    tiledb_sm_errmsg = tiledb_as_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_finalize(Array* array) {
  // If the array is NULL, do nothing
  if (array == nullptr)
    return TILEDB_SM_OK;

  // Finalize and close the array
  int rc_finalize = array->finalize();
  int rc_close = TILEDB_SM_OK;
  if (array->read_mode())
    rc_close = array_close(array->array_schema()->array_name());

  // Clean up
  delete array;

  // Errors
  if (rc_close != TILEDB_SM_OK)
    return TILEDB_SM_ERR;
  if (rc_finalize != TILEDB_AR_OK) {
    tiledb_sm_errmsg = tiledb_ar_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_sync(Array* array) {
  // If the array is NULL, do nothing
  if (array == nullptr)
    return TILEDB_SM_OK;

  // Sync array
  if (array->sync() != TILEDB_AR_OK) {
    tiledb_sm_errmsg = tiledb_ar_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_sync_attribute(
    Array* array, const std::string& attribute) {
  // If the array is NULL, do nothing
  if (array == nullptr)
    return TILEDB_SM_OK;

  // Sync array
  if (array->sync_attribute(attribute) != TILEDB_AR_OK) {
    tiledb_sm_errmsg = tiledb_ar_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_iterator_init(
    ArrayIterator*& array_it,
    const char* array_dir,
    int mode,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // Create Array object. This also creates/updates an open array entry
  Array* array;
  if (array_init(array, array_dir, mode, subarray, attributes, attribute_num) !=
      TILEDB_SM_OK) {
    array_it = nullptr;
    return TILEDB_SM_ERR;
  }

  // Create ArrayIterator object
  array_it = new ArrayIterator();
  if (array_it->init(array, buffers, buffer_sizes) != TILEDB_AIT_OK) {
    array_finalize(array);
    delete array_it;
    array_it = nullptr;
    tiledb_sm_errmsg = tiledb_ait_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_iterator_finalize(ArrayIterator* array_it) {
  // If the array iterator is NULL, do nothing
  if (array_it == nullptr)
    return TILEDB_SM_OK;

  // Finalize and close array
  std::string array_name = array_it->array_name();
  int rc_finalize = array_it->finalize();
  int rc_close = array_close(array_name);

  // Clean up
  delete array_it;

  // Errors
  if (rc_finalize != TILEDB_AIT_OK) {
    tiledb_sm_errmsg = tiledb_ait_errmsg;
    return TILEDB_SM_ERR;
  }
  if (rc_close != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

/* ****************************** */
/*            METADATA            */
/* ****************************** */

int StorageManager::metadata_consolidate(const char* metadata_dir) {
  // Load metadata schema
  ArraySchema* array_schema;
  if (metadata_load_schema(metadata_dir, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Set attributes
  char** attributes;
  int attribute_num = array_schema->attribute_num();
  attributes = new char*[attribute_num + 1];
  for (int i = 0; i < attribute_num + 1; ++i) {
    const char* attribute = array_schema->attribute(i).c_str();
    size_t attribute_len = strlen(attribute);
    attributes[i] = new char[attribute_len + 1];
    strcpy(attributes[i], attribute);
  }

  // Create a metadata object
  Metadata* metadata;
  int rc_init = metadata_init(
      metadata,
      metadata_dir,
      TILEDB_METADATA_READ,
      (const char**)attributes,
      attribute_num + 1);

  // Clean up
  for (int i = 0; i < attribute_num + 1; ++i)
    delete[] attributes[i];
  delete[] attributes;
  delete array_schema;

  if (rc_init != TILEDB_MT_OK) {
    tiledb_sm_errmsg = tiledb_mt_errmsg;
    return TILEDB_SM_ERR;
  }

  // Consolidate metadata
  Fragment* new_fragment;
  std::vector<std::string> old_fragment_names;
  int rc_metadata_consolidate =
      metadata->consolidate(new_fragment, old_fragment_names);

  // Close the underlying array
  std::string array_name = metadata->array_schema()->array_name();
  int rc_array_close = array_close(array_name);

  // Finalize consolidation
  int rc_consolidation_finalize =
      consolidation_finalize(new_fragment, old_fragment_names);

  // Finalize metadata
  int rc_metadata_finalize = metadata->finalize();
  delete metadata;

  // Errors
  if (rc_metadata_consolidate != TILEDB_MT_OK) {
    tiledb_sm_errmsg = tiledb_mt_errmsg;
    return TILEDB_SM_ERR;
  }
  if (rc_array_close != TILEDB_SM_OK || rc_metadata_finalize != TILEDB_SM_OK ||
      rc_consolidation_finalize != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_create(
    const MetadataSchemaC* metadata_schema_c) const {
  // Initialize array schema
  ArraySchema* array_schema = new ArraySchema();
  if (array_schema->init(metadata_schema_c) != TILEDB_AS_OK) {
    delete array_schema;
    tiledb_sm_errmsg = tiledb_as_errmsg;
    return TILEDB_SM_ERR;
  }

  // Get real array directory name
  std::string dir = array_schema->array_name();
  std::string parent_dir = utils::parent_dir(dir);

  // Check if the array directory is contained in a workspace, group or array
  if (!utils::is_workspace(parent_dir) && !utils::is_group(parent_dir) &&
      !utils::is_array(parent_dir)) {
    std::string errmsg = std::string("Cannot create metadata; Directory '") +
                         parent_dir +
                         "' must be a TileDB workspace, group, or array";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Create array with the new schema
  int rc = metadata_create(array_schema);

  // Clean up
  delete array_schema;

  // Return
  if (rc == TILEDB_SM_OK)
    return TILEDB_SM_OK;
  else
    return TILEDB_SM_ERR;
}

int StorageManager::metadata_create(const ArraySchema* array_schema) const {
  // Check metadata schema
  if (array_schema == nullptr) {
    std::string errmsg = "Cannot create metadata; Empty metadata schema";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Create array directory
  std::string dir = array_schema->array_name();
  if (utils::create_dir(dir) != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Open metadata schema file
  std::string filename = dir + "/" + TILEDB_METADATA_SCHEMA_FILENAME;
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1) {
    std::string errmsg =
        std::string("Cannot create metadata; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Serialize metadata schema
  void* array_schema_bin;
  size_t array_schema_bin_size;
  if (array_schema->serialize(array_schema_bin, array_schema_bin_size) !=
      TILEDB_AS_OK) {
    tiledb_sm_errmsg = tiledb_as_errmsg;
    return TILEDB_SM_ERR;
  }

  // Store the array schema
  ssize_t bytes_written = ::write(fd, array_schema_bin, array_schema_bin_size);
  if (bytes_written != ssize_t(array_schema_bin_size)) {
    free(array_schema_bin);
    std::string errmsg =
        std::string("Cannot create metadata; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Clean up
  free(array_schema_bin);
  if (::close(fd)) {
    std::string errmsg =
        std::string("Cannot create metadata; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Create consolidation filelock
  if (consolidation_filelock_create(dir) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_load_schema(
    const char* metadata_dir, ArraySchema*& array_schema) const {
  // Get real array path
  std::string real_metadata_dir = utils::real_dir(metadata_dir);

  // Check if metadata exists
  if (!utils::is_metadata(real_metadata_dir)) {
    PRINT_ERROR(
        std::string("Cannot load metadata schema; Metadata '") +
        real_metadata_dir + "' does not exist");
    return TILEDB_SM_ERR;
  }

  // Open array schema file
  std::string filename =
      real_metadata_dir + "/" + TILEDB_METADATA_SCHEMA_FILENAME;
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    std::string errmsg = "Cannot load metadata schema; File opening error";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  ssize_t buffer_size = st.st_size;
  if (buffer_size == 0) {
    std::string errmsg =
        "Cannot load metadata schema; Empty metadata schema file";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }
  void* buffer = malloc(buffer_size);

  // Load array schema
  ssize_t bytes_read = ::read(fd, buffer, buffer_size);
  if (bytes_read != buffer_size) {
    free(buffer);
    std::string errmsg = "Cannot load metadata schema; File reading error";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Initialize array schema
  array_schema = new ArraySchema();
  if (array_schema->deserialize(buffer, buffer_size) == TILEDB_AS_ERR) {
    free(buffer);
    delete array_schema;
    tiledb_sm_errmsg = tiledb_as_errmsg;
    return TILEDB_SM_ERR;
  }

  // Clean up
  free(buffer);
  if (::close(fd)) {
    delete array_schema;
    std::string errmsg = "Cannot load metadata schema; File closing error";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
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
    int attribute_num) {
  // Check metadata name length
  if (metadata_dir == nullptr || strlen(metadata_dir) > TILEDB_NAME_MAX_LEN) {
    std::string errmsg = "Invalid metadata name length";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Load metadata schema
  ArraySchema* array_schema;
  if (metadata_load_schema(metadata_dir, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Open the array that implements the metadata
  OpenArray* open_array = nullptr;
  if (mode == TILEDB_METADATA_READ) {
    if (array_open(
            utils::real_dir(metadata_dir), open_array, TILEDB_ARRAY_READ) !=
        TILEDB_SM_OK)
      return TILEDB_SM_ERR;
  }

  // Create metadata object
  metadata = new Metadata();
  int rc = metadata->init(
      array_schema,
      open_array->fragment_names_,
      open_array->book_keeping_,
      mode,
      attributes,
      attribute_num,
      config_);

  // Return
  if (rc != TILEDB_MT_OK) {
    delete array_schema;
    delete metadata;
    metadata = nullptr;
    array_close(metadata_dir);
    tiledb_sm_errmsg = tiledb_mt_errmsg;
    return TILEDB_SM_ERR;
  } else {
    return TILEDB_SM_OK;
  }
}

int StorageManager::metadata_finalize(Metadata* metadata) {
  // If the metadata is NULL, do nothing
  if (metadata == nullptr)
    return TILEDB_SM_OK;

  // Finalize the metadata and close the underlying array
  std::string array_name = metadata->array_schema()->array_name();
  int mode = metadata->array()->mode();
  int rc_finalize = metadata->finalize();
  int rc_close = TILEDB_SM_OK;
  if (mode == TILEDB_METADATA_READ)
    rc_close = array_close(array_name);

  // Clean up
  delete metadata;

  // Errors
  if (rc_close != TILEDB_SM_OK)
    return TILEDB_SM_ERR;
  if (rc_finalize != TILEDB_MT_OK) {
    tiledb_sm_errmsg = tiledb_mt_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_iterator_init(
    MetadataIterator*& metadata_it,
    const char* metadata_dir,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // Create metadata object
  Metadata* metadata;
  if (metadata_init(
          metadata,
          metadata_dir,
          TILEDB_METADATA_READ,
          attributes,
          attribute_num) != TILEDB_SM_OK) {
    metadata_it = nullptr;
    return TILEDB_SM_ERR;
  }

  // Create MetadataIterator object
  metadata_it = new MetadataIterator();
  if (metadata_it->init(metadata, buffers, buffer_sizes) != TILEDB_MIT_OK) {
    metadata_finalize(metadata);
    delete metadata_it;
    metadata_it = nullptr;
    tiledb_sm_errmsg = tiledb_mit_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_iterator_finalize(MetadataIterator* metadata_it) {
  // If the metadata iterator is NULL, do nothing
  if (metadata_it == nullptr)
    return TILEDB_SM_OK;

  // Close array and finalize metadata
  std::string metadata_name = metadata_it->metadata_name();
  int rc_finalize = metadata_it->finalize();
  int rc_close = array_close(metadata_name);

  // Clean up
  delete metadata_it;

  // Errors
  if (rc_finalize != TILEDB_MIT_OK) {
    tiledb_mit_errmsg = tiledb_mit_errmsg;
    return TILEDB_SM_ERR;
  }
  if (rc_close != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Success
  return TILEDB_SM_OK;
}

/* ****************************** */
/*               MISC             */
/* ****************************** */

int StorageManager::ls(
    const char* parent_dir, char** dirs, int* dir_types, int& dir_num) const {
  // Get real parent directory
  std::string parent_dir_real = utils::real_dir(parent_dir);

  // Initialize directory counter
  int dir_i = 0;

  // List all groups and arrays inside the parent directory
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(parent_dir_real.c_str());

  if (dir == nullptr) {
    dir_num = 0;
    return TILEDB_SM_OK;
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, ".."))
      continue;
    filename = parent_dir_real + "/" + next_file->d_name;
    if (utils::is_group(filename)) {  // Group
      if (dir_i == dir_num) {
        std::string errmsg =
            "Cannot list TileDB directory; Directory buffer overflow";
        PRINT_ERROR(errmsg);
        tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
        return TILEDB_SM_ERR;
      }
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_GROUP;
      ++dir_i;
    } else if (utils::is_metadata(filename)) {  // Metadata
      if (dir_i == dir_num) {
        std::string errmsg =
            "Cannot list TileDB directory; Directory buffer overflow";
        PRINT_ERROR(errmsg);
        tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
        return TILEDB_SM_ERR;
      }
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_METADATA;
      ++dir_i;
    } else if (utils::is_array(filename)) {  // Array
      if (dir_i == dir_num) {
        std::string errmsg =
            "Cannot list TileDB directory; Directory buffer overflow";
        PRINT_ERROR(errmsg);
        tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
        return TILEDB_SM_ERR;
      }
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_ARRAY;
      ++dir_i;
    } else if (utils::is_workspace(filename)) {  // Workspace
      if (dir_i == dir_num) {
        std::string errmsg =
            "Cannot list TileDB directory; Directory buffer overflow";
        PRINT_ERROR(errmsg);
        tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
        return TILEDB_SM_ERR;
      }
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_WORKSPACE;
      ++dir_i;
    }
  }

  // Close parent directory
  if (closedir(dir)) {
    std::string errmsg =
        std::string("Cannot close parent directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Set the number of directories
  dir_num = dir_i;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::ls_c(const char* parent_dir, int& dir_num) const {
  // Get real parent directory
  std::string parent_dir_real = utils::real_dir(parent_dir);

  // Initialize directory number
  dir_num = 0;

  // List all groups and arrays inside the parent directory
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(parent_dir_real.c_str());

  if (dir == nullptr) {
    dir_num = 0;
    return TILEDB_SM_OK;
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, ".."))
      continue;
    filename = parent_dir_real + "/" + next_file->d_name;
    if (utils::is_group(filename) || utils::is_metadata(filename) ||
        utils::is_array(filename) || utils::is_workspace(filename))
      ++dir_num;
  }

  // Close parent directory
  if (closedir(dir)) {
    std::string errmsg =
        std::string("Cannot close parent directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::clear(const std::string& dir) const {
  if (utils::is_workspace(dir)) {
    return workspace_clear(dir);
  } else if (utils::is_group(dir)) {
    return group_clear(dir);
  } else if (utils::is_array(dir)) {
    return array_clear(dir);
  } else if (utils::is_metadata(dir)) {
    return metadata_clear(dir);
  } else {
    std::string errmsg = "Clear failed; Invalid directory";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }
}

int StorageManager::delete_entire(const std::string& dir) {
  if (utils::is_workspace(dir)) {
    return workspace_delete(dir);
  } else if (utils::is_group(dir)) {
    return group_delete(dir);
  } else if (utils::is_array(dir)) {
    return array_delete(dir);
  } else if (utils::is_metadata(dir)) {
    return metadata_delete(dir);
  } else {
    std::string errmsg = "Delete failed; Invalid directory";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::move(
    const std::string& old_dir, const std::string& new_dir) {
  if (utils::is_workspace(old_dir)) {
    return workspace_move(old_dir, new_dir);
  } else if (utils::is_group(old_dir)) {
    return group_move(old_dir, new_dir);
  } else if (utils::is_array(old_dir)) {
    return array_move(old_dir, new_dir);
  } else if (utils::is_metadata(old_dir)) {
    return metadata_move(old_dir, new_dir);
  } else {
    std::string errmsg = "Move failed; Invalid source directory";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int StorageManager::array_clear(const std::string& array) const {
  // Get real array directory name
  std::string array_real = utils::real_dir(array);

  // Check if the array exists
  if (!utils::is_array(array_real)) {
    std::string errmsg =
        std::string("Array '") + array_real + "' does not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Delete the entire array directory except for the array schema file
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(array_real.c_str());

  if (dir == nullptr) {
    std::string errmsg =
        std::string("Cannot open array directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, "..") ||
        !strcmp(next_file->d_name, TILEDB_ARRAY_SCHEMA_FILENAME) ||
        !strcmp(next_file->d_name, TILEDB_SM_CONSOLIDATION_FILELOCK_NAME))
      continue;
    filename = array_real + "/" + next_file->d_name;
    if (utils::is_metadata(filename)) {  // Metadata
      metadata_delete(filename);
    } else if (utils::is_fragment(filename)) {  // Fragment
      if (utils::delete_dir(filename) != TILEDB_UT_OK)
        tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
      return TILEDB_SM_ERR;
    } else {  // Non TileDB related
      std::string errmsg =
          std::string("Cannot delete non TileDB related element '") + filename +
          "'";
      PRINT_ERROR(errmsg);
      tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
      return TILEDB_SM_ERR;
    }
  }

  // Close array directory
  if (closedir(dir)) {
    std::string errmsg =
        std::string("Cannot close the array directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_close(const std::string& array) {
  // Lock mutexes
  if (open_array_mtx_lock() != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Find the open array entry
  std::map<std::string, OpenArray*>::iterator it =
      open_arrays_.find(utils::real_dir(array));

  // Sanity check
  if (it == open_arrays_.end()) {
    std::string errmsg = "Cannot close array; Open array entry not found";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Lock the mutex of the array
  if (it->second->mutex_lock() != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Decrement counter
  --(it->second->cnt_);

  // Delete open array entry if necessary
  int rc_mtx_destroy = TILEDB_SM_OK;
  int rc_filelock = TILEDB_SM_OK;
  if (it->second->cnt_ == 0) {
    // Clean up book-keeping
    std::vector<BookKeeping*>::iterator bit = it->second->book_keeping_.begin();
    for (; bit != it->second->book_keeping_.end(); ++bit)
      delete *bit;

    // Unlock and destroy mutexes
    it->second->mutex_unlock();
    rc_mtx_destroy = it->second->mutex_destroy();

    // Unlock consolidation filelock
    rc_filelock =
        consolidation_filelock_unlock(it->second->consolidation_filelock_);

    // Delete array schema
    if (it->second->array_schema_ != nullptr)
      delete it->second->array_schema_;

    // Free open array
    delete it->second;

    // Delete open array entry
    open_arrays_.erase(it);
  } else {
    // Unlock the mutex of the array
    if (it->second->mutex_unlock() != TILEDB_SM_OK)
      return TILEDB_SM_ERR;
  }

  // Unlock mutexes
  int rc_mtx_unlock = open_array_mtx_unlock();

  // Return
  if (rc_mtx_destroy != TILEDB_SM_OK || rc_filelock != TILEDB_SM_OK ||
      rc_mtx_unlock != TILEDB_SM_OK)
    return TILEDB_SM_ERR;
  else
    return TILEDB_SM_OK;
}

int StorageManager::array_delete(const std::string& array) const {
  // Clear the array
  if (array_clear(array) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Delete array directory
  if (utils::delete_dir(array) != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_get_open_array_entry(
    const std::string& array, OpenArray*& open_array) {
  // Lock mutexes
  if (open_array_mtx_lock() != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Find the open array entry
  std::map<std::string, OpenArray*>::iterator it = open_arrays_.find(array);
  // Create and init entry if it does not exist
  if (it == open_arrays_.end()) {
    open_array = new OpenArray();
    open_array->cnt_ = 0;
    open_array->consolidation_filelock_ = -1;
    open_array->book_keeping_ = std::vector<BookKeeping*>();
    if (open_array->mutex_init() != TILEDB_SM_OK) {
      open_array->mutex_unlock();
      return TILEDB_SM_ERR;
    }
    open_arrays_[array] = open_array;
  } else {
    open_array = it->second;
  }

  // Increment counter
  ++(open_array->cnt_);

  // Unlock mutexes
  if (open_array_mtx_unlock() != TILEDB_SM_OK) {
    --open_array->cnt_;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_move(
    const std::string& old_array, const std::string& new_array) const {
  // Get real array directory name
  std::string old_array_real = utils::real_dir(old_array);
  std::string new_array_real = utils::real_dir(new_array);

  // Check if the old array exists
  if (!utils::is_array(old_array_real)) {
    std::string errmsg =
        std::string("Array '") + old_array_real + "' does not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Make sure that the new array is not an existing directory
  if (utils::is_dir(new_array_real)) {
    std::string errmsg =
        std::string("Directory '") + new_array_real + "' already exists";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Check if the new array are inside a workspace or group
  std::string new_array_parent_folder = utils::parent_dir(new_array_real);
  if (!utils::is_group(new_array_parent_folder) &&
      !utils::is_workspace(new_array_parent_folder)) {
    std::string errmsg = std::string("Folder '") + new_array_parent_folder +
                         "' must "
                         "be either a workspace or a group";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Rename array
  if (rename(old_array_real.c_str(), new_array_real.c_str())) {
    std::string errmsg = std::string("Cannot move array; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Incorporate new name in the array schema
  ArraySchema* array_schema;
  if (array_load_schema(new_array_real.c_str(), array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;
  array_schema->set_array_name(new_array_real.c_str());

  // Store the new schema
  if (array_store_schema(new_array_real, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Clean up
  delete array_schema;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_open(
    const std::string& array_name, OpenArray*& open_array, int mode) {
  // Get the open array entry
  if (array_get_open_array_entry(array_name, open_array) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Lock the mutex of the array
  if (open_array->mutex_lock() != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // First time the array is opened
  if (open_array->fragment_names_.size() == 0) {
    // Acquire shared lock on consolidation filelock
    if (consolidation_filelock_lock(
            array_name,
            open_array->consolidation_filelock_,
            TILEDB_SM_SHARED_LOCK) != TILEDB_SM_OK) {
      open_array->mutex_unlock();
      return TILEDB_SM_ERR;
    }

    // Get the fragment names
    array_get_fragment_names(array_name, open_array->fragment_names_);

    // Get array schema
    if (utils::is_array(array_name)) {  // Array
      if (array_load_schema(array_name.c_str(), open_array->array_schema_) !=
          TILEDB_SM_OK)
        return TILEDB_SM_ERR;
    } else {  // Metadata
      if (metadata_load_schema(array_name.c_str(), open_array->array_schema_) !=
          TILEDB_SM_OK)
        return TILEDB_SM_ERR;
    }

    // Load the book-keeping for each fragment
    if (array_load_book_keeping(
            open_array->array_schema_,
            open_array->fragment_names_,
            open_array->book_keeping_,
            mode) != TILEDB_SM_OK) {
      delete open_array->array_schema_;
      open_array->array_schema_ = nullptr;
      open_array->mutex_unlock();
      return TILEDB_SM_ERR;
    }
  }

  // Unlock the mutex of the array
  if (open_array->mutex_unlock() != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::array_store_schema(
    const std::string& dir, const ArraySchema* array_schema) const {
  // Open array schema file
  std::string filename = dir + "/" + TILEDB_ARRAY_SCHEMA_FILENAME;
  remove(filename.c_str());
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1) {
    std::string errmsg = std::string("Cannot store schema; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Serialize array schema
  void* array_schema_bin;
  size_t array_schema_bin_size;
  if (array_schema->serialize(array_schema_bin, array_schema_bin_size) !=
      TILEDB_AS_OK) {
    tiledb_sm_errmsg = tiledb_as_errmsg;
    return TILEDB_SM_ERR;
  }

  // Store the array schema
  ssize_t bytes_written = ::write(fd, array_schema_bin, array_schema_bin_size);
  if (bytes_written != ssize_t(array_schema_bin_size)) {
    free(array_schema_bin);
    std::string errmsg = std::string("Cannot store schema; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Clean up
  free(array_schema_bin);
  if (::close(fd)) {
    std::string errmsg = std::string("Cannot store schema; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::config_set(StorageManagerConfig* config) {
  // Store config locally
  config_ = config;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::consolidation_filelock_create(
    const std::string& dir) const {
  // Create file
  std::string filename = dir + "/" + TILEDB_SM_CONSOLIDATION_FILELOCK_NAME;
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);

  // Handle error
  if (fd == -1) {
    std::string errmsg =
        std::string("Cannot create consolidation filelock; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Close the file
  if (::close(fd)) {
    std::string errmsg =
        std::string("Cannot close consolidation filelock; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::consolidation_filelock_lock(
    const std::string& array_name, int& fd, int lock_type) const {
  // Prepare the flock struct
  struct flock fl;
  if (lock_type == TILEDB_SM_SHARED_LOCK) {
    fl.l_type = F_RDLCK;
  } else if (lock_type == TILEDB_SM_EXCLUSIVE_LOCK) {
    fl.l_type = F_WRLCK;
  } else {
    std::string errmsg =
        "Cannot lock consolidation filelock; Invalid lock type";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;
  fl.l_pid = getpid();

  // Prepare the filelock name
  std::string array_name_real = utils::real_dir(array_name);
  std::string filename =
      array_name_real + "/" + TILEDB_SM_CONSOLIDATION_FILELOCK_NAME;

  // Open the file
  fd = ::open(filename.c_str(), O_RDWR);
  if (fd == -1) {
    std::string errmsg =
        "Cannot lock consolidation filelock; Cannot open filelock";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Acquire the lock
  if (fcntl(fd, F_SETLKW, &fl) == -1) {
    std::string errmsg = "Cannot lock consolidation filelock; Cannot lock";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::consolidation_filelock_unlock(int fd) const {
  if (::close(fd) == -1) {
    std::string errmsg =
        "Cannot unlock consolidation filelock; Cannot close filelock";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  } else {
    return TILEDB_SM_OK;
  }
}

int StorageManager::consolidation_finalize(
    Fragment* new_fragment,
    const std::vector<std::string>& old_fragment_names) {
  // Trivial case - there was no consolidation
  if (old_fragment_names.size() == 0)
    return TILEDB_SM_OK;

  // Acquire exclusive lock on consolidation filelock
  int fd;
  if (consolidation_filelock_lock(
          new_fragment->array()->array_schema()->array_name(),
          fd,
          TILEDB_SM_EXCLUSIVE_LOCK) != TILEDB_SM_OK) {
    delete new_fragment;
    return TILEDB_SM_ERR;
  }

  // Finalize new fragment - makes the new fragment visible to new reads
  int rc = new_fragment->finalize();
  delete new_fragment;
  if (rc != TILEDB_FG_OK) {
    tiledb_sm_errmsg = tiledb_fg_errmsg;
    return TILEDB_SM_ERR;
  }

  // Make old fragments invisible to new reads
  int fragment_num = old_fragment_names.size();
  for (int i = 0; i < fragment_num; ++i) {
    // Delete special fragment file inside the fragment directory
    std::string old_fragment_filename =
        old_fragment_names[i] + "/" + TILEDB_FRAGMENT_FILENAME;
    if (remove(old_fragment_filename.c_str())) {
      std::string errmsg = std::string(
                               "Cannot remove fragment file during "
                               "finalizing consolidation; ") +
                           strerror(errno);
      PRINT_ERROR(errmsg);
      tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
      return TILEDB_SM_ERR;
    }
  }

  // Unlock consolidation filelock
  if (consolidation_filelock_unlock(fd) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Delete old fragments
  for (int i = 0; i < fragment_num; ++i) {
    if (utils::delete_dir(old_fragment_names[i]) != TILEDB_UT_OK) {
      tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
      return TILEDB_SM_ERR;
    }
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::create_group_file(const std::string& group) const {
  // Create file
  std::string filename = group + "/" + TILEDB_GROUP_FILENAME;
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1 || ::close(fd)) {
    std::string errmsg =
        std::string("Failed to create group file; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::create_workspace_file(const std::string& workspace) const {
  // Create file
  std::string filename = workspace + "/" + TILEDB_WORKSPACE_FILENAME;
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1 || ::close(fd)) {
    std::string errmsg =
        std::string("Failed to create workspace file; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::group_clear(const std::string& group) const {
  // Get real group path
  std::string group_real = utils::real_dir(group);

  // Check if group exists
  if (!utils::is_group(group_real)) {
    std::string errmsg =
        std::string("Group '") + group_real + "' does not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Do not delete if it is a workspace
  if (utils::is_workspace(group_real)) {
    std::string errmsg =
        std::string("Group '") + group_real + "' is also a workspace";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Delete all groups, arrays and metadata inside the group directory
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(group_real.c_str());

  if (dir == nullptr) {
    std::string errmsg = std::string("Cannot open group directory '") +
                         group_real + "'; " + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, "..") ||
        !strcmp(next_file->d_name, TILEDB_GROUP_FILENAME))
      continue;
    filename = group_real + "/" + next_file->d_name;
    if (utils::is_group(filename)) {  // Group
      if (group_delete(filename) != TILEDB_SM_OK)
        return TILEDB_SM_ERR;
    } else if (utils::is_metadata(filename)) {  // Metadata
      if (metadata_delete(filename) != TILEDB_SM_OK)
        return TILEDB_SM_ERR;
    } else if (utils::is_array(filename)) {  // Array
      if (array_delete(filename) != TILEDB_SM_OK)
        return TILEDB_SM_ERR;
    } else {  // Non TileDB related
      std::string errmsg =
          std::string("Cannot delete non TileDB related element '") + filename +
          "'";
      PRINT_ERROR(errmsg);
      tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
      return TILEDB_SM_ERR;
    }
  }

  // Close group directory
  if (closedir(dir)) {
    std::string errmsg =
        std::string("Cannot close the group directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::group_delete(const std::string& group) const {
  // Clear the group
  if (group_clear(group) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Delete group directory
  if (utils::delete_dir(group) != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::group_move(
    const std::string& old_group, const std::string& new_group) const {
  // Get real group directory names
  std::string old_group_real = utils::real_dir(old_group);
  std::string new_group_real = utils::real_dir(new_group);

  // Check if the old group is also a workspace
  if (utils::is_workspace(old_group_real)) {
    std::string errmsg =
        std::string("Group '") + old_group_real + "' is also a workspace";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Check if the old group exists
  if (!utils::is_group(old_group_real)) {
    std::string errmsg =
        std::string("Group '") + old_group_real + "' does not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Make sure that the new group is not an existing directory
  if (utils::is_dir(new_group_real)) {
    std::string errmsg =
        std::string("Directory '") + new_group_real + "' already exists";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Check if the new group is inside a workspace or group
  std::string new_group_parent_folder = utils::parent_dir(new_group_real);
  if (!utils::is_group(new_group_parent_folder) &&
      !utils::is_workspace(new_group_parent_folder)) {
    std::string errmsg = std::string("Folder '") + new_group_parent_folder +
                         "' must "
                         "be either a workspace or a group";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Rename
  if (rename(old_group_real.c_str(), new_group_real.c_str())) {
    std::string errmsg = std::string("Cannot move group; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_clear(const std::string& metadata) const {
  // Get real metadata directory name
  std::string metadata_real = utils::real_dir(metadata);

  // Check if the metadata exists
  if (!utils::is_metadata(metadata_real)) {
    std::string errmsg =
        std::string("Metadata '") + metadata_real + "' do not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Delete the entire metadata directory except for the array schema file
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(metadata_real.c_str());

  if (dir == nullptr) {
    std::string errmsg =
        std::string("Cannot open metadata directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, "..") ||
        !strcmp(next_file->d_name, TILEDB_METADATA_SCHEMA_FILENAME) ||
        !strcmp(next_file->d_name, TILEDB_SM_CONSOLIDATION_FILELOCK_NAME))
      continue;
    filename = metadata_real + "/" + next_file->d_name;
    if (utils::is_fragment(filename)) {  // Fragment
      if (utils::delete_dir(filename) != TILEDB_UT_OK) {
        tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
        return TILEDB_SM_ERR;
      }
    } else {  // Non TileDB related
      std::string errmsg =
          std::string("Cannot delete non TileDB related element '") + filename +
          "'";
      PRINT_ERROR(errmsg);
      tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
      return TILEDB_SM_ERR;
    }
  }

  // Close metadata directory
  if (closedir(dir)) {
    std::string errmsg =
        std::string("Cannot close the metadata directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_delete(const std::string& metadata) const {
  // Get real metadata directory name
  std::string metadata_real = utils::real_dir(metadata);

  // Clear the metadata
  if (metadata_clear(metadata_real) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Delete metadata directory
  if (utils::delete_dir(metadata_real) != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::metadata_move(
    const std::string& old_metadata, const std::string& new_metadata) const {
  // Get real metadata directory name
  std::string old_metadata_real = utils::real_dir(old_metadata);
  std::string new_metadata_real = utils::real_dir(new_metadata);

  // Check if the old metadata exists
  if (!utils::is_metadata(old_metadata_real)) {
    std::string errmsg =
        std::string("Metadata '") + old_metadata_real + "' do not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Make sure that the new metadata is not an existing directory
  if (utils::is_dir(new_metadata_real)) {
    std::string errmsg =
        std::string("Directory '") + new_metadata_real + "' already exists";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Check if the new metadata are inside a workspace, group or array
  std::string new_metadata_parent_folder = utils::parent_dir(new_metadata_real);
  if (!utils::is_group(new_metadata_parent_folder) &&
      !utils::is_workspace(new_metadata_parent_folder) &&
      !utils::is_array(new_metadata_parent_folder)) {
    std::string errmsg = std::string("Folder '") + new_metadata_parent_folder +
                         "' must "
                         "be workspace, group or array";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Rename metadata
  if (rename(old_metadata_real.c_str(), new_metadata_real.c_str())) {
    std::string errmsg =
        std::string("Cannot move metadata; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Incorporate new name in the array schema
  ArraySchema* array_schema;
  if (array_load_schema(new_metadata_real.c_str(), array_schema) !=
      TILEDB_SM_OK)
    return TILEDB_SM_ERR;
  array_schema->set_array_name(new_metadata_real.c_str());

  // Store the new schema
  if (array_store_schema(new_metadata_real, array_schema) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Clean up
  delete array_schema;

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::open_array_mtx_destroy() {
#ifdef HAVE_OPENMP
  int rc_omp_mtx = utils::mutex_destroy(&open_array_omp_mtx_);
#else
  int rc_omp_mtx = TILEDB_UT_OK;
#endif
  int rc_pthread_mtx = utils::mutex_destroy(&open_array_pthread_mtx_);

  // Errors
  if (rc_pthread_mtx != TILEDB_UT_OK || rc_omp_mtx != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::open_array_mtx_init() {
#ifdef HAVE_OPENMP
  int rc_omp_mtx = utils::mutex_init(&open_array_omp_mtx_);
#else
  int rc_omp_mtx = TILEDB_UT_OK;
#endif
  int rc_pthread_mtx = utils::mutex_init(&open_array_pthread_mtx_);

  // Errors
  if (rc_pthread_mtx != TILEDB_UT_OK || rc_omp_mtx != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::open_array_mtx_lock() {
#ifdef HAVE_OPENMP
  int rc_omp_mtx = utils::mutex_lock(&open_array_omp_mtx_);
#else
  int rc_omp_mtx = TILEDB_UT_OK;
#endif
  int rc_pthread_mtx = utils::mutex_lock(&open_array_pthread_mtx_);

  // Errors
  if (rc_pthread_mtx != TILEDB_UT_OK || rc_omp_mtx != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::open_array_mtx_unlock() {
#ifdef HAVE_OPENMP
  int rc_omp_mtx = utils::mutex_unlock(&open_array_omp_mtx_);
#else
  int rc_omp_mtx = TILEDB_UT_OK;
#endif
  int rc_pthread_mtx = utils::mutex_unlock(&open_array_pthread_mtx_);

  // Errors
  if (rc_pthread_mtx != TILEDB_UT_OK || rc_omp_mtx != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

void StorageManager::sort_fragment_names(
    std::vector<std::string>& fragment_names) const {
  // Initializations
  int fragment_num = fragment_names.size();
  std::string t_str;
  int64_t stripped_fragment_name_size, t;
  std::vector<std::pair<int64_t, int>> t_pos_vec;
  t_pos_vec.resize(fragment_num);

  // Get the timestamp for each fragment
  for (int i = 0; i < fragment_num; ++i) {
    // Strip fragment name
    std::string& fragment_name = fragment_names[i];
    std::string parent_fragment_name = utils::parent_dir(fragment_name);
    std::string stripped_fragment_name =
        fragment_name.substr(parent_fragment_name.size() + 1);
    assert(utils::starts_with(stripped_fragment_name, "__"));
    stripped_fragment_name_size = stripped_fragment_name.size();

    // Search for the timestamp in the end of the name after '_'
    for (int j = 2; j < stripped_fragment_name_size; ++j) {
      if (stripped_fragment_name[j] == '_') {
        t_str = stripped_fragment_name.substr(
            j + 1, stripped_fragment_name_size - j);
        sscanf(t_str.c_str(), "%lld", (long long int*)&t);
        t_pos_vec[i] = std::pair<int64_t, int>(t, i);
        break;
      }
    }
  }

  // Sort the names based on the timestamps
  SORT(t_pos_vec.begin(), t_pos_vec.end());
  std::vector<std::string> fragment_names_sorted;
  fragment_names_sorted.resize(fragment_num);
  for (int i = 0; i < fragment_num; ++i)
    fragment_names_sorted[i] = fragment_names[t_pos_vec[i].second];
  fragment_names = fragment_names_sorted;
}

int StorageManager::workspace_clear(const std::string& workspace) const {
  // Get real workspace path
  std::string workspace_real = utils::real_dir(workspace);

  // Delete all groups, arrays and metadata inside the workspace directory
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(workspace_real.c_str());

  if (dir == nullptr) {
    std::string errmsg = std::string("Cannot open workspace directory '") +
                         workspace_real + "'; " + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, "..") ||
        !strcmp(next_file->d_name, TILEDB_WORKSPACE_FILENAME) ||
        !strcmp(next_file->d_name, TILEDB_GROUP_FILENAME))
      continue;
    filename = workspace_real + "/" + next_file->d_name;
    if (utils::is_group(filename)) {  // Group
      if (group_delete(filename) != TILEDB_SM_OK)
        return TILEDB_SM_ERR;
    } else if (utils::is_metadata(filename)) {  // Metadata
      if (metadata_delete(filename) != TILEDB_SM_OK)
        return TILEDB_SM_ERR;
    } else if (utils::is_array(filename)) {  // Array
      if (array_delete(filename) != TILEDB_SM_OK)
        return TILEDB_SM_ERR;
    } else {  // Non TileDB related
      std::string errmsg =
          std::string("Cannot delete non TileDB related element '") + filename +
          "'";
      PRINT_ERROR(errmsg);
      tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
      return TILEDB_SM_ERR;
    }
  }

  // Close workspace directory
  if (closedir(dir)) {
    std::string errmsg =
        std::string("Cannot close the workspace directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::workspace_delete(const std::string& workspace) {
  // Get real paths
  std::string workspace_real;
  workspace_real = utils::real_dir(workspace);

  // Check if workspace exists
  if (!utils::is_workspace(workspace_real)) {
    std::string errmsg =
        std::string("Workspace '") + workspace_real + "' does not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Clear workspace
  if (workspace_clear(workspace_real) != TILEDB_SM_OK)
    return TILEDB_SM_ERR;

  // Delete directory
  if (utils::delete_dir(workspace_real) != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::workspace_move(
    const std::string& old_workspace, const std::string& new_workspace) {
  // Get real paths
  std::string old_workspace_real = utils::real_dir(old_workspace);
  std::string new_workspace_real = utils::real_dir(new_workspace);

  // Check if old workspace exists
  if (!utils::is_workspace(old_workspace_real)) {
    std::string errmsg =
        std::string("Workspace '") + old_workspace_real + "' does not exist";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Check new workspace
  if (new_workspace_real == "") {
    std::string errmsg =
        std::string("Invalid workspace '") + new_workspace_real + "'";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }
  if (utils::is_dir(new_workspace_real)) {
    std::string errmsg =
        std::string("Directory '") + new_workspace_real + "' already exists";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // New workspace should not be inside another workspace, group array or
  // metadata
  std::string new_workspace_real_parent = utils::parent_dir(new_workspace_real);
  if (utils::is_workspace(new_workspace_real_parent) ||
      utils::is_group(new_workspace_real_parent) ||
      utils::is_array(new_workspace_real_parent) ||
      utils::is_metadata(new_workspace_real_parent)) {
    std::string errmsg =
        std::string("Folder '") + new_workspace_real_parent +
        "' should not be a workspace, group, array, or metadata";
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Rename directory
  if (rename(old_workspace_real.c_str(), new_workspace_real.c_str())) {
    std::string errmsg = std::string("Cannot move group; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_sm_errmsg = TILEDB_SM_ERRMSG + errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::OpenArray::mutex_destroy() {
#ifdef HAVE_OPENMP
  int rc_omp_mtx = utils::mutex_destroy(&omp_mtx_);
#else
  int rc_omp_mtx = TILEDB_UT_OK;
#endif
  int rc_pthread_mtx = utils::mutex_destroy(&pthread_mtx_);

  // Error
  if (rc_pthread_mtx != TILEDB_UT_OK || rc_omp_mtx != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::OpenArray::mutex_init() {
#ifdef HAVE_OPENMP
  int rc_omp_mtx = utils::mutex_init(&omp_mtx_);
#else
  int rc_omp_mtx = TILEDB_UT_OK;
#endif
  int rc_pthread_mtx = utils::mutex_init(&pthread_mtx_);

  // Error
  if (rc_pthread_mtx != TILEDB_UT_OK || rc_omp_mtx != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::OpenArray::mutex_lock() {
#ifdef HAVE_OPENMP
  int rc_omp_mtx = utils::mutex_lock(&omp_mtx_);
#else
  int rc_omp_mtx = TILEDB_UT_OK;
#endif
  int rc_pthread_mtx = utils::mutex_lock(&pthread_mtx_);

  // Error
  if (rc_pthread_mtx != TILEDB_UT_OK || rc_omp_mtx != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
}

int StorageManager::OpenArray::mutex_unlock() {
#ifdef HAVE_OPENMP
  int rc_omp_mtx = utils::mutex_unlock(&omp_mtx_);
#else
  int rc_omp_mtx = TILEDB_UT_OK;
#endif
  int rc_pthread_mtx = utils::mutex_unlock(&pthread_mtx_);

  // Error
  if (rc_pthread_mtx != TILEDB_UT_OK || rc_omp_mtx != TILEDB_UT_OK) {
    tiledb_sm_errmsg = utils::tiledb_ut_errmsg;
    return TILEDB_SM_ERR;
  }

  // Success
  return TILEDB_SM_OK;
};

};  // namespace tiledb