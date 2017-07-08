/**
 * @file   storage_manager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
#include <cassert>
#include "array.h"
#include "basic_array_schema.h"
#include "logger.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

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
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManager::StorageManager() {
  config_ = nullptr;
  logger_ = nullptr;
}

StorageManager::~StorageManager() {
  if (config_ != nullptr) {
    delete config_;
    config_ = nullptr;
  }

  if (logger_ != nullptr) {
    delete logger_;
    logger_ = nullptr;
  }
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

Status StorageManager::finalize() {
  if (config_ != nullptr) {
    delete config_;
    config_ = nullptr;
  }

  if (logger_ != nullptr) {
    delete logger_;
    logger_ = nullptr;
  }

  return open_array_mtx_destroy();
}

Status StorageManager::init(Configurator* config) {
  // Attach logger
  logger_ = new Logger();

  // Clear previous configurator
  if (config_ != nullptr)
    delete config_;

  // Create new configurator and clone the input
  config_ = new Configurator(config);

  // Initialize mutexes and return
  return open_array_mtx_init();
}

// TODO: Object types should be Enums
int StorageManager::dir_type(const char* dir) {
  // Get real path
  std::string dir_real = utils::real_dir(dir);

  // Return type
  if (utils::is_group(dir_real))
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

Status StorageManager::group_create(const std::string& group) const {
  // Create group directory
  RETURN_NOT_OK(utils::create_dir(group));

  // Create group file
  if (!create_group_file(group).ok())
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot create group file for group:  ").append(group)));

  return Status::Ok();
}

/* ****************************** */
/*          BASIC ARRAY           */
/* ****************************** */

Status StorageManager::basic_array_create(const char* name) const {
  // Initialize basic array schema
  BasicArraySchema* schema = new BasicArraySchema(name);

  // Create basic array
  return array_create(schema->array_schema());
}

/* ****************************** */
/*             ARRAY              */
/* ****************************** */

Status StorageManager::array_consolidate(const char* array_dir) {
  // Create an array object
  Array* array;
  RETURN_NOT_OK(
      array_init(array, array_dir, ArrayMode::READ, nullptr, nullptr, 0));

  // Consolidate array (TODO: unhandled error handling here)
  Fragment* new_fragment;
  std::vector<std::string> old_fragment_names;
  Status st_array_consolidate =
      array->consolidate(new_fragment, old_fragment_names);

  // Close the array
  Status st_array_close = array_close(array->array_schema()->array_name());

  // Finalize consolidation
  Status st_consolidation_finalize =
      consolidation_finalize(new_fragment, old_fragment_names);

  // Finalize array
  Status st_array_finalize = array->finalize();
  delete array;
  if (!st_array_finalize.ok()) {
    // TODO: Status:
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Could not finalize array: ").append(array_dir)));
  }
  if (!(st_array_close.ok() && !st_consolidation_finalize.ok()))
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Could not consolidate array: ").append(array_dir)));

  // Success
  return Status::Ok();
}

Status StorageManager::array_create(const ArraySchemaC* array_schema_c) const {
  // Initialize array schema
  ArraySchema* array_schema = new ArraySchema();
  RETURN_NOT_OK_ELSE(array_schema->init(array_schema_c), delete array_schema);

  // Get real array directory name
  std::string dir = array_schema->array_name();
  std::string parent_dir = utils::parent_dir(dir);

  // Create array with the new schema
  Status st = array_create(array_schema);

  delete array_schema;

  return st;
}

Status StorageManager::array_create(const ArraySchema* array_schema) const {
  // Check array schema
  if (array_schema == nullptr) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot create array; Empty array schema"));
  }

  // Create array directory
  std::string dir = array_schema->array_name();
  RETURN_NOT_OK(utils::create_dir(dir));

  // Store array schema
  RETURN_NOT_OK(array_schema->store(dir));

  // Create consolidation filelock
  RETURN_NOT_OK(consolidation_filelock_create(dir));

  // Success
  return Status::Ok();
}

// TODO (jcb): is it true that this cannot fail?
void StorageManager::array_get_fragment_names(
    const std::string& array, std::vector<std::string>& fragment_names) {
  // Get directory names in the array folder
  fragment_names = utils::get_fragment_dirs(utils::real_dir(array));
  // Sort the fragment names
  sort_fragment_names(fragment_names);
}

Status StorageManager::array_load_book_keeping(
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    std::vector<BookKeeping*>& book_keeping,
    ArrayMode mode) {
  // TODO (jcb): is this assumed to be always > 0?
  // For easy reference
  int fragment_num = fragment_names.size();

  // Initialization
  book_keeping.resize(fragment_num);

  // Load the book-keeping for each fragment
  for (int i = 0; i < fragment_num; ++i) {
    // For easy reference
    int dense = !utils::is_file(
        fragment_names[i] + "/" + Configurator::coords() +
        Configurator::file_suffix());

    // Create new book-keeping structure for the fragment
    BookKeeping* f_book_keeping =
        new BookKeeping(array_schema, dense, fragment_names[i], mode);

    // Load book-keeping
    RETURN_NOT_OK(f_book_keeping->load());

    // Append to the open array entry
    book_keeping[i] = f_book_keeping;
  }

  return Status::Ok();
}

Status StorageManager::array_init(
    Array*& array,
    const char* array_dir,
    ArrayMode mode,
    const void* subarray,
    const char** attributes,
    int attribute_num) {
  // For easy reference
  unsigned name_max_len = Configurator::name_max_len();

  // TODO: (jcb) this should be handled by the array object
  // Check array name length
  if (array_dir == nullptr || strlen(array_dir) > name_max_len) {
    return LOG_STATUS(Status::StorageManagerError("Invalid array name length"));
  }

  // Load array schema
  ArraySchema* array_schema = new ArraySchema();
  RETURN_NOT_OK_ELSE(array_schema->load(array_dir), delete array_schema);

  // Open the array
  OpenArray* open_array = nullptr;
  if (is_read_mode(mode)) {
    RETURN_NOT_OK(array_open(utils::real_dir(array_dir), open_array, mode));
  }

  Status st;
  // Create the clone Array object
  Array* array_clone = new Array();
  st = array_clone->init(
      array_schema,
      open_array->fragment_names_,
      open_array->book_keeping_,
      mode,
      attributes,
      attribute_num,
      subarray,
      config_);

  // Handle error
  if (!st.ok()) {
    delete array_schema;
    delete array_clone;
    array = nullptr;
    if (is_read_mode(mode))
      array_close(array_dir);
    return st;
  }

  // Create actual array
  array = new Array();
  st = array->init(
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
  if (!st.ok()) {
    delete array_schema;
    delete array;
    array = nullptr;
    if (is_read_mode(mode))
      array_close(array_dir);
  }

  // Return
  return st;
}

Status StorageManager::array_finalize(Array* array) {
  // If the array is NULL, do nothing
  if (array == nullptr)
    return Status::Ok();

  // Finalize and close the array
  RETURN_NOT_OK_ELSE(array->finalize(), delete array);
  if (array->read_mode())
    RETURN_NOT_OK_ELSE(
        array_close(array->array_schema()->array_name()), delete array)

  // Clean up
  delete array;

  return Status::Ok();
}

Status StorageManager::array_sync(Array* array) {
  // If the array is NULL, do nothing
  if (array == nullptr)
    return Status::Ok();

  // Sync array
  RETURN_NOT_OK(array->sync());
  return Status::Ok();
}

Status StorageManager::array_sync_attribute(
    Array* array, const std::string& attribute) {
  // If the array is NULL, do nothing
  if (array == nullptr)
    return Status::Ok();

  // Sync array
  RETURN_NOT_OK(array->sync_attribute(attribute));
  return Status::Ok();
}

Status StorageManager::array_iterator_init(
    ArrayIterator*& array_it,
    const char* array_dir,
    ArrayMode mode,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  array_it = nullptr;
  // Create Array object. This also creates/updates an open array entry
  Array* array;
  RETURN_NOT_OK(
      array_init(array, array_dir, mode, subarray, attributes, attribute_num));

  // Create ArrayIterator object
  array_it = new ArrayIterator();
  Status st = array_it->init(array, buffers, buffer_sizes);
  if (!st.ok()) {
    array_finalize(array);
    delete array_it;
    array_it = nullptr;
    return st;
  }
  return Status::Ok();
}

Status StorageManager::array_iterator_finalize(ArrayIterator* array_it) {
  // If the array iterator is NULL, do nothing
  if (array_it == nullptr)
    return Status::Ok();

  // Finalize and close array
  std::string array_name = array_it->array_name();
  Status st_finalize = array_it->finalize();
  Status st_close = array_close(array_name);

  delete array_it;

  if (!st_finalize.ok())
    return st_finalize;
  if (!st_close.ok())
    return st_close;

  return Status::Ok();
}

/* ****************************** */
/*            METADATA            */
/* ****************************** */

Status StorageManager::metadata_consolidate(const char* metadata_dir) {
  // Load metadata schema
  ArraySchema* array_schema;
  RETURN_NOT_OK_ELSE(
      metadata_load_schema(metadata_dir, array_schema), delete array_schema);

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
  Status st = metadata_init(
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

  if (!st.ok())
    return st;

  // Consolidate metadata
  Fragment* new_fragment;
  std::vector<std::string> old_fragment_names;
  // TODO: (jcb) does it make sense to execute these functions if one error's
  Status st_metadata_consolidate =
      metadata->consolidate(new_fragment, old_fragment_names);

  // Close the underlying array
  std::string array_name = metadata->array_schema()->array_name();
  Status st_array_close = array_close(array_name);

  // Finalize consolidation
  Status st_consolidation_finalize =
      consolidation_finalize(new_fragment, old_fragment_names);

  // Finalize metadata
  Status st_metadata_finalize = metadata->finalize();
  delete metadata;

  // Errors
  if (!st_array_close.ok()) {
    return st_array_close;
  }
  if (!st_consolidation_finalize.ok()) {
    return st_consolidation_finalize;
  }
  if (!st_metadata_consolidate.ok()) {
    return st_metadata_consolidate;
  }
  if (!st_metadata_finalize.ok()) {
    return st_metadata_finalize;
  }
  return Status::Ok();
}

Status StorageManager::metadata_create(
    const MetadataSchemaC* metadata_schema_c) const {
  // Initialize array schema
  ArraySchema* array_schema = new ArraySchema();
  RETURN_NOT_OK_ELSE(
      array_schema->init(metadata_schema_c), delete array_schema);

  // Get real array directory name
  std::string dir = array_schema->array_name();
  std::string parent_dir = utils::parent_dir(dir);

  // Create array with the new schema
  Status st = metadata_create(array_schema);

  // Clean up
  delete array_schema;

  return st;
}

Status StorageManager::metadata_create(const ArraySchema* array_schema) const {
  // Check metadata schema
  if (array_schema == nullptr) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot create metadata; Empty metadata schema"));
  }

  // Create array directory
  std::string dir = array_schema->array_name();
  RETURN_NOT_OK(utils::create_dir(dir));

  // Open metadata schema file
  std::string filename = dir + "/" + Configurator::metadata_schema_filename();
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot create metadata; ") + strerror(errno)));
  }

  // Serialize metadata schema
  void* array_schema_bin;
  size_t array_schema_bin_size;
  RETURN_NOT_OK(
      array_schema->serialize(array_schema_bin, array_schema_bin_size));

  // Store the array schema
  ssize_t bytes_written = ::write(fd, array_schema_bin, array_schema_bin_size);
  if (bytes_written != ssize_t(array_schema_bin_size)) {
    free(array_schema_bin);
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot create metadata; ") + strerror(errno)));
  }

  // Clean up
  free(array_schema_bin);
  if (::close(fd)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot create metadata; ") + strerror(errno)));
  }

  // Create consolidation filelock
  RETURN_NOT_OK(consolidation_filelock_create(dir))

  return Status::Ok();
}

Status StorageManager::metadata_load_schema(
    const char* metadata_dir, ArraySchema*& array_schema) const {
  // Get real array path
  std::string real_metadata_dir = utils::real_dir(metadata_dir);

  // Check if metadata exists
  if (!utils::is_metadata(real_metadata_dir)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot load metadata schema; Metadata '") +
        real_metadata_dir + "' does not exist"));
  }

  // Open array schema file
  auto filename =
      real_metadata_dir + "/" + Configurator::metadata_schema_filename();
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot load metadata schema; File opening error")));
  }

  // Initialize buffer
  struct stat _stat;
  fstat(fd, &_stat);
  ssize_t buffer_size = _stat.st_size;
  if (buffer_size == 0) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load metadata schema; Empty metadata schema file"));
  }
  void* buffer = malloc(buffer_size);

  // Load array schema
  ssize_t bytes_read = ::read(fd, buffer, buffer_size);
  if (bytes_read != buffer_size) {
    free(buffer);
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load metadata schema; File reading error"));
  }

  // Initialize array schema
  array_schema = new ArraySchema();
  Status st = array_schema->deserialize(buffer, buffer_size);
  // Clean up
  free(buffer);
  if (!st.ok()) {
    delete array_schema;
    return st;
  }
  if (::close(fd)) {
    delete array_schema;
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load metadata schema; File closing error"));
  }
  return Status::Ok();
}

Status StorageManager::metadata_init(
    Metadata*& metadata,
    const char* metadata_dir,
    tiledb_metadata_mode_t mode,
    const char** attributes,
    int attribute_num) {
  // For easy reference
  unsigned name_max_len = Configurator::name_max_len();

  // Check metadata name length
  if (metadata_dir == nullptr || strlen(metadata_dir) > name_max_len) {
    return LOG_STATUS(
        Status::StorageManagerError("Invalid metadata name length"));
  }

  // Load metadata schema
  ArraySchema* array_schema;
  RETURN_NOT_OK_ELSE(
      metadata_load_schema(metadata_dir, array_schema), delete array_schema);

  // Open the array that implements the metadata
  OpenArray* open_array = nullptr;
  if (mode == TILEDB_METADATA_READ)
    RETURN_NOT_OK(
        array_open(utils::real_dir(metadata_dir), open_array, ArrayMode::READ))

  // Create metadata object
  metadata = new Metadata();
  Status st = metadata->init(
      array_schema,
      open_array->fragment_names_,
      open_array->book_keeping_,
      mode,
      attributes,
      attribute_num,
      config_);

  // Return
  if (!st.ok()) {
    delete array_schema;
    delete metadata;
    array_close(metadata_dir);
    return st;
  }
  return Status::Ok();
}

Status StorageManager::metadata_finalize(Metadata* metadata) {
  // If the metadata is NULL, do nothing
  if (metadata == nullptr)
    return Status::Ok();

  // Finalize the metadata and close the underlying array
  std::string array_name = metadata->array_schema()->array_name();
  ArrayMode mode = metadata->array()->mode();
  RETURN_NOT_OK_ELSE(metadata->finalize(), delete metadata);
  if (mode == ArrayMode::READ)
    RETURN_NOT_OK_ELSE(array_close(array_name), delete metadata);

  // Clean up
  delete metadata;

  return Status::Ok();
}

Status StorageManager::metadata_iterator_init(
    MetadataIterator*& metadata_it,
    const char* metadata_dir,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  metadata_it = nullptr;
  // Create metadata object
  Metadata* metadata;
  RETURN_NOT_OK(metadata_init(
      metadata, metadata_dir, TILEDB_METADATA_READ, attributes, attribute_num))

  // Create MetadataIterator object
  metadata_it = new MetadataIterator();
  Status st = metadata_it->init(metadata, buffers, buffer_sizes);
  if (!st.ok()) {
    metadata_finalize(metadata);
    delete metadata_it;
    metadata_it = nullptr;
    return st;
  }
  return Status::Ok();
}

Status StorageManager::metadata_iterator_finalize(
    MetadataIterator* metadata_it) {
  // If the metadata iterator is NULL, do nothing
  if (metadata_it == nullptr)
    return Status::Ok();

  // Close array and finalize metadata
  std::string metadata_name = metadata_it->metadata_name();
  Status st_finalize = metadata_it->finalize();
  Status st_close = array_close(metadata_name);

  // Clean up
  delete metadata_it;

  if (!st_finalize.ok())
    return st_finalize;
  if (!st_close.ok())
    return st_close;
  return Status::Ok();
}

/* ****************************** */
/*               MISC             */
/* ****************************** */

Status StorageManager::ls(
    const char* parent_dir,
    char** dirs,
    tiledb_object_t* dir_types,
    int& dir_num) const {
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
    return Status::Ok();
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, ".."))
      continue;
    filename = parent_dir_real + "/" + next_file->d_name;
    if (utils::is_group(filename)) {  // Group
      if (dir_i == dir_num) {
        return LOG_STATUS(Status::StorageManagerError(
            "Cannot list TileDB directory; Directory buffer overflow"));
      }
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_GROUP;
      ++dir_i;
    } else if (utils::is_metadata(filename)) {  // Metadata
      if (dir_i == dir_num) {
        return LOG_STATUS(Status::StorageManagerError(
            "Cannot list TileDB directory; Directory buffer overflow"));
      }
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_METADATA;
      ++dir_i;
    } else if (utils::is_array(filename)) {  // Array
      if (dir_i == dir_num) {
        return LOG_STATUS(Status::StorageManagerError(
            "Cannot list TileDB directory; Directory buffer overflow"));
      }
      strcpy(dirs[dir_i], next_file->d_name);
      dir_types[dir_i] = TILEDB_ARRAY;
      ++dir_i;
    }
  }

  // Close parent directory
  if (closedir(dir)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot close parent directory; ") + strerror(errno)));
  }

  // Set the number of directories
  dir_num = dir_i;

  // Success
  return Status::Ok();
}

Status StorageManager::ls_c(const char* parent_dir, int& dir_num) const {
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
    return Status::Ok();
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, ".."))
      continue;
    filename = parent_dir_real + "/" + next_file->d_name;
    if (utils::is_group(filename) || utils::is_metadata(filename) ||
        utils::is_array(filename))
      ++dir_num;
  }

  // Close parent directory
  if (closedir(dir)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot close parent directory; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status StorageManager::clear(const std::string& dir) const {
  if (utils::is_group(dir)) {
    return group_clear(dir);
  } else if (utils::is_array(dir)) {
    return array_clear(dir);
  } else if (utils::is_metadata(dir)) {
    return metadata_clear(dir);
  } else {
    return LOG_STATUS(
        Status::StorageManagerError("Clear failed; Invalid directory"));
  }
}

Status StorageManager::delete_entire(const std::string& dir) {
  if (utils::is_group(dir)) {
    return group_delete(dir);
  } else if (utils::is_array(dir)) {
    return array_delete(dir);
  } else if (utils::is_metadata(dir)) {
    return metadata_delete(dir);
  } else {
    return LOG_STATUS(
        Status::StorageManagerError("Delete failed; Invalid directory"));
  }
}

Status StorageManager::move(
    const std::string& old_dir, const std::string& new_dir) {
  if (utils::is_group(old_dir)) {
    return group_move(old_dir, new_dir);
  } else if (utils::is_array(old_dir)) {
    return array_move(old_dir, new_dir);
  } else if (utils::is_metadata(old_dir)) {
    return metadata_move(old_dir, new_dir);
  } else {
    return LOG_STATUS(
        Status::StorageManagerError("Move failed; Invalid source directory"));
  }
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Status StorageManager::array_clear(const std::string& array) const {
  // Get real array directory name
  std::string array_real = utils::real_dir(array);

  // Check if the array exists
  if (!utils::is_array(array_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Array '") + array_real + "' does not exist"));
  }

  // Delete the entire array directory except for the array schema file
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(array_real.c_str());

  if (dir == nullptr) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot open array directory; ") + strerror(errno)));
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, "..") ||
        !strcmp(next_file->d_name, Configurator::array_schema_filename()) ||
        !strcmp(next_file->d_name, Configurator::consolidation_filelock_name()))
      continue;
    filename = array_real + "/" + next_file->d_name;
    if (utils::is_metadata(filename)) {  // Metadata
      metadata_delete(filename);
    } else if (utils::is_fragment(filename)) {  // Fragment
      RETURN_NOT_OK(utils::delete_dir(filename));
    } else {  // Non TileDB related
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot delete non TileDB related element '") + filename +
          "'"));
    }
  }

  // Close array directory
  if (closedir(dir)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot close the array directory; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status StorageManager::array_close(const std::string& array) {
  // Lock mutexes
  RETURN_NOT_OK(open_array_mtx_lock());

  // Find the open array entry
  std::map<std::string, OpenArray*>::iterator it =
      open_arrays_.find(utils::real_dir(array));

  // Sanity check
  if (it == open_arrays_.end()) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot close array; Open array entry not found"));
  }

  // Lock the mutex of the array
  RETURN_NOT_OK(it->second->mutex_lock());

  // Decrement counter
  --(it->second->cnt_);

  // Delete open array entry if necessary
  Status st_mtx_destroy = Status::Ok();
  Status st_filelock = Status::Ok();
  if (it->second->cnt_ == 0) {
    // Clean up book-keeping
    std::vector<BookKeeping*>::iterator bit = it->second->book_keeping_.begin();
    for (; bit != it->second->book_keeping_.end(); ++bit)
      delete *bit;

    // Unlock and destroy mutexes
    it->second->mutex_unlock();
    st_mtx_destroy = it->second->mutex_destroy();

    // Unlock consolidation filelock
    st_filelock =
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
    RETURN_NOT_OK(it->second->mutex_unlock());
  }
  // Unlock mutexes
  Status st_mtx_unlock = open_array_mtx_unlock();
  if (!st_mtx_destroy.ok())
    return st_mtx_destroy;
  if (!st_filelock.ok())
    return st_filelock;
  if (!st_mtx_unlock.ok())
    return st_mtx_unlock;
  return Status::Ok();
}

Status StorageManager::array_delete(const std::string& array) const {
  RETURN_NOT_OK(array_clear(array));
  // Delete array directory
  RETURN_NOT_OK(utils::delete_dir(array));
  return Status::Ok();
}

Status StorageManager::array_get_open_array_entry(
    const std::string& array, OpenArray*& open_array) {
  // Lock mutexes
  RETURN_NOT_OK(open_array_mtx_lock());

  // Find the open array entry
  std::map<std::string, OpenArray*>::iterator it = open_arrays_.find(array);
  // Create and init entry if it does not exist
  if (it == open_arrays_.end()) {
    open_array = new OpenArray();
    open_array->cnt_ = 0;
    open_array->consolidation_filelock_ = -1;
    open_array->book_keeping_ = std::vector<BookKeeping*>();
    RETURN_NOT_OK_ELSE(open_array->mutex_init(), open_array->mutex_unlock());
    open_arrays_[array] = open_array;
  } else {
    open_array = it->second;
  }

  // Increment counter
  ++(open_array->cnt_);

  // Unlock mutexes
  RETURN_NOT_OK_ELSE(open_array_mtx_unlock(), --open_array->cnt_);
  return Status::Ok();
}

Status StorageManager::array_move(
    const std::string& old_array, const std::string& new_array) const {
  // Get real array directory name
  std::string old_array_real = utils::real_dir(old_array);
  std::string new_array_real = utils::real_dir(new_array);

  // Check if the old array exists
  if (!utils::is_array(old_array_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Array '") + old_array_real + "' does not exist"));
  }

  // Make sure that the new array is not an existing directory
  if (utils::is_dir(new_array_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Directory '") + new_array_real + "' already exists"));
  }

  // Rename array
  if (rename(old_array_real.c_str(), new_array_real.c_str())) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot move array; ") + strerror(errno)));
  }

  // Incorporate new name in the array schema
  ArraySchema* array_schema = new ArraySchema();
  RETURN_NOT_OK_ELSE(array_schema->load(new_array_real), delete array_schema);
  array_schema->set_array_name(new_array_real.c_str());

  // Store the new schema
  RETURN_NOT_OK_ELSE(array_schema->store(new_array_real), delete array_schema);

  // Clean up
  delete array_schema;

  // Success
  return Status::Ok();
}

Status StorageManager::array_open(
    const std::string& array_name, OpenArray*& open_array, ArrayMode mode) {
  // Get the open array entry
  RETURN_NOT_OK(array_get_open_array_entry(array_name, open_array));

  // Lock the mutex of the array
  RETURN_NOT_OK(open_array->mutex_lock());

  // First time the array is opened
  if (open_array->fragment_names_.size() == 0) {
    // Acquire shared lock on consolidation filelock
    RETURN_NOT_OK_ELSE(
        consolidation_filelock_lock(
            array_name, open_array->consolidation_filelock_, SHARED_LOCK),
        open_array->mutex_unlock());

    // Get the fragment names
    array_get_fragment_names(array_name, open_array->fragment_names_);

    // Get array schema
    open_array->array_schema_ = new ArraySchema();
    ArraySchema* array_schema = open_array->array_schema_;
    if (utils::is_array(array_name)) {  // Array
      RETURN_NOT_OK_ELSE(array_schema->load(array_name), delete array_schema);
    } else {  // Metadata
      RETURN_NOT_OK_ELSE(
          metadata_load_schema(array_name.c_str(), array_schema),
          delete array_schema);
    }
    // Load the book-keeping for each fragment
    Status st = array_load_book_keeping(
        open_array->array_schema_,
        open_array->fragment_names_,
        open_array->book_keeping_,
        mode);
    if (!st.ok()) {
      delete open_array->array_schema_;
      open_array->array_schema_ = nullptr;
      open_array->mutex_unlock();
      return st;
    }
  }
  // Unlock the mutex of the array
  RETURN_NOT_OK(open_array->mutex_unlock());
  return Status::Ok();
}

Status StorageManager::consolidation_filelock_create(
    const std::string& dir) const {
  // Create file
  std::string filename =
      dir + "/" + Configurator::consolidation_filelock_name();
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);

  // Handle error
  if (fd == -1) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot create consolidation filelock; ") +
        strerror(errno)));
  }

  // Close the file
  if (::close(fd)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot close consolidation filelock; ") +
        strerror(errno)));
  }
  return Status::Ok();
}

Status StorageManager::consolidation_filelock_lock(
    const std::string& array_name, int& fd, int lock_type) const {
  // Prepare the flock struct
  struct flock fl;
  if (lock_type == SHARED_LOCK) {
    fl.l_type = F_RDLCK;
  } else if (lock_type == EXCLUSIVE_LOCK) {
    fl.l_type = F_WRLCK;
  } else {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot lock consolidation filelock; Invalid lock type"));
  }
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;
  fl.l_pid = getpid();

  // Prepare the filelock name
  std::string array_name_real = utils::real_dir(array_name);
  std::string filename =
      array_name_real + "/" + Configurator::consolidation_filelock_name();

  // Open the file
  fd = ::open(filename.c_str(), O_RDWR);
  if (fd == -1) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot lock consolidation filelock; Cannot open filelock"));
  }

  // Acquire the lock
  if (fcntl(fd, F_SETLKW, &fl) == -1) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot lock consolidation filelock; Cannot lock"));
  }
  return Status::Ok();
}

Status StorageManager::consolidation_filelock_unlock(int fd) const {
  if (::close(fd) == -1) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot unlock consolidation filelock; Cannot close filelock"));
  }
  return Status::Ok();
}

Status StorageManager::consolidation_finalize(
    Fragment* new_fragment,
    const std::vector<std::string>& old_fragment_names) {
  // Trivial case - there was no consolidation
  if (old_fragment_names.size() == 0)
    return Status::Ok();

  // Acquire exclusive lock on consolidation filelock
  int fd;
  Status st;
  st = consolidation_filelock_lock(
      new_fragment->array()->array_schema()->array_name(), fd, EXCLUSIVE_LOCK);
  if (!st.ok()) {
    delete new_fragment;
    return st;
  }

  // Finalize new fragment - makes the new fragment visible to new reads
  st = new_fragment->finalize();
  delete new_fragment;
  if (!st.ok()) {
    return st;
  }

  // Make old fragments invisible to new reads
  int fragment_num = old_fragment_names.size();
  for (int i = 0; i < fragment_num; ++i) {
    // Delete special fragment file inside the fragment directory
    std::string old_fragment_filename =
        old_fragment_names[i] + "/" + Configurator::fragment_filename();
    if (remove(old_fragment_filename.c_str())) {
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot remove fragment file during "
                      "finalizing consolidation; ") +
          strerror(errno)));
    }
  }

  // Unlock consolidation filelock
  RETURN_NOT_OK(consolidation_filelock_unlock(fd));

  // Delete old fragments
  for (int i = 0; i < fragment_num; ++i) {
    RETURN_NOT_OK(utils::delete_dir(old_fragment_names[i]));
  }
  return Status::Ok();
}

Status StorageManager::create_group_file(const std::string& group) const {
  // Create file
  std::string filename = group + "/" + Configurator::group_filename();
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1 || ::close(fd)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Failed to create group file; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status StorageManager::group_clear(const std::string& group) const {
  // Get real group path
  std::string group_real = utils::real_dir(group);

  // Check if group exists
  if (!utils::is_group(group_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Group '") + group_real + "' does not exist"));
  }

  // Delete all groups, arrays and metadata inside the group directory
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(group_real.c_str());

  if (dir == nullptr) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot open group directory '") + group_real + "'; " +
        strerror(errno)));
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, "..") ||
        !strcmp(next_file->d_name, Configurator::group_filename()))
      continue;
    filename = group_real + "/" + next_file->d_name;
    if (utils::is_group(filename)) {  // Group
      RETURN_NOT_OK(group_delete(filename))
    } else if (utils::is_metadata(filename)) {  // Metadata
      RETURN_NOT_OK(metadata_delete(filename));
    } else if (utils::is_array(filename)) {  // Array
      RETURN_NOT_OK(array_delete(filename))
    } else {  // Non TileDB related
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot delete non TileDB related element '") + filename +
          "'"));
    }
  }

  // Close group directory
  if (closedir(dir)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot close the group directory; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status StorageManager::group_delete(const std::string& group) const {
  // Clear the group
  RETURN_NOT_OK(group_clear(group));

  // Delete group directory
  RETURN_NOT_OK(utils::delete_dir(group));

  return Status::Ok();
}

Status StorageManager::group_move(
    const std::string& old_group, const std::string& new_group) const {
  // Get real group directory names
  std::string old_group_real = utils::real_dir(old_group);
  std::string new_group_real = utils::real_dir(new_group);

  // Check if the old group exists
  if (!utils::is_group(old_group_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Group '") + old_group_real + "' does not exist"));
  }

  // Make sure that the new group is not an existing directory
  if (utils::is_dir(new_group_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Directory '") + new_group_real + "' already exists"));
  }

  // Rename
  if (rename(old_group_real.c_str(), new_group_real.c_str())) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot move group; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status StorageManager::metadata_clear(const std::string& metadata) const {
  // Get real metadata directory name
  std::string metadata_real = utils::real_dir(metadata);

  // Check if the metadata exists
  if (!utils::is_metadata(metadata_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Metadata '") + metadata_real + "' do not exist"));
  }

  // Delete the entire metadata directory except for the array schema file
  std::string filename;
  struct dirent* next_file;
  DIR* dir = opendir(metadata_real.c_str());

  if (dir == nullptr) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot open metadata directory; ") + strerror(errno)));
  }

  while ((next_file = readdir(dir))) {
    if (!strcmp(next_file->d_name, ".") || !strcmp(next_file->d_name, "..") ||
        !strcmp(next_file->d_name, Configurator::metadata_schema_filename()) ||
        !strcmp(next_file->d_name, Configurator::consolidation_filelock_name()))
      continue;
    filename = metadata_real + "/" + next_file->d_name;
    if (utils::is_fragment(filename)) {  // Fragment
      RETURN_NOT_OK(utils::delete_dir(filename));
    } else {  // Non TileDB related
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot delete non TileDB related element '") + filename +
          "'"));
    }
  }

  // Close metadata directory
  if (closedir(dir)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot close the metadata directory; ") +
        strerror(errno)));
  }
  return Status::Ok();
}

Status StorageManager::metadata_delete(const std::string& metadata) const {
  // Get real metadata directory name
  std::string metadata_real = utils::real_dir(metadata);

  // Clear the metadata
  RETURN_NOT_OK(metadata_clear(metadata_real));

  // Delete metadata directory
  RETURN_NOT_OK(utils::delete_dir(metadata_real));
  return Status::Ok();
}

Status StorageManager::metadata_move(
    const std::string& old_metadata, const std::string& new_metadata) const {
  // Get real metadata directory name
  std::string old_metadata_real = utils::real_dir(old_metadata);
  std::string new_metadata_real = utils::real_dir(new_metadata);

  // Check if the old metadata exists
  if (!utils::is_metadata(old_metadata_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Metadata '") + old_metadata_real + "' do not exist"));
  }

  // Make sure that the new metadata is not an existing directory
  if (utils::is_dir(new_metadata_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Directory '") + new_metadata_real + "' already exists"));
  }

  // Rename metadata
  if (rename(old_metadata_real.c_str(), new_metadata_real.c_str())) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot move metadata; ") + strerror(errno)));
  }

  // Incorporate new name in the array schema
  ArraySchema* array_schema = new ArraySchema();
  RETURN_NOT_OK_ELSE(
      array_schema->load(new_metadata_real), delete array_schema);
  array_schema->set_array_name(new_metadata_real.c_str());

  // Store the new schema
  RETURN_NOT_OK_ELSE(
      array_schema->store(new_metadata_real), delete array_schema);

  // Clean up
  delete array_schema;

  // Success
  return Status::Ok();
}

Status StorageManager::open_array_mtx_destroy() {
#ifdef HAVE_OPENMP
  Status st_omp_mtx = utils::mutex_destroy(&open_array_omp_mtx_);
#else
  Status st_omp_mtx = Status::Ok();
#endif
  Status st_pthread_mtx = utils::mutex_destroy(&open_array_pthread_mtx_);

  // Errors
  if (!st_omp_mtx.ok()) {
    return st_omp_mtx;
  }
  if (!st_pthread_mtx.ok()) {
    return st_pthread_mtx;
  }
  return Status::Ok();
}

Status StorageManager::open_array_mtx_init() {
#ifdef HAVE_OPENMP
  Status st_omp_mtx = utils::mutex_init(&open_array_omp_mtx_);
#else
  Status st_omp_mtx = Status::Ok();
#endif
  Status st_pthread_mtx = utils::mutex_init(&open_array_pthread_mtx_);

  // Errors
  if (!st_omp_mtx.ok()) {
    return st_omp_mtx;
  }
  if (!st_pthread_mtx.ok()) {
    return st_pthread_mtx;
  }
  return Status::Ok();
}

Status StorageManager::open_array_mtx_lock() {
#ifdef HAVE_OPENMP
  Status st_omp_mtx = utils::mutex_lock(&open_array_omp_mtx_);
#else
  Status st_omp_mtx = Status::Ok();
#endif
  Status st_pthread_mtx = utils::mutex_lock(&open_array_pthread_mtx_);

  // Errors
  if (!st_omp_mtx.ok()) {
    return st_omp_mtx;
  }
  if (!st_pthread_mtx.ok()) {
    return st_pthread_mtx;
  }
  return Status::Ok();
}

Status StorageManager::open_array_mtx_unlock() {
#ifdef HAVE_OPENMP
  Status st_omp_mtx = utils::mutex_unlock(&open_array_omp_mtx_);
#else
  Status st_omp_mtx = Status::Ok();
#endif
  Status st_pthread_mtx = utils::mutex_unlock(&open_array_pthread_mtx_);

  // Errors
  if (!st_omp_mtx.ok()) {
    return st_omp_mtx;
  }
  if (!st_pthread_mtx.ok()) {
    return st_pthread_mtx;
  }
  return Status::Ok();
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

Status StorageManager::OpenArray::mutex_destroy() {
#ifdef HAVE_OPENMP
  Status st_omp_mtx = utils::mutex_destroy(&omp_mtx_);
#else
  Status st_omp_mtx = Status::Ok();
#endif
  Status st_pthread_mtx = utils::mutex_destroy(&pthread_mtx_);

  // Error
  if (!st_omp_mtx.ok()) {
    return st_omp_mtx;
  }
  if (!st_pthread_mtx.ok()) {
    return st_pthread_mtx;
  }
  return Status::Ok();
}

Status StorageManager::OpenArray::mutex_init() {
#ifdef HAVE_OPENMP
  Status st_omp_mtx = utils::mutex_init(&omp_mtx_);
#else
  Status st_omp_mtx = Status::Ok();
#endif
  Status st_pthread_mtx = utils::mutex_init(&pthread_mtx_);

  // Error
  if (!st_omp_mtx.ok()) {
    return st_omp_mtx;
  }
  if (!st_pthread_mtx.ok()) {
    return st_pthread_mtx;
  }
  return Status::Ok();
}

Status StorageManager::OpenArray::mutex_lock() {
#ifdef HAVE_OPENMP
  Status st_omp_mtx = utils::mutex_lock(&omp_mtx_);
#else
  Status st_omp_mtx = Status::Ok();
#endif
  Status st_pthread_mtx = utils::mutex_lock(&pthread_mtx_);

  // Error
  if (!st_omp_mtx.ok()) {
    return st_omp_mtx;
  }
  if (!st_pthread_mtx.ok()) {
    return st_pthread_mtx;
  }
  return Status::Ok();
}

Status StorageManager::OpenArray::mutex_unlock() {
#ifdef HAVE_OPENMP
  Status st_omp_mtx = utils::mutex_unlock(&omp_mtx_);
#else
  Status st_omp_mtx = Status::Ok();
#endif
  Status st_pthread_mtx = utils::mutex_unlock(&pthread_mtx_);

  // Error
  if (!st_omp_mtx.ok()) {
    return st_omp_mtx;
  }
  if (!st_pthread_mtx.ok()) {
    return st_pthread_mtx;
  }
  return Status::Ok();
}

}  // namespace tiledb