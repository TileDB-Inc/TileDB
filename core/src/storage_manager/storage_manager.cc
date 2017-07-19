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

#include <cassert>

#include "array.h"
#include "basic_array_schema.h"
#include "filesystem.h"
#include "logger.h"
#include "storage_manager.h"
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
  aio_done_ = false;
  config_ = nullptr;
}

StorageManager::~StorageManager() {
  if (config_ != nullptr) {
    delete config_;
    config_ = nullptr;
  }
  open_array_mtx_destroy();

  aio_stop();
  delete aio_thread_[0];
  delete aio_thread_[1];
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

Status StorageManager::init(Configurator* config) {
  // Clear previous configurator
  if (config_ != nullptr)
    delete config_;

  // Create new configurator and clone the input
  config_ = new Configurator(config);

  // Create a thread to handle the user asynchronous I/O
  aio_thread_[0] = new std::thread(aio_start, this, 0);

  // Create a thread to handle the internal asynchronous I/O
  aio_thread_[1] = new std::thread(aio_start, this, 1);

  // Initialize mutexes and return
  return open_array_mtx_init();
}

// TODO: Object types should be Enums
int StorageManager::dir_type(const char* dir) {
  // Get real path
  std::string dir_real = filesystem::real_dir(dir);

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

void StorageManager::set_config(Configurator* config) {
  // TODO: make thread-safe?

  config_ = config;
}

/* ****************************** */
/*             GROUP              */
/* ****************************** */

Status StorageManager::group_create(const std::string& group) const {
  // Create group directory
  RETURN_NOT_OK(filesystem::create_dir(group));
  // Create group file
  RETURN_NOT_OK(filesystem::create_group_file(group));
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

Status StorageManager::aio_submit(AIORequest* aio_request, int i) {
  aio_push_request(aio_request, i);

  return Status::Ok();
}

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

Status StorageManager::array_create(ArraySchema* array_schema) const {
  // Check array schema
  if (array_schema == nullptr) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot create array; Empty array schema"));
  }

  // Create array directory
  std::string dir = array_schema->array_name();
  RETURN_NOT_OK(filesystem::create_dir(dir));

  // Store array schema
  RETURN_NOT_OK(array_schema->store(dir));

  // Create consolidation filelock
  RETURN_NOT_OK(consolidation_lock_create(dir));

  // Success
  return Status::Ok();
}

// TODO (jcb): is it true that this cannot fail?
void StorageManager::array_get_fragment_names(
    const std::string& array, std::vector<std::string>& fragment_names) {
  // Get directory names in the array folder
  fragment_names = filesystem::get_fragment_dirs(filesystem::real_dir(array));
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
    int dense = !filesystem::is_file(
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
    RETURN_NOT_OK(
        array_open(filesystem::real_dir(array_dir), open_array, mode));
  }

  Status st;
  // Create the clone Array object
  Array* array_clone = new Array();
  st = array_clone->init(
      this,
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
      this,
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
  std::string array_name =
      metadata->metadata_schema()->array_schema()->array_name();
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

Status StorageManager::metadata_create(MetadataSchema* metadata_schema) const {
  // Check array schema
  if (metadata_schema == nullptr) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot create metadata; Empty metadata schema"));
  }

  // Create metadata directory
  std::string dir = metadata_schema->metadata_name();
  RETURN_NOT_OK(filesystem::create_dir(dir));

  // Store array schema
  RETURN_NOT_OK(metadata_schema->store(dir));

  // Create consolidation filelock
  RETURN_NOT_OK(consolidation_lock_create(dir));

  // Success
  return Status::Ok();
}

Status StorageManager::metadata_load_schema(
    const char* metadata_dir, ArraySchema*& array_schema) const {
  // Get real array path
  std::string metadata_abspath = filesystem::real_dir(metadata_dir);

  // Check if metadata exists
  if (!utils::is_metadata(metadata_abspath)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot load metadata schema; Metadata '") +
        metadata_abspath + "' does not exist"));
  }
  auto filename =
      metadata_abspath + "/" + Configurator::metadata_schema_filename();

  char* buffer = nullptr;
  size_t buffer_size = 0;
  RETURN_NOT_OK(filesystem::read_from_file(filename, buffer, &buffer_size));

  // Initialize array schema
  array_schema = new ArraySchema();
  Status st = array_schema->deserialize(buffer, buffer_size);

  // cleanup
  delete buffer;
  if (!st.ok()) {
    delete array_schema;
  }

  return st;
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
    RETURN_NOT_OK(array_open(
        filesystem::real_dir(metadata_dir), open_array, ArrayMode::READ))

  // Create metadata object
  metadata = new Metadata();
  Status st = metadata->init(
      this,
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
  std::string array_name =
      metadata->metadata_schema()->array_schema()->array_name();
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
    const char* parent_path,
    char** object_paths,
    tiledb_object_t* object_types,
    int* object_num) const {
  // Initialize object counter
  int obj_idx = 0;

  std::vector<std::string> paths;
  RETURN_NOT_OK(filesystem::ls(parent_path, &paths));

  for (auto& path : paths) {
    if (utils::is_group(path)) {  // Group
      if (obj_idx == *object_num) {
        return LOG_STATUS(Status::StorageManagerError(
            "Cannot list TileDB path; object buffer overflow"));
      }
      strcpy(object_paths[obj_idx], path.c_str());
      object_types[obj_idx] = TILEDB_GROUP;
      ++obj_idx;
    } else if (utils::is_metadata(path)) {  // Metadata
      if (obj_idx == *object_num) {
        return LOG_STATUS(Status::StorageManagerError(
            "Cannot list TileDB path; object buffer overflow"));
      }
      strcpy(object_paths[obj_idx], path.c_str());
      object_types[obj_idx] = TILEDB_METADATA;
      ++obj_idx;
    } else if (utils::is_array(path)) {  // Array
      if (obj_idx == *object_num) {
        return LOG_STATUS(Status::StorageManagerError(
            "Cannot list TileDB path; object buffer overflow"));
      }
      strcpy(object_paths[obj_idx], path.c_str());
      object_types[obj_idx] = TILEDB_ARRAY;
      ++obj_idx;
    }
  }
  // Set the number of objects
  *object_num = obj_idx;

  return Status::Ok();
}

Status StorageManager::ls_c(const char* parent_path, int* object_num) const {
  // Initialize directory number
  object_num = 0;

  std::vector<std::string> paths;
  RETURN_NOT_OK(filesystem::ls(parent_path, &paths));

  *object_num =
      std::count_if(paths.begin(), paths.end(), [](std::string& path) {
        return utils::is_group(path) || utils::is_metadata(path) ||
               utils::is_array(path);
      });
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

void StorageManager::aio_handle_request(AIORequest* aio_request) {
  // For easy reference
  Array* array = aio_request->array();
  Status st = array->aio_handle_request(aio_request);
  if (!st.ok())
    LOG_STATUS(st);
}

void StorageManager::aio_handle_requests(int i) {
  while (!aio_done_) {
    std::unique_lock<std::mutex> lock(aio_mutex_[i]);
    aio_cv_[i].wait(
        lock, [this, i] { return (aio_queue_[i].size() > 0) || aio_done_; });
    if (aio_done_)
      break;
    AIORequest* aio_request = aio_queue_[i].front();
    aio_queue_[i].pop();
    lock.unlock();
    aio_handle_request(aio_request);
  }
}

Status StorageManager::aio_push_request(AIORequest* aio_request, int i) {
  // Set the request status
  aio_request->set_status(AIOStatus::INPROGRESS);

  // Push request
  {
    std::lock_guard<std::mutex> lock(aio_mutex_[i]);
    aio_queue_[i].emplace(aio_request);
  }

  // Signal AIO thread
  aio_cv_[i].notify_one();

  return Status::Ok();
}

void StorageManager::aio_start(StorageManager* storage_manager, int i) {
  storage_manager->aio_handle_requests(i);
}

void StorageManager::aio_stop() {
  aio_done_ = true;
  aio_cv_[0].notify_one();
  aio_cv_[1].notify_one();
  aio_thread_[0]->join();
  aio_thread_[1]->join();
}

Status StorageManager::array_clear(const std::string& array) const {
  // Get real array directory name
  std::string array_abspath = filesystem::real_dir(array);

  // Check if the array exists
  if (!utils::is_array(array_abspath)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Array '") + array_abspath + "' does not exist"));
  }

  // Delete the entire array directory except for the array schema file
  std::vector<std::string> paths;
  RETURN_NOT_OK(filesystem::ls(array_abspath, &paths));
  for (auto& path : paths) {
    if (utils::is_array_schema(path) || utils::is_consolidation_lock(path))
      continue;
    if (utils::is_metadata(path)) {  // Metadata
      RETURN_NOT_OK(metadata_delete(path));
    } else if (utils::is_fragment(path)) {  // Fragment
      RETURN_NOT_OK(filesystem::delete_dir(path));
    } else {  // Non TileDB related
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot delete non TileDB related path '") + path + "'"));
    }
  }
  return Status::Ok();
}

Status StorageManager::array_close(const std::string& array) {
  // Lock mutexes
  RETURN_NOT_OK(open_array_mtx_lock());

  // Find the open array entry
  std::map<std::string, OpenArray*>::iterator it =
      open_arrays_.find(filesystem::real_dir(array));

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
    st_filelock = consolidation_unlock(it->second->consolidation_filelock_);

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
  RETURN_NOT_OK(filesystem::delete_dir(array));
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
  std::string old_array_real = filesystem::real_dir(old_array);
  std::string new_array_real = filesystem::real_dir(new_array);

  // Check if the old array exists
  if (!utils::is_array(old_array_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Array '") + old_array_real + "' does not exist"));
  }

  // Make sure that the new array is not an existing directory
  if (filesystem::is_dir(new_array_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Array '") + new_array_real + "' already exists"));
  }

  // Rename array
  RETURN_NOT_OK(filesystem::move(old_array_real, new_array_real));

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
        consolidation_lock(
            array_name, &(open_array->consolidation_filelock_), true),
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

Status StorageManager::consolidation_lock_create(const std::string& dir) const {
  // Create file
  std::string filename =
      dir + "/" + Configurator::consolidation_filelock_name();
  return filesystem::filelock_create(filename);
}

Status StorageManager::consolidation_lock(
    const std::string& array_name, int* fd, bool shared) const {
  // Prepare the filelock name
  std::string array_name_real = filesystem::real_dir(array_name);
  std::string filename =
      array_name_real + "/" + Configurator::consolidation_filelock_name();
  return filesystem::filelock_lock(filename, fd, shared);
}

Status StorageManager::consolidation_unlock(int fd) const {
  return filesystem::filelock_unlock(fd);
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
  st = consolidation_lock(
      new_fragment->array()->array_schema()->array_name(), &fd, false);
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
  RETURN_NOT_OK(consolidation_unlock(fd));

  // Delete old fragments
  for (int i = 0; i < fragment_num; ++i) {
    RETURN_NOT_OK(filesystem::delete_dir(old_fragment_names[i]));
  }
  return Status::Ok();
}

Status StorageManager::group_clear(const std::string& group) const {
  // Get real group path
  std::string group_abspath = filesystem::real_dir(group);

  // Check if group exists
  if (!utils::is_group(group_abspath)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Group '") + group_abspath + "' does not exist"));
  }
  // Delete all groups, arrays and metadata inside the group directory
  std::vector<std::string> paths;
  RETURN_NOT_OK(filesystem::ls(group_abspath, &paths));
  for (auto& path : paths) {
    if (utils::is_group(path)) {  // Group
      RETURN_NOT_OK(group_delete(path))
    } else if (utils::is_metadata(path)) {  // Metadata
      RETURN_NOT_OK(metadata_delete(path));
    } else if (utils::is_array(path)) {  // Array
      RETURN_NOT_OK(array_delete(path))
    } else {  // Non TileDB related
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot delete non TileDB related element '") + path +
          "'"));
    }
  }
  return Status::Ok();
}

Status StorageManager::group_delete(const std::string& group) const {
  // Clear the group
  RETURN_NOT_OK(group_clear(group));

  // Delete group directory
  RETURN_NOT_OK(filesystem::delete_dir(group));

  return Status::Ok();
}

Status StorageManager::group_move(
    const std::string& old_group, const std::string& new_group) const {
  // Get real group directory names
  std::string old_group_real = filesystem::real_dir(old_group);
  std::string new_group_real = filesystem::real_dir(new_group);

  // Check if the old group exists
  if (!utils::is_group(old_group_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Group '") + old_group_real + "' does not exist"));
  }

  // Make sure that the new group is not an existing directory
  if (filesystem::is_dir(new_group_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Group '") + new_group_real + "' already exists"));
  }

  return filesystem::move(old_group_real, new_group_real);
}

Status StorageManager::metadata_clear(const std::string& metadata) const {
  // Get real metadata directory name
  std::string metadata_abspath = filesystem::real_dir(metadata);

  // Check if the metadata exists
  if (!utils::is_metadata(metadata_abspath)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Metadata '") + metadata_abspath + "' do not exist"));
  }

  // Delete the entire metadata directory except for the array schema file
  std::vector<std::string> paths;
  RETURN_NOT_OK(filesystem::ls(metadata_abspath, &paths));
  for (auto& path : paths) {
    if (utils::is_metadata_schema(path) || utils::is_consolidation_lock(path))
      continue;
    if (utils::is_fragment(path)) {
      RETURN_NOT_OK(filesystem::delete_dir(path));
    } else {
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot delete non TileDB related element '") + path +
          "'"));
    }
  }
  return Status::Ok();
}

Status StorageManager::metadata_delete(const std::string& metadata) const {
  // Get real metadata directory name
  std::string metadata_real = filesystem::real_dir(metadata);

  // Clear the metadata
  RETURN_NOT_OK(metadata_clear(metadata_real));

  // Delete metadata directory
  RETURN_NOT_OK(filesystem::delete_dir(metadata_real));
  return Status::Ok();
}

Status StorageManager::metadata_move(
    const std::string& old_metadata, const std::string& new_metadata) const {
  // Get real metadata directory name
  std::string old_metadata_real = filesystem::real_dir(old_metadata);
  std::string new_metadata_real = filesystem::real_dir(new_metadata);

  // Check if the old metadata exists
  if (!utils::is_metadata(old_metadata_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Metadata '") + old_metadata_real + "' do not exist"));
  }

  // Make sure that the new metadata is not an existing directory
  if (filesystem::is_dir(new_metadata_real)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Metadata'") + new_metadata_real + "' already exists"));
  }

  // Rename metadata
  RETURN_NOT_OK(filesystem::move(old_metadata_real, new_metadata_real));

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
    std::string parent_fragment_name = utils::parent_path(fragment_name);
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