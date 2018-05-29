/**
 * @file   storage_manager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include <algorithm>
#include <iostream>
#include <sstream>

#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile_io.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MIN(a, b) ((a) < (b) ? (a) : (b))

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManager::StorageManager() {
  consolidator_ = nullptr;
  array_schema_cache_ = nullptr;
  fragment_metadata_cache_ = nullptr;
  tile_cache_ = nullptr;
  vfs_ = nullptr;
  cancellation_in_progress_ = false;
  queries_in_progress_ = 0;
}

StorageManager::~StorageManager() {
  global_state::GlobalState::GetGlobalState().unregister_storage_manager(this);
  cancel_all_tasks();

  delete array_schema_cache_;
  delete consolidator_;
  delete fragment_metadata_cache_;
  delete tile_cache_;
  delete vfs_;

  // Release all filelocks
  for (auto& open_array_it : open_arrays_) {
    open_array_it.second->file_unlock(vfs_);
    delete open_array_it.second;
  }

  for (auto& fl_it : xfilelocks_) {
    auto filelock = fl_it.second;
    auto lock_uri = URI(fl_it.first).join_path(constants::array_filelock);
    if (filelock != INVALID_FILELOCK)
      vfs_->filelock_unlock(lock_uri, filelock);
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManager::array_close(const URI& array_uri) {
  // Lock mutex
  open_array_mtx_.lock();

  // Find the open array entry
  auto it = open_arrays_.find(array_uri.to_string());

  // Do nothing if array is closed
  if (it == open_arrays_.end()) {
    open_array_mtx_.unlock();
    return Status::Ok();
  }

  // For easy reference
  OpenArray* open_array = it->second;

  // Lock the mutex of the array and decrement counter
  open_array->mtx_lock();
  open_array->cnt_decr();

  // Close the array if the counter reaches 0
  if (open_array->cnt() == 0) {
    // Release file lock
    auto st = open_array->file_unlock(vfs_);
    if (!st.ok()) {
      open_array->mtx_unlock();
      open_array_mtx_.unlock();
      return st;
    }

    // Remove open array entry
    open_array->mtx_unlock();
    delete open_array;
    open_arrays_.erase(it);
  } else {  // Just unlock the array mutex
    open_array->mtx_unlock();
  }

  // Unlock mutex and notify condition on exclusive locks
  open_array_mtx_.unlock();
  xlock_cv_.notify_all();

  return Status::Ok();
}

Status StorageManager::array_open(
    const URI& array_uri, OpenArray** open_array) {
  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(this->object_type(array_uri, &obj_type));
  if (obj_type != ObjectType::ARRAY && obj_type != ObjectType::KEY_VALUE) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot open array; Array does not exist"));
  }

  // Lock mutex
  open_array_mtx_.lock();

  // Find the open array entry
  auto it = open_arrays_.find(array_uri.to_string());
  if (it != open_arrays_.end()) {
    *open_array = it->second;
  } else {  // Create a new entry
    *open_array = new OpenArray(array_uri);
    open_arrays_[array_uri.to_string()] = *open_array;
  }

  // Lock the array and increment counter
  (*open_array)->mtx_lock();
  (*open_array)->cnt_incr();

  // Unlock mutex
  open_array_mtx_.unlock();

  // Get a shared filelock for process-safety
  RETURN_NOT_OK((*open_array)->file_lock(vfs_, OpenArray::SLOCK));

  // Load array schema if not fetched already
  if ((*open_array)->array_schema() == nullptr) {
    auto st = load_array_schema(array_uri, obj_type, *open_array);
    if (!st.ok()) {
      (*open_array)->mtx_unlock();
      array_close((*open_array)->array_uri());
      return st;
    }
  }

  // Filelock the metadata
  auto filelock_uri = array_uri.join_path(constants::metadata_filelock);
  filelock_t fl;
  vfs_->filelock_lock(filelock_uri, &fl, VFS::SLOCK);

  // Get fragment metadata in the case of reads, if not fetched already
  auto st = load_fragment_metadata(*open_array);
  if (!st.ok()) {
    vfs_->filelock_unlock(filelock_uri, fl);
    (*open_array)->mtx_unlock();
    array_close((*open_array)->array_uri());
    return st;
  }

  // Unlock the array mutex and metadata filelock
  vfs_->filelock_unlock(filelock_uri, fl);
  (*open_array)->mtx_unlock();

  // Note that we retain the (shared) array filelock

  return Status::Ok();
}

Status StorageManager::array_compute_max_read_buffer_sizes(
    OpenArray* open_array,
    const void* subarray,
    const char** attributes,
    unsigned attribute_num,
    uint64_t* buffer_sizes) {
  // Open the array
  open_array->mtx_lock();
  auto array_schema = open_array->array_schema();
  auto metadata = open_array->fragment_metadata();
  open_array->mtx_unlock();

  // Check attributes
  RETURN_NOT_OK(array_schema->check_attributes(attributes, attribute_num));

  // Compute buffer sizes
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      buffer_sizes_tmp;
  for (unsigned i = 0; i < attribute_num; ++i)
    buffer_sizes_tmp[attributes[i]] = std::pair<uint64_t, uint64_t>(0, 0);
  RETURN_NOT_OK(array_compute_max_read_buffer_sizes(
      array_schema, metadata, subarray, &buffer_sizes_tmp));

  // Copy to input buffer sizes
  unsigned bid = 0;
  for (unsigned i = 0; i < attribute_num; ++i) {
    auto it = buffer_sizes_tmp.find(attributes[i]);
    if (!array_schema->var_size(attributes[i])) {
      buffer_sizes[bid] = it->second.first;
      ++bid;
    } else {
      buffer_sizes[bid] = it->second.first;
      buffer_sizes[bid + 1] = it->second.second;
      bid += 2;
    }
  }

  // Close array
  return Status::Ok();
}

Status StorageManager::array_compute_max_read_buffer_sizes(
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& fragment_metadata,
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Return if there are no metadata
  if (fragment_metadata.empty())
    return Status::Ok();

  // Compute buffer sizes
  switch (array_schema->coords_type()) {
    case Datatype::INT32:
      return array_compute_max_read_buffer_sizes<int>(
          array_schema,
          fragment_metadata,
          static_cast<const int*>(subarray),
          buffer_sizes);
    case Datatype::INT64:
      return array_compute_max_read_buffer_sizes<int64_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int64_t*>(subarray),
          buffer_sizes);
    case Datatype::FLOAT32:
      return array_compute_max_read_buffer_sizes<float>(
          array_schema,
          fragment_metadata,
          static_cast<const float*>(subarray),
          buffer_sizes);
    case Datatype::FLOAT64:
      return array_compute_max_read_buffer_sizes<double>(
          array_schema,
          fragment_metadata,
          static_cast<const double*>(subarray),
          buffer_sizes);
    case Datatype::INT8:
      return array_compute_max_read_buffer_sizes<int8_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int8_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT8:
      return array_compute_max_read_buffer_sizes<uint8_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint8_t*>(subarray),
          buffer_sizes);
    case Datatype::INT16:
      return array_compute_max_read_buffer_sizes<int16_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int16_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT16:
      return array_compute_max_read_buffer_sizes<uint16_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint16_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT32:
      return array_compute_max_read_buffer_sizes<uint32_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint32_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT64:
      return array_compute_max_read_buffer_sizes<uint64_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint64_t*>(subarray),
          buffer_sizes);
    default:
      return LOG_STATUS(Status::StorageManagerError(
          "Cannot compute max read buffer sizes; Invalid coordinates type"));
  }

  return Status::Ok();
}

Status StorageManager::array_compute_subarray_partitions(
    OpenArray* open_array,
    const void* subarray,
    Layout layout,
    const char** attributes,
    unsigned attribute_num,
    const uint64_t* buffer_sizes,
    void*** subarray_partitions,
    uint64_t* npartitions) {
  // Initialization
  *npartitions = 0;
  *subarray_partitions = nullptr;

  // Open the array
  open_array->mtx_lock();
  auto array_schema = open_array->array_schema();
  auto metadata = open_array->fragment_metadata();
  open_array->mtx_unlock();

  auto subarray_size = 2 * array_schema->coords_size();

  // Compute buffer sizes map
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      buffer_sizes_map;
  RETURN_NOT_OK(array_schema->check_attributes(attributes, attribute_num));
  for (unsigned i = 0, bid = 0; i < attribute_num; ++i) {
    if (array_schema->var_size(attributes[i])) {
      buffer_sizes_map[attributes[i]] = std::pair<uint64_t, uint64_t>(
          buffer_sizes[bid], buffer_sizes[bid + 1]);
      bid += 2;
    } else {
      buffer_sizes_map[attributes[i]] =
          std::pair<uint64_t, uint64_t>(buffer_sizes[bid], 0);
      ++bid;
    }
  }

  // Get partitions
  std::vector<void*> subarray_partitions_vec;
  RETURN_NOT_OK(Reader::compute_subarray_partitions(
      this,
      array_schema,
      metadata,
      subarray,
      layout,
      buffer_sizes_map,
      &subarray_partitions_vec));

  // Handle empty partitions
  if (subarray_partitions_vec.empty())
    return Status::Ok();

  // Copy subarray partitions
  *subarray_partitions =
      (void**)std::malloc(subarray_partitions_vec.size() * sizeof(void*));
  if (*subarray_partitions == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Failed to compute subarray partitions; memory allocation error"));
  for (uint64_t i = 0; i < (uint64_t)subarray_partitions_vec.size(); ++i) {
    (*subarray_partitions)[i] = std::malloc(subarray_size);
    if ((*subarray_partitions)[i] == nullptr) {
      for (uint64_t j = 0; j < i; ++j)
        std::free((*subarray_partitions)[j]);
      std::free(*subarray_partitions);
      *subarray_partitions = nullptr;
      return LOG_STATUS(Status::StorageManagerError(
          "Failed to compute subarray partitions; memory allocation error"));
    }
    std::memcpy(
        (*subarray_partitions)[i], subarray_partitions_vec[i], subarray_size);
  }
  *npartitions = (uint64_t)subarray_partitions_vec.size();

  return Status::Ok();
}

Status StorageManager::array_consolidate(const char* array_name) {
  // Check array URI
  URI array_uri(array_name);
  if (array_uri.is_invalid()) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot consolidate array; Invalid URI"));
  }
  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(object_type(array_uri, &obj_type));

  if (obj_type != ObjectType::ARRAY && obj_type != ObjectType::KEY_VALUE) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot consolidate array; Array does not exist"));
  }
  return consolidator_->consolidate(array_name);
}

Status StorageManager::array_create(
    const URI& array_uri, ArraySchema* array_schema) {
  // Check array schema
  if (array_schema == nullptr) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot create array; Empty array schema"));
  }

  // Check if array exists
  bool exists;
  RETURN_NOT_OK(is_array(array_uri, &exists));
  if (exists)
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot create array; Array '") + array_uri.c_str() +
        "' already exists"));

  object_create_mtx_.lock();

  array_schema->set_array_uri(array_uri);
  auto st = array_schema->check();
  if (!st.ok()) {
    object_create_mtx_.unlock();
    return st;
  }

  // Create array directory
  st = vfs_->create_dir(array_uri);
  if (!st.ok()) {
    object_create_mtx_.unlock();
    return st;
  }

  // Store array schema
  st = store_array_schema(array_schema);
  if (!st.ok()) {
    vfs_->remove_file(array_uri);
    object_create_mtx_.unlock();
    return st;
  }

  // Create array filelock
  URI array_filelock_uri = array_uri.join_path(constants::array_filelock);
  st = vfs_->touch(array_filelock_uri);
  if (!st.ok()) {
    vfs_->remove_file(array_uri);
    object_create_mtx_.unlock();
    return st;
  }

  // Create array filelock
  URI meta_filelock_uri = array_uri.join_path(constants::metadata_filelock);
  st = vfs_->touch(meta_filelock_uri);
  if (!st.ok()) {
    vfs_->remove_file(array_uri);
    object_create_mtx_.unlock();
    return st;
  }

  object_create_mtx_.unlock();

  // Success
  return Status::Ok();
}

Status StorageManager::array_get_non_empty_domain(
    OpenArray* open_array, void* domain, bool* is_empty) {
  if (open_array == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Open array object is null"));

  // Open the array
  *is_empty = true;
  open_array->mtx_lock();
  auto array_schema = open_array->array_schema();
  auto metadata = open_array->fragment_metadata();
  open_array->mtx_unlock();

  // Return if there are no metadata
  if (metadata.empty())
    return Status::Ok();

  // Compute domain
  auto dim_num = array_schema->dim_num();
  switch (array_schema->coords_type()) {
    case Datatype::INT32:
      array_get_non_empty_domain<int>(
          metadata, dim_num, static_cast<int*>(domain));
      break;
    case Datatype::INT64:
      array_get_non_empty_domain<int64_t>(
          metadata, dim_num, static_cast<int64_t*>(domain));
      break;
    case Datatype::FLOAT32:
      array_get_non_empty_domain<float>(
          metadata, dim_num, static_cast<float*>(domain));
      break;
    case Datatype::FLOAT64:
      array_get_non_empty_domain<double>(
          metadata, dim_num, static_cast<double*>(domain));
      break;
    case Datatype::INT8:
      array_get_non_empty_domain<int8_t>(
          metadata, dim_num, static_cast<int8_t*>(domain));
      break;
    case Datatype::UINT8:
      array_get_non_empty_domain<uint8_t>(
          metadata, dim_num, static_cast<uint8_t*>(domain));
      break;
    case Datatype::INT16:
      array_get_non_empty_domain<int16_t>(
          metadata, dim_num, static_cast<int16_t*>(domain));
      break;
    case Datatype::UINT16:
      array_get_non_empty_domain<uint16_t>(
          metadata, dim_num, static_cast<uint16_t*>(domain));
      break;
    case Datatype::UINT32:
      array_get_non_empty_domain<uint32_t>(
          metadata, dim_num, static_cast<uint32_t*>(domain));
      break;
    case Datatype::UINT64:
      array_get_non_empty_domain<uint64_t>(
          metadata, dim_num, static_cast<uint64_t*>(domain));
      break;
    default:
      return LOG_STATUS(Status::StorageManagerError(
          "Cannot get non-empty domain; Invalid coordinates type"));
  }

  *is_empty = false;

  // Close array
  return Status::Ok();
}

Status StorageManager::array_xlock(const URI& array_uri) {
  // Wait until the array is closed
  std::unique_lock<std::mutex> lk(open_array_mtx_);
  xlock_cv_.wait(lk, [this, array_uri] {
    return open_arrays_.find(array_uri.to_string()) == open_arrays_.end();
  });

  // Retrieve filelock
  filelock_t filelock = INVALID_FILELOCK;
  auto lock_uri = array_uri.join_path(constants::array_filelock);
  RETURN_NOT_OK(vfs_->filelock_lock(lock_uri, &filelock, VFS::XLOCK));
  xfilelocks_[array_uri.to_string()] = filelock;

  return Status::Ok();
}

Status StorageManager::array_xunlock(const URI& array_uri) {
  open_array_mtx_.lock();

  // Get filelock if it exists
  auto it = xfilelocks_.find(array_uri.to_string());
  if (it == xfilelocks_.end())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot unlock array exclusive lock; Filelock not found"));
  auto filelock = it->second;

  auto lock_uri = array_uri.join_path(constants::array_filelock);
  if (filelock != INVALID_FILELOCK)
    RETURN_NOT_OK(vfs_->filelock_unlock(lock_uri, filelock));
  xfilelocks_.erase(it);

  open_array_mtx_.unlock();
  xlock_cv_.notify_all();

  return Status::Ok();
}

Status StorageManager::async_push_query(Query* query) {
  async_thread_pool_->enqueue(
      [this, query]() {
        // Process query.
        Status st = query_submit(query);
        if (!st.ok())
          LOG_STATUS(st);
        return st;
      },
      [query]() {
        // Task was cancelled. This is safe to perform in a separate thread,
        // as we are guaranteed by the thread pool not to have entered
        // query->process() yet.
        query->cancel();
      });

  return Status::Ok();
}

Status StorageManager::cancel_all_tasks() {
  // Check if there is already a "cancellation" in progress.
  bool handle_cancel = false;
  {
    std::unique_lock<std::mutex> lck(cancellation_in_progress_mtx_);
    if (!cancellation_in_progress_) {
      cancellation_in_progress_ = true;
      handle_cancel = true;
    }
  }

  // Handle the cancellation.
  if (handle_cancel) {
    // Cancel any queued tasks.
    async_thread_pool_->cancel_all_tasks();
    vfs_->cancel_all_tasks();

    // Wait for in-progress queries to finish.
    wait_for_zero_in_progress();

    // Reset the cancellation flag.
    std::unique_lock<std::mutex> lck(cancellation_in_progress_mtx_);
    cancellation_in_progress_ = false;
  }

  return Status::Ok();
}

bool StorageManager::cancellation_in_progress() {
  std::unique_lock<std::mutex> lck(cancellation_in_progress_mtx_);
  return cancellation_in_progress_;
}

Config StorageManager::config() const {
  return config_;
}

Status StorageManager::create_dir(const URI& uri) {
  return vfs_->create_dir(uri);
}

Status StorageManager::touch(const URI& uri) {
  return vfs_->touch(uri);
}

void StorageManager::decrement_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_--;
  queries_in_progress_cv_.notify_all();
}

Status StorageManager::delete_fragment(const URI& uri) const {
  bool exists;
  RETURN_NOT_OK(is_fragment(uri, &exists));
  if (!exists) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot delete fragment; '" + uri.to_string() +
        "' is not a TileDB fragment"));
  }
  return vfs_->remove_dir(uri);
}

Status StorageManager::object_remove(const char* path) const {
  auto uri = URI(path);
  if (uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot remove object '") + path + "'; Invalid URI"));

  ObjectType obj_type;
  RETURN_NOT_OK(object_type(uri, &obj_type));
  if (obj_type == ObjectType::INVALID)
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot remove object '") + path +
        "'; Invalid TileDB object"));

  return vfs_->remove_dir(uri);
}

Status StorageManager::object_move(
    const char* old_path, const char* new_path) const {
  auto old_uri = URI(old_path);
  if (old_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot move object '") + old_path + "'; Invalid URI"));

  auto new_uri = URI(new_path);
  if (new_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot move object to '") + new_path + "'; Invalid URI"));

  ObjectType obj_type;
  RETURN_NOT_OK(object_type(old_uri, &obj_type));
  if (obj_type == ObjectType::INVALID)
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot move object '") + old_path +
        "'; Invalid TileDB object"));

  return vfs_->move_dir(old_uri, new_uri);
}

Status StorageManager::group_create(const std::string& group) {
  // Create group URI
  URI uri(group);
  if (uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot create group '" + group + "'; Invalid group URI"));

  // Check if group exists
  bool exists;
  RETURN_NOT_OK(is_group(uri, &exists));
  if (exists)
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot create group; Group '") + uri.c_str() +
        "' already exists"));

  object_create_mtx_.lock();

  // Create group directory
  RETURN_NOT_OK_ELSE(vfs_->create_dir(uri), object_create_mtx_.unlock());

  // Create group file
  URI group_filename = uri.join_path(constants::group_filename);
  auto st = vfs_->touch(group_filename);
  if (!st.ok()) {
    vfs_->remove_dir(uri);
    object_create_mtx_.unlock();
    return st;
  }

  object_create_mtx_.unlock();

  return Status::Ok();
}

Status StorageManager::init(Config* config) {
  if (config != nullptr)
    config_ = *config;
  consolidator_ = new Consolidator(this);
  Config::SMParams sm_params = config_.sm_params();
  array_schema_cache_ = new LRUCache(sm_params.array_schema_cache_size_);
  fragment_metadata_cache_ =
      new LRUCache(sm_params.fragment_metadata_cache_size_);
  async_thread_pool_ = std::unique_ptr<ThreadPool>(new ThreadPool());
  RETURN_NOT_OK(async_thread_pool_->init(sm_params.num_async_threads_));
  tile_cache_ = new LRUCache(sm_params.tile_cache_size_);
  vfs_ = new VFS();
  RETURN_NOT_OK(vfs_->init(config_.vfs_params()));
  RETURN_NOT_OK(init_tbb(sm_params));

  auto& global_state = global_state::GlobalState::GetGlobalState();
  RETURN_NOT_OK(global_state.initialize(config));
  global_state.register_storage_manager(this);

  return Status::Ok();
}

void StorageManager::increment_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_++;
  queries_in_progress_cv_.notify_all();
}

Status StorageManager::is_array(const URI& uri, bool* is_array) const {
  RETURN_NOT_OK(
      vfs_->is_file(uri.join_path(constants::array_schema_filename), is_array));
  return Status::Ok();
}

Status StorageManager::is_file(const URI& uri, bool* is_file) const {
  RETURN_NOT_OK(vfs_->is_file(uri, is_file));
  return Status::Ok();
}

Status StorageManager::is_fragment(const URI& uri, bool* is_fragment) const {
  RETURN_NOT_OK(vfs_->is_file(
      uri.join_path(constants::fragment_metadata_filename), is_fragment));
  return Status::Ok();
}

Status StorageManager::is_group(const URI& uri, bool* is_group) const {
  RETURN_NOT_OK(
      vfs_->is_file(uri.join_path(constants::group_filename), is_group));
  return Status::Ok();
}

Status StorageManager::is_kv(const URI& uri, bool* is_kv) const {
  RETURN_NOT_OK(
      vfs_->is_file(uri.join_path(constants::kv_schema_filename), is_kv));
  return Status::Ok();
}

Status StorageManager::load_array_schema(
    const URI& array_uri, ObjectType object_type, ArraySchema** array_schema) {
  if (array_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load array schema; Invalid array URI"));

  assert(
      object_type == ObjectType::ARRAY || object_type == ObjectType::KEY_VALUE);
  URI schema_uri = (object_type == ObjectType::ARRAY) ?
                       array_uri.join_path(constants::array_schema_filename) :
                       array_uri.join_path(constants::kv_schema_filename);

  // Try to read from cache
  bool in_cache;
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      array_schema_cache_->read(schema_uri.to_string(), buff, &in_cache),
      delete buff);

  // Read from file if not in cache
  if (!in_cache) {
    delete buff;
    auto tile_io = new TileIO(this, schema_uri);
    auto tile = (Tile*)nullptr;
    RETURN_NOT_OK_ELSE(tile_io->read_generic(&tile, 0), delete tile_io);
    tile->disown_buff();
    buff = tile->buffer();
    delete tile;
    delete tile_io;
  }

  // Deserialize
  bool is_kv = (object_type == ObjectType::KEY_VALUE);
  auto cbuff = new ConstBuffer(buff);
  *array_schema = new ArraySchema();
  (*array_schema)->set_array_uri(array_uri);
  Status st = (*array_schema)->deserialize(cbuff, is_kv);
  delete cbuff;
  if (!st.ok()) {
    delete array_schema;
    *array_schema = nullptr;
  }

  // Store in cache
  if (st.ok() && !in_cache && buff->size() <= array_schema_cache_->max_size()) {
    buff->disown_data();
    st = array_schema_cache_->insert(
        schema_uri.to_string(), buff->data(), buff->size());
  }

  delete buff;

  return st;
}

Status StorageManager::load_fragment_metadata(
    FragmentMetadata* fragment_metadata) {
  const URI& fragment_uri = fragment_metadata->fragment_uri();
  bool fragment_exists;
  RETURN_NOT_OK(is_fragment(fragment_uri, &fragment_exists));
  if (!fragment_exists)
    return Status::StorageManagerError(
        "Cannot load fragment metadata; Fragment does not exist");

  URI fragment_metadata_uri = fragment_uri.join_path(
      std::string(constants::fragment_metadata_filename));

  // Try to read from cache
  bool in_cache;
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      fragment_metadata_cache_->read(
          fragment_metadata_uri.to_string(), buff, &in_cache),
      delete buff);

  // Read from file if not in cache
  if (!in_cache) {
    delete buff;
    auto tile_io = new TileIO(this, fragment_metadata_uri);
    auto tile = (Tile*)nullptr;
    RETURN_NOT_OK_ELSE(tile_io->read_generic(&tile, 0), delete tile_io);
    tile->disown_buff();
    buff = tile->buffer();
    delete tile;
    delete tile_io;
  }

  // Deserialize
  auto cbuff = new ConstBuffer(buff);
  Status st = fragment_metadata->deserialize(cbuff);
  delete cbuff;

  // Store in cache
  if (st.ok() && !in_cache &&
      buff->size() <= fragment_metadata_cache_->max_size()) {
    buff->disown_data();
    st = fragment_metadata_cache_->insert(
        fragment_metadata_uri.to_string(), buff->data(), buff->size());
  }

  delete buff;

  return st;
}

Status StorageManager::object_type(const URI& uri, ObjectType* type) const {
  URI dir_uri = uri;
  if (uri.is_s3()) {
    // Always add a trailing '/' in the S3 case so that listing the URI as a
    // directory will work as expected. Listing a non-directory object is not an
    // error for S3.
    auto uri_str = uri.to_string();
    dir_uri = URI(utils::ends_with(uri_str, "/") ? uri_str : (uri_str + "/"));
  } else {
    // For non-S3, listing a non-directory is an error.
    bool is_dir = false;
    RETURN_NOT_OK(vfs_->is_dir(uri, &is_dir));
    if (!is_dir) {
      *type = ObjectType::INVALID;
      return Status::Ok();
    }
  }

  std::vector<URI> child_uris;
  RETURN_NOT_OK(vfs_->ls(dir_uri, &child_uris));

  for (const auto& child_uri : child_uris) {
    auto uri_str = child_uri.to_string();
    if (utils::ends_with(uri_str, constants::group_filename)) {
      *type = ObjectType::GROUP;
      return Status::Ok();
    } else if (utils::ends_with(uri_str, constants::kv_schema_filename)) {
      *type = ObjectType::KEY_VALUE;
      return Status::Ok();
    } else if (utils::ends_with(uri_str, constants::array_schema_filename)) {
      *type = ObjectType::ARRAY;
      return Status::Ok();
    }
  }

  *type = ObjectType::INVALID;
  return Status::Ok();
}

Status StorageManager::object_iter_begin(
    ObjectIter** obj_iter, const char* path, WalkOrder order) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot create object iterator; Invalid input path"));
  }

  // Get all contents of path
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(path_uri, &uris));

  // Create a new object iterator
  *obj_iter = new ObjectIter();
  (*obj_iter)->order_ = order;
  (*obj_iter)->recursive_ = true;

  // Include the uris that are TileDB objects in the iterator state
  ObjectType obj_type;
  for (auto& uri : uris) {
    RETURN_NOT_OK_ELSE(object_type(uri, &obj_type), delete *obj_iter);
    if (obj_type != ObjectType::INVALID) {
      (*obj_iter)->objs_.push_back(uri);
      if (order == WalkOrder::POSTORDER)
        (*obj_iter)->expanded_.push_back(false);
    }
  }

  return Status::Ok();
}

Status StorageManager::object_iter_begin(
    ObjectIter** obj_iter, const char* path) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot create object iterator; Invalid input path"));
  }

  // Get all contents of path
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(path_uri, &uris));

  // Create a new object iterator
  *obj_iter = new ObjectIter();
  (*obj_iter)->order_ = WalkOrder::PREORDER;
  (*obj_iter)->recursive_ = false;

  // Include the uris that are TileDB objects in the iterator state
  ObjectType obj_type;
  for (auto& uri : uris) {
    RETURN_NOT_OK(object_type(uri, &obj_type))
    if (obj_type != ObjectType::INVALID)
      (*obj_iter)->objs_.push_back(uri);
  }

  return Status::Ok();
}

void StorageManager::object_iter_free(ObjectIter* obj_iter) {
  delete obj_iter;
}

Status StorageManager::object_iter_next(
    ObjectIter* obj_iter, const char** path, ObjectType* type, bool* has_next) {
  // Handle case there is no next
  if (obj_iter->objs_.empty()) {
    *has_next = false;
    return Status::Ok();
  }

  // Retrieve next object
  switch (obj_iter->order_) {
    case WalkOrder::PREORDER:
      RETURN_NOT_OK(object_iter_next_preorder(obj_iter, path, type, has_next));
      break;
    case WalkOrder::POSTORDER:
      RETURN_NOT_OK(object_iter_next_postorder(obj_iter, path, type, has_next));
      break;
  }

  return Status::Ok();
}

Status StorageManager::object_iter_next_postorder(
    ObjectIter* obj_iter, const char** path, ObjectType* type, bool* has_next) {
  // Get all contents of the next URI recursively till the bottom,
  // if the front of the list has not been expanded
  if (obj_iter->expanded_.front() == false) {
    uint64_t obj_num;
    do {
      obj_num = obj_iter->objs_.size();
      std::vector<URI> uris;
      RETURN_NOT_OK(vfs_->ls(obj_iter->objs_.front(), &uris));
      obj_iter->expanded_.front() = true;

      // Push the new TileDB objects in the front of the iterator's list
      ObjectType obj_type;
      for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
        RETURN_NOT_OK(object_type(*it, &obj_type));
        if (obj_type != ObjectType::INVALID) {
          obj_iter->objs_.push_front(*it);
          obj_iter->expanded_.push_front(false);
        }
      }
    } while (obj_num != obj_iter->objs_.size());
  }

  // Prepare the values to be returned
  URI front_uri = obj_iter->objs_.front();
  obj_iter->next_ = front_uri.to_string();
  RETURN_NOT_OK(object_type(front_uri, type));
  *path = obj_iter->next_.c_str();
  *has_next = true;

  // Pop the front (next URI) of the iterator's object list
  obj_iter->objs_.pop_front();
  obj_iter->expanded_.pop_front();

  return Status::Ok();
}

Status StorageManager::object_iter_next_preorder(
    ObjectIter* obj_iter, const char** path, ObjectType* type, bool* has_next) {
  // Prepare the values to be returned
  URI front_uri = obj_iter->objs_.front();
  obj_iter->next_ = front_uri.to_string();
  RETURN_NOT_OK(object_type(front_uri, type));
  *path = obj_iter->next_.c_str();
  *has_next = true;

  // Pop the front (next URI) of the iterator's object list
  obj_iter->objs_.pop_front();

  // Return if no recursion is needed
  if (!obj_iter->recursive_)
    return Status::Ok();

  // Get all contents of the next URI
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(front_uri, &uris));

  // Push the new TileDB objects in the front of the iterator's list
  ObjectType obj_type;
  for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
    RETURN_NOT_OK(object_type(*it, &obj_type));
    if (obj_type != ObjectType::INVALID)
      obj_iter->objs_.push_front(*it);
  }

  return Status::Ok();
}

Status StorageManager::query_create(
    Query** query, OpenArray* open_array, QueryType type, URI fragment_uri) {
  // For easy reference
  open_array->mtx_lock();
  auto array_schema = open_array->array_schema();
  auto metadata = open_array->fragment_metadata();
  open_array->mtx_unlock();

  // Create query
  *query = new Query(this, type, array_schema, metadata);

  if (type == QueryType::WRITE)
    (*query)->set_fragment_uri(fragment_uri);

  return Status::Ok();
}

Status StorageManager::query_submit(Query* query) {
  // Initialize query
  RETURN_NOT_OK(query->init());

  // Process the query
  increment_in_progress();
  auto st = query->process();
  decrement_in_progress();
  return st;
}

Status StorageManager::query_submit_async(
    Query* query, std::function<void(void*)> callback, void* callback_data) {
  // Do nothing if the query is completed or failed
  auto status = query->status();
  if (status == QueryStatus::COMPLETED)
    return Status::Ok();

  // Initialize query
  RETURN_NOT_OK(query->init());

  // Push the query into the async queue
  query->set_callback(callback, callback_data);
  return async_push_query(query);
}

Status StorageManager::read_from_cache(
    const URI& uri,
    uint64_t offset,
    Buffer* buffer,
    uint64_t nbytes,
    bool* in_cache) const {
  std::stringstream key;
  key << uri.to_string() << "+" << offset;
  RETURN_NOT_OK(buffer->realloc(nbytes));
  RETURN_NOT_OK(
      tile_cache_->read(key.str(), buffer->data(), 0, nbytes, in_cache));
  buffer->set_size(nbytes);
  buffer->reset_offset();

  return Status::Ok();
}

Status StorageManager::read(
    const URI& uri, uint64_t offset, Buffer* buffer, uint64_t nbytes) const {
  RETURN_NOT_OK(buffer->realloc(nbytes));
  RETURN_NOT_OK(vfs_->read(uri, offset, buffer->data(), nbytes));
  buffer->set_size(nbytes);
  buffer->reset_offset();

  return Status::Ok();
}

Status StorageManager::store_array_schema(ArraySchema* array_schema) {
  auto& array_uri = array_schema->array_uri();
  URI array_schema_uri = array_uri.join_path(constants::array_schema_filename);
  URI kv_schema_uri = array_uri.join_path(constants::kv_schema_filename);
  URI schema_uri = (array_schema->is_kv()) ? kv_schema_uri : array_schema_uri;

  // Serialize
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(array_schema->serialize(buff), delete buff);

  // Delete file if it exists already
  bool exists;
  RETURN_NOT_OK(is_file(schema_uri, &exists));
  if (exists)
    RETURN_NOT_OK_ELSE(vfs_->remove_file(schema_uri), delete buff);

  // Write to file
  buff->reset_offset();
  auto tile = new Tile(
      constants::generic_tile_datatype,
      constants::generic_tile_compressor,
      constants::generic_tile_compression_level,
      constants::generic_tile_cell_size,
      0,
      buff,
      false);
  auto tile_io = new TileIO(this, schema_uri);
  Status st = tile_io->write_generic(tile);
  if (st.ok())
    st = close_file(schema_uri);

  delete tile;
  delete tile_io;
  delete buff;

  return st;
}

Status StorageManager::store_fragment_metadata(FragmentMetadata* metadata) {
  // Find the corresponding open array
  auto array_uri = metadata->array_uri();
  auto open_array_it = open_arrays_.find(array_uri.to_string());
  assert(open_array_it != open_arrays_.end());
  auto open_array = open_array_it->second;

  // Lock the open array
  open_array->mtx_lock();

  // Filelock the metadata
  auto filelock_uri = array_uri.join_path(constants::metadata_filelock);
  filelock_t fl;
  vfs_->filelock_lock(filelock_uri, &fl, VFS::XLOCK);

  // Do nothing if fragment directory does not exist. The fragment directory
  // is created only when some attribute file is written
  bool is_dir;
  const URI& fragment_uri = metadata->fragment_uri();
  RETURN_NOT_OK(vfs_->is_dir(fragment_uri, &is_dir));
  if (!is_dir) {
    vfs_->filelock_unlock(filelock_uri, fl);
    open_array->mtx_unlock();
    return Status::Ok();
  }

  // Serialize
  auto buff = new Buffer();
  auto st = metadata->serialize(buff);
  if (!st.ok()) {
    vfs_->filelock_unlock(filelock_uri, fl);
    open_array->mtx_unlock();
    delete buff;
  }

  // Write to file
  URI fragment_metadata_uri = fragment_uri.join_path(
      std::string(constants::fragment_metadata_filename));
  buff->reset_offset();
  auto tile = new Tile(
      constants::generic_tile_datatype,
      constants::generic_tile_compressor,
      constants::generic_tile_compression_level,
      constants::generic_tile_cell_size,
      0,
      buff,
      false);

  auto tile_io = new TileIO(this, fragment_metadata_uri);
  st = tile_io->write_generic(tile);
  if (st.ok())
    st = close_file(fragment_metadata_uri);

  delete tile;
  delete tile_io;
  delete buff;

  // Unlock the array and filelock
  vfs_->filelock_unlock(filelock_uri, fl);
  open_array->mtx_unlock();

  return st;
}

Status StorageManager::close_file(const URI& uri) {
  return vfs_->close_file(uri);
}

Status StorageManager::sync(const URI& uri) {
  return vfs_->sync(uri);
}

VFS* StorageManager::vfs() const {
  return vfs_;
}

void StorageManager::wait_for_zero_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_cv_.wait(
      lck, [this]() { return queries_in_progress_ == 0; });
}

Status StorageManager::write_to_cache(
    const URI& uri, uint64_t offset, Buffer* buffer) const {
  // Do nothing if the object size is larger than the cache size
  uint64_t object_size = buffer->size();
  if (object_size > tile_cache_->max_size())
    return Status::Ok();

  // Do not write metadata to cache
  std::string filename = uri.last_path_part();
  if (filename == constants::fragment_metadata_filename ||
      filename == constants::array_schema_filename ||
      filename == constants::kv_schema_filename) {
    return Status::Ok();
  }

  // Generate key (uri + offset)
  std::stringstream key;
  key << uri.to_string() << "+" << offset;

  // Insert to cache
  void* object = std::malloc(object_size);
  if (object == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot write to cache; Object memory allocation failed"));
  std::memcpy(object, buffer->data(), object_size);
  RETURN_NOT_OK(tile_cache_->insert(key.str(), object, object_size, false));

  return Status::Ok();
}

Status StorageManager::write(const URI& uri, Buffer* buffer) const {
  return vfs_->write(uri, buffer->data(), buffer->size());
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
Status StorageManager::array_compute_max_read_buffer_sizes(
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& metadata,
    const T* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Sanity check
  assert(!metadata.empty());

  // First we calculate a rough upper bound. Especially for dense
  // arrays, this will not be accurate, as it accounts only for the
  // non-empty regions of the subarray.
  for (auto& meta : metadata)
    RETURN_NOT_OK(meta->add_max_read_buffer_sizes(subarray, buffer_sizes));

  // Rectify bound for dense arrays
  if (array_schema->dense()) {
    auto cell_num = array_schema->domain()->cell_num(subarray);
    for (auto& it : *buffer_sizes) {
      if (array_schema->var_size(it.first)) {
        it.second.first = cell_num * constants::cell_var_offset_size;
        it.second.second +=
            cell_num * datatype_size(array_schema->type(it.first));
      } else {
        it.second.first = cell_num * array_schema->cell_size(it.first);
      }
    }
  }

  // Rectify bound for sparse arrays with integer domain
  if (!array_schema->dense() &&
      datatype_is_integer(array_schema->domain()->type())) {
    auto cell_num = array_schema->domain()->cell_num(subarray);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *buffer_sizes) {
        if (!array_schema->var_size(it.first))
          it.second.first = MIN(
              it.second.first, cell_num * array_schema->cell_size(it.first));
      }
    }
  }

  return Status::Ok();
}

template <class T>
void StorageManager::array_get_non_empty_domain(
    const std::vector<FragmentMetadata*>& metadata,
    unsigned dim_num,
    T* domain) {
  assert(!metadata.empty());
  uint64_t domain_size = 2 * sizeof(T) * dim_num;
  auto non_empty_domain =
      static_cast<const T*>(metadata[0]->non_empty_domain());
  std::memcpy(domain, non_empty_domain, domain_size);

  // Expand with the rest of the fragments
  auto metadata_num = metadata.size();
  auto coords = new T[dim_num];
  for (size_t j = 1; j < metadata_num; ++j) {
    non_empty_domain = static_cast<const T*>(metadata[j]->non_empty_domain());
    for (unsigned i = 0; i < dim_num; ++i)
      coords[i] = non_empty_domain[2 * i];
    utils::expand_mbr(domain, coords, dim_num);
    for (unsigned i = 0; i < dim_num; ++i)
      coords[i] = non_empty_domain[2 * i + 1];
    utils::expand_mbr(domain, coords, dim_num);
  }
  delete[] coords;
}

Status StorageManager::get_fragment_uris(
    const URI& array_uri, std::vector<URI>* fragment_uris) const {
  // Get all uris in the array directory
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(array_uri.add_trailing_slash(), &uris));

  // Get only the fragment uris
  bool exists;
  for (auto& uri : uris) {
    if (utils::starts_with(uri.last_path_part(), "."))
      continue;

    RETURN_NOT_OK(is_fragment(uri, &exists))
    if (exists)
      fragment_uris->push_back(uri);
  }

  return Status::Ok();
}

Status StorageManager::init_tbb(Config::SMParams& config) {
#ifdef HAVE_TBB
  tbb_scheduler_ = std::unique_ptr<tbb::task_scheduler_init>(
      new tbb::task_scheduler_init(config.num_tbb_threads_));
#endif
  return Status::Ok();
}

Status StorageManager::load_array_schema(
    const URI& array_uri, ObjectType object_type, OpenArray* open_array) {
  // Do nothing if the array schema is already loaded
  if (open_array->array_schema() != nullptr)
    return Status::Ok();

  auto array_schema = (ArraySchema*)nullptr;
  RETURN_NOT_OK(load_array_schema(array_uri, object_type, &array_schema));
  open_array->set_array_schema(array_schema);

  return Status::Ok();
}

Status StorageManager::load_fragment_metadata(OpenArray* open_array) {
  // Get all the fragment uris, sorted by timestamp
  std::vector<URI> fragment_uris;
  const URI& array_uri = open_array->array_uri();
  RETURN_NOT_OK(get_fragment_uris(array_uri, &fragment_uris));
  sort_fragment_uris(&fragment_uris);

  if (fragment_uris.empty())
    return Status::Ok();

  // Load the metadata for each fragment, only if they are not already loaded
  std::vector<FragmentMetadata*> fragment_metadata;
  for (auto& uri : fragment_uris) {
    auto metadata = open_array->fragment_metadata_get(uri);
    if (metadata == nullptr) {
      URI coords_uri =
          uri.join_path(constants::coords + constants::file_suffix);
      bool sparse;
      RETURN_NOT_OK(vfs_->is_file(coords_uri, &sparse));
      metadata = new FragmentMetadata(open_array->array_schema(), !sparse, uri);
      RETURN_NOT_OK_ELSE(load_fragment_metadata(metadata), delete metadata);
      fragment_metadata.push_back(metadata);
    }
  }
  open_array->set_fragment_metadata(fragment_metadata);

  return Status::Ok();
}

void StorageManager::sort_fragment_uris(std::vector<URI>* fragment_uris) const {
  // Do nothing if there are not enough fragments
  uint64_t fragment_num = fragment_uris->size();
  if (fragment_num == 0)
    return;

  // Initializations
  std::string t_str;
  uint64_t t, pos = 0;
  std::vector<std::pair<uint64_t, uint64_t>> t_pos_vec;

  // Get the timestamp for each fragment
  for (auto& uri : *fragment_uris) {
    // Get fragment name
    std::string uri_str = uri.c_str();
    if (uri_str.back() == '/')
      uri_str.pop_back();
    std::string fragment_name = URI(uri_str).last_path_part();
    assert(utils::starts_with(fragment_name, "__"));

    // Get timestamp in the end of the name after '_'
    assert(fragment_name.find_last_of("_") != std::string::npos);
    t_str = fragment_name.substr(fragment_name.find_last_of('_') + 1);
    sscanf(t_str.c_str(), "%lld", (long long int*)&t);
    t_pos_vec.emplace_back(std::pair<uint64_t, uint64_t>(t, pos++));
  }

  // Sort the names based on the timestamps
  std::sort(t_pos_vec.begin(), t_pos_vec.end());
  std::vector<URI> fragment_uris_sorted;
  fragment_uris_sorted.resize(fragment_num);
  for (uint64_t i = 0; i < fragment_num; ++i)
    fragment_uris_sorted[i] = (*fragment_uris)[t_pos_vec[i].second];
  *fragment_uris = fragment_uris_sorted;
}

}  // namespace sm
}  // namespace tiledb
