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

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile_io.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */
#ifndef MAX
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#endif

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

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

  // Release all filelocks and delete all opened arrays for reads
  for (auto& open_array_it : open_arrays_for_reads_) {
    open_array_it.second->file_unlock(vfs_);
    delete open_array_it.second;
  }

  // Delete all opened arrays for writes
  for (auto& open_array_it : open_arrays_for_writes_)
    delete open_array_it.second;

  for (auto& fl_it : xfilelocks_) {
    auto filelock = fl_it.second;
    auto lock_uri = URI(fl_it.first).join_path(constants::filelock_name);
    if (filelock != INVALID_FILELOCK)
      vfs_->filelock_unlock(lock_uri, filelock);
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManager::array_close(const URI& array_uri, QueryType query_type) {
  STATS_FUNC_IN(sm_array_close);

  if (query_type == QueryType::READ)
    return array_close_for_reads(array_uri);
  return array_close_for_writes(array_uri);

  STATS_FUNC_OUT(sm_array_close);
}

Status StorageManager::array_open(
    const URI& array_uri,
    QueryType query_type,
    const EncryptionKey& encryption_key,
    OpenArray** open_array,
    uint64_t timestamp) {
  STATS_FUNC_IN(sm_array_open);

  if (query_type == QueryType::READ)
    return array_open_for_reads(
        array_uri, encryption_key, open_array, timestamp);
  return array_open_for_writes(array_uri, encryption_key, open_array);

  STATS_FUNC_OUT(sm_array_open);
}

Status StorageManager::array_reopen(
    OpenArray* open_array,
    const EncryptionKey& encryption_key,
    uint64_t timestamp) {
  // Lock mutex
  {
    std::lock_guard<std::mutex> lock{open_array_for_reads_mtx_};

    // Find the open array entry
    auto array_uri = open_array->array_uri();
    auto it = open_arrays_for_reads_.find(array_uri.to_string());
    if (it == open_arrays_for_reads_.end()) {
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot reopen array ") + array_uri.to_string() +
          "; Array not open"));
    }
    // Lock the array
    open_array->mtx_lock();
  }

  // Get fragment metadata in the case of reads, if not fetched already
  bool in_cache;
  auto st =
      load_fragment_metadata(open_array, encryption_key, &in_cache, timestamp);
  if (!st.ok()) {
    open_array->mtx_unlock();
    return st;
  }

  // Check the encryption key. Note we always pass true for cache hit by
  // definition of reopening an array.
  st = check_array_encryption_key(
      open_array->array_schema(), encryption_key, true);

  // Unlock the mutexes
  open_array->mtx_unlock();

  return st;
}

Status StorageManager::array_compute_max_buffer_sizes(
    OpenArray* open_array,
    uint64_t timestamp,
    const void* subarray,
    const std::vector<std::string>& attributes,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        max_buffer_sizes) {
  // Error if the array was not opened in read mode
  if (open_array->query_type() != QueryType::READ)
    return LOG_STATUS(
        Status::StorageManagerError("Cannot compute maximum read buffer sizes; "
                                    "Array was not opened in read mode"));

  // Get array schema and fragment metadata
  open_array->mtx_lock();
  auto array_schema = open_array->array_schema();
  auto metadata = open_array->fragment_metadata(timestamp);
  open_array->mtx_unlock();

  // Check attributes
  RETURN_NOT_OK(array_schema->check_attributes(attributes));

  // Compute buffer sizes
  max_buffer_sizes->clear();
  for (const auto& attr : attributes)
    (*max_buffer_sizes)[attr] = std::pair<uint64_t, uint64_t>(0, 0);
  RETURN_NOT_OK(array_compute_max_buffer_sizes(
      array_schema, metadata, subarray, max_buffer_sizes));

  // Close array
  return Status::Ok();
}

Status StorageManager::array_compute_max_buffer_sizes(
    OpenArray* open_array,
    uint64_t timestamp,
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        max_buffer_sizes) {
  // Error if the array was not opened in read mode
  if (open_array->query_type() != QueryType::READ)
    return LOG_STATUS(
        Status::StorageManagerError("Cannot compute maximum buffer sizes; "
                                    "Array was not opened in read mode"));

  // Get array schema and fragment metadata
  open_array->mtx_lock();
  auto array_schema = open_array->array_schema();
  auto metadata = open_array->fragment_metadata(timestamp);
  open_array->mtx_unlock();

  // Get all attributes and coordinates
  std::vector<std::string> attributes;
  auto schema_attributes = array_schema->attributes();
  for (const auto& attr : schema_attributes)
    attributes.push_back(attr->name());
  attributes.push_back(constants::coords);

  return array_compute_max_buffer_sizes(
      open_array, timestamp, subarray, attributes, max_buffer_sizes);
}

Status StorageManager::array_compute_max_buffer_sizes(
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
      return array_compute_max_buffer_sizes<int>(
          array_schema,
          fragment_metadata,
          static_cast<const int*>(subarray),
          buffer_sizes);
    case Datatype::INT64:
      return array_compute_max_buffer_sizes<int64_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int64_t*>(subarray),
          buffer_sizes);
    case Datatype::FLOAT32:
      return array_compute_max_buffer_sizes<float>(
          array_schema,
          fragment_metadata,
          static_cast<const float*>(subarray),
          buffer_sizes);
    case Datatype::FLOAT64:
      return array_compute_max_buffer_sizes<double>(
          array_schema,
          fragment_metadata,
          static_cast<const double*>(subarray),
          buffer_sizes);
    case Datatype::INT8:
      return array_compute_max_buffer_sizes<int8_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int8_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT8:
      return array_compute_max_buffer_sizes<uint8_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint8_t*>(subarray),
          buffer_sizes);
    case Datatype::INT16:
      return array_compute_max_buffer_sizes<int16_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int16_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT16:
      return array_compute_max_buffer_sizes<uint16_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint16_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT32:
      return array_compute_max_buffer_sizes<uint32_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint32_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT64:
      return array_compute_max_buffer_sizes<uint64_t>(
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

Status StorageManager::array_compute_est_read_buffer_sizes(
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& fragment_metadata,
    const void* subarray,
    std::unordered_map<std::string, std::pair<double, double>>* buffer_sizes) {
  // Return if there are no metadata
  if (fragment_metadata.empty())
    return Status::Ok();

  // Compute buffer sizes
  switch (array_schema->coords_type()) {
    case Datatype::INT32:
      return array_compute_est_read_buffer_sizes<int>(
          array_schema,
          fragment_metadata,
          static_cast<const int*>(subarray),
          buffer_sizes);
    case Datatype::INT64:
      return array_compute_est_read_buffer_sizes<int64_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int64_t*>(subarray),
          buffer_sizes);
    case Datatype::FLOAT32:
      return array_compute_est_read_buffer_sizes<float>(
          array_schema,
          fragment_metadata,
          static_cast<const float*>(subarray),
          buffer_sizes);
    case Datatype::FLOAT64:
      return array_compute_est_read_buffer_sizes<double>(
          array_schema,
          fragment_metadata,
          static_cast<const double*>(subarray),
          buffer_sizes);
    case Datatype::INT8:
      return array_compute_est_read_buffer_sizes<int8_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int8_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT8:
      return array_compute_est_read_buffer_sizes<uint8_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint8_t*>(subarray),
          buffer_sizes);
    case Datatype::INT16:
      return array_compute_est_read_buffer_sizes<int16_t>(
          array_schema,
          fragment_metadata,
          static_cast<const int16_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT16:
      return array_compute_est_read_buffer_sizes<uint16_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint16_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT32:
      return array_compute_est_read_buffer_sizes<uint32_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint32_t*>(subarray),
          buffer_sizes);
    case Datatype::UINT64:
      return array_compute_est_read_buffer_sizes<uint64_t>(
          array_schema,
          fragment_metadata,
          static_cast<const uint64_t*>(subarray),
          buffer_sizes);
    default:
      return LOG_STATUS(
          Status::StorageManagerError("Cannot compute estimate for read buffer "
                                      "sizes; Invalid coordinates type"));
  }

  return Status::Ok();
}

Status StorageManager::array_consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
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
  return consolidator_->consolidate(
      array_name, encryption_type, encryption_key, key_length);
}

Status StorageManager::array_create(
    const URI& array_uri,
    ArraySchema* array_schema,
    const EncryptionKey& encryption_key) {
  // Check array schema
  if (array_schema == nullptr) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot create array; Empty array schema"));
  }

  // Check if array exists
  bool exists = false;
  RETURN_NOT_OK(is_array(array_uri, &exists));
  if (exists)
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Cannot create array; Array '") + array_uri.c_str() +
        "' already exists"));

  std::lock_guard<std::mutex> lock{object_create_mtx_};
  array_schema->set_array_uri(array_uri);
  RETURN_NOT_OK(array_schema->check());

  // Create array directory
  RETURN_NOT_OK(vfs_->create_dir(array_uri));

  // Store array schema
  Status st = store_array_schema(array_schema, encryption_key);
  if (!st.ok()) {
    vfs_->remove_file(array_uri);
    return st;
  }

  // Create array filelock
  URI filelock_uri = array_uri.join_path(constants::filelock_name);
  st = vfs_->touch(filelock_uri);
  if (!st.ok()) {
    vfs_->remove_file(array_uri);
    return st;
  }
  return st;
}

Status StorageManager::array_get_non_empty_domain(
    Array* array, void* domain, bool* is_empty) {
  if (array == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Array object is null"));

  if (open_arrays_for_reads_.find(array->array_uri().to_string()) ==
      open_arrays_for_reads_.end())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Array not opened for reads"));

  // Open the array
  *is_empty = true;
  auto array_schema = array->array_schema();
  auto metadata = array->fragment_metadata();

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
  // Wait until the array is closed for reads
  std::unique_lock<std::mutex> lk(open_array_for_reads_mtx_);
  xlock_cv_.wait(lk, [this, array_uri] {
    return open_arrays_for_reads_.find(array_uri.to_string()) ==
           open_arrays_for_reads_.end();
  });

  // Retrieve filelock
  filelock_t filelock = INVALID_FILELOCK;
  auto lock_uri = array_uri.join_path(constants::filelock_name);
  RETURN_NOT_OK(vfs_->filelock_lock(lock_uri, &filelock, false));
  xfilelocks_[array_uri.to_string()] = filelock;

  return Status::Ok();
}

Status StorageManager::array_xunlock(const URI& array_uri) {
  // Get filelock if it exists
  auto it = xfilelocks_.find(array_uri.to_string());
  if (it == xfilelocks_.end())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot unlock array exclusive lock; Filelock not found"));
  auto filelock = it->second;

  auto lock_uri = array_uri.join_path(constants::filelock_name);
  if (filelock != INVALID_FILELOCK)
    RETURN_NOT_OK(vfs_->filelock_unlock(lock_uri, filelock));
  xfilelocks_.erase(it);

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

Status StorageManager::check_array_encryption_key(
    const ArraySchema* schema,
    const EncryptionKey& encryption_key,
    bool was_cache_hit) {
  std::string uri = schema->array_uri().to_string();
  EncryptionKeyValidation* validation = nullptr;

  // Get or add the validation instance
  std::lock_guard<std::mutex> lck(open_arrays_encryption_keys_mtx_);
  auto it = open_arrays_encryption_keys_.find(uri);
  if (it == open_arrays_encryption_keys_.end()) {
    // Sanity check for cached schemas, which should already have added a
    // validation instance.
    if (was_cache_hit)
      return LOG_STATUS(
          Status::StorageManagerError("Encryption key check failed; schema was "
                                      "cached but key not previously used."));

    auto ptr =
        std::unique_ptr<EncryptionKeyValidation>(new EncryptionKeyValidation);
    validation = ptr.get();
    open_arrays_encryption_keys_[uri] = std::move(ptr);
  } else {
    validation = it->second.get();
  }

  assert(validation != nullptr);
  return validation->check_encryption_key(encryption_key);
}

void StorageManager::decrement_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_--;
  queries_in_progress_cv_.notify_all();
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

  std::lock_guard<std::mutex> lock{object_create_mtx_};

  // Create group directory
  RETURN_NOT_OK(vfs_->create_dir(uri));

  // Create group file
  URI group_filename = uri.join_path(constants::group_filename);
  Status st = vfs_->touch(group_filename);
  if (!st.ok()) {
    vfs_->remove_dir(uri);
    return st;
  }
  return st;
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
  reader_thread_pool_ = std::unique_ptr<ThreadPool>(new ThreadPool());
  RETURN_NOT_OK(reader_thread_pool_->init(sm_params.num_reader_threads_));
  writer_thread_pool_ = std::unique_ptr<ThreadPool>(new ThreadPool());
  RETURN_NOT_OK(writer_thread_pool_->init(sm_params.num_writer_threads_));
  tile_cache_ = new LRUCache(sm_params.tile_cache_size_);
  vfs_ = new VFS();
  RETURN_NOT_OK(vfs_->init(config_.vfs_params()));
  auto& global_state = global_state::GlobalState::GetGlobalState();
  RETURN_NOT_OK(global_state.initialize(config));
  global_state.register_storage_manager(this);

  STATS_COUNTER_ADD(sm_contexts_created, 1);

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
  RETURN_NOT_OK(TileIO::is_generic_tile(
      this, uri.join_path(constants::fragment_metadata_filename), is_fragment));
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
    const URI& array_uri,
    ObjectType object_type,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema,
    bool* in_cache) {
  if (array_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load array schema; Invalid array URI"));

  assert(
      object_type == ObjectType::ARRAY || object_type == ObjectType::KEY_VALUE);
  URI schema_uri = (object_type == ObjectType::ARRAY) ?
                       array_uri.join_path(constants::array_schema_filename) :
                       array_uri.join_path(constants::kv_schema_filename);

  // Try to read from cache
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      array_schema_cache_->read(schema_uri.to_string(), buff, in_cache),
      delete buff);

  // Read from file if not in cache
  if (!(*in_cache)) {
    delete buff;
    auto tile_io = new TileIO(this, schema_uri);
    auto tile = (Tile*)nullptr;
    RETURN_NOT_OK_ELSE(
        tile_io->read_generic(&tile, 0, encryption_key), delete tile_io);
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
    delete *array_schema;
    *array_schema = nullptr;
  }

  // Check encryption key is valid and correct. If the schema was not read from
  // cache, we only get here when the encryption key is actually
  // valid (reading the schema from disk would have failed with an invalid key).
  // If the schema was cached, this will check that the given key is the same as
  // the key used when first loading the schema.
  st = check_array_encryption_key(*array_schema, encryption_key, *in_cache);
  if (!st.ok()) {
    delete *array_schema;
    *array_schema = nullptr;
  }

  // Store in cache
  if (st.ok() && !(*in_cache) &&
      buff->size() <= array_schema_cache_->max_size()) {
    buff->disown_data();
    st = array_schema_cache_->insert(
        schema_uri.to_string(), buff->data(), buff->size());
  }

  delete buff;

  return st;
}

Status StorageManager::load_fragment_metadata(
    FragmentMetadata* fragment_metadata,
    const EncryptionKey& encryption_key,
    bool* in_cache) {
  const URI& fragment_uri = fragment_metadata->fragment_uri();
  bool fragment_exists;
  RETURN_NOT_OK(is_fragment(fragment_uri, &fragment_exists));
  if (!fragment_exists)
    return Status::StorageManagerError(
        "Cannot load fragment metadata; Fragment does not exist");

  URI fragment_metadata_uri = fragment_uri.join_path(
      std::string(constants::fragment_metadata_filename));

  // Try to read from cache
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      fragment_metadata_cache_->read(
          fragment_metadata_uri.to_string(), buff, in_cache),
      delete buff);

  // Read from file if not in cache
  if (!(*in_cache)) {
    delete buff;
    auto tile_io = new TileIO(this, fragment_metadata_uri);
    auto tile = (Tile*)nullptr;
    RETURN_NOT_OK_ELSE(
        tile_io->read_generic(&tile, 0, encryption_key), delete tile_io);
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
  if (st.ok() && !(*in_cache) &&
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
    dir_uri =
        URI(utils::parse::ends_with(uri_str, "/") ? uri_str : (uri_str + "/"));
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
    if (utils::parse::ends_with(uri_str, constants::group_filename)) {
      *type = ObjectType::GROUP;
      return Status::Ok();
    } else if (utils::parse::ends_with(
                   uri_str, constants::kv_schema_filename)) {
      *type = ObjectType::KEY_VALUE;
      return Status::Ok();
    } else if (utils::parse::ends_with(
                   uri_str, constants::array_schema_filename)) {
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

Status StorageManager::query_submit(Query* query) {
  STATS_COUNTER_ADD_IF(
      query->type() == QueryType::READ, sm_query_submit_read, 1);
  STATS_COUNTER_ADD_IF(
      query->type() == QueryType::WRITE, sm_query_submit_write, 1);
  STATS_COUNTER_ADD_IF(
      query->layout() == Layout::COL_MAJOR,
      sm_query_submit_layout_col_major,
      1);
  STATS_COUNTER_ADD_IF(
      query->layout() == Layout::ROW_MAJOR,
      sm_query_submit_layout_row_major,
      1);
  STATS_COUNTER_ADD_IF(
      query->layout() == Layout::GLOBAL_ORDER,
      sm_query_submit_layout_global_order,
      1);
  STATS_COUNTER_ADD_IF(
      query->layout() == Layout::UNORDERED,
      sm_query_submit_layout_unordered,
      1);
  STATS_FUNC_IN(sm_query_submit);

  // Process the query
  QueryInProgress in_progress(this);
  auto st = query->process();

  return st;

  STATS_FUNC_OUT(sm_query_submit);
}

Status StorageManager::query_submit_async(Query* query) {
  // Push the query into the async queue
  return async_push_query(query);
}

Status StorageManager::read_from_cache(
    const URI& uri,
    uint64_t offset,
    Buffer* buffer,
    uint64_t nbytes,
    bool* in_cache) const {
  STATS_FUNC_IN(sm_read_from_cache);

  std::stringstream key;
  key << uri.to_string() << "+" << offset;
  RETURN_NOT_OK(buffer->realloc(nbytes));
  RETURN_NOT_OK(
      tile_cache_->read(key.str(), buffer->data(), 0, nbytes, in_cache));
  buffer->set_size(nbytes);
  buffer->reset_offset();

  return Status::Ok();

  STATS_FUNC_OUT(sm_read_from_cache);
}

Status StorageManager::read(
    const URI& uri, uint64_t offset, Buffer* buffer, uint64_t nbytes) const {
  RETURN_NOT_OK(buffer->realloc(nbytes));
  RETURN_NOT_OK(vfs_->read(uri, offset, buffer->data(), nbytes));
  buffer->set_size(nbytes);
  buffer->reset_offset();

  return Status::Ok();
}

ThreadPool* StorageManager::reader_thread_pool() const {
  return reader_thread_pool_.get();
}

Status StorageManager::store_array_schema(
    ArraySchema* array_schema, const EncryptionKey& encryption_key) {
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
      constants::generic_tile_cell_size,
      0,
      buff,
      false);
  auto tile_io = new TileIO(this, schema_uri);
  Status st = tile_io->write_generic(tile, encryption_key);
  if (st.ok())
    st = close_file(schema_uri);

  delete tile;
  delete tile_io;
  delete buff;

  return st;
}

Status StorageManager::store_fragment_metadata(
    FragmentMetadata* metadata, const EncryptionKey& encryption_key) {
  // For thread-safety while loading fragment metadata
  std::lock_guard<std::mutex> lock{open_array_for_reads_mtx_};

  // Do nothing if fragment directory does not exist. The fragment directory
  // is created only when some attribute file is written
  bool is_dir = false;
  const URI& fragment_uri = metadata->fragment_uri();
  RETURN_NOT_OK(vfs_->is_dir(fragment_uri, &is_dir));
  if (!is_dir) {
    return Status::Ok();
  }

  // Serialize
  auto buff = new Buffer();
  Status st = metadata->serialize(buff);
  if (!st.ok()) {
    delete buff;
    return st;
  }

  // Write to file
  URI fragment_metadata_uri = fragment_uri.join_path(
      std::string(constants::fragment_metadata_filename));
  buff->reset_offset();
  auto tile = new Tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      buff,
      false);

  auto tile_io = new TileIO(this, fragment_metadata_uri);
  st = tile_io->write_generic(tile, encryption_key);
  if (st.ok()) {
    st = close_file(fragment_metadata_uri);
  }

  delete tile;
  delete tile_io;
  delete buff;

  return st;
}

Status StorageManager::close_file(const URI& uri) {
  return vfs_->close_file(uri);
}

Status StorageManager::sync(const URI& uri) {
  return vfs_->sync(uri);
}

ThreadPool* StorageManager::writer_thread_pool() const {
  return writer_thread_pool_.get();
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
  STATS_FUNC_IN(sm_write_to_cache);

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

  STATS_FUNC_OUT(sm_write_to_cache);
}

Status StorageManager::write(const URI& uri, Buffer* buffer) const {
  return vfs_->write(uri, buffer->data(), buffer->size());
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
Status StorageManager::array_compute_max_buffer_sizes(
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
    RETURN_NOT_OK(meta->add_max_buffer_sizes(subarray, buffer_sizes));

  // Rectify bound for dense arrays
  if (array_schema->dense()) {
    auto cell_num = array_schema->domain()->cell_num(subarray);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
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
  }

  // Rectify bound for sparse arrays with integer domain
  if (!array_schema->dense() &&
      datatype_is_integer(array_schema->domain()->type())) {
    auto cell_num = array_schema->domain()->cell_num(subarray);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *buffer_sizes) {
        if (!array_schema->var_size(it.first)) {
          // Check for overflow
          uint64_t new_size = cell_num * array_schema->cell_size(it.first);
          if (new_size / array_schema->cell_size((it.first)) != cell_num)
            continue;

          // Potentially rectify size
          it.second.first = MIN(it.second.first, new_size);
        }
      }
    }
  }

  return Status::Ok();
}

template <class T>
Status StorageManager::array_compute_est_read_buffer_sizes(
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& metadata,
    const T* subarray,
    std::unordered_map<std::string, std::pair<double, double>>* buffer_sizes) {
  // Sanity check
  assert(!metadata.empty());

  // First we calculate a rough upper bound. Especially for dense
  // arrays, this will not be accurate, as it accounts only for the
  // non-empty regions of the subarray.
  for (auto& meta : metadata)
    RETURN_NOT_OK(meta->add_est_read_buffer_sizes(subarray, buffer_sizes));

  // Rectify bound for dense arrays
  if (array_schema->dense()) {
    auto cell_num = array_schema->domain()->cell_num(subarray);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *buffer_sizes) {
        if (array_schema->var_size(it.first)) {
          it.second.first = cell_num * constants::cell_var_offset_size;
        } else {
          it.second.first = cell_num * array_schema->cell_size(it.first);
        }
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
        if (!array_schema->var_size(it.first)) {
          // Check for overflow
          uint64_t new_size = cell_num * array_schema->cell_size(it.first);
          if (new_size / array_schema->cell_size((it.first)) != cell_num)
            continue;

          // Potentially rectify size
          it.second.first = MIN(it.second.first, new_size);
        }
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
    utils::geometry::expand_mbr(domain, coords, dim_num);
    for (unsigned i = 0; i < dim_num; ++i)
      coords[i] = non_empty_domain[2 * i + 1];
    utils::geometry::expand_mbr(domain, coords, dim_num);
  }
  delete[] coords;
}

Status StorageManager::array_close_for_reads(const URI& array_uri) {
  // Lock mutex
  std::lock_guard<std::mutex> lock{open_array_for_reads_mtx_};

  // Find the open array entry
  auto it = open_arrays_for_reads_.find(array_uri.to_string());

  // Do nothing if array is closed
  if (it == open_arrays_for_reads_.end()) {
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
      return st;
    }
    // Remove open array entry
    open_array->mtx_unlock();
    delete open_array;
    open_arrays_for_reads_.erase(it);
  } else {  // Just unlock the array mutex
    open_array->mtx_unlock();
  }

  xlock_cv_.notify_all();

  return Status::Ok();
}

Status StorageManager::array_close_for_writes(const URI& array_uri) {
  // Lock mutex
  std::lock_guard<std::mutex> lock{open_array_for_writes_mtx_};

  // Find the open array entry
  auto it = open_arrays_for_writes_.find(array_uri.to_string());

  // Do nothing if array is closed
  if (it == open_arrays_for_writes_.end()) {
    return Status::Ok();
  }

  // For easy reference
  OpenArray* open_array = it->second;

  // Lock the mutex of the array and decrement counter
  open_array->mtx_lock();
  open_array->cnt_decr();

  // Close the array if the counter reaches 0
  if (open_array->cnt() == 0) {
    open_array->mtx_unlock();
    delete open_array;
    open_arrays_for_writes_.erase(it);
  } else {  // Just unlock the array mutex
    open_array->mtx_unlock();
  }

  return Status::Ok();
}

Status StorageManager::array_open_for_reads(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    OpenArray** open_array,
    uint64_t timestamp) {
  if (!vfs_->supports_uri_scheme(array_uri))
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot open array; URI scheme unsupported."));

  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(this->object_type(array_uri, &obj_type));
  if (obj_type != ObjectType::ARRAY && obj_type != ObjectType::KEY_VALUE) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot open array; Array does not exist"));
  }

  // Lock mutex
  {
    std::lock_guard<std::mutex> lock{open_array_for_reads_mtx_};

    // Find the open array entry
    auto it = open_arrays_for_reads_.find(array_uri.to_string());
    if (it != open_arrays_for_reads_.end()) {
      *open_array = it->second;
    } else {  // Create a new entry
      *open_array = new OpenArray(array_uri, QueryType::READ);
      open_arrays_for_reads_[array_uri.to_string()] = *open_array;
    }
    // Lock the array and increment counter
    (*open_array)->mtx_lock();
    (*open_array)->cnt_incr();
  }

  // Acquire a shared filelock
  auto st = (*open_array)->file_lock(vfs_);
  if (!st.ok()) {
    (*open_array)->mtx_unlock();
    array_close((*open_array)->array_uri(), QueryType::READ);
    return st;
  }

  // Load array schema if not fetched already
  bool in_cache = true;
  if ((*open_array)->array_schema() == nullptr) {
    auto st = load_array_schema(
        array_uri, obj_type, *open_array, encryption_key, &in_cache);
    if (!st.ok()) {
      (*open_array)->mtx_unlock();
      array_close((*open_array)->array_uri(), QueryType::READ);
      return st;
    }
  }

  // Check encryption key is valid and correct. If the schema was not read from
  // cache, we only get here when the encryption key is actually
  // valid (reading the schema from disk would have failed with an invalid key).
  // If the schema was cached, this will check that the given key is the same as
  // the key used when first loading the schema.
  st = check_array_encryption_key(
      (*open_array)->array_schema(), encryption_key, in_cache);
  if (!st.ok()) {
    (*open_array)->mtx_unlock();
    array_close((*open_array)->array_uri(), QueryType::READ);
    return st;
  }

  // Get fragment metadata in the case of reads, if not fetched already
  st =
      load_fragment_metadata(*open_array, encryption_key, &in_cache, timestamp);
  if (!st.ok()) {
    (*open_array)->mtx_unlock();
    array_close((*open_array)->array_uri(), QueryType::READ);
    return st;
  }

  // Unlock the array mutex
  (*open_array)->mtx_unlock();

  // Note that we retain the (shared) lock on the array filelock
  return Status::Ok();
}

Status StorageManager::array_open_for_writes(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    OpenArray** open_array) {
  if (!vfs_->supports_uri_scheme(array_uri))
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot open array; URI scheme unsupported."));

  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(this->object_type(array_uri, &obj_type));
  if (obj_type != ObjectType::ARRAY && obj_type != ObjectType::KEY_VALUE) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot open array; Array does not exist"));
  }

  // Lock mutex
  {
    std::lock_guard<std::mutex> lock{open_array_for_writes_mtx_};

    // Find the open array entry
    auto it = open_arrays_for_writes_.find(array_uri.to_string());
    if (it != open_arrays_for_writes_.end()) {
      *open_array = it->second;
    } else {  // Create a new entry
      *open_array = new OpenArray(array_uri, QueryType::WRITE);
      open_arrays_for_writes_[array_uri.to_string()] = *open_array;
    }
    // Lock the array and increment counter
    (*open_array)->mtx_lock();
    (*open_array)->cnt_incr();
  }

  // No shared filelock needed to be acquired

  // Load array schema if not fetched already
  bool in_cache = true;
  if ((*open_array)->array_schema() == nullptr) {
    auto st = load_array_schema(
        array_uri, obj_type, *open_array, encryption_key, &in_cache);
    if (!st.ok()) {
      (*open_array)->mtx_unlock();
      array_close((*open_array)->array_uri(), QueryType::WRITE);
      return st;
    }
  }

  // Check encryption key is valid and correct. If the schema was not read from
  // cache, we only get here when the encryption key is actually
  // valid (reading the schema from disk would have failed with an invalid key).
  // If the schema was cached, this will check that the given key is the same as
  // the key used when first loading the schema.
  auto st = check_array_encryption_key(
      (*open_array)->array_schema(), encryption_key, in_cache);
  if (!st.ok()) {
    (*open_array)->mtx_unlock();
    array_close((*open_array)->array_uri(), QueryType::WRITE);
    return st;
  }

  // No fragment metadata to be loaded

  // Unlock the array mutex
  (*open_array)->mtx_unlock();

  return Status::Ok();
}

Status StorageManager::get_fragment_uris(
    const URI& array_uri, std::vector<URI>* fragment_uris) const {
  // Get all uris in the array directory
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(array_uri.add_trailing_slash(), &uris));

  // Get only the fragment uris
  bool exists;
  for (auto& uri : uris) {
    if (utils::parse::starts_with(uri.last_path_part(), "."))
      continue;

    RETURN_NOT_OK(is_fragment(uri, &exists))
    if (exists)
      fragment_uris->push_back(uri);
  }

  return Status::Ok();
}

Status StorageManager::load_array_schema(
    const URI& array_uri,
    ObjectType object_type,
    OpenArray* open_array,
    const EncryptionKey& encryption_key,
    bool* in_cache) {
  // Do nothing if the array schema is already loaded
  if (open_array->array_schema() != nullptr)
    return Status::Ok();

  auto array_schema = (ArraySchema*)nullptr;
  RETURN_NOT_OK(load_array_schema(
      array_uri, object_type, encryption_key, &array_schema, in_cache));
  open_array->set_array_schema(array_schema);

  return Status::Ok();
}

Status StorageManager::load_fragment_metadata(
    OpenArray* open_array,
    const EncryptionKey& encryption_key,
    bool* in_cache,
    uint64_t timestamp) {
  // Get all the fragment uris, sorted by timestamp
  std::vector<URI> fragment_uris;
  const URI& array_uri = open_array->array_uri();
  RETURN_NOT_OK(get_fragment_uris(array_uri, &fragment_uris));

  // Check if the array is empty
  if (fragment_uris.empty())
    return Status::Ok();

  *in_cache = false;
  // Sort the URIs by timestamp
  std::vector<std::pair<uint64_t, URI>> sorted_fragment_uris;
  sort_fragment_uris(fragment_uris, &sorted_fragment_uris);

  // Load the metadata for each fragment, only if they are not already loaded
  for (auto& sf : sorted_fragment_uris) {
    auto frag_timestamp = sf.first;
    auto frag_uri = sf.second;
    if (!open_array->fragment_metadata_exists(frag_uri) &&
        frag_timestamp <= timestamp) {
      URI coords_uri =
          frag_uri.join_path(constants::coords + constants::file_suffix);
      bool sparse;
      RETURN_NOT_OK(vfs_->is_file(coords_uri, &sparse));
      auto metadata = new FragmentMetadata(
          open_array->array_schema(), !sparse, frag_uri, frag_timestamp);
      bool metadata_in_cache;
      RETURN_NOT_OK_ELSE(
          load_fragment_metadata(metadata, encryption_key, &metadata_in_cache),
          delete metadata);
      *in_cache |= metadata_in_cache;
      open_array->insert_fragment_metadata(metadata);
    }
  }

  return Status::Ok();
}

void StorageManager::sort_fragment_uris(
    const std::vector<URI>& fragment_uris,
    std::vector<std::pair<uint64_t, URI>>* sorted_fragment_uris) const {
  // Do nothing if there are not enough fragments
  if (fragment_uris.empty())
    return;

  // Initializations
  std::string t_str;
  uint64_t t;

  // Get the timestamp for each fragment
  for (auto& uri : fragment_uris) {
    // Get fragment name
    std::string uri_str = uri.c_str();
    if (uri_str.back() == '/')
      uri_str.pop_back();
    std::string fragment_name = URI(uri_str).last_path_part();
    assert(utils::parse::starts_with(fragment_name, "__"));

    // Get timestamp in the end of the name after '_'
    assert(fragment_name.find_last_of('_') != std::string::npos);
    t_str = fragment_name.substr(fragment_name.find_last_of('_') + 1);
    sscanf(t_str.c_str(), "%lld", (long long int*)&t);
    sorted_fragment_uris->emplace_back(t, uri);
  }

  // Sort the names based on the timestamps
  std::sort(sorted_fragment_uris->begin(), sorted_fragment_uris->end());
}

}  // namespace sm
}  // namespace tiledb
