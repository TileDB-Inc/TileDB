/**
 * @file   storage_manager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
#include <functional>
#include <iostream>
#include <sstream>

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/rest/rest_client.h"
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
  tile_cache_ = nullptr;
  vfs_ = nullptr;
  cancellation_in_progress_ = false;
  queries_in_progress_ = 0;
}

StorageManager::~StorageManager() {
  global_state::GlobalState::GetGlobalState().unregister_storage_manager(this);

  if (vfs_ != nullptr)
    cancel_all_tasks();

  delete tile_cache_;

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
      vfs_->filelock_unlock(lock_uri);
  }

  if (vfs_ != nullptr) {
    const Status st = vfs_->terminate();
    if (!st.ok()) {
      LOG_STATUS(Status::StorageManagerError("Failed to terminate VFS."));
    }

    delete vfs_;
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManager::array_close_for_reads(const URI& array_uri) {
  STATS_FUNC_IN(sm_array_close_for_reads);
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
  STATS_FUNC_OUT(sm_array_close_for_reads);
}

Status StorageManager::array_close_for_writes(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    Metadata* array_metadata) {
  STATS_FUNC_IN(sm_array_close_for_writes);
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

  // Flush the array metadata
  RETURN_NOT_OK(
      store_array_metadata(array_uri, encryption_key, array_metadata));

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
  STATS_FUNC_OUT(sm_array_close_for_writes);
}

Status StorageManager::array_open_for_reads(
    const URI& array_uri,
    uint64_t timestamp,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema,
    std::vector<FragmentMetadata*>* fragment_metadata,
    Metadata* metadata) {
  STATS_FUNC_IN(sm_array_open_for_reads);

  // Open array without fragments
  auto open_array = (OpenArray*)nullptr;
  RETURN_NOT_OK_ELSE(
      array_open_without_fragments(array_uri, encryption_key, &open_array),
      *array_schema = nullptr);

  // Retrieve array schema
  *array_schema = open_array->array_schema();

  // Determine which fragments to load
  std::vector<TimestampedURI> fragments_to_load;
  std::vector<URI> fragment_uris;
  RETURN_NOT_OK(get_fragment_uris(array_uri, &fragment_uris));
  RETURN_NOT_OK(get_sorted_uris(fragment_uris, timestamp, &fragments_to_load));

  // Get fragment metadata in the case of reads, if not fetched already
  Status st = load_fragment_metadata(
      open_array, encryption_key, fragments_to_load, fragment_metadata);
  if (!st.ok()) {
    open_array->mtx_unlock();
    array_close_for_reads(array_uri);
    *array_schema = nullptr;
    return st;
  }

  // Determine which array metadata to load
  std::vector<TimestampedURI> array_metadata_to_load;
  std::vector<URI> array_metadata_uris;
  RETURN_NOT_OK(get_array_metadata_uris(array_uri, &array_metadata_uris));
  RETURN_NOT_OK(
      get_sorted_uris(array_metadata_uris, timestamp, &array_metadata_to_load));

  // Get the array metadata
  st = load_array_metadata(
      open_array, encryption_key, array_metadata_to_load, metadata);
  if (!st.ok()) {
    open_array->mtx_unlock();
    array_close_for_reads(array_uri);
    *array_schema = nullptr;
    return st;
  }

  // Unlock the array mutex
  open_array->mtx_unlock();

  // Note that we retain the (shared) lock on the array filelock
  return Status::Ok();

  STATS_FUNC_OUT(sm_array_open_for_reads);
}

Status StorageManager::array_open_for_reads(
    const URI& array_uri,
    const std::vector<FragmentInfo>& fragments,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  STATS_FUNC_IN(sm_array_open_for_reads);

  // Open array without fragments
  auto open_array = (OpenArray*)nullptr;
  RETURN_NOT_OK_ELSE(
      array_open_without_fragments(array_uri, encryption_key, &open_array),
      *array_schema = nullptr);

  // Retrieve array schema
  *array_schema = open_array->array_schema();

  // Determine which fragments to load
  std::vector<TimestampedURI> fragments_to_load;
  for (const auto& fragment : fragments)
    fragments_to_load.emplace_back(fragment.uri_, fragment.timestamp_range_);

  // Get fragment metadata in the case of reads, if not fetched already
  Status st = load_fragment_metadata(
      open_array, encryption_key, fragments_to_load, fragment_metadata);
  if (!st.ok()) {
    open_array->mtx_unlock();
    array_close_for_reads(array_uri);
    *array_schema = nullptr;
    return st;
  }

  // Note: This function does not load any array metadata!

  // Unlock the array mutex
  open_array->mtx_unlock();

  // Note that we retain the (shared) lock on the array filelock
  return Status::Ok();

  STATS_FUNC_OUT(sm_array_open_for_reads);
}

Status StorageManager::array_open_for_writes(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema) {
  STATS_FUNC_IN(sm_array_open_for_writes);

  if (!vfs_->supports_uri_scheme(array_uri))
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot open array; URI scheme unsupported."));

  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(this->object_type(array_uri, &obj_type));
  if (obj_type != ObjectType::ARRAY) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot open array; Array does not exist"));
  }

  auto open_array = (OpenArray*)nullptr;

  // Lock mutex
  {
    std::lock_guard<std::mutex> lock{open_array_for_writes_mtx_};

    // Find the open array entry and check key correctness
    auto it = open_arrays_for_writes_.find(array_uri.to_string());
    if (it != open_arrays_for_writes_.end()) {
      RETURN_NOT_OK(it->second->set_encryption_key(encryption_key));
      open_array = it->second;
    } else {  // Create a new entry
      open_array = new OpenArray(array_uri, QueryType::WRITE);
      RETURN_NOT_OK_ELSE(
          open_array->set_encryption_key(encryption_key), delete open_array);
      open_arrays_for_writes_[array_uri.to_string()] = open_array;
    }

    // Lock the array and increment counter
    open_array->mtx_lock();
    open_array->cnt_incr();
  }

  // No shared filelock needed to be acquired

  // Load array schema if not fetched already
  if (open_array->array_schema() == nullptr) {
    auto st = load_array_schema(array_uri, open_array, encryption_key);
    if (!st.ok()) {
      open_array->mtx_unlock();
      array_close_for_writes(array_uri, encryption_key, nullptr);
      return st;
    }
  }

  // This library should not be able to write to newer-versioned arrays
  // (but it is ok to write to older arrays)
  if (open_array->array_schema()->version() > constants::format_version) {
    std::stringstream err;
    err << "Cannot open array for writes; Array format version (";
    err << open_array->array_schema()->version();
    err << ") is newer than library format version (";
    err << constants::format_version << ")";
    open_array->mtx_unlock();
    array_close_for_writes(array_uri, encryption_key, nullptr);
    return LOG_STATUS(Status::StorageManagerError(err.str()));
  }

  // No fragment metadata to be loaded
  *array_schema = open_array->array_schema();

  // Unlock the array mutex
  open_array->mtx_unlock();

  return Status::Ok();

  STATS_FUNC_OUT(sm_array_open_for_writes);
}

Status StorageManager::array_reopen(
    const URI& array_uri,
    uint64_t timestamp,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema,
    std::vector<FragmentMetadata*>* fragment_metadata,
    Metadata* metadata) {
  STATS_FUNC_IN(sm_array_reopen);

  auto open_array = (OpenArray*)nullptr;

  // Lock mutex
  {
    std::lock_guard<std::mutex> lock{open_array_for_reads_mtx_};

    // Find the open array entry
    auto it = open_arrays_for_reads_.find(array_uri.to_string());
    if (it == open_arrays_for_reads_.end()) {
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot reopen array ") + array_uri.to_string() +
          "; Array not open"));
    }
    RETURN_NOT_OK(it->second->set_encryption_key(encryption_key));
    open_array = it->second;

    // Lock the array
    open_array->mtx_lock();
  }

  // Determine which fragments to load
  std::vector<TimestampedURI> fragments_to_load;
  std::vector<URI> fragment_uris;
  RETURN_NOT_OK(get_fragment_uris(array_uri, &fragment_uris));
  RETURN_NOT_OK(get_sorted_uris(fragment_uris, timestamp, &fragments_to_load));

  // Get fragment metadata in the case of reads, if not fetched already
  auto st = load_fragment_metadata(
      open_array, encryption_key, fragments_to_load, fragment_metadata);
  if (!st.ok()) {
    open_array->mtx_unlock();
    array_close_for_reads(array_uri);
    *array_schema = nullptr;
    return st;
  }

  // Get the array schema
  *array_schema = open_array->array_schema();

  // Determin which array metadata to load
  std::vector<TimestampedURI> array_metadata_to_load;
  std::vector<URI> array_metadata_uris;
  RETURN_NOT_OK(get_array_metadata_uris(array_uri, &array_metadata_uris));
  RETURN_NOT_OK(
      get_sorted_uris(array_metadata_uris, timestamp, &array_metadata_to_load));

  // Get the array metadata
  st = load_array_metadata(
      open_array, encryption_key, array_metadata_to_load, metadata);
  if (!st.ok()) {
    open_array->mtx_unlock();
    array_close_for_reads(array_uri);
    *array_schema = nullptr;
    return st;
  }

  // Unlock the mutexes
  open_array->mtx_unlock();

  return st;

  STATS_FUNC_OUT(sm_array_reopen);
}

Status StorageManager::array_consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const Config* config) {
  // Check array URI
  URI array_uri(array_name);
  if (array_uri.is_invalid()) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot consolidate array; Invalid URI"));
  }
  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(object_type(array_uri, &obj_type));

  if (obj_type != ObjectType::ARRAY) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot consolidate array; Array does not exist"));
  }

  // If 'config' is unset, use the 'config_' that was set during initialization
  // of this StorageManager instance.
  if (!config) {
    config = &config_;
  }

  // Consolidate
  Consolidator consolidator(this);
  return consolidator.consolidate(
      array_name, encryption_type, encryption_key, key_length, config);
}

Status StorageManager::array_metadata_consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const Config* config) {
  // Check array URI
  URI array_uri(array_name);
  if (array_uri.is_invalid()) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot consolidate array metadata; Invalid URI"));
  }
  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(object_type(array_uri, &obj_type));

  if (obj_type != ObjectType::ARRAY) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot consolidate array metadata; Array does not exist"));
  }

  // If 'config' is unset, use the 'config_' that was set during initialization
  // of this StorageManager instance.
  if (!config) {
    config = &config_;
  }

  // Consolidate
  Consolidator consolidator(this);
  return consolidator.consolidate_array_metadata(
      array_name, encryption_type, encryption_key, key_length, config);
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

  // Create array metadata directory
  URI array_metadata_uri =
      array_uri.join_path(constants::array_metadata_folder_name);
  RETURN_NOT_OK(vfs_->create_dir(array_metadata_uri));

  // Store array schema
  Status st = store_array_schema(array_schema, encryption_key);
  if (!st.ok()) {
    vfs_->remove_file(array_uri);
    return st;
  }

  // Create array and array metadata filelocks
  URI array_filelock_uri = array_uri.join_path(constants::filelock_name);
  st = vfs_->touch(array_filelock_uri);
  if (!st.ok()) {
    vfs_->remove_dir(array_uri);
    return st;
  }

  return Status::Ok();
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
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      array_get_non_empty_domain<int64_t>(
          metadata, dim_num, static_cast<int64_t*>(domain));
      break;
    default:
      return LOG_STATUS(Status::StorageManagerError(
          "Cannot get non-empty domain; Invalid coordinates type"));
  }

  *is_empty = false;

  // Close array
  return Status::Ok();
}

Status StorageManager::array_get_encryption(
    const std::string& array_uri, EncryptionType* encryption_type) {
  URI uri(array_uri);

  if (uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get array encryption; Invalid array URI"));

  URI schema_uri = uri.join_path(constants::array_schema_filename);

  // Read tile header.
  TileIO::GenericTileHeader header;
  RETURN_NOT_OK(TileIO::read_generic_tile_header(this, schema_uri, 0, &header));
  *encryption_type = static_cast<EncryptionType>(header.encryption_type);

  return Status::Ok();
}

Status StorageManager::array_xlock(const URI& array_uri) {
  // Get exclusive lock for threads
  xlock_mtx_.lock();

  // Wait until the array is closed for reads
  std::unique_lock<std::mutex> lk(open_array_for_reads_mtx_);
  xlock_cv_.wait(lk, [this, array_uri] {
    return open_arrays_for_reads_.find(array_uri.to_string()) ==
           open_arrays_for_reads_.end();
  });

  // Get exclusive lock for processes through a filelock
  filelock_t filelock = INVALID_FILELOCK;
  auto lock_uri = array_uri.join_path(constants::filelock_name);
  RETURN_NOT_OK_ELSE(
      vfs_->filelock_lock(lock_uri, &filelock, false), xlock_mtx_.unlock());
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

  // Release exclusive lock for processes through the filelock
  auto lock_uri = array_uri.join_path(constants::filelock_name);
  if (filelock != INVALID_FILELOCK)
    RETURN_NOT_OK(vfs_->filelock_unlock(lock_uri));
  xfilelocks_.erase(it);

  // Release exclusive lock for threads
  xlock_mtx_.unlock();

  return Status::Ok();
}

Status StorageManager::async_push_query(Query* query) {
  cancelable_tasks_.enqueue(
      &async_thread_pool_,
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
    cancelable_tasks_.cancel_all_tasks();

    // Only call VFS cancel if the object has been constructed
    if (vfs_ != nullptr)
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

const Config& StorageManager::config() const {
  return config_;
}

Status StorageManager::create_dir(const URI& uri) {
  return vfs_->create_dir(uri);
}

Status StorageManager::is_dir(const URI& uri, bool* is_dir) const {
  return vfs_->is_dir(uri, is_dir);
}

Status StorageManager::touch(const URI& uri) {
  return vfs_->touch(uri);
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

Status StorageManager::get_fragment_info(
    const ArraySchema* array_schema,
    uint64_t timestamp,
    const EncryptionKey& encryption_key,
    std::vector<FragmentInfo>* fragment_info) {
  fragment_info->clear();

  // Open array for reading
  auto array_uri = array_schema->array_uri();
  auto array_schema_tmp = (ArraySchema*)nullptr;
  std::vector<FragmentMetadata*> fragment_metadata;
  RETURN_NOT_OK(array_open_for_reads(
      array_uri,
      timestamp,
      encryption_key,
      &array_schema_tmp,
      &fragment_metadata));

  // Return if array is empty
  if (fragment_metadata.empty())
    return array_close_for_reads(array_uri);

  uint64_t domain_size = 2 * array_schema->coords_size();
  for (auto meta : fragment_metadata) {
    const auto& uri = meta->fragment_uri();
    bool sparse = !meta->dense();

    std::vector<uint8_t> non_empty_domain;
    non_empty_domain.resize(domain_size);

    // Get fragment non-empty domain
    std::memcpy(&non_empty_domain[0], meta->non_empty_domain(), domain_size);

    // Get fragment size
    uint64_t size;
    RETURN_NOT_OK_ELSE(
        meta->fragment_size(&size), array_close_for_reads(array_uri));

    // Compute expanded non-empty domain only for dense fragments
    std::vector<uint8_t> expanded_non_empty_domain;
    expanded_non_empty_domain.resize(domain_size);
    std::memcpy(
        &expanded_non_empty_domain[0], meta->non_empty_domain(), domain_size);
    if (!sparse)
      array_schema->domain()->expand_domain(
          (void*)&expanded_non_empty_domain[0]);

    // Push new fragment info
    fragment_info->push_back(FragmentInfo(
        uri,
        sparse,
        meta->timestamp_range(),
        size,
        non_empty_domain,
        expanded_non_empty_domain));
  }

  // Close array
  return array_close_for_reads(array_uri);
}

const std::unordered_map<std::string, std::string>& StorageManager::tags()
    const {
  return tags_;
}

Status StorageManager::get_fragment_info(
    const ArraySchema* array_schema,
    const EncryptionKey& encryption_key,
    const URI& fragment_uri,
    FragmentInfo* fragment_info) {
  // Get fragment name
  std::string uri_str = fragment_uri.c_str();
  if (uri_str.back() == '/')
    uri_str.pop_back();
  std::string fragment_name = URI(uri_str).last_path_part();
  assert(utils::parse::starts_with(fragment_name, "__"));

  uint32_t fragment_name_version;
  RETURN_NOT_OK(utils::parse::get_fragment_name_version(
      fragment_name, &fragment_name_version));

  // Get timestamp range
  auto timestamp_range =
      utils::parse::get_timestamp_range(fragment_name_version, fragment_name);

  // Check if fragment is sparse
  bool sparse = false;
  if (fragment_name_version == 1) {  // This corresponds to format version <=2
    URI coords_uri =
        fragment_uri.join_path(constants::coords + constants::file_suffix);
    RETURN_NOT_OK(vfs_->is_file(coords_uri, &sparse));
  } else {
    // Do nothing. It does not matter what the `sparse` value
    // is, since the FragmentMetadata object will load the correct
    // value from the metadata file.

    // Also `sparse` is updated below after loading the metadata
  }

  // Get fragment non-empty domain
  FragmentMetadata metadata(
      this, array_schema, fragment_uri, timestamp_range, !sparse);
  RETURN_NOT_OK(metadata.load(encryption_key));

  // This is important for format version > 2
  sparse = !metadata.dense();

  // Get fragment size
  uint64_t size;
  RETURN_NOT_OK(metadata.fragment_size(&size));

  uint64_t domain_size = 2 * array_schema->coords_size();
  std::vector<uint8_t> non_empty_domain;
  non_empty_domain.resize(domain_size);
  std::memcpy(&non_empty_domain[0], metadata.non_empty_domain(), domain_size);

  // Compute expanded non-empty domain only for dense fragments
  std::vector<uint8_t> expanded_non_empty_domain;
  expanded_non_empty_domain.resize(domain_size);
  std::memcpy(
      &expanded_non_empty_domain[0], metadata.non_empty_domain(), domain_size);
  if (!sparse)
    array_schema->domain()->expand_domain((void*)&expanded_non_empty_domain[0]);

  // Set fragment info
  *fragment_info = FragmentInfo(
      fragment_uri,
      sparse,
      timestamp_range,
      size,
      non_empty_domain,
      expanded_non_empty_domain);

  return Status::Ok();
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

Status StorageManager::init(const Config* config) {
  if (config != nullptr)
    config_ = *config;

  // Get config params
  bool found = false;
  uint64_t num_async_threads = 0;
  RETURN_NOT_OK(config_.get<uint64_t>(
      "sm.num_async_threads", &num_async_threads, &found));
  assert(found);
  uint64_t num_reader_threads = 0;
  RETURN_NOT_OK(config_.get<uint64_t>(
      "sm.num_reader_threads", &num_reader_threads, &found));
  assert(found);
  uint64_t num_writer_threads = 0;
  RETURN_NOT_OK(config_.get<uint64_t>(
      "sm.num_writer_threads", &num_writer_threads, &found));
  assert(found);
  uint64_t tile_cache_size = 0;
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.tile_cache_size", &tile_cache_size, &found));
  assert(found);

  RETURN_NOT_OK(async_thread_pool_.init(num_async_threads));
  RETURN_NOT_OK(reader_thread_pool_.init(num_reader_threads));
  RETURN_NOT_OK(writer_thread_pool_.init(num_writer_threads));
  tile_cache_ = new LRUCache(tile_cache_size);

  // GlobalState must be initialized before `vfs->init` because S3::init calls
  // GetGlobalState
  auto& global_state = global_state::GlobalState::GetGlobalState();
  RETURN_NOT_OK(global_state.init(config));

  vfs_ = new VFS();
  RETURN_NOT_OK(vfs_->init(&config_, nullptr));
#ifdef TILEDB_SERIALIZATION
  RETURN_NOT_OK(init_rest_client());
#endif

  RETURN_NOT_OK(set_default_tags());

  global_state.register_storage_manager(this);

  STATS_COUNTER_ADD(sm_contexts_created, 1);

  return Status::Ok();
}

RestClient* StorageManager::rest_client() const {
  return rest_client_.get();
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

Status StorageManager::load_array_schema(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema) {
  if (array_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load array schema; Invalid array URI"));

  URI schema_uri = array_uri.join_path(constants::array_schema_filename);

  TileIO tile_io(this, schema_uri);
  Tile* tile = nullptr;
  RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key));

  auto chunked_buffer = tile->chunked_buffer();
  Buffer buff;
  buff.realloc(chunked_buffer->size());
  buff.set_size(chunked_buffer->size());
  RETURN_NOT_OK_ELSE(
      chunked_buffer->read(buff.data(), buff.size(), 0), delete tile);
  delete tile;

  // Deserialize
  ConstBuffer cbuff(&buff);
  *array_schema = new ArraySchema();
  (*array_schema)->set_array_uri(array_uri);
  Status st = (*array_schema)->deserialize(&cbuff);
  if (!st.ok()) {
    delete *array_schema;
    *array_schema = nullptr;
  }

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
    RETURN_NOT_OK(object_type(uri, &obj_type));
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
  RETURN_NOT_OK(tile_cache_->read(key.str(), buffer, 0, nbytes, in_cache));
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

Status StorageManager::read(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  RETURN_NOT_OK(vfs_->read(uri, offset, buffer, nbytes));
  return Status::Ok();
}

ThreadPool* StorageManager::reader_thread_pool() {
  return &reader_thread_pool_;
}

Status StorageManager::set_tag(
    const std::string& key, const std::string& value) {
  tags_[key] = value;

  // Tags are added to REST requests as HTTP headers.
  if (rest_client_ != nullptr)
    RETURN_NOT_OK(rest_client_->set_header(key, value));

  return Status::Ok();
}

Status StorageManager::store_array_schema(
    ArraySchema* array_schema, const EncryptionKey& encryption_key) {
  auto& array_uri = array_schema->array_uri();
  URI schema_uri = array_uri.join_path(constants::array_schema_filename);

  // Serialize
  Buffer buff;
  RETURN_NOT_OK(array_schema->serialize(&buff));

  // Delete file if it exists already
  bool exists;
  RETURN_NOT_OK(is_file(schema_uri, &exists));
  if (exists)
    RETURN_NOT_OK(vfs_->remove_file(schema_uri));

  ChunkedBuffer chunked_buffer;
  RETURN_NOT_OK(Tile::buffer_to_contigious_fixed_chunks(
      buff, 0, constants::generic_tile_cell_size, &chunked_buffer));
  buff.disown_data();

  // Write to file
  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      &chunked_buffer,
      false);
  TileIO tile_io(this, schema_uri);
  uint64_t nbytes;
  Status st = tile_io.write_generic(&tile, encryption_key, &nbytes);
  (void)nbytes;
  if (st.ok())
    st = close_file(schema_uri);

  chunked_buffer.free();

  return st;
}

Status StorageManager::store_array_metadata(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    Metadata* array_metadata) {
  // Trivial case
  if (array_metadata == nullptr)
    return Status::Ok();

  // Serialize array metadata
  Buffer metadata_buff;
  RETURN_NOT_OK(array_metadata->serialize(&metadata_buff));

  // Do nothing if there are no metadata to write
  if (metadata_buff.size() == 0)
    return Status::Ok();

  // Create a metadata file name
  URI array_metadata_uri;
  RETURN_NOT_OK(new_array_metadata_uri(
      array_uri, array_metadata->timestamp_range(), &array_metadata_uri));

  ChunkedBuffer* const chunked_buffer = new ChunkedBuffer();
  RETURN_NOT_OK_ELSE(
      Tile::buffer_to_contigious_fixed_chunks(
          metadata_buff, 0, constants::generic_tile_cell_size, chunked_buffer),
      delete chunked_buffer);
  metadata_buff.disown_data();

  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      chunked_buffer,
      true);

  TileIO tile_io(this, array_metadata_uri);
  uint64_t nbytes;
  Status st = tile_io.write_generic(&tile, encryption_key, &nbytes);
  (void)nbytes;
  if (st.ok()) {
    st = close_file(array_metadata_uri);
  }

  return st;
}

Status StorageManager::close_file(const URI& uri) {
  return vfs_->close_file(uri);
}

Status StorageManager::sync(const URI& uri) {
  return vfs_->sync(uri);
}

ThreadPool* StorageManager::writer_thread_pool() {
  return &writer_thread_pool_;
}

VFS* StorageManager::vfs() const {
  return vfs_;
}

void StorageManager::wait_for_zero_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_cv_.wait(
      lck, [this]() { return queries_in_progress_ == 0; });
}

Status StorageManager::init_rest_client() {
  const char* server_address;
  RETURN_NOT_OK(config_.get("rest.server_address", &server_address));
  if (server_address != nullptr) {
    rest_client_.reset(new RestClient);
    RETURN_NOT_OK(rest_client_->init(&config_));
  }

  return Status::Ok();
}

Status StorageManager::write_to_cache(
    const URI& uri, uint64_t offset, ChunkedBuffer* chunked_buffer) const {
  STATS_FUNC_IN(sm_write_to_cache);

  // Do nothing if the object size is larger than the cache size
  const uint64_t object_size = chunked_buffer->size();
  if (object_size > tile_cache_->max_size())
    return Status::Ok();

  // Do not write metadata to cache
  std::string filename = uri.last_path_part();
  if (filename == constants::fragment_metadata_filename ||
      filename == constants::array_schema_filename) {
    return Status::Ok();
  }

  // Generate key (uri + offset)
  std::stringstream key;
  key << uri.to_string() << "+" << offset;

  // Insert to cache
  void* const object = std::malloc(object_size);
  if (object == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot write to cache; Object memory allocation failed"));
  RETURN_NOT_OK(chunked_buffer->read(object, object_size, 0));
  RETURN_NOT_OK(tile_cache_->insert(key.str(), object, object_size, false));

  return Status::Ok();

  STATS_FUNC_OUT(sm_write_to_cache);
}

Status StorageManager::write(const URI& uri, Buffer* buffer) const {
  return vfs_->write(uri, buffer->data(), buffer->size());
}

Status StorageManager::write(const URI& uri, void* data, uint64_t size) const {
  return vfs_->write(uri, data, size);
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

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

Status StorageManager::array_open_without_fragments(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    OpenArray** open_array) {
  if (!vfs_->supports_uri_scheme(array_uri))
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot open array; URI scheme unsupported."));

  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(this->object_type(array_uri, &obj_type));
  if (obj_type != ObjectType::ARRAY) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot open array; Array does not exist"));
  }

  // Lock mutexes
  {
    std::lock_guard<std::mutex> lock{open_array_for_reads_mtx_};
    std::lock_guard<std::mutex> xlock{xlock_mtx_};

    // Find the open array entry and check encryption key
    auto it = open_arrays_for_reads_.find(array_uri.to_string());
    if (it != open_arrays_for_reads_.end()) {
      RETURN_NOT_OK(it->second->set_encryption_key(encryption_key));
      *open_array = it->second;
    } else {  // Create a new entry
      *open_array = new OpenArray(array_uri, QueryType::READ);
      RETURN_NOT_OK_ELSE(
          (*open_array)->set_encryption_key(encryption_key),
          delete *open_array);
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
    array_close_for_reads(array_uri);
    return st;
  }

  // Load array schema if not fetched already
  if ((*open_array)->array_schema() == nullptr) {
    auto st = load_array_schema(array_uri, *open_array, encryption_key);
    if (!st.ok()) {
      (*open_array)->mtx_unlock();
      array_close_for_reads(array_uri);
      return st;
    }
  }

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

    RETURN_NOT_OK(is_fragment(uri, &exists));
    if (exists)
      fragment_uris->push_back(uri);
  }

  return Status::Ok();
}

Status StorageManager::get_array_metadata_uris(
    const URI& array_uri, std::vector<URI>* array_metadata_uris) const {
  // Get all uris in the array metadata directory
  URI metadata_dir = array_uri.join_path(constants::array_metadata_folder_name);
  std::vector<URI> uris;

  bool is_dir;
  RETURN_NOT_OK(vfs_->is_dir(metadata_dir, &is_dir));
  if (!is_dir)
    return Status::Ok();

  RETURN_NOT_OK(vfs_->ls(metadata_dir.add_trailing_slash(), &uris));

  // Get only the metadata uris
  for (auto& uri : uris) {
    auto uri_last_path = uri.last_path_part();
    if (utils::parse::starts_with(uri_last_path, "."))
      continue;

    if (utils::parse::starts_with(uri_last_path, "__"))
      array_metadata_uris->push_back(uri);
  }

  return Status::Ok();
}

Status StorageManager::load_array_schema(
    const URI& array_uri,
    OpenArray* open_array,
    const EncryptionKey& encryption_key) {
  // Do nothing if the array schema is already loaded
  if (open_array->array_schema() != nullptr)
    return Status::Ok();

  auto array_schema = (ArraySchema*)nullptr;
  RETURN_NOT_OK(load_array_schema(array_uri, encryption_key, &array_schema));
  open_array->set_array_schema(array_schema);

  return Status::Ok();
}

Status StorageManager::load_array_metadata(
    OpenArray* open_array,
    const EncryptionKey& encryption_key,
    const std::vector<TimestampedURI>& array_metadata_to_load,
    Metadata* metadata) {
  // Special case
  if (metadata == nullptr)
    return Status::Ok();

  auto metadata_num = array_metadata_to_load.size();
  std::vector<std::shared_ptr<ConstBuffer>> metadata_buffs;
  metadata_buffs.resize(metadata_num);
  auto statuses = parallel_for(0, metadata_num, [&](size_t m) {
    const auto& uri = array_metadata_to_load[m].uri_;
    auto metadata_buff = open_array->array_metadata(uri);
    if (metadata_buff == nullptr) {  // Array metadata does not exist - load it
      TileIO tile_io(this, uri);
      auto tile = (Tile*)nullptr;
      RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key));

      auto chunked_buffer = tile->chunked_buffer();
      Buffer* const buff = new Buffer();
      RETURN_NOT_OK(buff->realloc(chunked_buffer->size()));
      buff->set_size(chunked_buffer->size());
      RETURN_NOT_OK_ELSE(
          chunked_buffer->read(buff->data(), buff->size(), 0), [&]() {
            delete tile;
            delete buff;
          }());
      delete tile;

      metadata_buff = std::make_shared<ConstBuffer>(buff);
      open_array->insert_array_metadata(uri, metadata_buff);
    }
    metadata_buffs[m] = metadata_buff;
    return Status::Ok();
  });
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Deserialize metadata buffers
  metadata->deserialize(metadata_buffs);

  // Sets the loaded metadata URIs
  metadata->set_loaded_metadata_uris(array_metadata_to_load);

  return Status::Ok();
}

Status StorageManager::load_fragment_metadata(
    OpenArray* open_array,
    const EncryptionKey& encryption_key,
    const std::vector<TimestampedURI>& fragments_to_load,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  // Load the metadata for each fragment, only if they are not already loaded
  auto fragment_num = fragments_to_load.size();
  fragment_metadata->resize(fragment_num);
  uint32_t f_version;
  auto statuses = parallel_for(0, fragment_num, [&](size_t f) {
    const auto& sf = fragments_to_load[f];
    auto array_schema = open_array->array_schema();
    auto metadata = open_array->fragment_metadata(sf.uri_);
    if (metadata == nullptr) {  // Fragment metadata does not exist - load it
      URI coords_uri =
          sf.uri_.join_path(constants::coords + constants::file_suffix);

      RETURN_NOT_OK(utils::parse::get_fragment_name_version(
          sf.uri_.to_string(), &f_version));

      // Note that the fragment metadata version is >= the array schema
      // version. Therefore, the check below is defensive and will always
      // ensure backwards compatibility.
      if (f_version == 1) {  // This is equivalent to format version <=2
        bool sparse;
        RETURN_NOT_OK(vfs_->is_file(coords_uri, &sparse));
        metadata = new FragmentMetadata(
            this, array_schema, sf.uri_, sf.timestamp_range_, !sparse);
      } else {  // Format version > 2
        metadata = new FragmentMetadata(
            this, array_schema, sf.uri_, sf.timestamp_range_);
      }

      RETURN_NOT_OK_ELSE(metadata->load(encryption_key), delete metadata);
      open_array->insert_fragment_metadata(metadata);
    }
    (*fragment_metadata)[f] = metadata;
    return Status::Ok();
  });
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  STATS_COUNTER_ADD(fragment_metadata_num_fragments, fragment_num);

  return Status::Ok();
}

Status StorageManager::get_sorted_uris(
    const std::vector<URI>& uris,
    uint64_t timestamp,
    std::vector<TimestampedURI>* sorted_uris) const {
  // Do nothing if there are not enough URIs
  if (uris.empty())
    return Status::Ok();

  // Get the timestamp for each URI
  uint32_t f_version;
  for (auto& uri : uris) {
    // Get fragment name
    std::string uri_str = uri.c_str();
    if (uri_str.back() == '/')
      uri_str.pop_back();
    std::string name = URI(uri_str).last_path_part();
    assert(utils::parse::starts_with(name, "__"));

    // Get timestamp range
    RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &f_version));
    auto timestamp_range = utils::parse::get_timestamp_range(f_version, name);
    auto t = timestamp_range.first;

    if (t <= timestamp)
      sorted_uris->emplace_back(uri, timestamp_range);
  }

  // Sort the names based on the timestamps
  std::sort(sorted_uris->begin(), sorted_uris->end());

  // Remove consolidated fragment URIs
  Consolidator::remove_consolidated_uris(sorted_uris);

  return Status::Ok();
}

Status StorageManager::new_array_metadata_uri(
    const URI& array_uri,
    const std::pair<uint64_t, uint64_t>& timestamp_range,
    URI* new_uri) const {
  std::string uuid;
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));

  std::stringstream ss;
  ss << "/__" << timestamp_range.first << "_" << timestamp_range.second << "_"
     << uuid;
  *new_uri = array_uri.join_path(constants::array_metadata_folder_name)
                 .join_path(ss.str());

  return Status::Ok();
}

Status StorageManager::set_default_tags() {
  const auto version = std::to_string(constants::library_version[0]) + "." +
                       std::to_string(constants::library_version[1]) + "." +
                       std::to_string(constants::library_version[2]);

  RETURN_NOT_OK(set_tag("x-tiledb-version", version));
  RETURN_NOT_OK(set_tag("x-tiledb-api-language", "c"));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
