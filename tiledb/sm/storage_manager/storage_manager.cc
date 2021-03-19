/**
 * @file   storage_manager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/cache/buffer_lru_cache.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_info.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/storage_manager/consolidator.h"
#include "tiledb/sm/storage_manager/open_array.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"

#include <algorithm>
#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManager::StorageManager(
    ThreadPool* const compute_tp, ThreadPool* const io_tp)
    : cancellation_in_progress_(false)
    , queries_in_progress_(0)
    , compute_tp_(compute_tp)
    , io_tp_(io_tp)
    , vfs_(nullptr) {
}

StorageManager::~StorageManager() {
  global_state::GlobalState::GetGlobalState().unregister_storage_manager(this);

  if (vfs_ != nullptr)
    cancel_all_tasks();

  // Release all filelocks and delete all opened arrays for reads
  for (auto& open_array_it : open_arrays_for_reads_) {
    open_array_it.second->file_unlock(vfs_);
    tdb_delete(open_array_it.second);
  }

  // Delete all opened arrays for writes
  for (auto& open_array_it : open_arrays_for_writes_)
    tdb_delete(open_array_it.second);

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

    tdb_delete(vfs_);
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

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
    tdb_delete(open_array);
    open_arrays_for_reads_.erase(it);
  } else {  // Just unlock the array mutex
    open_array->mtx_unlock();
  }

  xlock_cv_.notify_all();

  return Status::Ok();
}

Status StorageManager::array_close_for_writes(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    Metadata* array_metadata) {
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
    tdb_delete(open_array);
    open_arrays_for_writes_.erase(it);
  } else {  // Just unlock the array mutex
    open_array->mtx_unlock();
  }

  return Status::Ok();
}

Status StorageManager::array_open_for_reads(
    const URI& array_uri,
    uint64_t timestamp,
    const EncryptionKey& enc_key,
    ArraySchema** array_schema,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_ARRAY_OPEN)

  /* NOTE: these variables may be modified on a different thread
           in the load_array_fragments_task below.
  */
  std::vector<TimestampedURI> fragments_to_load;
  std::vector<URI> fragment_uris;
  URI meta_uri;
  Buffer f_buff;
  std::unordered_map<std::string, uint64_t> offsets;

  // Fetch array fragments async
  std::vector<ThreadPool::Task> load_array_fragments_task;
  load_array_fragments_task.emplace_back(io_tp_->execute([array_uri,
                                                          &enc_key,
                                                          &f_buff,
                                                          &fragments_to_load,
                                                          &fragment_uris,
                                                          &meta_uri,
                                                          &offsets,
                                                          &timestamp,
                                                          this]() {
    // Determine which fragments to load
    RETURN_NOT_OK(get_fragment_uris(array_uri, &fragment_uris, &meta_uri));
    RETURN_NOT_OK(
        get_sorted_uris(fragment_uris, timestamp, &fragments_to_load));
    // Get the consolidated fragment metadata
    RETURN_NOT_OK(
        load_consolidated_fragment_meta(meta_uri, enc_key, &f_buff, &offsets));
    return Status::Ok();
  }));

  auto open_array = (OpenArray*)nullptr;
  Status st = array_open_without_fragments(array_uri, enc_key, &open_array);

  if (!st.ok()) {
    io_tp_->wait_all(load_array_fragments_task);
    *array_schema = nullptr;
    return st;
  }

  // Wait for array fragments to be loaded
  st = io_tp_->wait_all(load_array_fragments_task);

  if (!st.ok()) {
    open_array->mtx_unlock();
    array_close_for_reads(array_uri);
    *array_schema = nullptr;
    return st;
  }

  // Retrieve array schema
  *array_schema = open_array->array_schema();

  // Get fragment metadata in the case of reads, if not fetched already
  st = load_fragment_metadata(
      open_array,
      enc_key,
      fragments_to_load,
      &f_buff,
      offsets,
      fragment_metadata);

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

  STATS_END_TIMER(stats::Stats::TimerType::READ_ARRAY_OPEN)
}

Status StorageManager::array_open_for_reads(
    const URI& array_uri,
    const FragmentInfo& fragment_info,
    const EncryptionKey& enc_key,
    ArraySchema** array_schema,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_ARRAY_OPEN)

  // Determine which fragments to load
  std::vector<TimestampedURI> fragments_to_load;
  const auto& fragments = fragment_info.fragments();
  for (const auto& fragment : fragments)
    fragments_to_load.emplace_back(fragment.uri(), fragment.timestamp_range());

  /* NOTE: these variables may be modified on a different thread
           in the load_array_fragments_task below.
  */
  URI meta_uri;
  std::vector<URI> uris;
  Buffer f_buff;
  std::unordered_map<std::string, uint64_t> offsets;

  std::vector<ThreadPool::Task> load_array_fragments_task;
  load_array_fragments_task.emplace_back(io_tp_->execute(
      [array_uri, &enc_key, &f_buff, &meta_uri, &offsets, &uris, this]() {
        RETURN_NOT_OK(this->vfs_->ls(array_uri.add_trailing_slash(), &uris));
        // Get the consolidated fragment metadata URI
        RETURN_NOT_OK(get_consolidated_fragment_meta_uri(uris, &meta_uri));
        // Get the consolidated fragment metadata
        RETURN_NOT_OK(load_consolidated_fragment_meta(
            meta_uri, enc_key, &f_buff, &offsets));
        return Status::Ok();
      }));

  auto open_array = (OpenArray*)nullptr;
  Status st = array_open_without_fragments(array_uri, enc_key, &open_array);
  if (!st.ok()) {
    io_tp_->wait_all(load_array_fragments_task);
    *array_schema = nullptr;
    return st;
  }

  // Wait for array schema to be loaded
  st = io_tp_->wait_all(load_array_fragments_task);
  if (!st.ok()) {
    open_array->mtx_unlock();
    array_close_for_reads(array_uri);
    *array_schema = nullptr;
    return st;
  }

  // Assign array schema
  *array_schema = open_array->array_schema();

  // Get fragment metadata in the case of reads, if not fetched already
  st = load_fragment_metadata(
      open_array,
      enc_key,
      fragments_to_load,
      &f_buff,
      offsets,
      fragment_metadata);

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

  STATS_END_TIMER(stats::Stats::TimerType::READ_ARRAY_OPEN)
}

Status StorageManager::array_open_for_writes(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema) {
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
      open_array = tdb_new(OpenArray, array_uri, QueryType::WRITE);
      RETURN_NOT_OK_ELSE(
          open_array->set_encryption_key(encryption_key),
          tdb_delete(open_array));
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
}

Status StorageManager::array_reopen(
    const URI& array_uri,
    uint64_t timestamp,
    const EncryptionKey& enc_key,
    ArraySchema** array_schema,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_ARRAY_OPEN)

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
    RETURN_NOT_OK(it->second->set_encryption_key(enc_key));
    open_array = it->second;

    // Lock the array
    open_array->mtx_lock();
  }

  // Determine which fragments to load
  std::vector<TimestampedURI> fragments_to_load;
  std::vector<URI> fragment_uris;
  URI meta_uri;
  RETURN_NOT_OK(get_fragment_uris(array_uri, &fragment_uris, &meta_uri));
  RETURN_NOT_OK(get_sorted_uris(fragment_uris, timestamp, &fragments_to_load));

  // Get the consolidated fragment metadata
  Buffer f_buff;
  std::unordered_map<std::string, uint64_t> offsets;
  RETURN_NOT_OK(
      load_consolidated_fragment_meta(meta_uri, enc_key, &f_buff, &offsets));

  // Get fragment metadata in the case of reads, if not fetched already
  auto st = load_fragment_metadata(
      open_array,
      enc_key,
      fragments_to_load,
      &f_buff,
      offsets,
      fragment_metadata);
  if (!st.ok()) {
    open_array->mtx_unlock();
    array_close_for_reads(array_uri);
    *array_schema = nullptr;
    return st;
  }

  // Get the array schema
  *array_schema = open_array->array_schema();

  // Unlock the mutexes
  open_array->mtx_unlock();

  return st;

  STATS_END_TIMER(stats::Stats::TimerType::READ_ARRAY_OPEN)
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

Status StorageManager::array_vacuum(
    const char* array_name, const Config* config) {
  // If 'config' is unset, use the 'config_' that was set during initialization
  // of this StorageManager instance.
  if (!config) {
    config = &config_;
  }

  // Get mode
  const char* mode;
  RETURN_NOT_OK(config->get("sm.vacuum.mode", &mode));

  if (mode == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot vacuum array; Vacuum mode cannot be null"));
  else if (std::string(mode) == "fragments")
    RETURN_NOT_OK(array_vacuum_fragments(array_name));
  else if (std::string(mode) == "fragment_meta")
    RETURN_NOT_OK(array_vacuum_fragment_meta(array_name));
  else if (std::string(mode) == "array_meta")
    RETURN_NOT_OK(array_vacuum_array_meta(array_name));
  else
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot vacuum array; Invalid vacuum mode"));

  return Status::Ok();
}

Status StorageManager::array_vacuum_fragments(const char* array_name) {
  if (array_name == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot vacuum fragments; Array name cannot be null"));

  // Get all URIs in the array directory
  URI array_uri(array_name);
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(array_uri.add_trailing_slash(), &uris));

  // Get URIs to be vacuumed
  std::vector<URI> to_vacuum, vac_uris;
  auto timestamp = utils::time::timestamp_now_ms();
  RETURN_NOT_OK(get_uris_to_vacuum(uris, timestamp, &to_vacuum, &vac_uris));

  // Delete the ok files
  RETURN_NOT_OK(array_xlock(array_uri));
  auto statuses =
      parallel_for(compute_tp_, 0, to_vacuum.size(), [&, this](size_t i) {
        auto uri = URI(to_vacuum[i].to_string() + constants::ok_file_suffix);
        RETURN_NOT_OK(vfs_->remove_file(uri));

        return Status::Ok();
      });
  for (const auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, array_xunlock(array_uri));
  RETURN_NOT_OK(array_xunlock(array_uri));

  // Delete fragment directories
  statuses =
      parallel_for(compute_tp_, 0, to_vacuum.size(), [&, this](size_t i) {
        RETURN_NOT_OK(vfs_->remove_dir(to_vacuum[i]));

        return Status::Ok();
      });
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  // Delete vacuum files
  statuses = parallel_for(compute_tp_, 0, vac_uris.size(), [&, this](size_t i) {
    RETURN_NOT_OK(vfs_->remove_file(vac_uris[i]));
    return Status::Ok();
  });
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status StorageManager::array_vacuum_fragment_meta(const char* array_name) {
  if (array_name == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot vacuum fragment metadata; Array name cannot be null"));

  // Get the consolidated fragment metadata URIs to be deleted
  // (all except the last one)
  URI array_uri(array_name);
  std::vector<URI> uris, to_vacuum;
  URI last;
  RETURN_NOT_OK(vfs_->ls(array_uri.add_trailing_slash(), &uris));
  RETURN_NOT_OK(get_consolidated_fragment_meta_uri(uris, &last));
  to_vacuum.reserve(uris.size());
  for (const auto& uri : uris) {
    if (utils::parse::ends_with(uri.to_string(), constants::meta_file_suffix) &&
        uri != last)
      to_vacuum.emplace_back(uri);
  }

  // Vacuum after exclusively locking the array
  RETURN_NOT_OK(array_xlock(array_uri));
  auto statuses =
      parallel_for(compute_tp_, 0, to_vacuum.size(), [&, this](size_t i) {
        RETURN_NOT_OK(vfs_->remove_file(to_vacuum[i]));
        return Status::Ok();
      });
  for (const auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, array_xunlock(array_uri));
  RETURN_NOT_OK(array_xunlock(array_uri));

  return Status::Ok();
}

Status StorageManager::array_vacuum_array_meta(const char* array_name) {
  if (array_name == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot vacuum array metadata; Array name cannot be null"));

  // Get all URIs in the array directory
  URI array_uri(array_name);
  auto meta_uri = array_uri.join_path(constants::array_metadata_folder_name);
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(meta_uri.add_trailing_slash(), &uris));

  // Get URIs to be vacuumed
  std::vector<URI> to_vacuum, vac_uris;
  auto timestamp = utils::time::timestamp_now_ms();
  RETURN_NOT_OK(get_uris_to_vacuum(uris, timestamp, &to_vacuum, &vac_uris));

  // Delete the array metadata files
  RETURN_NOT_OK(array_xlock(array_uri));
  auto statuses =
      parallel_for(compute_tp_, 0, to_vacuum.size(), [&, this](size_t i) {
        RETURN_NOT_OK(vfs_->remove_file(to_vacuum[i]));

        return Status::Ok();
      });
  for (const auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, array_xunlock(array_uri));
  RETURN_NOT_OK(array_xunlock(array_uri));

  // Delete vacuum files
  statuses = parallel_for(compute_tp_, 0, vac_uris.size(), [&, this](size_t i) {
    RETURN_NOT_OK(vfs_->remove_file(vac_uris[i]));
    return Status::Ok();
  });
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
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
  return consolidator.consolidate_array_meta(
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

  // Create array metadata directory
  URI array_metadata_uri =
      array_uri.join_path(constants::array_metadata_folder_name);
  RETURN_NOT_OK(vfs_->create_dir(array_metadata_uri));

  // Store array schema
  Status st = store_array_schema(array_schema, encryption_key);
  if (!st.ok()) {
    vfs_->remove_dir(array_uri);
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
    Array* array, NDRange* domain, bool* is_empty) {
  if (domain == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Domain object is null"));

  if (array == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Array object is null"));

  if (!array->is_remote() &&
      open_arrays_for_reads_.find(array->array_uri().to_string()) ==
          open_arrays_for_reads_.end())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Array not opened for reads"));

  *domain = array->non_empty_domain();
  *is_empty = domain->empty();

  return Status::Ok();
}

Status StorageManager::array_get_non_empty_domain(
    Array* array, void* domain, bool* is_empty) {
  if (array == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Array object is null"));

  if (!array->array_schema()->domain()->all_dims_same_type())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Function non-applicable to arrays with "
        "heterogenous dimensions"));

  if (!array->array_schema()->domain()->all_dims_fixed())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Function non-applicable to arrays with "
        "variable-sized dimensions"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));
  if (*is_empty)
    return Status::Ok();

  auto array_schema = array->array_schema();
  auto dim_num = array_schema->dim_num();
  auto domain_c = (unsigned char*)domain;
  uint64_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    std::memcpy(&domain_c[offset], dom[d].data(), dom[d].size());
    offset += dom[d].size();
  }

  return Status::Ok();
}

Status StorageManager::array_get_non_empty_domain_from_index(
    Array* array, unsigned idx, void* domain, bool* is_empty) {
  // For easy reference
  auto array_schema = array->array_schema();
  auto array_domain = array_schema->domain();

  // Sanity checks
  if (idx >= array_schema->dim_num())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (array_domain->dimension(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain->dimension(idx)->name();
    errmsg += "' is variable-sized";
    return LOG_STATUS(Status::StorageManagerError(errmsg));
  }

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));
  if (*is_empty)
    return Status::Ok();

  std::memcpy(domain, dom[idx].data(), dom[idx].size());
  return Status::Ok();
}

Status StorageManager::array_get_non_empty_domain_from_name(
    Array* array, const char* name, void* domain, bool* is_empty) {
  // Sanity check
  if (name == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension name"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  auto array_schema = array->array_schema();
  auto array_domain = array_schema->domain();
  auto dim_num = array_schema->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_name = array_schema->dimension(d)->name();
    if (name == dim_name) {
      // Sanity check
      if (array_domain->dimension(d)->var_size()) {
        std::string errmsg = "Cannot get non-empty domain; Dimension '";
        errmsg += dim_name + "' is variable-sized";
        return LOG_STATUS(Status::StorageManagerError(errmsg));
      }

      if (!*is_empty)
        std::memcpy(domain, dom[d].data(), dom[d].size());
      return Status::Ok();
    }
  }

  return LOG_STATUS(Status::StorageManagerError(
      std::string("Cannot get non-empty domain; Dimension name '") + name +
      "' does not exist"));
}

Status StorageManager::array_get_non_empty_domain_var_size_from_index(
    Array* array,
    unsigned idx,
    uint64_t* start_size,
    uint64_t* end_size,
    bool* is_empty) {
  // For easy reference
  auto array_schema = array->array_schema();
  auto array_domain = array_schema->domain();

  // Sanity checks
  if (idx >= array_schema->dim_num())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (!array_domain->dimension(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain->dimension(idx)->name();
    errmsg += "' is fixed-sized";
    return LOG_STATUS(Status::StorageManagerError(errmsg));
  }

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));
  if (*is_empty) {
    *start_size = 0;
    *end_size = 0;
    return Status::Ok();
  }

  *start_size = dom[idx].start_size();
  *end_size = dom[idx].end_size();

  return Status::Ok();
}

Status StorageManager::array_get_non_empty_domain_var_size_from_name(
    Array* array,
    const char* name,
    uint64_t* start_size,
    uint64_t* end_size,
    bool* is_empty) {
  // Sanity check
  if (name == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension name"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  auto array_schema = array->array_schema();
  auto array_domain = array_schema->domain();
  auto dim_num = array_schema->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_name = array_schema->dimension(d)->name();
    if (name == dim_name) {
      // Sanity check
      if (!array_domain->dimension(d)->var_size()) {
        std::string errmsg = "Cannot get non-empty domain; Dimension '";
        errmsg += dim_name + "' is fixed-sized";
        return LOG_STATUS(Status::StorageManagerError(errmsg));
      }

      if (*is_empty) {
        *start_size = 0;
        *end_size = 0;
      } else {
        *start_size = dom[d].start_size();
        *end_size = dom[d].end_size();
      }

      return Status::Ok();
    }
  }

  return LOG_STATUS(Status::StorageManagerError(
      std::string("Cannot get non-empty domain; Dimension name '") + name +
      "' does not exist"));
}

Status StorageManager::array_get_non_empty_domain_var_from_index(
    Array* array, unsigned idx, void* start, void* end, bool* is_empty) {
  // For easy reference
  auto array_schema = array->array_schema();
  auto array_domain = array_schema->domain();

  // Sanity checks
  if (idx >= array_schema->dim_num())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (!array_domain->dimension(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain->dimension(idx)->name();
    errmsg += "' is fixed-sized";
    return LOG_STATUS(Status::StorageManagerError(errmsg));
  }

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  if (*is_empty)
    return Status::Ok();

  std::memcpy(start, dom[idx].start(), dom[idx].start_size());
  std::memcpy(end, dom[idx].end(), dom[idx].end_size());

  return Status::Ok();
}

Status StorageManager::array_get_non_empty_domain_var_from_name(
    Array* array, const char* name, void* start, void* end, bool* is_empty) {
  // Sanity check
  if (name == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension name"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  auto array_schema = array->array_schema();
  auto array_domain = array_schema->domain();
  auto dim_num = array_schema->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_name = array_schema->dimension(d)->name();
    if (name == dim_name) {
      // Sanity check
      if (!array_domain->dimension(d)->var_size()) {
        std::string errmsg = "Cannot get non-empty domain; Dimension '";
        errmsg += dim_name + "' is fixed-sized";
        return LOG_STATUS(Status::StorageManagerError(errmsg));
      }

      if (!*is_empty) {
        std::memcpy(start, dom[d].start(), dom[d].start_size());
        std::memcpy(end, dom[d].end(), dom[d].end_size());
      }

      return Status::Ok();
    }
  }

  return LOG_STATUS(Status::StorageManagerError(
      std::string("Cannot get non-empty domain; Dimension name '") + name +
      "' does not exist"));
}

Status StorageManager::array_get_encryption(
    const std::string& array_uri, EncryptionType* encryption_type) {
  URI uri(array_uri);

  if (uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot get array encryption; Invalid array URI"));

  URI schema_uri = uri.join_path(constants::array_schema_filename);

  // Read tile header.
  GenericTileIO::GenericTileHeader header;
  RETURN_NOT_OK(
      GenericTileIO::read_generic_tile_header(this, schema_uri, 0, &header));
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
  cancelable_tasks_.execute(
      compute_tp_,
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
    const URI& array_uri,
    uint64_t timestamp,
    const EncryptionKey& encryption_key,
    FragmentInfo* fragment_info,
    bool get_to_vacuum) {
  fragment_info->clear();

  // Open array for reading
  auto array_schema = (ArraySchema*)nullptr;
  std::vector<FragmentMetadata*> fragment_metadata;
  RETURN_NOT_OK(array_open_for_reads(
      array_uri, timestamp, encryption_key, &array_schema, &fragment_metadata));

  fragment_info->set_dim_info(
      array_schema->dim_names(), array_schema->dim_types());

  // Return if array is empty
  if (fragment_metadata.empty())
    return array_close_for_reads(array_uri);

  std::vector<uint64_t> sizes(fragment_metadata.size());

  auto statuses = parallel_for(
      this->compute_tp_,
      0,
      fragment_metadata.size(),
      [&fragment_metadata, &sizes](uint64_t i) {
        const auto meta = fragment_metadata[i];

        // Get fragment size
        uint64_t size;
        RETURN_NOT_OK(meta->fragment_size(&size));
        sizes[i] = size;

        return Status::Ok();
      });

  for (const auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, array_close_for_reads(array_uri));

  for (uint64_t i = 0; i < fragment_metadata.size(); i++) {
    const auto meta = fragment_metadata[i];
    const auto& uri = meta->fragment_uri();
    bool sparse = !meta->dense();
    // Get non-empty domain, and compute expanded non-empty domain
    // (only for dense fragments)
    const auto& non_empty_domain = meta->non_empty_domain();
    auto expanded_non_empty_domain = non_empty_domain;
    if (!sparse)
      array_schema->domain()->expand_to_tiles(&expanded_non_empty_domain);

    // Push new fragment info
    fragment_info->append(SingleFragmentInfo(
        uri,
        meta->format_version(),
        sparse,
        meta->timestamp_range(),
        meta->cell_num(),
        sizes[i],
        meta->has_consolidated_footer(),
        non_empty_domain,
        expanded_non_empty_domain));
  }

  // Optionally get the URIs to vacuum
  if (get_to_vacuum) {
    std::vector<URI> to_vacuum, vac_uris, fragment_uris;
    URI meta_uri;
    RETURN_NOT_OK(get_fragment_uris(array_uri, &fragment_uris, &meta_uri));
    RETURN_NOT_OK(
        get_uris_to_vacuum(fragment_uris, timestamp, &to_vacuum, &vac_uris));
    fragment_info->set_to_vacuum(to_vacuum);
  }

  // Close array
  return array_close_for_reads(array_uri);
}

Status StorageManager::get_fragment_info(
    const ArraySchema* array_schema,
    uint64_t timestamp,
    const EncryptionKey& encryption_key,
    FragmentInfo* fragment_info,
    bool get_to_vacuum) {
  fragment_info->clear();

  // Open array for reading
  const auto& array_uri = array_schema->array_uri();
  return get_fragment_info(
      array_uri, timestamp, encryption_key, fragment_info, get_to_vacuum);
}

Status StorageManager::get_fragment_uris(
    const URI& array_uri,
    std::vector<URI>* fragment_uris,
    URI* meta_uri) const {
  STATS_START_TIMER(stats::Stats::TimerType::READ_GET_FRAGMENT_URIS)
  // Get all uris in the array directory
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(array_uri.add_trailing_slash(), &uris));

  // Get the fragments that have special "ok" URIs, which indicate
  // that fragments are "committed" for versions >= 5
  std::set<URI> ok_uris;
  for (size_t i = 0; i < uris.size(); ++i) {
    if (utils::parse::ends_with(
            uris[i].to_string(), constants::ok_file_suffix)) {
      auto name = uris[i].to_string();
      name = name.substr(0, name.size() - constants::ok_file_suffix.size());
      ok_uris.emplace(URI(name));
    }
  }

  // Get only the committed fragment uris
  std::vector<int> is_fragment(uris.size(), 0);
  auto statuses = parallel_for(compute_tp_, 0, uris.size(), [&](size_t i) {
    if (utils::parse::starts_with(uris[i].last_path_part(), "."))
      return Status::Ok();
    RETURN_NOT_OK(this->is_fragment(uris[i], ok_uris, &is_fragment[i]));
    return Status::Ok();
  });
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  for (size_t i = 0; i < uris.size(); ++i) {
    if (is_fragment[i])
      fragment_uris->emplace_back(uris[i]);
    else if (this->is_vacuum_file(uris[i]))
      fragment_uris->emplace_back(uris[i]);
  }

  // Get the latest consolidated fragment metadata URI
  RETURN_NOT_OK(get_consolidated_fragment_meta_uri(uris, meta_uri));

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_GET_FRAGMENT_URIS);
}

const std::unordered_map<std::string, std::string>& StorageManager::tags()
    const {
  return tags_;
}

Status StorageManager::get_fragment_info(
    const ArraySchema* array_schema,
    const EncryptionKey& encryption_key,
    const URI& fragment_uri,
    SingleFragmentInfo* fragment_info) {
  // Get timestamp range
  std::pair<uint64_t, uint64_t> timestamp_range;
  RETURN_NOT_OK(
      utils::parse::get_timestamp_range(fragment_uri, &timestamp_range));
  uint32_t version;
  auto name = fragment_uri.remove_trailing_slash().last_path_part();
  RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &version));

  // Check if fragment is sparse
  bool sparse = false;
  if (version == 1) {  // This corresponds to format version <=2
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
  FragmentMetadata meta(
      this, array_schema, fragment_uri, timestamp_range, !sparse);
  RETURN_NOT_OK(meta.load(encryption_key, nullptr, 0));

  // This is important for format version > 2
  sparse = !meta.dense();

  // Get fragment size
  uint64_t size;
  RETURN_NOT_OK(meta.fragment_size(&size));

  // Compute expanded non-empty domain only for dense fragments
  // Get non-empty domain, and compute expanded non-empty domain
  // (only for dense fragments)
  const auto& non_empty_domain = meta.non_empty_domain();
  auto expanded_non_empty_domain = non_empty_domain;
  if (!sparse)
    array_schema->domain()->expand_to_tiles(&expanded_non_empty_domain);

  // Set fragment info
  *fragment_info = SingleFragmentInfo(
      fragment_uri,
      meta.format_version(),
      sparse,
      timestamp_range,
      meta.cell_num(),
      size,
      meta.has_consolidated_footer(),
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

  // set logging level from config
  bool found = false;
  uint32_t level = static_cast<unsigned int>(Logger::Level::ERR);
  RETURN_NOT_OK(config_.get<uint32_t>("config.logging_level", &level, &found));
  assert(found);
  if (level > static_cast<unsigned int>(Logger::Level::TRACE)) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot set logger level; Unsupported level:" + std::to_string(level) +
        "set in configuration"));
  }

  global_logger().set_level(static_cast<Logger::Level>(level));

  // Get config params
  uint64_t tile_cache_size = 0;
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.tile_cache_size", &tile_cache_size, &found));
  assert(found);

  tile_cache_ =
      tdb_unique_ptr<BufferLRUCache>(tdb_new(BufferLRUCache, tile_cache_size));

  // GlobalState must be initialized before `vfs->init` because S3::init calls
  // GetGlobalState
  auto& global_state = global_state::GlobalState::GetGlobalState();
  RETURN_NOT_OK(global_state.init(config));

  vfs_ = tdb_new(VFS);
  RETURN_NOT_OK(vfs_->init(compute_tp_, io_tp_, &config_, nullptr));
#ifdef TILEDB_SERIALIZATION
  RETURN_NOT_OK(init_rest_client());
#endif

  RETURN_NOT_OK(set_default_tags());

  global_state.register_storage_manager(this);

  return Status::Ok();
}

ThreadPool* StorageManager::compute_tp() {
  return compute_tp_;
}

ThreadPool* StorageManager::io_tp() {
  return io_tp_;
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

Status StorageManager::is_fragment(
    const URI& uri, const std::set<URI>& ok_uris, int* is_fragment) const {
  // If the URI name has a suffix, then it is not a fragment
  auto name = uri.remove_trailing_slash().last_path_part();
  if (name.find_first_of('.') != std::string::npos) {
    *is_fragment = 0;
    return Status::Ok();
  }

  // Check set membership in ok_uris
  if (ok_uris.find(uri) != ok_uris.end()) {
    *is_fragment = 1;
    return Status::Ok();
  }

  // If the format version is >= 5, then the above suffices to check if
  // the URI is indeed a fragment
  uint32_t version;
  RETURN_NOT_OK(utils::parse::get_fragment_version(name, &version));
  if (version != UINT32_MAX && version >= 5) {
    *is_fragment = false;
    return Status::Ok();
  }

  // Versions < 5
  bool is_file;
  RETURN_NOT_OK(vfs_->is_file(
      uri.join_path(constants::fragment_metadata_filename), &is_file));
  *is_fragment = (int)is_file;
  return Status::Ok();
}

Status StorageManager::is_group(const URI& uri, bool* is_group) const {
  RETURN_NOT_OK(
      vfs_->is_file(uri.join_path(constants::group_filename), is_group));
  return Status::Ok();
}

bool StorageManager::is_vacuum_file(const URI& uri) const {
  // If the URI name has a suffix, then it is not a fragment
  if (utils::parse::ends_with(uri.to_string(), constants::vacuum_file_suffix))
    return true;

  return false;
}

Status StorageManager::load_array_schema(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_LOAD_ARRAY_SCHEMA)

  if (array_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load array schema; Invalid array URI"));

  URI schema_uri = array_uri.join_path(constants::array_schema_filename);

  GenericTileIO tile_io(this, schema_uri);
  Tile* tile = nullptr;
  RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key, config_));

  auto chunked_buffer = tile->chunked_buffer();
  Buffer buff;
  buff.realloc(chunked_buffer->size());
  buff.set_size(chunked_buffer->size());
  RETURN_NOT_OK_ELSE(
      chunked_buffer->read(buff.data(), buff.size(), 0), tdb_delete(tile));
  tdb_delete(tile);

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::READ_ARRAY_SCHEMA_SIZE, buff.size());

  // Deserialize
  ConstBuffer cbuff(&buff);
  *array_schema = tdb_new(ArraySchema);
  (*array_schema)->set_array_uri(array_uri);
  Status st = (*array_schema)->deserialize(&cbuff);
  if (!st.ok()) {
    tdb_delete(*array_schema);
    *array_schema = nullptr;
  }

  return st;

  STATS_END_TIMER(stats::Stats::TimerType::READ_LOAD_ARRAY_SCHEMA)
}

Status StorageManager::load_array_metadata(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    uint64_t timestamp,
    Metadata* metadata) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_LOAD_ARRAY_META)

  OpenArray* open_array;
  // Lock mutex
  {
    std::lock_guard<std::mutex> lock{open_array_for_reads_mtx_};

    // Find the open array entry
    auto it = open_arrays_for_reads_.find(array_uri.to_string());
    assert(it != open_arrays_for_reads_.end());
    open_array = it->second;

    // Lock the array
    open_array->mtx_lock();
  }

  // Determine which array metadata to load
  std::vector<TimestampedURI> array_metadata_to_load;
  std::vector<URI> array_metadata_uris;
  RETURN_NOT_OK_ELSE(
      get_array_metadata_uris(array_uri, &array_metadata_uris),
      open_array->mtx_unlock());
  RETURN_NOT_OK_ELSE(
      get_sorted_uris(array_metadata_uris, timestamp, &array_metadata_to_load),
      open_array->mtx_unlock());

  // Get the array metadata
  RETURN_NOT_OK_ELSE(
      load_array_metadata(
          open_array, encryption_key, array_metadata_to_load, metadata),
      open_array->mtx_unlock());

  // Unlock the array
  open_array->mtx_unlock();

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_LOAD_ARRAY_META)
}

Status StorageManager::object_type(const URI& uri, ObjectType* type) const {
  URI dir_uri = uri;
  if (uri.is_s3() || uri.is_azure() || uri.is_gcs()) {
    // Always add a trailing '/' in the S3/Azure/GCS case so that listing the
    // URI as a directory will work as expected. Listing a non-directory object
    // is not an error for S3/Azure/GCS.
    auto uri_str = uri.to_string();
    dir_uri =
        URI(utils::parse::ends_with(uri_str, "/") ? uri_str : (uri_str + "/"));
  } else {
    // For non public cloud backends, listing a non-directory is an error.
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
  *obj_iter = tdb_new(ObjectIter);
  (*obj_iter)->order_ = order;
  (*obj_iter)->recursive_ = true;

  // Include the uris that are TileDB objects in the iterator state
  ObjectType obj_type;
  for (auto& uri : uris) {
    RETURN_NOT_OK_ELSE(object_type(uri, &obj_type), tdb_delete(*obj_iter));
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
  *obj_iter = tdb_new(ObjectIter);
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
  tdb_delete(obj_iter);
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
  // Process the query
  QueryInProgress in_progress(this);
  auto st = query->process();

  return st;
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
  std::stringstream key;
  key << uri.to_string() << "+" << offset;
  RETURN_NOT_OK(tile_cache_->read(key.str(), buffer, 0, nbytes, in_cache));
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

Status StorageManager::read(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  RETURN_NOT_OK(vfs_->read(uri, offset, buffer, nbytes));
  return Status::Ok();
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

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::WRITE_ARRAY_SCHEMA_SIZE, buff.size());

  // Delete file if it exists already
  bool exists;
  RETURN_NOT_OK(is_file(schema_uri, &exists));
  if (exists)
    RETURN_NOT_OK(vfs_->remove_file(schema_uri));

  ChunkedBuffer chunked_buffer;
  RETURN_NOT_OK(Tile::buffer_to_contiguous_fixed_chunks(
      buff, 0, constants::generic_tile_cell_size, &chunked_buffer));
  buff.disown_data();

  // Write to file
  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      &chunked_buffer,
      false);
  GenericTileIO tile_io(this, schema_uri);
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
  STATS_START_TIMER(stats::Stats::TimerType::WRITE_ARRAY_META)

  // Trivial case
  if (array_metadata == nullptr)
    return Status::Ok();

  // Serialize array metadata
  Buffer metadata_buff;
  RETURN_NOT_OK(array_metadata->serialize(&metadata_buff));

  // Do nothing if there are no metadata to write
  if (metadata_buff.size() == 0)
    return Status::Ok();

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::WRITE_ARRAY_META_SIZE, metadata_buff.size());

  // Create a metadata file name
  URI array_metadata_uri;
  RETURN_NOT_OK(array_metadata->get_uri(array_uri, &array_metadata_uri));

  ChunkedBuffer* const chunked_buffer = tdb_new(ChunkedBuffer);
  RETURN_NOT_OK_ELSE(
      Tile::buffer_to_contiguous_fixed_chunks(
          metadata_buff, 0, constants::generic_tile_cell_size, chunked_buffer),
      tdb_delete(chunked_buffer));
  metadata_buff.disown_data();

  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      chunked_buffer,
      true);

  GenericTileIO tile_io(this, array_metadata_uri);
  uint64_t nbytes;
  Status st = tile_io.write_generic(&tile, encryption_key, &nbytes);
  (void)nbytes;

  if (st.ok()) {
    st = close_file(array_metadata_uri);
  }

  return st;

  STATS_END_TIMER(stats::Stats::TimerType::WRITE_ARRAY_META)
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

Status StorageManager::init_rest_client() {
  const char* server_address;
  RETURN_NOT_OK(config_.get("rest.server_address", &server_address));
  if (server_address != nullptr) {
    rest_client_.reset(tdb_new(RestClient));
    RETURN_NOT_OK(rest_client_->init(&config_, compute_tp_));
  }

  return Status::Ok();
}

Status StorageManager::write_to_cache(
    const URI& uri, uint64_t offset, Buffer* buffer) const {
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
  Buffer cached_buffer;
  RETURN_NOT_OK(cached_buffer.write(buffer->data(), buffer->size()));
  RETURN_NOT_OK(
      tile_cache_->insert(key.str(), std::move(cached_buffer), false));

  return Status::Ok();
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

Status StorageManager::array_open_without_fragments(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    OpenArray** open_array) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_ARRAY_OPEN_WITHOUT_FRAGMENTS)
  if (!vfs_->supports_uri_scheme(array_uri))
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot open array; URI scheme unsupported."));

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
      *open_array = tdb_new(OpenArray, array_uri, QueryType::READ);
      RETURN_NOT_OK_ELSE(
          (*open_array)->set_encryption_key(encryption_key),
          tdb_delete(*open_array));
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

  STATS_END_TIMER(stats::Stats::TimerType::READ_ARRAY_OPEN_WITHOUT_FRAGMENTS)
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

    if (utils::parse::starts_with(uri_last_path, "__") &&
        !utils::parse::ends_with(uri_last_path, constants::vacuum_file_suffix))
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
  std::vector<tdb_shared_ptr<Buffer>> metadata_buffs;
  metadata_buffs.resize(metadata_num);
  auto statuses = parallel_for(compute_tp_, 0, metadata_num, [&](size_t m) {
    const auto& uri = array_metadata_to_load[m].uri_;
    auto metadata_buff = open_array->array_metadata(uri);
    if (metadata_buff == nullptr) {  // Array metadata does not exist - load it
      GenericTileIO tile_io(this, uri);
      auto tile = (Tile*)nullptr;
      RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key, config_));

      auto chunked_buffer = tile->chunked_buffer();
      metadata_buff = tdb_make_shared(Buffer);
      RETURN_NOT_OK(metadata_buff->realloc(chunked_buffer->size()));
      metadata_buff->set_size(chunked_buffer->size());
      RETURN_NOT_OK_ELSE(
          chunked_buffer->read(metadata_buff->data(), metadata_buff->size(), 0),
          tdb_delete(tile));
      tdb_delete(tile);

      open_array->insert_array_metadata(uri, metadata_buff);
    }
    metadata_buffs[m] = metadata_buff;
    return Status::Ok();
  });
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Compute array metadata size for the statistics
  uint64_t meta_size = 0;
  for (const auto& b : metadata_buffs)
    meta_size += b->size();
  STATS_ADD_COUNTER(stats::Stats::CounterType::READ_ARRAY_META_SIZE, meta_size);

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
    Buffer* meta_buff,
    const std::unordered_map<std::string, uint64_t>& offsets,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_LOAD_FRAG_META)

  // Load the metadata for each fragment, only if they are not already loaded
  auto fragment_num = fragments_to_load.size();
  fragment_metadata->resize(fragment_num);
  auto statuses = parallel_for(compute_tp_, 0, fragment_num, [&](size_t f) {
    const auto& sf = fragments_to_load[f];
    auto array_schema = open_array->array_schema();
    auto metadata = open_array->fragment_metadata(sf.uri_);
    if (metadata == nullptr) {  // Fragment metadata does not exist - load it
      URI coords_uri =
          sf.uri_.join_path(constants::coords + constants::file_suffix);

      auto name = sf.uri_.remove_trailing_slash().last_path_part();
      uint32_t f_version;
      RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &f_version));

      // Note that the fragment metadata version is >= the array schema
      // version. Therefore, the check below is defensive and will always
      // ensure backwards compatibility.
      if (f_version == 1) {  // This is equivalent to format version <=2
        bool sparse;
        RETURN_NOT_OK(vfs_->is_file(coords_uri, &sparse));
        metadata = tdb_new(
            FragmentMetadata,
            this,
            array_schema,
            sf.uri_,
            sf.timestamp_range_,
            !sparse);
      } else {  // Format version > 2
        metadata = tdb_new(
            FragmentMetadata, this, array_schema, sf.uri_, sf.timestamp_range_);
      }

      // Potentially find the basic fragment metadata in the consolidated
      // metadata buffer
      Buffer* f_buff = nullptr;
      uint64_t offset = 0;
      auto it = offsets.find(sf.uri_.to_string());
      if (it != offsets.end()) {
        f_buff = meta_buff;
        offset = it->second;
      }

      // Load fragment metadata
      RETURN_NOT_OK_ELSE(
          metadata->load(encryption_key, f_buff, offset), tdb_delete(metadata));
      open_array->insert_fragment_metadata(metadata);
    }
    (*fragment_metadata)[f] = metadata;
    return Status::Ok();
  });
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_LOAD_FRAG_META)
}

Status StorageManager::load_consolidated_fragment_meta(
    const URI& uri,
    const EncryptionKey& enc_key,
    Buffer* f_buff,
    std::unordered_map<std::string, uint64_t>* offsets) {
  STATS_START_TIMER(stats::Stats::TimerType::READ_LOAD_CONSOLIDATED_FRAG_META)

  // No consolidated fragment metadata file
  if (uri.to_string().empty())
    return Status::Ok();

  GenericTileIO tile_io(this, uri);
  Tile* tile = nullptr;
  RETURN_NOT_OK(tile_io.read_generic(&tile, 0, enc_key, config_));

  auto chunked_buffer = tile->chunked_buffer();
  f_buff->realloc(chunked_buffer->size());
  f_buff->set_size(chunked_buffer->size());
  RETURN_NOT_OK_ELSE(
      chunked_buffer->read(f_buff->data(), f_buff->size(), 0),
      tdb_delete(tile));
  tdb_delete(tile);

  STATS_ADD_COUNTER(
      stats::Stats::CounterType::CONSOLIDATED_FRAG_META_SIZE, f_buff->size());

  uint32_t fragment_num;
  f_buff->reset_offset();
  f_buff->read(&fragment_num, sizeof(uint32_t));

  uint64_t name_size, offset;
  std::string name;
  for (uint32_t f = 0; f < fragment_num; ++f) {
    f_buff->read(&name_size, sizeof(uint64_t));
    name.resize(name_size);
    f_buff->read(&name[0], name_size);
    f_buff->read(&offset, sizeof(uint64_t));
    (*offsets)[name] = offset;
  }

  return Status::Ok();

  STATS_END_TIMER(stats::Stats::TimerType::READ_LOAD_CONSOLIDATED_FRAG_META)
}

Status StorageManager::get_consolidated_fragment_meta_uri(
    const std::vector<URI>& uris, URI* meta_uri) const {
  uint64_t t_latest = 0;
  std::pair<uint64_t, uint64_t> timestamp_range;
  for (const auto& uri : uris) {
    if (utils::parse::ends_with(uri.to_string(), constants::meta_file_suffix)) {
      RETURN_NOT_OK(utils::parse::get_timestamp_range(uri, &timestamp_range));
      if (timestamp_range.second > t_latest) {
        t_latest = timestamp_range.second;
        *meta_uri = uri;
      }
    }
  }

  return Status::Ok();
}

Status StorageManager::get_sorted_uris(
    const std::vector<URI>& uris,
    uint64_t timestamp,
    std::vector<TimestampedURI>* sorted_uris) const {
  // Do nothing if there are not enough URIs
  if (uris.empty())
    return Status::Ok();

  // Get the URIs that must be ignored
  std::vector<URI> vac_uris, to_ignore;
  RETURN_NOT_OK(get_uris_to_vacuum(uris, timestamp, &to_ignore, &vac_uris));
  std::set<URI> to_ignore_set;
  for (const auto& uri : to_ignore)
    to_ignore_set.emplace(uri);

  // Filter based on vacuumed URIs and timestamp
  for (auto& uri : uris) {
    // Ignore vacuumed URIs
    if (to_ignore_set.find(uri) != to_ignore_set.end())
      continue;

    // Also ignore any vac uris
    if (this->is_vacuum_file(uri))
      continue;

    // Add only URIs whose second timestamp is smaller than or equal to
    // the input timestamp
    std::pair<uint64_t, uint64_t> timestamp_range;
    RETURN_NOT_OK(utils::parse::get_timestamp_range(uri, &timestamp_range));
    auto t = timestamp_range.second;
    if (t <= timestamp)
      sorted_uris->emplace_back(uri, timestamp_range);
  }

  // Sort the names based on the timestamps
  std::sort(sorted_uris->begin(), sorted_uris->end());

  return Status::Ok();
}

Status StorageManager::get_uris_to_vacuum(
    const std::vector<URI>& uris,
    uint64_t timestamp,
    std::vector<URI>* to_vacuum,
    std::vector<URI>* vac_uris) const {
  // Get vacuum URIs
  std::unordered_map<std::string, size_t> uris_map;
  for (size_t i = 0; i < uris.size(); ++i) {
    std::pair<uint64_t, uint64_t> timestamp_range;
    RETURN_NOT_OK(utils::parse::get_timestamp_range(uris[i], &timestamp_range));
    if (timestamp_range.second > timestamp)
      continue;

    if (this->is_vacuum_file(uris[i]))
      vac_uris->emplace_back(uris[i]);
    else
      uris_map[uris[i].to_string()] = i;
  }

  // Compute fragment URIs to vacuum as a bitmap vector
  std::vector<int32_t> to_vacuum_vec(uris.size(), 0);
  auto statuses =
      parallel_for(compute_tp_, 0, vac_uris->size(), [&, this](size_t i) {
        uint64_t size = 0;
        RETURN_NOT_OK(vfs_->file_size((*vac_uris)[i], &size));
        std::string names;
        names.resize(size);
        RETURN_NOT_OK(vfs_->read((*vac_uris)[i], 0, &names[0], size));
        std::stringstream ss(names);
        for (std::string uri_str; std::getline(ss, uri_str);) {
          auto it = uris_map.find(uri_str);
          if (it != uris_map.end())
            to_vacuum_vec[it->second] = 1;
        }

        return Status::Ok();
      });
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  // Compute the URIs to vacuum
  to_vacuum->clear();
  for (size_t i = 0; i < uris.size(); ++i) {
    if (to_vacuum_vec[i] == 1)
      to_vacuum->emplace_back(uris[i]);
  }

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
