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

#include <blosc.h>
#include <algorithm>

#include "logger.h"
#include "storage_manager.h"
#include "utils.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManager::StorageManager() {
  async_done_ = false;
  async_thread_[0] = nullptr;
  async_thread_[1] = nullptr;
  consolidator_ = new Consolidator(this);
  vfs_ = nullptr;
  blosc_init();
}

StorageManager::~StorageManager() {
  async_stop();
  delete async_thread_[0];
  delete async_thread_[1];
  delete vfs_;
  blosc_destroy();
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManager::array_consolidate(const char* array_name) {
  return consolidator_->consolidate(array_name);
}

Status StorageManager::array_create(ArrayMetadata* array_metadata) {
  // Check array_metadata metadata
  if (array_metadata == nullptr) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot create array; Empty array metadata"));
  }

  // Create array directory
  const URI& array_uri = array_metadata->array_uri();
  RETURN_NOT_OK(vfs_->create_dir(array_uri));

  // Store array metadata
  RETURN_NOT_OK(store(array_metadata));

  // Create array filelock
  URI filelock_uri = array_uri.join_path(constants::array_filelock_name);
  RETURN_NOT_OK(vfs_->create_file(filelock_uri));

  // Success
  return Status::Ok();
}

Status StorageManager::array_lock(const URI& array_uri, bool shared) {
  // Lock mutex
  locked_array_mtx_.lock();

  // Create a new locked array entry or retrieve an existing one
  LockedArray* locked_array;
  auto it = locked_arrays_.find(array_uri.to_string());
  if (it == locked_arrays_.end()) {
    locked_array = new LockedArray();
    locked_arrays_[array_uri.to_string()] = locked_array;
  } else {
    locked_array = it->second;
  }

  locked_array->incr_total_locks();

  // Unlock the mutex
  locked_array_mtx_.unlock();

  // Lock the filelock
  URI filelock_uri = array_uri.join_path(constants::array_filelock_name);
  Status st = locked_array->lock(vfs_, filelock_uri, shared);
  if (!st.ok()) {
    array_unlock(array_uri, shared);
    return st;
  }

  return Status::Ok();
}

Status StorageManager::array_unlock(const URI& array_uri, bool shared) {
  // Lock the array mutex
  locked_array_mtx_.lock();

  // Find the locked array entry
  auto it = locked_arrays_.find(array_uri.to_string());
  if (it == locked_arrays_.end()) {
    locked_array_mtx_.unlock();
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot shared-unlock array; Array not locked"));
  }
  auto locked_array = it->second;

  // Unlock the array
  URI filelock_uri = array_uri.join_path(constants::array_filelock_name);
  Status st = locked_array->unlock(vfs_, filelock_uri, shared);

  // Decrement total locks and delete entry if necessary
  locked_array->decr_total_locks();
  if (locked_array->no_locks()) {
    delete it->second;
    locked_arrays_.erase(it);
  }

  // Unlock the open array mutex
  locked_array_mtx_.unlock();

  return st;
}

Status StorageManager::async_push_query(Query* query, int i) {
  // Set the request status
  query->set_status(QueryStatus::INPROGRESS);

  // Push request
  {
    std::lock_guard<std::mutex> lock(async_mtx_[i]);
    async_queue_[i].emplace(query);
  }

  // Signal AIO thread
  async_cv_[i].notify_one();

  return Status::Ok();
}

Status StorageManager::create_dir(const URI& uri) {
  return vfs_->create_dir(uri);
}

Status StorageManager::create_file(const URI& uri) {
  return vfs_->create_file(uri);
}

Status StorageManager::delete_file(const URI& uri) const {
  return vfs_->delete_file(uri);
}

Status StorageManager::delete_fragment(const URI& uri) const {
  if (!is_fragment(uri)) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot delete fragment directory; input "
                                    "directory is not a TileDB fragment"));
  }

  return vfs_->delete_dir(uri);
}

Status StorageManager::file_size(const URI& uri, uint64_t* size) const {
  return vfs_->file_size(uri, size);
}

Status StorageManager::group_create(const std::string& group) const {
  // Create group URI
  URI group_uri(group);
  if (group_uri.is_invalid())
    return LOG_STATUS(
        Status::StorageManagerError("Cannot create group; Invalid group URI"));

  // Create group directory
  RETURN_NOT_OK(vfs_->create_dir(group_uri));

  // Create group file
  URI group_filename = group_uri.join_path(constants::group_filename);
  RETURN_NOT_OK(vfs_->create_file(group_filename));

  return Status::Ok();
}

Status StorageManager::init() {
  async_thread_[0] = new std::thread(async_start, this, 0);
  async_thread_[1] = new std::thread(async_start, this, 1);
  vfs_ = new VFS();

  return Status::Ok();
}

bool StorageManager::is_dir(const URI& uri) {
  return vfs_->is_dir(uri);
}

bool StorageManager::is_file(const URI& uri) {
  return vfs_->is_file(uri);
}

bool StorageManager::is_fragment(const URI& uri) const {
  return vfs_->is_file(uri.join_path(constants::fragment_filename));
}

Status StorageManager::load(
    const std::string& array_name, ArrayMetadata* array_metadata) {
  URI array_uri = URI(array_name);
  if (array_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load array metadata; Invalid array URI"));

  URI array_metadata_uri =
      array_uri.join_path(constants::array_metadata_filename);

  uint64_t buffer_size = 0;
  RETURN_NOT_OK(file_size(array_metadata_uri, &buffer_size));

  // Read from file
  auto tile = (Tile*)nullptr;
  auto tile_io = new TileIO(this, array_metadata_uri);
  RETURN_NOT_OK_ELSE(tile_io->read_generic(&tile, 0), delete tile_io);

  // Deserialize
  tile->reset_offset();
  auto cbuff = new ConstBuffer(tile->buffer());
  Status st = array_metadata->deserialize(cbuff);

  delete cbuff;
  delete tile;
  delete tile_io;

  return st;
}

Status StorageManager::load(FragmentMetadata* fragment_metadata) {
  const URI& fragment_uri = fragment_metadata->fragment_uri();

  if (!vfs_->is_dir(fragment_uri))
    return Status::StorageManagerError(
        "Cannot load fragment metadata; Fragment directory does not exist");

  URI fragment_metadata_uri = fragment_uri.join_path(
      std::string(constants::fragment_metadata_filename));

  // Read from file
  auto tile = (Tile*)nullptr;
  auto tile_io = new TileIO(this, fragment_metadata_uri);
  RETURN_NOT_OK_ELSE(tile_io->read_generic(&tile, 0), delete tile_io);

  // Deserialize
  tile->reset_offset();
  auto cbuff = new ConstBuffer(tile->buffer());
  Status st = fragment_metadata->deserialize(cbuff);

  delete cbuff;
  delete tile;
  delete tile_io;

  return st;
}

Status StorageManager::move_dir(const URI& old_uri, const URI& new_uri) {
  return vfs_->move_dir(old_uri, new_uri);
}

Status StorageManager::query_finalize(Query* query) {
  RETURN_NOT_OK(query->finalize());
  RETURN_NOT_OK(array_close(
      query->array_metadata()->array_uri(), query->fragment_metadata()));

  return Status::Ok();
}

Status StorageManager::query_init(
    Query* query,
    const char* array_name,
    QueryType type,
    Layout layout,
    const void* subarray,
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes,
    const URI& consolidation_fragment_uri) {
  // Open the array
  std::vector<FragmentMetadata*> fragment_metadata;
  auto array_metadata = (const ArrayMetadata*)nullptr;
  RETURN_NOT_OK(array_open(
      URI(array_name), type, subarray, &array_metadata, &fragment_metadata));

  // Initialize query
  return query->init(
      this,
      array_metadata,
      fragment_metadata,
      type,
      layout,
      subarray,
      attributes,
      attribute_num,
      buffers,
      buffer_sizes,
      consolidation_fragment_uri);
}

Status StorageManager::query_submit(Query* query) {
  QueryType query_type = query->type();
  if (query_type == QueryType::READ)
    return query->read();

  return query->write();
}

Status StorageManager::query_submit_async(
    Query* query, void* (*callback)(void*), void* callback_data) {
  // Push the query into the async queue
  query->set_callback(callback, callback_data);
  return async_push_query(query, 0);
}

Status StorageManager::read_from_file(
    const URI& uri, uint64_t offset, Buffer* buffer, uint64_t nbytes) const {
  RETURN_NOT_OK(buffer->realloc(nbytes));
  RETURN_NOT_OK(vfs_->read_from_file(uri, offset, buffer->data(), nbytes));
  buffer->set_size(nbytes);
  buffer->reset_offset();

  return Status::Ok();
}

Status StorageManager::store(ArrayMetadata* array_metadata) {
  RETURN_NOT_OK(array_metadata->check());

  URI array_metadata_uri =
      array_metadata->array_uri().join_path(constants::array_metadata_filename);

  // Serialize
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(array_metadata->serialize(buff), delete buff);

  // Delete file if it exists already
  if (is_file(array_metadata_uri))
    RETURN_NOT_OK_ELSE(delete_file(array_metadata_uri), delete buff);

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
  auto tile_io = new TileIO(this, array_metadata_uri);
  Status st = tile_io->write_generic(tile);

  delete tile;
  delete tile_io;
  delete buff;

  return st;
}

Status StorageManager::store(FragmentMetadata* metadata) {
  const URI& fragment_uri = metadata->fragment_uri();

  if (!vfs_->is_dir(fragment_uri))
    return Status::Ok();

  // Serialize
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(metadata->serialize(buff), delete buff);

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
  Status st = tile_io->write_generic(tile);

  delete tile;
  delete tile_io;
  delete buff;

  //  Status st =
  //      vfs_->write_to_file(metadata_filename, buff->data(), buff->offset());

  //  delete buff;
  return st;
}

Status StorageManager::sync(const URI& uri) {
  return vfs_->sync(uri);
}

Status StorageManager::write_to_file(const URI& uri, Buffer* buffer) const {
  return vfs_->write_to_file(uri, buffer->data(), buffer->size());
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Status StorageManager::array_close(
    URI array_uri, const std::vector<FragmentMetadata*>& fragment_metadata) {
  // Lock mutex
  open_array_mtx_.lock();

  // Find the open array entry
  auto it = open_arrays_.find(array_uri.to_string());

  // Sanity check
  if (it == open_arrays_.end()) {
    open_array_mtx_.unlock();
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot close array; Open array entry not found"));
  }

  // For easy reference
  OpenArray* open_array = it->second;

  // Lock the mutex of the array
  open_array->mtx_lock();

  // Decrement counter
  open_array->decr_cnt();

  // Remove fragment metadata
  for (auto& metadata : fragment_metadata)
    open_array->fragment_metadata_rm(metadata->fragment_uri());

  // Potentially remove open array entry
  if (open_array->cnt() == 0) {
    // TODO: we may want to leave this to a cache manager
    open_array->mtx_unlock();
    delete open_array;
    open_arrays_.erase(it);
  } else {
    // Unlock the mutex of the array
    open_array->mtx_unlock();
  }

  // Unlock mutex
  open_array_mtx_.unlock();

  // Unlock the array
  RETURN_NOT_OK(array_unlock(array_uri, true));

  return Status::Ok();
}

Status StorageManager::array_open(
    const URI& array_uri,
    QueryType type,
    const void* subarray,
    const ArrayMetadata** array_metadata,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  // Lock the array in shared mode
  RETURN_NOT_OK(array_lock(array_uri, true));

  // Lock mutex
  open_array_mtx_.lock();

  // Get the open array entry
  OpenArray* open_array;
  Status st = open_array_get_entry(array_uri, &open_array);
  if (!st.ok()) {
    open_array_mtx_.unlock();
    return st;
  }

  // Lock the mutex of the array
  open_array->mtx_lock();

  // Unlock mutex
  open_array_mtx_.unlock();

  // Increment counter
  open_array->incr_cnt();

  // Load array metadata
  RETURN_NOT_OK_ELSE(
      open_array_load_metadata(array_uri, open_array),
      array_open_error(open_array, *fragment_metadata));
  *array_metadata = open_array->array_metadata();

  // Get fragment metadata only in read mode
  if (type == QueryType::READ)
    RETURN_NOT_OK_ELSE(
        open_array_load_fragment_metadata(
            open_array, subarray, fragment_metadata),
        array_open_error(open_array, *fragment_metadata));

  // Unlock the array mutex
  open_array->mtx_unlock();

  return Status::Ok();
}

Status StorageManager::array_open_error(
    OpenArray* open_array,
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  open_array->mtx_unlock();
  return array_close(open_array->array_uri(), fragment_metadata);
}

void StorageManager::async_process_query(Query* query) {
  // For easy reference
  Status st = query->async_process();
  if (!st.ok())
    LOG_STATUS(st);
}

void StorageManager::async_process_queries(int i) {
  while (!async_done_) {
    std::unique_lock<std::mutex> lock(async_mtx_[i]);
    async_cv_[i].wait(
        lock, [this, i] { return !async_queue_[i].empty() || async_done_; });
    if (async_done_)
      break;
    auto query = async_queue_[i].front();
    async_queue_[i].pop();
    lock.unlock();
    async_process_query(query);
  }
}

void StorageManager::async_start(StorageManager* storage_manager, int i) {
  storage_manager->async_process_queries(i);
}

void StorageManager::async_stop() {
  async_done_ = true;
  async_cv_[0].notify_one();
  async_cv_[1].notify_one();
  async_thread_[0]->join();
  async_thread_[1]->join();
}

Status StorageManager::get_fragment_uris(
    const URI& array_uri,
    const void* subarray,
    std::vector<URI>* fragment_uris) const {
  // Get all uris in the array directory
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(array_uri, &uris));

  // Get only the fragment uris
  for (auto& uri : uris) {
    // TODO: check here if the fragment overlaps subarray
    if (utils::starts_with(uri.last_path_part(), "."))
      continue;
    if (vfs_->is_file(uri.join_path(constants::fragment_filename)))
      fragment_uris->push_back(uri);
  }

  return Status::Ok();
}

Status StorageManager::open_array_get_entry(
    const URI& array_uri, OpenArray** open_array) {
  // Find the open array entry
  auto it = open_arrays_.find(array_uri.to_string());
  // Create and init entry if it does not exist
  if (it == open_arrays_.end()) {
    *open_array = new OpenArray();
    open_arrays_[array_uri.to_string()] = *open_array;
  } else {
    *open_array = it->second;
  }

  return Status::Ok();
}

Status StorageManager::open_array_load_metadata(
    const URI& array_uri, OpenArray* open_array) {
  // Do nothing if the array metadata is already loaded
  if (open_array->array_metadata() != nullptr)
    return Status::Ok();

  auto array_metadata = new ArrayMetadata(array_uri);
  RETURN_NOT_OK_ELSE(
      load(array_uri.to_string(), array_metadata), delete array_metadata);
  open_array->set_array_metadata(array_metadata);

  return Status::Ok();
}

Status StorageManager::open_array_load_fragment_metadata(
    OpenArray* open_array,
    const void* subarray,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  // Get all the fragment uris, sorted by timestamp
  std::vector<URI> fragment_uris;
  const URI& array_uri = open_array->array_uri();
  RETURN_NOT_OK(get_fragment_uris(array_uri, subarray, &fragment_uris));
  sort_fragment_uris(&fragment_uris);

  if (fragment_uris.empty())
    return Status::Ok();

  // Load the metadata for each fragment
  for (auto& uri : fragment_uris) {
    // Find metadata entry in open array
    auto metadata = open_array->fragment_metadata_get(uri);
    // If not found, load metadata and store in open array
    if (metadata == nullptr) {
      URI coords_uri = uri.join_path(
          std::string("/") + constants::coords + constants::file_suffix);
      bool dense = !vfs_->is_file(coords_uri);
      metadata = new FragmentMetadata(open_array->array_metadata(), dense, uri);
      RETURN_NOT_OK_ELSE(load(metadata), delete metadata);
      open_array->fragment_metadata_add(metadata);
    }

    // Add to list
    fragment_metadata->push_back(metadata);
  }

  return Status::Ok();
}

void StorageManager::sort_fragment_uris(std::vector<URI>* fragment_uris) const {
  // Do nothing if there are not enough fragments
  uint64_t fragment_num = fragment_uris->size();
  if (fragment_num <= 0)
    return;

  // Initializations
  std::string t_str;
  uint64_t t, pos = 0;
  std::vector<std::pair<uint64_t, uint64_t>> t_pos_vec;

  // Get the timestamp for each fragment
  for (auto& uri : *fragment_uris) {
    // Get fragment name
    std::string fragment_name = uri.last_path_part();
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

}  // namespace tiledb
