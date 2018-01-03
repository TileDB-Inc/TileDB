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

#include <algorithm>
#include <sstream>

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
  consolidator_ = nullptr;
  array_metadata_cache_ = nullptr;
  fragment_metadata_cache_ = nullptr;
  tile_cache_ = nullptr;
  vfs_ = nullptr;
}

StorageManager::~StorageManager() {
  async_stop();
  delete async_thread_[0];
  delete async_thread_[1];
  delete array_metadata_cache_;
  delete tile_cache_;
  delete vfs_;
  for (auto& open_array : open_arrays_)
    delete open_array.second;
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManager::array_consolidate(const char* array_name) {
  // Check array URI
  URI array_uri(array_name);
  if (array_uri.is_invalid()) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot consolidate array; Invalid URI"));
  }
  // Check if array exists
  auto obj_type = object_type(array_uri);
  if (obj_type != ObjectType::ARRAY && obj_type != ObjectType::KEY_VALUE) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot consolidate array; Array does not exist"));
  }
  return consolidator_->consolidate(array_name);
}

Status StorageManager::array_create(ArrayMetadata* array_metadata) {
  // Check array metadata
  if (array_metadata == nullptr) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot create array; Empty array metadata"));
  }
  RETURN_NOT_OK(array_metadata->check());

  // Create array directory
  const URI& array_uri = array_metadata->array_uri();
  RETURN_NOT_OK(vfs_->create_dir(array_uri));

  // Store array metadata
  RETURN_NOT_OK_ELSE(
      store_array_metadata(array_metadata), vfs_->remove_path(array_uri));

  // Create array filelock
  URI filelock_uri = array_uri.join_path(constants::array_filelock_name);
  RETURN_NOT_OK_ELSE(
      vfs_->create_file(filelock_uri), vfs_->remove_path(array_uri));

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

Status StorageManager::create_fragment_file(const URI& uri) {
  return create_file(uri.join_path(constants::fragment_filename));
}

Status StorageManager::create_file(const URI& uri) {
  return vfs_->create_file(uri);
}

Status StorageManager::delete_fragment(const URI& uri) const {
  if (!is_fragment(uri)) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot delete fragment directory; '" + uri.to_string() +
        "' is not a TileDB fragment"));
  }
  return vfs_->remove_path(uri);
}

Status StorageManager::remove_path(const URI& uri) const {
  if (object_type(uri) == ObjectType::INVALID) {
    return LOG_STATUS(Status::StorageManagerError(
        "Not a valid TileDB object: " + uri.to_string()));
  }
  return vfs_->remove_path(uri);
}

Status StorageManager::move(
    const URI& old_uri, const URI& new_uri, bool force) const {
  if (object_type(old_uri) == ObjectType::INVALID) {
    return LOG_STATUS(Status::StorageManagerError(
        "Not a valid TileDB object: " + old_uri.to_string()));
  }
  if (force && vfs_->is_dir(new_uri)) {
    RETURN_NOT_OK(remove_path(new_uri));
  }
  return vfs_->move_path(old_uri, new_uri);
}

Status StorageManager::group_create(const std::string& group) const {
  // Create group URI
  URI uri(group);
  if (uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot create group; '" + group + "' invalid group URI"));

  // Create group directory
  RETURN_NOT_OK(vfs_->create_dir(uri));

  // Create group file
  URI group_filename = uri.join_path(constants::group_filename);
  RETURN_NOT_OK(vfs_->create_file(group_filename));

  return Status::Ok();
}

Status StorageManager::init(Config* config) {
  if (config != nullptr)
    config_ = *config;
  RETURN_NOT_OK(config_.init());
  consolidator_ = new Consolidator(this);
  array_metadata_cache_ =
      new LRUCache(config_.get_tiledb_array_metadata_cache_size());
  fragment_metadata_cache_ =
      new LRUCache(config_.get_tiledb_fragment_metadata_cache_size());
  tile_cache_ = new LRUCache(config_.get_tiledb_tile_cache_size());
  async_thread_[0] = new std::thread(async_start, this, 0);
  async_thread_[1] = new std::thread(async_start, this, 1);
  vfs_ = new VFS();
#ifdef HAVE_S3
  S3::S3Config s3_config;
  s3_config.region_ = config_.get_tiledb_s3_region();
  s3_config.scheme_ = config_.get_tiledb_s3_scheme();
  s3_config.endpoint_override_ = config_.get_tiledb_s3_endpoint_override();
  s3_config.use_virtual_addressing_ =
      config_.get_tiledb_s3_use_virtual_addressing();
  s3_config.file_buffer_size_ = config_.get_tiledb_s3_file_buffer_size();
  s3_config.connect_timeout_ms_ = config_.get_tiledb_s3_connect_timeout_ms();
  s3_config.request_timeout_ms_ = config_.get_tiledb_s3_request_timeout_ms();
  RETURN_NOT_OK(vfs_->init(s3_config));
#else
  RETURN_NOT_OK(vfs_->init());
#endif

  return Status::Ok();
}

bool StorageManager::is_array(const URI& uri) const {
  return vfs_->is_file(uri.join_path(constants::array_metadata_filename));
}

bool StorageManager::is_dir(const URI& uri) const {
  return vfs_->is_dir(uri);
}

bool StorageManager::is_file(const URI& uri) const {
  return vfs_->is_file(uri);
}

bool StorageManager::is_fragment(const URI& uri) const {
  return vfs_->is_file(uri.join_path(constants::fragment_filename));
}

bool StorageManager::is_group(const URI& uri) const {
  return vfs_->is_file(uri.join_path(constants::group_filename));
}

bool StorageManager::is_kv(const URI& uri) const {
  return vfs_->is_file(uri.join_path(constants::kv_filename));
}

Status StorageManager::load_array_metadata(
    const URI& array_uri, ArrayMetadata** array_metadata) {
  if (array_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load array metadata; Invalid array URI"));

  // Initialization
  URI array_metadata_uri =
      array_uri.join_path(constants::array_metadata_filename);

  // Try to read from cache
  bool in_cache;
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      array_metadata_cache_->read(
          array_metadata_uri.to_string(), buff, &in_cache),
      delete buff);

  // Read from file if not in cache
  if (!in_cache) {
    delete buff;
    auto tile_io = new TileIO(this, array_metadata_uri);
    auto tile = (Tile*)nullptr;
    RETURN_NOT_OK_ELSE(tile_io->read_generic(&tile, 0), delete tile_io);
    tile->disown_buff();
    buff = tile->buffer();
    delete tile;
    delete tile_io;
  }

  // Deserialize
  auto cbuff = new ConstBuffer(buff);
  *array_metadata = new ArrayMetadata(array_uri);
  Status st = (*array_metadata)->deserialize(cbuff);
  delete cbuff;
  if (!st.ok()) {
    delete array_metadata;
    *array_metadata = nullptr;
  }

  // Store in cache
  if (st.ok() && !in_cache) {
    buff->disown_data();
    st = array_metadata_cache_->insert(
        array_metadata_uri.to_string(), buff->data(), buff->size());
  }

  delete buff;

  return st;
}

Status StorageManager::load_fragment_metadata(
    FragmentMetadata* fragment_metadata) {
  const URI& fragment_uri = fragment_metadata->fragment_uri();

  if (!vfs_->is_dir(fragment_uri))
    return Status::StorageManagerError(
        "Cannot load fragment metadata; Fragment directory does not exist");

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
  if (st.ok() && !in_cache) {
    buff->disown_data();
    st = fragment_metadata_cache_->insert(
        fragment_metadata_uri.to_string(), buff->data(), buff->size());
  }

  delete buff;

  return st;
}

Status StorageManager::move_path(const URI& old_uri, const URI& new_uri) {
  return vfs_->move_path(old_uri, new_uri);
}

ObjectType StorageManager::object_type(const URI& uri) const {
  if (is_group(uri))
    return ObjectType::GROUP;
  if (is_kv(uri))
    return ObjectType::KEY_VALUE;
  if (is_array(uri))
    return ObjectType::ARRAY;

  return ObjectType::INVALID;
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

  // Include the uris that are TileDB objects in the iterator state
  for (auto& uri : uris) {
    if (object_type(uri) != ObjectType::INVALID) {
      (*obj_iter)->objs_.push_back(uri);
      if (order == WalkOrder::POSTORDER)
        (*obj_iter)->expanded_.push_back(false);
    }
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
      for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
        if (object_type(*it) != ObjectType::INVALID) {
          obj_iter->objs_.push_front(*it);
          obj_iter->expanded_.push_front(false);
        }
      }
    } while (obj_num != obj_iter->objs_.size());
  }

  // Prepare the values to be returned
  URI front_uri = obj_iter->objs_.front();
  obj_iter->next_ = front_uri.to_string();
  *type = object_type(front_uri);
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
  *type = object_type(front_uri);
  *path = obj_iter->next_.c_str();
  *has_next = true;

  // Pop the front (next URI) of the iterator's object list
  obj_iter->objs_.pop_front();

  // Get all contents of the next URI
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(front_uri, &uris));

  // Push the new TileDB objects in the front of the iterator's list
  for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
    if (object_type(*it) != ObjectType::INVALID)
      obj_iter->objs_.push_front(*it);
  }

  return Status::Ok();
}

Status StorageManager::query_finalize(Query* query) {
  RETURN_NOT_OK(query->finalize());
  RETURN_NOT_OK(array_close(query->array_metadata()->array_uri()));

  return Status::Ok();
}

Status StorageManager::query_init(
    Query* query, const char* array_name, QueryType type) {
  // Open the array
  std::vector<FragmentMetadata*> fragment_metadata;
  auto array_metadata = (const ArrayMetadata*)nullptr;
  RETURN_NOT_OK(
      array_open(URI(array_name), type, &array_metadata, &fragment_metadata));

  // Set basic query members
  query->set_storage_manager(this);
  query->set_type(type);
  query->set_array_metadata(array_metadata);
  query->set_fragment_metadata(fragment_metadata);

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
  RETURN_NOT_OK(
      array_open(URI(array_name), type, &array_metadata, &fragment_metadata));

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
  // Initialize query
  if (query->status() != QueryStatus::INCOMPLETE)
    RETURN_NOT_OK(query->init());

  // Based on the query type, invoke the appropriate call
  QueryType query_type = query->type();
  if (query_type == QueryType::READ)
    return query->read();

  return query->write();
}

Status StorageManager::query_submit_async(
    Query* query, void* (*callback)(void*), void* callback_data) {
  // Initialize query
  if (query->status() != QueryStatus::INCOMPLETE)
    RETURN_NOT_OK(query->init());

  // Push the query into the async queue
  query->set_callback(callback, callback_data);
  return async_push_query(query, 0);
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

Status StorageManager::read_from_file(
    const URI& uri, uint64_t offset, Buffer* buffer, uint64_t nbytes) const {
  RETURN_NOT_OK(buffer->realloc(nbytes));
  RETURN_NOT_OK(vfs_->read_from_file(uri, offset, buffer->data(), nbytes));
  buffer->set_size(nbytes);
  buffer->reset_offset();

  return Status::Ok();
}

Status StorageManager::store_array_metadata(ArrayMetadata* array_metadata) {
  auto& array_uri = array_metadata->array_uri();
  URI array_metadata_uri =
      array_uri.join_path(constants::array_metadata_filename);

  // Serialize
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(array_metadata->serialize(buff), delete buff);

  // Delete file if it exists already
  if (is_file(array_metadata_uri))
    RETURN_NOT_OK_ELSE(remove_path(array_metadata_uri), delete buff);

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
  if (st.ok())
    st = sync(array_metadata_uri);

  delete tile;
  delete tile_io;
  delete buff;

  // If it is a key-value store, create a new key-value special file
  if (st.ok() && array_metadata->is_kv()) {
    URI kv_uri = array_uri.join_path(constants::kv_filename);
    st = vfs_->create_file(kv_uri);
    if (!st.ok()) {
      vfs_->remove_path(array_metadata_uri);
      vfs_->remove_path(kv_uri);
    }
  }

  return st;
}

Status StorageManager::store_fragment_metadata(FragmentMetadata* metadata) {
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
  if (st.ok())
    st = sync(fragment_metadata_uri);

  delete tile;
  delete tile_io;
  delete buff;

  return st;
}

Status StorageManager::sync(const URI& uri) {
  return vfs_->sync(uri);
}

Status StorageManager::write_to_cache(
    const URI& uri, uint64_t offset, Buffer* buffer) const {
  // Do not write metadata to cache
  std::string filename = uri.last_path_part();
  if (filename == constants::fragment_metadata_filename ||
      filename == constants::array_metadata_filename) {
    return Status::Ok();
  }

  // Generate key (uri + offset)
  std::stringstream key;
  key << uri.to_string() << "+" << offset;

  // Insert to cache
  uint64_t object_size = buffer->size();
  void* object = std::malloc(object_size);
  if (object == nullptr)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot write to cache; Object memory allocation failed"));
  std::memcpy(object, buffer->data(), object_size);
  RETURN_NOT_OK(tile_cache_->insert(key.str(), object, object_size));

  return Status::Ok();
}

Status StorageManager::write_to_file(const URI& uri, Buffer* buffer) const {
  return vfs_->write_to_file(uri, buffer->data(), buffer->size());
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Status StorageManager::array_close(URI array_uri) {
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

  // Potentially remove open array entry
  if (open_array->cnt() == 0) {
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
    const ArrayMetadata** array_metadata,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  // Check if array exists
  if (!is_array(array_uri)) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot open array; Array does not exist"));
  }

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
      open_array_load_array_metadata(array_uri, open_array),
      array_open_error(open_array));
  *array_metadata = open_array->array_metadata();

  // Get fragment metadata only in read mode
  if (type == QueryType::READ)
    RETURN_NOT_OK_ELSE(
        open_array_load_fragment_metadata(open_array, fragment_metadata),
        array_open_error(open_array));

  // Unlock the array mutex
  open_array->mtx_unlock();

  return Status::Ok();
}

Status StorageManager::array_open_error(OpenArray* open_array) {
  open_array->mtx_unlock();
  return array_close(open_array->array_uri());
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
  // Check if async was never started
  if (async_thread_[0] == nullptr)
    return;

  async_done_ = true;
  async_cv_[0].notify_one();
  async_cv_[1].notify_one();
  async_thread_[0]->join();
  async_thread_[1]->join();
}

Status StorageManager::get_fragment_uris(
    const URI& array_uri, std::vector<URI>* fragment_uris) const {
  // Get all uris in the array directory
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(array_uri, &uris));

  // Get only the fragment uris
  for (auto& uri : uris) {
    if (utils::starts_with(uri.last_path_part(), "."))
      continue;
    if (is_fragment(uri))
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

Status StorageManager::open_array_load_array_metadata(
    const URI& array_uri, OpenArray* open_array) {
  // Do nothing if the array metadata is already loaded
  if (open_array->array_metadata() != nullptr)
    return Status::Ok();

  auto array_metadata = (ArrayMetadata*)nullptr;
  RETURN_NOT_OK(load_array_metadata(array_uri, &array_metadata));
  open_array->set_array_metadata(array_metadata);

  return Status::Ok();
}

Status StorageManager::open_array_load_fragment_metadata(
    OpenArray* open_array, std::vector<FragmentMetadata*>* fragment_metadata) {
  // Get all the fragment uris, sorted by timestamp
  std::vector<URI> fragment_uris;
  const URI& array_uri = open_array->array_uri();
  RETURN_NOT_OK(get_fragment_uris(array_uri, &fragment_uris));
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
      RETURN_NOT_OK_ELSE(load_fragment_metadata(metadata), delete metadata);
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
  if (fragment_num == 0)
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
