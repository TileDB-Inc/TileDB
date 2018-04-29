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

#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile_io.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManager::StorageManager() {
  async_done_ = false;
  async_thread_ = nullptr;
  consolidator_ = nullptr;
  array_schema_cache_ = nullptr;
  fragment_metadata_cache_ = nullptr;
  tile_cache_ = nullptr;
  vfs_ = nullptr;
}

StorageManager::~StorageManager() {
  async_stop();
  delete async_thread_;
  delete array_schema_cache_;
  delete consolidator_;
  delete fragment_metadata_cache_;
  delete tile_cache_;
  delete vfs_;
  for (auto& open_array : open_arrays_)
    delete open_array.second;
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManager::array_compute_max_read_buffer_sizes(
    const char* array_uri,
    const void* subarray,
    const char** attributes,
    unsigned attribute_num,
    uint64_t* buffer_sizes) {
  // Open the array
  auto uri = URI(array_uri);
  std::vector<FragmentMetadata*> metadata;
  auto array_schema = (const ArraySchema*)nullptr;
  RETURN_NOT_OK(array_open(uri, QueryType::READ, &array_schema, &metadata));

  // Compute buffer sizes
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      buffer_sizes_tmp;
  for (unsigned i = 0; i < attribute_num; ++i)
    buffer_sizes_tmp[attributes[i]] = std::pair<uint64_t, uint64_t>(0, 0);
  RETURN_NOT_OK_ELSE(
      array_compute_max_read_buffer_sizes(
          array_schema, metadata, subarray, &buffer_sizes_tmp),
      array_close(uri));

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
  return array_close(uri);
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
  URI filelock_uri = array_uri.join_path(constants::filelock_name);
  st = vfs_->touch(filelock_uri);
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
    const char* array_uri, void* domain, bool* is_empty) {
  // Open the array
  *is_empty = true;
  auto uri = URI(array_uri);
  std::vector<FragmentMetadata*> metadata;
  auto array_schema = (const ArraySchema*)nullptr;
  RETURN_NOT_OK(array_open(uri, QueryType::READ, &array_schema, &metadata));

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
  return array_close(uri);
}

Status StorageManager::object_lock(const URI& uri, LockType lock_type) {
  // Lock mutex
  locked_object_mtx_.lock();

  // Create a new locked object entry or retrieve an existing one
  LockedObject* locked_object;
  auto it = locked_objs_.find(uri.to_string());
  if (it == locked_objs_.end()) {
    locked_object = new LockedObject();
    locked_objs_[uri.to_string()] = locked_object;
  } else {
    locked_object = it->second;
  }

  locked_object->incr_total_locks();

  // Unlock the mutex
  locked_object_mtx_.unlock();

  // Lock the filelock
  URI filelock_uri = uri.join_path(constants::filelock_name);
  Status st = locked_object->lock(vfs_, filelock_uri, (lock_type == SLOCK));
  if (!st.ok()) {
    object_unlock(uri, lock_type);
    return st;
  }

  return Status::Ok();
}

Status StorageManager::object_unlock(const URI& uri, LockType lock_type) {
  // Lock the object mutex
  locked_object_mtx_.lock();

  // Find the locked object entry
  auto it = locked_objs_.find(uri.to_string());
  if (it == locked_objs_.end()) {
    locked_object_mtx_.unlock();
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot shared-unlock object; Object not locked"));
  }
  auto locked_object = it->second;

  // Unlock the object
  URI filelock_uri = uri.join_path(constants::filelock_name);
  Status st = locked_object->unlock(vfs_, filelock_uri, (lock_type == SLOCK));

  // Decrement total locks and delete entry if necessary
  locked_object->decr_total_locks();
  if (locked_object->no_locks()) {
    delete it->second;
    locked_objs_.erase(it);
  }

  // Unlock the locked object mutex
  locked_object_mtx_.unlock();

  return st;
}

Status StorageManager::async_push_query(Query* query) {
  // Push request
  {
    std::lock_guard<std::mutex> lock(async_mtx_);
    async_queue_.emplace(query);
  }

  // Signal AIO thread
  async_cv_.notify_one();

  return Status::Ok();
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
  tile_cache_ = new LRUCache(sm_params.tile_cache_size_);
  async_thread_ = new std::thread(async_start, this);
  vfs_ = new VFS();
  RETURN_NOT_OK(vfs_->init(config_.vfs_params()));
  return Status::Ok();
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
    const URI& array_uri, ArraySchema** array_schema) {
  if (array_uri.is_invalid())
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load array schema; Invalid array URI"));

  bool is_array = false;
  bool is_kv = false;
  RETURN_NOT_OK(this->is_array(array_uri, &is_array));
  if (!is_array)
    RETURN_NOT_OK(this->is_kv(array_uri, &is_kv));

  if (!is_array && !is_kv)
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot load array schema; Schema file not found"));

  URI schema_uri = (is_array) ?
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
  bool is_group;
  RETURN_NOT_OK(this->is_group(uri, &is_group));
  if (is_group) {
    *type = ObjectType::GROUP;
    return Status::Ok();
  }

  bool is_kv;
  RETURN_NOT_OK(this->is_kv(uri, &is_kv));
  if (is_kv) {
    *type = ObjectType::KEY_VALUE;
    return Status::Ok();
  }

  bool is_array;
  RETURN_NOT_OK(this->is_array(uri, &is_array));
  if (is_array) {
    *type = ObjectType::ARRAY;
    return Status::Ok();
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

Status StorageManager::query_finalize(Query* query) {
  auto st_query = query->finalize();
  auto st_array = array_close(query->array_schema()->array_uri());

  if (!st_query.ok())
    return st_query;
  if (!st_array.ok())
    return st_array;
  return Status::Ok();
}

Status StorageManager::query_init(
    Query** query, const char* array_name, QueryType type) {
  // Open the array
  std::vector<FragmentMetadata*> fragment_metadata;
  auto array_schema = (const ArraySchema*)nullptr;
  RETURN_NOT_OK(
      array_open(URI(array_name), type, &array_schema, &fragment_metadata));

  // Create query
  *query = new Query(type);
  (*query)->set_storage_manager(this);
  (*query)->set_array_schema(array_schema);
  (*query)->set_fragment_metadata(fragment_metadata);

  return Status::Ok();
}

Status StorageManager::query_init(
    Query** query,
    const char* array_name,
    QueryType type,
    Layout layout,
    const void* subarray,
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes) {
  // Open the array
  std::vector<FragmentMetadata*> fragment_metadata;
  auto array_schema = (const ArraySchema*)nullptr;
  RETURN_NOT_OK(
      array_open(URI(array_name), type, &array_schema, &fragment_metadata));

  // Set basic query members
  *query = new Query(type);
  (*query)->set_array_schema(array_schema);
  (*query)->set_storage_manager(this);
  (*query)->set_layout(layout);
  (*query)->set_fragment_metadata(fragment_metadata);
  Status st = (*query)->set_subarray(subarray);
  if (!st.ok()) {
    delete *query;
    *query = nullptr;
    return st;
  }
  st = (*query)->set_buffers(attributes, attribute_num, buffers, buffer_sizes);
  if (!st.ok()) {
    delete *query;
    *query = nullptr;
    return st;
  }

  return Status::Ok();
}

Status StorageManager::query_submit(Query* query) {
  // Initialize query
  if (query->status() != QueryStatus::INCOMPLETE)
    RETURN_NOT_OK(query->init());

  // Process the query
  return query->process();
}

Status StorageManager::query_submit_async(
    Query* query, std::function<void(void*)> callback, void* callback_data) {
  // Do nothing if the query is completed
  if (query->status() == QueryStatus::COMPLETED)
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
  // Do nothing if fragment directory does not exist. The fragment directory
  // is created only when some attribute file is written
  bool is_dir;
  const URI& fragment_uri = metadata->fragment_uri();
  RETURN_NOT_OK(vfs_->is_dir(fragment_uri, &is_dir));
  if (!is_dir)
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
    st = close_file(fragment_metadata_uri);

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

VFS* StorageManager::vfs() const {
  return vfs_;
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
  RETURN_NOT_OK(object_unlock(array_uri, SLOCK));

  return Status::Ok();
}

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

Status StorageManager::array_open(
    const URI& array_uri,
    QueryType type,
    const ArraySchema** array_schema,
    std::vector<FragmentMetadata*>* fragment_metadata) {
  // Check if array exists
  bool is_array = false;
  RETURN_NOT_OK(this->is_array(array_uri, &is_array));
  bool is_kv = false;
  if (!is_array)
    RETURN_NOT_OK(this->is_kv(array_uri, &is_kv));

  if (!is_array && !is_kv) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot open array; Array does not exist"));
  }

  // Lock the array in shared mode
  RETURN_NOT_OK(object_lock(array_uri, SLOCK));

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

  // Load array schema
  RETURN_NOT_OK_ELSE(
      open_array_load_array_schema(array_uri, open_array),
      array_open_error(open_array));
  *array_schema = open_array->array_schema();

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
  Status st = query->process();
  if (!st.ok())
    LOG_STATUS(st);
}

void StorageManager::async_process_queries() {
  while (!async_done_) {
    std::unique_lock<std::mutex> lock(async_mtx_);
    async_cv_.wait(
        lock, [this] { return !async_queue_.empty() || async_done_; });
    if (async_done_)
      break;
    auto query = async_queue_.front();
    async_queue_.pop();
    lock.unlock();
    async_process_query(query);
  }
}

void StorageManager::async_start(StorageManager* storage_manager) {
  storage_manager->async_process_queries();
}

void StorageManager::async_stop() {
  if (async_thread_ == nullptr)
    return;

  async_done_ = true;
  async_cv_.notify_one();
  async_thread_->join();
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

Status StorageManager::open_array_load_array_schema(
    const URI& array_uri, OpenArray* open_array) {
  // Do nothing if the array schema is already loaded
  if (open_array->array_schema() != nullptr)
    return Status::Ok();

  auto array_schema = (ArraySchema*)nullptr;
  RETURN_NOT_OK(load_array_schema(array_uri, &array_schema));
  open_array->set_array_schema(array_schema);

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
      bool sparse;
      RETURN_NOT_OK(vfs_->is_file(coords_uri, &sparse));
      metadata = new FragmentMetadata(open_array->array_schema(), !sparse, uri);
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
