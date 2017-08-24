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
#include <cassert>

#include "../../include/vfs/filesystem.h"
#include "array.h"
#include "basic_array_schema.h"
#include "logger.h"
#include "query.h"
#include "storage_manager.h"
#include "utils.h"

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

  aio_stop();
  delete aio_thread_[0];
  delete aio_thread_[1];
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

Status StorageManager::init(Config* config) {
  // Clear previous config
  if (config_ != nullptr)
    delete config_;

  // Create new config and clone the input
  config_ = new Config(config);

  // Create a thread to handle the user asynchronous I/O
  aio_thread_[0] = new std::thread(aio_start, this, 0);

  // Create a thread to handle the internal asynchronous I/O
  aio_thread_[1] = new std::thread(aio_start, this, 1);

  return Status::Ok();
}

// TODO: Object types should be Enums
int StorageManager::dir_type(const char* dir) {
  // Get real path
  std::string dir_real = vfs::real_dir(dir);

  // Return type
  if (utils::is_group(dir_real))
    return TILEDB_GROUP;
  else if (utils::is_array(dir_real))
    return TILEDB_ARRAY;
  else
    return -1;
}

void StorageManager::set_config(const Config* config) {
  // TODO: make thread-safe?

  config_ = new Config(config);
}

/* ****************************** */
/*             GROUP              */
/* ****************************** */

Status StorageManager::group_create(const uri::URI& group) const {
  // Create group directory
  RETURN_NOT_OK(vfs::create_dir(group.to_string()));
  // Create group file
  RETURN_NOT_OK(vfs::create_group_file(group.to_string()));
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

Status StorageManager::array_consolidate(const uri::URI& array_uri) {
  // Create an array object
  Array* array;
  RETURN_NOT_OK(
      array_init(array, array_uri, QueryMode::READ, nullptr, nullptr, 0));

  // Consolidate array (TODO: unhandled error handling here)
  Fragment* new_fragment;
  std::vector<uri::URI> old_fragments;
  Status st_array_consolidate =
      array->consolidate(new_fragment, &old_fragments);

  // Close the array
  Status st_array_close = array_close(array->array_schema()->array_uri());

  // Finalize consolidation
  Status st_consolidation_finalize =
      consolidation_finalize(new_fragment, old_fragments);

  // Finalize array
  Status st_array_finalize = array->finalize();
  delete array;
  if (!st_array_finalize.ok()) {
    // TODO: Status:
    return LOG_STATUS(
        Status::StorageManagerError(std::string("Could not finalize array: ")
                                        .append(array_uri.to_string())));
  }
  if (!(st_array_close.ok() && !st_consolidation_finalize.ok()))
    return LOG_STATUS(
        Status::StorageManagerError(std::string("Could not consolidate array: ")
                                        .append(array_uri.to_string())));
  return Status::Ok();
}

Status StorageManager::array_create(ArraySchema* array_schema) const {
  // Check array schema
  if (array_schema == nullptr) {
    return LOG_STATUS(
        Status::StorageManagerError("Cannot create array; Empty array schema"));
  }

  // Create array directory
  uri::URI uri = array_schema->array_uri();

  RETURN_NOT_OK(vfs::create_dir(uri.to_posix_path()));

  // Store array schema
  RETURN_NOT_OK(array_schema->store(uri));

  // Create consolidation filelock
  RETURN_NOT_OK(consolidation_lock_create(uri.to_posix_path()));

  // Success
  return Status::Ok();
}

// TODO (jcb): is it true that this cannot fail?
void StorageManager::array_get_fragment_names(
    const uri::URI& array_uri, std::vector<std::string>& fragment_names) {
  // Get directory names in the array folder
  fragment_names =
      vfs::get_fragment_dirs(vfs::real_dir(array_uri.to_posix_path()));
  // Sort the fragment names
  sort_fragment_names(fragment_names);
}

Status StorageManager::array_load_metadata(
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    std::vector<FragmentMetadata*>& metadata,
    QueryMode mode) {
  // TODO (jcb): is this assumed to be always > 0?
  // For easy reference
  int fragment_num = fragment_names.size();

  // Initialization
  metadata.resize(fragment_num);

  // Load the metadata for each fragment
  for (int i = 0; i < fragment_num; ++i) {
    bool dense = !vfs::is_file(
        fragment_names[i] + "/" + constants::coords + constants::file_suffix);
    // Create new metadata structure for the fragment
    FragmentMetadata* meta =
        new FragmentMetadata(array_schema, dense, fragment_names[i]);
    // Load metadata
    RETURN_NOT_OK(meta->load());
    // Append to the open array entry
    metadata[i] = meta;
  }
  return Status::Ok();
}

Status StorageManager::array_init(
    Array*& array,
    const uri::URI& array_uri,
    QueryMode mode,
    const void* subarray,
    const char** attributes,
    int attribute_num) {
  // Load array schema
  ArraySchema* array_schema = new ArraySchema();
  RETURN_NOT_OK_ELSE(array_schema->load(array_uri), delete array_schema);

  // Open the array
  OpenArray* open_array = nullptr;
  if (is_read_mode(mode)) {
    RETURN_NOT_OK(array_open(vfs::abs_path(array_uri), open_array, mode));
  }

  // Create actual array
  array = new Array();
  Status st = array->init(
      this,
      array_schema,
      open_array->fragment_names_,
      open_array->fragment_metadata_,
      mode,
      attributes,
      attribute_num,
      subarray,
      config_);

  // Handle error
  if (!st.ok()) {
    delete array_schema;
    delete array;
    array = nullptr;
    if (is_read_mode(mode))
      array_close(array_uri);
  }

  return st;
}

Status StorageManager::array_finalize(Array* array) {
  // If the array is NULL, do nothing
  if (array == nullptr)
    return Status::Ok();

  // Finalize and close the array
  RETURN_NOT_OK_ELSE(array->finalize(), delete array);
  if (is_read_mode(array->query_->mode()))
    RETURN_NOT_OK_ELSE(
        array_close(array->array_schema()->array_uri()), delete array)

  // Clean up
  delete array;

  return Status::Ok();
}

/* ****************************** */
/*               MISC             */
/* ****************************** */

Status StorageManager::ls(
    const uri::URI& parent_uri,
    char** object_paths,
    tiledb_object_t* object_types,
    int* object_num) const {
  // Initialize object counter
  int obj_idx = 0;

  std::vector<std::string> paths;
  RETURN_NOT_OK(vfs::ls(parent_uri.to_posix_path(), &paths));

  for (auto& path : paths) {
    if (utils::is_group(path)) {  // Group
      if (obj_idx == *object_num) {
        return LOG_STATUS(Status::StorageManagerError(
            "Cannot list TileDB path; object buffer overflow"));
      }
      strcpy(object_paths[obj_idx], path.c_str());
      object_types[obj_idx] = TILEDB_GROUP;
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

Status StorageManager::ls_c(const uri::URI& parent_uri, int* object_num) const {
  *object_num = 0;
  // Initialize directory number
  std::vector<std::string> paths;
  RETURN_NOT_OK(vfs::ls(parent_uri.to_posix_path(), &paths));

  *object_num =
      std::count_if(paths.begin(), paths.end(), [](std::string& path) {
        return utils::is_group(path) || utils::is_array(path);
      });
  return Status::Ok();
}

Status StorageManager::clear(const uri::URI& uri) const {
  if (utils::is_group(uri)) {
    return group_clear(uri);
  } else if (utils::is_array(uri)) {
    return array_clear(uri);
  } else {
    return LOG_STATUS(
        Status::StorageManagerError("Clear failed; Invalid directory"));
  }
}

Status StorageManager::delete_entire(const uri::URI& uri) {
  if (utils::is_group(uri)) {
    return group_delete(uri);
  } else if (utils::is_array(uri)) {
    return array_delete(uri);
  } else {
    return LOG_STATUS(
        Status::StorageManagerError("Delete failed; Invalid directory"));
  }
}

Status StorageManager::move(const uri::URI& old_uri, const uri::URI& new_uri) {
  if (utils::is_group(old_uri)) {
    return group_move(old_uri, new_uri);
  } else if (utils::is_array(old_uri)) {
    return array_move(old_uri, new_uri);
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
  Query* query = aio_request->query();
  Status st = query->array()->aio_handle_request(aio_request);
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

Status StorageManager::array_clear(const uri::URI& array) const {
  // Get real array directory name
  uri::URI array_uri = vfs::abs_path(array);

  // Check if the array exists
  if (!utils::is_array(array_uri)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Array '") + array_uri.to_string() + "' does not exist"));
  }

  // Delete the entire array directory except for the array schema file
  std::vector<std::string> paths;
  RETURN_NOT_OK(vfs::ls(array_uri.to_posix_path(), &paths));
  for (auto& path : paths) {
    if (utils::is_array_schema(path) || utils::is_consolidation_lock(path))
      continue;
    if (utils::is_fragment(path)) {  // Fragment
      RETURN_NOT_OK(vfs::delete_dir(path));
    } else {  // Non TileDB related
      return LOG_STATUS(Status::StorageManagerError(
          std::string("Cannot delete non TileDB related path '") + path + "'"));
    }
  }
  return Status::Ok();
}

Status StorageManager::array_close(const uri::URI& array_uri) {
  // Lock mutexes
  open_array_mtx_.lock();

  // Find the open array entry
  std::map<std::string, OpenArray*>::iterator it =
      open_arrays_.find(vfs::abs_path(array_uri).to_string());

  // Sanity check
  if (it == open_arrays_.end()) {
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot close array; Open array entry not found"));
  }

  // Lock the mutex of the array
  it->second->mtx_lock();

  // Decrement counter
  --(it->second->cnt_);

  // Delete open array entry if necessary
  Status st_mtx_destroy = Status::Ok();
  Status st_filelock = Status::Ok();
  if (it->second->cnt_ == 0) {
    // Clean up metadata
    std::vector<FragmentMetadata*>::iterator bit =
        it->second->fragment_metadata_.begin();
    for (; bit != it->second->fragment_metadata_.end(); ++bit)
      delete *bit;

    // Unlock mutex of the array
    it->second->mtx_unlock();

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
    it->second->mtx_unlock();
  }

  // Unlock mutex
  open_array_mtx_.unlock();

  return Status::Ok();
}

Status StorageManager::array_delete(const uri::URI& array) const {
  RETURN_NOT_OK(array_clear(array));
  // Delete array directory
  RETURN_NOT_OK(vfs::delete_dir(array));
  return Status::Ok();
}

Status StorageManager::array_get_open_array_entry(
    const uri::URI& array_uri, OpenArray*& open_array) {
  // Lock mutex
  open_array_mtx_.lock();

  // Find the open array entry
  std::map<std::string, OpenArray*>::iterator it =
      open_arrays_.find(array_uri.to_string());
  // Create and init entry if it does not exist
  if (it == open_arrays_.end()) {
    open_array = new OpenArray();
    open_array->cnt_ = 0;
    open_array->consolidation_filelock_ = -1;
    open_array->fragment_metadata_ = std::vector<FragmentMetadata*>();
    open_arrays_[array_uri.to_posix_path()] = open_array;
  } else {
    open_array = it->second;
  }

  // Increment counter
  ++(open_array->cnt_);

  // Unlock mutex
  open_array_mtx_.unlock();

  return Status::Ok();
}

Status StorageManager::array_move(
    const uri::URI& old_array, const uri::URI& new_array) const {
  uri::URI old_array_uri = vfs::abs_path(old_array);
  uri::URI new_array_uri = vfs::abs_path(new_array);

  // Check if the old array exists
  if (!utils::is_array(old_array_uri)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Array '") + old_array_uri.to_string() +
        "' does not exist"));
  }

  // Make sure that the new array is not an existing directory
  if (vfs::is_dir(new_array_uri)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Array '") + new_array_uri.to_string() +
        "' already exists"));
  }
  // Rename array
  RETURN_NOT_OK(vfs::move(old_array_uri, new_array_uri));

  // Incorporate new name in the array schema
  ArraySchema* array_schema = new ArraySchema();
  RETURN_NOT_OK_ELSE(array_schema->load(new_array_uri), delete array_schema);
  array_schema->set_array_uri(new_array_uri);

  // Store the new schema
  RETURN_NOT_OK_ELSE(array_schema->store(new_array_uri), delete array_schema);

  // Clean up
  delete array_schema;

  // Success
  return Status::Ok();
}

Status StorageManager::array_open(
    const uri::URI& array_uri, OpenArray*& open_array, QueryMode mode) {
  // Get the open array entry
  RETURN_NOT_OK(array_get_open_array_entry(array_uri, open_array));

  // Lock the mutex of the array
  open_array->mtx_lock();

  // First time the array is opened
  if (open_array->fragment_names_.size() == 0) {
    // Acquire shared lock on consolidation filelock
    RETURN_NOT_OK_ELSE(
        consolidation_lock(
            array_uri.to_posix_path(),
            &(open_array->consolidation_filelock_),
            true),
        open_array->mtx_unlock());

    // Get the fragment names
    array_get_fragment_names(array_uri, open_array->fragment_names_);

    // Get array schema
    open_array->array_schema_ = new ArraySchema();
    ArraySchema* array_schema = open_array->array_schema_;
    if (utils::is_array(array_uri)) {  // Array
      RETURN_NOT_OK_ELSE(array_schema->load(array_uri), delete array_schema);
    }
    // Load the metadata for each fragment
    Status st = array_load_metadata(
        open_array->array_schema_,
        open_array->fragment_names_,
        open_array->fragment_metadata_,
        mode);
    if (!st.ok()) {
      delete open_array->array_schema_;
      open_array->array_schema_ = nullptr;
      open_array->mtx_unlock();
      return st;
    }
  }

  // Unlock the mutex of the array
  open_array->mtx_unlock();

  return Status::Ok();
}

Status StorageManager::consolidation_lock_create(const std::string& dir) const {
  // Create file
  std::string filename = dir + "/" + constants::consolidation_filelock_name;
  return vfs::filelock_create(filename);
}

Status StorageManager::consolidation_lock(
    const std::string& array_name, int* fd, bool shared) const {
  // Prepare the filelock name
  std::string array_name_real = vfs::real_dir(array_name);
  std::string filename =
      array_name_real + "/" + constants::consolidation_filelock_name;
  return vfs::filelock_lock(filename, fd, shared);
}

Status StorageManager::consolidation_unlock(int fd) const {
  return vfs::filelock_unlock(fd);
}

Status StorageManager::consolidation_finalize(
    Fragment* new_fragment, const std::vector<uri::URI>& old_fragments) {
  // Trivial case - there was no consolidation
  if (old_fragments.size() == 0)
    return Status::Ok();

  // Acquire exclusive lock on consolidation filelock
  int fd;
  Status st;
  st = consolidation_lock(
      new_fragment->array()->array_schema()->array_uri().to_posix_path(),
      &fd,
      false);
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

  // TODO: abstract framgent IO
  // Make old fragments invisible to new reads
  int fragment_num = old_fragments.size();
  for (int i = 0; i < fragment_num; ++i) {
    // Delete special fragment file inside the fragment directory
    std::string old_fragment_filename =
        old_fragments[i]
            .join_path(constants::fragment_filename)
            .to_posix_path();
    RETURN_NOT_OK(vfs::delete_file(old_fragment_filename));
  }

  // Unlock consolidation filelock
  RETURN_NOT_OK(consolidation_unlock(fd));

  // Delete old fragments
  for (int i = 0; i < fragment_num; ++i) {
    RETURN_NOT_OK(vfs::delete_dir(old_fragments[i]));
  }
  return Status::Ok();
}

Status StorageManager::group_clear(const uri::URI& group) const {
  uri::URI group_uri = vfs::abs_path(group);

  // Check if group exists
  if (!utils::is_group(group_uri)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Group '") + group_uri.to_string() + "' does not exist"));
  }

  // Delete all groups, arrays inside the group directory
  // TODO: this functionalty should be moved to a Filesystem IO backend
  std::vector<std::string> paths;
  RETURN_NOT_OK(vfs::ls(group_uri.to_string(), &paths));
  for (auto& path : paths) {
    if (utils::is_group(path)) {  // Group
      RETURN_NOT_OK(group_delete(path))
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

Status StorageManager::group_delete(const uri::URI& group) const {
  // Clear the group
  RETURN_NOT_OK(group_clear(group));

  // Delete group directory
  RETURN_NOT_OK(vfs::delete_dir(group));

  return Status::Ok();
}

Status StorageManager::group_move(
    const uri::URI& old_group, const uri::URI& new_group) const {
  // Get real group directory names
  uri::URI old_group_uri = vfs::abs_path(old_group);
  uri::URI new_group_uri = vfs::abs_path(new_group);

  // Check if the old group exists
  if (!utils::is_group(old_group_uri)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Group '") + old_group_uri.to_string() +
        "' does not exist"));
  }

  // Make sure that the new group is not an existing directory
  if (vfs::is_dir(new_group_uri)) {
    return LOG_STATUS(Status::StorageManagerError(
        std::string("Group '") + new_group_uri.to_string() +
        "' already exists"));
  }

  return vfs::move(old_group_uri, new_group_uri);
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
  std::sort(t_pos_vec.begin(), t_pos_vec.end());
  std::vector<std::string> fragment_names_sorted;
  fragment_names_sorted.resize(fragment_num);
  for (int i = 0; i < fragment_num; ++i)
    fragment_names_sorted[i] = fragment_names[t_pos_vec[i].second];
  fragment_names = fragment_names_sorted;
}

void StorageManager::OpenArray::mtx_lock() {
  mtx_.lock();
}

void StorageManager::OpenArray::mtx_unlock() {
  mtx_.unlock();
}

}  // namespace tiledb