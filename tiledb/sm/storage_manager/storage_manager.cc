/**
 * @file   storage_manager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include "tiledb/common/common.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <sstream>

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/array_schema/auxiliary.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/consolidator/consolidator.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/object/object.h"
#include "tiledb/sm/object/object_mutex.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"

#include <algorithm>
#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace tiledb::sm {

class StorageManagerException : public StatusException {
 public:
  explicit StorageManagerException(const std::string& message)
      : StatusException("StorageManager", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

StorageManagerCanonical::StorageManagerCanonical(
    ContextResources& resources,
    shared_ptr<Logger> logger,
    const Config& config)
    : resources_(resources)
    , logger_(logger)
    , cancellation_in_progress_(false)
    , config_(config)
    , queries_in_progress_(0) {
  /*
   * This is a transitional version the implementation of this constructor. To
   * complete the transition, the `init` member function must disappear.
   */
  throw_if_not_ok(init());
}

Status StorageManagerCanonical::init() {
  auto& global_state = global_state::GlobalState::GetGlobalState();
  global_state.init(config_);

  RETURN_NOT_OK(set_default_tags());

  global_state.register_storage_manager(this);

  return Status::Ok();
}

StorageManagerCanonical::~StorageManagerCanonical() {
  global_state::GlobalState::GetGlobalState().unregister_storage_manager(this);

  throw_if_not_ok(cancel_all_tasks());

  bool found{false};
  bool use_malloc_trim{false};
  const Status& st =
      config_.get<bool>("sm.mem.malloc_trim", &use_malloc_trim, &found);
  if (st.ok() && found && use_malloc_trim) {
    tdb_malloc_trim();
  }
  assert(found);
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManagerCanonical::array_upgrade_version(
    const URI& array_uri, const Config& override_config) {
  // Check if array exists
  if (!is_array(resources_, array_uri)) {
    throw StorageManagerException(
        "Cannot upgrade array; Array '" + array_uri.to_string() +
        "' does not exist");
  }

  // Load URIs from the array directory
  tiledb::sm::ArrayDirectory array_dir{
      resources(),
      array_uri,
      0,
      UINT64_MAX,
      tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY};

  // Get encryption key from config
  bool found = false;
  std::string encryption_key_from_cfg =
      override_config.get("sm.encryption_key", &found);
  assert(found);
  std::string encryption_type_from_cfg =
      override_config.get("sm.encryption_type", &found);
  assert(found);
  auto [st1, etc] = encryption_type_enum(encryption_type_from_cfg);
  RETURN_NOT_OK(st1);
  EncryptionType encryption_type_cfg = etc.value();

  EncryptionKey encryption_key_cfg;
  if (encryption_key_from_cfg.empty()) {
    RETURN_NOT_OK(encryption_key_cfg.set_key(encryption_type_cfg, nullptr, 0));
  } else {
    RETURN_NOT_OK(encryption_key_cfg.set_key(
        encryption_type_cfg,
        (const void*)encryption_key_from_cfg.c_str(),
        static_cast<uint32_t>(encryption_key_from_cfg.size())));
  }

  auto&& array_schema = array_dir.load_array_schema_latest(
      encryption_key_cfg, resources().ephemeral_memory_tracker());

  if (array_schema->version() < constants::format_version) {
    array_schema->generate_uri();
    array_schema->set_version(constants::format_version);

    // Create array schema directory if necessary
    URI array_schema_dir_uri =
        array_uri.join_path(constants::array_schema_dir_name);
    throw_if_not_ok(resources_.vfs().create_dir(array_schema_dir_uri));

    // Store array schema
    auto st = store_array_schema(resources_, array_schema, encryption_key_cfg);
    RETURN_NOT_OK_ELSE(st, logger_->status_no_return_value(st));

    // Create commit directory if necessary
    URI array_commit_uri =
        array_uri.join_path(constants::array_commits_dir_name);
    throw_if_not_ok(resources_.vfs().create_dir(array_commit_uri));

    // Create fragments directory if necessary
    URI array_fragments_uri =
        array_uri.join_path(constants::array_fragments_dir_name);
    throw_if_not_ok(resources_.vfs().create_dir(array_fragments_uri));

    // Create fragment metadata directory if necessary
    URI array_fragment_metadata_uri =
        array_uri.join_path(constants::array_fragment_meta_dir_name);
    throw_if_not_ok(resources_.vfs().create_dir(array_fragment_metadata_uri));
  }

  return Status::Ok();
}

Status StorageManagerCanonical::async_push_query(Query* query) {
  cancelable_tasks_.execute(
      &resources_.compute_tp(),
      [this, query]() {
        // Process query.
        Status st = query_submit(query);
        if (!st.ok()) {
          resources_.logger()->status_no_return_value(st);
        }
        return st;
      },
      [query]() {
        // Task was cancelled. This is safe to perform in a separate thread,
        // as we are guaranteed by the thread pool not to have entered
        // query->process() yet.
        throw_if_not_ok(query->cancel());
      });

  return Status::Ok();
}

Status StorageManagerCanonical::cancel_all_tasks() {
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
    throw_if_not_ok(resources_.vfs().cancel_all_tasks());

    // Wait for in-progress queries to finish.
    wait_for_zero_in_progress();

    // Reset the cancellation flag.
    std::unique_lock<std::mutex> lck(cancellation_in_progress_mtx_);
    cancellation_in_progress_ = false;
  }

  return Status::Ok();
}

bool StorageManagerCanonical::cancellation_in_progress() {
  std::unique_lock<std::mutex> lck(cancellation_in_progress_mtx_);
  return cancellation_in_progress_;
}

void StorageManagerCanonical::decrement_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_--;
  queries_in_progress_cv_.notify_all();
}

const std::unordered_map<std::string, std::string>&
StorageManagerCanonical::tags() const {
  return tags_;
}

void StorageManagerCanonical::increment_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_++;
  queries_in_progress_cv_.notify_all();
}

Status StorageManagerCanonical::object_iter_begin(
    ObjectIter** obj_iter, const char* path, WalkOrder order) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    throw StorageManagerException(
        "Cannot create object iterator; Invalid input path");
  }

  // Get all contents of path
  std::vector<URI> uris;
  throw_if_not_ok(resources_.vfs().ls(path_uri, &uris));

  // Create a new object iterator
  *obj_iter = tdb_new(ObjectIter);
  (*obj_iter)->order_ = order;
  (*obj_iter)->recursive_ = true;

  // Include the uris that are TileDB objects in the iterator state
  ObjectType obj_type;
  for (auto& uri : uris) {
    RETURN_NOT_OK_ELSE(
        object_type(resources_, uri, &obj_type), tdb_delete(*obj_iter));
    if (obj_type != ObjectType::INVALID) {
      (*obj_iter)->objs_.push_back(uri);
      if (order == WalkOrder::POSTORDER)
        (*obj_iter)->expanded_.push_back(false);
    }
  }

  return Status::Ok();
}

Status StorageManagerCanonical::object_iter_begin(
    ObjectIter** obj_iter, const char* path) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    throw StorageManagerException(
        "Cannot create object iterator; Invalid input path");
  }

  // Get all contents of path
  std::vector<URI> uris;
  throw_if_not_ok(resources_.vfs().ls(path_uri, &uris));

  // Create a new object iterator
  *obj_iter = tdb_new(ObjectIter);
  (*obj_iter)->order_ = WalkOrder::PREORDER;
  (*obj_iter)->recursive_ = false;

  // Include the uris that are TileDB objects in the iterator state
  ObjectType obj_type;
  for (auto& uri : uris) {
    throw_if_not_ok(object_type(resources_, uri, &obj_type));
    if (obj_type != ObjectType::INVALID) {
      (*obj_iter)->objs_.push_back(uri);
    }
  }

  return Status::Ok();
}

void StorageManagerCanonical::object_iter_free(ObjectIter* obj_iter) {
  tdb_delete(obj_iter);
}

Status StorageManagerCanonical::object_iter_next(
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

Status StorageManagerCanonical::object_iter_next_postorder(
    ObjectIter* obj_iter, const char** path, ObjectType* type, bool* has_next) {
  // Get all contents of the next URI recursively till the bottom,
  // if the front of the list has not been expanded
  if (obj_iter->expanded_.front() == false) {
    uint64_t obj_num;
    do {
      obj_num = obj_iter->objs_.size();
      std::vector<URI> uris;
      throw_if_not_ok(resources_.vfs().ls(obj_iter->objs_.front(), &uris));
      obj_iter->expanded_.front() = true;

      // Push the new TileDB objects in the front of the iterator's list
      ObjectType obj_type;
      for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
        throw_if_not_ok(object_type(resources_, *it, &obj_type));
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
  throw_if_not_ok(object_type(resources_, front_uri, type));
  *path = obj_iter->next_.c_str();
  *has_next = true;

  // Pop the front (next URI) of the iterator's object list
  obj_iter->objs_.pop_front();
  obj_iter->expanded_.pop_front();

  return Status::Ok();
}

Status StorageManagerCanonical::object_iter_next_preorder(
    ObjectIter* obj_iter, const char** path, ObjectType* type, bool* has_next) {
  // Prepare the values to be returned
  URI front_uri = obj_iter->objs_.front();
  obj_iter->next_ = front_uri.to_string();
  throw_if_not_ok(object_type(resources_, front_uri, type));
  *path = obj_iter->next_.c_str();
  *has_next = true;

  // Pop the front (next URI) of the iterator's object list
  obj_iter->objs_.pop_front();

  // Return if no recursion is needed
  if (!obj_iter->recursive_)
    return Status::Ok();

  // Get all contents of the next URI
  std::vector<URI> uris;
  throw_if_not_ok(resources_.vfs().ls(front_uri, &uris));

  // Push the new TileDB objects in the front of the iterator's list
  ObjectType obj_type;
  for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
    throw_if_not_ok(object_type(resources_, *it, &obj_type));
    if (obj_type != ObjectType::INVALID)
      obj_iter->objs_.push_front(*it);
  }

  return Status::Ok();
}

Status StorageManagerCanonical::query_submit(Query* query) {
  // Process the query
  QueryInProgress in_progress(this);
  auto st = query->process();

  return st;
}

Status StorageManagerCanonical::query_submit_async(Query* query) {
  // Push the query into the async queue
  return async_push_query(query);
}

Status StorageManagerCanonical::set_tag(
    const std::string& key, const std::string& value) {
  tags_[key] = value;

  // Tags are added to REST requests as HTTP headers.
  if (resources_.rest_client() != nullptr)
    throw_if_not_ok(resources_.rest_client()->set_header(key, value));

  return Status::Ok();
}

void StorageManagerCanonical::wait_for_zero_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_cv_.wait(
      lck, [this]() { return queries_in_progress_ == 0; });
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Status StorageManagerCanonical::set_default_tags() {
  const auto version = std::to_string(constants::library_version[0]) + "." +
                       std::to_string(constants::library_version[1]) + "." +
                       std::to_string(constants::library_version[2]);

  RETURN_NOT_OK(set_tag("x-tiledb-version", version));
  RETURN_NOT_OK(set_tag("x-tiledb-api-language", "c"));

  return Status::Ok();
}

void StorageManagerCanonical::group_metadata_vacuum(
    const char* group_name, const Config& config) {
  // Check group URI
  URI group_uri(group_name);
  if (group_uri.is_invalid()) {
    throw Status_StorageManagerError(
        "Cannot vacuum group metadata; Invalid URI");
  }

  // Check if group exists
  ObjectType obj_type;
  throw_if_not_ok(object_type(resources_, group_uri, &obj_type));

  if (obj_type != ObjectType::GROUP) {
    throw Status_StorageManagerError(
        "Cannot vacuum group metadata; Group does not exist");
  }

  auto consolidator =
      Consolidator::create(ConsolidationMode::GROUP_META, config, this);
  consolidator->vacuum(group_name);
}

}  // namespace tiledb::sm
