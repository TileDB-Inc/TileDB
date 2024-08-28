/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file implements the data-oriented operations on class Array.
 *
 */

#include "tiledb/common/common.h"

#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/array_schema/array_schema_operations.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/crypto/crypto.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/object/object.h"
#include "tiledb/sm/object/object_mutex.h"
#include "tiledb/sm/query/update_value.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/tile/generic_tile_io.h"

#include <cassert>
#include <cmath>
#include <iostream>

using namespace tiledb::common;

namespace tiledb::sm {

class ArrayException : public StatusException {
 public:
  explicit ArrayException(const std::string& message)
      : StatusException("Array", message) {
  }
};

void ensure_supported_schema_version_for_read(format_version_t version);

ConsistencyController& controller() {
  static ConsistencyController controller;
  return controller;
}

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Array::Array(
    ContextResources& resources,
    const URI& array_uri,
    ConsistencyController& cc)
    : resources_(resources)
    , array_uri_(array_uri)
    , array_uri_serialized_(array_uri)
    , is_open_(false)
    , is_opening_or_closing_(false)
    , array_dir_timestamp_start_(0)
    , user_set_timestamp_end_(nullopt)
    , array_dir_timestamp_end_(UINT64_MAX)
    , new_component_timestamp_(nullopt)
    , config_(resources_.config())
    , remote_(array_uri.is_tiledb())
    , memory_tracker_(resources_.create_memory_tracker())
    , consistency_controller_(cc)
    , consistency_sentry_(nullopt) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

void Array::evolve_array_schema(
    ContextResources& resources,
    const URI& array_uri,
    ArraySchemaEvolution* schema_evolution,
    const EncryptionKey& encryption_key) {
  // Check array schema
  if (schema_evolution == nullptr) {
    throw ArrayException("Cannot evolve array; Empty schema evolution");
  }

  if (array_uri.is_tiledb()) {
    throw_if_not_ok(
        resources.rest_client()->post_array_schema_evolution_to_rest(
            array_uri, schema_evolution));
    return;
  }

  // Load URIs from the array directory
  tiledb::sm::ArrayDirectory array_dir{
      resources,
      array_uri,
      0,
      UINT64_MAX,
      tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY};

  // Check if array exists
  if (!is_array(resources, array_uri)) {
    throw ArrayException(
        "Cannot evolve array; '" + array_uri.to_string() + "' does not exist");
  }

  auto&& array_schema = array_dir.load_array_schema_latest(
      encryption_key, resources.ephemeral_memory_tracker());

  // Load required enumerations before evolution.
  auto enmr_names = schema_evolution->enumeration_names_to_extend();
  if (enmr_names.size() > 0) {
    std::unordered_set<std::string> enmr_path_set;
    for (auto name : enmr_names) {
      enmr_path_set.insert(array_schema->get_enumeration_path_name(name));
    }
    std::vector<std::string> enmr_paths;
    for (auto path : enmr_path_set) {
      enmr_paths.emplace_back(path);
    }

    auto loaded_enmrs = array_dir.load_enumerations_from_paths(
        enmr_paths, encryption_key, resources.create_memory_tracker());

    for (auto enmr : loaded_enmrs) {
      array_schema->store_enumeration(enmr);
    }
  }

  // Evolve schema
  auto array_schema_evolved = schema_evolution->evolve_schema(array_schema);
  try {
    store_array_schema(resources, array_schema_evolved, encryption_key);
  } catch (...) {
    std::throw_with_nested(ArrayException(
        "Cannot evolve schema; Not able to store evolved array schema."));
  }
}

const URI& Array::array_uri() const {
  return array_uri_;
}

const URI& Array::array_uri_serialized() const {
  return array_uri_serialized_;
}

const EncryptionKey* Array::encryption_key() const {
  return opened_array_->encryption_key();
}

void Array::create(
    ContextResources& resources,
    const URI& array_uri,
    const shared_ptr<ArraySchema>& array_schema,
    const EncryptionKey& encryption_key) {
  // Check array schema
  if (array_schema == nullptr) {
    throw ArrayException("Cannot create array; Empty array schema");
  }

  // Check if array exists
  if (is_array(resources, array_uri)) {
    throw ArrayException(
        "Cannot create array; Array '" + array_uri.to_string() +
        "' already exists");
  }

  std::lock_guard<std::mutex> lock{object_mtx};
  array_schema->set_array_uri(array_uri);
  array_schema->generate_uri(array_schema->timestamp_range());
  array_schema->check(resources.config());

  // Check current domain is specified correctly if set
  if (!array_schema->get_current_domain()->empty()) {
    array_schema->get_current_domain()->check_schema_sanity(
        array_schema->shared_domain());
  }

  // Create array directory
  throw_if_not_ok(resources.vfs().create_dir(array_uri));

  // Create array schema directory
  URI array_schema_dir_uri =
      array_uri.join_path(constants::array_schema_dir_name);
  throw_if_not_ok(resources.vfs().create_dir(array_schema_dir_uri));

  // Create the enumerations directory inside the array schema directory
  URI array_enumerations_uri =
      array_schema_dir_uri.join_path(constants::array_enumerations_dir_name);
  throw_if_not_ok(resources.vfs().create_dir(array_enumerations_uri));

  // Create commit directory
  URI array_commit_uri = array_uri.join_path(constants::array_commits_dir_name);
  throw_if_not_ok(resources.vfs().create_dir(array_commit_uri));

  // Create fragments directory
  URI array_fragments_uri =
      array_uri.join_path(constants::array_fragments_dir_name);
  throw_if_not_ok(resources.vfs().create_dir(array_fragments_uri));

  // Create array metadata directory
  URI array_metadata_uri =
      array_uri.join_path(constants::array_metadata_dir_name);
  throw_if_not_ok(resources.vfs().create_dir(array_metadata_uri));

  // Create fragment metadata directory
  URI array_fragment_metadata_uri =
      array_uri.join_path(constants::array_fragment_meta_dir_name);
  throw_if_not_ok(resources.vfs().create_dir(array_fragment_metadata_uri));

  // Create dimension label directory
  URI array_dimension_labels_uri =
      array_uri.join_path(constants::array_dimension_labels_dir_name);
  throw_if_not_ok(resources.vfs().create_dir(array_dimension_labels_uri));

  // Store the array schema
  try {
    // Get encryption key from config
    if (encryption_key.encryption_type() == EncryptionType::NO_ENCRYPTION) {
      bool found = false;
      std::string encryption_key_from_cfg =
          resources.config().get("sm.encryption_key", &found);
      assert(found);
      std::string encryption_type_from_cfg =
          resources.config().get("sm.encryption_type", &found);
      assert(found);
      auto&& [st_enc, etc] = encryption_type_enum(encryption_type_from_cfg);
      throw_if_not_ok(st_enc);
      EncryptionType encryption_type_cfg = etc.value();

      EncryptionKey encryption_key_cfg;
      if (encryption_key_from_cfg.empty()) {
        throw_if_not_ok(
            encryption_key_cfg.set_key(encryption_type_cfg, nullptr, 0));
      } else {
        throw_if_not_ok(encryption_key_cfg.set_key(
            encryption_type_cfg,
            (const void*)encryption_key_from_cfg.c_str(),
            static_cast<uint32_t>(encryption_key_from_cfg.size())));
      }
      store_array_schema(resources, array_schema, encryption_key_cfg);
    } else {
      store_array_schema(resources, array_schema, encryption_key);
    }
  } catch (...) {
    throw_if_not_ok(resources.vfs().remove_dir(array_uri));
    throw;
  }
}

// Used in Consolidator
Status Array::open_without_fragments(
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer = resources_.stats().start_timer("array_open_without_fragments");
  Status st;
  // Checks
  if (is_open()) {
    return LOG_STATUS(Status_ArrayError(
        "Cannot open array without fragments; Array already open"));
  }
  if (remote_ && encryption_type != EncryptionType::NO_ENCRYPTION) {
    return LOG_STATUS(
        Status_ArrayError("Cannot open array without fragments; encrypted "
                          "remote arrays are not supported."));
  }

  opened_array_ = make_shared<OpenedArray>(
      HERE(),
      resources_,
      memory_tracker_,
      array_uri_,
      encryption_type,
      encryption_key,
      key_length,
      timestamp_start(),
      timestamp_end_opened_at(),
      is_remote());

  /* Note: query_type_ MUST be set before calling set_array_open()
    because it will be examined by the ConsistencyController. */
  query_type_ = QueryType::READ;
  memory_tracker_->set_type(MemoryTrackerType::ARRAY_READ);

  /* Note: the open status MUST be exception safe. If anything interrupts the
   * opening process, it will throw and the array will be set as closed. */
  try {
    set_array_open(query_type_);

    if (remote_) {
      auto rest_client = resources_.rest_client();
      if (rest_client == nullptr) {
        throw ArrayException(
            "Cannot open array; remote array with no REST client.");
      }
      /* #TODO Change get_array_schema_from_rest function signature to
        throw instead of return Status */
      if (!use_refactored_array_open()) {
        auto&& [st, array_schema_latest] =
            rest_client->get_array_schema_from_rest(array_uri_);
        if (!st.ok()) {
          throw StatusException(st);
        }
        set_array_schema_latest(array_schema_latest.value());
      } else {
        rest_client->post_array_from_rest(array_uri_, resources_, this);
      }
    } else {
      {
        auto timer_se = resources_.stats().start_timer(
            "array_open_without_fragments_load_directory");
        set_array_directory(ArrayDirectory(
            resources_, array_uri_, 0, UINT64_MAX, ArrayDirectoryMode::READ));
      }
      auto&& [array_schema_latest, array_schemas_all] =
          open_for_reads_without_fragments();
      set_array_schema_latest(array_schema_latest);
      set_array_schemas_all(std::move(array_schemas_all));
    }
  } catch (...) {
    set_array_closed();
    std::throw_with_nested(
        ArrayException("Error opening array without fragments."));
  }

  is_opening_or_closing_ = false;
  return Status::Ok();
}

void Array::load_fragments(
    const std::vector<TimestampedURI>& fragments_to_load) {
  auto timer_se = resources_.stats().start_timer("sm_array_load_fragments");

  // Load the fragment metadata
  std::unordered_map<std::string, std::pair<Tile*, uint64_t>> offsets;
  set_fragment_metadata(FragmentMetadata::load(
      resources_,
      memory_tracker(),
      opened_array_->array_schema_latest_ptr(),
      array_schemas_all(),
      *encryption_key(),
      fragments_to_load,
      offsets));
}

Status Array::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  return Array::open(
      query_type,
      array_dir_timestamp_start_,
      user_set_timestamp_end_.value_or(UINT64_MAX),
      encryption_type,
      encryption_key,
      key_length);
}

Status Array::open(
    QueryType query_type,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer = resources_.stats().start_timer(
      "array_open_" + query_type_str(query_type));
  Status st;
  // Checks
  if (is_open()) {
    return LOG_STATUS(
        Status_ArrayError("Cannot open array; Array already open."));
  }

  query_type_ = query_type;
  if (query_type_ == QueryType::READ) {
    memory_tracker_->set_type(MemoryTrackerType::ARRAY_READ);
  } else {
    memory_tracker_->set_type(MemoryTrackerType::ARRAY_WRITE);
  }

  set_timestamps(
      timestamp_start, timestamp_end, query_type_ == QueryType::READ);

  /* Note: the open status MUST be exception safe. If anything interrupts the
   * opening process, it will throw and the array will be set as closed. */
  try {
    set_array_open(query_type);

    if (query_type == QueryType::UPDATE) {
      bool found = false;
      bool allow_updates = false;
      if (!config_
               .get<bool>(
                   "sm.allow_updates_experimental", &allow_updates, &found)
               .ok()) {
        throw ArrayException("Cannot get setting");
      }
      assert(found);

      if (!allow_updates) {
        throw ArrayException(
            "Cannot open array; Update query type is only experimental, do "
            "not use.");
      }
    }

    // Get encryption key from config
    std::string encryption_key_from_cfg;
    if (!encryption_key) {
      bool found = false;
      encryption_key_from_cfg = config_.get("sm.encryption_key", &found);
      assert(found);
    }

    if (!encryption_key_from_cfg.empty()) {
      encryption_key = encryption_key_from_cfg.c_str();
      key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
      std::string encryption_type_from_cfg;
      bool found = false;
      encryption_type_from_cfg = config_.get("sm.encryption_type", &found);
      assert(found);
      auto [st, et] = encryption_type_enum(encryption_type_from_cfg);
      if (!st.ok()) {
        throw StatusException(st);
      }
      encryption_type = et.value();

      if (!EncryptionKey::is_valid_key_length(
              encryption_type,
              static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
        encryption_key = nullptr;
        key_length = 0;
      }
    }

    if (remote_ && encryption_type != EncryptionType::NO_ENCRYPTION) {
      throw ArrayException(
          "Cannot open array; encrypted remote arrays are not supported.");
    }

    opened_array_ = make_shared<OpenedArray>(
        HERE(),
        resources_,
        memory_tracker_,
        array_uri_,
        encryption_type,
        encryption_key,
        key_length,
        this->timestamp_start(),
        timestamp_end_opened_at(),
        is_remote());

    if (remote_) {
      auto rest_client = resources_.rest_client();
      if (rest_client == nullptr) {
        throw ArrayException(
            "Cannot open array; remote array with no REST client.");
      }
      if (!use_refactored_array_open()) {
        auto&& [st, array_schema_latest] =
            rest_client->get_array_schema_from_rest(array_uri_);
        throw_if_not_ok(st);
        set_array_schema_latest(array_schema_latest.value());
        if (serialize_enumerations()) {
          // The route for array open v1 does not currently support loading
          // enumerations. Once #5359 is merged and deployed to REST this will
          // not be the case.
          load_all_enumerations(false);
        }
      } else {
        rest_client->post_array_from_rest(array_uri_, resources_, this);
      }
    } else if (query_type == QueryType::READ) {
      {
        auto timer_se =
            resources_.stats().start_timer("array_open_read_load_directory");
        set_array_directory(ArrayDirectory(
            resources_,
            array_uri_,
            array_dir_timestamp_start_,
            array_dir_timestamp_end_));
      }
      auto&& [array_schema_latest, array_schemas_all, fragment_metadata] =
          open_for_reads();
      set_array_schema_latest(array_schema_latest);
      set_array_schemas_all(std::move(array_schemas_all));
      set_fragment_metadata(std::move(fragment_metadata));
    } else if (
        query_type == QueryType::WRITE ||
        query_type == QueryType::MODIFY_EXCLUSIVE) {
      {
        auto timer_se =
            resources_.stats().start_timer("array_open_write_load_directory");
        set_array_directory(ArrayDirectory(
            resources_,
            array_uri_,
            array_dir_timestamp_start_,
            array_dir_timestamp_end_,
            ArrayDirectoryMode::SCHEMA_ONLY));
      }

      // Set schemas
      auto&& [array_schema_latest, array_schemas] = open_for_writes();
      set_array_schema_latest(array_schema_latest);
      set_array_schemas_all(std::move(array_schemas));

      // Set the timestamp
      opened_array_->metadata().reset(timestamp_end_opened_at());
    } else if (
        query_type == QueryType::DELETE || query_type == QueryType::UPDATE) {
      {
        auto timer_se = resources_.stats().start_timer(
            "array_open_delete_or_update_load_directory");
        set_array_directory(ArrayDirectory(
            resources_,
            array_uri_,
            array_dir_timestamp_start_,
            array_dir_timestamp_end_,
            ArrayDirectoryMode::READ));
      }

      // Set schemas
      auto&& [latest, array_schemas] = open_for_writes();
      set_array_schema_latest(latest);
      set_array_schemas_all(std::move(array_schemas));

      auto version = array_schema_latest().version();
      if (query_type == QueryType::DELETE &&
          version < constants::deletes_min_version) {
        std::stringstream err;
        err << "Cannot open array for deletes; Array format version (";
        err << version;
        err << ") is smaller than the minimum supported version (";
        err << constants::deletes_min_version << ").";
        return LOG_STATUS(Status_ArrayError(err.str()));
      } else if (
          query_type == QueryType::UPDATE &&
          version < constants::updates_min_version) {
        std::stringstream err;
        err << "Cannot open array for updates; Array format version (";
        err << version;
        err << ") is smaller than the minimum supported version (";
        err << constants::updates_min_version << ").";
        return LOG_STATUS(Status_ArrayError(err.str()));
      }

      // Updates the timestamp to use for metadata.
      opened_array_->metadata().reset(timestamp_end_opened_at());
    } else {
      throw ArrayException("Cannot open array; Unsupported query type.");
    }
  } catch (std::exception& e) {
    set_array_closed();
    return LOG_STATUS(Status_ArrayError(e.what()));
  }

  is_opening_or_closing_ = false;
  return Status::Ok();
}

Status Array::close() {
  Status st;
  // Check if array is open
  if (!is_open()) {
    // If array is not open treat this as a no-op
    // This keeps existing behavior from TileDB 2.6 and older
    return Status::Ok();
  }

  try {
    set_array_closed();

    if (remote_) {
      // Update array metadata for write queries if metadata was written by the
      // user
      if ((query_type_ == QueryType::WRITE ||
           query_type_ == QueryType::MODIFY_EXCLUSIVE) &&
          opened_array_->metadata().num() > 0) {
        // Set metadata loaded to be true so when serialization fetchs the
        // metadata it won't trigger a deadlock
        set_metadata_loaded(true);
        auto rest_client = resources_.rest_client();
        if (rest_client == nullptr) {
          throw ArrayException(
              "Error closing array; remote array with no REST client.");
        }
        throw_if_not_ok(rest_client->post_array_metadata_to_rest(
            array_uri_,
            array_dir_timestamp_start_,
            array_dir_timestamp_end_,
            this));
      }
    } else {
      if (query_type_ == QueryType::WRITE ||
          query_type_ == QueryType::MODIFY_EXCLUSIVE) {
        opened_array_->metadata().store(
            resources_, array_uri_, *encryption_key());
      } else if (
          query_type_ != QueryType::READ && query_type_ != QueryType::DELETE &&
          query_type_ != QueryType::UPDATE) {
        throw std::logic_error("Error closing array; Unsupported query type.");
      }
    }

    opened_array_.reset();
  } catch (std::exception& e) {
    is_opening_or_closing_ = false;
    throw ArrayException(e.what());
  }

  is_opening_or_closing_ = false;
  return Status::Ok();
}

void Array::delete_fragments(
    ContextResources& resources,
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    std::optional<ArrayDirectory> array_dir) {
  // Get the fragment URIs to be deleted
  if (array_dir == std::nullopt) {
    array_dir = ArrayDirectory(resources, uri, timestamp_start, timestamp_end);
  }
  auto filtered_fragment_uris = array_dir->filtered_fragment_uris(true);
  const auto& fragment_uris = filtered_fragment_uris.fragment_uris();

  // Retrieve commit uris to delete and ignore
  std::vector<URI> commit_uris_to_delete;
  std::vector<URI> commit_uris_to_ignore;
  for (auto& fragment : fragment_uris) {
    auto commit_uri = array_dir->get_commit_uri(fragment.uri_);
    commit_uris_to_delete.emplace_back(commit_uri);
    if (array_dir->consolidated_commit_uris_set().count(commit_uri.c_str()) !=
        0) {
      commit_uris_to_ignore.emplace_back(commit_uri);
    }
  }

  // Write ignore file
  if (commit_uris_to_ignore.size() != 0) {
    array_dir->write_commit_ignore_file(commit_uris_to_ignore);
  }

  // Delete fragments and commits
  auto vfs = &(resources.vfs());
  throw_if_not_ok(parallel_for(
      &resources.compute_tp(), 0, fragment_uris.size(), [&](size_t i) {
        throw_if_not_ok(vfs->remove_dir(fragment_uris[i].uri_));
        bool is_file = false;
        throw_if_not_ok(vfs->is_file(commit_uris_to_delete[i], &is_file));
        if (is_file) {
          throw_if_not_ok(vfs->remove_file(commit_uris_to_delete[i]));
        }
        return Status::Ok();
      }));
}

void Array::delete_fragments(
    const URI& uri, uint64_t timestamp_start, uint64_t timestamp_end) {
  // Check that data deletion is allowed
  ensure_array_is_valid_for_delete(uri);

  // Delete fragments
  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw ArrayException(
          "[delete_fragments] Remote array with no REST client.");
    }
    rest_client->post_delete_fragments_to_rest(
        uri, this, timestamp_start, timestamp_end);
  } else {
    Array::delete_fragments(resources_, uri, timestamp_start, timestamp_end);
  }
}

void Array::delete_array(ContextResources& resources, const URI& uri) {
  auto& vfs = resources.vfs();
  auto array_dir =
      ArrayDirectory(resources, uri, 0, std::numeric_limits<uint64_t>::max());

  // Delete fragments and commits
  Array::delete_fragments(
      resources, uri, 0, std::numeric_limits<uint64_t>::max(), array_dir);

  // Delete array metadata, fragment metadata and array schema files
  // Note: metadata files may not be present, try to delete anyway
  vfs.remove_files(&resources.compute_tp(), array_dir.array_meta_uris());
  vfs.remove_files(&resources.compute_tp(), array_dir.fragment_meta_uris());
  vfs.remove_files(&resources.compute_tp(), array_dir.array_schema_uris());

  // Delete all tiledb child directories
  // Note: using vfs.ls() here could delete user data
  std::vector<URI> dirs;
  auto parent_dir = array_dir.uri().c_str();
  for (auto array_dir_name : constants::array_dir_names) {
    dirs.emplace_back(URI(parent_dir + array_dir_name));
  }
  vfs.remove_dirs(&resources.compute_tp(), dirs);
  vfs.remove_dir_if_empty(array_dir.uri());
}

void Array::delete_array(const URI& uri) {
  // Check that data deletion is allowed
  ensure_array_is_valid_for_delete(uri);

  // Delete array data
  if (uri.is_tiledb()) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw ArrayException("[delete_array] Remote array with no REST client.");
    }
    rest_client->delete_array_from_rest(uri);
  } else {
    Array::delete_array(resources_, uri);
  }

  // Close the array
  throw_if_not_ok(this->close());
}

void Array::delete_fragments_list(const std::vector<URI>& fragment_uris) {
  // Check that data deletion is allowed
  ensure_array_is_valid_for_delete(array_uri_);

  // Delete fragments_list
  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw ArrayException(
          "[delete_fragments_list] Remote array with no REST client.");
    }
    rest_client->post_delete_fragments_list_to_rest(
        array_uri_, this, fragment_uris);
  } else {
    auto array_dir = ArrayDirectory(
        resources_, array_uri_, 0, std::numeric_limits<uint64_t>::max());
    array_dir.delete_fragments_list(fragment_uris);
  }
}

void Array::encryption_type(
    ContextResources& resources,
    const URI& uri,
    EncryptionType* encryption_type) {
  if (uri.is_invalid()) {
    throw ArrayException("[encryption_type] Invalid array URI");
  }

  if (uri.is_tiledb()) {
    throw std::invalid_argument(
        "Getting the encryption type of remote arrays is not supported.");
  }

  // Load URIs from the array directory
  optional<tiledb::sm::ArrayDirectory> array_dir;
  array_dir.emplace(
      resources,
      uri,
      0,
      UINT64_MAX,
      tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY);

  // Read tile header
  auto&& header = GenericTileIO::read_generic_tile_header(
      resources, array_dir->latest_array_schema_uri(), 0);
  *encryption_type = static_cast<EncryptionType>(header.encryption_type);
}

shared_ptr<const Enumeration> Array::get_enumeration(
    const std::string& enumeration_name) {
  return get_enumerations({enumeration_name})[0];
}

std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
Array::get_enumerations_all_schemas() {
  if (!is_open_) {
    throw ArrayException("Unable to load enumerations; Array is not open.");
  } else if (!use_refactored_array_open()) {
    throw ArrayException(
        "Unable to load enumerations for all array schemas; The array must "
        "be opened using `rest.use_refactored_array_open=true`");
  }
  std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
      ret;

  // Check if all enumerations are already loaded.
  bool all_enmrs_loaded = true;
  for (const auto& schema : array_schemas_all()) {
    if (schema.second->get_enumeration_names().size() !=
        schema.second->get_loaded_enumeration_names().size()) {
      all_enmrs_loaded = false;
      break;
    }
    ret[schema.first] = schema.second->get_loaded_enumerations();
  }

  if (!all_enmrs_loaded) {
    ret.clear();

    if (remote_) {
      auto rest_client = resources_.rest_client();
      if (rest_client == nullptr) {
        throw ArrayException(
            "Error loading enumerations; Remote array with no REST client.");
      }

      // Pass an empty list of enumeration names. REST will use timestamps to
      // load all enumerations on all schemas for the array within that range.
      ret = rest_client->post_enumerations_from_rest(
          array_uri_,
          array_dir_timestamp_start_,
          array_dir_timestamp_end_,
          config_,
          array_schema_latest(),
          {},
          memory_tracker_);
    } else {
      auto latest_schema = opened_array_->array_schema_latest_ptr();
      for (const auto& schema : array_schemas_all()) {
        auto enmrs = ret[schema.first];
        enmrs.reserve(schema.second->get_enumeration_names().size());

        std::unordered_set<std::string> enmrs_to_load;
        auto enumeration_names = schema.second->get_enumeration_names();
        // Dedupe requested names and filter out anything already loaded.
        for (auto& enmr_name : enumeration_names) {
          if (schema.second->is_enumeration_loaded(enmr_name)) {
            enmrs.push_back(schema.second->get_enumeration(enmr_name));
            continue;
          }
          enmrs_to_load.insert(enmr_name);
        }

        // Create a vector of paths to be loaded.
        std::vector<std::string> paths_to_load;
        for (auto& enmr_name : enmrs_to_load) {
          auto path = schema.second->get_enumeration_path_name(enmr_name);
          paths_to_load.push_back(path);
        }

        // Load the enumerations from storage
        auto loaded = array_directory().load_enumerations_from_paths(
            paths_to_load, *encryption_key(), memory_tracker_);

        enmrs.insert(enmrs.begin(), loaded.begin(), loaded.end());
        ret[schema.first] = enmrs;
      }
    }
  }

  // Store the loaded enumerations into the schemas.
  for (const auto& [schema_name, enmrs] : ret) {
    // This case will only be hit for remote arrays if the client evolves the
    // schema and does not reopen the array before loading all enumerations.
    if (!array_schemas_all().contains(schema_name)) {
      throw ArrayException(
          "Array opened using timestamp range (" +
          std::to_string(array_dir_timestamp_start_) + ", " +
          std::to_string(array_dir_timestamp_end_) +
          ") has no loaded schema named '" + schema_name +
          "'; If the array was recently evolved be sure to reopen it after "
          "applying the evolution.");
    }

    auto schema = array_schemas_all().at(schema_name);
    for (const auto& enmr : enmrs) {
      if (!schema->is_enumeration_loaded(enmr->name())) {
        schema->store_enumeration(enmr);
      }
    }
  }

  return ret;
}

std::vector<shared_ptr<const Enumeration>> Array::get_enumerations(
    const std::vector<std::string>& enumeration_names) {
  if (!is_open_) {
    throw ArrayException("Unable to load enumerations; Array is not open.");
  }

  // Dedupe requested names and filter out anything already loaded.
  std::unordered_set<std::string> enmrs_to_load;
  for (auto& enmr_name : enumeration_names) {
    if (array_schema_latest().is_enumeration_loaded(enmr_name)) {
      continue;
    }
    enmrs_to_load.insert(enmr_name);
  }

  // Only attempt to load enumerations if we have at least one Enumeration
  // to load.
  if (enmrs_to_load.size() > 0) {
    std::vector<shared_ptr<const Enumeration>> loaded;

    if (remote_) {
      auto rest_client = resources_.rest_client();
      if (rest_client == nullptr) {
        throw ArrayException(
            "Error loading enumerations; Remote array with no REST client.");
      }

      std::vector<std::string> names_to_load;
      for (auto& enmr_name : enmrs_to_load) {
        names_to_load.push_back(enmr_name);
      }

      loaded = rest_client->post_enumerations_from_rest(
          array_uri_,
          array_dir_timestamp_start_,
          array_dir_timestamp_end_,
          config_,
          array_schema_latest(),
          names_to_load,
          memory_tracker_)[array_schema_latest().name()];
    } else {
      // Create a vector of paths to be loaded.
      std::vector<std::string> paths_to_load;
      for (auto& enmr_name : enmrs_to_load) {
        auto path = array_schema_latest().get_enumeration_path_name(enmr_name);
        paths_to_load.push_back(path);
      }

      // Load the enumerations from storage
      loaded = array_directory().load_enumerations_from_paths(
          paths_to_load, *encryption_key(), memory_tracker_);
    }

    // Store the loaded enumerations in the latest schema
    auto schema = opened_array_->array_schema_latest_ptr();
    for (auto& enmr : loaded) {
      if (!schema->is_enumeration_loaded(enmr->name())) {
        schema->store_enumeration(enmr);
      }
    }
  }

  // Return the requested list of enumerations
  std::vector<shared_ptr<const Enumeration>> ret(enumeration_names.size());
  for (size_t i = 0; i < enumeration_names.size(); i++) {
    ret[i] = array_schema_latest().get_enumeration(enumeration_names[i]);
  }
  return ret;
}

void Array::load_all_enumerations(bool all_schemas) {
  if (!is_open_) {
    throw ArrayException("Unable to load all enumerations; Array is not open.");
  }
  // Load all enumerations, discarding the returned list of loaded enumerations.
  if (all_schemas) {
    // Unless we are using array open V3, Array::array_schemas_all_ will not be
    // initialized. We throw an exception since this is required to store the
    // loaded enumerations.
    if (!use_refactored_array_open()) {
      throw ArrayException(
          "Unable to load enumerations for all array schemas; The array must "
          "be opened using `rest.use_refactored_array_open=true`");
    }

    get_enumerations_all_schemas();
  } else {
    get_enumerations(array_schema_latest().get_enumeration_names());
  }
}

bool Array::is_empty() const {
  return opened_array_->fragment_metadata().empty();
}

bool Array::is_open() {
  std::lock_guard<std::mutex> lock(mtx_);
  return is_open_;
}

bool Array::is_remote() const {
  return remote_;
}

tuple<Status, optional<shared_ptr<ArraySchema>>> Array::get_array_schema()
    const {
  // Error if the array is not open
  if (!is_open_)
    return {
        LOG_STATUS(
            Status_ArrayError("Cannot get array schema; Array is not open")),
        nullopt};

  return {Status::Ok(), opened_array_->array_schema_latest_ptr()};
}

QueryType Array::get_query_type() const {
  return query_type_;
}

Status Array::reopen() {
  // Note: Array will only reopen for reads. This is why we are checking the
  // timestamp for the array directory and not new components. This needs to be
  // updated if non-read reopens are allowed.
  if (!user_set_timestamp_end_.has_value() ||
      user_set_timestamp_end_.value() == array_dir_timestamp_end_) {
    // The user has not set `timestamp_end_` since it was last opened or set it
    // to use the current timestamp. In this scenario, re-open at the current
    // timestamp.
    return reopen(array_dir_timestamp_start_, utils::time::timestamp_now_ms());
  } else {
    // The user has changed the end timestamp. Reopen at the user set timestamp.
    return reopen(array_dir_timestamp_start_, user_set_timestamp_end_.value());
  }
}

Status Array::reopen(uint64_t timestamp_start, uint64_t timestamp_end) {
  auto timer = resources_.stats().start_timer("array_reopen");
  // Check the array was opened already in READ mode.
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot reopen array; Array is not open"));
  }
  if (query_type_ != QueryType::READ) {
    return LOG_STATUS(Status_ArrayError(
        "Cannot reopen array; Array was not opened in read mode"));
  }

  // Update the user set timestamp and the timestamp range to pass to the array
  // directory.
  if (timestamp_end == UINT64_MAX) {
    user_set_timestamp_end_ = nullopt;
    array_dir_timestamp_end_ = utils::time::timestamp_now_ms();

  } else {
    user_set_timestamp_end_ = timestamp_end;
    array_dir_timestamp_end_ = timestamp_end;
  }
  array_dir_timestamp_start_ = timestamp_start;

  // Reopen metadata.
  auto key = opened_array_->encryption_key();
  opened_array_ = make_shared<OpenedArray>(
      HERE(),
      resources_,
      memory_tracker_,
      array_uri_,
      key->encryption_type(),
      key->key().data(),
      static_cast<uint32_t>(key->key().size()),
      this->timestamp_start(),
      timestamp_end_opened_at(),
      is_remote());

  // Use open to reopen a remote array.
  if (remote_) {
    try {
      set_array_closed();
    } catch (std::exception& e) {
      is_opening_or_closing_ = false;
      throw ArrayException(e.what());
    }
    is_opening_or_closing_ = false;

    return open(
        query_type_,
        encryption_key()->encryption_type(),
        encryption_key()->key().data(),
        encryption_key()->key().size());
  }

  // Reload the array directory in READ mode (reopen only supports reads).
  try {
    {
      auto timer_se = resources_.stats().start_timer("array_reopen_directory");
      set_array_directory(ArrayDirectory(
          resources_,
          array_uri_,
          array_dir_timestamp_start_,
          array_dir_timestamp_end_,
          query_type_ == QueryType::READ ? ArrayDirectoryMode::READ :
                                           ArrayDirectoryMode::SCHEMA_ONLY));
    }
  } catch (const std::logic_error& le) {
    return LOG_STATUS(Status_ArrayDirectoryError(le.what()));
  }

  // Reopen the array and update private variables.
  auto&& [array_schema_latest, array_schemas_all, fragment_metadata] =
      open_for_reads();
  set_array_schema_latest(array_schema_latest);
  set_array_schemas_all(std::move(array_schemas_all));
  set_fragment_metadata(std::move(fragment_metadata));

  return Status::Ok();
}

void Array::set_config(Config config) {
  if (is_open()) {
    throw ArrayException("[set_config] Cannot set a config on an open array");
  }
  config_.inherit(config);
}

void Array::delete_metadata(const char* key) {
  // Check if array is open
  if (!is_open_) {
    throw ArrayException("Cannot delete metadata. Array is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE &&
      query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    throw ArrayException(
        "Cannot delete metadata. Array was not opened in write or "
        "modify_exclusive mode");
  }

  // Check if key is null
  if (key == nullptr) {
    throw ArrayException("Cannot delete metadata. Key cannot be null");
  }

  opened_array_->metadata().del(key);
}

void Array::put_metadata(
    const char* key,
    Datatype value_type,
    uint32_t value_num,
    const void* value) {
  // Check if array is open
  if (!is_open_) {
    throw ArrayException("Cannot put metadata; Array is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE &&
      query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    throw ArrayException(
        "Cannot put metadata; Array was not opened in write or "
        "modify_exclusive mode");
  }

  // Check if key is null
  if (key == nullptr) {
    throw ArrayException("Cannot put metadata; Key cannot be null");
  }

  // Check if value type is ANY
  if (value_type == Datatype::ANY) {
    throw ArrayException("Cannot put metadata; Value type cannot be ANY");
  }

  opened_array_->metadata().put(key, value_type, value_num, value);
}

void Array::get_metadata(
    const char* key,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if array is open
  if (!is_open_) {
    throw ArrayException("Cannot get metadata; Array is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw ArrayException(
        "Cannot get metadata; Array was not opened in read mode");
  }

  // Check if key is null
  if (key == nullptr) {
    throw ArrayException("Cannot get metadata; Key cannot be null");
  }

  // Load array metadata, if not loaded yet
  if (!metadata_loaded()) {
    throw_if_not_ok(load_metadata());
  }

  opened_array_->metadata().get(key, value_type, value_num, value);
}

void Array::get_metadata(
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if array is open
  if (!is_open_) {
    throw ArrayException("Cannot get metadata; Array is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw ArrayException(
        "Cannot get metadata; Array was not opened in read mode");
  }

  // Load array metadata, if not loaded yet
  if (!metadata_loaded()) {
    throw_if_not_ok(load_metadata());
  }

  opened_array_->metadata().get(
      index, key, key_len, value_type, value_num, value);
}

uint64_t Array::metadata_num() {
  // Check if array is open
  if (!is_open_) {
    throw ArrayException("Cannot get number of metadata; Array is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw ArrayException(
        "Cannot get number of metadata; Array was not opened in read mode");
  }

  // Load array metadata, if not loaded yet
  if (!metadata_loaded()) {
    throw_if_not_ok(load_metadata());
  }

  return opened_array_->metadata().num();
}

std::optional<Datatype> Array::metadata_type(const char* key) {
  // Check if array is open
  if (!is_open_) {
    throw ArrayException("Cannot get metadata; Array is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw ArrayException(
        "Cannot get metadata; Array was not opened in read mode");
  }

  // Check if key is null
  if (key == nullptr) {
    throw ArrayException("Cannot get metadata; Key cannot be null");
  }

  // Load array metadata, if not loaded yet
  if (!metadata_loaded()) {
    throw_if_not_ok(load_metadata());
  }

  return opened_array_->metadata().metadata_type(key);
}

Metadata* Array::unsafe_metadata() {
  return &opened_array_->metadata();
}

Metadata& Array::metadata() {
  // Load array metadata for array opened for reads, if not loaded yet
  if (query_type_ == QueryType::READ && !metadata_loaded()) {
    throw_if_not_ok(load_metadata());
  }

  return opened_array_->metadata();
}

const NDRange Array::non_empty_domain() {
  if (!non_empty_domain_computed()) {
    throw_if_not_ok(compute_non_empty_domain());
  }
  return loaded_non_empty_domain();
}

void Array::non_empty_domain(NDRange* domain, bool* is_empty) {
  if (domain == nullptr) {
    throw std::invalid_argument("[non_empty_domain] Domain object is null");
  }
  if (is_empty == nullptr) {
    throw std::invalid_argument("[non_empty_domain] is_empty* is null");
  }

  if (!is_open()) {
    throw ArrayException("[non_empty_domain] Array is not open");
  }

  QueryType query_type{get_query_type()};
  if (query_type != QueryType::READ) {
    throw ArrayException("[non_empty_domain] Array not opened for reads");
  }

  *domain = non_empty_domain();
  *is_empty = domain->empty();
}

void Array::non_empty_domain(void* domain, bool* is_empty) {
  if (domain == nullptr) {
    throw std::invalid_argument("[non_empty_domain] Domain object is null");
  }
  if (is_empty == nullptr) {
    throw std::invalid_argument("[non_empty_domain] is_empty* is null");
  }

  if (!array_schema_latest().domain().all_dims_same_type()) {
    throw ArrayException(
        "[non_empty_domain] Function non-applicable to arrays with "
        "heterogenous dimensions");
  }

  if (!array_schema_latest().domain().all_dims_fixed()) {
    throw ArrayException(
        "[non_empty_domain] Function non-applicable to arrays with "
        "variable-sized dimensions");
  }

  NDRange dom;
  non_empty_domain(&dom, is_empty);
  if (*is_empty) {
    return;
  }

  const auto& array_schema = array_schema_latest();
  auto dim_num = array_schema.dim_num();
  auto domain_c = (unsigned char*)domain;
  uint64_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    std::memcpy(&domain_c[offset], dom[d].data(), dom[d].size());
    offset += dom[d].size();
  }
}

void Array::non_empty_domain_from_index(
    unsigned idx, void* domain, bool* is_empty) {
  if (!is_open_) {
    throw ArrayException("[non_empty_domain_from_index] Array is not open");
  }
  // For easy reference
  const auto& array_schema = array_schema_latest();
  auto& array_domain{array_schema.domain()};

  // Sanity checks
  if (idx >= array_schema.dim_num()) {
    throw ArrayException(
        "Cannot get non-empty domain; Invalid dimension index");
  }
  if (array_domain.dimension_ptr(idx)->var_size()) {
    throw ArrayException(
        "Cannot get non-empty domain; Dimension '" +
        array_domain.dimension_ptr(idx)->name() + "' is var-sized");
  }

  NDRange dom;
  non_empty_domain(&dom, is_empty);
  if (*is_empty) {
    return;
  }

  std::memcpy(domain, dom[idx].data(), dom[idx].size());
}

void Array::non_empty_domain_var_size_from_index(
    unsigned idx, uint64_t* start_size, uint64_t* end_size, bool* is_empty) {
  // For easy reference
  const auto& array_schema = array_schema_latest();
  auto& array_domain{array_schema.domain()};

  // Sanity checks
  if (idx >= array_schema.dim_num()) {
    throw ArrayException(
        "Cannot get non-empty domain; Invalid dimension index");
  }
  if (!array_domain.dimension_ptr(idx)->var_size()) {
    throw ArrayException(
        "Cannot get non-empty domain; Dimension '" +
        array_domain.dimension_ptr(idx)->name() + "' is fixed-sized");
  }

  NDRange dom;
  non_empty_domain(&dom, is_empty);
  if (*is_empty) {
    *start_size = 0;
    *end_size = 0;
    return;
  }

  *start_size = dom[idx].start_size();
  *end_size = dom[idx].end_size();
}

void Array::non_empty_domain_var_from_index(
    unsigned idx, void* start, void* end, bool* is_empty) {
  // For easy reference
  const auto& array_schema = array_schema_latest();
  auto& array_domain{array_schema.domain()};

  // Sanity checks
  if (idx >= array_schema.dim_num()) {
    throw ArrayException(
        "Cannot get non-empty domain; Invalid dimension index");
  }
  if (!array_domain.dimension_ptr(idx)->var_size()) {
    throw ArrayException(
        "Cannot get non-empty domain; Dimension '" +
        array_domain.dimension_ptr(idx)->name() + "' is fixed-sized");
  }

  NDRange dom;
  non_empty_domain(&dom, is_empty);
  if (*is_empty) {
    return;
  }

  auto start_str = dom[idx].start_str();
  std::memcpy(start, start_str.data(), start_str.size());
  auto end_str = dom[idx].end_str();
  std::memcpy(end, end_str.data(), end_str.size());
}

void Array::non_empty_domain_from_name(
    std::string_view field_name, void* domain, bool* is_empty) {
  // Sanity check
  if (field_name.data() == nullptr) {
    throw std::invalid_argument(
        "[non_empty_domain_from_name] Invalid dimension name");
  }

  // Check if array is open - must be open for reads
  if (!is_open_) {
    throw ArrayException("[non_empty_domain_from_name] Array is not open");
  }

  NDRange dom;
  non_empty_domain(&dom, is_empty);

  const auto& array_schema = array_schema_latest();
  auto& array_domain{array_schema.domain()};
  auto dim_num = array_schema.dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema.dimension_ptr(d)->name()};
    if (field_name == dim_name) {
      // Sanity check
      if (array_domain.dimension_ptr(d)->var_size()) {
        throw ArrayException(
            "Cannot get non-empty domain; Dimension '" + dim_name +
            "' is variable-sized");
      }
      if (!*is_empty) {
        std::memcpy(domain, dom[d].data(), dom[d].size());
      }
      return;
    }
  }

  throw ArrayException(
      "Cannot get non-empty domain; Dimension name '" +
      std::string(field_name) + "' does not exist");
}

void Array::non_empty_domain_var_size_from_name(
    std::string_view field_name,
    uint64_t* start_size,
    uint64_t* end_size,
    bool* is_empty) {
  // Sanity check
  if (field_name.data() == nullptr) {
    throw std::invalid_argument("[non_empty_domain] Invalid dimension name");
  }

  NDRange dom;
  non_empty_domain(&dom, is_empty);

  const auto& array_schema = array_schema_latest();
  auto& array_domain{array_schema.domain()};
  auto dim_num = array_schema.dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema.dimension_ptr(d)->name()};
    if (field_name == dim_name) {
      // Sanity check
      if (!array_domain.dimension_ptr(d)->var_size()) {
        throw ArrayException(
            "Cannot get non-empty domain; Dimension '" + dim_name +
            "' is fixed-sized");
      }
      if (*is_empty) {
        *start_size = 0;
        *end_size = 0;
      } else {
        *start_size = dom[d].start_size();
        *end_size = dom[d].end_size();
      }
      return;
    }
  }

  throw ArrayException(
      "Cannot get non-empty domain; Dimension name '" +
      std::string(field_name) + "' does not exist");
}

void Array::non_empty_domain_var_from_name(
    std::string_view field_name, void* start, void* end, bool* is_empty) {
  // Sanity check
  if (field_name.data() == nullptr) {
    throw std::invalid_argument("[non_empty_domain] Invalid dimension name");
  }

  NDRange dom;
  non_empty_domain(&dom, is_empty);

  const auto& array_schema = array_schema_latest();
  auto& array_domain{array_schema.domain()};
  auto dim_num = array_schema.dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema.dimension_ptr(d)->name()};
    if (field_name == dim_name) {
      // Sanity check
      if (!array_domain.dimension_ptr(d)->var_size()) {
        throw ArrayException(
            "Cannot get non-empty domain; Dimension '" + dim_name +
            "' is fixed-sized");
      }
      if (!*is_empty) {
        auto start_str = dom[d].start_str();
        std::memcpy(start, start_str.data(), start_str.size());
        auto end_str = dom[d].end_str();
        std::memcpy(end, end_str.data(), end_str.size());
      }
      return;
    }
  }

  throw ArrayException(
      "Cannot get non-empty domain; Dimension name '" +
      std::string(field_name) + "' does not exist");
}

bool Array::serialize_non_empty_domain() const {
  auto found = false;
  auto serialize_ned_array_open = false;
  auto status = config_.get<bool>(
      "rest.load_non_empty_domain_on_array_open",
      &serialize_ned_array_open,
      &found);
  if (!status.ok() || !found) {
    throw std::runtime_error(
        "Cannot get rest.load_non_empty_domain_on_array_open configuration "
        "option from config");
  }

  return serialize_ned_array_open;
}

bool Array::serialize_enumerations() const {
  return config_.get<bool>(
             "rest.load_enumerations_on_array_open", Config::must_find) ||
         config_.get<bool>(
             "rest.load_enumerations_on_array_open_all_schemas",
             Config::must_find);
}

bool Array::serialize_metadata() const {
  auto found = false;
  auto serialize_metadata_array_open = false;
  auto status = config_.get<bool>(
      "rest.load_metadata_on_array_open",
      &serialize_metadata_array_open,
      &found);
  if (!status.ok() || !found) {
    throw std::runtime_error(
        "Cannot get rest.load_metadata_on_array_open configuration option from "
        "config");
  }

  return serialize_metadata_array_open;
}

void Array::set_timestamps(
    uint64_t timestamp_start, uint64_t timestamp_end, bool set_current_time) {
  array_dir_timestamp_start_ = timestamp_start;
  array_dir_timestamp_end_ = (set_current_time && timestamp_end == UINT64_MAX) ?
                                 utils::time::timestamp_now_ms() :
                                 timestamp_end;
  if (timestamp_end == 0 || timestamp_end == UINT64_MAX) {
    new_component_timestamp_ = nullopt;
  } else {
    new_component_timestamp_ = timestamp_end;
  }
}

bool Array::use_refactored_array_open() const {
  auto found = false;
  auto refactored_array_open = false;
  auto status = config_.get<bool>(
      "rest.use_refactored_array_open", &refactored_array_open, &found);
  if (!status.ok() || !found) {
    throw std::runtime_error(
        "Cannot get rest.use_refactored_array_open configuration option from "
        "config");
  }

  return refactored_array_open || use_refactored_query_submit();
}

bool Array::use_refactored_query_submit() const {
  auto found = false;
  auto refactored_query_submit = false;
  auto status = config_.get<bool>(
      "rest.use_refactored_array_open_and_query_submit",
      &refactored_query_submit,
      &found);
  if (!status.ok() || !found) {
    throw std::runtime_error(
        "Cannot get rest.use_refactored_array_open_and_query_submit "
        "configuration option from config");
  }

  return refactored_query_submit;
}

std::unordered_map<std::string, uint64_t> Array::get_average_var_cell_sizes()
    const {
  std::unordered_map<std::string, uint64_t> ret;

  // Keep the current opened array alive for the duration of this call.
  auto opened_array = opened_array_;
  auto& fragment_metadata = opened_array->fragment_metadata();
  auto& array_schema_latest = opened_array->array_schema_latest();

  // Find the names of the var-sized dimensions or attributes.
  std::vector<std::string> var_names;

  // Start with dimensions.
  for (unsigned d = 0; d < array_schema_latest.dim_num(); d++) {
    auto dim = array_schema_latest.dimension_ptr(d);
    if (dim->var_size()) {
      var_names.emplace_back(dim->name());
      ret.emplace(dim->name(), 0);
    }
  }

  // Now attributes.
  for (auto& attr : array_schema_latest.attributes()) {
    if (attr->var_size()) {
      var_names.emplace_back(attr->name());
      ret.emplace(attr->name(), 0);
    }
  }

  // Load all metadata for tile var sizes among fragments.
  for (const auto& var_name : var_names) {
    throw_if_not_ok(parallel_for(
        &resources_.compute_tp(),
        0,
        fragment_metadata.size(),
        [&](const unsigned f) {
          // Gracefully skip loading tile sizes for attributes added in schema
          // evolution that do not exists in this fragment.
          const auto& schema = fragment_metadata[f]->array_schema();
          if (!schema->is_field(var_name)) {
            return Status::Ok();
          }

          fragment_metadata[f]->loaded_metadata()->load_tile_var_sizes(
              *encryption_key(), var_name);
          return Status::Ok();
        }));
  }

  // Now compute for each var size names, the average cell size.
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, var_names.size(), [&](const uint64_t n) {
        uint64_t total_size = 0;
        uint64_t cell_num = 0;
        auto& var_name = var_names[n];

        // Sum the total tile size and cell num for each fragments.
        for (unsigned f = 0; f < fragment_metadata.size(); f++) {
          // Skip computation for fields that don't exist for a particular
          // fragment.
          const auto& schema = fragment_metadata[f]->array_schema();
          if (!schema->is_field(var_name)) {
            continue;
          }

          // Go through all tiles.
          for (uint64_t t = 0; t < fragment_metadata[f]->tile_num(); t++) {
            total_size +=
                fragment_metadata[f]->loaded_metadata()->tile_var_size(
                    var_name, t);
            cell_num += fragment_metadata[f]->cell_num(t);
          }
        }

        uint64_t average_cell_size = total_size / cell_num;
        ret[var_name] = std::max<uint64_t>(average_cell_size, 1);

        return Status::Ok();
      }));

  return ret;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

tuple<
    shared_ptr<ArraySchema>,
    std::unordered_map<std::string, shared_ptr<ArraySchema>>,
    std::vector<shared_ptr<FragmentMetadata>>>
Array::open_for_reads() {
  auto timer_se = resources_.stats().start_timer(
      "array_open_read_load_schemas_and_fragment_meta");
  auto result = FragmentInfo::load_array_schemas_and_fragment_metadata(
      resources_, array_directory(), memory_tracker(), *encryption_key());

  auto version = std::get<0>(result)->version();
  ensure_supported_schema_version_for_read(version);

  return result;
}

tuple<
    shared_ptr<ArraySchema>,
    std::unordered_map<std::string, shared_ptr<ArraySchema>>>
Array::open_for_reads_without_fragments() {
  auto timer_se = resources_.stats().start_timer(
      "array_open_read_without_fragments_load_schemas");

  // Load array schemas
  auto result =
      array_directory().load_array_schemas(*encryption_key(), memory_tracker_);

  auto version = std::get<0>(result)->version();
  ensure_supported_schema_version_for_read(version);

  return result;
}

tuple<
    shared_ptr<ArraySchema>,
    std::unordered_map<std::string, shared_ptr<ArraySchema>>>
Array::open_for_writes() {
  auto timer_se =
      resources_.stats().start_timer("array_open_write_load_schemas");
  // Checks
  if (!resources_.vfs().supports_uri_scheme(array_uri_)) {
    throw ArrayException("Cannot open array; URI scheme unsupported.");
  }

  // Load array schemas
  auto&& [array_schema_latest, array_schemas_all] =
      array_directory().load_array_schemas(*encryption_key(), memory_tracker_);

  // If building experimentally, this library should not be able to
  // write to newer-versioned or older-versioned arrays
  // Else, this library should not be able to write to newer-versioned arrays
  // (but it is ok to write to older arrays)
  auto version = array_schema_latest->version();
  if constexpr (is_experimental_build) {
    if (version != constants::format_version) {
      throw ArrayException(
          "Cannot open array for writes; Array format version (" +
          std::to_string(version) + ") is not the library format version (" +
          std::to_string(constants::format_version) + ")");
    }
  } else {
    if (version > constants::format_version) {
      throw ArrayException(
          "Cannot open array for writes; Array format version (" +
          std::to_string(version) + ") is newer than library format version (" +
          std::to_string(constants::format_version) + ")");
    }
  }

  return {array_schema_latest, array_schemas_all};
}

void Array::do_load_metadata() {
  if (!array_directory().loaded()) {
    throw ArrayException(
        "Cannot load metadata; array directory is not loaded.");
  }
  auto timer_se = resources_.stats().start_timer("sm_load_array_metadata");

  // Determine which array metadata to load
  const auto& array_metadata_to_load = array_directory().array_meta_uris();

  auto metadata_num = array_metadata_to_load.size();
  std::vector<shared_ptr<Tile>> metadata_tiles(metadata_num);
  throw_if_not_ok(
      parallel_for(&resources_.compute_tp(), 0, metadata_num, [&](size_t m) {
        const auto& uri = array_metadata_to_load[m].uri_;

        metadata_tiles[m] = GenericTileIO::load(
            resources_, uri, 0, *encryption_key(), memory_tracker_);

        return Status::Ok();
      }));

  // Compute array metadata size for the statistics
  uint64_t meta_size = 0;
  for (const auto& t : metadata_tiles) {
    meta_size += t->size();
  }
  resources_.stats().add_counter("read_array_meta_size", meta_size);

  opened_array_->metadata() = Metadata::deserialize(metadata_tiles);

  // Sets the loaded metadata URIs
  opened_array_->metadata().set_loaded_metadata_uris(array_metadata_to_load);
}

Status Array::load_metadata() {
  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      return LOG_STATUS(Status_ArrayError(
          "Cannot load metadata; remote array with no REST client."));
    }
    RETURN_NOT_OK(rest_client->get_array_metadata_from_rest(
        array_uri_,
        array_dir_timestamp_start_,
        timestamp_end_opened_at(),
        this));
  } else {
    do_load_metadata();
  }
  set_metadata_loaded(true);
  return Status::Ok();
}

Status Array::load_remote_non_empty_domain() {
  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      return LOG_STATUS(Status_ArrayError(
          "Cannot load metadata; remote array with no REST client."));
    }
    RETURN_NOT_OK(rest_client->get_array_non_empty_domain(
        this, array_dir_timestamp_start_, timestamp_end_opened_at()));
    set_non_empty_domain_computed(true);
  }
  return Status::Ok();
}

const ArrayDirectory& Array::load_array_directory() {
  if (array_directory().loaded()) {
    return array_directory();
  }

  if (remote_) {
    throw std::logic_error(
        "Loading array directory for remote arrays is not supported");
  }

  auto mode = (query_type_ == QueryType::WRITE ||
               query_type_ == QueryType::MODIFY_EXCLUSIVE) ?
                  ArrayDirectoryMode::SCHEMA_ONLY :
                  ArrayDirectoryMode::READ;

  set_array_directory(ArrayDirectory(
      resources_,
      array_uri_,
      array_dir_timestamp_start_,
      array_dir_timestamp_end_,
      mode));

  return array_directory();
}

void Array::upgrade_version(
    ContextResources& resources,
    const URI& array_uri,
    const Config& override_config) {
  // Check if array exists
  if (!is_array(resources, array_uri)) {
    throw ArrayException(
        "Cannot upgrade array; Array '" + array_uri.to_string() +
        "' does not exist");
  }

  // Load URIs from the array directory
  tiledb::sm::ArrayDirectory array_dir{
      resources,
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
  throw_if_not_ok(st1);
  EncryptionType encryption_type_cfg = etc.value();

  EncryptionKey encryption_key_cfg;
  if (encryption_key_from_cfg.empty()) {
    throw_if_not_ok(
        encryption_key_cfg.set_key(encryption_type_cfg, nullptr, 0));
  } else {
    throw_if_not_ok(encryption_key_cfg.set_key(
        encryption_type_cfg,
        (const void*)encryption_key_from_cfg.c_str(),
        static_cast<uint32_t>(encryption_key_from_cfg.size())));
  }

  auto&& array_schema = array_dir.load_array_schema_latest(
      encryption_key_cfg, resources.ephemeral_memory_tracker());

  if (array_schema->version() < constants::format_version) {
    array_schema->generate_uri();
    array_schema->set_version(constants::format_version);

    // Create array schema directory if necessary
    URI array_schema_dir_uri =
        array_uri.join_path(constants::array_schema_dir_name);
    throw_if_not_ok(resources.vfs().create_dir(array_schema_dir_uri));

    // Store array schema
    store_array_schema(resources, array_schema, encryption_key_cfg);

    // Create commit directory if necessary
    URI array_commit_uri =
        array_uri.join_path(constants::array_commits_dir_name);
    throw_if_not_ok(resources.vfs().create_dir(array_commit_uri));

    // Create fragments directory if necessary
    URI array_fragments_uri =
        array_uri.join_path(constants::array_fragments_dir_name);
    throw_if_not_ok(resources.vfs().create_dir(array_fragments_uri));

    // Create fragment metadata directory if necessary
    URI array_fragment_metadata_uri =
        array_uri.join_path(constants::array_fragment_meta_dir_name);
    throw_if_not_ok(resources.vfs().create_dir(array_fragment_metadata_uri));
  }
}

Status Array::compute_non_empty_domain() {
  // Keep the current opened array alive for the duration of this call.
  auto opened_array = opened_array_;
  auto& fragment_metadata = opened_array->fragment_metadata();
  auto& array_schema_latest = opened_array->array_schema_latest();
  auto& non_empty_domain = opened_array->non_empty_domain();

  if (remote_) {
    RETURN_NOT_OK(load_remote_non_empty_domain());
  } else if (!fragment_metadata.empty()) {
    const auto& frag0_dom = fragment_metadata[0]->non_empty_domain();
    non_empty_domain.assign(frag0_dom.begin(), frag0_dom.end());

    auto metadata_num = fragment_metadata.size();
    for (size_t j = 1; j < metadata_num; ++j) {
      const auto& meta_dom = fragment_metadata[j]->non_empty_domain();
      // Validate that this fragment's non-empty domain is set
      // It should _always_ be set, however we've seen cases where disk
      // corruption or other out-of-band activity can cause the fragment to be
      // corrupt for these cases we want to check to prevent any segfaults
      // later.
      if (!meta_dom.empty()) {
        array_schema_latest.domain().expand_ndrange(
            meta_dom, &non_empty_domain);
      } else {
        // If the fragment's non-empty domain is indeed empty, lets log it so
        // the user gets a message warning that this fragment might be corrupt
        // Note: LOG_STATUS only prints if TileDB is built in verbose mode.
        LOG_STATUS_NO_RETURN_VALUE(Status_ArrayError(
            "Non empty domain unexpectedly empty for fragment: " +
            fragment_metadata[j]->fragment_uri().to_string()));
      }
    }
  }
  opened_array->non_empty_domain_computed() = true;
  return Status::Ok();
}

void Array::set_array_open(const QueryType& query_type) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (is_opening_or_closing_) {
    is_opening_or_closing_ = false;
    throw std::runtime_error(
        "[Array::set_array_open] "
        "May not perform simultaneous open or close operations.");
  }
  is_opening_or_closing_ = true;
  /**
   * Note: there is no danger in passing *this here;
   * only the pointer value is used and nothing is called on the Array objects.
   */
  consistency_sentry_.emplace(
      consistency_controller_.make_sentry(array_uri_, *this, query_type));
  is_open_ = true;
}

void Array::set_serialized_array_open() {
  is_open_ = true;

  opened_array_ = make_shared<OpenedArray>(
      HERE(),
      resources_,
      memory_tracker_,
      array_uri_,
      EncryptionType::NO_ENCRYPTION,
      nullptr,
      0,
      timestamp_start(),
      timestamp_end_opened_at(),
      array_uri_.is_tiledb());
}

void Array::set_query_type(QueryType query_type) {
  query_type_ = query_type;
  if (query_type_ == QueryType::READ) {
    memory_tracker_->set_type(MemoryTrackerType::ARRAY_READ);
  } else {
    memory_tracker_->set_type(MemoryTrackerType::ARRAY_WRITE);
  }
}

void Array::set_array_closed() {
  std::lock_guard<std::mutex> lock(mtx_);

  /* Note: if the opening process is interrupted, the array must be closed. */
  if (is_opening_or_closing_) {
    is_opening_or_closing_ = false;
    if (!is_open_) {
      throw std::runtime_error(
          "[Array::set_array_closed] "
          "May not perform simultaneous open or close operations.");
    }
  } else {
    is_opening_or_closing_ = true;
  }

  /* Note: the Sentry object will also be released upon Array destruction. */
  consistency_sentry_.reset();
  is_open_ = false;
}

void Array::ensure_array_is_valid_for_delete(const URI& uri) {
  // Check that query type is MODIFY_EXCLUSIVE
  if (query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    throw ArrayException(
        "[ensure_array_is_valid_for_delete] "
        "Query type must be MODIFY_EXCLUSIVE to perform a delete.");
  }

  // Check that array is open
  if (!is_open() && !controller().is_open(uri)) {
    throw ArrayException("[ensure_array_is_valid_for_delete] Array is closed");
  }

  // Check that array is not in the process of opening or closing
  if (is_opening_or_closing_) {
    throw ArrayException(
        "[ensure_array_is_valid_for_delete] "
        "May not perform simultaneous open or close operations.");
  }
}

void ensure_supported_schema_version_for_read(format_version_t version) {
  // We do not allow reading from arrays written by newer version of TileDB
  if (version > constants::format_version) {
    throw ArrayException(
        "Cannot open array for reads; Array format version (" +
        std::to_string(version) + ") is newer than library format version (" +
        std::to_string(constants::format_version) + ")");
  }
}

// NB: this is used to implement `tiledb_array_schema_get_enumeration_*`
// but is defined here instead of array_schema to avoid a circular dependency
// (array_directory depends on array_schema).
void load_enumeration_into_schema(
    Context& ctx, const std::string& enmr_name, ArraySchema& array_schema) {
  if (array_schema.is_enumeration_loaded(enmr_name)) {
    return;
  }

  auto tracker = ctx.resources().ephemeral_memory_tracker();

  if (array_schema.array_uri().is_tiledb()) {
    auto rest_client = ctx.resources().rest_client();
    if (rest_client == nullptr) {
      throw ArrayException(
          "Error loading enumerations; Remote array schema with no REST "
          "client.");
    }

    auto ret = rest_client->post_enumerations_from_rest(
        array_schema.array_uri(),
        array_schema.timestamp_start(),
        array_schema.timestamp_end(),
        ctx.resources().config(),
        array_schema,
        {enmr_name},
        tracker);

    // response is a map {schema: [enumerations]}
    // we should be the only schema, and expect only one enumeration
    for (auto enumeration : ret[array_schema.name()]) {
      array_schema.store_enumeration(enumeration);
    }
  } else {
    auto& path = array_schema.get_enumeration_path_name(enmr_name);

    // Create key
    tiledb::sm::EncryptionKey key;
    throw_if_not_ok(
        key.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0));

    // Load URIs from the array directory
    tiledb::sm::ArrayDirectory array_dir(
        ctx.resources(),
        array_schema.array_uri(),
        0,
        UINT64_MAX,
        tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY);

    auto enumeration = array_dir.load_enumeration(path, key, tracker);

    array_schema.store_enumeration(enumeration);
  }
}

}  // namespace tiledb::sm
