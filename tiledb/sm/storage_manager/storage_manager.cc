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

#include "tiledb/common/common.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <sstream>

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/cache/buffer_lru_cache.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_info.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/consolidator.h"
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
    ThreadPool* const compute_tp,
    ThreadPool* const io_tp,
    stats::Stats* const parent_stats,
    tdb_shared_ptr<Logger> logger)
    : stats_(parent_stats->create_child("StorageManager"))
    , logger_(logger)
    , cancellation_in_progress_(false)
    , queries_in_progress_(0)
    , compute_tp_(compute_tp)
    , io_tp_(io_tp)
    , vfs_(nullptr) {
}

StorageManager::~StorageManager() {
  global_state::GlobalState::GetGlobalState().unregister_storage_manager(this);

  if (vfs_ != nullptr) {
    cancel_all_tasks();
    const Status st = vfs_->terminate();
    if (!st.ok()) {
      logger_->status(Status_StorageManagerError("Failed to terminate VFS."));
    }

    tdb_delete(vfs_);
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManager::array_close_for_reads(Array* array) {
  assert(open_arrays_.find(array) != open_arrays_.end());

  // Remove entry from open arrays
  std::lock_guard<std::mutex> lock{open_arrays_mtx_};
  open_arrays_.erase(array);

  return Status::Ok();
}

Status StorageManager::array_close_for_writes(Array* array) {
  assert(open_arrays_.find(array) != open_arrays_.end());

  // Flush the array metadata
  RETURN_NOT_OK(store_array_metadata(
      array->array_uri(), *array->encryption_key(), array->metadata()));

  // Remove entry from open arrays
  std::lock_guard<std::mutex> lock{open_arrays_mtx_};
  open_arrays_.erase(array);

  return Status::Ok();
}

std::tuple<
    Status,
    std::optional<ArraySchema*>,
    std::optional<std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>>,
    std::optional<std::vector<tdb_shared_ptr<FragmentMetadata>>>>
StorageManager::load_array_schemas_and_fragment_metadata(
    const URI& array_uri,
    MemoryTracker* memory_tracker,
    const EncryptionKey& enc_key,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  auto timer_se =
      stats_->start_timer("get_array_schemas_and_fragment_metadata");

  // Get the fragment URIs
  std::vector<URI> fragment_uris;
  URI meta_uri;
  RETURN_NOT_OK_TUPLE(
      get_fragment_uris(array_uri, &fragment_uris, &meta_uri),
      std::nullopt,
      std::nullopt,
      std::nullopt);

  // Determine which fragments to load
  std::vector<TimestampedURI> fragments_to_load;
  RETURN_NOT_OK_TUPLE(
      get_sorted_uris(
          fragment_uris, &fragments_to_load, timestamp_start, timestamp_end),
      std::nullopt,
      std::nullopt,
      std::nullopt);

  // Get the consolidated fragment metadata
  Buffer f_buff;
  std::unordered_map<std::string, uint64_t> offsets;
  RETURN_NOT_OK_TUPLE(
      load_consolidated_fragment_meta(meta_uri, enc_key, &f_buff, &offsets),
      std::nullopt,
      std::nullopt,
      std::nullopt);

  // Load array schemas
  auto&& [st_schemas, array_schema_latest, array_schemas_all] =
      load_array_schemas(array_uri, enc_key);
  RETURN_NOT_OK_TUPLE(st_schemas, std::nullopt, std::nullopt, std::nullopt);

  // Load the fragment metadata
  auto&& [st_fragment_meta, fragment_metadata] = load_fragment_metadata(
      memory_tracker,
      array_schema_latest.value(),
      array_schemas_all.value(),
      enc_key,
      fragments_to_load,
      &f_buff,
      offsets);
  RETURN_NOT_OK_TUPLE(
      st_fragment_meta, std::nullopt, std::nullopt, std::nullopt);

  return {
      Status::Ok(), array_schema_latest, array_schemas_all, fragment_metadata};
}

std::tuple<
    Status,
    std::optional<ArraySchema*>,
    std::optional<std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>>,
    std::optional<std::vector<tdb_shared_ptr<FragmentMetadata>>>>
StorageManager::array_open_for_reads(Array* array) {
  auto timer_se = stats_->start_timer("array_open_for_reads");
  auto&& [st, array_schema_latest, array_schemas_all, fragment_metadata] =
      load_array_schemas_and_fragment_metadata(
          array->array_uri(),
          array->memory_tracker(),
          *array->encryption_key(),
          array->timestamp_start(),
          array->timestamp_end_opened_at());
  RETURN_NOT_OK_TUPLE(st, std::nullopt, std::nullopt, std::nullopt);

  // Mark the array as open
  std::lock_guard<std::mutex> lock{open_arrays_mtx_};
  open_arrays_.insert(array);

  return {
      Status::Ok(), array_schema_latest, array_schemas_all, fragment_metadata};
}

std::tuple<
    Status,
    std::optional<ArraySchema*>,
    std::optional<std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>>>
StorageManager::array_open_for_reads_without_fragments(Array* array) {
  auto timer_se = stats_->start_timer("array_open_for_reads_without_fragments");

  // Load array schemas
  auto&& [st_schemas, array_schema_latest, array_schemas_all] =
      load_array_schemas(array->array_uri(), *array->encryption_key());
  RETURN_NOT_OK_TUPLE(st_schemas, std::nullopt, std::nullopt);

  // Mark the array as now open
  std::lock_guard<std::mutex> lock{open_arrays_mtx_};
  open_arrays_.insert(array);

  return {Status::Ok(), array_schema_latest, array_schemas_all};
}

std::tuple<
    Status,
    std::optional<ArraySchema*>,
    std::optional<std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>>>
StorageManager::array_open_for_writes(Array* array) {
  // Checks
  if (!vfs_->supports_uri_scheme(array->array_uri()))
    return {logger_->status(Status_StorageManagerError(
                "Cannot open array; URI scheme unsupported.")),
            std::nullopt,
            std::nullopt};

  // Load array schemas
  auto&& [st_schemas, array_schema_latest, array_schemas_all] =
      load_array_schemas(array->array_uri(), *array->encryption_key());
  RETURN_NOT_OK_TUPLE(st_schemas, std::nullopt, std::nullopt);

  // This library should not be able to write to newer-versioned arrays
  // (but it is ok to write to older arrays)
  auto version = array_schema_latest.value()->version();
  if (version > constants::format_version) {
    std::stringstream err;
    err << "Cannot open array for writes; Array format version (";
    err << version;
    err << ") is newer than library format version (";
    err << constants::format_version << ")";
    return {logger_->status(Status_StorageManagerError(err.str())),
            std::nullopt,
            std::nullopt};
  }

  // Mark the array as open
  std::lock_guard<std::mutex> lock{open_arrays_mtx_};
  open_arrays_.insert(array);

  return {Status::Ok(), array_schema_latest, array_schemas_all};
}

std::tuple<Status, std::optional<std::vector<tdb_shared_ptr<FragmentMetadata>>>>
StorageManager::array_load_fragments(
    Array* array, const std::vector<TimestampedURI>& fragments_to_load) {
  auto timer_se = stats_->start_timer("array_load_fragments");

  // Check if the array is open
  auto it = open_arrays_.find(array);
  if (it == open_arrays_.end()) {
    return {logger_->status(Status_StorageManagerError(
                std::string("Cannot load array fragments from ") +
                array->array_uri().to_string() + "; Array not open")),
            std::nullopt};
  }

  // Load the fragment metadata
  std::unordered_map<std::string, uint64_t> offsets;
  auto&& [st_fragment_meta, fragment_metadata] = load_fragment_metadata(
      array->memory_tracker(),
      array->array_schema_latest(),
      array->array_schemas_all(),
      *array->encryption_key(),
      fragments_to_load,
      nullptr,
      offsets);
  RETURN_NOT_OK_TUPLE(st_fragment_meta, std::nullopt);

  return {Status::Ok(), fragment_metadata};
}

std::tuple<
    Status,
    std::optional<ArraySchema*>,
    std::optional<std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>>,
    std::optional<std::vector<tdb_shared_ptr<FragmentMetadata>>>>
StorageManager::array_reopen(Array* array) {
  auto timer_se = stats_->start_timer("read_array_open");

  // Check if array is open
  auto it = open_arrays_.find(array);
  if (it == open_arrays_.end()) {
    return {logger_->status(Status_StorageManagerError(
                std::string("Cannot reopen array ") +
                array->array_uri().to_string() + "; Array not open")),
            std::nullopt,
            std::nullopt,
            std::nullopt};
  }

  return array_open_for_reads(array);
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
    return logger_->status(
        Status_StorageManagerError("Cannot consolidate array; Invalid URI"));
  }

  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(object_type(array_uri, &obj_type));

  if (obj_type != ObjectType::ARRAY) {
    return logger_->status(Status_StorageManagerError(
        "Cannot consolidate array; Array does not exist"));
  }

  // If 'config' is unset, use the 'config_' that was set during initialization
  // of this StorageManager instance.
  if (!config) {
    config = &config_;
  }

  // Get encryption key from config
  std::string encryption_key_from_cfg;
  if (!encryption_key) {
    bool found = false;
    encryption_key_from_cfg = config->get("sm.encryption_key", &found);
    assert(found);
  }

  if (!encryption_key_from_cfg.empty()) {
    encryption_key = encryption_key_from_cfg.c_str();
    std::string encryption_type_from_cfg;
    bool found = false;
    encryption_type_from_cfg = config->get("sm.encryption_type", &found);
    assert(found);
    auto [st, et] = encryption_type_enum(encryption_type_from_cfg);
    RETURN_NOT_OK(st);
    encryption_type = et.value();

    if (EncryptionKey::is_valid_key_length(
            encryption_type,
            static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
      const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
      if (unit_test_cfg.array_encryption_key_length.is_set()) {
        key_length = unit_test_cfg.array_encryption_key_length.get();
      } else {
        key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
      }
    } else {
      encryption_key = nullptr;
      key_length = 0;
    }
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

  bool found = false;
  uint64_t timestamp_start;
  RETURN_NOT_OK(config->get<uint64_t>(
      "sm.vacuum.timestamp_start", &timestamp_start, &found));
  assert(found);

  uint64_t timestamp_end;
  RETURN_NOT_OK(
      config->get<uint64_t>("sm.vacuum.timestamp_end", &timestamp_end, &found));
  assert(found);

  if (mode == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot vacuum array; Vacuum mode cannot be null"));
  else if (std::string(mode) == "fragments")
    RETURN_NOT_OK(
        array_vacuum_fragments(array_name, timestamp_start, timestamp_end));
  else if (std::string(mode) == "fragment_meta")
    RETURN_NOT_OK(array_vacuum_fragment_meta(array_name));
  else if (std::string(mode) == "array_meta")
    RETURN_NOT_OK(
        array_vacuum_array_meta(array_name, timestamp_start, timestamp_end));
  else
    return logger_->status(
        Status_StorageManagerError("Cannot vacuum array; Invalid vacuum mode"));

  return Status::Ok();
}

Status StorageManager::array_vacuum_fragments(
    const char* array_name, uint64_t timestamp_start, uint64_t timestamp_end) {
  if (array_name == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot vacuum fragments; Array name cannot be null"));

  // Get all URIs in the array directory
  URI array_uri(array_name);
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(array_uri.add_trailing_slash(), &uris));

  // Get URIs to be vacuumed
  std::vector<URI> to_vacuum, vac_uris;
  RETURN_NOT_OK(get_uris_to_vacuum(
      uris, timestamp_start, timestamp_end, &to_vacuum, &vac_uris));

  // Delete the ok files
  auto status =
      parallel_for(compute_tp_, 0, to_vacuum.size(), [&, this](size_t i) {
        auto uri = URI(to_vacuum[i].to_string() + constants::ok_file_suffix);
        RETURN_NOT_OK(vfs_->remove_file(uri));

        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Delete fragment directories
  status = parallel_for(compute_tp_, 0, to_vacuum.size(), [&, this](size_t i) {
    RETURN_NOT_OK(vfs_->remove_dir(to_vacuum[i]));

    return Status::Ok();
  });
  RETURN_NOT_OK(status);

  // Delete vacuum files
  status = parallel_for(compute_tp_, 0, vac_uris.size(), [&, this](size_t i) {
    RETURN_NOT_OK(vfs_->remove_file(vac_uris[i]));
    return Status::Ok();
  });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status StorageManager::array_vacuum_fragment_meta(const char* array_name) {
  if (array_name == nullptr)
    return logger_->status(Status_StorageManagerError(
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
  auto status =
      parallel_for(compute_tp_, 0, to_vacuum.size(), [&, this](size_t i) {
        RETURN_NOT_OK(vfs_->remove_file(to_vacuum[i]));
        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status StorageManager::array_vacuum_array_meta(
    const char* array_name, uint64_t timestamp_start, uint64_t timestamp_end) {
  if (array_name == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot vacuum array metadata; Array name cannot be null"));

  // Get all URIs in the array directory
  URI array_uri(array_name);
  auto meta_uri = array_uri.join_path(constants::array_metadata_folder_name);
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs_->ls(meta_uri.add_trailing_slash(), &uris));

  // Get URIs to be vacuumed
  std::vector<URI> to_vacuum, vac_uris;
  RETURN_NOT_OK(get_uris_to_vacuum(
      uris, timestamp_start, timestamp_end, &to_vacuum, &vac_uris));

  // Delete the array metadata files
  auto status =
      parallel_for(compute_tp_, 0, to_vacuum.size(), [&, this](size_t i) {
        RETURN_NOT_OK(vfs_->remove_file(to_vacuum[i]));

        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Delete vacuum files
  status = parallel_for(compute_tp_, 0, vac_uris.size(), [&, this](size_t i) {
    RETURN_NOT_OK(vfs_->remove_file(vac_uris[i]));
    return Status::Ok();
  });
  RETURN_NOT_OK(status);

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
    return logger_->status(Status_StorageManagerError(
        "Cannot consolidate array metadata; Invalid URI"));
  }
  // Check if array exists
  ObjectType obj_type;
  RETURN_NOT_OK(object_type(array_uri, &obj_type));

  if (obj_type != ObjectType::ARRAY) {
    return logger_->status(Status_StorageManagerError(
        "Cannot consolidate array metadata; Array does not exist"));
  }

  // If 'config' is unset, use the 'config_' that was set during initialization
  // of this StorageManager instance.
  if (!config) {
    config = &config_;
  }

  // Get encryption key from config
  std::string encryption_key_from_cfg;
  if (!encryption_key) {
    bool found = false;
    encryption_key_from_cfg = config->get("sm.encryption_key", &found);
    assert(found);
  }

  if (!encryption_key_from_cfg.empty()) {
    encryption_key = encryption_key_from_cfg.c_str();
    std::string encryption_type_from_cfg;
    bool found = false;
    encryption_type_from_cfg = config->get("sm.encryption_type", &found);
    assert(found);
    auto [st, et] = encryption_type_enum(encryption_type_from_cfg);
    RETURN_NOT_OK(st);
    encryption_type = et.value();

    if (EncryptionKey::is_valid_key_length(
            encryption_type,
            static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
      const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
      if (unit_test_cfg.array_encryption_key_length.is_set()) {
        key_length = unit_test_cfg.array_encryption_key_length.get();
      } else {
        key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
      }
    } else {
      encryption_key = nullptr;
      key_length = 0;
    }
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
    return logger_->status(
        Status_StorageManagerError("Cannot create array; Empty array schema"));
  }

  // Check if array exists
  bool exists = false;
  RETURN_NOT_OK(is_array(array_uri, &exists));
  if (exists)
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot create array; Array '") + array_uri.c_str() +
        "' already exists"));

  std::lock_guard<std::mutex> lock{object_create_mtx_};
  array_schema->set_array_uri(array_uri);
  RETURN_NOT_OK(array_schema->generate_uri());
  RETURN_NOT_OK(array_schema->check());

  // Create array directory
  RETURN_NOT_OK(vfs_->create_dir(array_uri));

  // Create array schema directory
  URI array_schema_folder_uri =
      array_uri.join_path(constants::array_schema_folder_name);
  RETURN_NOT_OK(vfs_->create_dir(array_schema_folder_uri));

  // Create array metadata directory
  URI array_metadata_uri =
      array_uri.join_path(constants::array_metadata_folder_name);
  RETURN_NOT_OK(vfs_->create_dir(array_metadata_uri));
  Status st;

  // Get encryption key from config
  if (encryption_key.encryption_type() == EncryptionType::NO_ENCRYPTION) {
    bool found = false;
    std::string encryption_key_from_cfg =
        config_.get("sm.encryption_key", &found);
    assert(found);
    std::string encryption_type_from_cfg =
        config_.get("sm.encryption_type", &found);
    assert(found);
    auto [st, etc] = encryption_type_enum(encryption_type_from_cfg);
    RETURN_NOT_OK(st);
    EncryptionType encryption_type_cfg = etc.value();

    EncryptionKey encryption_key_cfg;
    if (encryption_key_from_cfg.empty()) {
      RETURN_NOT_OK(
          encryption_key_cfg.set_key(encryption_type_cfg, nullptr, 0));
    } else {
      uint32_t key_length = 0;
      if (EncryptionKey::is_valid_key_length(
              encryption_type_cfg,
              static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
        const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
        if (unit_test_cfg.array_encryption_key_length.is_set()) {
          key_length = unit_test_cfg.array_encryption_key_length.get();
        } else {
          key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
        }
      }
      RETURN_NOT_OK(encryption_key_cfg.set_key(
          encryption_type_cfg,
          (const void*)encryption_key_from_cfg.c_str(),
          key_length));
    }
    st = store_array_schema(array_schema, encryption_key_cfg);
  } else {
    st = store_array_schema(array_schema, encryption_key);
  }

  // Store array schema
  if (!st.ok()) {
    vfs_->remove_dir(array_uri);
    return st;
  }

  return Status::Ok();
}

Status StorageManager::array_evolve_schema(
    const URI& array_uri,
    ArraySchemaEvolution* schema_evolution,
    const EncryptionKey& encryption_key) {
  // Check array schema
  if (schema_evolution == nullptr) {
    return logger_->status(Status_StorageManagerError(
        "Cannot evolve array; Empty schema evolution"));
  }

  if (array_uri.is_tiledb()) {
    return rest_client_->post_array_schema_evolution_to_rest(
        array_uri, schema_evolution);
  }

  // Check if array exists
  bool exists = false;
  RETURN_NOT_OK(is_array(array_uri, &exists));
  if (!exists)
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot evolve array; Array '") + array_uri.c_str() +
        "' not exists"));

  ArraySchema* array_schema = (ArraySchema*)nullptr;
  RETURN_NOT_OK(
      load_array_schema_latest(array_uri, encryption_key, &array_schema));

  // Evolve schema
  ArraySchema* array_schema_evolved = (ArraySchema*)nullptr;
  RETURN_NOT_OK(
      schema_evolution->evolve_schema(array_schema, &array_schema_evolved));

  Status st = store_array_schema(array_schema_evolved, encryption_key);
  if (!st.ok()) {
    tdb_delete(array_schema_evolved);
    logger_->status(st);
    return logger_->status(Status_StorageManagerError(
        "Cannot evolve schema;  Not able to store evolved array schema."));
  }

  tdb_delete(array_schema);
  array_schema = nullptr;
  tdb_delete(array_schema_evolved);
  array_schema_evolved = nullptr;

  return Status::Ok();
}

Status StorageManager::array_upgrade_version(
    const URI& array_uri, const Config* config) {
  // Check if array exists
  bool exists = false;
  RETURN_NOT_OK(is_array(array_uri, &exists));
  if (!exists)
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot upgrade array; Array '") + array_uri.c_str() +
        "' does not exist"));

  // If 'config' is unset, use the 'config_' that was set during initialization
  // of this StorageManager instance.
  if (!config) {
    config = &config_;
  }

  // Get encryption key from config
  bool found = false;
  std::string encryption_key_from_cfg =
      config_.get("sm.encryption_key", &found);
  assert(found);
  std::string encryption_type_from_cfg =
      config_.get("sm.encryption_type", &found);
  assert(found);
  auto [st, etc] = encryption_type_enum(encryption_type_from_cfg);
  RETURN_NOT_OK(st);
  EncryptionType encryption_type_cfg = etc.value();

  EncryptionKey encryption_key_cfg;
  if (encryption_key_from_cfg.empty()) {
    RETURN_NOT_OK(encryption_key_cfg.set_key(encryption_type_cfg, nullptr, 0));
  } else {
    uint32_t key_length = 0;
    if (EncryptionKey::is_valid_key_length(
            encryption_type_cfg,
            static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
      const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
      if (unit_test_cfg.array_encryption_key_length.is_set()) {
        key_length = unit_test_cfg.array_encryption_key_length.get();
      } else {
        key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
      }
    }
    RETURN_NOT_OK(encryption_key_cfg.set_key(
        encryption_type_cfg,
        (const void*)encryption_key_from_cfg.c_str(),
        key_length));
  }

  ArraySchema* array_schema = (ArraySchema*)nullptr;
  RETURN_NOT_OK(
      load_array_schema_latest(array_uri, encryption_key_cfg, &array_schema));

  if (array_schema->version() < constants::format_version) {
    Status st = array_schema->generate_uri();
    if (!st.ok()) {
      logger_->status(st);
      // Clean up
      tdb_delete(array_schema);
      return st;
    }
    array_schema->set_version(constants::format_version);

    // Create array schema directory if necessary
    URI array_schema_folder_uri =
        array_uri.join_path(constants::array_schema_folder_name);
    st = vfs_->create_dir(array_schema_folder_uri);
    if (!st.ok()) {
      logger_->status(st);
      // Clean up
      tdb_delete(array_schema);
      return st;
    }

    st = store_array_schema(array_schema, encryption_key_cfg);
    if (!st.ok()) {
      logger_->status(st);
      // Clean up
      tdb_delete(array_schema);
      return st;
    }
  }

  // Clean up
  tdb_delete(array_schema);

  return Status::Ok();
}

Status StorageManager::array_get_non_empty_domain(
    Array* array, NDRange* domain, bool* is_empty) {
  if (domain == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Domain object is null"));

  if (array == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Array object is null"));

  if (!array->is_open())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Array is not open"));

  QueryType query_type;
  RETURN_NOT_OK(array->get_query_type(&query_type));
  if (query_type != QueryType::READ)
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Array not opened for reads"));

  auto&& [st, domain_opt] = array->non_empty_domain();
  RETURN_NOT_OK(st);
  if (domain_opt.has_value()) {
    *domain = domain_opt.value();
    *is_empty = domain->empty();
  }

  return Status::Ok();
}

Status StorageManager::array_get_non_empty_domain(
    Array* array, void* domain, bool* is_empty) {
  if (array == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Array object is null"));

  if (!array->array_schema_latest()->domain()->all_dims_same_type())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Function non-applicable to arrays with "
        "heterogenous dimensions"));

  if (!array->array_schema_latest()->domain()->all_dims_fixed())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Function non-applicable to arrays with "
        "variable-sized dimensions"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));
  if (*is_empty)
    return Status::Ok();

  auto array_schema = array->array_schema_latest();
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
  auto array_schema = array->array_schema_latest();
  auto array_domain = array_schema->domain();

  // Sanity checks
  if (idx >= array_schema->dim_num())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (array_domain->dimension(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain->dimension(idx)->name();
    errmsg += "' is variable-sized";
    return logger_->status(Status_StorageManagerError(errmsg));
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
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension name"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  auto array_schema = array->array_schema_latest();
  auto array_domain = array_schema->domain();
  auto dim_num = array_schema->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_name = array_schema->dimension(d)->name();
    if (name == dim_name) {
      // Sanity check
      if (array_domain->dimension(d)->var_size()) {
        std::string errmsg = "Cannot get non-empty domain; Dimension '";
        errmsg += dim_name + "' is variable-sized";
        return logger_->status(Status_StorageManagerError(errmsg));
      }

      if (!*is_empty)
        std::memcpy(domain, dom[d].data(), dom[d].size());
      return Status::Ok();
    }
  }

  return logger_->status(Status_StorageManagerError(
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
  auto array_schema = array->array_schema_latest();
  auto array_domain = array_schema->domain();

  // Sanity checks
  if (idx >= array_schema->dim_num())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (!array_domain->dimension(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain->dimension(idx)->name();
    errmsg += "' is fixed-sized";
    return logger_->status(Status_StorageManagerError(errmsg));
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
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension name"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  auto array_schema = array->array_schema_latest();
  auto array_domain = array_schema->domain();
  auto dim_num = array_schema->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_name = array_schema->dimension(d)->name();
    if (name == dim_name) {
      // Sanity check
      if (!array_domain->dimension(d)->var_size()) {
        std::string errmsg = "Cannot get non-empty domain; Dimension '";
        errmsg += dim_name + "' is fixed-sized";
        return logger_->status(Status_StorageManagerError(errmsg));
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

  return logger_->status(Status_StorageManagerError(
      std::string("Cannot get non-empty domain; Dimension name '") + name +
      "' does not exist"));
}

Status StorageManager::array_get_non_empty_domain_var_from_index(
    Array* array, unsigned idx, void* start, void* end, bool* is_empty) {
  // For easy reference
  auto array_schema = array->array_schema_latest();
  auto array_domain = array_schema->domain();

  // Sanity checks
  if (idx >= array_schema->dim_num())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (!array_domain->dimension(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain->dimension(idx)->name();
    errmsg += "' is fixed-sized";
    return logger_->status(Status_StorageManagerError(errmsg));
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
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension name"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  auto array_schema = array->array_schema_latest();
  auto array_domain = array_schema->domain();
  auto dim_num = array_schema->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_name = array_schema->dimension(d)->name();
    if (name == dim_name) {
      // Sanity check
      if (!array_domain->dimension(d)->var_size()) {
        std::string errmsg = "Cannot get non-empty domain; Dimension '";
        errmsg += dim_name + "' is fixed-sized";
        return logger_->status(Status_StorageManagerError(errmsg));
      }

      if (!*is_empty) {
        std::memcpy(start, dom[d].start(), dom[d].start_size());
        std::memcpy(end, dom[d].end(), dom[d].end_size());
      }

      return Status::Ok();
    }
  }

  return logger_->status(Status_StorageManagerError(
      std::string("Cannot get non-empty domain; Dimension name '") + name +
      "' does not exist"));
}

Status StorageManager::array_get_encryption(
    const std::string& array_uri, EncryptionType* encryption_type) {
  URI uri(array_uri);

  if (uri.is_invalid())
    return logger_->status(Status_StorageManagerError(
        "Cannot get array encryption; Invalid array URI"));

  URI schema_uri;
  RETURN_NOT_OK(get_latest_array_schema_uri(uri, &schema_uri));

  // Read tile header.
  GenericTileIO::GenericTileHeader header;
  RETURN_NOT_OK(
      GenericTileIO::read_generic_tile_header(this, schema_uri, 0, &header));
  *encryption_type = static_cast<EncryptionType>(header.encryption_type);

  return Status::Ok();
}

Status StorageManager::async_push_query(Query* query) {
  cancelable_tasks_.execute(
      compute_tp_,
      [this, query]() {
        // Process query.
        Status st = query_submit(query);
        if (!st.ok())
          logger_->status(st);
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
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot remove object '") + path + "'; Invalid URI"));

  ObjectType obj_type;
  RETURN_NOT_OK(object_type(uri, &obj_type));
  if (obj_type == ObjectType::INVALID)
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot remove object '") + path +
        "'; Invalid TileDB object"));

  return vfs_->remove_dir(uri);
}

Status StorageManager::object_move(
    const char* old_path, const char* new_path) const {
  auto old_uri = URI(old_path);
  if (old_uri.is_invalid())
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot move object '") + old_path + "'; Invalid URI"));

  auto new_uri = URI(new_path);
  if (new_uri.is_invalid())
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot move object to '") + new_path + "'; Invalid URI"));

  ObjectType obj_type;
  RETURN_NOT_OK(object_type(old_uri, &obj_type));
  if (obj_type == ObjectType::INVALID)
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot move object '") + old_path +
        "'; Invalid TileDB object"));

  return vfs_->move_dir(old_uri, new_uri);
}

Status StorageManager::get_fragment_uris(
    const URI& array_uri,
    std::vector<URI>* fragment_uris,
    URI* meta_uri) const {
  auto timer_se = stats_->start_timer("read_get_fragment_uris");
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
  auto status = parallel_for(compute_tp_, 0, uris.size(), [&](size_t i) {
    if (utils::parse::starts_with(uris[i].last_path_part(), "."))
      return Status::Ok();
    RETURN_NOT_OK(this->is_fragment(uris[i], ok_uris, &is_fragment[i]));
    return Status::Ok();
  });
  RETURN_NOT_OK(status);

  for (size_t i = 0; i < uris.size(); ++i) {
    if (is_fragment[i])
      fragment_uris->emplace_back(uris[i]);
    else if (this->is_vacuum_file(uris[i]))
      fragment_uris->emplace_back(uris[i]);
  }

  // Get the latest consolidated fragment metadata URI
  RETURN_NOT_OK(get_consolidated_fragment_meta_uri(uris, meta_uri));

  return Status::Ok();
}

Status StorageManager::get_uris_to_vacuum(
    const std::vector<URI>& uris,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    std::vector<URI>* to_vacuum,
    std::vector<URI>* vac_uris,
    bool allow_partial) const {
  // Get vacuum URIs
  std::vector<URI> vac_files;
  std::unordered_set<std::string> non_vac_uris_set;
  std::unordered_map<std::string, size_t> uris_map;
  for (size_t i = 0; i < uris.size(); ++i) {
    std::pair<uint64_t, uint64_t> timestamp_range;
    RETURN_NOT_OK(utils::parse::get_timestamp_range(uris[i], &timestamp_range));

    if (this->is_vacuum_file(uris[i])) {
      if (allow_partial) {
        if (timestamp_range.first <= timestamp_end &&
            timestamp_range.second >= timestamp_start)
          vac_files.emplace_back(uris[i]);
      } else {
        if (timestamp_range.first >= timestamp_start &&
            timestamp_range.second <= timestamp_end)
          vac_files.emplace_back(uris[i]);
      }
    } else {
      if (timestamp_range.first < timestamp_start ||
          timestamp_range.second > timestamp_end) {
        non_vac_uris_set.emplace(uris[i].to_string());
      } else {
        uris_map[uris[i].to_string()] = i;
      }
    }
  }

  // Compute fragment URIs to vacuum as a bitmap vector
  // Also determine which vac files to vacuum
  std::vector<int32_t> to_vacuum_vec(uris.size(), 0);
  std::vector<int32_t> to_vacuum_vac_files_vec(vac_files.size(), 0);
  auto status =
      parallel_for(compute_tp_, 0, vac_files.size(), [&, this](size_t i) {
        uint64_t size = 0;
        RETURN_NOT_OK(vfs_->file_size(vac_files[i], &size));
        std::string names;
        names.resize(size);
        RETURN_NOT_OK(vfs_->read(vac_files[i], 0, &names[0], size));
        std::stringstream ss(names);
        bool vacuum_vac_file = true;
        for (std::string uri_str; std::getline(ss, uri_str);) {
          auto it = uris_map.find(uri_str);
          if (it != uris_map.end())
            to_vacuum_vec[it->second] = 1;

          if (vacuum_vac_file &&
              non_vac_uris_set.find(uri_str) != non_vac_uris_set.end()) {
            vacuum_vac_file = false;
          }
        }

        to_vacuum_vac_files_vec[i] = vacuum_vac_file;

        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Compute the URIs to vacuum
  to_vacuum->clear();
  for (size_t i = 0; i < uris.size(); ++i) {
    if (to_vacuum_vec[i] == 1)
      to_vacuum->emplace_back(uris[i]);
  }

  // Compute the vac URIs to vacuum
  vac_uris->clear();
  for (size_t i = 0; i < vac_files.size(); ++i) {
    if (to_vacuum_vac_files_vec[i] == 1)
      vac_uris->emplace_back(vac_files[i]);
  }

  return Status::Ok();
}

const std::unordered_map<std::string, std::string>& StorageManager::tags()
    const {
  return tags_;
}

Status StorageManager::group_create(const std::string& group) {
  // Create group URI
  URI uri(group);
  if (uri.is_invalid())
    return logger_->status(Status_StorageManagerError(
        "Cannot create group '" + group + "'; Invalid group URI"));

  // Check if group exists
  bool exists;
  RETURN_NOT_OK(is_group(uri, &exists));
  if (exists)
    return logger_->status(Status_StorageManagerError(
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
  RETURN_NOT_OK(vfs_->init(stats_, compute_tp_, io_tp_, &config_, nullptr));
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
  // Check if the schema directory exists or not
  bool is_dir = false;
  // Since is_dir could return NOT Ok status, we will not use RETURN_NOT_OK here
  Status st =
      vfs_->is_dir(uri.join_path(constants::array_schema_folder_name), &is_dir);
  if (st.ok() && is_dir) {
    *is_array = true;
    return Status::Ok();
  }

  // If there is no schema directory, we check schema file
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

Status StorageManager::get_array_schema_uris(
    const URI& array_uri, std::vector<URI>* schema_uris) const {
  auto timer_se = stats_->start_timer("read_get_array_schema_uris");

  schema_uris->clear();
  URI old_schema_uri = array_uri.join_path(constants::array_schema_filename);
  bool has_file = false;
  RETURN_NOT_OK(vfs_->is_file(old_schema_uri, &has_file));
  if (has_file) {
    schema_uris->push_back(old_schema_uri);
  }

  URI schema_folder_uri =
      array_uri.join_path(constants::array_schema_folder_name);
  // Check if schema_folder_uri exists. For some file systems, such as win, ls
  // will return error if the folder does not exist.
  bool has_dir = false;
  RETURN_NOT_OK(vfs_->is_dir(schema_folder_uri, &has_dir));
  if (has_dir) {
    std::vector<URI> array_schema_uris;
    RETURN_NOT_OK(vfs_->ls(schema_folder_uri, &array_schema_uris));
    if (array_schema_uris.size() > 0) {
      schema_uris->reserve(schema_uris->size() + array_schema_uris.size());
      std::copy(
          array_schema_uris.begin(),
          array_schema_uris.end(),
          std::back_inserter(*schema_uris));
    }
  }

  // Check if schema_uris is empty
  if (schema_uris->empty()) {
    return logger_->status(Status_StorageManagerError(
        "Cannot get the array schemas; No array schemas found."));
  }

  return Status::Ok();
}

Status StorageManager::get_latest_array_schema_uri(
    const URI& array_uri, URI* uri) const {
  auto timer_se = stats_->start_timer("read_get_latest_array_schema_uri");

  std::vector<URI> schema_uris;
  RETURN_NOT_OK(get_array_schema_uris(array_uri, &schema_uris));
  if (schema_uris.size() == 0) {
    return logger_->status(Status_StorageManagerError(
        "Cannot get the latest array schema; No array schemas found."));
  }
  *uri = schema_uris.back();
  if (uri->is_invalid()) {
    return logger_->status(
        Status_StorageManagerError("Could not find array schema URI"));
  }

  return Status::Ok();
}

Status StorageManager::load_array_schema_from_uri(
    const URI& schema_uri,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema) {
  auto timer_se = stats_->start_timer("read_load_array_schema_from_uri");

  GenericTileIO tile_io(this, schema_uri);
  Tile* tile = nullptr;

  // Get encryption key from config
  if (encryption_key.encryption_type() == EncryptionType::NO_ENCRYPTION) {
    bool found = false;
    std::string encryption_key_from_cfg =
        config_.get("sm.encryption_key", &found);
    assert(found);
    std::string encryption_type_from_cfg =
        config_.get("sm.encryption_type", &found);
    assert(found);
    auto [st, etc] = encryption_type_enum(encryption_type_from_cfg);
    RETURN_NOT_OK(st);
    EncryptionType encryption_type_cfg = etc.value();

    EncryptionKey encryption_key_cfg;
    if (encryption_key_from_cfg.empty()) {
      RETURN_NOT_OK(
          encryption_key_cfg.set_key(encryption_type_cfg, nullptr, 0));
    } else {
      uint32_t key_length = 0;
      if (EncryptionKey::is_valid_key_length(
              encryption_type_cfg,
              static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
        const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
        if (unit_test_cfg.array_encryption_key_length.is_set()) {
          key_length = unit_test_cfg.array_encryption_key_length.get();
        } else {
          key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
        }
      }
      RETURN_NOT_OK(encryption_key_cfg.set_key(
          encryption_type_cfg,
          (const void*)encryption_key_from_cfg.c_str(),
          key_length));
    }
    RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key_cfg, config_));
  } else {
    RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key, config_));
  }

  Buffer buff;
  buff.realloc(tile->size());
  buff.set_size(tile->size());
  RETURN_NOT_OK_ELSE(tile->read(buff.data(), 0, buff.size()), tdb_delete(tile));
  tdb_delete(tile);

  stats_->add_counter("read_array_schema_size", buff.size());

  // Deserialize
  ConstBuffer cbuff(&buff);
  *array_schema = tdb_new(ArraySchema);
  Status st = (*array_schema)->deserialize(&cbuff);
  if (!st.ok()) {
    tdb_delete(*array_schema);
    *array_schema = nullptr;
    return st;
  }
  (*array_schema)->set_uri(schema_uri);
  return st;
}

Status StorageManager::load_array_schema_latest(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    ArraySchema** array_schema) {
  auto timer_se = stats_->start_timer("read_load_array_schema");

  if (array_uri.is_invalid())
    return logger_->status(Status_StorageManagerError(
        "Cannot load array schema; Invalid array URI"));

  URI schema_uri;
  RETURN_NOT_OK(get_latest_array_schema_uri(array_uri, &schema_uri));

  RETURN_NOT_OK(
      load_array_schema_from_uri(schema_uri, encryption_key, array_schema));
  (*array_schema)->set_array_uri(array_uri);
  return Status::Ok();
}

std::tuple<
    Status,
    std::optional<ArraySchema*>,
    std::optional<std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>>>
StorageManager::load_array_schemas(
    const URI& array_uri, const EncryptionKey& encryption_key) {
  auto array_schema = (ArraySchema*)nullptr;
  RETURN_NOT_OK_TUPLE(
      load_array_schema_latest(array_uri, encryption_key, &array_schema),
      std::nullopt,
      std::nullopt);

  auto&& [st, schemas] = load_all_array_schemas(array_uri, encryption_key);
  RETURN_NOT_OK_TUPLE(st, std::nullopt, std::nullopt);

  return {Status::Ok(), array_schema, schemas};
}

std::tuple<
    Status,
    std::optional<std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>>>
StorageManager::load_all_array_schemas(
    const URI& array_uri, const EncryptionKey& encryption_key) {
  auto timer_se = stats_->start_timer("read_load_all_array_schemas");

  if (array_uri.is_invalid())
    return {logger_->status(Status_StorageManagerError(
                "Cannot load all array schemas; Invalid array URI")),
            std::nullopt};

  std::vector<URI> schema_uris;
  RETURN_NOT_OK_TUPLE(
      get_array_schema_uris(array_uri, &schema_uris), std::nullopt);
  if (schema_uris.empty()) {
    return {logger_->status(Status_StorageManagerError(
                "Cannot get the array schema vector; No array schemas found.")),
            std::nullopt};
  }

  std::vector<ArraySchema*> schema_vector;
  auto schema_num = schema_uris.size();
  schema_vector.resize(schema_num);

  auto status =
      parallel_for(compute_tp_, 0, schema_num, [&](size_t schema_ith) {
        auto& schema_uri = schema_uris[schema_ith];
        auto array_schema = (ArraySchema*)nullptr;
        RETURN_NOT_OK(load_array_schema_from_uri(
            schema_uri, encryption_key, &array_schema));
        schema_vector[schema_ith] = array_schema;
        return Status::Ok();
      });
  RETURN_NOT_OK_TUPLE(status, std::nullopt);

  std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>> array_schemas;
  for (const auto& array_schema : schema_vector) {
    array_schemas[array_schema->name()] =
        tdb_shared_ptr<ArraySchema>(array_schema);
  }

  return {Status::Ok(), array_schemas};
}

Status StorageManager::load_array_metadata(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    Metadata* metadata) {
  auto timer_se = stats_->start_timer("read_load_array_meta");

  // Special case
  if (metadata == nullptr)
    return Status::Ok();

  // Determine which array metadata to load
  std::vector<TimestampedURI> array_metadata_to_load;
  std::vector<URI> array_metadata_uris;
  RETURN_NOT_OK(get_array_metadata_uris(array_uri, &array_metadata_uris));
  RETURN_NOT_OK(get_sorted_uris(
      array_metadata_uris,
      &array_metadata_to_load,
      timestamp_start,
      timestamp_end));

  auto metadata_num = array_metadata_to_load.size();
  std::vector<tdb_shared_ptr<Buffer>> metadata_buffs;
  metadata_buffs.resize(metadata_num);
  auto status = parallel_for(compute_tp_, 0, metadata_num, [&](size_t m) {
    const auto& uri = array_metadata_to_load[m].uri_;
    GenericTileIO tile_io(this, uri);
    auto tile = (Tile*)nullptr;
    RETURN_NOT_OK(tile_io.read_generic(&tile, 0, encryption_key, config_));
    auto metadata_buff = tdb::make_shared<Buffer>(HERE());
    RETURN_NOT_OK(metadata_buff->realloc(tile->size()));
    metadata_buff->set_size(tile->size());
    RETURN_NOT_OK_ELSE(
        tile->read(metadata_buff->data(), 0, metadata_buff->size()),
        tdb_delete(tile));
    tdb_delete(tile);

    metadata_buffs[m] = metadata_buff;

    return Status::Ok();
  });
  RETURN_NOT_OK(status);

  // Compute array metadata size for the statistics
  uint64_t meta_size = 0;
  for (const auto& b : metadata_buffs)
    meta_size += b->size();
  stats_->add_counter("read_array_meta_size", meta_size);

  // Deserialize metadata buffers
  metadata->deserialize(metadata_buffs);

  // Sets the loaded metadata URIs
  metadata->set_loaded_metadata_uris(array_metadata_to_load);

  return Status::Ok();
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

  bool exists = false;
  RETURN_NOT_OK(vfs_->is_dir(
      dir_uri.join_path(constants::array_schema_folder_name), &exists));
  if (exists) {
    *type = ObjectType::ARRAY;
    return Status::Ok();
  }

  RETURN_NOT_OK(vfs_->is_file(
      dir_uri.join_path(constants::array_schema_filename), &exists));
  if (exists) {
    *type = ObjectType::ARRAY;
    return Status::Ok();
  }

  RETURN_NOT_OK(
      vfs_->is_file(dir_uri.join_path(constants::group_filename), &exists));
  if (exists) {
    *type = ObjectType::GROUP;
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
    return logger_->status(Status_StorageManagerError(
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
    return logger_->status(Status_StorageManagerError(
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
    FilteredBuffer& buffer,
    uint64_t nbytes,
    bool* in_cache) const {
  std::stringstream key;
  key << uri.to_string() << "+" << offset;
  buffer.expand(nbytes);
  RETURN_NOT_OK(tile_cache_->read(key.str(), buffer, 0, nbytes, in_cache));

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
  const URI schema_uri = array_schema->uri();

  // Serialize
  Buffer buff;
  RETURN_NOT_OK(array_schema->serialize(&buff));

  stats_->add_counter("write_array_schema_size", buff.size());

  // Delete file if it exists already
  bool exists;
  RETURN_NOT_OK(is_file(schema_uri, &exists));
  if (exists)
    RETURN_NOT_OK(vfs_->remove_file(schema_uri));

  // Check if the array schema directory exists
  // If not create it, this is caused by a pre-v10 array
  bool schema_dir_exists = false;
  URI array_schema_folder_uri =
      array_schema->array_uri().join_path(constants::array_schema_folder_name);
  RETURN_NOT_OK(is_dir(array_schema_folder_uri, &schema_dir_exists));
  if (!schema_dir_exists)
    RETURN_NOT_OK(create_dir(array_schema_folder_uri));

  // Write to file
  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      buff.data(),
      buff.size());
  buff.disown_data();

  GenericTileIO tile_io(this, schema_uri);
  uint64_t nbytes;
  Status st = tile_io.write_generic(&tile, encryption_key, &nbytes);
  (void)nbytes;
  if (st.ok())
    st = close_file(schema_uri);

  return st;
}

Status StorageManager::store_array_metadata(
    const URI& array_uri,
    const EncryptionKey& encryption_key,
    Metadata* array_metadata) {
  auto timer_se = stats_->start_timer("write_array_meta");

  // Trivial case
  if (array_metadata == nullptr)
    return Status::Ok();

  // Serialize array metadata
  Buffer metadata_buff;
  RETURN_NOT_OK(array_metadata->serialize(&metadata_buff));

  // Do nothing if there are no metadata to write
  if (metadata_buff.size() == 0)
    return Status::Ok();

  stats_->add_counter("write_array_meta_size", metadata_buff.size());

  // Create a metadata file name
  URI array_metadata_uri;
  RETURN_NOT_OK(array_metadata->get_uri(array_uri, &array_metadata_uri));

  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      metadata_buff.data(),
      metadata_buff.size());
  metadata_buff.disown_data();

  GenericTileIO tile_io(this, array_metadata_uri);
  uint64_t nbytes;
  Status st = tile_io.write_generic(&tile, encryption_key, &nbytes);
  (void)nbytes;

  if (st.ok()) {
    st = close_file(array_metadata_uri);
  }

  metadata_buff.clear();

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

Status StorageManager::init_rest_client() {
  const char* server_address;
  RETURN_NOT_OK(config_.get("rest.server_address", &server_address));
  if (server_address != nullptr) {
    rest_client_.reset(tdb_new(RestClient));
    RETURN_NOT_OK(rest_client_->init(stats_, &config_, compute_tp_));
  }

  return Status::Ok();
}

Status StorageManager::write_to_cache(
    const URI& uri, uint64_t offset, const FilteredBuffer& buffer) const {
  // Do not write metadata or array schema to cache
  std::string filename = uri.last_path_part();
  std::string uri_str = uri.to_string();
  if (filename == constants::fragment_metadata_filename ||
      (uri_str.find(constants::array_schema_folder_name) !=
       std::string::npos) ||
      filename == constants::array_schema_filename) {
    return Status::Ok();
  }

  // Generate key (uri + offset)
  std::stringstream key;
  key << uri.to_string() << "+" << offset;

  // Insert to cache
  FilteredBuffer cached_buffer(buffer);
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

stats::Stats* StorageManager::stats() {
  return stats_;
}

tdb_shared_ptr<Logger> StorageManager::logger() const {
  return logger_;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

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

std::tuple<Status, std::optional<std::vector<tdb_shared_ptr<FragmentMetadata>>>>
StorageManager::load_fragment_metadata(
    MemoryTracker* memory_tracker,
    ArraySchema* array_schema_latest,
    const std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>&
        array_schemas_all,
    const EncryptionKey& encryption_key,
    const std::vector<TimestampedURI>& fragments_to_load,
    Buffer* meta_buff,
    const std::unordered_map<std::string, uint64_t>& offsets) {
  auto timer_se = stats_->start_timer("load_fragment_metadata");

  // Load the metadata for each fragment
  auto fragment_num = fragments_to_load.size();
  std::vector<tdb_shared_ptr<FragmentMetadata>> fragment_metadata;
  fragment_metadata.resize(fragment_num);
  auto status = parallel_for(compute_tp_, 0, fragment_num, [&](size_t f) {
    const auto& sf = fragments_to_load[f];

    URI coords_uri =
        sf.uri_.join_path(constants::coords + constants::file_suffix);

    auto name = sf.uri_.remove_trailing_slash().last_path_part();
    uint32_t f_version;
    RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &f_version));

    // Note that the fragment metadata version is >= the array schema
    // version. Therefore, the check below is defensive and will always
    // ensure backwards compatibility.
    tdb_shared_ptr<FragmentMetadata> metadata;
    if (f_version == 1) {  // This is equivalent to format version <=2
      bool sparse;
      RETURN_NOT_OK(vfs_->is_file(coords_uri, &sparse));
      metadata = tdb::make_shared<FragmentMetadata>(
          HERE(),
          this,
          memory_tracker,
          array_schema_latest,
          sf.uri_,
          sf.timestamp_range_,
          !sparse);
    } else {  // Format version > 2
      metadata = tdb::make_shared<FragmentMetadata>(
          HERE(),
          this,
          memory_tracker,
          array_schema_latest,
          sf.uri_,
          sf.timestamp_range_);
    }

    // Potentially find the basic fragment metadata in the consolidated
    // metadata buffer
    Buffer* f_buff = nullptr;
    uint64_t offset = 0;

    auto it = offsets.end();
    if (metadata->format_version() >= 9) {
      it = offsets.find(name);
    } else {
      it = offsets.find(sf.uri_.to_string());
    }
    if (it != offsets.end()) {
      f_buff = meta_buff;
      offset = it->second;
    }

    // Load fragment metadata
    RETURN_NOT_OK(
        metadata->load(encryption_key, f_buff, offset, array_schemas_all));

    fragment_metadata[f] = metadata;
    return Status::Ok();
  });
  RETURN_NOT_OK_TUPLE(status, std::nullopt);

  return {Status::Ok(), fragment_metadata};
}

Status StorageManager::load_consolidated_fragment_meta(
    const URI& uri,
    const EncryptionKey& enc_key,
    Buffer* f_buff,
    std::unordered_map<std::string, uint64_t>* offsets) {
  auto timer_se = stats_->start_timer("read_load_consolidated_frag_meta");

  // No consolidated fragment metadata file
  if (uri.to_string().empty())
    return Status::Ok();

  GenericTileIO tile_io(this, uri);
  Tile* tile = nullptr;
  RETURN_NOT_OK(tile_io.read_generic(&tile, 0, enc_key, config_));

  f_buff->realloc(tile->size());
  f_buff->set_size(tile->size());
  RETURN_NOT_OK_ELSE(
      tile->read(f_buff->data(), 0, f_buff->size()), tdb_delete(tile));
  tdb_delete(tile);

  stats_->add_counter("consolidated_frag_meta_size", f_buff->size());

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
    std::vector<TimestampedURI>* sorted_uris,
    uint64_t timestamp_start,
    uint64_t timestamp_end) const {
  // Do nothing if there are not enough URIs
  if (uris.empty())
    return Status::Ok();

  // Get the URIs that must be ignored
  std::vector<URI> vac_uris, to_ignore;
  RETURN_NOT_OK(get_uris_to_vacuum(
      uris, timestamp_start, timestamp_end, &to_ignore, &vac_uris, false));
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

    // Add only URIs whose first timestamp is greater than or equal to the
    // timestamp_start and whose second timestamp is smaller than or equal to
    // the timestamp_end
    std::pair<uint64_t, uint64_t> timestamp_range;
    RETURN_NOT_OK(utils::parse::get_timestamp_range(uri, &timestamp_range));
    auto t1 = timestamp_range.first;
    auto t2 = timestamp_range.second;
    if (t1 >= timestamp_start && t2 <= timestamp_end)
      sorted_uris->emplace_back(uri, timestamp_range);
  }

  // Sort the names based on the timestamps
  std::sort(sorted_uris->begin(), sorted_uris->end());

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
