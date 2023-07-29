/**
 * @file   storage_manager.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/consolidator/consolidator.h"
#include "tiledb/sm/consolidator/fragment_consolidator.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_info.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/group/group_details_v1.h"
#include "tiledb/sm/group/group_details_v2.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/deletes_and_updates/serialization.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/update_value.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/storage_format/uri/parse_uri.h"

#include <algorithm>
#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

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
  RETURN_NOT_OK(global_state.init(config_));

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
      config().get<bool>("sm.mem.malloc_trim", &use_malloc_trim, &found);
  if (st.ok() && found && use_malloc_trim) {
    tdb_malloc_trim();
  }
  assert(found);
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status StorageManagerCanonical::group_close_for_reads(Group* group) {
  assert(open_groups_.find(group) != open_groups_.end());

  // Remove entry from open groups
  std::lock_guard<std::mutex> lock{open_groups_mtx_};
  open_groups_.erase(group);

  return Status::Ok();
}

Status StorageManagerCanonical::group_close_for_writes(Group* group) {
  assert(open_groups_.find(group) != open_groups_.end());

  // Flush the group metadata
  RETURN_NOT_OK(store_metadata(
      group->group_uri(), *group->encryption_key(), group->unsafe_metadata()));

  // Store any changes required
  if (group->changes_applied()) {
    const URI& group_detail_folder_uri = group->group_detail_uri();
    auto&& [st, group_detail_uri] = group->generate_detail_uri();
    RETURN_NOT_OK(st);
    RETURN_NOT_OK(store_group_detail(
        group_detail_folder_uri,
        group_detail_uri.value(),
        group->group_details(),
        *group->encryption_key()));
  }

  // Remove entry from open groups
  std::lock_guard<std::mutex> lock{open_groups_mtx_};
  open_groups_.erase(group);

  return Status::Ok();
}

std::tuple<
    Status,
    optional<shared_ptr<ArraySchema>>,
    optional<std::unordered_map<std::string, shared_ptr<ArraySchema>>>,
    optional<std::vector<shared_ptr<FragmentMetadata>>>>
StorageManagerCanonical::load_array_schemas_and_fragment_metadata(
    const ArrayDirectory& array_dir,
    MemoryTracker* memory_tracker,
    const EncryptionKey& enc_key) {
  auto timer_se =
      stats()->start_timer("sm_load_array_schemas_and_fragment_metadata");

  // Load array schemas
  auto&& [array_schema_latest, array_schemas_all] =
      array_dir.load_array_schemas(enc_key);

  const auto filtered_fragment_uris =
      load_filtered_fragment_uris(array_schema_latest->dense(), array_dir);
  const auto& meta_uris = array_dir.fragment_meta_uris();
  const auto& fragments_to_load = filtered_fragment_uris.fragment_uris();

  // Get the consolidated fragment metadatas
  std::vector<shared_ptr<Tile>> fragment_metadata_tiles(meta_uris.size());
  std::vector<std::vector<std::pair<std::string, uint64_t>>> offsets_vectors(
      meta_uris.size());
  auto status = parallel_for(compute_tp(), 0, meta_uris.size(), [&](size_t i) {
    auto&& [st, tile_opt, offsets] =
        load_consolidated_fragment_meta(meta_uris[i], enc_key);
    RETURN_NOT_OK(st);
    fragment_metadata_tiles[i] =
        make_shared<Tile>(HERE(), std::move(*tile_opt));
    offsets_vectors[i] = std::move(offsets.value());
    return st;
  });
  RETURN_NOT_OK_TUPLE(status, nullopt, nullopt, nullopt);

  // Get the unique fragment metadatas into a map.
  std::unordered_map<std::string, std::pair<Tile*, uint64_t>> offsets;
  for (uint64_t i = 0; i < offsets_vectors.size(); i++) {
    for (auto& offset : offsets_vectors[i]) {
      if (offsets.count(offset.first) == 0) {
        offsets.emplace(
            offset.first,
            std::make_pair(fragment_metadata_tiles[i].get(), offset.second));
      }
    }
  }

  // Load the fragment metadata
  auto&& [st_fragment_meta, fragment_metadata] = load_fragment_metadata(
      memory_tracker,
      array_schema_latest,
      array_schemas_all,
      enc_key,
      fragments_to_load,
      offsets);
  RETURN_NOT_OK_TUPLE(st_fragment_meta, nullopt, nullopt, nullopt);

  return {
      Status::Ok(), array_schema_latest, array_schemas_all, fragment_metadata};
}

tuple<Status, optional<std::vector<shared_ptr<FragmentMetadata>>>>
StorageManagerCanonical::array_load_fragments(
    Array* array, const std::vector<TimestampedURI>& fragments_to_load) {
  auto timer_se = stats()->start_timer("sm_array_load_fragments");

  // Load the fragment metadata
  std::unordered_map<std::string, std::pair<Tile*, uint64_t>> offsets;
  auto&& [st_fragment_meta, fragment_metadata] = load_fragment_metadata(
      array->memory_tracker(),
      array->array_schema_latest_ptr(),
      array->array_schemas_all(),
      *array->encryption_key(),
      fragments_to_load,
      offsets);
  RETURN_NOT_OK_TUPLE(st_fragment_meta, nullopt);

  return {Status::Ok(), fragment_metadata};
}

Status StorageManagerCanonical::array_consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const Config& config) {
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

  // Get encryption key from config
  std::string encryption_key_from_cfg;
  if (!encryption_key) {
    bool found = false;
    encryption_key_from_cfg = config.get("sm.encryption_key", &found);
    assert(found);
  }

  if (!encryption_key_from_cfg.empty()) {
    encryption_key = encryption_key_from_cfg.c_str();
    std::string encryption_type_from_cfg;
    bool found = false;
    encryption_type_from_cfg = config.get("sm.encryption_type", &found);
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
  auto mode = Consolidator::mode_from_config(config);
  auto consolidator = Consolidator::create(mode, config, this);
  return consolidator->consolidate(
      array_name, encryption_type, encryption_key, key_length);
}

Status StorageManagerCanonical::fragments_consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const std::vector<std::string> fragment_uris,
    const Config& config) {
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

  // Get encryption key from config
  std::string encryption_key_from_cfg;
  if (!encryption_key) {
    bool found = false;
    encryption_key_from_cfg = config.get("sm.encryption_key", &found);
    assert(found);
  }

  if (!encryption_key_from_cfg.empty()) {
    encryption_key = encryption_key_from_cfg.c_str();
    std::string encryption_type_from_cfg;
    bool found = false;
    encryption_type_from_cfg = config.get("sm.encryption_type", &found);
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
  auto consolidator =
      Consolidator::create(ConsolidationMode::FRAGMENT, config, this);
  auto fragment_consolidator =
      dynamic_cast<FragmentConsolidator*>(consolidator.get());
  return fragment_consolidator->consolidate_fragments(
      array_name, encryption_type, encryption_key, key_length, fragment_uris);
}

void StorageManagerCanonical::write_consolidated_commits_file(
    format_version_t write_version,
    ArrayDirectory array_dir,
    const std::vector<URI>& commit_uris) {
  // Compute the file name.
  auto name = array_dir.compute_new_fragment_name(
      commit_uris.front(), commit_uris.back(), write_version);

  // Compute size of consolidated file. Save the sizes of the files to re-use
  // below.
  storage_size_t total_size = 0;
  const auto base_uri_size = array_dir.uri().to_string().size();
  std::vector<storage_size_t> file_sizes(commit_uris.size());
  for (uint64_t i = 0; i < commit_uris.size(); i++) {
    const auto& uri = commit_uris[i];
    total_size += uri.to_string().size() - base_uri_size + 1;

    // If the file is a delete, add the file size to the count and the size of
    // the size variable.
    if (stdx::string::ends_with(
            uri.to_string(), constants::delete_file_suffix)) {
      throw_if_not_ok(vfs()->file_size(uri, &file_sizes[i]));
      total_size += file_sizes[i];
      total_size += sizeof(storage_size_t);
    }
  }

  // Write consolidated file, URIs are relative to the array URI.
  std::vector<uint8_t> data(total_size);
  storage_size_t file_index = 0;
  for (uint64_t i = 0; i < commit_uris.size(); i++) {
    // Add the uri.
    const auto& uri = commit_uris[i];
    std::string relative_uri = uri.to_string().substr(base_uri_size) + "\n";
    memcpy(&data[file_index], relative_uri.data(), relative_uri.size());
    file_index += relative_uri.size();

    // For deletes, read the delete condition to the output file.
    if (stdx::string::ends_with(
            uri.to_string(), constants::delete_file_suffix)) {
      memcpy(&data[file_index], &file_sizes[i], sizeof(storage_size_t));
      file_index += sizeof(storage_size_t);
      throw_if_not_ok(vfs()->read(uri, 0, &data[file_index], file_sizes[i]));
      file_index += file_sizes[i];
    }
  }

  // Write the file to storage.
  URI consolidated_commits_uri =
      array_dir.get_commits_dir(write_version)
          .join_path(name + constants::con_commits_file_suffix);
  throw_if_not_ok(
      vfs()->write(consolidated_commits_uri, data.data(), data.size()));
  throw_if_not_ok(vfs()->close_file(consolidated_commits_uri));
}

void StorageManagerCanonical::delete_array(const char* array_name) {
  if (array_name == nullptr) {
    throw std::invalid_argument("[delete_array] Array name cannot be null");
  }

  // Delete fragments and commits
  delete_fragments(array_name, 0, std::numeric_limits<uint64_t>::max());

  auto array_dir = ArrayDirectory(
      resources(), URI(array_name), 0, std::numeric_limits<uint64_t>::max());

  // Delete array metadata, fragment metadata and array schema files
  // Note: metadata files may not be present, try to delete anyway
  vfs()->remove_files(compute_tp(), array_dir.array_meta_uris());
  vfs()->remove_files(compute_tp(), array_dir.fragment_meta_uris());
  vfs()->remove_files(compute_tp(), array_dir.array_schema_uris());

  // Delete all tiledb child directories
  // Note: using vfs()->ls() here could delete user data
  std::vector<URI> dirs;
  auto parent_dir = array_dir.uri().c_str();
  for (auto array_dir_name : constants::array_dir_names) {
    dirs.emplace_back(URI(parent_dir + array_dir_name));
  }
  vfs()->remove_dirs(compute_tp(), dirs);
}

void StorageManagerCanonical::delete_fragments(
    const char* array_name, uint64_t timestamp_start, uint64_t timestamp_end) {
  if (array_name == nullptr) {
    throw std::invalid_argument("[delete_fragments] Array name cannot be null");
  }

  auto array_dir = ArrayDirectory(
      resources(), URI(array_name), timestamp_start, timestamp_end);

  // Get the fragment URIs to be deleted
  auto filtered_fragment_uris = array_dir.filtered_fragment_uris(true);
  const auto& fragment_uris = filtered_fragment_uris.fragment_uris();

  // Retrieve commit uris to delete and ignore
  std::vector<URI> commit_uris_to_delete;
  std::vector<URI> commit_uris_to_ignore;
  for (auto& fragment : fragment_uris) {
    auto commit_uri = array_dir.get_commit_uri(fragment.uri_);
    commit_uris_to_delete.emplace_back(commit_uri);
    if (array_dir.consolidated_commit_uris_set().count(commit_uri.c_str()) !=
        0) {
      commit_uris_to_ignore.emplace_back(commit_uri);
    }
  }

  // Write ignore file
  if (commit_uris_to_ignore.size() != 0) {
    array_dir.write_commit_ignore_file(commit_uris_to_ignore);
  }

  // Delete fragments and commits
  throw_if_not_ok(
      parallel_for(compute_tp(), 0, fragment_uris.size(), [&](size_t i) {
        RETURN_NOT_OK(vfs()->remove_dir(fragment_uris[i].uri_));
        bool is_file = false;
        RETURN_NOT_OK(vfs()->is_file(commit_uris_to_delete[i], &is_file));
        if (is_file) {
          RETURN_NOT_OK(vfs()->remove_file(commit_uris_to_delete[i]));
        }
        return Status::Ok();
      }));
}

void StorageManagerCanonical::delete_group(const char* group_name) {
  if (group_name == nullptr) {
    throw Status_StorageManagerError(
        "[delete_group] Group name cannot be null");
  }

  auto group_dir = GroupDirectory(
      vfs(),
      compute_tp(),
      URI(group_name),
      0,
      std::numeric_limits<uint64_t>::max());

  // Delete the group detail, group metadata and group files
  vfs()->remove_files(compute_tp(), group_dir.group_detail_uris());
  vfs()->remove_files(compute_tp(), group_dir.group_meta_uris());
  vfs()->remove_files(compute_tp(), group_dir.group_meta_uris_to_vacuum());
  vfs()->remove_files(compute_tp(), group_dir.group_meta_vac_uris_to_vacuum());
  vfs()->remove_files(compute_tp(), group_dir.group_file_uris());

  // Delete all tiledb child directories
  // Note: using vfs()->ls() here could delete user data
  std::vector<URI> dirs;
  auto parent_dir = group_dir.uri().c_str();
  for (auto group_dir_name : constants::group_dir_names) {
    dirs.emplace_back(URI(parent_dir + group_dir_name));
  }
  vfs()->remove_dirs(compute_tp(), dirs);
}

void StorageManagerCanonical::array_vacuum(
    const char* array_name, const Config& config) {
  auto mode = Consolidator::mode_from_config(config, true);
  auto consolidator = Consolidator::create(mode, config, this);
  consolidator->vacuum(array_name);
}

Status StorageManagerCanonical::array_metadata_consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const Config& config) {
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

  // Get encryption key from config
  std::string encryption_key_from_cfg;
  if (!encryption_key) {
    bool found = false;
    encryption_key_from_cfg = config.get("sm.encryption_key", &found);
    assert(found);
  }

  if (!encryption_key_from_cfg.empty()) {
    encryption_key = encryption_key_from_cfg.c_str();
    std::string encryption_type_from_cfg;
    bool found = false;
    encryption_type_from_cfg = config.get("sm.encryption_type", &found);
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
  auto consolidator =
      Consolidator::create(ConsolidationMode::ARRAY_META, config, this);
  return consolidator->consolidate(
      array_name, encryption_type, encryption_key, key_length);
}

Status StorageManagerCanonical::array_create(
    const URI& array_uri,
    const shared_ptr<ArraySchema>& array_schema,
    const EncryptionKey& encryption_key) {
  // Check array schema
  if (array_schema == nullptr) {
    return logger_->status(
        Status_StorageManagerError("Cannot create array; Empty array schema"));
  }

  // Check if array exists
  bool exists = is_array(array_uri);
  if (exists)
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot create array; Array '") + array_uri.c_str() +
        "' already exists"));

  std::lock_guard<std::mutex> lock{object_create_mtx_};
  array_schema->set_array_uri(array_uri);
  RETURN_NOT_OK(array_schema->generate_uri());
  RETURN_NOT_OK(array_schema->check());

  // Create array directory
  RETURN_NOT_OK(vfs()->create_dir(array_uri));

  // Create array schema directory
  URI array_schema_dir_uri =
      array_uri.join_path(constants::array_schema_dir_name);
  RETURN_NOT_OK(vfs()->create_dir(array_schema_dir_uri));

  // Create the enumerations directory inside the array schema directory
  URI array_enumerations_uri =
      array_schema_dir_uri.join_path(constants::array_enumerations_dir_name);
  RETURN_NOT_OK(vfs()->create_dir(array_enumerations_uri));

  // Create commit directory
  URI array_commit_uri = array_uri.join_path(constants::array_commits_dir_name);
  RETURN_NOT_OK(vfs()->create_dir(array_commit_uri));

  // Create fragments directory
  URI array_fragments_uri =
      array_uri.join_path(constants::array_fragments_dir_name);
  RETURN_NOT_OK(vfs()->create_dir(array_fragments_uri));

  // Create array metadata directory
  URI array_metadata_uri =
      array_uri.join_path(constants::array_metadata_dir_name);
  RETURN_NOT_OK(vfs()->create_dir(array_metadata_uri));

  // Create fragment metadata directory
  URI array_fragment_metadata_uri =
      array_uri.join_path(constants::array_fragment_meta_dir_name);
  RETURN_NOT_OK(vfs()->create_dir(array_fragment_metadata_uri));

  // Create dimension label directory
  URI array_dimension_labels_uri =
      array_uri.join_path(constants::array_dimension_labels_dir_name);
  RETURN_NOT_OK(vfs()->create_dir(array_dimension_labels_uri));

  // Get encryption key from config
  Status st;
  if (encryption_key.encryption_type() == EncryptionType::NO_ENCRYPTION) {
    bool found = false;
    std::string encryption_key_from_cfg =
        config_.get("sm.encryption_key", &found);
    assert(found);
    std::string encryption_type_from_cfg =
        config_.get("sm.encryption_type", &found);
    assert(found);
    auto&& [st_enc, etc] = encryption_type_enum(encryption_type_from_cfg);
    RETURN_NOT_OK(st_enc);
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
    throw_if_not_ok(vfs()->remove_dir(array_uri));
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
    return rest_client()->post_array_schema_evolution_to_rest(
        array_uri, schema_evolution);
  }

  // Load URIs from the array directory
  tiledb::sm::ArrayDirectory array_dir{
      resources(),
      array_uri,
      0,
      UINT64_MAX,
      tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY};

  // Check if array exists
  if (!is_array(array_uri)) {
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot evolve array; Array '") + array_uri.c_str() +
        "' not exists"));
  }

  auto&& array_schema = array_dir.load_array_schema_latest(encryption_key);

  // Evolve schema
  auto array_schema_evolved = schema_evolution->evolve_schema(array_schema);

  Status st = store_array_schema(array_schema_evolved, encryption_key);
  if (!st.ok()) {
    logger_->status_no_return_value(st);
    return logger_->status(Status_StorageManagerError(
        "Cannot evolve schema;  Not able to store evolved array schema."));
  }

  return Status::Ok();
}

Status StorageManagerCanonical::array_upgrade_version(
    const URI& array_uri, const Config& override_config) {
  // Check if array exists
  if (!is_array(array_uri))
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot upgrade array; Array '") + array_uri.c_str() +
        "' does not exist"));

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

  auto&& array_schema = array_dir.load_array_schema_latest(encryption_key_cfg);

  if (array_schema->version() < constants::format_version) {
    auto st = array_schema->generate_uri();
    RETURN_NOT_OK_ELSE(st, logger_->status_no_return_value(st));
    array_schema->set_version(constants::format_version);

    // Create array schema directory if necessary
    URI array_schema_dir_uri =
        array_uri.join_path(constants::array_schema_dir_name);
    st = vfs()->create_dir(array_schema_dir_uri);
    RETURN_NOT_OK_ELSE(st, logger_->status_no_return_value(st));

    // Store array schema
    st = store_array_schema(array_schema, encryption_key_cfg);
    RETURN_NOT_OK_ELSE(st, logger_->status_no_return_value(st));

    // Create commit directory if necessary
    URI array_commit_uri =
        array_uri.join_path(constants::array_commits_dir_name);
    RETURN_NOT_OK_ELSE(
        vfs()->create_dir(array_commit_uri),
        logger_->status_no_return_value(st));

    // Create fragments directory if necessary
    URI array_fragments_uri =
        array_uri.join_path(constants::array_fragments_dir_name);
    st = vfs()->create_dir(array_fragments_uri);
    RETURN_NOT_OK_ELSE(st, logger_->status_no_return_value(st));

    // Create fragment metadata directory if necessary
    URI array_fragment_metadata_uri =
        array_uri.join_path(constants::array_fragment_meta_dir_name);
    st = vfs()->create_dir(array_fragment_metadata_uri);
    RETURN_NOT_OK_ELSE(st, logger_->status_no_return_value(st));
  }

  return Status::Ok();
}

Status StorageManagerCanonical::array_get_non_empty_domain(
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

  QueryType query_type{array->get_query_type()};
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

Status StorageManagerCanonical::array_get_non_empty_domain(
    Array* array, void* domain, bool* is_empty) {
  if (array == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Array object is null"));

  if (!array->array_schema_latest().domain().all_dims_same_type())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Function non-applicable to arrays with "
        "heterogenous dimensions"));

  if (!array->array_schema_latest().domain().all_dims_fixed())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Function non-applicable to arrays with "
        "variable-sized dimensions"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));
  if (*is_empty)
    return Status::Ok();

  const auto& array_schema = array->array_schema_latest();
  auto dim_num = array_schema.dim_num();
  auto domain_c = (unsigned char*)domain;
  uint64_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    std::memcpy(&domain_c[offset], dom[d].data(), dom[d].size());
    offset += dom[d].size();
  }

  return Status::Ok();
}

Status StorageManagerCanonical::array_get_non_empty_domain_from_index(
    Array* array, unsigned idx, void* domain, bool* is_empty) {
  // Check if array is open - must be open for reads
  if (!array->is_open())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Array is not open"));

  // For easy reference
  const auto& array_schema = array->array_schema_latest();
  auto& array_domain{array_schema.domain()};

  // Sanity checks
  if (idx >= array_schema.dim_num())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (array_domain.dimension_ptr(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain.dimension_ptr(idx)->name();
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

Status StorageManagerCanonical::array_get_non_empty_domain_from_name(
    Array* array, const char* name, void* domain, bool* is_empty) {
  // Sanity check
  if (name == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension name"));

  // Check if array is open - must be open for reads
  if (!array->is_open())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Array is not open"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  const auto& array_schema = array->array_schema_latest();
  auto& array_domain{array_schema.domain()};
  auto dim_num = array_schema.dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema.dimension_ptr(d)->name()};
    if (name == dim_name) {
      // Sanity check
      if (array_domain.dimension_ptr(d)->var_size()) {
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

Status StorageManagerCanonical::array_get_non_empty_domain_var_size_from_index(
    Array* array,
    unsigned idx,
    uint64_t* start_size,
    uint64_t* end_size,
    bool* is_empty) {
  // For easy reference
  const auto& array_schema = array->array_schema_latest();
  auto& array_domain{array_schema.domain()};

  // Sanity checks
  if (idx >= array_schema.dim_num())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (!array_domain.dimension_ptr(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain.dimension_ptr(idx)->name();
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

Status StorageManagerCanonical::array_get_non_empty_domain_var_size_from_name(
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

  const auto& array_schema = array->array_schema_latest();
  auto& array_domain{array_schema.domain()};
  auto dim_num = array_schema.dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema.dimension_ptr(d)->name()};
    if (name == dim_name) {
      // Sanity check
      if (!array_domain.dimension_ptr(d)->var_size()) {
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

Status StorageManagerCanonical::array_get_non_empty_domain_var_from_index(
    Array* array, unsigned idx, void* start, void* end, bool* is_empty) {
  // For easy reference
  const auto& array_schema = array->array_schema_latest();
  auto& array_domain{array_schema.domain()};

  // Sanity checks
  if (idx >= array_schema.dim_num())
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension index"));
  if (!array_domain.dimension_ptr(idx)->var_size()) {
    std::string errmsg = "Cannot get non-empty domain; Dimension '";
    errmsg += array_domain.dimension_ptr(idx)->name();
    errmsg += "' is fixed-sized";
    return logger_->status(Status_StorageManagerError(errmsg));
  }

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  if (*is_empty)
    return Status::Ok();

  auto start_str = dom[idx].start_str();
  std::memcpy(start, start_str.data(), start_str.size());
  auto end_str = dom[idx].end_str();
  std::memcpy(end, end_str.data(), end_str.size());

  return Status::Ok();
}

Status StorageManagerCanonical::array_get_non_empty_domain_var_from_name(
    Array* array, const char* name, void* start, void* end, bool* is_empty) {
  // Sanity check
  if (name == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot get non-empty domain; Invalid dimension name"));

  NDRange dom;
  RETURN_NOT_OK(array_get_non_empty_domain(array, &dom, is_empty));

  const auto& array_schema = array->array_schema_latest();
  auto& array_domain{array_schema.domain()};
  auto dim_num = array_schema.dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema.dimension_ptr(d)->name()};
    if (name == dim_name) {
      // Sanity check
      if (!array_domain.dimension_ptr(d)->var_size()) {
        std::string errmsg = "Cannot get non-empty domain; Dimension '";
        errmsg += dim_name + "' is fixed-sized";
        return logger_->status(Status_StorageManagerError(errmsg));
      }

      if (!*is_empty) {
        auto start_str = dom[d].start_str();
        std::memcpy(start, start_str.data(), start_str.size());
        auto end_str = dom[d].end_str();
        std::memcpy(end, end_str.data(), end_str.size());
      }

      return Status::Ok();
    }
  }

  return logger_->status(Status_StorageManagerError(
      std::string("Cannot get non-empty domain; Dimension name '") + name +
      "' does not exist"));
}

Status StorageManagerCanonical::array_get_encryption(
    const ArrayDirectory& array_dir, EncryptionType* encryption_type) {
  const URI& uri = array_dir.uri();

  if (uri.is_invalid()) {
    return logger_->status(Status_StorageManagerError(
        "Cannot get array encryption; Invalid array URI"));
  }

  const URI& schema_uri = array_dir.latest_array_schema_uri();

  // Read tile header
  auto&& header =
      GenericTileIO::read_generic_tile_header(resources_, schema_uri, 0);
  *encryption_type = static_cast<EncryptionType>(header.encryption_type);

  return Status::Ok();
}

Status StorageManagerCanonical::async_push_query(Query* query) {
  cancelable_tasks_.execute(
      compute_tp(),
      [this, query]() {
        // Process query.
        Status st = query_submit(query);
        if (!st.ok())
          logger_->status_no_return_value(st);
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
    throw_if_not_ok(resources().vfs().cancel_all_tasks());

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

const Config& StorageManagerCanonical::config() const {
  return config_;
}

void StorageManagerCanonical::decrement_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_--;
  queries_in_progress_cv_.notify_all();
}

Status StorageManagerCanonical::object_remove(const char* path) const {
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

  return vfs()->remove_dir(uri);
}

Status StorageManagerCanonical::object_move(
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

  return vfs()->move_dir(old_uri, new_uri);
}

const std::unordered_map<std::string, std::string>&
StorageManagerCanonical::tags() const {
  return tags_;
}

Status StorageManagerCanonical::group_create(const std::string& group_uri) {
  // Create group URI
  URI uri(group_uri);
  if (uri.is_invalid())
    return logger_->status(Status_StorageManagerError(
        "Cannot create group '" + group_uri + "'; Invalid group URI"));

  // Check if group exists
  bool exists;
  RETURN_NOT_OK(is_group(uri, &exists));
  if (exists)
    return logger_->status(Status_StorageManagerError(
        std::string("Cannot create group; Group '") + uri.c_str() +
        "' already exists"));

  std::lock_guard<std::mutex> lock{object_create_mtx_};

  if (uri.is_tiledb()) {
    Group group(uri, this);
    RETURN_NOT_OK(rest_client()->post_group_create_to_rest(uri, &group));
    return Status::Ok();
  }

  // Create group directory
  RETURN_NOT_OK(vfs()->create_dir(uri));

  // Create group file
  URI group_filename = uri.join_path(constants::group_filename);
  RETURN_NOT_OK(vfs()->touch(group_filename));

  // Create metadata folder
  RETURN_NOT_OK(
      vfs()->create_dir(uri.join_path(constants::group_metadata_dir_name)));

  // Create group detail folder
  RETURN_NOT_OK(
      vfs()->create_dir(uri.join_path(constants::group_detail_dir_name)));
  return Status::Ok();
}

void StorageManagerCanonical::increment_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_++;
  queries_in_progress_cv_.notify_all();
}

bool StorageManagerCanonical::is_array(const URI& uri) const {
  // Handle remote array
  if (uri.is_tiledb()) {
    auto&& [st, exists] = rest_client()->check_array_exists_from_rest(uri);
    throw_if_not_ok(st);
    assert(exists.has_value());
    return exists.value();
  } else {
    // Check if the schema directory exists or not
    bool dir_exists = false;
    throw_if_not_ok(vfs()->is_dir(
        uri.join_path(constants::array_schema_dir_name), &dir_exists));

    if (dir_exists) {
      return true;
    }

    bool schema_exists = false;
    // If there is no schema directory, we check schema file
    throw_if_not_ok(vfs()->is_file(
        uri.join_path(constants::array_schema_filename), &schema_exists));
    return schema_exists;
  }

  // TODO: mark unreachable
}

Status StorageManagerCanonical::is_group(const URI& uri, bool* is_group) const {
  // Handle remote array
  if (uri.is_tiledb()) {
    auto&& [st, exists] = rest_client()->check_group_exists_from_rest(uri);
    RETURN_NOT_OK(st);
    *is_group = *exists;
  } else {
    // Check for new group details directory
    RETURN_NOT_OK(vfs()->is_dir(
        uri.join_path(constants::group_detail_dir_name), is_group));

    if (*is_group) {
      return Status::Ok();
    }

    // Fall back to older group file to check for legacy (pre-format 12) groups
    RETURN_NOT_OK(
        vfs()->is_file(uri.join_path(constants::group_filename), is_group));
  }
  return Status::Ok();
}

void StorageManagerCanonical::load_array_metadata(
    const ArrayDirectory& array_dir,
    const EncryptionKey& encryption_key,
    Metadata* metadata) {
  auto timer_se = stats()->start_timer("sm_load_array_metadata");

  // Special case
  if (metadata == nullptr) {
    return;
  }

  // Determine which array metadata to load
  const auto& array_metadata_to_load = array_dir.array_meta_uris();

  auto metadata_num = array_metadata_to_load.size();
  std::vector<shared_ptr<Tile>> metadata_tiles(metadata_num);
  throw_if_not_ok(parallel_for(compute_tp(), 0, metadata_num, [&](size_t m) {
    const auto& uri = array_metadata_to_load[m].uri_;

    auto&& tile = GenericTileIO::load(resources_, uri, 0, encryption_key);
    metadata_tiles[m] = tdb::make_shared<Tile>(HERE(), std::move(tile));

    return Status::Ok();
  }));

  // Compute array metadata size for the statistics
  uint64_t meta_size = 0;
  for (const auto& t : metadata_tiles) {
    meta_size += t->size();
  }
  stats()->add_counter("read_array_meta_size", meta_size);

  *metadata = Metadata::deserialize(metadata_tiles);

  // Sets the loaded metadata URIs
  metadata->set_loaded_metadata_uris(array_metadata_to_load);
}

tuple<
    Status,
    optional<std::vector<QueryCondition>>,
    optional<std::vector<std::vector<UpdateValue>>>>
StorageManagerCanonical::load_delete_and_update_conditions(const Array& array) {
  auto& locations = array.array_directory().delete_and_update_tiles_location();
  auto conditions = std::vector<QueryCondition>(locations.size());
  auto update_values = std::vector<std::vector<UpdateValue>>(locations.size());

  auto status = parallel_for(compute_tp(), 0, locations.size(), [&](size_t i) {
    // Get condition marker.
    auto& uri = locations[i].uri();

    // Read the condition from storage.
    auto&& tile = GenericTileIO::load(
        resources_, uri, locations[i].offset(), *array.encryption_key());

    if (tiledb::sm::utils::parse::ends_with(
            locations[i].condition_marker(),
            tiledb::sm::constants::delete_file_suffix)) {
      conditions[i] =
          tiledb::sm::deletes_and_updates::serialization::deserialize_condition(
              i, locations[i].condition_marker(), tile.data(), tile.size());
    } else if (tiledb::sm::utils::parse::ends_with(
                   locations[i].condition_marker(),
                   tiledb::sm::constants::update_file_suffix)) {
      auto&& [cond, uvs] = tiledb::sm::deletes_and_updates::serialization::
          deserialize_update_condition_and_values(
              i, locations[i].condition_marker(), tile.data(), tile.size());
      conditions[i] = std::move(cond);
      update_values[i] = std::move(uvs);
    } else {
      throw Status_StorageManagerError("Unknown condition marker extension");
    }

    throw_if_not_ok(conditions[i].check(array.array_schema_latest()));
    return Status::Ok();
  });
  RETURN_NOT_OK_TUPLE(status, nullopt, nullopt);

  return {Status::Ok(), conditions, update_values};
}

Status StorageManagerCanonical::object_type(
    const URI& uri, ObjectType* type) const {
  URI dir_uri = uri;
  if (uri.is_s3() || uri.is_azure() || uri.is_gcs()) {
    // Always add a trailing '/' in the S3/Azure/GCS case so that listing the
    // URI as a directory will work as expected. Listing a non-directory object
    // is not an error for S3/Azure/GCS.
    auto uri_str = uri.to_string();
    dir_uri =
        URI(utils::parse::ends_with(uri_str, "/") ? uri_str : (uri_str + "/"));
  } else if (!uri.is_tiledb()) {
    // For non public cloud backends, listing a non-directory is an error.
    bool is_dir = false;
    RETURN_NOT_OK(vfs()->is_dir(uri, &is_dir));
    if (!is_dir) {
      *type = ObjectType::INVALID;
      return Status::Ok();
    }
  }
  bool exists = is_array(uri);
  if (exists) {
    *type = ObjectType::ARRAY;
    return Status::Ok();
  }

  RETURN_NOT_OK(is_group(uri, &exists));
  if (exists) {
    *type = ObjectType::GROUP;
    return Status::Ok();
  }

  *type = ObjectType::INVALID;
  return Status::Ok();
}

Status StorageManagerCanonical::object_iter_begin(
    ObjectIter** obj_iter, const char* path, WalkOrder order) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    return logger_->status(Status_StorageManagerError(
        "Cannot create object iterator; Invalid input path"));
  }

  // Get all contents of path
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs()->ls(path_uri, &uris));

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

Status StorageManagerCanonical::object_iter_begin(
    ObjectIter** obj_iter, const char* path) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    return logger_->status(Status_StorageManagerError(
        "Cannot create object iterator; Invalid input path"));
  }

  // Get all contents of path
  std::vector<URI> uris;
  RETURN_NOT_OK(vfs()->ls(path_uri, &uris));

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
      RETURN_NOT_OK(vfs()->ls(obj_iter->objs_.front(), &uris));
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

Status StorageManagerCanonical::object_iter_next_preorder(
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
  RETURN_NOT_OK(vfs()->ls(front_uri, &uris));

  // Push the new TileDB objects in the front of the iterator's list
  ObjectType obj_type;
  for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
    RETURN_NOT_OK(object_type(*it, &obj_type));
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
  if (rest_client() != nullptr)
    RETURN_NOT_OK(rest_client()->set_header(key, value));

  return Status::Ok();
}

Status StorageManagerCanonical::store_group_detail(
    const URI& group_detail_folder_uri,
    const URI& group_detail_uri,
    tdb_shared_ptr<GroupDetails> group,
    const EncryptionKey& encryption_key) {
  // Serialize
  SizeComputationSerializer size_computation_serializer;
  group->serialize(size_computation_serializer);

  WriterTile tile{WriterTile::from_generic(size_computation_serializer.size())};

  Serializer serializer(tile.data(), tile.size());
  group->serialize(serializer);

  stats()->add_counter("write_group_size", tile.size());

  // Check if the array schema directory exists
  // If not create it, this is caused by a pre-v10 array
  bool group_detail_dir_exists = false;
  RETURN_NOT_OK(
      vfs()->is_dir(group_detail_folder_uri, &group_detail_dir_exists));
  if (!group_detail_dir_exists)
    RETURN_NOT_OK(vfs()->create_dir(group_detail_folder_uri));

  RETURN_NOT_OK(
      store_data_to_generic_tile(tile, group_detail_uri, encryption_key));

  return Status::Ok();
}

Status StorageManagerCanonical::store_array_schema(
    const shared_ptr<ArraySchema>& array_schema,
    const EncryptionKey& encryption_key) {
  const URI schema_uri = array_schema->uri();

  // Serialize
  SizeComputationSerializer size_computation_serializer;
  array_schema->serialize(size_computation_serializer);

  WriterTile tile{WriterTile::from_generic(size_computation_serializer.size())};
  Serializer serializer(tile.data(), tile.size());
  array_schema->serialize(serializer);

  stats()->add_counter("write_array_schema_size", tile.size());

  // Delete file if it exists already
  bool exists;
  RETURN_NOT_OK(vfs()->is_file(schema_uri, &exists));
  if (exists)
    RETURN_NOT_OK(vfs()->remove_file(schema_uri));

  // Check if the array schema directory exists
  // If not create it, this is caused by a pre-v10 array
  bool schema_dir_exists = false;
  URI array_schema_dir_uri =
      array_schema->array_uri().join_path(constants::array_schema_dir_name);
  RETURN_NOT_OK(vfs()->is_dir(array_schema_dir_uri, &schema_dir_exists));

  if (!schema_dir_exists)
    RETURN_NOT_OK(vfs()->create_dir(array_schema_dir_uri));

  RETURN_NOT_OK(store_data_to_generic_tile(tile, schema_uri, encryption_key));

  // Create the `__enumerations` directory under `__schema` if it doesn't
  // exist. This might happen if someone tries to add an enumeration to an
  // array created before version 19.
  bool enumerations_dir_exists = false;
  URI array_enumerations_dir_uri =
      array_schema_dir_uri.join_path(constants::array_enumerations_dir_name);
  RETURN_NOT_OK(
      vfs()->is_dir(array_enumerations_dir_uri, &enumerations_dir_exists));

  if (!enumerations_dir_exists) {
    RETURN_NOT_OK(vfs()->create_dir(array_enumerations_dir_uri));
  }

  // Serialize all enumerations into the `__enumerations` directory
  for (auto& enmr_name : array_schema->get_loaded_enumeration_names()) {
    auto enmr = array_schema->get_enumeration(enmr_name);
    if (enmr == nullptr) {
      return logger_->status(Status_StorageManagerError(
          "Error serializing enumeration; Loaded enumeration is null"));
    }

    SizeComputationSerializer enumeration_size_serializer;
    enmr->serialize(enumeration_size_serializer);

    WriterTile tile{
        WriterTile::from_generic(enumeration_size_serializer.size())};
    Serializer serializer(tile.data(), tile.size());
    enmr->serialize(serializer);

    auto abs_enmr_uri = array_enumerations_dir_uri.join_path(enmr->path_name());
    RETURN_NOT_OK(
        store_data_to_generic_tile(tile, abs_enmr_uri, encryption_key));
  }

  return Status::Ok();
}

Status StorageManagerCanonical::store_metadata(
    const URI& uri, const EncryptionKey& encryption_key, Metadata* metadata) {
  auto timer_se = stats()->start_timer("write_meta");

  // Trivial case
  if (metadata == nullptr)
    return Status::Ok();

  // Serialize array metadata

  SizeComputationSerializer size_computation_serializer;
  metadata->serialize(size_computation_serializer);

  // Do nothing if there are no metadata to write
  if (0 == size_computation_serializer.size()) {
    return Status::Ok();
  }
  WriterTile tile{WriterTile::from_generic(size_computation_serializer.size())};
  Serializer serializer(tile.data(), tile.size());
  metadata->serialize(serializer);

  stats()->add_counter("write_meta_size", serializer.size());

  // Create a metadata file name
  URI metadata_uri;
  RETURN_NOT_OK(metadata->get_uri(uri, &metadata_uri));

  RETURN_NOT_OK(store_data_to_generic_tile(tile, metadata_uri, encryption_key));

  return Status::Ok();
}

Status StorageManagerCanonical::store_data_to_generic_tile(
    WriterTile& tile, const URI& uri, const EncryptionKey& encryption_key) {
  GenericTileIO tile_io(resources_, uri);
  uint64_t nbytes = 0;
  Status st = tile_io.write_generic(&tile, encryption_key, &nbytes);

  if (st.ok()) {
    st = vfs()->close_file(uri);
  }

  return st;
}

void StorageManagerCanonical::wait_for_zero_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_cv_.wait(
      lck, [this]() { return queries_in_progress_ == 0; });
}

shared_ptr<Logger> StorageManagerCanonical::logger() const {
  return logger_;
}

tuple<Status, optional<shared_ptr<GroupDetails>>>
StorageManagerCanonical::load_group_from_uri(
    const URI& group_uri, const URI& uri, const EncryptionKey& encryption_key) {
  auto timer_se = stats()->start_timer("sm_load_group_from_uri");

  auto&& tile = GenericTileIO::load(resources_, uri, 0, encryption_key);

  stats()->add_counter("read_group_size", tile.size());

  // Deserialize
  Deserializer deserializer(tile.data(), tile.size());
  auto opt_group = GroupDetails::deserialize(deserializer, group_uri);
  return {Status::Ok(), opt_group};
}

tuple<Status, optional<shared_ptr<GroupDetails>>>
StorageManagerCanonical::load_group_from_all_uris(
    const URI& group_uri,
    const std::vector<TimestampedURI>& uris,
    const EncryptionKey& encryption_key) {
  auto timer_se = stats()->start_timer("sm_load_group_from_uri");

  std::vector<shared_ptr<Deserializer>> deserializers;
  for (auto& uri : uris) {
    auto&& tile = GenericTileIO::load(resources_, uri.uri_, 0, encryption_key);

    stats()->add_counter("read_group_size", tile.size());

    // Deserialize
    shared_ptr<Deserializer> deserializer =
        tdb::make_shared<TileDeserializer>(HERE(), std::move(tile));
    deserializers.emplace_back(deserializer);
  }

  auto opt_group = GroupDetails::deserialize(deserializers, group_uri);
  return {Status::Ok(), opt_group};
}

tuple<Status, optional<shared_ptr<GroupDetails>>>
StorageManagerCanonical::load_group_details(
    const shared_ptr<GroupDirectory>& group_directory,
    const EncryptionKey& encryption_key) {
  auto timer_se = stats()->start_timer("sm_load_group_details");
  const URI& latest_group_uri = group_directory->latest_group_details_uri();
  if (latest_group_uri.is_invalid()) {
    // Returning ok because not having the latest group details means the group
    // has just been created and no members have been added yet.
    return {Status::Ok(), std::nullopt};
  }

  // V1 groups did not have the version appended so only have 4 "_"
  // (__<timestamp>_<timestamp>_<uuid>)
  auto part = latest_group_uri.last_path_part();
  if (std::count(part.begin(), part.end(), '_') == 4) {
    return load_group_from_uri(
        group_directory->uri(), latest_group_uri, encryption_key);
  }

  // V2 and newer should loop over all uris all the time to handle deletes at
  // read-time
  return load_group_from_all_uris(
      group_directory->uri(),
      group_directory->group_detail_uris(),
      encryption_key);
}

std::tuple<Status, std::optional<tdb_shared_ptr<GroupDetails>>>
StorageManagerCanonical::group_open_for_reads(Group* group) {
  auto timer_se = stats()->start_timer("group_open_for_reads");

  // Load group data
  auto&& [st, group_deserialized] =
      load_group_details(group->group_directory(), *group->encryption_key());
  RETURN_NOT_OK_TUPLE(st, std::nullopt);

  // Mark the array as now open
  std::lock_guard<std::mutex> lock{open_groups_mtx_};
  open_groups_.insert(group);

  if (group_deserialized.has_value()) {
    return {Status::Ok(), group_deserialized.value()};
  }

  // Return ok because having no members is acceptable if the group has never
  // been written to.
  return {Status::Ok(), std::nullopt};
}

std::tuple<Status, std::optional<tdb_shared_ptr<GroupDetails>>>
StorageManagerCanonical::group_open_for_writes(Group* group) {
  auto timer_se = stats()->start_timer("group_open_for_writes");

  // Load group data
  auto&& [st, group_deserialized] =
      load_group_details(group->group_directory(), *group->encryption_key());
  RETURN_NOT_OK_TUPLE(st, std::nullopt);

  // Mark the array as now open
  std::lock_guard<std::mutex> lock{open_groups_mtx_};
  open_groups_.insert(group);

  if (group_deserialized.has_value()) {
    return {Status::Ok(), group_deserialized.value()};
  }

  // Return ok because having no members is acceptable if the group has never
  // been written to.
  return {Status::Ok(), std::nullopt};
}

void StorageManagerCanonical::load_group_metadata(
    const shared_ptr<GroupDirectory>& group_dir,
    const EncryptionKey& encryption_key,
    Metadata* metadata) {
  auto timer_se = stats()->start_timer("sm_load_group_metadata");

  // Special case
  if (metadata == nullptr) {
    return;
  }

  // Determine which group metadata to load
  const auto& group_metadata_to_load = group_dir->group_meta_uris();

  auto metadata_num = group_metadata_to_load.size();
  // TBD: Might use DynamicArray when it is more capable.
  std::vector<shared_ptr<Tile>> metadata_tiles(metadata_num);
  throw_if_not_ok(parallel_for(compute_tp(), 0, metadata_num, [&](size_t m) {
    const auto& uri = group_metadata_to_load[m].uri_;

    auto&& tile = GenericTileIO::load(resources_, uri, 0, encryption_key);
    metadata_tiles[m] = tdb::make_shared<Tile>(HERE(), std::move(tile));

    return Status::Ok();
  }));

  // Compute array metadata size for the statistics
  uint64_t meta_size = 0;
  for (const auto& t : metadata_tiles) {
    meta_size += t->size();
  }
  stats()->add_counter("read_array_meta_size", meta_size);

  // Copy the deserialized metadata into the original Metadata object
  *metadata = Metadata::deserialize(metadata_tiles);
  metadata->set_loaded_metadata_uris(group_metadata_to_load);
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

tuple<Status, optional<std::vector<shared_ptr<FragmentMetadata>>>>
StorageManagerCanonical::load_fragment_metadata(
    MemoryTracker* memory_tracker,
    const shared_ptr<const ArraySchema>& array_schema_latest,
    const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
        array_schemas_all,
    const EncryptionKey& encryption_key,
    const std::vector<TimestampedURI>& fragments_to_load,
    const std::unordered_map<std::string, std::pair<Tile*, uint64_t>>&
        offsets) {
  auto timer_se = stats()->start_timer("sm_load_fragment_metadata");

  // Load the metadata for each fragment
  auto fragment_num = fragments_to_load.size();
  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata;
  fragment_metadata.resize(fragment_num);
  auto status = parallel_for(compute_tp(), 0, fragment_num, [&](size_t f) {
    const auto& sf = fragments_to_load[f];

    URI coords_uri =
        sf.uri_.join_path(constants::coords + constants::file_suffix);

    auto name = sf.uri_.remove_trailing_slash().last_path_part();
    uint32_t f_version;
    RETURN_NOT_OK(utils::parse::get_fragment_name_version(name, &f_version));

    // Note that the fragment metadata version is >= the array schema
    // version. Therefore, the check below is defensive and will always
    // ensure backwards compatibility.
    shared_ptr<FragmentMetadata> metadata;
    if (f_version == 1) {  // This is equivalent to format version <=2
      bool sparse;
      RETURN_NOT_OK(vfs()->is_file(coords_uri, &sparse));
      metadata = make_shared<FragmentMetadata>(
          HERE(),
          this,
          memory_tracker,
          array_schema_latest,
          sf.uri_,
          sf.timestamp_range_,
          !sparse);
    } else {  // Format version > 2
      metadata = make_shared<FragmentMetadata>(
          HERE(),
          this,
          memory_tracker,
          array_schema_latest,
          sf.uri_,
          sf.timestamp_range_);
    }

    // Potentially find the basic fragment metadata in the consolidated
    // metadata buffer
    Tile* fragment_metadata_tile = nullptr;
    uint64_t offset = 0;

    auto it = offsets.end();
    if (metadata->format_version() >= 9) {
      it = offsets.find(name);
    } else {
      it = offsets.find(sf.uri_.to_string());
    }
    if (it != offsets.end()) {
      fragment_metadata_tile = it->second.first;
      offset = it->second.second;
    }

    // Load fragment metadata
    RETURN_NOT_OK(metadata->load(
        encryption_key, fragment_metadata_tile, offset, array_schemas_all));

    fragment_metadata[f] = metadata;
    return Status::Ok();
  });
  RETURN_NOT_OK_TUPLE(status, nullopt);

  return {Status::Ok(), fragment_metadata};
}

tuple<
    Status,
    optional<Tile>,
    optional<std::vector<std::pair<std::string, uint64_t>>>>
StorageManagerCanonical::load_consolidated_fragment_meta(
    const URI& uri, const EncryptionKey& enc_key) {
  auto timer_se = stats()->start_timer("sm_read_load_consolidated_frag_meta");

  // No consolidated fragment metadata file
  if (uri.to_string().empty())
    return {Status::Ok(), nullopt, nullopt};

  auto&& tile = GenericTileIO::load(resources_, uri, 0, enc_key);

  stats()->add_counter("consolidated_frag_meta_size", tile.size());

  uint32_t fragment_num;
  Deserializer deserializer(tile.data(), tile.size());
  fragment_num = deserializer.read<uint32_t>();

  uint64_t name_size, offset;
  std::string name;
  std::vector<std::pair<std::string, uint64_t>> ret;
  ret.reserve(fragment_num);
  for (uint32_t f = 0; f < fragment_num; ++f) {
    name_size = deserializer.read<uint64_t>();
    name.resize(name_size);
    deserializer.read(&name[0], name_size);
    offset = deserializer.read<uint64_t>();
    ret.emplace_back(name, offset);
  }

  return {Status::Ok(), std::move(tile), ret};
}

const ArrayDirectory::FilteredFragmentUris
StorageManagerCanonical::load_filtered_fragment_uris(
    const bool dense, const ArrayDirectory& array_dir) {
  auto timer_se = stats()->start_timer("sm_load_filtered_fragment_uris");

  return array_dir.filtered_fragment_uris(dense);
}

Status StorageManagerCanonical::set_default_tags() {
  const auto version = std::to_string(constants::library_version[0]) + "." +
                       std::to_string(constants::library_version[1]) + "." +
                       std::to_string(constants::library_version[2]);

  RETURN_NOT_OK(set_tag("x-tiledb-version", version));
  RETURN_NOT_OK(set_tag("x-tiledb-api-language", "c"));

  return Status::Ok();
}

Status StorageManagerCanonical::group_metadata_consolidate(
    const char* group_name, const Config& config) {
  // Check group URI
  URI group_uri(group_name);
  if (group_uri.is_invalid()) {
    return logger_->status(Status_StorageManagerError(
        "Cannot consolidate group metadata; Invalid URI"));
  }
  // Check if group exists
  ObjectType obj_type;
  RETURN_NOT_OK(object_type(group_uri, &obj_type));

  if (obj_type != ObjectType::GROUP) {
    return logger_->status(Status_StorageManagerError(
        "Cannot consolidate group metadata; Group does not exist"));
  }

  // Consolidate
  // Encryption credentials are loaded by Group from config
  auto consolidator =
      Consolidator::create(ConsolidationMode::GROUP_META, config, this);
  return consolidator->consolidate(
      group_name, EncryptionType::NO_ENCRYPTION, nullptr, 0);
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
  throw_if_not_ok(object_type(group_uri, &obj_type));

  if (obj_type != ObjectType::GROUP) {
    throw Status_StorageManagerError(
        "Cannot vacuum group metadata; Group does not exist");
  }

  auto consolidator =
      Consolidator::create(ConsolidationMode::GROUP_META, config, this);
  consolidator->vacuum(group_name);
}

}  // namespace sm
}  // namespace tiledb
