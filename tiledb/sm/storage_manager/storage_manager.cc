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
#include "tiledb/sm/query/deletes_and_updates/serialization.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/update_value.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/storage_format/uri/generate_uri.h"
#include "tiledb/storage_format/uri/parse_uri.h"

#include <algorithm>
#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace tiledb::sm {

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

Status StorageManagerCanonical::group_close_for_reads(Group*) {
  // Closing a group does nothing at present
  return Status::Ok();
}

Status StorageManagerCanonical::group_close_for_writes(Group* group) {
  // Flush the group metadata
  RETURN_NOT_OK(store_metadata(
      group->group_uri(), *group->encryption_key(), group->unsafe_metadata()));

  // Store any changes required
  if (group->group_details()->is_modified()) {
    const URI& group_detail_folder_uri = group->group_detail_uri();
    auto group_detail_uri = group->generate_detail_uri();
    RETURN_NOT_OK(store_group_detail(
        group_detail_folder_uri,
        group_detail_uri,
        group->group_details(),
        *group->encryption_key()));
  }
  return Status::Ok();
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
  array_schema->generate_uri();
  array_schema->check(config_);

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
      RETURN_NOT_OK(encryption_key_cfg.set_key(
          encryption_type_cfg,
          (const void*)encryption_key_from_cfg.c_str(),
          static_cast<uint32_t>(encryption_key_from_cfg.size())));
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

  auto&& array_schema = array_dir.load_array_schema_latest(
      encryption_key, resources_.ephemeral_memory_tracker());

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
        enmr_paths, encryption_key, resources_.create_memory_tracker());

    for (auto enmr : loaded_enmrs) {
      array_schema->store_enumeration(enmr);
    }
  }

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
    auto st = vfs()->create_dir(array_schema_dir_uri);
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

  *domain = array->non_empty_domain();
  *is_empty = domain->empty();

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
    Group group(resources_, uri, this);
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

tuple<
    Status,
    optional<std::vector<QueryCondition>>,
    optional<std::vector<std::vector<UpdateValue>>>>
StorageManagerCanonical::load_delete_and_update_conditions(
    const OpenedArray& opened_array) {
  auto& locations =
      opened_array.array_directory().delete_and_update_tiles_location();
  auto conditions = std::vector<QueryCondition>(locations.size());
  auto update_values = std::vector<std::vector<UpdateValue>>(locations.size());

  auto status = parallel_for(compute_tp(), 0, locations.size(), [&](size_t i) {
    // Get condition marker.
    auto& uri = locations[i].uri();

    // Read the condition from storage.
    auto tile = GenericTileIO::load(
        resources_,
        uri,
        locations[i].offset(),
        *(opened_array.encryption_key()),
        resources_.ephemeral_memory_tracker());

    if (tiledb::sm::utils::parse::ends_with(
            locations[i].condition_marker(),
            tiledb::sm::constants::delete_file_suffix)) {
      conditions[i] =
          tiledb::sm::deletes_and_updates::serialization::deserialize_condition(
              i, locations[i].condition_marker(), tile->data(), tile->size());
    } else if (tiledb::sm::utils::parse::ends_with(
                   locations[i].condition_marker(),
                   tiledb::sm::constants::update_file_suffix)) {
      auto&& [cond, uvs] = tiledb::sm::deletes_and_updates::serialization::
          deserialize_update_condition_and_values(
              i, locations[i].condition_marker(), tile->data(), tile->size());
      conditions[i] = std::move(cond);
      update_values[i] = std::move(uvs);
    } else {
      throw Status_StorageManagerError("Unknown condition marker extension");
    }

    throw_if_not_ok(conditions[i].check(opened_array.array_schema_latest()));
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
  auto members = group->members_to_serialize();
  SizeComputationSerializer size_computation_serializer;
  group->serialize(members, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(),
      resources_.ephemeral_memory_tracker())};

  Serializer serializer(tile->data(), tile->size());
  group->serialize(members, serializer);

  stats()->add_counter("write_group_size", tile->size());

  // Check if the array schema directory exists
  // If not create it, this is caused by a pre-v10 array
  bool group_detail_dir_exists = false;
  RETURN_NOT_OK(
      vfs()->is_dir(group_detail_folder_uri, &group_detail_dir_exists));
  if (!group_detail_dir_exists)
    RETURN_NOT_OK(vfs()->create_dir(group_detail_folder_uri));

  GenericTileIO::store_data(resources_, group_detail_uri, tile, encryption_key);

  return Status::Ok();
}

Status StorageManagerCanonical::store_array_schema(
    const shared_ptr<ArraySchema>& array_schema,
    const EncryptionKey& encryption_key) {
  const URI schema_uri = array_schema->uri();

  // Serialize
  SizeComputationSerializer size_computation_serializer;
  array_schema->serialize(size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(),
      resources_.ephemeral_memory_tracker())};
  Serializer serializer(tile->data(), tile->size());
  array_schema->serialize(serializer);

  stats()->add_counter("write_array_schema_size", tile->size());

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

  GenericTileIO::store_data(resources_, schema_uri, tile, encryption_key);

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

    auto tile{WriterTile::from_generic(
        enumeration_size_serializer.size(),
        resources_.ephemeral_memory_tracker())};
    Serializer serializer(tile->data(), tile->size());
    enmr->serialize(serializer);

    auto abs_enmr_uri = array_enumerations_dir_uri.join_path(enmr->path_name());
    GenericTileIO::store_data(resources_, abs_enmr_uri, tile, encryption_key);
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
  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(),
      resources_.ephemeral_memory_tracker())};
  Serializer serializer(tile->data(), tile->size());
  metadata->serialize(serializer);

  stats()->add_counter("write_meta_size", size_computation_serializer.size());

  // Create a metadata file name
  URI metadata_uri = metadata->get_uri(uri);

  GenericTileIO::store_data(resources_, metadata_uri, tile, encryption_key);

  return Status::Ok();
}

void StorageManagerCanonical::wait_for_zero_in_progress() {
  std::unique_lock<std::mutex> lck(queries_in_progress_mtx_);
  queries_in_progress_cv_.wait(
      lck, [this]() { return queries_in_progress_ == 0; });
}

shared_ptr<Logger> StorageManagerCanonical::logger() const {
  return logger_;
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

}  // namespace tiledb::sm
