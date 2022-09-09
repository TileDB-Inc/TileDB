/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements class Array.
 */

#include "tiledb/common/common.h"

#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/crypto/crypto.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <cassert>
#include <cmath>
#include <iostream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

ConsistencyController& controller() {
  static ConsistencyController controller;
  return controller;
}

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Array::Array(
    const URI& array_uri,
    StorageManager* storage_manager,
    ConsistencyController& cc)
    : array_schema_latest_(nullptr)
    , array_uri_(array_uri)
    , array_dir_()
    , array_uri_serialized_(array_uri)
    , encryption_key_(make_shared<EncryptionKey>(HERE()))
    , is_open_(false)
    , is_opening_or_closing_(false)
    , timestamp_start_(0)
    , timestamp_end_(UINT64_MAX)
    , timestamp_end_opened_at_(UINT64_MAX)
    , storage_manager_(storage_manager)
    , config_(storage_manager_->config())
    , remote_(array_uri.is_tiledb())
    , metadata_()
    , metadata_loaded_(false)
    , non_empty_domain_computed_(false)
    , consistency_controller_(cc)
    , consistency_sentry_(nullopt) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

void Array::set_array_schema_latest(
    const shared_ptr<ArraySchema>& array_schema) {
  array_schema_latest_ = array_schema;
}

const ArraySchema& Array::array_schema_latest() const {
  return *(array_schema_latest_.get());
}

shared_ptr<const ArraySchema> Array::array_schema_latest_ptr() const {
  return array_schema_latest_;
}

void Array::set_array_schemas_all(
    std::unordered_map<std::string, shared_ptr<ArraySchema>>& all_schemas) {
  array_schemas_all_ = all_schemas;
}

const URI& Array::array_uri() const {
  return array_uri_;
}

const ArrayDirectory& Array::array_directory() const {
  return array_dir_;
}

const URI& Array::array_uri_serialized() const {
  return array_uri_serialized_;
}

const EncryptionKey* Array::encryption_key() const {
  return encryption_key_.get();
}

// Used in Consolidator
Status Array::open_without_fragments(
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
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

  metadata_.clear();
  metadata_loaded_ = false;
  non_empty_domain_computed_ = false;

  /* Note: query_type_ MUST be set before calling set_array_open()
    because it will be examined by the ConsistencyController. */
  query_type_ = QueryType::READ;

  /* Note: the open status MUST be exception safe. If anything interrupts the
   * opening process, it will throw and the array will be set as closed. */
  try {
    set_array_open(query_type_);

    // Copy the key bytes.
    st = encryption_key_->set_key(encryption_type, encryption_key, key_length);
    if (!st.ok()) {
      throw StatusException(st);
    }

    if (remote_) {
      auto rest_client = storage_manager_->rest_client();
      if (rest_client == nullptr) {
        throw Status_ArrayError(
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
        array_schema_latest_ = array_schema_latest.value();
      } else {
        auto st = rest_client->post_array_from_rest(array_uri_, this);
        if (!st.ok()) {
          throw StatusException(st);
        }
      }
    } else {
      array_dir_ = ArrayDirectory(
          storage_manager_->vfs(),
          storage_manager_->compute_tp(),
          array_uri_,
          0,
          UINT64_MAX,
          ArrayDirectoryMode::READ);

      auto&& [st, array_schema, array_schemas] =
          storage_manager_->array_open_for_reads_without_fragments(this);
      if (!st.ok())
        throw StatusException(st);

      array_schema_latest_ = array_schema.value();
      array_schemas_all_ = array_schemas.value();
    }
  } catch (std::exception& e) {
    set_array_closed();
    throw Status_ArrayError(e.what());
  }

  is_opening_or_closing_ = false;
  return Status::Ok();
}

Status Array::load_fragments(
    const std::vector<TimestampedURI>& fragments_to_load) {
  auto&& [st, fragment_metadata] =
      storage_manager_->array_load_fragments(this, fragments_to_load);
  RETURN_NOT_OK(st);

  fragment_metadata_ = std::move(fragment_metadata.value());

  return Status::Ok();
}

Status Array::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  return Array::open(
      query_type,
      timestamp_start_,
      timestamp_end_,
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
  Status st;
  // Checks
  if (is_open()) {
    return LOG_STATUS(
        Status_ArrayError("Cannot open array; Array already open"));
  }

  metadata_.clear();
  metadata_loaded_ = false;
  non_empty_domain_computed_ = false;
  timestamp_start_ = timestamp_start;
  timestamp_end_opened_at_ = timestamp_end;
  query_type_ = query_type;

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
        throw Status_ArrayError("Cannot get setting");
      }
      assert(found);

      if (!allow_updates) {
        throw Status_ArrayError(
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
      std::string encryption_type_from_cfg;
      bool found = false;
      encryption_type_from_cfg = config_.get("sm.encryption_type", &found);
      assert(found);
      auto [st, et] = encryption_type_enum(encryption_type_from_cfg);
      if (!st.ok()) {
        throw StatusException(st);
      }
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

    if (remote_ && encryption_type != EncryptionType::NO_ENCRYPTION) {
      throw Status_ArrayError(
          "Cannot open array; encrypted remote arrays are not supported.");
    }

    // Copy the key bytes.
    st = encryption_key_->set_key(encryption_type, encryption_key, key_length);
    if (!st.ok()) {
      throw StatusException(st);
    }

    if (timestamp_end_opened_at_ == UINT64_MAX) {
      if (query_type == QueryType::READ) {
        timestamp_end_opened_at_ = utils::time::timestamp_now_ms();
      } else if (
          query_type == QueryType::WRITE ||
          query_type == QueryType::MODIFY_EXCLUSIVE ||
          query_type == QueryType::DELETE || query_type == QueryType::UPDATE) {
        timestamp_end_opened_at_ = 0;
      } else {
        throw Status_ArrayError("Cannot open array; Unsupported query type.");
      }
    }

    if (remote_) {
      auto rest_client = storage_manager_->rest_client();
      if (rest_client == nullptr) {
        throw Status_ArrayError(
            "Cannot open array; remote array with no REST client.");
      }
      if (!use_refactored_array_open()) {
        auto&& [st, array_schema_latest] =
            rest_client->get_array_schema_from_rest(array_uri_);
        if (!st.ok()) {
          throw StatusException(st);
        }
        array_schema_latest_ = array_schema_latest.value();
      } else {
        auto st = rest_client->post_array_from_rest(array_uri_, this);
        if (!st.ok()) {
          throw StatusException(st);
        }
      }
    } else if (query_type == QueryType::READ) {
      array_dir_ = ArrayDirectory(
          storage_manager_->vfs(),
          storage_manager_->compute_tp(),
          array_uri_,
          timestamp_start_,
          timestamp_end_opened_at_);

      auto&& [st, array_schema_latest, array_schemas, fragment_metadata] =
          storage_manager_->array_open_for_reads(this);
      if (!st.ok()) {
        throw StatusException(st);
      }
      // Set schemas
      array_schema_latest_ = array_schema_latest.value();
      array_schemas_all_ = array_schemas.value();
      fragment_metadata_ = fragment_metadata.value();
    } else if (
        query_type == QueryType::WRITE ||
        query_type == QueryType::MODIFY_EXCLUSIVE) {
      array_dir_ = ArrayDirectory(
          storage_manager_->vfs(),
          storage_manager_->compute_tp(),
          array_uri_,
          timestamp_start_,
          timestamp_end_opened_at_,
          ArrayDirectoryMode::SCHEMA_ONLY);

      auto&& [st, array_schema_latest, array_schemas] =
          storage_manager_->array_open_for_writes(this);
      throw_if_not_ok(st);

      // Set schemas
      array_schema_latest_ = array_schema_latest.value();
      array_schemas_all_ = array_schemas.value();

      metadata_.reset(timestamp_end_opened_at_);
    } else if (
        query_type == QueryType::DELETE || query_type == QueryType::UPDATE) {
      array_dir_ = ArrayDirectory(
          storage_manager_->vfs(),
          storage_manager_->compute_tp(),
          array_uri_,
          timestamp_start_,
          timestamp_end_opened_at_,
          ArrayDirectoryMode::READ);

      auto&& [st, array_schema_latest, array_schemas] =
          storage_manager_->array_open_for_writes(this);
      throw_if_not_ok(st);

      // Set schemas
      array_schema_latest_ = array_schema_latest.value();
      array_schemas_all_ = array_schemas.value();

      auto version = array_schema_latest_->version();
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

      metadata_.reset(timestamp_end_opened_at_);
    } else {
      throw Status_ArrayError("Cannot open array; Unsupported query type.");
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

  non_empty_domain_.clear();
  non_empty_domain_computed_ = false;
  clear_last_max_buffer_sizes();
  fragment_metadata_.clear();

  try {
    set_array_closed();

    if (remote_) {
      // Update array metadata for write queries if metadata was written by the
      // user
      if ((query_type_ == QueryType::WRITE ||
           query_type_ == QueryType::MODIFY_EXCLUSIVE) &&
          metadata_.num() > 0) {
        // Set metadata loaded to be true so when serialization fetchs the
        // metadata it won't trigger a deadlock
        metadata_loaded_ = true;
        auto rest_client = storage_manager_->rest_client();
        if (rest_client == nullptr)
          throw Status_ArrayError(
              "Error closing array; remote array with no REST client.");
        st = rest_client->post_array_metadata_to_rest(
            array_uri_, timestamp_start_, timestamp_end_opened_at_, this);
        if (!st.ok())
          throw StatusException(st);
      }

      // Storage manager does not own the array schema for remote arrays.
      array_schema_latest_.reset();
    } else {
      array_schema_latest_.reset();
      if (query_type_ == QueryType::READ) {
        st = storage_manager_->array_close_for_reads(this);
        if (!st.ok())
          throw StatusException(st);
      } else if (
          query_type_ == QueryType::WRITE ||
          query_type_ == QueryType::MODIFY_EXCLUSIVE) {
        st = storage_manager_->array_close_for_writes(this);
        if (!st.ok())
          throw StatusException(st);
      } else if (query_type_ == QueryType::DELETE) {
        st = storage_manager_->array_close_for_deletes(this);
        if (!st.ok())
          throw StatusException(st);
      } else if (query_type_ == QueryType::UPDATE) {
        st = storage_manager_->array_close_for_updates(this);
        if (!st.ok())
          throw StatusException(st);
      } else {
        throw Status_ArrayError("Error closing array; Unsupported query type.");
      }
    }

    array_schemas_all_.clear();
    metadata_.clear();
    metadata_loaded_ = false;

  } catch (std::exception& e) {
    is_opening_or_closing_ = false;
    throw Status_ArrayError(e.what());
  }

  is_opening_or_closing_ = false;
  return Status::Ok();
}

Status Array::delete_fragments(
    const URI& uri, uint64_t timestamp_start, uint64_t timestamp_end) {
  // Check that query type is MODIFY_EXCLUSIVE
  if (query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_ArrayError(
        "[Array::delete_fragments] Query type must be MODIFY_EXCLUSIVE"));
  }

  // Check that array is open
  if (!is_open() && !controller().is_open(uri)) {
    return LOG_STATUS(
        Status_ArrayError("[Array::delete_fragments] Array is closed"));
  }

  // Check that array is not in the process of opening or closing
  if (is_opening_or_closing_) {
    return LOG_STATUS(Status_ArrayError(
        "[Array::delete_fragments] "
        "May not perform simultaneous open or close operations."));
  }

  // Delete fragments
  RETURN_NOT_OK(storage_manager_->delete_fragments(
      uri.c_str(), timestamp_start, timestamp_end));

  return Status::Ok();
}

bool Array::is_empty() const {
  return fragment_metadata_.empty();
}

bool Array::is_open() {
  std::lock_guard<std::mutex> lock(mtx_);
  return is_open_;
}

bool Array::is_remote() const {
  return remote_;
}

std::vector<shared_ptr<FragmentMetadata>> Array::fragment_metadata() const {
  return fragment_metadata_;
}

tuple<Status, optional<shared_ptr<ArraySchema>>> Array::get_array_schema()
    const {
  // Error if the array is not open
  if (!is_open_)
    return {LOG_STATUS(Status_ArrayError(
                "Cannot get array schema; Array is not open")),
            nullopt};

  return {Status::Ok(), array_schema_latest_};
}

QueryType Array::get_query_type() const {
  // Error if the array is not open
  if (!is_open_) {
    throw StatusException(
        Status_ArrayError("Cannot get query_type; Array is not open"));
  }

  return query_type_;
}

Status Array::get_max_buffer_size(
    const char* name, const void* subarray, uint64_t* buffer_size) {
  // Check if array is open
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get max buffer size; Array is not open"));
  }

  // Error if the array was not opened in read mode
  if (query_type_ != QueryType::READ) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get max buffer size; "
                          "Array was not opened in read mode"));
  }

  // Check if name is null
  if (name == nullptr) {
    return LOG_STATUS(Status_ArrayError(
        "Cannot get max buffer size; Attribute/Dimension name is null"));
  }

  // Not applicable to heterogeneous domains
  if (!array_schema_latest_->domain().all_dims_same_type()) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get max buffer size; Function not "
                          "applicable to heterogeneous domains"));
  }

  // Not applicable to variable-sized dimensions
  if (!array_schema_latest_->domain().all_dims_fixed()) {
    return LOG_STATUS(Status_ArrayError(
        "Cannot get max buffer size; Function not "
        "applicable to domains with variable-sized dimensions"));
  }

  // Check if name is attribute or dimension
  bool is_dim = array_schema_latest_->is_dim(name);
  bool is_attr = array_schema_latest_->is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr) {
    return LOG_STATUS(Status_ArrayError(
        std::string("Cannot get max buffer size; Attribute/Dimension '") +
        name + "' does not exist"));
  }

  // Check if attribute/dimension is fixed sized
  if (array_schema_latest_->var_size(name)) {
    return LOG_STATUS(Status_ArrayError(
        std::string("Cannot get max buffer size; Attribute/Dimension '") +
        name + "' is var-sized"));
  }

  RETURN_NOT_OK(compute_max_buffer_sizes(subarray));

  // Retrieve buffer size
  auto it = last_max_buffer_sizes_.find(name);
  assert(it != last_max_buffer_sizes_.end());
  *buffer_size = it->second.first;

  return Status::Ok();
}

Status Array::get_max_buffer_size(
    const char* name,
    const void* subarray,
    uint64_t* buffer_off_size,
    uint64_t* buffer_val_size) {
  // Check if array is open
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get max buffer size; Array is not open"));
  }

  // Error if the array was not opened in read mode
  if (query_type_ != QueryType::READ) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get max buffer size; "
                          "Array was not opened in read mode"));
  }

  // Check if name is null
  if (name == nullptr) {
    return LOG_STATUS(Status_ArrayError(
        "Cannot get max buffer size; Attribute/Dimension name is null"));
  }

  // Not applicable to heterogeneous domains
  if (!array_schema_latest_->domain().all_dims_same_type()) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get max buffer size; Function not "
                          "applicable to heterogeneous domains"));
  }

  // Not applicable to variable-sized dimensions
  if (!array_schema_latest_->domain().all_dims_fixed()) {
    return LOG_STATUS(Status_ArrayError(
        "Cannot get max buffer size; Function not "
        "applicable to domains with variable-sized dimensions"));
  }

  RETURN_NOT_OK(compute_max_buffer_sizes(subarray));

  // Check if attribute/dimension exists
  auto it = last_max_buffer_sizes_.find(name);
  if (it == last_max_buffer_sizes_.end()) {
    return LOG_STATUS(Status_ArrayError(
        std::string("Cannot get max buffer size; Attribute/Dimension '") +
        name + "' does not exist"));
  }

  // Check if attribute/dimension is var-sized
  if (!array_schema_latest_->var_size(name)) {
    return LOG_STATUS(Status_ArrayError(
        std::string("Cannot get max buffer size; Attribute/Dimension '") +
        name + "' is fixed-sized"));
  }

  // Retrieve buffer sizes
  *buffer_off_size = it->second.first;
  *buffer_val_size = it->second.second;

  return Status::Ok();
}

const EncryptionKey& Array::get_encryption_key() const {
  return *encryption_key_;
}

Status Array::reopen() {
  if (timestamp_end_ == timestamp_end_opened_at_) {
    // The user has not set `timestamp_end_` since it was last opened.
    // In this scenario, re-open at the current timestamp.
    return reopen(timestamp_start_, utils::time::timestamp_now_ms());
  } else {
    // The user has set `timestamp_end_`. Reopen at that time stamp.
    return reopen(timestamp_start_, timestamp_end_);
  }
}

Status Array::reopen(uint64_t timestamp_start, uint64_t timestamp_end) {
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot reopen array; Array is not open"));
  }

  if (query_type_ != QueryType::READ) {
    return LOG_STATUS(
        Status_ArrayError("Cannot reopen array; Array was "
                          "not opened in read mode"));
  }

  clear_last_max_buffer_sizes();

  timestamp_start_ = timestamp_start;
  timestamp_end_opened_at_ = timestamp_end;
  fragment_metadata_.clear();
  metadata_.clear();
  metadata_loaded_ = false;
  non_empty_domain_.clear();
  non_empty_domain_computed_ = false;

  if (timestamp_end_opened_at_ == UINT64_MAX) {
    timestamp_end_opened_at_ = utils::time::timestamp_now_ms();
  }

  if (remote_) {
    return open(
        query_type_,
        encryption_key_->encryption_type(),
        encryption_key_->key().data(),
        encryption_key_->key().size());
  }

  try {
    array_dir_ = ArrayDirectory(
        storage_manager_->vfs(),
        storage_manager_->compute_tp(),
        array_uri_,
        timestamp_start_,
        timestamp_end_opened_at_,
        query_type_ == QueryType::READ ? ArrayDirectoryMode::READ :
                                         ArrayDirectoryMode::SCHEMA_ONLY);
  } catch (const std::logic_error& le) {
    return LOG_STATUS(Status_ArrayDirectoryError(le.what()));
  }

  auto&& [st, array_schema_latest, array_schemas, fragment_metadata] =
      storage_manager_->array_reopen(this);
  RETURN_NOT_OK(st);

  array_schema_latest_ = array_schema_latest.value();
  array_schemas_all_ = array_schemas.value();
  fragment_metadata_ = fragment_metadata.value();

  return Status::Ok();
}

Status Array::set_timestamp_start(const uint64_t timestamp_start) {
  timestamp_start_ = timestamp_start;
  return Status::Ok();
}

uint64_t Array::timestamp_start() const {
  return timestamp_start_;
}

Status Array::set_timestamp_end(const uint64_t timestamp_end) {
  timestamp_end_ = timestamp_end;
  return Status::Ok();
}

uint64_t Array::timestamp_end() const {
  return timestamp_end_;
}

uint64_t Array::timestamp_end_opened_at() const {
  return timestamp_end_opened_at_;
}

Status Array::set_config(Config config) {
  config_.inherit(config);
  return Status::Ok();
}

Config Array::config() const {
  return config_;
}

Status Array::set_uri_serialized(const std::string& uri) {
  array_uri_serialized_ = URI(uri);
  return Status::Ok();
}

Status Array::delete_metadata(const char* key) {
  // Check if array is open
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot delete metadata. Array is not open"));
  }

  // Check mode
  if (query_type_ != QueryType::WRITE &&
      query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(
        Status_ArrayError("Cannot delete metadata. Array was "
                          "not opened in write or modify_exclusive mode"));
  }

  // Check if key is null
  if (key == nullptr) {
    return LOG_STATUS(
        Status_ArrayError("Cannot delete metadata. Key cannot be null"));
  }

  RETURN_NOT_OK(metadata_.del(key));

  return Status::Ok();
}

Status Array::put_metadata(
    const char* key,
    Datatype value_type,
    uint32_t value_num,
    const void* value) {
  // Check if array is open
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot put metadata; Array is not open"));
  }

  // Check mode
  if (query_type_ != QueryType::WRITE &&
      query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(
        Status_ArrayError("Cannot put metadata; Array was "
                          "not opened in write or modify_exclusive mode"));
  }

  // Check if key is null
  if (key == nullptr) {
    return LOG_STATUS(
        Status_ArrayError("Cannot put metadata; Key cannot be null"));
  }

  // Check if value type is ANY
  if (value_type == Datatype::ANY) {
    return LOG_STATUS(
        Status_ArrayError("Cannot put metadata; Value type cannot be ANY"));
  }

  RETURN_NOT_OK(metadata_.put(key, value_type, value_num, value));

  return Status::Ok();
}

Status Array::get_metadata(
    const char* key,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if array is open
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get metadata; Array is not open"));
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get metadata; Array was "
                          "not opened in read mode"));
  }

  // Check if key is null
  if (key == nullptr) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get metadata; Key cannot be null"));
  }

  // Load array metadata, if not loaded yet
  if (!metadata_loaded_) {
    RETURN_NOT_OK(load_metadata());
  }

  RETURN_NOT_OK(metadata_.get(key, value_type, value_num, value));

  return Status::Ok();
}

Status Array::get_metadata(
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if array is open
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get metadata; Array is not open"));
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get metadata; Array was "
                          "not opened in read mode"));
  }

  // Load array metadata, if not loaded yet
  if (!metadata_loaded_) {
    RETURN_NOT_OK(load_metadata());
  }

  RETURN_NOT_OK(
      metadata_.get(index, key, key_len, value_type, value_num, value));

  return Status::Ok();
}

Status Array::get_metadata_num(uint64_t* num) {
  // Check if array is open
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get number of metadata; Array is not open"));
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get number of metadata; Array was "
                          "not opened in read mode"));
  }

  // Load array metadata, if not loaded yet
  if (!metadata_loaded_) {
    RETURN_NOT_OK(load_metadata());
  }

  *num = metadata_.num();

  return Status::Ok();
}

Status Array::has_metadata_key(
    const char* key, Datatype* value_type, bool* has_key) {
  // Check if array is open
  if (!is_open_) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get metadata; Array is not open"));
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get metadata; Array was "
                          "not opened in read mode"));
  }

  // Check if key is null
  if (key == nullptr) {
    return LOG_STATUS(
        Status_ArrayError("Cannot get metadata; Key cannot be null"));
  }

  // Load array metadata, if not loaded yet
  if (!metadata_loaded_) {
    RETURN_NOT_OK(load_metadata());
  }

  RETURN_NOT_OK(metadata_.has_key(key, value_type, has_key));

  return Status::Ok();
}

Metadata* Array::unsafe_metadata() {
  return &metadata_;
}

Status Array::metadata(Metadata** metadata) {
  // Load array metadata for array opened for reads, if not loaded yet
  if (query_type_ == QueryType::READ && !metadata_loaded_) {
    RETURN_NOT_OK(load_metadata());
  }

  *metadata = &metadata_;

  return Status::Ok();
}

NDRange* Array::loaded_non_empty_domain() {
  return &non_empty_domain_;
}

tuple<Status, optional<const NDRange>> Array::non_empty_domain() {
  if (!non_empty_domain_computed_) {
    // Compute non-empty domain
    RETURN_NOT_OK_TUPLE(compute_non_empty_domain(), nullopt);
  }

  return {Status::Ok(), non_empty_domain_};
}

void Array::set_non_empty_domain(const NDRange& non_empty_domain) {
  non_empty_domain_ = non_empty_domain;
}

MemoryTracker* Array::memory_tracker() {
  return &memory_tracker_;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void Array::clear_last_max_buffer_sizes() {
  last_max_buffer_sizes_.clear();
  last_max_buffer_sizes_subarray_.clear();
  last_max_buffer_sizes_subarray_.shrink_to_fit();
}

Status Array::compute_max_buffer_sizes(const void* subarray) {
  // Applicable only to domains where all dimensions have the same type
  if (!array_schema_latest_->domain().all_dims_same_type()) {
    return LOG_STATUS(
        Status_ArrayError("Cannot compute max buffer sizes; Inapplicable when "
                          "dimension domains have different types"));
  }

  // Allocate space for max buffer sizes subarray
  auto dim_num = array_schema_latest_->dim_num();
  auto coord_size{
      array_schema_latest_->domain().dimension_ptr(0)->coord_size()};
  auto subarray_size = 2 * dim_num * coord_size;
  last_max_buffer_sizes_subarray_.resize(subarray_size);

  // Compute max buffer sizes
  if (last_max_buffer_sizes_.empty() ||
      std::memcmp(
          &last_max_buffer_sizes_subarray_[0], subarray, subarray_size) != 0) {
    last_max_buffer_sizes_.clear();

    // Get all attributes and coordinates
    auto attributes = array_schema_latest_->attributes();
    last_max_buffer_sizes_.clear();
    for (const auto& attr : attributes)
      last_max_buffer_sizes_[attr->name()] =
          std::pair<uint64_t, uint64_t>(0, 0);
    last_max_buffer_sizes_[constants::coords] =
        std::pair<uint64_t, uint64_t>(0, 0);
    for (unsigned d = 0; d < dim_num; ++d)
      last_max_buffer_sizes_
          [array_schema_latest_->domain().dimension_ptr(d)->name()] =
              std::pair<uint64_t, uint64_t>(0, 0);

    RETURN_NOT_OK(compute_max_buffer_sizes(subarray, &last_max_buffer_sizes_));
  }

  // Update subarray
  std::memcpy(&last_max_buffer_sizes_subarray_[0], subarray, subarray_size);

  return Status::Ok();
}

Status Array::compute_max_buffer_sizes(
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) const {
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return LOG_STATUS(Status_ArrayError(
          "Cannot get max buffer sizes; remote array with no REST client."));
    }

    return rest_client->get_array_max_buffer_sizes(
        array_uri_, *(array_schema_latest_.get()), subarray, buffer_sizes);
  }

  // Return if there are no metadata
  if (fragment_metadata_.empty()) {
    return Status::Ok();
  }

  // First we calculate a rough upper bound. Especially for dense
  // arrays, this will not be accurate, as it accounts only for the
  // non-empty regions of the subarray.
  for (auto& meta : fragment_metadata_) {
    RETURN_NOT_OK(
        meta->add_max_buffer_sizes(*encryption_key_, subarray, buffer_sizes));
  }

  // Prepare an NDRange for the subarray
  auto dim_num = array_schema_latest_->dim_num();
  NDRange sub(dim_num);
  auto sub_ptr = (const unsigned char*)subarray;
  uint64_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto r_size{2 * array_schema_latest_->dimension_ptr(d)->coord_size()};
    sub[d] = Range(&sub_ptr[offset], r_size);
    offset += r_size;
  }

  // Rectify bound for dense arrays
  if (array_schema_latest_->dense()) {
    auto cell_num = array_schema_latest_->domain().cell_num(sub);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *buffer_sizes) {
        if (array_schema_latest_->var_size(it.first)) {
          it.second.first = cell_num * constants::cell_var_offset_size;
          it.second.second +=
              cell_num * datatype_size(array_schema_latest_->type(it.first));
        } else {
          it.second.first =
              cell_num * array_schema_latest_->cell_size(it.first);
        }
      }
    }
  }

  // Rectify bound for sparse arrays with integer domain, without duplicates
  if (!array_schema_latest_->dense() && !array_schema_latest_->allows_dups() &&
      array_schema_latest_->domain().all_dims_int()) {
    auto cell_num = array_schema_latest_->domain().cell_num(sub);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *buffer_sizes) {
        if (!array_schema_latest_->var_size(it.first)) {
          // Check for overflow
          uint64_t new_size =
              cell_num * array_schema_latest_->cell_size(it.first);
          if (new_size / array_schema_latest_->cell_size((it.first)) !=
              cell_num) {
            continue;
          }

          // Potentially rectify size
          it.second.first = std::min(it.second.first, new_size);
        }
      }
    }
  }

  return Status::Ok();
}

Status Array::load_metadata() {
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return LOG_STATUS(Status_ArrayError(
          "Cannot load metadata; remote array with no REST client."));
    }
    RETURN_NOT_OK(rest_client->get_array_metadata_from_rest(
        array_uri_, timestamp_start_, timestamp_end_opened_at_, this));
  } else {
    assert(array_dir_.loaded());
    RETURN_NOT_OK(storage_manager_->load_array_metadata(
        array_dir_, *encryption_key_, &metadata_));
  }
  metadata_loaded_ = true;
  return Status::Ok();
}

Status Array::load_remote_non_empty_domain() {
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return LOG_STATUS(Status_ArrayError(
          "Cannot load metadata; remote array with no REST client."));
    }
    RETURN_NOT_OK(rest_client->get_array_non_empty_domain(
        this, timestamp_start_, timestamp_end_opened_at_));
    non_empty_domain_computed_ = true;
  }
  return Status::Ok();
}

Status Array::compute_non_empty_domain() {
  if (remote_) {
    RETURN_NOT_OK(load_remote_non_empty_domain());
  } else if (!fragment_metadata_.empty()) {
    const auto& frag0_dom = fragment_metadata_[0]->non_empty_domain();
    non_empty_domain_.assign(frag0_dom.begin(), frag0_dom.end());

    auto metadata_num = fragment_metadata_.size();
    for (size_t j = 1; j < metadata_num; ++j) {
      const auto& meta_dom = fragment_metadata_[j]->non_empty_domain();
      // Validate that this fragment's non-empty domain is set
      // It should _always_ be set, however we've seen cases where disk
      // corruption or other out-of-band activity can cause the fragment to be
      // corrupt for these cases we want to check to prevent any segfaults
      // later.
      if (!meta_dom.empty()) {
        array_schema_latest_->domain().expand_ndrange(
            meta_dom, &non_empty_domain_);
      } else {
        // If the fragment's non-empty domain is indeed empty, lets log it so
        // the user gets a message warning that this fragment might be corrupt
        // Note: LOG_STATUS only prints if TileDB is built in verbose mode.
        LOG_STATUS(Status_ArrayError(
            "Non empty domain unexpectedly empty for fragment: " +
            fragment_metadata_[j]->fragment_uri().to_string()));
      }
    }
  }
  non_empty_domain_computed_ = true;
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

bool Array::use_refactored_array_open() const {
  auto found = false;
  auto refactored_array_open = false;
  auto status = config_.get<bool>(
      "rest.use_refactored_array_open", &refactored_array_open, &found);
  if (!status.ok() || !found) {
    throw std::runtime_error(
        "Cannot get use_refactored_array_open configuration option from "
        "config");
  }

  return refactored_array_open;
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
}  // namespace sm
}  // namespace tiledb
