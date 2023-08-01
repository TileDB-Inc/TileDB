/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"

#include <cassert>
#include <cmath>
#include <iostream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

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
    const URI& array_uri,
    StorageManager* storage_manager,
    ConsistencyController& cc)
    : array_schema_latest_(nullptr)
    , array_uri_(array_uri)
    , array_dir_(storage_manager->resources(), array_uri)
    , array_uri_serialized_(array_uri)
    , encryption_key_(make_shared<EncryptionKey>(HERE()))
    , is_open_(false)
    , is_opening_or_closing_(false)
    , timestamp_start_(0)
    , timestamp_end_(UINT64_MAX)
    , timestamp_end_opened_at_(UINT64_MAX)
    , storage_manager_(storage_manager)
    , resources_(storage_manager_->resources())
    , config_(resources_.config())
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
  auto timer =
      storage_manager_->stats()->start_timer("array_open_without_fragments");
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
      auto rest_client = resources_.rest_client();
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
        auto st = rest_client->post_array_from_rest(
            array_uri_, storage_manager_, this);
        if (!st.ok()) {
          throw StatusException(st);
        }
      }
    } else {
      {
        auto timer_se = storage_manager_->stats()->start_timer(
            "array_open_without_fragments_load_directory");
        array_dir_ = ArrayDirectory(
            resources_, array_uri_, 0, UINT64_MAX, ArrayDirectoryMode::READ);
      }
      auto&& [array_schema, array_schemas] = open_for_reads_without_fragments();
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
  auto timer = storage_manager_->stats()->start_timer(
      "array_open_" + query_type_str(query_type));
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
      auto rest_client = resources_.rest_client();
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
        auto st = rest_client->post_array_from_rest(
            array_uri_, storage_manager_, this);
        if (!st.ok()) {
          throw StatusException(st);
        }
      }
    } else if (query_type == QueryType::READ) {
      {
        auto timer_se = storage_manager_->stats()->start_timer(
            "array_open_read_load_directory");
        array_dir_ = ArrayDirectory(
            resources_, array_uri_, timestamp_start_, timestamp_end_opened_at_);
      }
      auto&& [array_schema_latest, array_schemas, fragment_metadata] =
          open_for_reads();

      // Set schemas
      array_schema_latest_ = array_schema_latest.value();
      array_schemas_all_ = array_schemas.value();
      fragment_metadata_ = fragment_metadata.value();
    } else if (
        query_type == QueryType::WRITE ||
        query_type == QueryType::MODIFY_EXCLUSIVE) {
      {
        auto timer_se = storage_manager_->stats()->start_timer(
            "array_open_write_load_directory");
        array_dir_ = ArrayDirectory(
            resources_,
            array_uri_,
            timestamp_start_,
            timestamp_end_opened_at_,
            ArrayDirectoryMode::SCHEMA_ONLY);
      }
      auto&& [st, array_schema_latest, array_schemas] = open_for_writes();
      throw_if_not_ok(st);

      // Set schemas
      array_schema_latest_ = array_schema_latest.value();
      array_schemas_all_ = array_schemas.value();

      metadata_.reset(timestamp_end_opened_at_);
    } else if (
        query_type == QueryType::DELETE || query_type == QueryType::UPDATE) {
      {
        auto timer_se = storage_manager_->stats()->start_timer(
            "array_open_delete_or_update_load_directory");
        array_dir_ = ArrayDirectory(
            resources_,
            array_uri_,
            timestamp_start_,
            timestamp_end_opened_at_,
            ArrayDirectoryMode::READ);
      }
      auto&& [st, array_schema_latest, array_schemas] = open_for_writes();
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
        auto rest_client = resources_.rest_client();
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
      if (query_type_ == QueryType::WRITE ||
          query_type_ == QueryType::MODIFY_EXCLUSIVE) {
        st = storage_manager_->store_metadata(
            array_uri_, *encryption_key_.get(), &metadata_);
        if (!st.ok()) {
          throw StatusException(st);
        }
      } else if (
          query_type_ != QueryType::READ && query_type_ != QueryType::DELETE &&
          query_type_ != QueryType::UPDATE) {
        throw std::logic_error("Error closing array; Unsupported query type.");
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

void Array::delete_array(const URI& uri) {
  // Check that data deletion is allowed
  ensure_array_is_valid_for_delete(uri);

  // Delete array data
  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw ArrayException("[delete_array] Remote array with no REST client.");
    }
    rest_client->delete_array_from_rest(uri);
  } else {
    storage_manager_->delete_array(uri.c_str());
  }

  // Close the array
  throw_if_not_ok(this->close());
}

void Array::delete_fragments(
    const URI& uri, uint64_t timestamp_start, uint64_t timestamp_end) {
  // Check that data deletion is allowed
  ensure_array_is_valid_for_delete(uri);

  // Delete fragments
  // #TODO Add rest support for delete_fragments
  if (remote_) {
    throw ArrayException(
        "[delete_fragments] Remote arrays currently unsupported.");
  } else {
    storage_manager_->delete_fragments(
        uri.c_str(), timestamp_start, timestamp_end);
  }
}

void Array::delete_fragments_list(
    const URI& uri, const std::vector<URI>& fragment_uris) {
  // Check that data deletion is allowed
  ensure_array_is_valid_for_delete(uri);

  // Delete fragments
  // #TODO Add rest support for delete_fragments_list
  if (remote_) {
    throw ArrayException(
        "[delete_fragments_list] Remote arrays currently unsupported.");
  } else {
    auto array_dir = ArrayDirectory(
        resources_, uri, 0, std::numeric_limits<uint64_t>::max());
    array_dir.delete_fragments_list(fragment_uris);
  }
}

shared_ptr<const Enumeration> Array::get_enumeration(
    const std::string& enumeration_name) {
  if (remote_) {
    throw ArrayException("Unable to load all enumerations; Array is remote.");
  }

  if (!is_open_) {
    throw ArrayException("Cannot get enumeration; Array is not open");
  }

  if (array_schema_latest_->is_enumeration_loaded(enumeration_name)) {
    return array_schema_latest_->get_enumeration(enumeration_name);
  }

  return array_dir_.load_enumeration(
      array_schema_latest_, enumeration_name, get_encryption_key());
}

void Array::load_all_enumerations(bool latest_only) {
  if (remote_) {
    throw ArrayException("Unable to load all enumerations; Array is remote.");
  }

  if (!is_open_) {
    throw ArrayException("Cannot load all enumerations; Array is not open");
  }

  std::vector<shared_ptr<ArraySchema>> schemas;
  if (latest_only) {
    schemas.emplace_back(array_schema_latest_);
  } else {
    schemas.reserve(array_schemas_all_.size());
    for (auto& iter : array_schemas_all_) {
      schemas.emplace_back(iter.second);
    }
  }

  for (auto& schema : schemas) {
    array_dir_.load_all_enumerations(schema, get_encryption_key());
  }
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
    return {
        LOG_STATUS(
            Status_ArrayError("Cannot get array schema; Array is not open")),
        nullopt};

  return {Status::Ok(), array_schema_latest_};
}

QueryType Array::get_query_type() const {
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
  auto timer = storage_manager_->stats()->start_timer("array_reopen");
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
    try {
      set_array_closed();
    } catch (std::exception& e) {
      is_opening_or_closing_ = false;
      throw Status_ArrayError(e.what());
    }
    is_opening_or_closing_ = false;

    return open(
        query_type_,
        encryption_key_->encryption_type(),
        encryption_key_->key().data(),
        encryption_key_->key().size());
  }

  try {
    {
      auto timer_se =
          storage_manager_->stats()->start_timer("array_reopen_directory");
      array_dir_ = ArrayDirectory(
          resources_,
          array_uri_,
          timestamp_start_,
          timestamp_end_opened_at_,
          query_type_ == QueryType::READ ? ArrayDirectoryMode::READ :
                                           ArrayDirectoryMode::SCHEMA_ONLY);
    }
  } catch (const std::logic_error& le) {
    return LOG_STATUS(Status_ArrayDirectoryError(le.what()));
  }

  auto&& [array_schema_latest, array_schemas, fragment_metadata] =
      open_for_reads();

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

Status Array::set_timestamp_end_opened_at(
    const uint64_t timestamp_end_opened_at) {
  timestamp_end_opened_at_ = timestamp_end_opened_at;
  return Status::Ok();
}

uint64_t Array::timestamp_end() const {
  return timestamp_end_;
}

uint64_t Array::timestamp_end_opened_at() const {
  return timestamp_end_opened_at_;
}

void Array::set_config(Config config) {
  if (is_open()) {
    throw ArrayException("[set_config] Cannot set a config on an open array");
  }
  config_.inherit(config);
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

  // Find the names of the var-sized dimensions or attributes.
  std::vector<std::string> var_names;

  // Start with dimensions.
  for (unsigned d = 0; d < array_schema_latest_->dim_num(); d++) {
    auto dim = array_schema_latest_->dimension_ptr(d);
    if (dim->var_size()) {
      var_names.emplace_back(dim->name());
      ret.emplace(dim->name(), 0);
    }
  }

  // Now attributes.
  for (auto& attr : array_schema_latest_->attributes()) {
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
        fragment_metadata_.size(),
        [&](const unsigned f) {
          // Gracefully skip loading tile sizes for attributes added in schema
          // evolution that do not exists in this fragment.
          const auto& schema = fragment_metadata_[f]->array_schema();
          if (!schema->is_field(var_name)) {
            return Status::Ok();
          }

          return fragment_metadata_[f]->load_tile_var_sizes(
              *encryption_key_, var_name);
        }));
  }

  // Now compute for each var size names, the average cell size.
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, var_names.size(), [&](const uint64_t n) {
        uint64_t total_size = 0;
        uint64_t cell_num = 0;
        auto& var_name = var_names[n];

        // Sum the total tile size and cell num for each fragments.
        for (unsigned f = 0; f < fragment_metadata_.size(); f++) {
          // Skip computation for fields that don't exist for a particular
          // fragment.
          const auto& schema = fragment_metadata_[f]->array_schema();
          if (!schema->is_field(var_name)) {
            continue;
          }

          // Go through all tiles.
          for (uint64_t t = 0; t < fragment_metadata_[f]->tile_num(); t++) {
            total_size += fragment_metadata_[f]->tile_var_size(var_name, t);
            cell_num += fragment_metadata_[f]->cell_num(t);
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
    optional<shared_ptr<ArraySchema>>,
    optional<std::unordered_map<std::string, shared_ptr<ArraySchema>>>,
    optional<std::vector<shared_ptr<FragmentMetadata>>>>
Array::open_for_reads() {
  auto timer_se = resources_.stats().start_timer(
      "array_open_read_load_schemas_and_fragment_meta");
  auto&& [st, array_schema_latest, array_schemas_all, fragment_metadata] =
      storage_manager_->load_array_schemas_and_fragment_metadata(
          array_directory(), memory_tracker(), *encryption_key());

  throw_if_not_ok(st);

  auto version = array_schema_latest.value()->version();
  ensure_supported_schema_version_for_read(version);

  return {array_schema_latest, array_schemas_all, fragment_metadata};
}

tuple<
    optional<shared_ptr<ArraySchema>>,
    optional<std::unordered_map<std::string, shared_ptr<ArraySchema>>>>
Array::open_for_reads_without_fragments() {
  auto timer_se = resources_.stats().start_timer(
      "array_open_read_without_fragments_load_schemas");

  // Load array schemas
  auto&& [array_schema_latest, array_schemas_all] =
      array_dir_.load_array_schemas(*encryption_key());

  auto version = array_schema_latest->version();
  ensure_supported_schema_version_for_read(version);

  return {array_schema_latest, array_schemas_all};
}

tuple<
    Status,
    optional<shared_ptr<ArraySchema>>,
    optional<std::unordered_map<std::string, shared_ptr<ArraySchema>>>>
Array::open_for_writes() {
  auto timer_se =
      resources_.stats().start_timer("array_open_write_load_schemas");
  // Checks
  if (!resources_.vfs().supports_uri_scheme(array_uri_))
    return {
        resources_.logger()->status(Status_StorageManagerError(
            "Cannot open array; URI scheme unsupported.")),
        nullopt,
        nullopt};

  // Load array schemas
  auto&& [array_schema_latest, array_schemas_all] =
      array_dir_.load_array_schemas(*encryption_key_);

  // If building experimentally, this library should not be able to
  // write to newer-versioned or older-versioned arrays
  // Else, this library should not be able to write to newer-versioned arrays
  // (but it is ok to write to older arrays)
  auto version = array_schema_latest->version();
  if constexpr (is_experimental_build) {
    if (version != constants::format_version) {
      std::stringstream err;
      err << "Cannot open array for writes; Array format version (";
      err << version;
      err << ") is not the library format version (";
      err << constants::format_version << ")";
      return {
          resources_.logger()->status(Status_StorageManagerError(err.str())),
          nullopt,
          nullopt};
    }
  } else {
    if (version > constants::format_version) {
      std::stringstream err;
      err << "Cannot open array for writes; Array format version (";
      err << version;
      err << ") is newer than library format version (";
      err << constants::format_version << ")";
      return {
          resources_.logger()->status(Status_StorageManagerError(err.str())),
          nullopt,
          nullopt};
    }
  }

  return {Status::Ok(), array_schema_latest, array_schemas_all};
}

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
    auto rest_client = resources_.rest_client();
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

void Array::load_local_metadata() {
  assert(array_dir_.loaded());
  auto timer_se = resources_.stats().start_timer("sm_load_array_metadata");

  // Determine which array metadata to load
  const auto& array_metadata_to_load = array_dir_.array_meta_uris();

  auto metadata_num = array_metadata_to_load.size();
  std::vector<shared_ptr<Tile>> metadata_tiles(metadata_num);
  throw_if_not_ok(
      parallel_for(&resources_.compute_tp(), 0, metadata_num, [&](size_t m) {
        const auto& uri = array_metadata_to_load[m].uri_;

        auto&& tile = GenericTileIO::load(resources_, uri, 0, *encryption_key_);
        metadata_tiles[m] = tdb::make_shared<Tile>(HERE(), std::move(tile));

        return Status::Ok();
      }));

  // Compute array metadata size for the statistics
  uint64_t meta_size = 0;
  for (const auto& t : metadata_tiles) {
    meta_size += t->size();
  }
  resources_.stats().add_counter("read_array_meta_size", meta_size);

  metadata_ = Metadata::deserialize(metadata_tiles);

  // Sets the loaded metadata URIs
  metadata_.set_loaded_metadata_uris(array_metadata_to_load);
}

Status Array::load_metadata() {
  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      return LOG_STATUS(Status_ArrayError(
          "Cannot load metadata; remote array with no REST client."));
    }
    RETURN_NOT_OK(rest_client->get_array_metadata_from_rest(
        array_uri_, timestamp_start_, timestamp_end_opened_at_, this));
  } else {
    load_local_metadata();
  }
  metadata_loaded_ = true;
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
        this, timestamp_start_, timestamp_end_opened_at_));
    non_empty_domain_computed_ = true;
  }
  return Status::Ok();
}

ArrayDirectory& Array::load_array_directory() {
  if (array_dir_.loaded()) {
    return array_dir_;
  }

  if (remote_) {
    throw std::logic_error(
        "Loading array directory for remote arrays is not supported");
  }

  auto mode = (query_type_ == QueryType::WRITE ||
               query_type_ == QueryType::MODIFY_EXCLUSIVE) ?
                  ArrayDirectoryMode::SCHEMA_ONLY :
                  ArrayDirectoryMode::READ;

  array_dir_ = ArrayDirectory(
      resources_, array_uri_, timestamp_start_, timestamp_end_opened_at_, mode);

  return array_dir_;
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
        LOG_STATUS_NO_RETURN_VALUE(Status_ArrayError(
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
    std::stringstream err;
    err << "Cannot open array for reads; Array format version (";
    err << version;
    err << ") is newer than library format version (";
    err << constants::format_version << ")";
    throw Status_StorageManagerError(err.str());
  }
}

void Array::set_serialized_array_open() {
  is_open_ = true;
}

}  // namespace sm
}  // namespace tiledb
