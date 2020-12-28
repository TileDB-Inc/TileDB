/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include "tiledb/sm/array/array.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/crypto/crypto.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <cassert>
#include <iostream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Array::Array(const URI& array_uri, StorageManager* storage_manager)
    : array_uri_(array_uri)
    , storage_manager_(storage_manager) {
  is_open_ = false;
  array_schema_ = nullptr;
  timestamp_ = 0;
  remote_ = array_uri.is_tiledb();
  metadata_loaded_ = false;
  non_empty_domain_computed_ = false;
};

/* ********************************* */
/*                API                */
/* ********************************* */

ArraySchema* Array::array_schema() const {
  std::unique_lock<std::mutex> lck(mtx_);
  return array_schema_;
}

const URI& Array::array_uri() const {
  std::unique_lock<std::mutex> lck(mtx_);
  return array_uri_;
}

const EncryptionKey* Array::encryption_key() const {
  return &encryption_key_;
}

Status Array::open(
    QueryType query_type,
    uint64_t timestamp,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  std::unique_lock<std::mutex> lck(mtx_);

  if (is_open_)
    return LOG_STATUS(Status::ArrayError(
        "Cannot open array at timestamp; Array already open"));

  if (remote_ && encryption_type != EncryptionType::NO_ENCRYPTION)
    return LOG_STATUS(Status::ArrayError(
        "Cannot open array; encrypted remote arrays are not supported."));

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_.set_key(encryption_type, encryption_key, key_length));

  timestamp_ = timestamp;
  metadata_.clear();
  metadata_loaded_ = false;
  non_empty_domain_computed_ = false;

  query_type_ = query_type;
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::ArrayError(
          "Cannot open array; remote array with no REST client."));
    RETURN_NOT_OK(
        rest_client->get_array_schema_from_rest(array_uri_, &array_schema_));
  } else if (query_type == QueryType::READ) {
    RETURN_NOT_OK(storage_manager_->array_open_for_reads(
        array_uri_,
        timestamp_,
        encryption_key_,
        &array_schema_,
        &fragment_metadata_));
  } else {
    RETURN_NOT_OK(storage_manager_->array_open_for_writes(
        array_uri_, encryption_key_, &array_schema_));
    metadata_.reset(timestamp_);
  }

  is_open_ = true;

  return Status::Ok();
}

// Used in Consolidator
Status Array::open(
    QueryType query_type,
    const FragmentInfo& fragment_info,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  std::unique_lock<std::mutex> lck(mtx_);

  if (is_open_)
    return LOG_STATUS(Status::ArrayError(
        "Cannot open array with fragments; Array already open"));

  if (query_type != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot open array with fragments; The array can "
                           "opened at a timestamp only in read mode"));

  if (remote_ && encryption_type != EncryptionType::NO_ENCRYPTION)
    return LOG_STATUS(Status::ArrayError(
        "Cannot open array; encrypted remote arrays are not supported."));

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_.set_key(encryption_type, encryption_key, key_length));

  timestamp_ = utils::time::timestamp_now_ms();
  metadata_.clear();
  metadata_loaded_ = false;
  non_empty_domain_computed_ = false;

  query_type_ = QueryType::READ;
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::ArrayError(
          "Cannot open array; remote array with no REST client."));
    RETURN_NOT_OK(
        rest_client->get_array_schema_from_rest(array_uri_, &array_schema_));
  } else {
    // Open the array.
    RETURN_NOT_OK(storage_manager_->array_open_for_reads(
        array_uri_,
        fragment_info,
        encryption_key_,
        &array_schema_,
        &fragment_metadata_));
  }

  is_open_ = true;

  return Status::Ok();
}

Status Array::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  std::unique_lock<std::mutex> lck(mtx_);

  if (is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot open array; Array already open"));

  if (remote_ && encryption_type != EncryptionType::NO_ENCRYPTION)
    return LOG_STATUS(Status::ArrayError(
        "Cannot open array; encrypted remote arrays are not supported."));

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_.set_key(encryption_type, encryption_key, key_length));

  timestamp_ =
      (query_type == QueryType::READ) ? utils::time::timestamp_now_ms() : 0;
  metadata_.clear();
  metadata_loaded_ = false;
  non_empty_domain_computed_ = false;

  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::ArrayError(
          "Cannot open array; remote array with no REST client."));
    RETURN_NOT_OK(
        rest_client->get_array_schema_from_rest(array_uri_, &array_schema_));
  } else if (query_type == QueryType::READ) {
    RETURN_NOT_OK(storage_manager_->array_open_for_reads(
        array_uri_,
        timestamp_,
        encryption_key_,
        &array_schema_,
        &fragment_metadata_));
  } else {
    RETURN_NOT_OK(storage_manager_->array_open_for_writes(
        array_uri_, encryption_key_, &array_schema_));
    metadata_.reset(timestamp_);
  }

  query_type_ = query_type;
  is_open_ = true;

  return Status::Ok();
}

Status Array::close() {
  std::unique_lock<std::mutex> lck(mtx_);

  if (!is_open_)
    return Status::Ok();

  is_open_ = false;
  non_empty_domain_.clear();
  non_empty_domain_computed_ = false;
  clear_last_max_buffer_sizes();
  fragment_metadata_.clear();

  if (remote_) {
    // Update array metadata for write queries if metadata was written by the
    // user
    if (query_type_ == QueryType::WRITE && metadata_.num() > 0) {
      // Set metadata loaded to be true so when serialization fetchs the
      // metadata it won't trigger a deadlock
      metadata_loaded_ = true;
      auto rest_client = storage_manager_->rest_client();
      if (rest_client == nullptr)
        return LOG_STATUS(Status::ArrayError(
            "Error closing array; remote array with no REST client."));
      RETURN_NOT_OK(rest_client->post_array_metadata_to_rest(array_uri_, this));
    }

    // Storage manager does not own the array schema for remote arrays.
    delete array_schema_;
    array_schema_ = nullptr;
  } else {
    array_schema_ = nullptr;
    if (query_type_ == QueryType::READ) {
      RETURN_NOT_OK(storage_manager_->array_close_for_reads(array_uri_));
    } else {
      RETURN_NOT_OK(storage_manager_->array_close_for_writes(
          array_uri_, encryption_key_, &metadata_));
    }
  }

  metadata_.clear();
  metadata_loaded_ = false;

  return Status::Ok();
}

bool Array::is_empty() const {
  std::unique_lock<std::mutex> lck(mtx_);
  return fragment_metadata_.empty();
}

bool Array::is_open() const {
  std::unique_lock<std::mutex> lck(mtx_);
  return is_open_;
}

bool Array::is_remote() const {
  return remote_;
}

std::vector<FragmentMetadata*> Array::fragment_metadata() const {
  std::unique_lock<std::mutex> lck(mtx_);
  return fragment_metadata_;
}

Status Array::get_array_schema(ArraySchema** array_schema) const {
  std::unique_lock<std::mutex> lck(mtx_);

  // Error if the array is not open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot get array schema; Array is not open"));

  *array_schema = array_schema_;

  return Status::Ok();
}

Status Array::get_query_type(QueryType* query_type) const {
  std::unique_lock<std::mutex> lck(mtx_);

  // Error if the array is not open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot get query_type; Array is not open"));

  *query_type = query_type_;

  return Status::Ok();
}

Status Array::get_max_buffer_size(
    const char* name, const void* subarray, uint64_t* buffer_size) {
  std::unique_lock<std::mutex> lck(mtx_);
  // Check if array is open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; Array is not open"));

  // Error if the array was not opened in read mode
  if (query_type_ != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; "
                           "Array was not opened in read mode"));

  // Check if name is null
  if (name == nullptr)
    return LOG_STATUS(Status::ArrayError(
        "Cannot get max buffer size; Attribute/Dimension name is null"));

  // Not applicable to heterogeneous domains
  if (!array_schema_->domain()->all_dims_same_type())
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; Function not "
                           "applicable to heterogeneous domains"));

  // Not applicable to variable-sized dimensions
  if (!array_schema_->domain()->all_dims_fixed())
    return LOG_STATUS(Status::ArrayError(
        "Cannot get max buffer size; Function not "
        "applicable to domains with variable-sized dimensions"));

  // Check if name is attribute or dimension
  bool is_dim = array_schema_->is_dim(name);
  bool is_attr = array_schema_->is_attr(name);

  // Check if attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if attribute/dimension is fixed sized
  if (array_schema_->var_size(name))
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute/Dimension '") +
        name + "' is var-sized"));

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
  std::unique_lock<std::mutex> lck(mtx_);

  // Check if array is open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; Array is not open"));

  // Error if the array was not opened in read mode
  if (query_type_ != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; "
                           "Array was not opened in read mode"));

  // Check if name is null
  if (name == nullptr)
    return LOG_STATUS(Status::ArrayError(
        "Cannot get max buffer size; Attribute/Dimension name is null"));

  // Not applicable to heterogeneous domains
  if (!array_schema_->domain()->all_dims_same_type())
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; Function not "
                           "applicable to heterogeneous domains"));

  // Not applicable to variable-sized dimensions
  if (!array_schema_->domain()->all_dims_fixed())
    return LOG_STATUS(Status::ArrayError(
        "Cannot get max buffer size; Function not "
        "applicable to domains with variable-sized dimensions"));

  RETURN_NOT_OK(compute_max_buffer_sizes(subarray));

  // Check if attribute/dimension exists
  auto it = last_max_buffer_sizes_.find(name);
  if (it == last_max_buffer_sizes_.end())
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute/Dimension '") +
        name + "' does not exist"));

  // Check if attribute/dimension is var-sized
  if (!array_schema_->var_size(name))
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute/Dimension '") +
        name + "' is fixed-sized"));

  // Retrieve buffer sizes
  *buffer_off_size = it->second.first;
  *buffer_val_size = it->second.second;

  return Status::Ok();
}

const EncryptionKey& Array::get_encryption_key() const {
  std::unique_lock<std::mutex> lck(mtx_);
  return encryption_key_;
}

Status Array::reopen() {
  return reopen(utils::time::timestamp_now_ms());
}

Status Array::reopen(uint64_t timestamp) {
  std::unique_lock<std::mutex> lck(mtx_);

  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot reopen array; Array is not open"));

  if (query_type_ != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot reopen array; Array was "
                           "not opened in read mode"));

  clear_last_max_buffer_sizes();

  timestamp_ = timestamp;
  fragment_metadata_.clear();
  metadata_.clear();
  metadata_loaded_ = false;
  non_empty_domain_.clear();
  non_empty_domain_computed_ = false;

  if (remote_) {
    return open(
        query_type_,
        encryption_key_.encryption_type(),
        encryption_key_.key().data(),
        encryption_key_.key().size());
  }

  RETURN_NOT_OK(storage_manager_->array_reopen(
      array_uri_,
      timestamp_,
      encryption_key_,
      &array_schema_,
      &fragment_metadata_));

  return Status::Ok();
}

uint64_t Array::timestamp() const {
  std::unique_lock<std::mutex> lck(mtx_);
  return timestamp_;
}

Status Array::set_timestamp(uint64_t timestamp) {
  std::unique_lock<std::mutex> lck(mtx_);
  timestamp_ = timestamp;
  return Status::Ok();
}

Status Array::set_uri(const std::string& uri) {
  std::unique_lock<std::mutex> lck(mtx_);
  array_uri_ = URI(uri);
  return Status::Ok();
}

Status Array::delete_metadata(const char* key) {
  // Check if array is open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot delete metadata; Array is not open"));

  // Check mode
  if (query_type_ != QueryType::WRITE)
    return LOG_STATUS(
        Status::ArrayError("Cannot delete metadata; Array was "
                           "not opened in write mode"));

  // Check if key is null
  if (key == nullptr)
    return LOG_STATUS(
        Status::ArrayError("Cannot delete metadata; Key cannot be null"));

  RETURN_NOT_OK(metadata_.del(key));

  return Status::Ok();
}

Status Array::put_metadata(
    const char* key,
    Datatype value_type,
    uint32_t value_num,
    const void* value) {
  // Check if array is open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot put metadata; Array is not open"));

  // Check mode
  if (query_type_ != QueryType::WRITE)
    return LOG_STATUS(
        Status::ArrayError("Cannot put metadata; Array was "
                           "not opened in write mode"));

  // Check if key is null
  if (key == nullptr)
    return LOG_STATUS(
        Status::ArrayError("Cannot put metadata; Key cannot be null"));

  // Check if value type is ANY
  if (value_type == Datatype::ANY)
    return LOG_STATUS(
        Status::ArrayError("Cannot put metadata; Value type cannot be ANY"));

  RETURN_NOT_OK(metadata_.put(key, value_type, value_num, value));

  return Status::Ok();
}

Status Array::get_metadata(
    const char* key,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if array is open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot get metadata; Array is not open"));

  // Check mode
  if (query_type_ != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot get metadata; Array was "
                           "not opened in read mode"));

  // Check if key is null
  if (key == nullptr)
    return LOG_STATUS(
        Status::ArrayError("Cannot get metadata; Key cannot be null"));

  // Load array metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

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
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot get metadata; Array is not open"));

  // Check mode
  if (query_type_ != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot get metadata; Array was "
                           "not opened in read mode"));

  // Load array metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  RETURN_NOT_OK(
      metadata_.get(index, key, key_len, value_type, value_num, value));

  return Status::Ok();
}

Status Array::get_metadata_num(uint64_t* num) {
  // Check if array is open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot get number of metadata; Array is not open"));

  // Check mode
  if (query_type_ != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot get number of metadata; Array was "
                           "not opened in read mode"));

  // Load array metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  *num = metadata_.num();

  return Status::Ok();
}

Status Array::has_metadata_key(
    const char* key, Datatype* value_type, bool* has_key) {
  // Check if array is open
  if (!is_open_)
    return LOG_STATUS(
        Status::ArrayError("Cannot get metadata; Array is not open"));

  // Check mode
  if (query_type_ != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot get metadata; Array was "
                           "not opened in read mode"));

  // Check if key is null
  if (key == nullptr)
    return LOG_STATUS(
        Status::ArrayError("Cannot get metadata; Key cannot be null"));

  // Load array metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  RETURN_NOT_OK(metadata_.has_key(key, value_type, has_key));

  return Status::Ok();
}

Metadata* Array::metadata() {
  return &metadata_;
}

Status Array::metadata(Metadata** metadata) {
  // Load array metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  *metadata = &metadata_;

  return Status::Ok();
}

const NDRange& Array::non_empty_domain() {
  if (!non_empty_domain_computed_) {
    // Compute non-empty domain
    compute_non_empty_domain();
  }
  return non_empty_domain_;
}

void Array::set_non_empty_domain(const NDRange& non_empty_domain) {
  std::lock_guard<std::mutex> lock{mtx_};
  non_empty_domain_ = non_empty_domain;
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
  if (!array_schema_->domain()->all_dims_same_type())
    return LOG_STATUS(
        Status::ArrayError("Cannot compute max buffer sizes; Inapplicable when "
                           "dimension domains have different types"));

  // Allocate space for max buffer sizes subarray
  auto dim_num = array_schema_->dim_num();
  auto coord_size = array_schema_->domain()->dimension(0)->coord_size();
  auto subarray_size = 2 * dim_num * coord_size;
  last_max_buffer_sizes_subarray_.resize(subarray_size);

  // Compute max buffer sizes
  if (last_max_buffer_sizes_.empty() ||
      std::memcmp(
          &last_max_buffer_sizes_subarray_[0], subarray, subarray_size) != 0) {
    last_max_buffer_sizes_.clear();

    // Get all attributes and coordinates
    auto attributes = array_schema_->attributes();
    last_max_buffer_sizes_.clear();
    for (const auto& attr : attributes)
      last_max_buffer_sizes_[attr->name()] =
          std::pair<uint64_t, uint64_t>(0, 0);
    last_max_buffer_sizes_[constants::coords] =
        std::pair<uint64_t, uint64_t>(0, 0);
    for (unsigned d = 0; d < dim_num; ++d)
      last_max_buffer_sizes_[array_schema_->domain()->dimension(d)->name()] =
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
    if (rest_client == nullptr)
      return LOG_STATUS(Status::ArrayError(
          "Cannot get max buffer sizes; remote array with no REST client."));
    return rest_client->get_array_max_buffer_sizes(
        array_uri_, array_schema_, subarray, buffer_sizes);
  }

  // Return if there are no metadata
  if (fragment_metadata_.empty())
    return Status::Ok();

  // First we calculate a rough upper bound. Especially for dense
  // arrays, this will not be accurate, as it accounts only for the
  // non-empty regions of the subarray.
  for (auto& meta : fragment_metadata_)
    RETURN_NOT_OK(
        meta->add_max_buffer_sizes(encryption_key_, subarray, buffer_sizes));

  // Prepare an NDRange for the subarray
  auto dim_num = array_schema_->dim_num();
  NDRange sub(dim_num);
  auto sub_ptr = (const unsigned char*)subarray;
  uint64_t offset = 0;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto r_size = 2 * array_schema_->dimension(d)->coord_size();
    sub[d] = Range(&sub_ptr[offset], r_size);
    offset += r_size;
  }

  // Rectify bound for dense arrays
  if (array_schema_->dense()) {
    auto cell_num = array_schema_->domain()->cell_num(sub);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *buffer_sizes) {
        if (array_schema_->var_size(it.first)) {
          it.second.first = cell_num * constants::cell_var_offset_size;
          it.second.second +=
              cell_num * datatype_size(array_schema_->type(it.first));
        } else {
          it.second.first = cell_num * array_schema_->cell_size(it.first);
        }
      }
    }
  }

  // Rectify bound for sparse arrays with integer domain, without duplicates
  if (!array_schema_->dense() && !array_schema_->allows_dups() &&
      array_schema_->domain()->all_dims_int()) {
    auto cell_num = array_schema_->domain()->cell_num(sub);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *buffer_sizes) {
        if (!array_schema_->var_size(it.first)) {
          // Check for overflow
          uint64_t new_size = cell_num * array_schema_->cell_size(it.first);
          if (new_size / array_schema_->cell_size((it.first)) != cell_num)
            continue;

          // Potentially rectify size
          it.second.first = std::min(it.second.first, new_size);
        }
      }
    }
  }

  return Status::Ok();
}

Status Array::load_metadata() {
  std::lock_guard<std::mutex> lock{mtx_};
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::ArrayError(
          "Cannot load metadata; remote array with no REST client."));
    RETURN_NOT_OK(rest_client->get_array_metadata_from_rest(
        array_uri_, timestamp_, this));
  } else {
    RETURN_NOT_OK(storage_manager_->load_array_metadata(
        array_uri_, encryption_key_, timestamp_, &metadata_));
  }
  metadata_loaded_ = true;
  return Status::Ok();
}

Status Array::load_remote_non_empty_domain() {
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::ArrayError(
          "Cannot load metadata; remote array with no REST client."));
    RETURN_NOT_OK(rest_client->get_array_non_empty_domain(this, timestamp_));
  }
  return Status::Ok();
}

Status Array::compute_non_empty_domain() {
  if (remote_) {
    RETURN_NOT_OK(load_remote_non_empty_domain());
  } else if (!fragment_metadata_.empty()) {
    std::lock_guard<std::mutex> lock{mtx_};
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
      if (!meta_dom.empty())
        array_schema_->domain()->expand_ndrange(meta_dom, &non_empty_domain_);
      else {
        // If the fragment's non-empty domain is indeed empty, lets log it so
        // the user gets a message warning that this fragment might be corrupt
        // Note: LOG_STATUS only prints if TileDB is built in verbose mode.
        LOG_STATUS(Status::ArrayError(
            "Non empty domain unexpectedly empty for fragment: " +
            fragment_metadata_[j]->fragment_uri().to_string()));
      }
    }
  }
  non_empty_domain_computed_ = true;
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
