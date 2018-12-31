/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/encryption/encryption.h"
#include "tiledb/sm/misc/logger.h"

#include <cassert>
#include <iostream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

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
  last_max_buffer_sizes_subarray_ = nullptr;
}

Array::~Array() {
  std::free(last_max_buffer_sizes_subarray_);
}

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

Status Array::compute_max_buffer_sizes(
    const void* subarray,
    const std::vector<std::string>& attributes,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        max_buffer_sizes) const {
  std::unique_lock<std::mutex> lck(mtx_);

  // Error if the array is not open
  if (!is_open_)
    return LOG_STATUS(Status::ArrayError(
        "Cannot compute max buffer sizes; Array is not open"));

  // Error if the array was not opened in read mode
  if (query_type_ != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot compute max read buffer sizes; "
                           "Array was not opened in read mode"));

  // Check attributes
  RETURN_NOT_OK(array_schema_->check_attributes(attributes));

  // Compute buffer sizes
  max_buffer_sizes->clear();
  for (const auto& attr : attributes)
    (*max_buffer_sizes)[attr] = std::pair<uint64_t, uint64_t>(0, 0);
  RETURN_NOT_OK(compute_max_buffer_sizes(subarray, max_buffer_sizes));

  // Close array
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

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_.set_key(encryption_type, encryption_key, key_length));

  if (query_type == QueryType::READ) {
    timestamp_ = utils::time::timestamp_now_ms();
    RETURN_NOT_OK(storage_manager_->array_open_for_reads(
        array_uri_,
        timestamp_,
        encryption_key_,
        &array_schema_,
        &fragment_metadata_));
  } else {
    timestamp_ = 0;
    RETURN_NOT_OK(storage_manager_->array_open_for_writes(
        array_uri_, encryption_key_, &array_schema_));
  }

  query_type_ = query_type;
  is_open_ = true;

  return Status::Ok();
}

Status Array::open(
    QueryType query_type,
    const std::vector<FragmentInfo>& fragments,
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

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_.set_key(encryption_type, encryption_key, key_length));

  timestamp_ = utils::time::timestamp_now_ms();

  // Open the array.
  RETURN_NOT_OK(storage_manager_->array_open_for_reads(
      array_uri_,
      fragments,
      encryption_key_,
      &array_schema_,
      &fragment_metadata_));

  query_type_ = QueryType::READ;
  is_open_ = true;

  return Status::Ok();
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

  if (query_type != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot open array at timestamp; The array can "
                           "opened at a timestamp only in read mode"));

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_.set_key(encryption_type, encryption_key, key_length));

  timestamp_ = timestamp;

  // Open the array.
  RETURN_NOT_OK(storage_manager_->array_open_for_reads(
      array_uri_,
      timestamp_,
      encryption_key_,
      &array_schema_,
      &fragment_metadata_));

  query_type_ = query_type;
  is_open_ = true;

  return Status::Ok();
}

Status Array::close() {
  std::unique_lock<std::mutex> lck(mtx_);

  if (!is_open_)
    return Status::Ok();

  is_open_ = false;
  clear_last_max_buffer_sizes();
  array_schema_ = nullptr;
  fragment_metadata_.clear();

  if (query_type_ == QueryType::READ) {
    RETURN_NOT_OK(storage_manager_->array_close_for_reads(array_uri_));
  } else {
    RETURN_NOT_OK(storage_manager_->array_close_for_writes(array_uri_));
  }

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
    const char* attribute, const void* subarray, uint64_t* buffer_size) {
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

  // Check if attribute is null
  if (attribute == nullptr)
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; Attribute is null"));

  RETURN_NOT_OK(compute_max_buffer_sizes(subarray));

  // Normalize attribute name
  std::string norm_attribute;
  RETURN_NOT_OK(
      ArraySchema::attribute_name_normalized(attribute, &norm_attribute));

  // Check if attribute exists
  auto it = last_max_buffer_sizes_.find(norm_attribute);
  if (it == last_max_buffer_sizes_.end())
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute '") +
        norm_attribute + "' does not exist"));

  // Check if attribute is fixed sized
  if (array_schema_->var_size(norm_attribute))
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute '") +
        norm_attribute + "' is var-sized"));

  // Retrieve buffer size
  *buffer_size = it->second.first;

  return Status::Ok();
}

Status Array::get_max_buffer_size(
    const char* attribute,
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

  // Check if attribute is null
  if (attribute == nullptr)
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; Attribute is null"));

  RETURN_NOT_OK(compute_max_buffer_sizes(subarray));

  // Normalize attribute name
  std::string norm_attribute;
  RETURN_NOT_OK(
      ArraySchema::attribute_name_normalized(attribute, &norm_attribute));

  // Check if attribute exists
  auto it = last_max_buffer_sizes_.find(norm_attribute);
  if (it == last_max_buffer_sizes_.end())
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute '") +
        norm_attribute + "' does not exist"));

  // Check if attribute is var-sized
  if (!array_schema_->var_size(norm_attribute))
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute '") +
        norm_attribute + "' is fixed-sized"));

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
        Status::ArrayError("Cannot reopen array; Array store was "
                           "not opened in read mode"));

  clear_last_max_buffer_sizes();

  timestamp_ = timestamp;
  fragment_metadata_.clear();

  return storage_manager_->array_reopen(
      array_uri_,
      timestamp_,
      encryption_key_,
      &array_schema_,
      &fragment_metadata_);
}

uint64_t Array::timestamp() const {
  std::unique_lock<std::mutex> lck(mtx_);
  return timestamp_;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void Array::clear_last_max_buffer_sizes() {
  last_max_buffer_sizes_.clear();
  std::free(last_max_buffer_sizes_subarray_);
  last_max_buffer_sizes_subarray_ = nullptr;
}

Status Array::compute_max_buffer_sizes(const void* subarray) {
  // Allocate space for max buffer sizes subarray
  auto subarray_size = 2 * array_schema_->coords_size();
  if (last_max_buffer_sizes_subarray_ == nullptr) {
    last_max_buffer_sizes_subarray_ = std::malloc(subarray_size);
    if (last_max_buffer_sizes_subarray_ == nullptr)
      return LOG_STATUS(Status::ArrayError(
          "Cannot compute max buffer sizes; Subarray allocation failed"));
  }

  // Compute max buffer sizes
  if (last_max_buffer_sizes_.empty() ||
      std::memcmp(last_max_buffer_sizes_subarray_, subarray, subarray_size) !=
          0) {
    last_max_buffer_sizes_.clear();

    // Get all attributes and coordinates
    auto attributes = array_schema_->attributes();
    last_max_buffer_sizes_.clear();
    for (const auto& attr : attributes)
      last_max_buffer_sizes_[attr->name()] =
          std::pair<uint64_t, uint64_t>(0, 0);
    last_max_buffer_sizes_[constants::coords] =
        std::pair<uint64_t, uint64_t>(0, 0);
    RETURN_NOT_OK(compute_max_buffer_sizes(subarray, &last_max_buffer_sizes_));
  }

  // Update subarray
  std::memcpy(last_max_buffer_sizes_subarray_, subarray, subarray_size);

  return Status::Ok();
}

Status Array::compute_max_buffer_sizes(
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) const {
  // Return if there are no metadata
  if (fragment_metadata_.empty())
    return Status::Ok();

  // Compute buffer sizes
  switch (array_schema_->coords_type()) {
    case Datatype::INT32:
      return compute_max_buffer_sizes<int>(
          static_cast<const int*>(subarray), buffer_sizes);
    case Datatype::INT64:
      return compute_max_buffer_sizes<int64_t>(
          static_cast<const int64_t*>(subarray), buffer_sizes);
    case Datatype::FLOAT32:
      return compute_max_buffer_sizes<float>(
          static_cast<const float*>(subarray), buffer_sizes);
    case Datatype::FLOAT64:
      return compute_max_buffer_sizes<double>(
          static_cast<const double*>(subarray), buffer_sizes);
    case Datatype::INT8:
      return compute_max_buffer_sizes<int8_t>(
          static_cast<const int8_t*>(subarray), buffer_sizes);
    case Datatype::UINT8:
      return compute_max_buffer_sizes<uint8_t>(
          static_cast<const uint8_t*>(subarray), buffer_sizes);
    case Datatype::INT16:
      return compute_max_buffer_sizes<int16_t>(
          static_cast<const int16_t*>(subarray), buffer_sizes);
    case Datatype::UINT16:
      return compute_max_buffer_sizes<uint16_t>(
          static_cast<const uint16_t*>(subarray), buffer_sizes);
    case Datatype::UINT32:
      return compute_max_buffer_sizes<uint32_t>(
          static_cast<const uint32_t*>(subarray), buffer_sizes);
    case Datatype::UINT64:
      return compute_max_buffer_sizes<uint64_t>(
          static_cast<const uint64_t*>(subarray), buffer_sizes);
    default:
      return LOG_STATUS(Status::ArrayError(
          "Cannot compute max read buffer sizes; Invalid coordinates type"));
  }

  return Status::Ok();
}

template <class T>
Status Array::compute_max_buffer_sizes(
    const T* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        max_buffer_sizes) const {
  // Sanity check
  assert(!fragment_metadata_.empty());

  // First we calculate a rough upper bound. Especially for dense
  // arrays, this will not be accurate, as it accounts only for the
  // non-empty regions of the subarray.
  for (auto& meta : fragment_metadata_)
    RETURN_NOT_OK(meta->add_max_buffer_sizes(subarray, max_buffer_sizes));

  // Rectify bound for dense arrays
  if (array_schema_->dense()) {
    auto cell_num = array_schema_->domain()->cell_num(subarray);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *max_buffer_sizes) {
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

  // Rectify bound for sparse arrays with integer domain
  if (!array_schema_->dense() &&
      datatype_is_integer(array_schema_->domain()->type())) {
    auto cell_num = array_schema_->domain()->cell_num(subarray);
    // `cell_num` becomes 0 when `subarray` is huge, leading to a
    // `uint64_t` overflow.
    if (cell_num != 0) {
      for (auto& it : *max_buffer_sizes) {
        if (!array_schema_->var_size(it.first)) {
          // Check for overflow
          uint64_t new_size = cell_num * array_schema_->cell_size(it.first);
          if (new_size / array_schema_->cell_size((it.first)) != cell_num)
            continue;

          // Potentially rectify size
          it.second.first = MIN(it.second.first, new_size);
        }
      }
    }
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
