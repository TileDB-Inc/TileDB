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

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Array::Array(const URI& array_uri, StorageManager* storage_manager)
    : array_uri_(array_uri)
    , storage_manager_(storage_manager) {
  is_open_ = false;
  open_array_ = nullptr;
  snapshot_ = 0;
  last_max_buffer_sizes_subarray_ = nullptr;
  encryption_type_ = EncryptionType::NO_ENCRYPTION;
}

Array::~Array() {
  std::free(last_max_buffer_sizes_subarray_);
  if (encryption_key_.data() != nullptr)
    std::memset(encryption_key_.data(), 0, encryption_key_.alloced_size());
}

/* ********************************* */
/*                API                */
/* ********************************* */

ArraySchema* Array::array_schema() const {
  if (!is_open())
    return nullptr;

  open_array_->mtx_lock();
  auto array_schema = open_array_->array_schema();
  open_array_->mtx_unlock();
  return array_schema;
}

const URI& Array::array_uri() const {
  return array_uri_;
}

Status Array::compute_max_buffer_sizes(
    const void* subarray,
    const std::vector<std::string>& attributes,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        max_buffer_sizes) const {
  if (!is_open())
    return LOG_STATUS(Status::ArrayError(
        "Cannot compute max buffer sizes; Array is not open"));

  return storage_manager_->array_compute_max_buffer_sizes(
      open_array_, snapshot_, subarray, attributes, max_buffer_sizes);
}

Status Array::open(QueryType query_type, const void* encryption_key) {
  if (is_open())
    return LOG_STATUS(
        Status::ArrayError("Cannot open array; Array already open"));

  RETURN_NOT_OK(storage_manager_->array_open(
      array_uri_, query_type, encryption_key, &open_array_, &snapshot_));

  if (encryption_key != nullptr) {
    auto schema = open_array_->array_schema();
    RETURN_NOT_OK(
        set_encryption_key(schema->encryption_type(), encryption_key));
  }

  is_open_ = true;

  return Status::Ok();
}

Status Array::close() {
  std::unique_lock<std::mutex> lck(mtx_);

  if (!is_open())
    return Status::Ok();

  is_open_ = false;
  clear_last_max_buffer_sizes();
  RETURN_NOT_OK(
      storage_manager_->array_close(array_uri_, open_array_->query_type()));
  open_array_ = nullptr;

  return Status::Ok();
}

bool Array::is_empty() const {
  return is_open() && open_array_->is_empty(snapshot_);
}

bool Array::is_open() const {
  return open_array_ != nullptr && is_open_;
}

std::vector<FragmentMetadata*> Array::fragment_metadata() const {
  if (!is_open())
    return std::vector<FragmentMetadata*>();

  open_array_->mtx_lock();
  auto metadata = open_array_->fragment_metadata(snapshot_);
  open_array_->mtx_unlock();
  return metadata;
}

Status Array::get_array_schema(ArraySchema** array_schema) const {
  // Error if the array is not open
  if (!is_open())
    return LOG_STATUS(
        Status::ArrayError("Cannot get array schema; Array is not open"));

  *array_schema = open_array_->array_schema();

  return Status::Ok();
}

Status Array::get_query_type(QueryType* query_type) const {
  // Error if the array is not open
  if (!is_open())
    return LOG_STATUS(
        Status::ArrayError("Cannot get query_type; Array is not open"));

  *query_type = open_array_->query_type();

  return Status::Ok();
}

Status Array::get_max_buffer_size(
    const char* attribute, const void* subarray, uint64_t* buffer_size) {
  // Check if array is open
  if (!is_open())
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; Array is not open"));

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
  if (open_array_->array_schema()->var_size(norm_attribute))
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
  // Check if array is open
  if (!is_open())
    return LOG_STATUS(
        Status::ArrayError("Cannot get max buffer size; Array is not open"));

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
  if (!open_array_->array_schema()->var_size(norm_attribute))
    return LOG_STATUS(Status::ArrayError(
        std::string("Cannot get max buffer size; Attribute '") +
        norm_attribute + "' is fixed-sized"));

  // Retrieve buffer sizes
  *buffer_off_size = it->second.first;
  *buffer_val_size = it->second.second;

  return Status::Ok();
}

EncryptionType Array::get_encryption_type() const {
  return encryption_type_;
}

ConstBuffer Array::get_encryption_key() const {
  return ConstBuffer(encryption_key_.data(), encryption_key_.size());
}

Status Array::reopen() {
  std::unique_lock<std::mutex> lck(mtx_);

  if (!is_open())
    return LOG_STATUS(
        Status::ArrayError("Cannot reopen array; Array is not open"));

  if (open_array_->query_type() != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot reopen array; Key-value store was "
                           "not opened in read mode"));

  clear_last_max_buffer_sizes();

  return storage_manager_->array_reopen(
      open_array_, encryption_key_.data(), &snapshot_);
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
  auto subarray_size = 2 * open_array_->array_schema()->coords_size();
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
    RETURN_NOT_OK(storage_manager_->array_compute_max_buffer_sizes(
        open_array_, snapshot_, subarray, &last_max_buffer_sizes_));
  }

  // Update subarray
  std::memcpy(last_max_buffer_sizes_subarray_, subarray, subarray_size);

  return Status::Ok();
}

Status Array::set_encryption_key(
    EncryptionType encryption_type, const void* key_bytes) {
  // Get the key size (in bytes)
  unsigned key_size;
  switch (encryption_type) {
    case EncryptionType::NO_ENCRYPTION:
      return LOG_STATUS(Status::ArrayError(
          "Error setting encryption key: array not encrypted."));
    case EncryptionType::AES_256_GCM:
      key_size = Encryption::AES256GCM_KEY_BYTES;
      break;
    default:
      return LOG_STATUS(Status::ArrayError(
          "Error setting encryption key: invalid encryption type."));
  }

  // Copy the key
  encryption_key_.reset_size();
  encryption_key_.reset_offset();
  if (encryption_key_.free_space() < key_size)
    RETURN_NOT_OK(
        encryption_key_.realloc(encryption_key_.alloced_size() + key_size));
  RETURN_NOT_OK(encryption_key_.write(key_bytes, key_size));
  encryption_key_.reset_offset();

  encryption_type_ = encryption_type;

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
