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
#include "tiledb/sm/misc/stats.h"

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

Status Array::capnp(::Array::Builder* arrayBuilder) const {
  STATS_FUNC_IN(serialization_array_to_capnp);
  arrayBuilder->setTimestamp(this->timestamp());
  arrayBuilder->setEncryptionKey(kj::arrayPtr(
      static_cast<const kj::byte*>(this->encryption_key_.key().data()),
      this->encryption_key_.key().size()));
  arrayBuilder->setEncryptionType(
      encryption_type_str(this->encryption_key_.encryption_type()));
  if (!this->last_max_buffer_sizes_.empty()) {
    ::MapMaxBufferSizes::Builder lastMaxBufferSizesBuilder =
        arrayBuilder->initLastMaxBufferSizes();
    capnp::List<MapMaxBufferSizes::Entry>::Builder entries =
        lastMaxBufferSizesBuilder.initEntries(
            this->last_max_buffer_sizes_.size());
    size_t i = 0;
    for (auto maxBufferSize : this->last_max_buffer_sizes_) {
      ::MapMaxBufferSizes::Entry::Builder entry = entries[i++];
      entry.setKey(maxBufferSize.first);
      ::MaxBufferSize::Builder maxBufferSizeBuilder = entry.initValue();
      maxBufferSizeBuilder.setBufferOffsetSize(maxBufferSize.second.first);
      maxBufferSizeBuilder.setBufferSize(maxBufferSize.second.second);
    }

    if (this->last_max_buffer_sizes_subarray_ != nullptr) {
      ::DomainArray::Builder subarray =
          arrayBuilder->initLastMaxBufferSizesSubarray();
      switch (this->array_schema()->domain()->type()) {
        case Datatype::INT8: {
          subarray.setInt8(kj::arrayPtr(
              static_cast<const int8_t*>(this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::UINT8: {
          subarray.setUint8(kj::arrayPtr(
              static_cast<const uint8_t*>(
                  this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::INT16: {
          subarray.setInt16(kj::arrayPtr(
              static_cast<const int16_t*>(
                  this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::UINT16: {
          subarray.setUint16(kj::arrayPtr(
              static_cast<const uint16_t*>(
                  this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::INT32: {
          subarray.setInt32(kj::arrayPtr(
              static_cast<const int32_t*>(
                  this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::UINT32: {
          subarray.setUint32(kj::arrayPtr(
              static_cast<const uint32_t*>(
                  this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::INT64: {
          subarray.setInt64(kj::arrayPtr(
              static_cast<const int64_t*>(
                  this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::UINT64: {
          subarray.setUint64(kj::arrayPtr(
              static_cast<const uint64_t*>(
                  this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::FLOAT32: {
          subarray.setFloat32(kj::arrayPtr(
              static_cast<const float*>(this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        case Datatype::FLOAT64: {
          subarray.setFloat64(kj::arrayPtr(
              static_cast<const double*>(this->last_max_buffer_sizes_subarray_),
              this->array_schema()->dim_num() * 2));
          break;
        }
        default: {
          return Status::Error("Unknown/Unsupported domain datatype in capnp");
        }
      }
    }
  }
  return Status::Ok();
  STATS_FUNC_OUT(serialization_array_to_capnp);
}

tiledb::sm::Status Array::from_capnp(::Array::Reader array) {
  STATS_FUNC_IN(serialization_array_from_capnp);
  this->timestamp_ = array.getTimestamp();

  // set default encryption to avoid uninitialized warnings
  EncryptionType encryptionType = EncryptionType::NO_ENCRYPTION;
  auto st = encryption_type_enum(array.getEncryptionType(), &encryptionType);
  if (!st.ok()) {
    return st;
  }

  st = this->encryption_key_.set_key(
      encryptionType,
      array.getEncryptionKey().begin(),
      array.getEncryptionKey().size());
  if (!st.ok()) {
    return st;
  }

  if (array.hasLastMaxBufferSizes()) {
    ::MapMaxBufferSizes::Reader lastMaxBufferSizesBuilder =
        array.getLastMaxBufferSizes();
    if (lastMaxBufferSizesBuilder.hasEntries()) {
      capnp::List<MapMaxBufferSizes::Entry>::Reader entries =
          lastMaxBufferSizesBuilder.getEntries();
      for (::MapMaxBufferSizes::Entry::Reader entry : entries) {
        this->last_max_buffer_sizes_.emplace(
            entry.getKey().cStr(),
            std::make_pair(
                entry.getValue().getBufferOffsetSize(),
                entry.getValue().getBufferSize()));
      }
    }
  }

  if (array.hasLastMaxBufferSizesSubarray()) {
    ::DomainArray::Reader lastMaxBuffserSizesSubArray =
        array.getLastMaxBufferSizesSubarray();
    switch (this->array_schema()->domain()->type()) {
      case Datatype::INT8: {
        if (lastMaxBuffserSizesSubArray.hasInt8()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getInt8();
          int8_t* lastMaxBuffserSizesSubArrayLocal =
              new int8_t[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::UINT8: {
        if (lastMaxBuffserSizesSubArray.hasUint8()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getUint8();
          uint8_t* lastMaxBuffserSizesSubArrayLocal =
              new uint8_t[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::INT16: {
        if (lastMaxBuffserSizesSubArray.hasInt16()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getInt16();
          int16_t* lastMaxBuffserSizesSubArrayLocal =
              new int16_t[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::UINT16: {
        if (lastMaxBuffserSizesSubArray.hasUint16()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getUint16();
          uint16_t* lastMaxBuffserSizesSubArrayLocal =
              new uint16_t[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::INT32: {
        if (lastMaxBuffserSizesSubArray.hasInt32()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getInt32();
          int32_t* lastMaxBuffserSizesSubArrayLocal =
              new int32_t[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::UINT32: {
        if (lastMaxBuffserSizesSubArray.hasUint32()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getUint32();
          uint32_t* lastMaxBuffserSizesSubArrayLocal =
              new uint32_t[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::INT64: {
        if (lastMaxBuffserSizesSubArray.hasInt64()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getInt64();
          int64_t* lastMaxBuffserSizesSubArrayLocal =
              new int64_t[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::UINT64: {
        if (lastMaxBuffserSizesSubArray.hasUint64()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getUint64();
          uint64_t* lastMaxBuffserSizesSubArrayLocal =
              new uint64_t[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::FLOAT32: {
        if (lastMaxBuffserSizesSubArray.hasFloat32()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getFloat32();
          float* lastMaxBuffserSizesSubArrayLocal =
              new float[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      case Datatype::FLOAT64: {
        if (lastMaxBuffserSizesSubArray.hasFloat64()) {
          auto lastMaxBuffserSizesSubArrayList =
              lastMaxBuffserSizesSubArray.getFloat64();
          double* lastMaxBuffserSizesSubArrayLocal =
              new double[lastMaxBuffserSizesSubArrayList.size()];
          for (size_t i = 0; i < lastMaxBuffserSizesSubArrayList.size(); i++)
            lastMaxBuffserSizesSubArrayLocal[i] =
                lastMaxBuffserSizesSubArrayList[i];

          this->last_max_buffer_sizes_subarray_ =
              lastMaxBuffserSizesSubArrayLocal;
        }
        break;
      }
      default: {
        return Status::Error(
            "Unknown/Unsupported domain datatype in from_capnp");
      }
    }
  }

  return Status::Ok();
  STATS_FUNC_OUT(serialization_array_from_capnp);
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
      open_array_, timestamp_, subarray, attributes, max_buffer_sizes);
}

Status Array::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  if (is_open())
    return LOG_STATUS(
        Status::ArrayError("Cannot open array; Array already open"));

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_.set_key(encryption_type, encryption_key, key_length));

  timestamp_ = utils::time::timestamp_now_ms();

  // Open the array.
  RETURN_NOT_OK(storage_manager_->array_open(
      array_uri_, query_type, encryption_key_, &open_array_, timestamp_));

  is_open_ = true;

  return Status::Ok();
}

Status Array::open_at(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    uint64_t timestamp) {
  if (is_open())
    return LOG_STATUS(Status::ArrayError(
        "Cannot open array at timestamp; Array already open"));

  if (query_type != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot open array at timestamp; This is applicable "
                           "only to read queries"));

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_.set_key(encryption_type, encryption_key, key_length));

  timestamp_ = timestamp;

  // Open the array.
  RETURN_NOT_OK(storage_manager_->array_open(
      array_uri_, query_type, encryption_key_, &open_array_, timestamp));

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
  return is_open() && open_array_->is_empty(timestamp_);
}

bool Array::is_open() const {
  return open_array_ != nullptr && is_open_;
}

std::vector<FragmentMetadata*> Array::fragment_metadata() const {
  if (!is_open())
    return std::vector<FragmentMetadata*>();

  open_array_->mtx_lock();
  auto metadata = open_array_->fragment_metadata(timestamp_);
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

const EncryptionKey& Array::get_encryption_key() const {
  return encryption_key_;
}

Status Array::reopen() {
  return reopen_at(utils::time::timestamp_now_ms());
}

Status Array::reopen_at(uint64_t timestamp) {
  std::unique_lock<std::mutex> lck(mtx_);

  if (!is_open())
    return LOG_STATUS(
        Status::ArrayError("Cannot reopen array; Array is not open"));

  if (open_array_->query_type() != QueryType::READ)
    return LOG_STATUS(
        Status::ArrayError("Cannot reopen array; Array store was "
                           "not opened in read mode"));

  clear_last_max_buffer_sizes();

  timestamp_ = timestamp;

  return storage_manager_->array_reopen(
      open_array_, encryption_key_, timestamp_);
}

uint64_t Array::timestamp() const {
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
        open_array_, timestamp_, subarray, &last_max_buffer_sizes_));
  }

  // Update subarray
  std::memcpy(last_max_buffer_sizes_subarray_, subarray, subarray_size);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
