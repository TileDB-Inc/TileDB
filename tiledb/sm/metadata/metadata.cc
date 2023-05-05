/**
 * @file   metadata.cc
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
 * This file implements class Metadata.
 */

#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/storage_format/serialization/serializers.h"

#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Return a Metadata error class Status with a given message **/
inline Status Status_MetadataError(const std::string& msg) {
  return {"[TileDB::Metadata] Error", msg};
}

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Metadata::Metadata()
    : Metadata(std::map<std::string, MetadataValue>()) {
}

Metadata::Metadata(const std::map<std::string, MetadataValue>& metadata_map)
    : metadata_map_(metadata_map)
    , timestamp_range_([]() -> std::pair<uint64_t, uint64_t> {
      auto t = utils::time::timestamp_now_ms();
      return std::make_pair(t, t);
    }()) {
  build_metadata_index();
}

Metadata::Metadata(const Metadata& rhs)
    : metadata_map_(rhs.metadata_map_)
    , timestamp_range_(rhs.timestamp_range_)
    , loaded_metadata_uris_(rhs.loaded_metadata_uris_)
    , uri_(rhs.uri_) {
  if (!rhs.metadata_index_.empty())
    build_metadata_index();
}

Metadata& Metadata::operator=(const Metadata& other) {
  metadata_map_ = other.metadata_map_;
  timestamp_range_ = other.timestamp_range_;
  loaded_metadata_uris_ = other.loaded_metadata_uris_;
  uri_ = other.uri_;
  build_metadata_index();
  return *this;
}

Metadata::~Metadata() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

void Metadata::clear() {
  metadata_map_.clear();
  metadata_index_.clear();
  loaded_metadata_uris_.clear();
  timestamp_range_ = std::make_pair(0, 0);
  uri_ = URI();
}

Status Metadata::get_uri(const URI& array_uri, URI* meta_uri) {
  if (uri_.to_string().empty())
    RETURN_NOT_OK(generate_uri(array_uri));

  *meta_uri = uri_;
  return Status::Ok();
}

Status Metadata::generate_uri(const URI& array_uri) {
  std::stringstream ss;
  ss << "__" << timestamp_range_.first << "_" << timestamp_range_.second << "_"
     << uuid::generate_uuid(false);
  uri_ = array_uri.join_path(constants::array_metadata_dir_name)
             .join_path(ss.str());

  return Status::Ok();
}

Metadata Metadata::deserialize(
    const std::vector<shared_ptr<Tile>>& metadata_tiles) {
  if (metadata_tiles.empty()) {
    return Metadata();
  }
  std::map<std::string, MetadataValue> metadata_map;

  Status st;
  uint32_t key_len;
  char del;
  size_t value_len;
  for (const auto& tile : metadata_tiles) {
    // Iterate over all items
    Deserializer deserializer(tile->data(), tile->size());
    while (deserializer.remaining_bytes()) {
      key_len = deserializer.read<uint32_t>();
      std::string key(deserializer.get_ptr<char>(key_len), key_len);
      deserializer.read(&del, sizeof(char));

      metadata_map.erase(key);

      // Handle deletion
      if (del)
        continue;

      MetadataValue value_struct;
      value_struct.del_ = del;
      value_struct.type_ = deserializer.read<char>();
      value_struct.num_ = deserializer.read<uint32_t>();

      if (value_struct.num_) {
        value_len = value_struct.num_ *
                    datatype_size(static_cast<Datatype>(value_struct.type_));
        value_struct.value_.resize(value_len);
        deserializer.read((void*)value_struct.value_.data(), value_len);
      }

      // Insert to metadata
      metadata_map.emplace(std::make_pair(key, std::move(value_struct)));
    }
  }

  return Metadata(metadata_map);
}

void Metadata::serialize(Serializer& serializer) const {
  // Do nothing if there are no metadata to serialize
  if (metadata_map_.empty()) {
    return;
  }

  for (const auto& meta : metadata_map_) {
    auto key_len = (uint32_t)meta.first.size();
    serializer.write<uint32_t>(key_len);
    serializer.write(meta.first.data(), meta.first.size());
    const auto& value = meta.second;
    serializer.write<char>(value.del_);
    if (!value.del_) {
      serializer.write<char>(value.type_);
      serializer.write<uint32_t>(value.num_);
      if (value.num_)
        serializer.write(value.value_.data(), value.value_.size());
    }
  }
}

const std::pair<uint64_t, uint64_t>& Metadata::timestamp_range() const {
  return timestamp_range_;
}

Status Metadata::del(const char* key) {
  assert(key != nullptr);

  std::unique_lock<std::mutex> lck(mtx_);

  MetadataValue value;
  value.del_ = 1;
  metadata_map_.insert_or_assign(key, std::move(value));
  build_metadata_index();

  return Status::Ok();
}

Status Metadata::put(
    const char* key,
    Datatype value_type,
    uint32_t value_num,
    const void* value) {
  assert(key != nullptr);
  assert(value_type != Datatype::ANY);

  if (value == nullptr)
    value_num = 0;

  std::unique_lock<std::mutex> lck(mtx_);

  size_t value_size = value_num * datatype_size(value_type);
  MetadataValue value_struct;
  value_struct.del_ = 0;
  value_struct.type_ = static_cast<char>(value_type);
  value_struct.num_ = value_num;
  if (value_size) {
    value_struct.value_.resize(value_size);
    std::memcpy(value_struct.value_.data(), value, value_size);
  }
  metadata_map_.insert_or_assign(key, std::move(value_struct));
  build_metadata_index();

  return Status::Ok();
}

Status Metadata::get(
    const char* key,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) const {
  assert(key != nullptr);

  auto it = metadata_map_.find(key);
  if (it == metadata_map_.end()) {
    // Key not found
    *value = nullptr;
    return Status::Ok();
  }

  // Key found
  auto& value_struct = it->second;
  *value_type = static_cast<Datatype>(value_struct.type_);
  if (value_struct.num_ == 0) {
    // zero-valued keys
    *value_num = 1;
    *value = nullptr;
  } else {
    *value_num = value_struct.num_;
    *value = (const void*)(value_struct.value_.data());
  }

  return Status::Ok();
}

Status Metadata::get(
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  if (metadata_index_.empty())
    build_metadata_index();

  if (index >= metadata_index_.size())
    return LOG_STATUS(
        Status_MetadataError("Cannot get metadata; index out of bounds"));

  // Get key
  auto& key_str = *(metadata_index_[index].first);
  *key = key_str.c_str();
  *key_len = (uint32_t)key_str.size();

  // Get value
  auto& value_struct = *(metadata_index_[index].second);
  *value_type = static_cast<Datatype>(value_struct.type_);
  if (value_struct.num_ == 0) {
    // zero-valued keys
    *value_num = 1;
    *value = nullptr;
  } else {
    *value_num = value_struct.num_;
    *value = (const void*)(value_struct.value_.data());
  }
  return Status::Ok();
}

Status Metadata::has_key(const char* key, Datatype* value_type, bool* has_key) {
  assert(key != nullptr);

  auto it = metadata_map_.find(key);
  if (it == metadata_map_.end()) {
    // Key not found
    *has_key = false;
    return Status::Ok();
  }

  // Key found
  auto& value_struct = it->second;
  *value_type = static_cast<Datatype>(value_struct.type_);
  *has_key = true;

  return Status::Ok();
}

uint64_t Metadata::num() const {
  return metadata_map_.size();
}

void Metadata::set_loaded_metadata_uris(
    const std::vector<TimestampedURI>& loaded_metadata_uris) {
  if (loaded_metadata_uris.empty()) {
    return;
  }

  loaded_metadata_uris_.clear();
  for (const auto& uri : loaded_metadata_uris) {
    loaded_metadata_uris_.push_back(uri.uri_);
  }

  timestamp_range_.first = loaded_metadata_uris.front().timestamp_range_.first;
  timestamp_range_.second = loaded_metadata_uris.back().timestamp_range_.second;
}

const std::vector<URI>& Metadata::loaded_metadata_uris() const {
  return loaded_metadata_uris_;
}

void Metadata::swap(Metadata* metadata) {
  std::swap(metadata_map_, metadata->metadata_map_);
  std::swap(metadata_index_, metadata->metadata_index_);
  std::swap(timestamp_range_, metadata->timestamp_range_);
  std::swap(loaded_metadata_uris_, metadata->loaded_metadata_uris_);
}

void Metadata::reset(uint64_t timestamp) {
  clear();
  timestamp = (timestamp != 0) ? timestamp : utils::time::timestamp_now_ms();
  timestamp_range_ = std::make_pair(timestamp, timestamp);
}

void Metadata::reset(
    const uint64_t timestamp_start, const uint64_t timestamp_end) {
  clear();
  timestamp_range_ = std::make_pair(timestamp_start, timestamp_end);
}

Metadata::iterator Metadata::begin() const {
  return metadata_map_.cbegin();
}

Metadata::iterator Metadata::end() const {
  return metadata_map_.cend();
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void Metadata::build_metadata_index() {
  // Create metadata index for fast lookups from index
  metadata_index_.resize(metadata_map_.size());
  size_t i = 0;
  for (auto& m : metadata_map_)
    metadata_index_[i++] = std::make_pair(&(m.first), &(m.second));
}

}  // namespace sm
}  // namespace tiledb
