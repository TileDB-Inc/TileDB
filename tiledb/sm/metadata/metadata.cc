/**
 * @file   metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/assert.h"
#include "tiledb/common/exception/exception.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/storage_format/uri/generate_uri.h"

#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace tiledb::sm {

class MetadataException : public StatusException {
 public:
  explicit MetadataException(const std::string& message)
      : StatusException("Metadata", message) {
  }
};

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */
Metadata::Metadata(shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , metadata_map_(memory_tracker_->get_resource(MemoryType::METADATA))
    , metadata_index_(memory_tracker_->get_resource(MemoryType::METADATA))
    , loaded_metadata_uris_(memory_tracker_->get_resource(MemoryType::METADATA))
    , timestamped_name_([]() -> std::string {
      auto t = utils::time::timestamp_now_ms();
      return tiledb::storage_format::generate_timestamped_name(
          t, t, std::nullopt);
    }()) {
  build_metadata_index();
}

/* ********************************* */
/*                API                */
/* ********************************* */

Metadata& Metadata::operator=(Metadata& other) {
  clear();
  for (auto& [k, v] : other.metadata_map_) {
    metadata_map_.emplace(k, v);
  }

  timestamped_name_ = other.timestamped_name_;

  for (auto& uri : other.loaded_metadata_uris_) {
    loaded_metadata_uris_.emplace_back(uri);
  }

  build_metadata_index();

  return *this;
}

Metadata& Metadata::operator=(
    std::map<std::string, Metadata::MetadataValue>&& md_map) {
  clear();
  for (auto& [k, v] : md_map) {
    metadata_map_.emplace(k, v);
  }

  build_metadata_index();

  return *this;
}

void Metadata::clear() {
  metadata_map_.clear();
  metadata_index_.clear();
  loaded_metadata_uris_.clear();
  uri_ = URI();
}

URI Metadata::get_uri(const URI& array_uri) {
  if (uri_.to_string().empty())
    generate_uri(array_uri);

  return uri_;
}

void Metadata::generate_uri(const URI& array_uri) {
  uri_ = array_uri.join_path(constants::array_metadata_dir_name)
             .join_path(timestamped_name_);
}

std::map<std::string, Metadata::MetadataValue> Metadata::deserialize(
    const std::vector<shared_ptr<Tile>>& metadata_tiles) {
  if (metadata_tiles.empty()) {
    return {};
  }

  std::map<std::string, MetadataValue> metadata_map;
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

  return metadata_map;
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

void Metadata::store(
    ContextResources& resources,
    const URI& uri,
    const EncryptionKey& encryption_key) {
  [[maybe_unused]] auto timer_se = resources.stats().start_timer("write_meta");

  // Serialize array metadata
  SizeComputationSerializer size_computation_serializer;
  serialize(size_computation_serializer);

  // Do nothing if there are no metadata to write
  if (0 == size_computation_serializer.size()) {
    return;
  }
  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(),
      resources.ephemeral_memory_tracker())};
  Serializer serializer(tile->data(), tile->size());
  serialize(serializer);
  resources.stats().add_counter(
      "write_meta_size", size_computation_serializer.size());

  // Create a metadata file name
  GenericTileIO::store_data(resources, get_uri(uri), tile, encryption_key);
}

void Metadata::del(const char* key) {
  iassert(key != nullptr);

  std::unique_lock<std::mutex> lck(mtx_);

  MetadataValue value;
  value.del_ = 1;
  metadata_map_.insert_or_assign(key, std::move(value));
  build_metadata_index();
}

void Metadata::put(
    const char* key,
    Datatype value_type,
    uint32_t value_num,
    const void* value) {
  iassert(key != nullptr);
  iassert(value_type != Datatype::ANY);

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
}

void Metadata::get(
    const char* key,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) const {
  iassert(key != nullptr);

  auto it = metadata_map_.find(key);
  if (it == metadata_map_.end()) {
    // Key not found
    *value = nullptr;
    return;
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
}

void Metadata::get(
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  if (metadata_index_.empty())
    build_metadata_index();

  if (index >= metadata_index_.size())
    throw MetadataException("Cannot get metadata; index out of bounds");

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
}

std::optional<Datatype> Metadata::metadata_type(const char* key) {
  iassert(key != nullptr);

  auto it = metadata_map_.find(key);
  if (it == metadata_map_.end()) {
    // Key not found
    return nullopt;
  }

  // Key found
  auto& value_struct = it->second;
  return static_cast<Datatype>(value_struct.type_);
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

  timestamped_name_ = tiledb::storage_format::generate_timestamped_name(
      loaded_metadata_uris.front().timestamp_range_.first,
      loaded_metadata_uris.back().timestamp_range_.second,
      std::nullopt);
}

const tdb::pmr::vector<URI>& Metadata::loaded_metadata_uris() const {
  return loaded_metadata_uris_;
}

void Metadata::reset(uint64_t timestamp) {
  clear();
  timestamped_name_ = tiledb::storage_format::generate_timestamped_name(
      timestamp, timestamp, std::nullopt);
}

void Metadata::reset(
    const uint64_t timestamp_start, const uint64_t timestamp_end) {
  clear();
  timestamped_name_ = tiledb::storage_format::generate_timestamped_name(
      timestamp_start, timestamp_end, std::nullopt);
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

}  // namespace tiledb::sm
