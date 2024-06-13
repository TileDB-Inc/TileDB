/**
 * @file   metadata.h
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
 This file defines class Metadata.
 */

#ifndef TILEDB_METADATA_H
#define TILEDB_METADATA_H

#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Buffer;
class ConstBuffer;
class MemoryTracker;
enum class Datatype : uint8_t;

/**
 * Handles writing/reading metadata. The persistent format of the metadata is:
 * `0 (char) | key_len (uint32_t) | key (char*) | value_type (Datatype) |
 *  value_num (uint32_t) | values (void*)`
 * or
 * `1 (char) | key_len (uint32_t) | key (char*) | value_type (Datatype) |
 *  value_num (uint32_t) | values (void*)`
 *
 * The first char value is 1 if it is a deletion and 0 if it is an insertion.
 */
class Metadata {
 public:
  /* ********************************* */
  /*       PUBLIC TYPE DEFINITIONS     */
  /* ********************************* */

  /** Represents a metadata value. */
  struct MetadataValue {
    /** 1 if it is a deletion and 0 if it is an insertion. */
    char del_ = 0;
    /** The value datatype. */
    char type_ = 0;
    /** The number of values. */
    uint32_t num_ = 0;
    /** The value in binary format. */
    std::vector<uint8_t> value_;
  };

  /** Iterator type for iterating over metadata values. */
  typedef tdb::pmr::map<std::string, MetadataValue>::const_iterator iterator;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor is deleted. */
  Metadata() = delete;

  /** Constructor. */
  Metadata(shared_ptr<MemoryTracker> memory_tracker);

  DISABLE_COPY(Metadata);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Metadata);

  /** Destructor. */
  ~Metadata() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Copy assignment. */
  Metadata& operator=(Metadata& other);

  /** Assignment via std::map. */
  Metadata& operator=(std::map<std::string, MetadataValue>&& md_map);

  /** Returns the memory tracker. */
  inline shared_ptr<MemoryTracker> memory_tracker() {
    return memory_tracker_;
  }

  /** Clears the metadata. */
  void clear();

  /** Retrieves the array metadata URI. */
  URI get_uri(const URI& array_uri);

  /** Generates a new array metadata URI. */
  void generate_uri(const URI& array_uri);

  /**
   * Deserializes the input metadata buffers. Note that the buffers are
   * assumed to be sorted on time. The function will take care of any
   * deleted or overwritten metadata items considering the order.
   */
  static std::map<std::string, MetadataValue> deserialize(
      const std::vector<shared_ptr<Tile>>& metadata_tiles);

  /** Serializes all key-value metadata items into the input buffer. */
  void serialize(Serializer& serializer) const;

  /**
   * Stores the metadata into persistent storage.
   *
   * @param resources The context resources.
   * @param uri The object URI.
   * @param encryption_key The encryption key to use.
   */
  void store(
      ContextResources& resources,
      const URI& uri,
      const EncryptionKey& encryption_key);

  /**
   * Deletes a metadata item.
   *
   * @param key The key of the metadata to be deleted.
   */
  void del(const char* key);

  /**
   * Puts a metadata item as a key-value pair.
   *
   * @param key The metadata key.
   * @param value_type The datatype of the value.
   * @param value_num The number of items in the value part (they could be more
   *     than one).
   * @param value The metadata value.
   */
  void put(
      const char* key,
      Datatype value_type,
      uint32_t value_num,
      const void* value);

  /**
   * Gets a metadata item as a key-value pair.
   *
   * @param key The metadata key.
   * @param[out] value_type The datatype of the value.
   * @param[out] value_num The number of items in the value part (they could be
   * more than one).
   * @param[out] value The metadata value. It will be `nullptr` if the key does
   *     not exist
   */
  void get(
      const char* key,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value) const;

  /**
   * Gets a metadata item as a key-value pair using an index.
   *
   * @param index The index used to retrieve the metadata.
   * @param[out] key The metadata key.
   * @param[out] key_len The metadata key length.
   * @param[out] value_type The datatype of the value.
   * @param[out] value_num The number of items in the value part (they could be
   * more than one).
   * @param[out] value The metadata value. It will be `nullptr` if the key does
   *     not exist
   */
  void get(
      uint64_t index,
      const char** key,
      uint32_t* key_len,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value);

  /** Returns the number of metadata items. */
  uint64_t num() const;

  /** Gets the type of the given metadata or nullopt if it does not exist. */
  std::optional<Datatype> metadata_type(const char* key);

  /**
   * Sets the URIs of the metadata files that have been loaded
   * to this object.
   */
  void set_loaded_metadata_uris(
      const std::vector<TimestampedURI>& loaded_metadata_uris);

  /**
   * Returns the URIs of the metadata files that have been loaded
   * to this object.
   */
  const tdb::pmr::vector<URI>& loaded_metadata_uris() const;

  /**
   * Clears the metadata and assigns the input timestamp to
   * its timestamp range. If `timestamp` is 0, then the metadata
   * timestamp range is set to the current time.
   */
  void reset(uint64_t timestamp);

  /**
   * Clears the metadata and assigns the input timestamp to
   * its timestamp range.
   */
  void reset(uint64_t timestamp_start, uint64_t timestamp_end);

  /** Returns an iterator to the beginning of the metadata. */
  iterator begin() const;

  /** Returns an iterator to the end of the metadata. */
  iterator end() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The memory tracker. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** A map from metadata key to metadata value. */
  tdb::pmr::map<std::string, MetadataValue> metadata_map_;

  /**
   * A vector pointing to all the values in `metadata_map_`. It facilitates
   * searching metadata from index. Used only for reading metadata (inapplicable
   * when writing metadata).
   */
  tdb::pmr::vector<std::pair<const std::string*, MetadataValue*>>
      metadata_index_;

  /** Mutex for thread-safety. */
  mutable std::mutex mtx_;

  /**
   * The URIs of the metadata files that have been loaded to this object.
   * This is needed to know which files to delete upon consolidation.
   */
  tdb::pmr::vector<URI> loaded_metadata_uris_;

  /** The URI of the array metadata file. */
  URI uri_;

  /** Timestamped name. */
  std::string timestamped_name_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Build the metadata index vector from the metadata map
   */
  void build_metadata_index();
};

}  // namespace tiledb::sm

#endif  // TILEDB_METADATA_H
