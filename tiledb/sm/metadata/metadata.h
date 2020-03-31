/**
 * @file   metadata.h
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
 This file defines class Metadata.
 */

#ifndef TILEDB_METADATA_H
#define TILEDB_METADATA_H

#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/uri.h"

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
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
  typedef std::map<std::string, MetadataValue>::const_iterator iterator;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Metadata();

  /** Destructor. */
  ~Metadata();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Clears the metadata. */
  void clear();

  /** Retrieves the array metadata URI. */
  Status get_uri(const URI& array_uri, URI* meta_uri);

  /** Generates a new array metadata URI. */
  Status generate_uri(const URI& array_uri);

  /**
   * Deserializes the input metadata buffers. Note that the buffers are
   * assummed to be sorted on time. The function will take care of any
   * deleted or overwritten metadata items considering the order.
   */
  Status deserialize(
      const std::vector<std::shared_ptr<ConstBuffer>>& metadata_buffs);

  /** Serializes all key-value metadata items into the input buffer. */
  Status serialize(Buffer* buff) const;

  /** Returns the timestamp range. */
  const std::pair<uint64_t, uint64_t>& timestamp_range() const;

  /**
   * Deletes a metadata item.
   *
   * @param key The key of the metadata to be deleted.
   * @return Status
   */
  Status del(const char* key);

  /**
   * Puts a metadata item as a key-value pair.
   *
   * @param key The metadata key.
   * @param value_type The datatype of the value.
   * @param value_num The number of items in the value part (they could be more
   *     than one).
   * @param value The metadata value.
   * @return Status
   */
  Status put(
      const char* key,
      Datatype value_type,
      uint32_t value_num,
      const void* value);

  /**
   * Gets a metadata item as a key-value pair.
   *
   * @param key The metadata key.
   * @param value_type The datatype of the value.
   * @param value_num The number of items in the value part (they could be more
   * than one).
   * @param value The metadata value. It will be `nullptr` if the key does
   *     not exist
   * @return Status
   */
  Status get(
      const char* key,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value) const;

  /**
   * Gets a metadata item as a key-value pair using an index.
   *
   * @param index The index used to retrieve the metadata.
   * @param key The metadata key.
   * @param key_len The metadata key length.
   * @param value_type The datatype of the value.
   * @param value_num The number of items in the value part (they could be more
   * than one).
   * @param value The metadata value. It will be `nullptr` if the key does
   *     not exist
   * @return Status
   */
  Status get(
      uint64_t index,
      const char** key,
      uint32_t* key_len,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value);

  /** Returns the number of metadata items. */
  uint64_t num() const;

  /**
   * Checks if metadata has specified key.
   *
   * @param key The metadata key.
   * @param value_type The datatype of the value.
   * @param value Set to `1` if the array metadata has a key of the
   *      given name, else `0`.
   * @return Status
   */
  Status has_key(const char* key, Datatype* value_type, bool* has_key);

  /**
   * Sets the URIs of the metadata files that have been loaded
   * to this object.
   */
  Status set_loaded_metadata_uris(
      const std::vector<TimestampedURI>& loaded_metadata_uris);

  /**
   * Returns the URIs of the metadata files that have been loaded
   * to this object.
   */
  const std::vector<URI>& loaded_metadata_uris() const;

  /** Swaps the contents between the object and the input. */
  void swap(Metadata* metadata);

  /**
   * Clears the metadata and assigns the input timestamp to
   * its timestamp range. If `timestamp` is 0, then the metadata
   * timestamp range is set to the current time.
   */
  void reset(uint64_t timestamp);

  /** Returns an iterator to the beginning of the metadata. */
  iterator begin() const;

  /** Returns an iterator to the end of the metadata. */
  iterator end() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** A map from metadata key to metadata value. */
  std::map<std::string, MetadataValue> metadata_map_;

  /**
   * A vector pointing to all the values in `metadata_map_`. It facilitates
   * searching metadata from index. Used only for reading metadata (inapplicable
   * when writing metadata).
   */
  std::vector<std::pair<const std::string*, MetadataValue*>> metadata_index_;

  /** Mutex for thread-safety. */
  mutable std::mutex mtx_;

  /**
   * The timestamp range covered by the metadata that was read or written.
   * This is used to determine the metadata file name.
   */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /**
   * The URIs of the metadata files that have been loaded to this object.
   * This is needed to know which files to delete upon consolidation.
   */
  std::vector<URI> loaded_metadata_uris_;

  /** The URI of the array metadata file. */
  URI uri_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Build the metadata index vector from the metadata map
   * @return Status
   */
  Status build_metadata_index();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_METADATA_H
