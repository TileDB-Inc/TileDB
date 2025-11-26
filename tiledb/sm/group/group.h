/**
 * @file   group.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * This file defines TileDB Group
 */

#ifndef TILEDB_GROUP_H
#define TILEDB_GROUP_H

#include <atomic>

#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group_details.h"
#include "tiledb/sm/group/group_directory.h"
#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/metadata/metadata.h"

using namespace tiledb::common;

namespace tiledb::sm {

class ContextResources;

class GroupDetailsException : public StatusException {
 public:
  explicit GroupDetailsException(const std::string& message)
      : StatusException("Group Details", message) {
  }
};

class Group {
 public:
  /**
   * Constructs a Group object given a ContextResources reference and a uri.
   *
   * @param resources A ContextResources reference
   * @param group_uri The location of the group
   */
  Group(ContextResources& resources, const URI& group_uri);

  /** Destructor. */
  ~Group() = default;

  /**
   * Creates a TileDB group.
   *
   * @param resources The context resources.
   * @param uri The URI of the group to be created.
   */
  static void create(ContextResources& resources, const URI& uri);

  /** Returns the group directory object. */
  const shared_ptr<GroupDirectory> group_directory() const;

  /**
   * Opens the group for reading.
   *
   * @param query_type The query type. This should always be READ. It
   *    is here only for sanity check.
   *
   * @note Applicable only to reads.
   */
  void open(QueryType query_type);

  /**
   * Opens the group.
   *
   * @param query_type The query type.
   * @param timestamp_start The start timestamp at which to open the group
   * @param timestamp_end The end timestamp at which to open the group
   * @param query_type The query type. This should always be READ. It
   *
   */
  void open(
      QueryType query_type, uint64_t timestamp_start, uint64_t timestamp_end);

  /**
   * Closes a group opened for reads.
   */
  inline void close_for_reads() {
    // Closing a group opened for reads does nothing at present.
  }

  /**
   * Closes a group opened for writes.
   */
  void close_for_writes();

  /** Closes the group and frees all memory. */
  void close();

  /**
   * Clear a group
   */
  void clear();

  /**
   *
   * Handles local and remote deletion of data from a group with the given URI.
   *
   * @pre The group must be opened in MODIFY_EXCLUSIVE mode.
   * @note If recursive == false, data added to the group will be left as-is.
   *
   * @param uri The URI of the group to be deleted.
   * @param recursive True if all data inside the group is to be deleted.
   */
  void delete_group(const URI& uri, bool recursive = false);

  /**
   * Deletes metadata from an group opened in WRITE mode.
   *
   * @param key The key of the metadata item to be deleted.
   */
  void delete_metadata(const char* key);

  /**
   * Puts metadata into an group opened in WRITE mode.
   *
   * @param key The key of the metadata item to be added. UTF-8 encodings
   *     are acceptable.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata.
   * @param value The metadata value in binary form.
   */
  void put_metadata(
      const char* key,
      Datatype value_type,
      uint32_t value_num,
      const void* value);

  /**
   * Gets metadata from an group opened in READ mode. If the key does not
   * exist, then `value` will be `nullptr`.
   *
   * @param key The key of the metadata item to be retrieved. UTF-8 encodings
   *     are acceptable.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata.
   * @param value The metadata value in binary form.
   */
  void get_metadata(
      const char* key,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value);

  /**
   * Gets metadata from an group opened in READ mode using an index.
   *
   * @param index The index used to retrieve the metadata.
   * @param key The metadata key.
   * @param key_len The metadata key length.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata.
   * @param value The metadata value in binary form.
   */
  void get_metadata(
      uint64_t index,
      const char** key,
      uint32_t* key_len,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value);

  /** Returns the number of group metadata items. */
  uint64_t get_metadata_num();

  /** Gets the type of the given metadata or nullopt if it does not exist. */
  std::optional<Datatype> metadata_type(const char* key);

  /** Retrieves the group metadata object. */
  Metadata* metadata();

  /**
   * Retrieves the group metadata object.
   *
   * @note This is potentially an unsafe operation
   * it could have contention with locks from lazy loading of metadata.
   * This should only be used by the serialization class
   * (tiledb/sm/serialization/group.cc). In that class we need to
   * fetch the underlying Metadata object to set the values we are loading from
   * REST. A lock should already by taken before load_metadata is called.
   */
  Metadata* unsafe_metadata();

  /**
   * Set metadata loaded
   * * This should only be used by the serialization class
   * (tiledb/sm/serialization/group.cc).
   * @param bool metadata was loaded
   * @return void
   */
  void set_metadata_loaded(const bool metadata_loaded);

  /**
   * Consolidates the metadata of a group into a single file.
   *
   * @param resources The context resources.
   * @param group_name The name of the group whose metadata will be
   *     consolidated.
   * @param config Configuration parameters for the consolidation
   *     (`nullptr` means default, which will use the config associated with
   *      this instance).
   */
  static void consolidate_metadata(
      ContextResources& resources,
      const char* group_name,
      const Config& config);

  /**
   * Vacuums the consolidated metadata files of a group.
   *
   * @param resources The context resources.
   * @param group_name The name of the group whose metadata will be
   *     vacuumed.
   * @param config Configuration parameters for vacuuming
   *     (`nullptr` means default, which will use the config associated with
   *      this instance).
   */
  static void vacuum_metadata(
      ContextResources& resources,
      const char* group_name,
      const Config& config);

  /** Returns a constant pointer to the encryption key. */
  const EncryptionKey* encryption_key() const;

  /**
   * Return query type
   * @return QueryType
   */
  QueryType query_type() const;

  /**
   * Return config
   * @return Config
   */
  const Config& config() const;

  /**
   * Set the config on the group
   *
   * @param config
   *
   * @pre The Group must be closed
   */
  void set_config(Config config);

  /**
   * Set the config on the group
   *
   * @param config
   *
   * @note This is a potentially unsafe operation. Groups should be closed when
   * setting a config. Until C.41 compliance, this is necessary for
   * serialization.
   */
  inline void unsafe_set_config(Config config) {
    config_.inherit(config);
  }

  /**
   * Add a member to a group, this will be flushed to disk on close
   *
   * @param group_member_uri group member uri
   * @param relative is this URI relative
   * @param name optional name for member
   * @param type optional type for member if known in advance
   */
  void mark_member_for_addition(
      const URI& group_member_uri,
      const bool& relative,
      std::optional<std::string>& name,
      std::optional<ObjectType> type);

  /**
   * Remove a member from a group, this will be flushed to disk on close
   *
   * @param name Name of member to remove. If the member has no name,
   * this parameter should be set to the URI of the member. In that case, only
   * the unnamed member with the given URI will be removed.
   */
  void mark_member_for_removal(const std::string& name);

  /**
   * Get the vector of members to modify, used in serialization only
   * @return members_to_modify
   */
  const std::vector<shared_ptr<GroupMember>>& members_to_modify() const;

  /**
   * Get the unordered map of members
   * @return members
   */
  const std::unordered_map<std::string, shared_ptr<GroupMember>>& members()
      const;

  /**
   * Add a member to a group
   *
   * @param group_member to add
   * @return void
   */
  void add_member(const shared_ptr<GroupMember> group_member);

  /**
   * Delete a member from the group
   *
   * @param group_member
   */
  void delete_member(const shared_ptr<GroupMember> group_member);

  /** Returns the group URI. */
  const URI& group_uri() const;

  /** Get the current URI of the group details file. */
  const URI group_detail_uri() const;

  /**
   * Function to generate a URL of a detail file
   *
   * @return uri
   */
  URI generate_detail_uri() const;

  /**
   * Get count of members
   *
   * @return member count
   */
  uint64_t member_count() const;

  /**
   * Get a member by index
   *
   * @param index of member
   * @return Tuple of URI string, ObjectType, optional GroupMember name
   */
  tuple<std::string, ObjectType, optional<std::string>> member_by_index(
      uint64_t index);

  /**
   * Get a member by name
   *
   * @param name of member
   * @return Tuple of URI string, ObjectType, optional GroupMember name,
   * bool which is true if the URI is relative to the group.
   */
  tuple<std::string, ObjectType, optional<std::string>, bool> member_by_name(
      const std::string& name);

  /** Returns `true` if the group is open. */
  bool is_open() const;

  /** Returns `true` if the group is remote */
  bool is_remote() const;

  /** Retrieves the query type. Errors if the group is not open. */
  QueryType get_query_type() const;

  /**
   * Dump a string representation of a group
   *
   * @param indent_size
   * @param num_indents
   * @return string representation
   */
  std::string dump(
      const uint64_t indent_size,
      const uint64_t num_indents,
      bool recursive = false,
      bool print_self = true) const;

  /**
   * Group Details
   *
   * @return GroupDetails
   */
  shared_ptr<GroupDetails> group_details();

  /**
   * Group Details
   *
   * @return GroupDetails
   */
  const shared_ptr<GroupDetails> group_details() const;

  /**
   * Set the group URI
   *
   * @param uri
   */
  void set_uri(const URI& uri);

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */
  /** Memory tracker for the group. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The group URI. */
  URI group_uri_;

  /** The group directory object for listing URIs. */
  shared_ptr<GroupDirectory> group_dir_;

  /** The group config. */
  Config config_;

  /** True if the group is remote (has `tiledb://` URI scheme). */
  bool remote_;

  /** The group metadata. */
  Metadata metadata_;

  /** True if the group metadata is loaded. */
  bool metadata_loaded_;

  /** `True` if the group has been opened. */
  std::atomic<bool> is_open_;

  /** The query type the group was opened for. */
  QueryType query_type_;

  /**
   * The starting timestamp between to open at.
   * In TileDB, timestamps are in ms elapsed since
   * 1970-01-01 00:00:00 +0000 (UTC).
   */
  uint64_t timestamp_start_;

  /**
   * The ending timestamp between to open at.
   * In TileDB, timestamps are in ms elapsed since
   * 1970-01-01 00:00:00 +0000 (UTC). A value of UINT64_T
   * will be interpreted as the current timestamp.
   */
  uint64_t timestamp_end_;

  /**
   * The private encryption key used to encrypt the group.
   *
   * Note: This is the only place in TileDB where the user's private key
   * bytes should be stored. Whenever a key is needed, a pointer to this
   * memory region should be passed instead of a copy of the bytes.
   */
  shared_ptr<EncryptionKey> encryption_key_;

  /** Group Details. */
  shared_ptr<GroupDetails> group_details_;

  /** Mutex for thread safety. */
  mutable std::mutex mtx_;

  /** The ContextResources class. */
  ContextResources& resources_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /**
   * Load group metadata, handles remote groups vs non-remote groups
   */
  void load_metadata();

  /**
   * Load group metadata from disk
   */
  void load_metadata_from_storage(
      const shared_ptr<GroupDirectory>& group_dir,
      const EncryptionKey& encryption_key);

  /** Opens an group for reads. */
  void group_open_for_reads();

  /**
   * Load group details from disk
   */
  void load_group_details();

  /**
   * Load a group detail from URI
   *
   * @param uri location to load
   */
  void load_group_from_uri(const URI& uri);

  /**
   * Load a group detail from URIs
   *
   * @param uris locations to load
   */
  void load_group_from_all_uris(const std::vector<TimestampedURI>& uris);

  /** Opens an group for writes. */
  void group_open_for_writes();
};
}  // namespace tiledb::sm

#endif  // TILEDB_GROUP_H
