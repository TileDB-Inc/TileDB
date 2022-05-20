/**
 * @file   group.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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

#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group_directory.h"
#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class StorageManager;
class Group {
 public:
  Group(
      const URI& group_uri, StorageManager* storage_manager, uint32_t version);

  /** Destructor. */
  virtual ~Group() = default;

  /** Returns the group directory object. */
  const shared_ptr<GroupDirectory> group_directory() const;

  /**
   * Opens the group for reading.
   *
   * @param query_type The query type. This should always be READ. It
   *    is here only for sanity check.
   * @return Status
   *
   * @note Applicable only to reads.
   */
  Status open(QueryType query_type);

  /** Closes the group and frees all memory. */
  Status close();

  /**
   * Clear a group
   *
   * @return
   */
  Status clear();

  /**
   * Deletes metadata from an group opened in WRITE mode.
   *
   * @param key The key of the metadata item to be deleted.
   * @return Status
   */
  Status delete_metadata(const char* key);

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
   * @return Status
   */
  Status put_metadata(
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
   * @return Status
   */
  Status get_metadata(
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
   * @return Status
   */
  Status get_metadata(
      uint64_t index,
      const char** key,
      uint32_t* key_len,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value);

  /** Returns the number of group metadata items. */
  Status get_metadata_num(uint64_t* num);

  /** Sets has_key == 1 and corresponding value_type if the group has key. */
  Status has_metadata_key(const char* key, Datatype* value_type, bool* has_key);

  /** Retrieves the group metadata object. */
  Status metadata(Metadata** metadata);

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
  const Metadata* metadata() const;

  /**
   * Set metadata loaded
   * * This should only be used by the serialization class
   * (tiledb/sm/serialization/group.cc).
   * @param bool metadata was loaded
   * @return void
   */
  void set_metadata_loaded(const bool metadata_loaded);

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
  const Config* config() const;

  /**
   * Set the config on the group
   *
   * @param config
   * @return status
   */
  Status set_config(Config config);

  /**
   * Add a member to a group, this will be flushed to disk on close
   *
   * @param group_member_uri group member uri
   * @param relative is this URI relative
   * @param name optional name for member
   * @return Status
   */
  Status mark_member_for_addition(
      const URI& group_member_uri,
      const bool& relative,
      std::optional<std::string>& name);

  /**
   * Remove a member from a group, this will be flushed to disk on close
   *
   * @param uri of member to remove
   * @return Status
   */
  Status mark_member_for_removal(const URI& uri);

  /**
   * Remove a member from a group, this will be flushed to disk on close
   *
   * @param uri of member to remove
   * @return Status
   */
  Status mark_member_for_removal(const std::string& uri);

  /**
   * Get the unordered map of members to remove, used in serialization only
   * @return members_to_add
   */
  const std::unordered_set<std::string>& members_to_remove() const;

  /**
   * Get the unordered map of members to add, used in serialization only
   * @return members_to_add
   */
  const std::unordered_map<std::string, tdb_shared_ptr<GroupMember>>&
  members_to_add() const;

  /**
   * Get the unordered map of members
   * @return members
   */
  const std::unordered_map<std::string, tdb_shared_ptr<GroupMember>>& members()
      const;

  /**
   * Add a member to a group, this will be flushed to disk on close
   *
   * @param group_member to add
   * @return Status
   */
  Status add_member(const tdb_shared_ptr<GroupMember>& group_member);

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @param version The format spec version.
   * @return Status
   */
  virtual Status serialize(Buffer* buff);

  /**
   * Applies and pending changes and then calls serialize
   *
   * @param buff The buffer to serialize the data into.
   * @param version The format spec version.
   * @return Status
   */
  Status apply_and_serialize(Buffer* buff);

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version The format spec version.
   * @return Status and Attribute
   */
  static std::tuple<Status, std::optional<tdb_shared_ptr<Group>>> deserialize(
      ConstBuffer* buff, const URI& group_uri, StorageManager* storage_manager);

  /** Returns the group URI. */
  const URI& group_uri() const;

  /** Get the current URI of the group details file. */
  const URI group_detail_uri() const;

  /**
   * Function to generate a URL of a detail file
   *
   * @return tuple of status and uri
   */
  tuple<Status, optional<URI>> generate_detail_uri() const;

  /**
   * Have changes been applied to a group in write mode
   * @return changes_applied_
   */
  bool changes_applied() const;

  /**
   * Set changes applied, only used in serialization
   * @param changes_applied should changes be considered to be applied? If so
   * then this will enable writes from a deserialized group
   *
   */
  void set_changes_applied(bool changes_applied);

  /**
   * Get count of members
   *
   * @return tuple of Status and optional member count
   */
  tuple<Status, optional<uint64_t>> member_count() const;

  /**
   * Get a member by index
   *
   * @param index of member
   * @return Tuple of Status, URI string, ObjectType, optional name
   */
  tuple<
      Status,
      optional<std::string>,
      optional<ObjectType>,
      optional<std::string>>
  member_by_index(uint64_t index);

  /**
   * Get a member by name
   *
   * @param name of member
   * @return Tuple of Status, URI string, ObjectType, optional name
   */
  tuple<
      Status,
      optional<std::string>,
      optional<ObjectType>,
      optional<std::string>>
  member_by_name(const std::string& name);

  /** Returns `true` if the group is open. */
  bool is_open() const;

  /** Returns `true` if the group is remote */
  bool is_remote() const;

  /** Retrieves the query type. Errors if the group is not open. */
  Status get_query_type(QueryType* query_type) const;

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

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */
  /** The group URI. */
  URI group_uri_;

  /** The group directory object for listing URIs. */
  std::shared_ptr<GroupDirectory> group_dir_;

  /** TileDB storage manager. */
  StorageManager* storage_manager_;

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
  tdb_shared_ptr<EncryptionKey> encryption_key_;

  /** The mapping of all members of this group. */
  std::unordered_map<std::string, tdb_shared_ptr<GroupMember>> members_;

  /** Vector for index based lookup. */
  std::vector<tdb_shared_ptr<GroupMember>> members_vec_;

  /** Unordered map of members by their name, if the member doesn't have a name,
   * it will not be in the map. */
  std::unordered_map<std::string, tdb_shared_ptr<GroupMember>> members_by_name_;

  /** Mapping of members slated for removal. */
  std::unordered_set<std::string> members_to_remove_;

  /** Mapping of members slated for adding. */
  std::unordered_map<std::string, tdb_shared_ptr<GroupMember>> members_to_add_;

  /** Mutex for thread safety. */
  mutable std::mutex mtx_;

  /* Format version. */
  const uint32_t version_;

  /* Were changes applied and is a write required */
  bool changes_applied_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /**
   * Load group metadata, handles remote groups vs non-remote groups
   * @return  Status
   */
  Status load_metadata();

  /**
   * Apply any pending member additions or removals
   *
   * mutates members_ and clears members_to_add_ and members_to_remove_
   *
   * @return Status
   */
  Status apply_pending_changes();

  /**
   * Generate new name in the form of timestmap_timestamp_uuid
   *
   * @return tuple of status and optional string
   */
  tuple<Status, optional<std::string>> generate_name() const;
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GROUP_H
