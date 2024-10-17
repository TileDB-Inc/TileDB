/**
 * @file   group_details.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file defines TileDB Group Details
 */

#ifndef TILEDB_GROUP_DETAILS_H
#define TILEDB_GROUP_DETAILS_H

#include <atomic>

#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group_directory.h"
#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

using namespace tiledb::common;

namespace tiledb::sm {

class GroupDetails {
 public:
  GroupDetails(const URI& group_uri, uint32_t version);

  /** Destructor. */
  virtual ~GroupDetails() = default;

  /**
   * Clear a group
   */
  void clear();

  /**
   * Add a member to a group, this will be flushed to disk on close
   *
   * @param resources the context resources
   * @param group_member_uri group member uri
   * @param relative is this URI relative
   * @param name optional name for member
   * @param type optional type for member if known in advance
   */
  void mark_member_for_addition(
      ContextResources& resources,
      const URI& group_member_uri,
      const bool& relative,
      std::optional<std::string>& name,
      std::optional<ObjectType> type);

  /**
   * Remove a member from a group, this will be flushed to disk on close
   *
   * @param name_or_uri Name or URI of member to remove. If the URI is
   * registered multiple times in the group, the name needs to be specified so
   * that the correct one can be removed. Note that if a URI is registered as
   * both a named and unnamed member, the unnamed member will be removed
   * successfully using the URI.
   */
  void mark_member_for_removal(const std::string& name_or_uri);

  /**
   * Get the vector of members to modify, used in serialization only
   * @return members_to_modify
   */
  const std::vector<shared_ptr<GroupMember>>& members_to_modify() const;

  /**
   * Get whether the group has been modified.
   *
   * This determines whether to write the group details on close.
   */
  bool is_modified() const {
    return is_modified_;
  }

  /**
   * Marks the group as modified.
   *
   * Used only by serialization, to support writing the group details of a
   * deserialized group.
   */
  void set_modified() {
    is_modified_ = true;
  }

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

  /**
   * Store the group details
   *
   * @param resources the context resources
   * @param group_detail_folder_uri group details folder
   * @param group_detail_uri uri for detail file to write
   * @param encryption_key encryption key for at-rest encryption
   */
  void store(
      ContextResources& resources,
      const URI& group_detail_folder_uri,
      const URI& group_detail_uri,
      const EncryptionKey& encryption_key);

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param members The members to serialize. Should be retrieved from
   * members_to_serialize().
   * @param serializer The buffer to serialize the data into.
   */
  virtual void serialize(
      const std::vector<std::shared_ptr<GroupMember>>& members,
      Serializer& serializer) const = 0;

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param deserializer The buffer to deserialize from.
   * @param version The format spec version.
   * @return Group detail.
   */
  static std::optional<shared_ptr<GroupDetails>> deserialize(
      Deserializer& deserializer, const URI& group_uri);

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param deserializers List of buffers for each details file to deserialize
   * from.
   * @param version The format spec version.
   * @return Group detail.
   */
  static std::optional<shared_ptr<GroupDetails>> deserialize(
      const std::vector<shared_ptr<Deserializer>>& deserializer,
      const URI& group_uri);

  /** Returns the group URI. */
  const URI& group_uri() const;

  /**
   * Get count of members
   *
   * @return member count
   */
  uint64_t member_count() const;

  /**
   * Get a member by index.
   *
   * @param index of member
   * @return Tuple of URI string, ObjectType, optional GroupMember name
   */
  tuple<std::string, ObjectType, optional<std::string>> member_by_index(
      uint64_t index);

  /**
   * Get a member by name.
   *
   * @param name of member
   * @return Tuple of URI string, ObjectType, optional GroupMember name,
   * bool which is true if the URI is relative to the group.
   */
  tuple<std::string, ObjectType, optional<std::string>, bool> member_by_name(
      const std::string& name);

  /**
   * Return format version
   * @return format version
   */
  format_version_t version() const;

  /**
   * Get the members to write to storage, after accounting for duplicate members
   * and member removals.
   */
  virtual std::vector<std::shared_ptr<GroupMember>> members_to_serialize()
      const = 0;

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** The group URI. */
  URI group_uri_;

  /** Whether the group has been modified. */
  bool is_modified_;

  /**
   * The mapping of all members of this group. This is the canonical store of
   * the group's members. The key is the member's key().
   */
  std::unordered_map<std::string, shared_ptr<GroupMember>> members_;

  /** Vector for index based lookup. */
  optional<std::vector<shared_ptr<GroupMember>>> members_vec_;

  /**
   * Map of members for uri based lookup. Note that if a URI is found more than
   * once, it will not be found here, but in `duplicated_uris_`.
   */
  optional<std::unordered_map<std::string, shared_ptr<GroupMember>>>
      members_by_uri_;

  /** Set of duplicated URIs, with count. */
  optional<std::unordered_map<std::string, uint64_t>> duplicated_uris_;

  /** Mapping of members slated for addition/removal. */
  std::vector<shared_ptr<GroupMember>> members_to_modify_;

  /** Set of member keys that have been marked for addition. */
  std::unordered_set<std::string> member_keys_to_add_;

  /** Set of member keys that have been marked for removal. */
  std::unordered_set<std::string> member_keys_to_delete_;

  /** Mutex for thread safety. */
  mutable std::mutex mtx_;

  /** Format version. */
  const uint32_t version_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /** Ensure we have built lookup table for members by index. */
  void ensure_lookup_by_index();

  /** Ensure we have built lookup table for members by uri. */
  void ensure_lookup_by_uri();

  /** Invalidate the built lookup tables. */
  void invalidate_lookups();
};

}  // namespace tiledb::sm

#endif  // TILEDB_GROUP_DETAILS_H
