/**
 * @file   group_details.h
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
 * This file defines TileDB Group Details
 */

#ifndef TILEDB_GROUP_DETAILS_H
#define TILEDB_GROUP_DETAILS_H

#include <atomic>

#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group_directory.h"
#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

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
   * @param group_member_uri group member uri
   * @param relative is this URI relative
   * @param name optional name for member
   * @return Status
   */
  Status mark_member_for_addition(
      const URI& group_member_uri,
      const bool& relative,
      std::optional<std::string>& name,
      StorageManager* storage_manager);

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
   * Get the vector of members to modify, used in serialization only
   * @return members_to_modify
   */
  const std::vector<shared_ptr<GroupMember>>& members_to_modify() const;

  /**
   * Get whether the group has been modified.
   */
  bool is_modified() const {
    std::lock_guard<std::mutex> lck(mtx_);
    return !members_to_modify_.empty();
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
   * Serializes the object members into a binary buffer.
   *
   * @param format_version The format spec version.
   * @param members The members to serialize.
   * @param serializer The buffer to serialize the data into.
   * @return Status
   */
  static void serialize(
      format_version_t format_version,
      const std::vector<std::shared_ptr<GroupMember>>& members,
      Serializer& serializer);

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param deserializer The buffer to deserialize from.
   * @param version The format spec version.
   * @return Status and Attribute
   */
  static std::optional<shared_ptr<GroupDetails>> deserialize(
      Deserializer& deserializer, const URI& group_uri);

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param deserializers List of buffers for each details file to deserialize
   * from.
   * @param version The format spec version.
   * @return Status and Attribute
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

  /** The mapping of all members of this group. */
  std::unordered_map<std::string, shared_ptr<GroupMember>> members_by_uri_;

  /** Vector for index based lookup. */
  std::vector<shared_ptr<GroupMember>> members_vec_;

  /** Unordered map of members by their name, if the member doesn't have a name,
   * it will not be in the map. */
  std::unordered_map<std::string, shared_ptr<GroupMember>> members_by_name_;

  /** Mapping of members slated for adding. */
  std::vector<shared_ptr<GroupMember>> members_to_modify_;

  /** Mutex for thread safety. */
  mutable std::mutex mtx_;

  /* Format version. */
  const uint32_t version_;
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GROUP_DETAILS_H
