/**
 * @file   group_member.h
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
 * This file defines TileDB Group Member
 */

#ifndef TILEDB_GROUP_MEMBER_H
#define TILEDB_GROUP_MEMBER_H

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb::sm {

class GroupMember {
 public:
  GroupMember(
      const URI& uri,
      const ObjectType& type,
      const bool& relative,
      uint32_t version,
      const std::optional<std::string>& name,
      const bool& deleted);

  /** Destructor. */
  virtual ~GroupMember() = default;

  /** Return URI. */
  const URI& uri() const;

  /** Return object type. */
  ObjectType type() const;

  /** Return the Name. */
  const std::optional<std::string> name() const;

  /**
   * Return the discriminating key of the member within a group. No
   * multiple members with the same key may exist in a group.
   *
   * This method returns the member's name, or its URI if it does not exist.
   */
  const std::string key() const {
    return name_ ? *name_ : uri_.to_string();
  }

  /** Return if object is relative. */
  bool relative() const;

  bool deleted() const;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @return Status
   */
  virtual void serialize(Serializer& serializer);

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version The format spec version.
   * @return Status and Attribute
   */
  static shared_ptr<GroupMember> deserialize(Deserializer& deserializer);

  /**
   * Return format version
   * @return format version
   */
  format_version_t version() const;

 protected:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
  /** The group member URI. */
  URI uri_;

  /** The group member type. */
  ObjectType type_;

  /** The group member optional name. */
  std::optional<std::string> name_;

  /** Is the URI relative to the group. */
  bool relative_;

  /* Format version. */
  const uint32_t version_;

  /** Is group member deleted from group. */
  bool deleted_;
};
}  // namespace tiledb::sm

std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::GroupMember& group_member);
#endif  // TILEDB_GROUP_MEMBER_H
