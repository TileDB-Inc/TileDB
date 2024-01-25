/**
 * @file   group_member_v1.h
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
 * This file defines TileDB Group Member
 */

#ifndef TILEDB_GROUP_MEMBER_V1_H
#define TILEDB_GROUP_MEMBER_V1_H

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class GroupMemberV1 : public GroupMember {
 public:
  GroupMemberV1(
      const URI& uri,
      const ObjectType& type,
      const bool& relative,
      const std::optional<std::string>& name);

  ~GroupMemberV1() override = default;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @return Status
   */
  void serialize(Serializer& serializer) override;

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version The format spec version.
   * @return Status and Attribute
   */
  static shared_ptr<GroupMember> deserialize(Deserializer& deserializer);

 private:
  /* Format version for class. */
  inline static const format_version_t format_version_ = 1;
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GROUP_MEMBER_V1_H
