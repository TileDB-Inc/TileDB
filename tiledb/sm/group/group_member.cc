/**
 * @file   group_member.cc
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
 * This file implements TileDB GroupMember
 */

#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/group/group_member_v1.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
GroupMember::GroupMember(
    const URI& uri,
    const ObjectType& type,
    const bool& relative,
    uint32_t version,
    const std::optional<std::string>& name)
    : uri_(uri)
    , type_(type)
    , name_(name)
    , relative_(relative)
    , version_(version) {
}

const URI& GroupMember::uri() const {
  return uri_;
}

ObjectType GroupMember::type() const {
  return type_;
}

const std::optional<std::string> GroupMember::name() const {
  return name_;
}

const bool& GroupMember::relative() const {
  return relative_;
}

Status GroupMember::serialize(Buffer*) {
  return Status_GroupMemberError("Invalid call to GroupMember::serialize");
}

std::tuple<Status, std::optional<tdb_shared_ptr<GroupMember>>>
GroupMember::deserialize(ConstBuffer* buff) {
  uint32_t version = 0;
  RETURN_NOT_OK_TUPLE(buff->read(&version, sizeof(version)), std::nullopt);
  if (version == 1) {
    return GroupMemberV1::deserialize(buff);
  }

  return {Status_GroupError(
              "Unsupported group member version " + std::to_string(version)),
          std::nullopt};
}
}  // namespace sm
}  // namespace tiledb

std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::GroupMember& group_member) {
  if (group_member.name().has_value()) {
    os << group_member.name().value() << " ";
  } else {
    os << group_member.uri().last_path_part() << " ";
  }
  os << object_type_str(group_member.type());
  return os;
}
