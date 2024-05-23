/**
 * @file   group_member.cc
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
 * This file implements TileDB GroupMember
 */

#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/group/group_member_v1.h"
#include "tiledb/sm/group/group_member_v2.h"

using namespace tiledb::common;

namespace tiledb::sm {

class GroupMemberException : public StatusException {
 public:
  explicit GroupMemberException(const std::string& message)
      : StatusException("GroupMember", message) {
  }
};

GroupMember::GroupMember(
    const URI& uri,
    const ObjectType& type,
    const bool& relative,
    uint32_t version,
    const std::optional<std::string>& name,
    const bool& deleted)
    : uri_(uri)
    , type_(type)
    , name_(name)
    , relative_(relative)
    , version_(version)
    , deleted_(deleted) {
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

bool GroupMember::relative() const {
  return relative_;
}

bool GroupMember::deleted() const {
  return deleted_;
}

format_version_t GroupMember::version() const {
  return version_;
}

void GroupMember::serialize(Serializer&) {
  throw GroupMemberException("Invalid call to GroupMember::serialize");
}

shared_ptr<GroupMember> GroupMember::deserialize(Deserializer& deserializer) {
  uint32_t version = 0;
  version = deserializer.read<uint32_t>();
  if (version == 1) {
    return GroupMemberV1::deserialize(deserializer);
  } else if (version == 2) {
    return GroupMemberV2::deserialize(deserializer);
  }
  throw GroupMemberException(
      "Unsupported group member version " + std::to_string(version));
}
}  // namespace tiledb::sm

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
