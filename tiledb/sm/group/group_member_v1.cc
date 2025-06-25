/**
 * @file   group_member_v1.cc
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
 * This file implements TileDB GroupMemberV1
 */

#include "tiledb/sm/group/group_member_v1.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

GroupMemberV1::GroupMemberV1(
    const URI& uri,
    const ObjectType& type,
    const bool& relative,
    const std::optional<std::string>& name)
    : GroupMember(
          uri, type, relative, GroupMemberV1::format_version_, name, false) {};

// ===== FORMAT =====
// format_version (uint32_t)
// type (uint8_t)
// relative (uint8_t)
// uri_size (uint64_t)
// uri (string)
void GroupMemberV1::serialize(Serializer& serializer) {
  serializer.write<uint32_t>(GroupMemberV1::format_version_);

  // Write type
  uint8_t type = static_cast<uint8_t>(type_);
  serializer.write<uint8_t>(type);

  // Write relative
  serializer.write<uint8_t>(relative_);

  // Write uri
  uint64_t uri_size = uri_.to_string().size();
  serializer.write<uint64_t>(uri_size);
  serializer.write(uri_.c_str(), uri_size);

  // Write name
  auto name_set = static_cast<uint8_t>(name_.has_value());
  serializer.write<uint8_t>(name_set);
  if (name_.has_value()) {
    uint64_t name_size = name_->size();
    serializer.write<uint64_t>(name_size);
    serializer.write(name_->data(), name_size);
  }
}

shared_ptr<GroupMember> GroupMemberV1::deserialize(Deserializer& deserializer) {
  uint8_t type_placeholder = deserializer.read<uint8_t>();
  ObjectType type = static_cast<ObjectType>(type_placeholder);

  uint8_t relative_int = deserializer.read<uint8_t>();
  auto relative = static_cast<bool>(relative_int);

  uint64_t uri_size = deserializer.read<uint64_t>();

  std::string uri_string;
  uri_string.resize(uri_size);
  deserializer.read(&uri_string[0], uri_size);

  uint8_t name_set_int;
  std::optional<std::string> name;
  name_set_int = deserializer.read<uint8_t>();
  auto name_set = static_cast<bool>(name_set_int);
  if (name_set) {
    uint64_t name_size = 0;
    name_size = deserializer.read<uint64_t>();

    std::string name_string;
    name_string.resize(name_size);
    deserializer.read(&name_string[0], name_size);
    name = name_string;
  }

  shared_ptr<GroupMemberV1> group_member = tdb::make_shared<GroupMemberV1>(
      HERE(), URI(uri_string, !relative), type, relative, name);
  return group_member;
}

}  // namespace sm
}  // namespace tiledb
