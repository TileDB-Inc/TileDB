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

using namespace tiledb::common;

namespace tiledb {
namespace sm {
GroupMember::GroupMember(
    const URI& uri,
    const ObjectType& type,
    const bool& relative,
    format_version_t version,
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

// ===== FORMAT =====
// format_version (uint32_t)
// type (uint8_t)
// relative (uint8_t)
// uri_size (uint64_t)
// uri (string)
// deleted (uint8_t) (v2+)
void GroupMember::serialize(Serializer& serializer) {
  serializer.write<uint32_t>(version_);

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

  // Write deleted
  if (version_ >= 2) {
    serializer.write<uint8_t>(deleted_);
  }
}

shared_ptr<GroupMember> deserialize_group(
    Deserializer& deserializer, uint32_t version) {
  // We skip reading "version" because it is already read by
  // GroupMember::deserialize to determine the version and class to call

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

  bool deleted = false;
  if (version >= 2) {
    deleted = static_cast<bool>(deserializer.read<uint8_t>());
  }

  return tdb::make_shared<GroupMember>(
      HERE(),
      URI(uri_string, !relative),
      type,
      relative,
      version,
      name,
      deleted);
}

shared_ptr<GroupMember> GroupMember::deserialize(Deserializer& deserializer) {
  uint32_t version = 0;
  version = deserializer.read<uint32_t>();
  if (version >= 1 && version <= 2) {
    return deserialize_group(deserializer, version);
  }
  throw StatusException(Status_GroupError(
      "Unsupported group member version " + std::to_string(version)));
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
