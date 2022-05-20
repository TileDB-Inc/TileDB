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
    : GroupMember(uri, type, relative, GroupMemberV1::format_version_, name){};

// ===== FORMAT =====
// format_version (uint32_t)
// type (uint8_t)
// relative (uint8_t)
// uri_size (uint32_t)
// uri (string)
Status GroupMemberV1::serialize(Buffer* buff) {
  RETURN_NOT_OK(buff->write(
      &GroupMemberV1::format_version_, sizeof(GroupMemberV1::format_version_)));

  // Write type
  uint8_t type = static_cast<uint8_t>(type_);
  RETURN_NOT_OK(buff->write(&type, sizeof(type)));

  // Write relative
  RETURN_NOT_OK(buff->write(&relative_, sizeof(relative_)));

  // Write uri
  uint64_t uri_size = uri_.to_string().size();
  RETURN_NOT_OK(buff->write(&uri_size, sizeof(uri_size)));
  RETURN_NOT_OK(buff->write(uri_.c_str(), uri_size));

  // Write name
  bool name_set = name_.has_value();
  RETURN_NOT_OK(buff->write(&name_set, sizeof(bool)));
  if (name_set) {
    uint64_t name_size = name_->size();
    RETURN_NOT_OK(buff->write(&name_size, sizeof(name_size)));
    RETURN_NOT_OK(buff->write(name_->data(), name_size));
  }

  return Status::Ok();
}

std::tuple<Status, std::optional<tdb_shared_ptr<GroupMember>>>
GroupMemberV1::deserialize(ConstBuffer* buff) {
  uint8_t type_placeholder;
  RETURN_NOT_OK_TUPLE(
      buff->read(&type_placeholder, sizeof(type_placeholder)), std::nullopt);
  ObjectType type = static_cast<ObjectType>(type_placeholder);

  bool relative;
  RETURN_NOT_OK_TUPLE(buff->read(&relative, sizeof(relative)), std::nullopt);

  uint64_t uri_size = 0;
  RETURN_NOT_OK_TUPLE(buff->read(&uri_size, sizeof(uri_size)), std::nullopt);

  std::string uri_string;
  uri_string.resize(uri_size);
  RETURN_NOT_OK_TUPLE(buff->read(&uri_string[0], uri_size), std::nullopt);

  bool name_set;
  std::optional<std::string> name;
  RETURN_NOT_OK_TUPLE(buff->read(&name_set, sizeof(name_set)), std::nullopt);
  if (name_set) {
    uint64_t name_size = 0;
    RETURN_NOT_OK_TUPLE(
        buff->read(&name_size, sizeof(name_size)), std::nullopt);

    std::string name_string;
    name_string.resize(name_size);
    RETURN_NOT_OK_TUPLE(buff->read(&name_string[0], name_size), std::nullopt);
    name = name_string;
  }

  tdb_shared_ptr<GroupMemberV1> group_member = tdb::make_shared<GroupMemberV1>(
      HERE(), URI(uri_string, !relative), type, relative, name);
  return {Status::Ok(), group_member};
}

}  // namespace sm
}  // namespace tiledb