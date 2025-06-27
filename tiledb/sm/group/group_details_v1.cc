/**
 * @file   group_details_v1.cc
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
 * This file implements TileDB Group Details V1
 */

#include "tiledb/sm/group/group_details_v1.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
GroupDetailsV1::GroupDetailsV1(const URI& group_uri)
    : GroupDetails(group_uri, GroupDetailsV1::format_version_) {};

// ===== FORMAT =====
// format_version (format_version_t)
// group_member_num (uint64_t)
//   group_member #1
//   group_member #2
//   ...
void GroupDetailsV1::serialize(
    const std::vector<std::shared_ptr<GroupMember>>& members,
    Serializer& serializer) const {
  serializer.write<format_version_t>(GroupDetailsV1::format_version_);
  serializer.write<uint64_t>(members.size());
  for (auto& it : members) {
    it->serialize(serializer);
  }
}

shared_ptr<GroupDetails> GroupDetailsV1::deserialize(
    Deserializer& deserializer, const URI& group_uri) {
  shared_ptr<GroupDetailsV1> group =
      tdb::make_shared<GroupDetailsV1>(HERE(), group_uri);

  uint64_t member_count = deserializer.read<uint64_t>();
  for (uint64_t i = 0; i < member_count; i++) {
    auto&& member = GroupMember::deserialize(deserializer);
    group->add_member(member);
  }

  return group;
}

std::vector<std::shared_ptr<GroupMember>> GroupDetailsV1::members_to_serialize()
    const {
  std::lock_guard<std::mutex> lck(mtx_);
  decltype(members_) members = members_;

  // Remove members first
  for (const auto& member : members_to_modify_) {
    auto key = member->key();
    if (member->deleted()) {
      members.erase(key);

      // Check to remove relative URIs
      if (key.find(group_uri_.add_trailing_slash().to_string()) !=
          std::string::npos) {
        // Get the substring relative path
        auto relative_uri = key.substr(
            group_uri_.add_trailing_slash().to_string().size(), key.size());
        members.erase(relative_uri);
      }
    } else {
      members.emplace(member->uri().to_string(), member);
    }
  }

  std::vector<std::shared_ptr<GroupMember>> result;
  result.reserve(members.size());
  for (auto& it : members) {
    result.emplace_back(it.second);
  }
  return result;
}

}  // namespace sm
}  // namespace tiledb
