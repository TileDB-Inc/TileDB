/**
 * @file   group_details_v2.cc
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
 * This file implements TileDB Group Details V2
 */

#include "tiledb/sm/group/group_details_v2.h"
#include "group.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
GroupDetailsV2::GroupDetailsV2(const URI& group_uri)
    : GroupDetails(group_uri, GroupDetailsV2::format_version_){};

// ===== FORMAT =====
// format_version (format_version_t)
// group_member_num (uint64_t)
//   group_member #1
//   group_member #2
//   ...
void GroupDetailsV2::serialize(
    const std::vector<std::shared_ptr<GroupMember>>& members,
    Serializer& serializer) const {
  serializer.write<format_version_t>(GroupDetailsV2::format_version_);
  serializer.write<uint64_t>(members.size());
  for (auto& it : members) {
    it->serialize(serializer);
  }
}

shared_ptr<GroupDetails> GroupDetailsV2::deserialize(
    Deserializer& deserializer, const URI& group_uri) {
  shared_ptr<GroupDetailsV2> group =
      tdb::make_shared<GroupDetailsV2>(HERE(), group_uri);

  uint64_t member_count = 0;
  member_count = deserializer.read<uint64_t>();
  for (uint64_t i = 0; i < member_count; i++) {
    auto member = GroupMember::deserialize(deserializer);
    if (member->deleted()) {
      group->delete_member(member);
    } else {
      group->add_member(member);
    }
  }

  return group;
}

shared_ptr<GroupDetails> GroupDetailsV2::deserialize(
    const std::vector<shared_ptr<Deserializer>>& deserializers,
    const URI& group_uri) {
  shared_ptr<GroupDetailsV2> group =
      tdb::make_shared<GroupDetailsV2>(HERE(), group_uri);

  for (auto& deserializer : deserializers) {
    // Read and assert version
    format_version_t details_version = deserializer->read<format_version_t>();
    if (details_version != 2) {
      throw GroupDetailsException(
          "Invalid version " + std::to_string(details_version) +
          "; expected 2.");
    }

    // Read members
    uint64_t member_count = deserializer->read<uint64_t>();
    for (uint64_t i = 0; i < member_count; i++) {
      auto member = GroupMember::deserialize(*deserializer);
      if (member->deleted()) {
        group->delete_member(member);
      } else {
        group->add_member(member);
      }
    }
  }

  return group;
}

std::vector<std::shared_ptr<GroupMember>> GroupDetailsV2::members_to_serialize()
    const {
  std::lock_guard<std::mutex> lck(mtx_);

  decltype(members_) members = members_;

  // Add each member, overriding if the user adds/removes it multiple times.
  for (auto it : members_to_modify_) {
    members[it->key()] = it;
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
