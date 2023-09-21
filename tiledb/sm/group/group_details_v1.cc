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
    : GroupDetails(group_uri, GroupDetailsV1::format_version_){};

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
  decltype(members_by_uri_) members_by_uri = members_by_uri_;

  // Remove members first
  for (const auto& member : members_to_modify_) {
    auto& uri = member->uri();
    if (member->deleted()) {
      members_by_uri.erase(uri.to_string());

      // Check to remove relative URIs
      auto uri_str = uri.to_string();
      if (uri_str.find(group_uri_.add_trailing_slash().to_string()) !=
          std::string::npos) {
        // Get the substring relative path
        auto relative_uri = uri_str.substr(
            group_uri_.add_trailing_slash().to_string().size(), uri_str.size());
        members_by_uri.erase(relative_uri);
      }
    } else {
      members_by_uri.emplace(member->uri().to_string(), member);
    }
  }

  std::vector<std::shared_ptr<GroupMember>> result;
  result.reserve(members_by_uri.size());
  for (auto& it : members_by_uri) {
    result.emplace_back(it.second);
  }
  return result;
}

}  // namespace sm
}  // namespace tiledb
