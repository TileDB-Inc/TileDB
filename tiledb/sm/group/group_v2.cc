/**
 * @file   group_v2.cc
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
 * This file implements TileDB Group
 */

#include "tiledb/sm/group/group_v2.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
GroupV2::GroupV2(const URI& group_uri, StorageManager* storage_manager)
    : Group(group_uri, storage_manager, GroupV2::format_version_){};

// ===== FORMAT =====
// format_version (format_version_t)
// group_member_num (uint64_t)
//   group_member #1
//   group_member #2
//   ...
void GroupV2::serialize(Serializer& serializer) {
  serializer.write<format_version_t>(GroupV2::format_version_);
  uint64_t group_member_num = members_.size();
  serializer.write<uint64_t>(group_member_num);
  for (auto& it : members_) {
    it.second->serialize(serializer);
  }
}

tdb_shared_ptr<Group> GroupV2::deserialize(
    Deserializer& deserializer,
    const URI& group_uri,
    StorageManager* storage_manager) {
  tdb_shared_ptr<GroupV2> group =
      tdb::make_shared<GroupV2>(HERE(), group_uri, storage_manager);

  uint64_t member_count = 0;
  member_count = deserializer.read<uint64_t>();
  for (uint64_t i = 0; i < member_count; i++) {
    auto&& member = GroupMember::deserialize(deserializer);
    if (member->deleted()) {
      group->delete_member(member);
    } else {
      group->add_member(member);
    }
  }

  return group;
}

tdb_shared_ptr<Group> GroupV2::deserialize(
    std::vector<shared_ptr<Deserializer>>& deserializers,
    const URI& group_uri,
    StorageManager* storage_manager) {
  tdb_shared_ptr<GroupV2> group =
      tdb::make_shared<GroupV2>(HERE(), group_uri, storage_manager);

  for (auto& deserializer : deserializers) {
    uint64_t member_count = 0;
    member_count = deserializer->read<uint64_t>();
    for (uint64_t i = 0; i < member_count; i++) {
      auto&& member = GroupMember::deserialize(*deserializer);
      if (member->deleted()) {
        group->delete_member(member);
      } else {
        group->add_member(member);
      }
    }
  }
}

Status GroupV2::apply_pending_changes() {
  std::lock_guard<std::mutex> lck(mtx_);

  members_.clear();
  members_vec_.clear();
  members_by_name_.clear();
  members_vec_.reserve(members_to_modify_.size());

  for (auto& it : members_to_modify_) {
    members_.emplace(it->uri().to_string(), it);
    members_vec_.emplace_back(it);
    if (it->name().has_value()) {
      members_by_name_.emplace(it->name().value(), it);
    }
  }
  changes_applied_ = !members_to_modify_.empty();
  members_to_modify_.clear();

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
