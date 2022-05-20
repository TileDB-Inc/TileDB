/**
 * @file   group_v1.cc
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

#include "tiledb/sm/group/group_v1.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
GroupV1::GroupV1(const URI& group_uri, StorageManager* storage_manager)
    : Group(group_uri, storage_manager, GroupV1::format_version_){};

// ===== FORMAT =====
// format_version (uint32_t)
// group_member_num (uint64_t)
//   group_member #1
//   group_member #2
//   ...
Status GroupV1::serialize(Buffer* buff) {
  RETURN_NOT_OK(
      buff->write(&GroupV1::format_version_, sizeof(GroupV1::format_version_)));
  uint64_t group_member_num = members_.size();
  RETURN_NOT_OK(buff->write(&group_member_num, sizeof(group_member_num)));
  for (auto& it : members_) {
    RETURN_NOT_OK(it.second->serialize(buff));
  }
  return Status::Ok();
}

std::tuple<Status, std::optional<tdb_shared_ptr<Group>>> GroupV1::deserialize(
    ConstBuffer* buff, const URI& group_uri, StorageManager* storage_manager) {
  tdb_shared_ptr<GroupV1> group =
      tdb::make_shared<GroupV1>(HERE(), group_uri, storage_manager);

  uint64_t member_count = 0;
  RETURN_NOT_OK_TUPLE(
      buff->read(&member_count, sizeof(member_count)), std::nullopt);

  for (uint64_t i = 0; i < member_count; i++) {
    auto&& [st, member] = GroupMember::deserialize(buff);
    RETURN_NOT_OK_TUPLE(st, std::nullopt);
    RETURN_NOT_OK_TUPLE(group->add_member(member.value()), std::nullopt);
  }

  return {Status::Ok(), group};
}
}  // namespace sm
}  // namespace tiledb