/**
 * @file   group_details.cc
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
 * This file implements TileDB Group Details
 */

#include "tiledb/sm/group/group_details.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/group/group_details_v1.h"
#include "tiledb/sm/group/group_details_v2.h"
#include "tiledb/sm/group/group_member_v1.h"
#include "tiledb/sm/group/group_member_v2.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

GroupDetails::GroupDetails(const URI& group_uri, uint32_t version)
    : group_uri_(group_uri)
    , version_(version)
    , changes_applied_(false)
    , is_populated_(false) {
}

Status GroupDetails::clear() {
  members_.clear();
  members_by_name_.clear();
  members_vec_.clear();
  members_to_modify_.clear();
  is_populated_ = false;

  return Status::Ok();
}

void GroupDetails::add_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  assert(!is_populated_);
  auto key = group_member->name_or_uri();
  members_[key] = group_member;
}

void GroupDetails::delete_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  assert(!is_populated_);
  members_.erase(group_member->name_or_uri());
}

void GroupDetails::finish_populating() {
  std::lock_guard<std::mutex> lck(mtx_);
  assert(!is_populated_);
  is_populated_ = true;
}

Status GroupDetails::mark_member_for_addition(
    const URI& group_member_uri,
    const bool& relative,
    std::optional<std::string>& name,
    StorageManager* storage_manager) {
  std::lock_guard<std::mutex> lck(mtx_);
  // TODO: Safety checks for not double adding, making sure its remove + add,
  // etc

  URI absolute_group_member_uri = group_member_uri;
  if (relative) {
    absolute_group_member_uri =
        group_uri_.join_path(group_member_uri.to_string());
  }
  ObjectType type = ObjectType::INVALID;
  RETURN_NOT_OK(storage_manager->object_type(absolute_group_member_uri, &type));
  if (type == ObjectType::INVALID) {
    return Status_GroupError(
        "Cannot add group member " + absolute_group_member_uri.to_string() +
        ", type is INVALID. The member likely does not exist.");
  }

  auto group_member = tdb::make_shared<GroupMemberV2>(
      HERE(), group_member_uri, type, relative, name, false);

  members_to_modify_.emplace_back(group_member);

  return Status::Ok();
}

Status GroupDetails::mark_member_for_removal(const std::string& name_or_uri) {
  std::lock_guard<std::mutex> lck(mtx_);

  auto it = members_.find(name_or_uri);
  if (it == members_.end()) {
    // try URI to see if we need to convert the local file to file://
    it = members_.find(URI(name_or_uri).to_string());
  }
  if (it != members_.end()) {
    auto member_to_delete = make_shared<GroupMemberV2>(
        it->second->uri(),
        it->second->type(),
        it->second->relative(),
        it->second->name(),
        true);
    members_to_modify_.emplace_back(member_to_delete);
    return Status::Ok();
  }
  return Status_GroupError(
      "Cannot remove group member " + name_or_uri +
      ", member does not exist in group.");
}

const std::vector<shared_ptr<GroupMember>>& GroupDetails::members_to_modify()
    const {
  std::lock_guard<std::mutex> lck(mtx_);
  return members_to_modify_;
}

const std::unordered_map<std::string, shared_ptr<GroupMember>>&
GroupDetails::members() const {
  std::lock_guard<std::mutex> lck(mtx_);
  return members_;
}

void GroupDetails::serialize(Serializer&) {
  throw StatusException(Status_GroupError("Invalid call to Group::serialize"));
}

std::optional<shared_ptr<GroupDetails>> GroupDetails::deserialize(
    Deserializer& deserializer, const URI& group_uri) {
  uint32_t version = 0;
  version = deserializer.read<uint32_t>();
  if (version == 1) {
    return GroupDetailsV1::deserialize(deserializer, group_uri);
  } else if (version == 2) {
    return GroupDetailsV2::deserialize(deserializer, group_uri);
  }

  throw StatusException(Status_GroupError(
      "Unsupported group version " + std::to_string(version)));
}

std::optional<shared_ptr<GroupDetails>> GroupDetails::deserialize(
    const std::vector<shared_ptr<Deserializer>>& deserializer,
    const URI& group_uri) {
  // Currently this is only supported for v2 on-disk format
  return GroupDetailsV2::deserialize(deserializer, group_uri);
}

const URI& GroupDetails::group_uri() const {
  return group_uri_;
}

bool GroupDetails::changes_applied() const {
  return changes_applied_;
}

uint64_t GroupDetails::member_count() const {
  std::lock_guard<std::mutex> lck(mtx_);

  return members_.size();
}

tuple<std::string, ObjectType, optional<std::string>>
GroupDetails::member_by_index(uint64_t index) {
  std::lock_guard<std::mutex> lck(mtx_);
  assert(is_populated_);

  if (members_vec_.size() != members_.size()) {
    members_vec_.clear();
    members_vec_.reserve(members_.size());
    for (auto& [key, member] : members_) {
      members_vec_.emplace_back(member);
    }
  }

  if (index >= members_vec_.size()) {
    throw Status_GroupError(
        "index " + std::to_string(index) + " is larger than member count " +
        std::to_string(members_vec_.size()));
  }

  auto member = members_vec_[index];

  std::string uri = member->uri().to_string();
  if (member->relative()) {
    uri = group_uri_.join_path(member->uri().to_string()).to_string();
  }

  return {uri, member->type(), member->name()};
}

tuple<std::string, ObjectType, optional<std::string>, bool>
GroupDetails::member_by_name(const std::string& name) {
  std::lock_guard<std::mutex> lck(mtx_);
  assert(is_populated_);

  if (members_by_name_.size() != members_.size()) {
    members_by_name_.clear();
    members_by_name_.reserve(members_.size());
    for (auto& [key, member] : members_) {
      if (member->name().has_value()) {
        bool added =
            members_by_name_.insert_or_assign(member->name().value(), member)
                .second;
        // add_member makes sure that the name is unique, so the call to
        // insert_or_assign should always add a member.
        assert(added);
        std::ignore = added;
      }
    }
  }

  auto it = members_by_name_.find(name);
  if (it == members_by_name_.end()) {
    throw Status_GroupError(name + " does not exist in group");
  }

  auto member = it->second;
  std::string uri = member->uri().to_string();
  if (member->relative()) {
    uri = group_uri_.join_path(member->uri().to_string()).to_string();
  }

  return {uri, member->type(), member->name(), member->relative()};
}

format_version_t GroupDetails::version() const {
  return version_;
}

}  // namespace sm
}  // namespace tiledb
