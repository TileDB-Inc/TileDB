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
#include "tiledb/sm/group/group_internal.h"
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
    , changes_applied_(false) {
}

void GroupDetails::clear() {
  members_.clear();
  members_by_name_.clear();
  members_vec_.clear();
  members_to_modify_.clear();
}

void GroupDetails::add_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  const std::string& uri = group_member->uri().to_string();
  members_.emplace(uri, group_member);
  members_vec_.emplace_back(group_member);
  if (group_member->name().has_value()) {
    members_by_name_.emplace(group_member->name().value(), group_member);
  }
}

void GroupDetails::delete_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  const std::string& uri = group_member->uri().to_string();
  auto it = members_.find(uri);
  if (it != members_.end()) {
    for (size_t i = 0; i < members_vec_.size(); i++) {
      if (members_vec_[i] == it->second) {
        members_vec_.erase(members_vec_.begin() + i);
        break;
      }
    }
    auto name = it->second->name();
    members_.erase(it);
    if (group_member->name().has_value()) {
      members_by_name_.erase(group_member->name().value());
    } else if (name.has_value()) {
      members_by_name_.erase(name.value());
    }
  }
}

void GroupDetails::mark_member_for_addition(
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
  throw_if_not_ok(
      storage_manager->object_type(absolute_group_member_uri, &type));
  if (type == ObjectType::INVALID) {
    throw GroupException(
        "Cannot add group member " + absolute_group_member_uri.to_string() +
        ", type is INVALID. The member likely does not exist.");
  }

  auto group_member = tdb::make_shared<GroupMemberV2>(
      HERE(), group_member_uri, type, relative, name, false);

  members_to_modify_.emplace_back(group_member);
}

void GroupDetails::mark_member_for_removal(const URI& uri) {
  mark_member_for_removal(uri.to_string());
}

void GroupDetails::mark_member_for_removal(const std::string& uri) {
  std::lock_guard<std::mutex> lck(mtx_);

  auto it = members_.find(uri);
  auto it_name = members_by_name_.find(uri);
  if (it != members_.end()) {
    auto member_to_delete = make_shared<GroupMemberV2>(
        it->second->uri(),
        it->second->type(),
        it->second->relative(),
        it->second->name(),
        true);
    members_to_modify_.emplace_back(member_to_delete);
  } else if (it_name != members_by_name_.end()) {
    // If the user passed the name, convert it to the URI for removal
    auto member_to_delete = make_shared<GroupMemberV2>(
        it_name->second->uri(),
        it_name->second->type(),
        it_name->second->relative(),
        it_name->second->name(),
        true);
    members_to_modify_.emplace_back(member_to_delete);
  } else {
    // try URI to see if we need to convert the local file to file://
    URI uri_uri(uri);
    it = members_.find(uri_uri.to_string());
    if (it != members_.end()) {
      auto member_to_delete = make_shared<GroupMemberV2>(
          it->second->uri(),
          it->second->type(),
          it->second->relative(),
          it->second->name(),
          true);
      members_to_modify_.emplace_back(member_to_delete);
    } else {
      throw GroupException(
          "Cannot remove group member " + uri +
          ", member does not exist in group.");
    }
  }
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
  throw GroupException("Invalid call to Group::serialize");
}

shared_ptr<GroupDetails> GroupDetails::deserialize(
    Deserializer& deserializer, const URI& group_uri) {
  uint32_t version = 0;
  version = deserializer.read<uint32_t>();
  if (version == 1) {
    return GroupDetailsV1::deserialize(deserializer, group_uri);
  } else if (version == 2) {
    return GroupDetailsV2::deserialize(deserializer, group_uri);
  }

  throw GroupException("Unsupported group version " + std::to_string(version));
}

shared_ptr<GroupDetails> GroupDetails::deserialize(
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

  if (index >= members_vec_.size()) {
    throw GroupException(
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

  auto it = members_by_name_.find(name);
  if (it == members_by_name_.end()) {
    throw GroupException(name + " does not exist in group");
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
