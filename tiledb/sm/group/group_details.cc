/**
 * @file   group_details.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2025 TileDB, Inc.
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
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/group/group_details_v1.h"
#include "tiledb/sm/group/group_details_v2.h"
#include "tiledb/sm/group/group_member_v1.h"
#include "tiledb/sm/group/group_member_v2.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/object/object.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/tile/generic_tile_io.h"

using namespace tiledb::common;

namespace tiledb::sm {

GroupDetails::GroupDetails(const URI& group_uri, uint32_t version)
    : group_uri_(group_uri)
    , is_modified_(false)
    , version_(version) {
}

void GroupDetails::clear() {
  members_.clear();
  invalidate_lookups();
  members_to_modify_.clear();
  member_keys_to_add_.clear();
  member_keys_to_delete_.clear();
  is_modified_ = false;
}

void GroupDetails::add_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  auto key = group_member->key();
  members_[key] = group_member;
  // Invalidate the lookup tables.
  invalidate_lookups();
}

void GroupDetails::delete_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  members_.erase(group_member->key());
  // Invalidate the lookup tables.
  invalidate_lookups();
}

void GroupDetails::mark_member_for_addition(
    ContextResources& resources,
    const URI& group_member_uri,
    const bool& relative,
    std::optional<std::string>& name,
    std::optional<ObjectType> type) {
  std::lock_guard<std::mutex> lck(mtx_);
  ObjectType obj_type = ObjectType::INVALID;
  if (type.has_value()) {
    obj_type = type.value();
  } else {
    URI absolute_group_member_uri = group_member_uri;
    if (relative) {
      absolute_group_member_uri =
          group_uri_.join_path(group_member_uri.to_string());
    }

    // 3.0 REST will identify the object type server side.
    if (!absolute_group_member_uri.is_tiledb() ||
        (resources.rest_client()->rest_enabled() &&
         resources.rest_client()->rest_legacy())) {
      obj_type = object_type(resources, absolute_group_member_uri);
    }
  }
  auto group_member = tdb::make_shared<GroupMemberV2>(
      HERE(), group_member_uri, obj_type, relative, name, false);

  if (!member_keys_to_add_.insert(group_member->key()).second) {
    throw GroupDetailsException(
        "Cannot add group member " + group_member->key() +
        ", a member with the same name or URI has already been added.");
  }

  members_to_modify_.emplace_back(group_member);
  is_modified_ = true;
}

void GroupDetails::mark_member_for_removal(const std::string& name_or_uri) {
  std::lock_guard<std::mutex> lck(mtx_);

  // Try to find the member by key.
  shared_ptr<GroupMemberV2> member_to_delete = nullptr;
  auto it = members_.find(name_or_uri);
  if (it == members_.end()) {
    // try URI to see if we need to convert the local file to file://
    it = members_.find(URI(name_or_uri).to_string());
  }

  // We found the member by key, set the member to delete pointer.
  if (it != members_.end()) {
    member_to_delete = make_shared<GroupMemberV2>(
        it->second->uri(),
        it->second->type(),
        it->second->relative(),
        it->second->name(),
        true);
  } else {
    // Try to lookup by URI.
    ensure_lookup_by_uri();

    // Make sure the user cannot delete members by URI when there are more than
    // one group member with the same URI.
    auto it_dup = duplicated_uris_->find(name_or_uri);
    if (it_dup == duplicated_uris_->end()) {
      // try URI to see if we need to convert the local file to file://
      it_dup = duplicated_uris_->find(URI(name_or_uri).to_string());
    }
    if (it_dup != duplicated_uris_->end()) {
      throw GroupDetailsException(
          "Cannot remove group member " + name_or_uri +
          ", there are multiple members with the same URI, please remove by "
          "name.");
    }

    // Try to find the member by URI.
    auto it_by_url = members_by_uri_->find(name_or_uri);
    if (it_by_url == members_by_uri_->end()) {
      // try URI to see if we need to convert the local file to file://
      it_by_url = members_by_uri_->find(URI(name_or_uri).to_string());
    }

    // The member was found, set the member to delete pointer.
    if (it_by_url != members_by_uri_->end()) {
      member_to_delete = make_shared<GroupMemberV2>(
          it_by_url->second->uri(),
          it_by_url->second->type(),
          it_by_url->second->relative(),
          it_by_url->second->name(),
          true);
    }
  }

  // Delete the member, if set.
  if (member_to_delete != nullptr) {
    if (member_keys_to_add_.count(member_to_delete->key()) != 0) {
      throw GroupDetailsException(
          "Cannot remove group member " + member_to_delete->key() +
          ", a member with the same name or URI has already been added.");
    }

    if (!member_keys_to_delete_.insert(member_to_delete->key()).second) {
      throw GroupDetailsException(
          "Cannot remove group member " + member_to_delete->key() +
          ", a member with the same name or URI has already been removed.");
    }

    members_to_modify_.emplace_back(member_to_delete);
    is_modified_ = true;
  } else {
    throw GroupDetailsException(
        "Cannot remove group member " + name_or_uri +
        ", member does not exist in group.");
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

void GroupDetails::store(
    ContextResources& resources,
    const URI& group_detail_folder_uri,
    const URI& group_detail_uri,
    const EncryptionKey& encryption_key) {
  // Serialize
  auto members = members_to_serialize();
  SizeComputationSerializer size_computation_serializer;
  serialize(members, size_computation_serializer);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(),
      resources.ephemeral_memory_tracker())};

  Serializer serializer(tile->data(), tile->size());
  serialize(members, serializer);
  resources.stats().add_counter("write_group_size", tile->size());

  // Check if the array schema directory exists
  // If not create it, this is caused by a pre-v10 array
  auto& vfs = resources.vfs();
  if (!vfs.is_dir(group_detail_folder_uri)) {
    vfs.create_dir(group_detail_folder_uri);
  }
  GenericTileIO::store_data(resources, group_detail_uri, tile, encryption_key);
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

  throw GroupDetailsException(
      "Unsupported group version " + std::to_string(version));
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

void GroupDetails::set_group_uri(const URI& uri) {
  std::lock_guard<std::mutex> lck(mtx_);
  group_uri_ = uri;
}

uint64_t GroupDetails::member_count() const {
  std::lock_guard<std::mutex> lck(mtx_);

  return members_.size();
}

tuple<std::string, ObjectType, optional<std::string>>
GroupDetails::member_by_index(uint64_t index) {
  std::lock_guard<std::mutex> lck(mtx_);

  if (index >= members_.size()) {
    throw GroupDetailsException(
        "index " + std::to_string(index) + " is larger than member count " +
        std::to_string(members_.size()));
  }

  ensure_lookup_by_index();
  auto member = members_vec_->at(index);
  std::string uri = member->uri().to_string();
  if (member->relative()) {
    uri = group_uri_.join_path(member->uri().to_string()).to_string();
  }

  return {uri, member->type(), member->name()};
}

tuple<std::string, ObjectType, optional<std::string>, bool>
GroupDetails::member_by_name(const std::string& name) {
  std::lock_guard<std::mutex> lck(mtx_);

  auto it = members_.find(name);

  // If we didn't find the key in the members list or if the found member is a
  // nameless member, return as not found.
  if (it == members_.end() || !it->second->name().has_value()) {
    throw GroupDetailsException(name + " does not exist in group");
  }

  auto member = it->second;
  std::string uri = member->uri().to_string();
  // Relative tiledb URIs are returned in the expected format from REST.
  if (!member->uri().is_tiledb() && member->relative()) {
    uri = group_uri_.join_path(member->uri().to_string()).to_string();
  }

  return {uri, member->type(), member->name(), member->relative()};
}

format_version_t GroupDetails::version() const {
  return version_;
}

void GroupDetails::ensure_lookup_by_index() {
  // Populate the the member by index lookup if it hasn't been generated.
  if (members_vec_ == nullopt) {
    members_vec_.emplace();
    members_vec_->reserve(members_.size());
    for (auto& [key, member] : members_) {
      members_vec_->emplace_back(member);
    }
  }
}

void GroupDetails::ensure_lookup_by_uri() {
  // Populate the the member by uri lookup if it hasn't been generated.
  if (members_by_uri_ == nullopt) {
    if (duplicated_uris_ != nullopt) {
      GroupDetailsException("`duplicated_uris_` should not be generated.");
    }

    members_by_uri_.emplace();
    duplicated_uris_.emplace();
    for (auto& [key, member] : members_) {
      const std::string& uri = member->uri().to_string();

      // See if the URI is already duplicated.
      auto dup_it = duplicated_uris_->find(uri);
      if (dup_it != duplicated_uris_->end()) {
        dup_it->second++;
        continue;
      }

      // See if the URI already exists.
      auto it = members_by_uri_->find(uri);
      if (it != members_by_uri_->end()) {
        members_by_uri_->erase(it);
        duplicated_uris_->emplace(uri, 2);
        continue;
      }

      members_by_uri_->emplace(uri, member);
    }
  }
}

void GroupDetails::invalidate_lookups() {
  members_vec_ = nullopt;
  members_by_uri_ = nullopt;
  duplicated_uris_ = nullopt;
}

}  // namespace tiledb::sm
