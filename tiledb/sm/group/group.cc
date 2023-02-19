/**
 * @file   group.cc
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

#include "tiledb/sm/group/group.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/group/group_member_v1.h"
#include "tiledb/sm/group/group_member_v2.h"
#include "tiledb/sm/group/group_v1.h"
#include "tiledb/sm/group/group_v2.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class DeleteGroupStatusException : public StatusException {
 public:
  explicit DeleteGroupStatusException(const std::string& message)
      : StatusException("DeleteGroup", message) {
  }
};

Group::Group(
    const URI& group_uri, StorageManager* storage_manager, uint32_t version)
    : group_uri_(group_uri)
    , storage_manager_(storage_manager)
    , config_(storage_manager_->config())
    , remote_(group_uri.is_tiledb())
    , metadata_loaded_(false)
    , is_open_(false)
    , query_type_(QueryType::READ)
    , timestamp_start_(0)
    , timestamp_end_(UINT64_MAX)
    , encryption_key_(tdb::make_shared<EncryptionKey>(HERE()))
    , version_(version)
    , changes_applied_(false) {
}

Status Group::open(
    QueryType query_type, uint64_t timestamp_start, uint64_t timestamp_end) {
  // Checks
  if (is_open_) {
    return Status_GroupError("Cannot open group; Group already open");
  }

  if (query_type != QueryType::READ && query_type != QueryType::WRITE &&
      query_type != QueryType::MODIFY_EXCLUSIVE) {
    return Status_GroupError("Cannot open group; Unsupported query type");
  }

  if (timestamp_end == UINT64_MAX) {
    if (query_type == QueryType::READ) {
      timestamp_end = utils::time::timestamp_now_ms();
    } else {
      assert(
          query_type == QueryType::WRITE ||
          query_type == QueryType::MODIFY_EXCLUSIVE);
      timestamp_end = 0;
    }
  }

  timestamp_start_ = timestamp_start;
  timestamp_end_ = timestamp_end;

  // Get encryption key from config
  std::string encryption_key_from_cfg;
  bool found = false;
  encryption_key_from_cfg = config_.get("sm.encryption_key", &found);
  assert(found);

  EncryptionType encryption_type = EncryptionType::NO_ENCRYPTION;
  const void* encryption_key = nullptr;
  uint32_t key_length = 0;

  if (!encryption_key_from_cfg.empty()) {
    encryption_key = encryption_key_from_cfg.c_str();
    std::string encryption_type_from_cfg;
    encryption_type_from_cfg = config_.get("sm.encryption_type", &found);
    assert(found);
    auto [st, et] = encryption_type_enum(encryption_type_from_cfg);
    RETURN_NOT_OK(st);
    encryption_type = et.value();

    if (EncryptionKey::is_valid_key_length(
            encryption_type,
            static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
      const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
      if (unit_test_cfg.array_encryption_key_length.is_set()) {
        key_length = unit_test_cfg.array_encryption_key_length.get();
      } else {
        key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
      }
    } else {
      encryption_key = nullptr;
      key_length = 0;
    }
  }

  if (remote_ && encryption_type != EncryptionType::NO_ENCRYPTION)
    return Status_GroupError(
        "Cannot open group; encrypted remote groups are not supported.");

  // Copy the key bytes.
  RETURN_NOT_OK(
      encryption_key_->set_key(encryption_type, encryption_key, key_length));

  metadata_.clear();
  metadata_loaded_ = false;

  // Make sure to reset any values
  members_to_modify_.clear();
  members_.clear();

  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return Status_GroupError(
          "Cannot open group; remote group with no REST client.");
    RETURN_NOT_OK(rest_client->post_group_from_rest(group_uri_, this));
  } else if (query_type == QueryType::READ) {
    try {
      group_dir_ = make_shared<GroupDirectory>(
          HERE(),
          storage_manager_->vfs(),
          storage_manager_->compute_tp(),
          group_uri_,
          timestamp_start,
          timestamp_end);
    } catch (const std::logic_error& le) {
      return Status_GroupDirectoryError(le.what());
    }

    auto&& [st, members] = storage_manager_->group_open_for_reads(this);
    RETURN_NOT_OK(st);
    if (members.has_value()) {
      members_ = members.value();
      members_by_name_.clear();
      members_vec_.clear();
      members_vec_.reserve(members_.size());
      for (auto& it : members_) {
        members_vec_.emplace_back(it.second);
        if (it.second->name().has_value()) {
          members_by_name_.emplace(it.second->name().value(), it.second);
        }
      }
    }
  } else {
    try {
      group_dir_ = make_shared<GroupDirectory>(
          HERE(),
          storage_manager_->vfs(),
          storage_manager_->compute_tp(),
          group_uri_,
          timestamp_start,
          (timestamp_end != 0) ? timestamp_end :
                                 utils::time::timestamp_now_ms());
    } catch (const std::logic_error& le) {
      return Status_GroupDirectoryError(le.what());
    }

    auto&& [st, members] = storage_manager_->group_open_for_writes(this);
    RETURN_NOT_OK(st);

    if (members.has_value()) {
      members_ = members.value();
      members_by_name_.clear();
      members_vec_.clear();
      members_vec_.reserve(members_.size());
      for (auto& it : members_) {
        members_vec_.emplace_back(it.second);
        if (it.second->name().has_value()) {
          members_by_name_.emplace(it.second->name().value(), it.second);
        }
      }
    }

    metadata_.reset(timestamp_end);
  }

  query_type_ = query_type;
  is_open_ = true;

  return Status::Ok();
}

Status Group::open(QueryType query_type) {
  bool found = false;
  RETURN_NOT_OK(config_.get<uint64_t>(
      "sm.group.timestamp_start", &timestamp_start_, &found));
  assert(found);
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.group.timestamp_end", &timestamp_end_, &found));
  assert(found);

  return Group::open(query_type, timestamp_start_, timestamp_end_);
}

Status Group::close() {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError("Cannot close group; Group not open.");

  if (remote_) {
    // Update group metadata for write queries if metadata was written by the
    // user
    if (query_type_ == QueryType::WRITE ||
        query_type_ == QueryType::MODIFY_EXCLUSIVE) {
      if (metadata_.num() > 0) {
        // Set metadata loaded to be true so when serialization fetches the
        // metadata it won't trigger a deadlock
        metadata_loaded_ = true;
        auto rest_client = storage_manager_->rest_client();
        if (rest_client == nullptr)
          return Status_GroupError(
              "Error closing group; remote group with no REST client.");
        RETURN_NOT_OK(
            rest_client->put_group_metadata_to_rest(group_uri_, this));
      }
      if (!members_to_modify_.empty()) {
        auto rest_client = storage_manager_->rest_client();
        if (rest_client == nullptr)
          return Status_GroupError(
              "Error closing group; remote group with no REST client.");
        RETURN_NOT_OK(rest_client->patch_group_to_rest(group_uri_, this));

        members_to_modify_.clear();
      }
    }
    // Storage manager does not own the group schema for remote groups.
  } else {
    if (query_type_ == QueryType::READ) {
      RETURN_NOT_OK(storage_manager_->group_close_for_reads(this));
    } else if (
        query_type_ == QueryType::WRITE ||
        query_type_ == QueryType::MODIFY_EXCLUSIVE) {
      // If changes haven't been applied, apply them
      if (!changes_applied_) {
        RETURN_NOT_OK(apply_pending_changes());
      }
      RETURN_NOT_OK(storage_manager_->group_close_for_writes(this));
    }
  }

  metadata_.clear();
  metadata_loaded_ = false;
  is_open_ = false;
  return clear();
}

bool Group::is_open() const {
  return is_open_;
}

bool Group::is_remote() const {
  return remote_;
}

Status Group::get_query_type(QueryType* query_type) const {
  // Error if the group is not open
  if (!is_open_)
    return LOG_STATUS(
        Status_GroupError("Cannot get query_type; Group is not open"));

  *query_type = query_type_;

  return Status::Ok();
}

void Group::delete_group(const URI& uri, bool recursive) {
  // Check that group is open
  if (!is_open_) {
    throw DeleteGroupStatusException("Group is not open");
  }

  // Check that query type is MODIFY_EXCLUSIVE
  if (query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    throw DeleteGroupStatusException("Query type must be MODIFY_EXCLUSIVE");
  }

  // Delete group members within the group when deleting recursively
  if (recursive) {
    for (auto member : members_vec_) {
      URI uri = member->uri();
      if (member->relative()) {
        uri = group_uri_.join_path(member->uri().to_string());
      }

      if (member->type() == ObjectType::ARRAY) {
        storage_manager_->delete_array(uri.to_string().c_str());
      } else if (member->type() == ObjectType::GROUP) {
        GroupV1 group_rec(uri, storage_manager_);
        throw_if_not_ok(group_rec.open(QueryType::MODIFY_EXCLUSIVE));
        group_rec.delete_group(uri, true);
      }
    }
  }

  // Delete group data
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      throw DeleteGroupStatusException("Remote group with no REST client.");
    rest_client->delete_group_from_rest(uri);
  } else {
    storage_manager_->delete_group(uri.c_str());
  }

  // Close the deleted group
  throw_if_not_ok(this->close());
}

Status Group::delete_metadata(const char* key) {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError("Cannot delete metadata. Group is not open");

  // Check mode
  if (query_type_ != QueryType::WRITE &&
      query_type_ != QueryType::MODIFY_EXCLUSIVE)
    return Status_GroupError(
        "Cannot delete metadata. Group was "
        "not opened in write or modify_exclusive mode");

  // Check if key is null
  if (key == nullptr)
    return Status_GroupError("Cannot delete metadata. Key cannot be null");

  RETURN_NOT_OK(metadata_.del(key));

  return Status::Ok();
}

Status Group::put_metadata(
    const char* key,
    Datatype value_type,
    uint32_t value_num,
    const void* value) {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError("Cannot put metadata; Group is not open");

  // Check mode
  if (query_type_ != QueryType::WRITE &&
      query_type_ != QueryType::MODIFY_EXCLUSIVE)
    return Status_GroupError(
        "Cannot put metadata; Group was "
        "not opened in write or modify_exclusive mode");

  // Check if key is null
  if (key == nullptr)
    return Status_GroupError("Cannot put metadata; Key cannot be null");

  // Check if value type is ANY
  if (value_type == Datatype::ANY)
    return Status_GroupError("Cannot put metadata; Value type cannot be ANY");

  RETURN_NOT_OK(metadata_.put(key, value_type, value_num, value));

  return Status::Ok();
}

Status Group::get_metadata(
    const char* key,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError("Cannot get metadata; Group is not open");

  // Check mode
  if (query_type_ != QueryType::READ)
    return Status_GroupError(
        "Cannot get metadata; Group was "
        "not opened in read mode");

  // Check if key is null
  if (key == nullptr)
    return Status_GroupError("Cannot get metadata; Key cannot be null");

  // Load group metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  RETURN_NOT_OK(metadata_.get(key, value_type, value_num, value));

  return Status::Ok();
}

Status Group::get_metadata(
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError("Cannot get metadata; Group is not open");

  // Check mode
  if (query_type_ != QueryType::READ)
    return Status_GroupError(
        "Cannot get metadata; Group was "
        "not opened in read mode");

  // Load group metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  RETURN_NOT_OK(
      metadata_.get(index, key, key_len, value_type, value_num, value));

  return Status::Ok();
}

Status Group::get_metadata_num(uint64_t* num) {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError(
        "Cannot get number of metadata; Group is not open");

  // Check mode
  if (query_type_ != QueryType::READ)
    return Status_GroupError(
        "Cannot get number of metadata; Group was "
        "not opened in read mode");

  // Load group metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  *num = metadata_.num();

  return Status::Ok();
}

Status Group::has_metadata_key(
    const char* key, Datatype* value_type, bool* has_key) {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError("Cannot get metadata; Group is not open");

  // Check mode
  if (query_type_ != QueryType::READ)
    return Status_GroupError(
        "Cannot get metadata; Group was "
        "not opened in read mode");

  // Check if key is null
  if (key == nullptr)
    return Status_GroupError("Cannot get metadata; Key cannot be null");

  // Load group metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  RETURN_NOT_OK(metadata_.has_key(key, value_type, has_key));

  return Status::Ok();
}

Metadata* Group::unsafe_metadata() {
  return &metadata_;
}

const Metadata* Group::metadata() const {
  return &metadata_;
}

Status Group::metadata(Metadata** metadata) {
  // Load group metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  *metadata = &metadata_;

  return Status::Ok();
}

void Group::set_metadata_loaded(const bool metadata_loaded) {
  metadata_loaded_ = metadata_loaded;
}

const EncryptionKey* Group::encryption_key() const {
  return encryption_key_.get();
}

QueryType Group::query_type() const {
  return query_type_;
}

const Config& Group::config() const {
  return config_;
}

Status Group::set_config(Config config) {
  config_ = config;
  return Status::Ok();
}

Status Group::clear() {
  members_.clear();
  members_by_name_.clear();
  members_vec_.clear();
  members_to_modify_.clear();

  return Status::Ok();
}

void Group::add_member(const tdb_shared_ptr<GroupMember>& group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  const std::string& uri = group_member->uri().to_string();
  members_[uri] = group_member;
  members_vec_.emplace_back(group_member);
  if (group_member->name().has_value()) {
    members_by_name_[group_member->name().value()] = group_member;
  }
}

Status Group::mark_member_for_addition(
    const URI& group_member_uri,
    const bool& relative,
    std::optional<std::string>& name) {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    return Status_GroupError("Cannot add member; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE &&
      query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    return Status_GroupError(
        "Cannot get member; Group was not opened in write or modify_exclusive "
        "mode");
  }

  const std::string& uri = group_member_uri.to_string();
  if (members_to_remove_.find(uri) != members_to_remove_.end()) {
    return Status_GroupError(
        "Cannot add group member " + uri + ", member already set for removal.");
  }

  // If the name is set, validate its unique
  if (name.has_value()) {
    if (members_to_remove_.find(*name) != members_to_remove_.end()) {
      return Status_GroupError(
          "Cannot add group member " + *name +
          ", member already set for removal.");
    }

    if (members_by_name_.find(*name) != members_by_name_.end()) {
      return Status_GroupError(
          "Cannot add group member " + *name +
          ", member already exists in group.");
    }
  }

  URI absolute_group_member_uri = group_member_uri;
  if (relative) {
    absolute_group_member_uri =
        group_uri_.join_path(group_member_uri.to_string());
  }
  ObjectType type = ObjectType::INVALID;
  RETURN_NOT_OK(
      storage_manager_->object_type(absolute_group_member_uri, &type));
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

Status Group::mark_member_for_removal(const URI& uri) {
  return mark_member_for_removal(uri.to_string());
}

Status Group::mark_member_for_removal(const std::string& uri) {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    return Status_GroupError(
        "Cannot mark member for removal; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE &&
      query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    return Status_GroupError(
        "Cannot get member; Group was not opened in write or modify_exclusive "
        "mode");
  }

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
    return Status::Ok();
  } else if (it_name != members_by_name_.end()) {
    // If the user passed the name, convert it to the URI for removal
    auto member_to_delete = make_shared<GroupMemberV2>(
        it_name->second->uri(),
        it_name->second->type(),
        it_name->second->relative(),
        it_name->second->name(),
        true);
    members_to_modify_.emplace_back(member_to_delete);
    return Status::Ok();
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
      return Status::Ok();
    } else {
      return Status_GroupError(
          "Cannot remove group member " + uri +
          ", member does not exist in group.");
    }
  }

  return Status::Ok();
}

const std::vector<tdb_shared_ptr<GroupMember>>& Group::members_to_modify()
    const {
  std::lock_guard<std::mutex> lck(mtx_);
  return members_to_modify_;
}

const std::unordered_map<std::string, tdb_shared_ptr<GroupMember>>&
Group::members() const {
  std::lock_guard<std::mutex> lck(mtx_);
  return members_;
}

void Group::serialize(Serializer&) {
  throw StatusException(Status_GroupError("Invalid call to Group::serialize"));
}

std::optional<tdb_shared_ptr<Group>> Group::deserialize(
    Deserializer& deserializer,
    const URI& group_uri,
    StorageManager* storage_manager) {
  uint32_t version = 0;
  version = deserializer.read<uint32_t>();
  if (version == 1) {
    return GroupV1::deserialize(deserializer, group_uri, storage_manager);
  }
  else if (version == 2) {
    return GroupV2::deserialize(deserializer, group_uri, storage_manager);
  }

  throw StatusException(Status_GroupError(
      "Unsupported group version " + std::to_string(version)));
}

std::optional<tdb_shared_ptr<Group>> Group::deserialize(
    std::vector<Deserializer>& deserializer,
    const URI& group_uri,
    StorageManager* storage_manager) {
  uint32_t version = 0;
  version = deserializer.read<uint32_t>();
  if (version == 1) {
    return GroupV1::deserialize(deserializer, group_uri, storage_manager);
  }
  else if (version == 2) {
    return GroupV2::deserialize(deserializer, group_uri, storage_manager);
  }

  throw StatusException(Status_GroupError(
      "Unsupported group version " + std::to_string(version)));
}

const URI& Group::group_uri() const {
  return group_uri_;
}

const URI Group::group_detail_uri() const {
  return group_uri_.join_path(constants::group_detail_dir_name);
}

const shared_ptr<GroupDirectory> Group::group_directory() const {
  return group_dir_;
}

tuple<Status, optional<URI>> Group::generate_detail_uri() const {
  auto&& [st, name] = generate_name();
  RETURN_NOT_OK_TUPLE(st, std::nullopt);

  URI uri = group_uri_.join_path(constants::group_detail_dir_name)
                .join_path(name.value());

  return {Status::Ok(), uri};
}

bool Group::changes_applied() const {
  return changes_applied_;
}

void Group::set_changes_applied(const bool changes_applied) {
  changes_applied_ = changes_applied;
}

uint64_t Group::member_count() const {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    throw Status_GroupError("Cannot get member count; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw Status_GroupError(
        "Cannot get member; Group was not opened in read mode");
  }

  return members_.size();
}

tuple<std::string, ObjectType, optional<std::string>> Group::member_by_index(
    uint64_t index) {
  std::lock_guard<std::mutex> lck(mtx_);

  // Check if group is open
  if (!is_open_) {
    throw Status_GroupError("Cannot get member by index; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw Status_GroupError(
        "Cannot get member; Group was not opened in read mode");
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
Group::member_by_name(const std::string& name) {
  std::lock_guard<std::mutex> lck(mtx_);

  // Check if group is open
  if (!is_open_) {
    throw Status_GroupError("Cannot get member by name; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw Status_GroupError(
        "Cannot get member; Group was not opened in read mode");
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

std::string Group::dump(
    const uint64_t indent_size,
    const uint64_t num_indents,
    bool recursive,
    bool print_self) const {
  // Build the indentation literal and the leading indentation literal.
  const std::string indent(indent_size, '-');
  const std::string l_indent(indent_size * num_indents, '-');

  std::stringstream ss;
  if (print_self) {
    ss << l_indent << group_uri_.last_path_part() << " "
       << object_type_str(ObjectType::GROUP) << std::endl;
  }

  for (const auto& it : members_vec_) {
    ss << "|" << indent << l_indent << " " << *it << std::endl;
    if (it->type() == ObjectType::GROUP && recursive) {
      URI uri = it->uri();
      if (it->relative()) {
        uri = group_uri_.join_path(it->uri().to_string());
      }

      GroupV1 group_rec(uri, storage_manager_);
      throw_if_not_ok(group_rec.open(QueryType::READ));
      ss << group_rec.dump(indent_size, num_indents + 2, recursive, false);
      throw_if_not_ok(group_rec.close());
    }
  }

  return ss.str();
}

/* ********************************* */
/*         PROTECTED METHODS         */
/* ********************************* */

tuple<Status, optional<std::string>> Group::generate_name() const {
  std::string uuid;
  RETURN_NOT_OK_TUPLE(uuid::generate_uuid(&uuid, false), std::nullopt);

  auto timestamp =
      (timestamp_end_ != 0) ? timestamp_end_ : utils::time::timestamp_now_ms();
  std::stringstream ss;
  ss << "__" << timestamp << "_" << timestamp << "_" << uuid;

  return {Status::Ok(), ss.str()};
}

Status Group::load_metadata() {
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return Status_GroupError(
          "Cannot load metadata; remote group with no REST client.");
    RETURN_NOT_OK(rest_client->post_group_metadata_from_rest(group_uri_, this));
  } else {
    assert(group_dir_->loaded());
    storage_manager_->load_group_metadata(
        group_dir_, *encryption_key_, &metadata_);
  }
  metadata_loaded_ = true;
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
