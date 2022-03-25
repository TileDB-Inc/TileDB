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
#include "tiledb/sm/group/group_v1.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/misc/time.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
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

Status Group::open(QueryType query_type) {
  // Checks
  if (is_open_)
    return Status_GroupError("Cannot open group; Group already open");
  if (!remote_) {
    bool is_group = false;
    RETURN_NOT_OK(storage_manager_->is_group(group_uri_, &is_group));
    if (!is_group)
      return Status_GroupError("Cannot open group; Group does not exist");
  }

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

  RETURN_NOT_OK(config_.get<uint64_t>(
      "sm.group.timestamp_start", &timestamp_start_, &found));
  assert(found);
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.group.timestamp_end", &timestamp_end_, &found));
  assert(found);

  if (timestamp_end_ == UINT64_MAX) {
    if (query_type == QueryType::READ) {
      timestamp_end_ = utils::time::timestamp_now_ms();
    } else {
      assert(query_type == QueryType::WRITE);
      timestamp_end_ = 0;
    }
  }

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
          timestamp_start_,
          timestamp_end_);
    } catch (const std::logic_error& le) {
      return Status_GroupDirectoryError(le.what());
    }

    auto&& [st, members] = storage_manager_->group_open_for_reads(this);
    RETURN_NOT_OK(st);
    if (members.has_value()) {
      members_ = members.value();
      members_vec_.clear();
      members_vec_.reserve(members_.size());
      for (auto& it : members_) {
        members_vec_.emplace_back(it.second);
      }
    }
  } else {
    try {
      group_dir_ = make_shared<GroupDirectory>(
          HERE(),
          storage_manager_->vfs(),
          storage_manager_->compute_tp(),
          group_uri_,
          timestamp_start_,
          timestamp_end_);
    } catch (const std::logic_error& le) {
      return Status_GroupDirectoryError(le.what());
    }

    auto&& [st] = storage_manager_->group_open_for_writes(this);
    RETURN_NOT_OK(st);

    metadata_.reset(timestamp_start_, timestamp_end_);
  }

  query_type_ = query_type;
  is_open_ = true;

  return Status::Ok();
}

Status Group::close() {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError("Cannot close group; Group not open.");

  if (remote_) {
    // Update group metadata for write queries if metadata was written by the
    // user
    if (query_type_ == QueryType::WRITE) {
      if (metadata_.num() > 0) {
        // Set metadata loaded to be true so when serialization fetches the
        // metadata it won't trigger a deadlock
        metadata_loaded_ = true;
        auto rest_client = storage_manager_->rest_client();
        if (rest_client == nullptr)
          return Status_GroupError(
              "Error closing group; remote group with no REST client.");
        RETURN_NOT_OK(rest_client->post_group_metadata_to_rest(
            group_uri_, timestamp_start_, timestamp_end_, this));
      }
      if (!members_to_remove_.empty() || !members_to_add_.empty()) {
        auto rest_client = storage_manager_->rest_client();
        if (rest_client == nullptr)
          return Status_GroupError(
              "Error closing group; remote group with no REST client.");
        RETURN_NOT_OK(rest_client->post_group_to_rest(group_uri_, this));
      }
    }

    // Storage manager does not own the group schema for remote groups.
  } else {
    if (query_type_ == QueryType::READ) {
      RETURN_NOT_OK(storage_manager_->group_close_for_reads(this));
    } else if (query_type_ == QueryType::WRITE) {
      RETURN_NOT_OK(apply_pending_changes());
      RETURN_NOT_OK(storage_manager_->group_close_for_writes(this));
    }
  }

  metadata_.clear();
  metadata_loaded_ = false;
  is_open_ = false;

  return Status::Ok();
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

Status Group::delete_metadata(const char* key) {
  // Check if group is open
  if (!is_open_)
    return Status_GroupError("Cannot delete metadata. Group is not open");

  // Check mode
  if (query_type_ != QueryType::WRITE)
    return Status_GroupError(
        "Cannot delete metadata. Group was "
        "not opened in write mode");

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
  if (query_type_ != QueryType::WRITE)
    return Status_GroupError(
        "Cannot put metadata; Group was "
        "not opened in write mode");

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

Metadata* Group::metadata() {
  return &metadata_;
}

Status Group::metadata(Metadata** metadata) {
  // Load group metadata, if not loaded yet
  if (!metadata_loaded_)
    RETURN_NOT_OK(load_metadata());

  *metadata = &metadata_;

  return Status::Ok();
}

const EncryptionKey* Group::encryption_key() const {
  return encryption_key_.get();
}

QueryType Group::query_type() const {
  return query_type_;
}

const Config* Group::config() const {
  return &config_;
}

Status Group::set_config(Config config) {
  config_ = config;
  return Status::Ok();
}

Status Group::add_member(const tdb_shared_ptr<GroupMember>& group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  const std::string& uri = group_member->uri().to_string();
  members_.emplace(uri, group_member);
  members_vec_.emplace_back(group_member);

  return Status::Ok();
}

Status Group::mark_member_for_addition(
    const tdb_shared_ptr<GroupMember>& group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    return Status_GroupError("Cannot get member by index; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE) {
    return Status_GroupError(
        "Cannot get member; Group was not opened in read mode");
  }

  const std::string& uri = group_member->uri().to_string();
  if (members_to_remove_.find(uri) != members_to_remove_.end()) {
    return Status_GroupError(
        "Cannot add group member " + uri + ", member already set for removal.");
  }
  members_to_add_.emplace(uri, group_member);

  return Status::Ok();
}

Status Group::mark_member_for_addition(
    const URI& group_member_uri, const bool& relative) {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    return Status_GroupError("Cannot get member by index; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE) {
    return Status_GroupError(
        "Cannot get member; Group was not opened in read mode");
  }

  const std::string& uri = group_member_uri.to_string();
  if (members_to_remove_.find(uri) != members_to_remove_.end()) {
    return Status_GroupError(
        "Cannot add group member " + uri + ", member already set for removal.");
  }

  URI absolute_group_member_uri = group_member_uri;
  if (relative) {
    absolute_group_member_uri =
        group_uri_.join_path(group_member_uri.to_string());
  }
  ObjectType type = ObjectType::INVALID;
  RETURN_NOT_OK(
      storage_manager_->object_type(absolute_group_member_uri, &type));

  auto group_member =
      tdb::make_shared<GroupMemberV1>(HERE(), group_member_uri, type, relative);

  members_to_add_.emplace(uri, group_member);

  return Status::Ok();
}

Status Group::mark_member_for_removal(const URI& uri) {
  return mark_member_for_removal(uri.to_string());
}

Status Group::mark_member_for_removal(const std::string& uri) {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    return Status_GroupError("Cannot get member by index; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE) {
    return Status_GroupError(
        "Cannot get member; Group was not opened in read mode");
  }
  if (members_to_add_.find(uri) != members_to_add_.end()) {
    return Status_GroupError(
        "Cannot remove group member " + uri +
        ", member already set for adding.");
  }

  members_to_remove_.emplace(uri);
  return Status::Ok();
}

const std::unordered_set<std::string>& Group::members_to_remove() const {
  std::lock_guard<std::mutex> lck(mtx_);
  return members_to_remove_;
}

const std::unordered_map<std::string, tdb_shared_ptr<GroupMember>>&
Group::members_to_add() const {
  std::lock_guard<std::mutex> lck(mtx_);
  return members_to_add_;
}

const std::unordered_map<std::string, tdb_shared_ptr<GroupMember>>&
Group::members() const {
  std::lock_guard<std::mutex> lck(mtx_);
  return members_;
}

Status Group::serialize(Buffer*) {
  return Status_GroupError("Invalid call to Group::serialize");
}

Status Group::apply_and_serialize(Buffer* buff) {
  RETURN_NOT_OK(apply_pending_changes());
  return serialize(buff);
}

std::tuple<Status, std::optional<tdb_shared_ptr<Group>>> Group::deserialize(
    ConstBuffer* buff, const URI& group_uri, StorageManager* storage_manager) {
  uint32_t version = 0;
  RETURN_NOT_OK_TUPLE(buff->read(&version, sizeof(version)), std::nullopt);
  if (version == 1) {
    return GroupV1::deserialize(buff, group_uri, storage_manager);
  }

  return {
      Status_GroupError("Unsupported group version " + std::to_string(version)),
      std::nullopt};
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

tuple<Status, optional<uint64_t>> Group::member_count() const {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    return {Status_GroupError("Cannot get member by index; Group is not open"),
            std::nullopt};
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    return {Status_GroupError(
                "Cannot get member; Group was not opened in read mode"),
            std::nullopt};
  }

  return {Status::Ok(), members_.size()};
}

tuple<Status, optional<std::string>, optional<ObjectType>>
Group::member_by_index(uint64_t index) {
  std::lock_guard<std::mutex> lck(mtx_);

  // Check if group is open
  if (!is_open_) {
    return {Status_GroupError("Cannot get member by index; Group is not open"),
            std::nullopt,
            std::nullopt};
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    return {Status_GroupError(
                "Cannot get member; Group was not opened in read mode"),
            std::nullopt,
            std::nullopt};
    ;
  }

  if (index > members_vec_.size()) {
    return {
        Status_GroupError(
            "index " + std::to_string(index) + " is larger than member count " +
            std::to_string(members_vec_.size())),
        std::nullopt,
        std::nullopt};
    ;
  }

  auto member = members_vec_[index];

  std::string uri = member->uri().to_string();
  if (member->relative()) {
    uri = group_uri_.join_path(member->uri().to_string()).to_string();
  }

  return {Status::Ok(), uri, member->type()};
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
      GroupV1 group_rec(it->uri(), storage_manager_);
      group_rec.open(QueryType::READ);
      ss << group_rec.dump(indent_size, num_indents + 2, recursive, false);
      group_rec.close();
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
    RETURN_NOT_OK(rest_client->get_group_metadata_from_rest(
        group_uri_, timestamp_start_, timestamp_end_, this));
  } else {
    assert(group_dir_->loaded());
    RETURN_NOT_OK(storage_manager_->load_group_metadata(
        group_dir_, *encryption_key_, &metadata_));
  }
  metadata_loaded_ = true;
  return Status::Ok();
}

Status Group::apply_pending_changes() {
  std::lock_guard<std::mutex> lck(mtx_);

  // Remove members first
  for (const auto& uri : members_to_remove_) {
    members_.erase(uri);
    // Check to remove relative URIs
    if (uri.find(group_uri_.add_trailing_slash().to_string()) !=
        std::string::npos) {
      // Get the substring relative path
      auto relative_uri = uri.substr(
          group_uri_.add_trailing_slash().to_string().size(), uri.size());
      members_.erase(relative_uri);
    }
  }

  for (const auto& it : members_to_add_) {
    members_.emplace(it);
  }

  changes_applied_ = !members_to_add_.empty() || !members_to_remove_.empty();
  members_to_remove_.clear();
  members_to_add_.clear();

  members_vec_.clear();
  members_vec_.reserve(members_.size());
  for (auto& it : members_) {
    members_vec_.emplace_back(it.second);
  }

  return Status::Ok();
}
}  // namespace sm
}  // namespace tiledb
