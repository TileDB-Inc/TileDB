/**
 * @file   group.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

class GroupStatusException : public StatusException {
 public:
  explicit GroupStatusException(const std::string& message)
      : StatusException("Group", message) {
  }
};

Group::Group(const URI& group_uri, StorageManager* storage_manager)
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

  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return Status_GroupError(
          "Cannot open group; remote group with no REST client.");
    }

    // Set initial group details to be deserialized into
    group_details_ = tdb::make_shared<GroupDetailsV2>(HERE(), group_uri_);

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

    auto&& [st, group_details] = storage_manager_->group_open_for_reads(this);
    RETURN_NOT_OK(st);
    if (group_details.has_value()) {
      group_details_ = group_details.value();
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

    auto&& [st, group_details] = storage_manager_->group_open_for_writes(this);
    RETURN_NOT_OK(st);

    if (group_details.has_value()) {
      group_details_ = group_details.value();
    }

    metadata_.reset(timestamp_end);
  }

  // Handle new empty group
  if (!group_details_) {
    group_details_ = tdb::make_shared<GroupDetailsV2>(HERE(), group_uri_);
  }

  query_type_ = query_type;
  is_open_ = true;
  changes_applied_ = false;

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
      if (!members_to_modify().empty()) {
        auto rest_client = storage_manager_->rest_client();
        if (rest_client == nullptr)
          return Status_GroupError(
              "Error closing group; remote group with no REST client.");
        RETURN_NOT_OK(rest_client->patch_group_to_rest(group_uri_, this));
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
        RETURN_NOT_OK(group_details_->apply_pending_changes());
        changes_applied_ = group_details_->changes_applied();
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

shared_ptr<GroupDetails> Group::group_details() {
  return group_details_;
}

const shared_ptr<GroupDetails> Group::group_details() const {
  return group_details_;
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
    throw GroupStatusException("[delete_group] Group is not open");
  }

  // Check that query type is MODIFY_EXCLUSIVE
  if (query_type_ != QueryType::MODIFY_EXCLUSIVE) {
    throw GroupStatusException(
        "[delete_group] Query type must be MODIFY_EXCLUSIVE");
  }

  // Delete group members within the group when deleting recursively
  if (recursive) {
    for (auto member_entry : members()) {
      const auto& member = member_entry.second;
      URI member_uri = member->uri();
      if (member->relative()) {
        member_uri = group_uri_.join_path(member->uri().to_string());
      }

      if (member->type() == ObjectType::ARRAY) {
        storage_manager_->delete_array(member_uri.to_string().c_str());
      } else if (member->type() == ObjectType::GROUP) {
        Group group_rec(member_uri, storage_manager_);
        throw_if_not_ok(group_rec.open(QueryType::MODIFY_EXCLUSIVE));
        group_rec.delete_group(member_uri, true);
      }
    }
  }

  // Delete group data
  if (remote_) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      throw GroupStatusException(
          "[delete_group] Remote group with no REST client.");
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

void Group::set_config(Config config) {
  if (is_open()) {
    throw GroupStatusException("[set_config] Cannot set config; Group is open");
  }
  config_.inherit(config);
}

Status Group::clear() {
  return group_details_->clear();
}

void Group::add_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  group_details_->add_member(group_member);
}

void Group::delete_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  group_details_->delete_member(group_member);
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
  return group_details_->mark_member_for_addition(
      group_member_uri, relative, name, storage_manager_);
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

  return group_details_->mark_member_for_removal(uri);
}

const std::vector<shared_ptr<GroupMember>>& Group::members_to_modify() const {
  std::lock_guard<std::mutex> lck(mtx_);
  return group_details_->members_to_modify();
}

const std::unordered_map<std::string, shared_ptr<GroupMember>>& Group::members()
    const {
  std::lock_guard<std::mutex> lck(mtx_);
  return group_details_->members();
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

  return group_details_->member_count();
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

  return group_details_->member_by_index(index);
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

  return group_details_->member_by_name(name);
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

  for (const auto& member_entry : members()) {
    const auto& it = member_entry.second;
    ss << "|" << indent << l_indent << " " << *it << std::endl;
    if (it->type() == ObjectType::GROUP && recursive) {
      URI member_uri = it->uri();
      if (it->relative()) {
        member_uri = group_uri_.join_path(it->uri().to_string());
      }

      Group group_rec(member_uri, storage_manager_);
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

  const auto& version = group_details_->version();
  auto timestamp =
      (timestamp_end_ != 0) ? timestamp_end_ : utils::time::timestamp_now_ms();
  std::stringstream ss;
  ss << "__" << timestamp << "_" << timestamp << "_" << uuid;
  if (version > 1) {
    ss << "_" << version;
  }

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
