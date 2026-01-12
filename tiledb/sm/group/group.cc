/**
 * @file   group.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2025 TileDB, Inc.
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
#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/consolidator/consolidator.h"
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
#include "tiledb/sm/object/object.h"
#include "tiledb/sm/object/object_mutex.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb::common;

namespace tiledb::sm {

class GroupException : public StatusException {
 public:
  explicit GroupException(const std::string& message)
      : StatusException("Group", message) {
  }
};

Group::Group(ContextResources& resources, const URI& group_uri)
    : memory_tracker_(resources.create_memory_tracker())
    , group_uri_(group_uri)
    , config_(resources.config())
    , remote_(group_uri.is_tiledb())
    , metadata_(memory_tracker_)
    , metadata_loaded_(false)
    , is_open_(false)
    , query_type_(QueryType::READ)
    , timestamp_start_(0)
    , timestamp_end_(UINT64_MAX)
    , encryption_key_(tdb::make_shared<EncryptionKey>(HERE()))
    , resources_(resources) {
  memory_tracker_->set_type(MemoryTrackerType::GROUP);
}

void Group::create(ContextResources& resources, const URI& uri) {
  // Create group URI
  if (uri.is_invalid())
    throw GroupException(
        "Cannot create group '" + uri.to_string() + "'; Invalid group URI");

  // Check if group exists
  if (is_group(resources, uri)) {
    throw GroupException(
        "Cannot create group; Group '" + uri.to_string() + "' already exists");
  }

  std::lock_guard<std::mutex> lock{object_mtx};
  if (uri.is_tiledb()) {
    Group group(resources, uri);
    throw_if_not_ok(
        resources.rest_client()->post_group_create_to_rest(uri, &group));
    return;
  }

  // Create group directory
  resources.vfs().create_dir(uri);

  // Create group file
  URI group_filename = uri.join_path(constants::group_filename);
  resources.vfs().touch(group_filename);

  // Create metadata folder
  resources.vfs().create_dir(uri.join_path(constants::group_metadata_dir_name));

  // Create group detail folder
  resources.vfs().create_dir(uri.join_path(constants::group_detail_dir_name));
}

void Group::open(
    QueryType query_type, uint64_t timestamp_start, uint64_t timestamp_end) {
  // Checks
  if (is_open_) {
    throw GroupException("Cannot open group; Group already open");
  }

  if (query_type == QueryType::MODIFY_EXCLUSIVE) {
    resources_.logger()->warn(
        "Opening group in MODIFY_EXCLUSIVE mode is deprecated and has no "
        "additional behavior over WRITE. Use WRITE mode instead.");
    query_type = QueryType::WRITE;
  }

  if (query_type != QueryType::READ && query_type != QueryType::WRITE) {
    throw GroupException("Cannot open group; Unsupported query type");
  }

  if (timestamp_end == UINT64_MAX) {
    if (query_type == QueryType::READ) {
      timestamp_end = utils::time::timestamp_now_ms();
    } else {
      iassert(
          query_type == QueryType::WRITE,
          "query_type = {}",
          query_type_str(query_type));
      timestamp_end = 0;
    }
  }

  timestamp_start_ = timestamp_start;
  timestamp_end_ = timestamp_end;

  // Get encryption key from config
  const std::string encryption_key_from_cfg =
      config_.get<std::string>("sm.encryption_key", Config::must_find);

  EncryptionType encryption_type = EncryptionType::NO_ENCRYPTION;
  const void* encryption_key = nullptr;
  uint32_t key_length = 0;

  if (!encryption_key_from_cfg.empty()) {
    encryption_key = encryption_key_from_cfg.c_str();
    key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
    const std::string encryption_type_from_cfg =
        config_.get<std::string>("sm.encryption_type", Config::must_find);
    auto [st, et] = encryption_type_enum(encryption_type_from_cfg);
    throw_if_not_ok(st);
    encryption_type = et.value();

    if (!EncryptionKey::is_valid_key_length(
            encryption_type,
            static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
      encryption_key = nullptr;
      key_length = 0;
    }
  }

  if (remote_ && encryption_type != EncryptionType::NO_ENCRYPTION) {
    throw GroupException(
        "Cannot open group; encrypted remote groups are not supported.");
  }

  // Copy the key bytes.
  throw_if_not_ok(
      encryption_key_->set_key(encryption_type, encryption_key, key_length));

  metadata_.clear();
  metadata_loaded_ = false;

  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw GroupException(
          "Cannot open group; remote group with no REST client.");
    }

    // Set initial group details to be deserialized into
    group_details_ = tdb::make_shared<GroupDetailsV2>(HERE(), group_uri_);

    throw_if_not_ok(rest_client->post_group_from_rest(group_uri_, this));
  } else if (query_type == QueryType::READ) {
    group_dir_ = make_shared<GroupDirectory>(
        HERE(),
        resources_.vfs(),
        resources_.compute_tp(),
        group_uri_,
        timestamp_start,
        timestamp_end);
    group_open_for_reads();
  } else {
    group_dir_ = make_shared<GroupDirectory>(
        HERE(),
        resources_.vfs(),
        resources_.compute_tp(),
        group_uri_,
        timestamp_start,
        (timestamp_end != 0) ? timestamp_end : utils::time::timestamp_now_ms());
    group_open_for_writes();
    metadata_.reset(timestamp_end);
  }

  // Handle new empty group
  if (!group_details_) {
    group_details_ = tdb::make_shared<GroupDetailsV2>(HERE(), group_uri_);
  }

  query_type_ = query_type;
  is_open_ = true;
}

void Group::open(QueryType query_type) {
  timestamp_start_ =
      config_.get<uint64_t>("sm.group.timestamp_start", Config::must_find);
  timestamp_end_ =
      config_.get<uint64_t>("sm.group.timestamp_end", Config::must_find);

  Group::open(query_type, timestamp_start_, timestamp_end_);
}

void Group::close_for_writes() {
  // Flush the group metadata
  unsafe_metadata()->store(resources_, group_uri(), *encryption_key());

  // Store any changes required
  if (group_details()->is_modified()) {
    const URI& group_detail_folder_uri = group_detail_uri();
    auto group_detail_uri = generate_detail_uri();
    group_details()->store(
        resources_,
        group_detail_folder_uri,
        group_detail_uri,
        *encryption_key());
  }
}

void Group::close() {
  // Check if group is open
  if (!is_open_)
    return;

  if (remote_) {
    // Update group metadata for write queries if metadata was written by the
    // user
    if (query_type_ == QueryType::WRITE) {
      if (metadata_.num() > 0) {
        // Set metadata loaded to be true so when serialization fetches the
        // metadata it won't trigger a deadlock
        metadata_loaded_ = true;
        auto rest_client = resources_.rest_client();
        if (rest_client == nullptr) {
          throw GroupException(
              "Error closing group; remote group with no REST client.");
        }
        throw_if_not_ok(
            rest_client->put_group_metadata_to_rest(group_uri_, this));
      }
      if (!members_to_modify().empty()) {
        auto rest_client = resources_.rest_client();
        if (rest_client == nullptr) {
          throw GroupException(
              "Error closing group; remote group with no REST client.");
        }
        throw_if_not_ok(rest_client->patch_group_to_rest(group_uri_, this));
      }
    }
    // Storage manager does not own the group schema for remote groups.
  } else {
    if (query_type_ == QueryType::READ) {
      close_for_reads();
    } else if (query_type_ == QueryType::WRITE) {
      try {
        close_for_writes();
      } catch (StatusException& exc) {
        throw GroupException(
            std::string(exc.what()) +
            " : Was storage for the group moved or deleted before closing?");
      }
    }
  }

  metadata_.clear();
  metadata_loaded_ = false;
  is_open_ = false;
  clear();
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

void Group::set_uri(const URI& uri) {
  std::lock_guard<std::mutex> lck(mtx_);
  group_uri_ = uri;
  group_details_->set_group_uri(uri);
}

QueryType Group::get_query_type() const {
  // Error if the group is not open
  if (!is_open_) {
    throw GroupException("Cannot get query_type; Group is not open");
  }

  return query_type_;
}

void Group::delete_group(const URI& uri, bool recursive) {
  // Check that group is open
  if (!is_open_) {
    throw GroupException("[delete_group] Group is not open");
  }

  // Check that query type is WRITE
  if (query_type_ != QueryType::WRITE) {
    throw GroupException("[delete_group] Query type must be WRITE");
  }

  // Delete group data
  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw GroupException("[delete_group] Remote group with no REST client.");
    }
    rest_client->delete_group_from_rest(uri, recursive);
  } else {
    // Delete group members within the group when deleting recursively
    if (recursive) {
      for (auto member_entry : members()) {
        const auto& member = member_entry.second;
        URI member_uri = member->uri();
        if (member->relative()) {
          member_uri = group_uri_.join_path(member->uri().to_string());
        }

        if (member->type() == ObjectType::ARRAY) {
          Array::delete_array(resources_, member_uri);
        } else if (member->type() == ObjectType::GROUP) {
          Group group_rec(resources_, member_uri);
          group_rec.open(QueryType::WRITE);
          group_rec.delete_group(member_uri, true);
        }
      }
    }

    auto& vfs = resources_.vfs();
    auto& compute_tp = resources_.compute_tp();
    auto group_dir = GroupDirectory(
        vfs, compute_tp, uri, 0, std::numeric_limits<uint64_t>::max());

    // Delete the group detail, group metadata and group files
    vfs.remove_files(&compute_tp, group_dir.group_detail_uris());
    vfs.remove_files(&compute_tp, group_dir.group_meta_uris());
    vfs.remove_files(&compute_tp, group_dir.group_meta_uris_to_vacuum());
    vfs.remove_files(&compute_tp, group_dir.group_meta_vac_uris_to_vacuum());
    vfs.remove_files(&compute_tp, group_dir.group_file_uris());

    // Delete all tiledb child directories
    // Note: using vfs().ls() here could delete user data
    std::vector<URI> dirs;
    auto parent_dir = group_dir.uri().c_str();
    for (auto group_dir_name : constants::group_dir_names) {
      dirs.emplace_back(URI(parent_dir + group_dir_name));
    }
    vfs.remove_dirs(&compute_tp, dirs);
    vfs.remove_dir_if_empty(group_dir.uri());
  }
  // Clear metadata and other pending changes to avoid patching a deleted group.
  metadata_.clear();
  group_details_->clear();

  // Close the deleted group
  this->close();
}

void Group::delete_metadata(const char* key) {
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot delete metadata. Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE) {
    throw GroupException(
        "Cannot delete metadata. Group was not opened in write mode");
  }

  // Check if key is null
  if (key == nullptr) {
    throw GroupException("Cannot delete metadata. Key cannot be null");
  }

  metadata_.del(key);
}

void Group::put_metadata(
    const char* key,
    Datatype value_type,
    uint32_t value_num,
    const void* value) {
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot put metadata; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE) {
    throw GroupException(
        "Cannot put metadata; Group was not opened in write mode");
  }

  // Check if key is null
  if (key == nullptr) {
    throw GroupException("Cannot put metadata; Key cannot be null");
  }

  // Check if value type is ANY
  if (value_type == Datatype::ANY) {
    throw GroupException("Cannot put metadata; Value type cannot be ANY");
  }

  metadata_.put(key, value_type, value_num, value);
}

void Group::get_metadata(
    const char* key,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot get metadata; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw GroupException(
        "Cannot get metadata; Group was not opened in read mode");
  }

  // Check if key is null
  if (key == nullptr) {
    throw GroupException("Cannot get metadata; Key cannot be null");
  }

  // Load group metadata, if not loaded yet
  if (!metadata_loaded_) {
    load_metadata();
  }

  metadata_.get(key, value_type, value_num, value);
}

void Group::get_metadata(
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    Datatype* value_type,
    uint32_t* value_num,
    const void** value) {
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot get metadata; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw GroupException(
        "Cannot get metadata; Group was not opened in read mode");
  }

  // Load group metadata, if not loaded yet
  if (!metadata_loaded_) {
    load_metadata();
  }

  metadata_.get(index, key, key_len, value_type, value_num, value);
}

uint64_t Group::get_metadata_num() {
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot get number of metadata; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw GroupException(
        "Cannot get number of metadata; Group was not opened in read mode");
  }

  // Load group metadata, if not loaded yet
  if (!metadata_loaded_) {
    load_metadata();
  }

  return metadata_.num();
}

std::optional<Datatype> Group::metadata_type(const char* key) {
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot get metadata; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw GroupException(
        "Cannot get metadata; Group was not opened in read mode");
  }

  // Check if key is null
  if (key == nullptr) {
    throw GroupException("Cannot get metadata; Key cannot be null");
  }

  // Load group metadata, if not loaded yet
  if (!metadata_loaded_) {
    load_metadata();
  }

  return metadata_.metadata_type(key);
}

Metadata* Group::metadata() {
  // Load group metadata, if not loaded yet
  if (!metadata_loaded_)
    load_metadata();

  return &metadata_;
}

Metadata* Group::unsafe_metadata() {
  return &metadata_;
}

void Group::set_metadata_loaded(const bool metadata_loaded) {
  metadata_loaded_ = metadata_loaded;
}

void Group::consolidate_metadata(
    ContextResources& resources, const char* group_name, const Config& config) {
  // Check group URI
  URI group_uri(group_name);
  if (group_uri.is_invalid()) {
    throw GroupException("Cannot consolidate group metadata; Invalid URI");
  }
  // Check if group exists
  if (object_type(resources, group_uri) != ObjectType::GROUP) {
    throw GroupException(
        "Cannot consolidate group metadata; Group does not exist");
  }

  // Consolidate
  // Encryption credentials are loaded by Group from config
  StorageManager sm(resources, resources.logger(), config);
  auto consolidator = Consolidator::create(
      resources, ConsolidationMode::GROUP_META, config, &sm);
  throw_if_not_ok(consolidator->consolidate(
      group_name, EncryptionType::NO_ENCRYPTION, nullptr, 0));
}

void Group::vacuum_metadata(
    ContextResources& resources, const char* group_name, const Config& config) {
  // Check group URI
  URI group_uri(group_name);
  if (group_uri.is_invalid()) {
    throw GroupException("Cannot vacuum group metadata; Invalid URI");
  }

  // Check if group exists
  if (object_type(resources, group_uri) != ObjectType::GROUP) {
    throw GroupException("Cannot vacuum group metadata; Group does not exist");
  }

  StorageManager sm(resources, resources.logger(), config);
  auto consolidator = Consolidator::create(
      resources, ConsolidationMode::GROUP_META, config, &sm);
  consolidator->vacuum(group_name);
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
    throw GroupException("[set_config] Cannot set config; Group is open");
  }
  config_.inherit(config);
}

void Group::clear() {
  group_details_->clear();
}

void Group::add_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  group_details_->add_member(group_member);
}

void Group::delete_member(const shared_ptr<GroupMember> group_member) {
  std::lock_guard<std::mutex> lck(mtx_);
  group_details_->delete_member(group_member);
}

void Group::mark_member_for_addition(
    const URI& group_member_uri,
    const bool& relative,
    std::optional<std::string>& name,
    std::optional<ObjectType> type) {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot add member; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE) {
    throw GroupException(
        "Cannot add member; Group was not opened in write mode");
  }
  group_details_->mark_member_for_addition(
      resources_, group_member_uri, relative, name, type);
}

void Group::mark_member_for_removal(const std::string& name) {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot mark member for removal; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::WRITE) {
    throw GroupException(
        "Cannot get member; Group was not opened in write mode");
  }

  group_details_->mark_member_for_removal(name);
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

URI Group::generate_detail_uri() const {
  auto ts_name = tiledb::storage_format::generate_timestamped_name(
      timestamp_end_, group_details_->version());

  return group_uri_.join_path(constants::group_detail_dir_name)
      .join_path(ts_name);
}

uint64_t Group::member_count() const {
  std::lock_guard<std::mutex> lck(mtx_);
  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot get member count; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw GroupException(
        "Cannot get member; Group was not opened in read mode");
  }

  return group_details_->member_count();
}

tuple<std::string, ObjectType, optional<std::string>> Group::member_by_index(
    uint64_t index) {
  std::lock_guard<std::mutex> lck(mtx_);

  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot get member by index; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw GroupException(
        "Cannot get member; Group was not opened in read mode");
  }

  return group_details_->member_by_index(index);
}

tuple<std::string, ObjectType, optional<std::string>, bool>
Group::member_by_name(const std::string& name) {
  std::lock_guard<std::mutex> lck(mtx_);

  // Check if group is open
  if (!is_open_) {
    throw GroupException("Cannot get member by name; Group is not open");
  }

  // Check mode
  if (query_type_ != QueryType::READ) {
    throw GroupException(
        "Cannot get member; Group was not opened in read mode");
  }

  return group_details_->member_by_name(name);
}

std::string Group::dump(
    const uint64_t indent_size,
    const uint64_t num_indents,
    bool recursive,
    bool print_self) const {
  // Create a set to track visited groups and prevent cycles
  std::unordered_set<std::reference_wrapper<URI>, URIRefHash, URIRefEqual>
      visited;
  visited.insert(std::ref(const_cast<URI&>(group_uri_)));

  // Create a string stream to hold the dump output
  std::stringstream ss;

  dump(indent_size, num_indents, recursive, print_self, visited, ss);
  return ss.str();
}

void Group::dump(
    const uint64_t indent_size,
    const uint64_t num_indents,
    bool recursive,
    bool print_self,
    std::unordered_set<std::reference_wrapper<URI>, URIRefHash, URIRefEqual>&
        visited,
    std::stringstream& ss) const {
  // Build the indentation literal and the leading indentation literal.
  const std::string indent(indent_size, '-');
  const std::string l_indent(indent_size * num_indents, '-');

  if (print_self) {
    ss << l_indent << group_uri_.last_path_part() << " "
       << object_type_str(ObjectType::GROUP) << std::endl;
  }

  for (const auto& member_entry : members()) {
    const auto& it = member_entry.second;
    ss << "|" << indent << l_indent << " " << *it;
    if (it->type() == ObjectType::GROUP && recursive) {
      URI member_uri = it->uri();
      if (it->relative()) {
        member_uri = group_uri_.join_path(it->uri().to_string());
      }

      // Check if we've already visited this group to avoid cycles
      if (visited.find(std::ref(member_uri)) != visited.end()) {
        ss << std::endl;
        continue;
      }

      Group group_rec(resources_, member_uri);
      try {
        group_rec.open(QueryType::READ);
        ss << std::endl;
        // Mark this group as visited before recursing
        visited.insert(std::ref(member_uri));
        group_rec.dump(
            indent_size, num_indents + 2, recursive, false, visited, ss);
        // Remove from visited set after recursion to allow the same group
        // to appear in different branches (but not in the same path)
        visited.erase(std::ref(member_uri));
        group_rec.close();
      } catch (GroupNotFoundException&) {
        // If the group no longer exists in storage it will be listed but we
        // won't be able to dump its members
        ss << " (does not exist)" << std::endl;
      }
    } else {
      ss << std::endl;
    }
  }
}

/* ********************************* */
/*         PROTECTED METHODS         */
/* ********************************* */

void Group::load_metadata() {
  if (remote_) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw GroupException(
          "Cannot load metadata; remote group with no REST client.");
    }
    throw_if_not_ok(
        rest_client->post_group_metadata_from_rest(group_uri_, this));
  } else {
    passert(group_dir_->loaded());
    load_metadata_from_storage(group_dir_, *encryption_key_);
  }
  metadata_loaded_ = true;
}

void Group::load_metadata_from_storage(
    const shared_ptr<GroupDirectory>& group_dir,
    const EncryptionKey& encryption_key) {
  auto timer_se =
      resources_.stats().start_timer("group_load_metadata_from_storage");

  // Determine which group metadata to load
  const auto& group_metadata_to_load = group_dir->group_meta_uris();

  auto metadata_num = group_metadata_to_load.size();
  // TBD: Might use DynamicArray when it is more capable.
  std::vector<shared_ptr<Tile>> metadata_tiles(metadata_num);
  throw_if_not_ok(
      parallel_for(&resources_.compute_tp(), 0, metadata_num, [&](size_t m) {
        const auto& uri = group_metadata_to_load[m].uri_;

        metadata_tiles[m] = GenericTileIO::load(
            resources_,
            uri,
            0,
            encryption_key,
            resources_.ephemeral_memory_tracker());

        return Status::Ok();
      }));

  // Compute array metadata size for the statistics
  uint64_t meta_size = 0;
  for (const auto& t : metadata_tiles) {
    meta_size += t->size();
  }
  resources_.stats().add_counter("group_read_group_meta_size", meta_size);

  // Copy the deserialized metadata into the original Metadata object
  metadata_ = Metadata::deserialize(metadata_tiles);
  metadata_.set_loaded_metadata_uris(group_metadata_to_load);
}

void Group::group_open_for_reads() {
  auto timer_se = resources_.stats().start_timer("group_open_for_reads");

  // Load group data
  load_group_details();
}

void Group::load_group_details() {
  auto timer_se = resources_.stats().start_timer("load_group_details");
  const URI& latest_group_uri = group_directory()->latest_group_details_uri();
  if (latest_group_uri.is_invalid()) {
    return;
  }

  // V1 groups did not have the version appended so only have 4 "_"
  // (__<timestamp>_<timestamp>_<uuid>)
  // Since 2.19, V1 groups also have the version appended so we have
  // to check for that as well
  auto part = latest_group_uri.last_path_part();
  auto underscoreCount = std::count(part.begin(), part.end(), '_');
  if (underscoreCount == 4 ||
      (underscoreCount == 5 && utils::parse::ends_with(part, "_1"))) {
    load_group_from_uri(latest_group_uri);
    return;
  }

  // V2 and newer should loop over all uris all the time to handle deletes at
  // read-time
  load_group_from_all_uris(group_directory()->group_detail_uris());
}

void Group::load_group_from_uri(const URI& uri) {
  auto timer_se = resources_.stats().start_timer("load_group_from_uri");

  auto tile = GenericTileIO::load(
      resources_,
      uri,
      0,
      *encryption_key(),
      resources_.ephemeral_memory_tracker());

  resources_.stats().add_counter("read_group_size", tile->size());

  // Deserialize
  Deserializer deserializer(tile->data(), tile->size());
  auto opt_group =
      GroupDetails::deserialize(deserializer, group_directory()->uri());

  if (opt_group.has_value()) {
    group_details_ = opt_group.value();
  }
}

void Group::load_group_from_all_uris(const std::vector<TimestampedURI>& uris) {
  auto timer_se = resources_.stats().start_timer("load_group_from_all_uris");

  std::vector<shared_ptr<Deserializer>> deserializers;
  for (auto& uri : uris) {
    auto tile = GenericTileIO::load(
        resources_,
        uri.uri_,
        0,
        *encryption_key(),
        resources_.ephemeral_memory_tracker());

    resources_.stats().add_counter("read_group_size", tile->size());

    // Deserialize
    shared_ptr<Deserializer> deserializer =
        tdb::make_shared<TileDeserializer>(HERE(), tile);
    deserializers.emplace_back(deserializer);
  }

  auto opt_group =
      GroupDetails::deserialize(deserializers, group_directory()->uri());

  if (opt_group.has_value()) {
    group_details_ = opt_group.value();
  }
}

void Group::group_open_for_writes() {
  auto timer_se = resources_.stats().start_timer("group_open_for_writes");

  load_group_details();
}

}  // namespace tiledb::sm
