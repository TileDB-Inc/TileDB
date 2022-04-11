/**
 * @file   group_directory.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements class GroupDirectory.
 */

#include "tiledb/sm/group/group_directory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

GroupDirectory::GroupDirectory(
    VFS* vfs,
    ThreadPool* tp,
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    GroupDirectoryMode mode)
    : uri_(uri.add_trailing_slash())
    , vfs_(vfs)
    , tp_(tp)
    , timestamp_start_(timestamp_start)
    , timestamp_end_(timestamp_end)
    , mode_(mode)
    , loaded_(false) {
  auto st = load();
  if (!st.ok()) {
    throw std::logic_error(st.message());
  }
}

/* ********************************* */
/*                API                */
/* ********************************* */

const URI& GroupDirectory::uri() const {
  return uri_;
}

/** Returns the latest array schema URI. */
const URI& GroupDirectory::latest_group_details_uri() const {
  return latest_group_details_uri_;
}

const std::vector<URI>& GroupDirectory::group_meta_uris_to_vacuum() const {
  return group_meta_uris_to_vacuum_;
}

const std::vector<URI>& GroupDirectory::group_meta_vac_uris_to_vacuum() const {
  return group_meta_vac_uris_to_vacuum_;
}

const std::vector<TimestampedURI>& GroupDirectory::group_meta_uris() const {
  return group_meta_uris_;
}

/** Returns the URIs of the group metadata files to vacuum. */
const std::vector<URI>& GroupDirectory::group_detail_uris_to_vacuum() const {
  return group_detail_uris_to_vacuum_;
}

/** Returns the URIs of the group metadata vacuum files to vacuum. */
const std::vector<URI>& GroupDirectory::group_detail_vac_uris_to_vacuum()
    const {
  return group_detail_vac_uris_to_vacuum_;
}

/** Returns the filtered group metadata URIs. */
const std::vector<TimestampedURI>& GroupDirectory::group_detail_uris() const {
  return group_detail_uris_;
}

Status GroupDirectory::load() {
  assert(!loaded_);
  // We use mode here to avoid warning on errors
  // Mode will be used for consolidation settings
  (void)mode_;

  std::vector<ThreadPool::Task> tasks;
  std::vector<URI> root_dir_uris;
  std::vector<URI> commits_dir_uris;

  // Lists all directories in parallel. Skipping for schema only.
  // Some processing is also done here for things that don't depend on others.
  // List (in parallel) the root directory URIs
  tasks.emplace_back(tp_->execute([&]() {
    auto&& [st, uris] = list_root_dir_uris();
    RETURN_NOT_OK(st);

    root_dir_uris = std::move(uris.value());

    return Status::Ok();
  }));

  // Load (in parallel) the group metadata URIs
  tasks.emplace_back(tp_->execute([&]() { return load_group_meta_uris(); }));

  // Load (in paralell) the group details URIs
  tasks.emplace_back(tp_->execute([&] { return load_group_detail_uris(); }));

  // Wait for all tasks to complete
  RETURN_NOT_OK(tp_->wait_all(tasks));

  // The URI manager has been loaded successfully
  loaded_ = true;

  return Status::Ok();
}

tuple<Status, optional<std::string>> GroupDirectory::compute_new_fragment_name(
    const URI& first, const URI& last, uint32_t format_version) const {
  // Get uuid
  std::string uuid;
  RETURN_NOT_OK_TUPLE(uuid::generate_uuid(&uuid, false), nullopt);

  // For creating the new fragment URI

  // Get timestamp ranges
  std::pair<uint64_t, uint64_t> t_first, t_last;
  RETURN_NOT_OK_TUPLE(
      utils::parse::get_timestamp_range(first, &t_first), nullopt);
  RETURN_NOT_OK_TUPLE(
      utils::parse::get_timestamp_range(last, &t_last), nullopt);

  // Create new URI
  std::stringstream ss;
  ss << "/__" << t_first.first << "_" << t_last.second << "_" << uuid << "_"
     << format_version;

  return {Status::Ok(), ss.str()};
}

bool GroupDirectory::loaded() const {
  return loaded_;
}

/* ********************************* */
/*         PRIVATE METHODS           */
/* ********************************* */

tuple<Status, optional<std::vector<URI>>> GroupDirectory::list_root_dir_uris() {
  // List the group directory URIs
  std::vector<URI> group_dir_uris;
  RETURN_NOT_OK_TUPLE(vfs_->ls(uri_, &group_dir_uris), nullopt);

  return {Status::Ok(), group_dir_uris};
}

Status GroupDirectory::load_group_meta_uris() {
  // Load the URIs in the group metadata directory
  std::vector<URI> group_meta_dir_uris;
  auto group_meta_uri = uri_.join_path(constants::group_metadata_dir_name);
  RETURN_NOT_OK(vfs_->ls(group_meta_uri, &group_meta_dir_uris));

  // Compute and group metadata URIs and the vacuum file URIs to vacuum.
  auto&& [st1, group_meta_uris_to_vacuum, group_meta_vac_uris_to_vacuum] =
      compute_uris_to_vacuum(group_meta_dir_uris);
  RETURN_NOT_OK(st1);
  group_meta_uris_to_vacuum_ = std::move(group_meta_uris_to_vacuum.value());
  group_meta_vac_uris_to_vacuum_ =
      std::move(group_meta_vac_uris_to_vacuum.value());

  // Compute filtered group metadata URIs
  auto&& [st2, group_meta_filtered_uris] =
      compute_filtered_uris(group_meta_dir_uris, group_meta_uris_to_vacuum_);
  RETURN_NOT_OK(st2);
  group_meta_uris_ = std::move(group_meta_filtered_uris.value());
  group_meta_dir_uris.clear();

  return Status::Ok();
}

Status GroupDirectory::load_group_detail_uris() {
  // Load the URIs in the group details directory
  std::vector<URI> group_detail_dir_uris;
  auto group_detail_uri = uri_.join_path(constants::group_detail_dir_name);
  RETURN_NOT_OK(vfs_->ls(group_detail_uri, &group_detail_dir_uris));

  // Compute and group details URIs and the vacuum file URIs to vacuum.
  auto&& [st1, group_detail_uris_to_vacuum, group_detail_vac_uris_to_vacuum] =
      compute_uris_to_vacuum(group_detail_dir_uris);
  RETURN_NOT_OK(st1);
  group_detail_uris_to_vacuum_ = std::move(group_detail_uris_to_vacuum.value());
  group_detail_vac_uris_to_vacuum_ =
      std::move(group_detail_vac_uris_to_vacuum.value());

  // Compute filtered group details URIs
  auto&& [st2, group_detail_filtered_uris] = compute_filtered_uris(
      group_detail_dir_uris, group_detail_uris_to_vacuum_);
  RETURN_NOT_OK(st2);
  group_detail_uris_ = std::move(group_detail_filtered_uris.value());
  group_detail_dir_uris.clear();

  // Set the latest array schema URI
  if (!group_detail_uris_.empty()) {
    latest_group_details_uri_ = group_detail_uris_.back().uri_;
    assert(!latest_group_details_uri_.is_invalid());
  }

  return Status::Ok();
}

tuple<Status, optional<std::vector<URI>>, optional<std::vector<URI>>>
GroupDirectory::compute_uris_to_vacuum(const std::vector<URI>& uris) const {
  // Get vacuum URIs
  std::vector<URI> vac_files;
  std::unordered_set<std::string> non_vac_uris_set;
  std::unordered_map<std::string, size_t> uris_map;
  for (size_t i = 0; i < uris.size(); ++i) {
    std::pair<uint64_t, uint64_t> timestamp_range;
    RETURN_NOT_OK_TUPLE(
        utils::parse::get_timestamp_range(uris[i], &timestamp_range),
        nullopt,
        nullopt);

    if (is_vacuum_file(uris[i])) {
      if (timestamp_range.first >= timestamp_start_ &&
          timestamp_range.second <= timestamp_end_)
        vac_files.emplace_back(uris[i]);
    } else {
      if (timestamp_range.first < timestamp_start_ ||
          timestamp_range.second > timestamp_end_) {
        non_vac_uris_set.emplace(uris[i].to_string());
      } else {
        uris_map[uris[i].to_string()] = i;
      }
    }
  }

  // Compute fragment URIs to vacuum as a bitmap vector
  // Also determine which vac files to vacuum
  std::vector<int32_t> to_vacuum_vec(uris.size(), 0);
  std::vector<int32_t> to_vacuum_vac_files_vec(vac_files.size(), 0);
  auto status = parallel_for(tp_, 0, vac_files.size(), [&](size_t i) {
    uint64_t size = 0;
    RETURN_NOT_OK(vfs_->file_size(vac_files[i], &size));
    std::string names;
    names.resize(size);
    RETURN_NOT_OK(vfs_->read(vac_files[i], 0, &names[0], size));
    std::stringstream ss(names);
    bool vacuum_vac_file = true;
    for (std::string uri_str; std::getline(ss, uri_str);) {
      auto it = uris_map.find(uri_str);
      if (it != uris_map.end())
        to_vacuum_vec[it->second] = 1;

      if (vacuum_vac_file &&
          non_vac_uris_set.find(uri_str) != non_vac_uris_set.end()) {
        vacuum_vac_file = false;
      }
    }

    to_vacuum_vac_files_vec[i] = vacuum_vac_file;

    return Status::Ok();
  });
  RETURN_NOT_OK_TUPLE(status, nullopt, nullopt);

  // Compute the fragment URIs to vacuum
  std::vector<URI> uris_to_vacuum;
  for (size_t i = 0; i < uris.size(); ++i) {
    if (to_vacuum_vec[i] == 1)
      uris_to_vacuum.emplace_back(uris[i]);
  }

  // Compute the vacuum URIs to vacuum
  std::vector<URI> vac_uris_to_vacuum;
  for (size_t i = 0; i < vac_files.size(); ++i) {
    if (to_vacuum_vac_files_vec[i] == 1)
      vac_uris_to_vacuum.emplace_back(vac_files[i]);
  }

  return {Status::Ok(), uris_to_vacuum, vac_uris_to_vacuum};
}

tuple<Status, optional<std::vector<TimestampedURI>>>
GroupDirectory::compute_filtered_uris(
    const std::vector<URI>& uris, const std::vector<URI>& to_ignore) const {
  std::vector<TimestampedURI> filtered_uris;

  // Do nothing if there are not enough URIs
  if (uris.empty()) {
    return {Status::Ok(), filtered_uris};
  }

  // Get the URIs that must be ignored
  std::unordered_set<std::string> to_ignore_set;
  for (const auto& uri : to_ignore)
    to_ignore_set.emplace(uri.c_str());

  // Filter based on vacuumed URIs and timestamp
  for (auto& uri : uris) {
    // Ignore vacuumed URIs
    if (to_ignore_set.count(uri.c_str()) != 0)
      continue;

    // Also ignore any vac uris
    if (is_vacuum_file(uri))
      continue;

    // Add only URIs whose first timestamp is greater than or equal to the
    // timestamp_start and whose second timestamp is smaller than or equal to
    // the timestamp_end
    std::pair<uint64_t, uint64_t> timestamp_range;
    RETURN_NOT_OK_TUPLE(
        utils::parse::get_timestamp_range(uri, &timestamp_range), nullopt);
    auto t1 = timestamp_range.first;
    auto t2 = timestamp_range.second;
    if (t1 >= timestamp_start_ && t2 <= timestamp_end_)
      filtered_uris.emplace_back(uri, timestamp_range);
  }

  // Sort the names based on the timestamps
  std::sort(filtered_uris.begin(), filtered_uris.end());

  return {Status::Ok(), filtered_uris};
}

bool GroupDirectory::is_vacuum_file(const URI& uri) const {
  if (utils::parse::ends_with(uri.to_string(), constants::vacuum_file_suffix))
    return true;

  return false;
}

}  // namespace sm
}  // namespace tiledb
