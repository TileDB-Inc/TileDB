/**
 * @file   array_directory.cc
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
 * This file implements class ArrayDirectory.
 */

#include "tiledb/sm/array/array_directory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

ArrayDirectory::ArrayDirectory(
    VFS* vfs,
    ThreadPool* tp,
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    bool only_schemas)
    : uri_(uri.add_trailing_slash())
    , vfs_(vfs)
    , tp_(tp)
    , timestamp_start_(timestamp_start)
    , timestamp_end_(timestamp_end)
    , only_schemas_(only_schemas)
    , loaded_(false) {
  auto st = load();
  if (!st.ok()) {
    throw std::logic_error(st.message());
  }
}

/* ********************************* */
/*                API                */
/* ********************************* */

const URI& ArrayDirectory::uri() const {
  return uri_;
}

const std::vector<URI>& ArrayDirectory::array_schema_uris() const {
  return array_schema_uris_;
}

const URI& ArrayDirectory::latest_array_schema_uri() const {
  return latest_array_schema_uri_;
}

const std::vector<URI>& ArrayDirectory::array_meta_uris_to_vacuum() const {
  return array_meta_uris_to_vacuum_;
}

const std::vector<URI>& ArrayDirectory::array_meta_vac_uris_to_vacuum() const {
  return array_meta_vac_uris_to_vacuum_;
}

const std::vector<TimestampedURI>& ArrayDirectory::array_meta_uris() const {
  return array_meta_uris_;
}

Status ArrayDirectory::load() {
  assert(!loaded_);

  std::vector<ThreadPool::Task> tasks;
  std::vector<URI> fragment_uris_v1_v11;
  std::vector<URI> fragment_uris_v12_or_higher;
  URI latest_fragment_meta_uri_v1_v11;
  URI latest_fragment_meta_uri_v12_or_higher;

  // Load (in parallel) the root directory data
  if (!only_schemas_)
    tasks.emplace_back(tp_->execute([&]() {
      auto&& [st, fragment_uris, latest_fragment_meta_uri] =
          load_root_dir_uris();
      RETURN_NOT_OK(st);
      fragment_uris_v1_v11 = std::move(fragment_uris.value());
      latest_fragment_meta_uri_v1_v11 =
          std::move(latest_fragment_meta_uri.value());

      return Status::Ok();
    }));

  // Load (in parallel) the commit directory data
  if (!only_schemas_)
    tasks.emplace_back(tp_->execute([&]() {
      auto&& [st, fragment_uris] = load_commit_dir_uris();
      RETURN_NOT_OK(st);
      fragment_uris_v12_or_higher = std::move(fragment_uris.value());

      return Status::Ok();
    }));

  // Load (in parallel) the fragment metadata directory data
  if (!only_schemas_)
    tasks.emplace_back(tp_->execute([&]() {
      auto&& [st, latest_fragment_meta_uri] = load_fragment_metadata_dir_uris();
      RETURN_NOT_OK(st);
      latest_fragment_meta_uri_v12_or_higher =
          std::move(latest_fragment_meta_uri.value());

      return Status::Ok();
    }));

  // Load (in parallel) the array metadata URIs
  if (!only_schemas_)
    tasks.emplace_back(tp_->execute([&]() { return load_array_meta_uris(); }));

  // Load (in parallel) the array schema URIs
  tasks.emplace_back(tp_->execute([&]() { return load_array_schema_uris(); }));

  // Wait for all tasks to complete
  RETURN_NOT_OK(tp_->wait_all(tasks));

  // Append the two fragment URI vectors together
  auto fragment_uris = std::move(fragment_uris_v1_v11);
  fragment_uris.insert(
      fragment_uris.end(),
      fragment_uris_v12_or_higher.begin(),
      fragment_uris_v12_or_higher.end());

  // Compute and fragment URIs and the vacuum file URIs to vacuum
  auto&& [st2, fragment_uris_to_vacuum, fragment_vac_uris_to_vacuum] =
      compute_uris_to_vacuum(fragment_uris);
  RETURN_NOT_OK(st2);
  fragment_uris_to_vacuum_ = std::move(fragment_uris_to_vacuum.value());
  fragment_vac_uris_to_vacuum_ = std::move(fragment_vac_uris_to_vacuum.value());

  // Compute filtered fragment URIs
  auto&& [st3, fragment_filtered_uris] =
      compute_filtered_uris(fragment_uris, fragment_uris_to_vacuum_);
  RETURN_NOT_OK(st3);
  fragment_uris_ = std::move(fragment_filtered_uris.value());
  fragment_uris.clear();

  // Set the proper fragment metadata latest
  latest_fragment_meta_uri_ = latest_fragment_meta_uri_v12_or_higher.empty() ?
                                  latest_fragment_meta_uri_v1_v11 :
                                  latest_fragment_meta_uri_v12_or_higher;

  // The URI manager has been loaded successfully
  loaded_ = true;

  return Status::Ok();
}

const std::vector<URI>& ArrayDirectory::fragment_uris_to_vacuum() const {
  return fragment_uris_to_vacuum_;
}

const std::vector<URI>& ArrayDirectory::fragment_vac_uris_to_vacuum() const {
  return fragment_vac_uris_to_vacuum_;
}

const std::vector<TimestampedURI>& ArrayDirectory::fragment_uris() const {
  return fragment_uris_;
}

const std::vector<URI>& ArrayDirectory::fragment_meta_uris() const {
  return fragment_meta_uris_;
}

const URI& ArrayDirectory::latest_fragment_meta_uri() const {
  return latest_fragment_meta_uri_;
}

URI ArrayDirectory::get_fragments_dir(uint32_t write_version) const {
  if (write_version < 12) {
    return uri_;
  }

  return uri_.join_path(constants::array_fragments_dir_name);
}

URI ArrayDirectory::get_fragment_metadata_dir(uint32_t write_version) const {
  if (write_version < 12) {
    return uri_;
  }

  return uri_.join_path(constants::array_fragment_meta_dir_name);
}

URI ArrayDirectory::get_commits_dir(uint32_t write_version) const {
  if (write_version < 12) {
    return uri_;
  }

  return uri_.join_path(constants::array_commit_dir_name);
}

tuple<Status, optional<URI>> ArrayDirectory::get_commit_uri(
    const URI& fragment_uri) const {
  auto name = fragment_uri.remove_trailing_slash().last_path_part();
  uint32_t version;
  RETURN_NOT_OK_TUPLE(
      utils::parse::get_fragment_version(name, &version), nullopt);

  if (version == UINT32_MAX || version < 12) {
    return {Status::Ok(),
            URI(fragment_uri.to_string() + constants::ok_file_suffix)};
  }

  auto temp_uri =
      uri_.join_path(constants::array_commit_dir_name).join_path(name);
  return {Status::Ok(),
          URI(temp_uri.to_string() + constants::write_file_suffix)};
}

tuple<Status, optional<URI>> ArrayDirectory::get_vaccum_uri(
    const URI& fragment_uri) const {
  auto name = fragment_uri.remove_trailing_slash().last_path_part();
  uint32_t version;
  RETURN_NOT_OK_TUPLE(
      utils::parse::get_fragment_version(name, &version), nullopt);

  if (version == UINT32_MAX || version < 12) {
    return {Status::Ok(),
            URI(fragment_uri.to_string() + constants::vacuum_file_suffix)};
  }

  auto temp_uri =
      uri_.join_path(constants::array_commit_dir_name).join_path(name);
  return {Status::Ok(),
          URI(temp_uri.to_string() + constants::vacuum_file_suffix)};
}

bool ArrayDirectory::loaded() const {
  return loaded_;
}

/* ********************************* */
/*         PRIVATE METHODS           */
/* ********************************* */

tuple<Status, optional<std::vector<URI>>, optional<URI>>
ArrayDirectory::load_root_dir_uris() {
  // List the array directory URIs
  std::vector<URI> array_dir_uris;
  RETURN_NOT_OK_TUPLE(vfs_->ls(uri_, &array_dir_uris), nullopt, nullopt);

  // Compute and store the fragment URIs
  auto&& [st1, fragment_uris] = compute_fragment_uris_v1_v11(array_dir_uris);
  RETURN_NOT_OK_TUPLE(st1, nullopt, nullopt);

  auto&& [st2, fragment_meta_uri] =
      compute_latest_fragment_meta_uri(array_dir_uris);
  RETURN_NOT_OK_TUPLE(st2, nullopt, nullopt);

  return {Status::Ok(), fragment_uris.value(), fragment_meta_uri.value()};
}

tuple<Status, optional<std::vector<URI>>>
ArrayDirectory::load_commit_dir_uris() {
  std::vector<URI> fragment_uris;

  // List the commit folder array directory URIs
  auto commit_uri = uri_.join_path(constants::array_commit_dir_name);
  std::vector<URI> commit_dir_uris;
  RETURN_NOT_OK_TUPLE(vfs_->ls(commit_uri, &commit_dir_uris), nullopt);

  // Find the commited fragments
  for (size_t i = 0; i < commit_dir_uris.size(); ++i) {
    if (stdx::string::ends_with(
            commit_dir_uris[i].to_string(), constants::write_file_suffix)) {
      auto name = commit_dir_uris[i].last_path_part();
      name = name.substr(0, name.size() - constants::write_file_suffix.size());
      fragment_uris.emplace_back(
          uri_.join_path(constants::array_fragments_dir_name).join_path(name));
    } else if (is_vacuum_file(commit_dir_uris[i])) {
      fragment_uris.emplace_back(commit_dir_uris[i]);
    }
  }

  return {Status::Ok(), fragment_uris};
}

tuple<Status, optional<URI>> ArrayDirectory::load_fragment_metadata_dir_uris() {
  // List the commit folder array directory URIs
  auto fragment_metadata_uri =
      uri_.join_path(constants::array_fragment_meta_dir_name);
  std::vector<URI> fragment_metadata_dir_uris;
  RETURN_NOT_OK_TUPLE(
      vfs_->ls(fragment_metadata_uri, &fragment_metadata_dir_uris), nullopt);

  auto&& [st, fragment_meta_uri] =
      compute_latest_fragment_meta_uri(fragment_metadata_dir_uris);
  RETURN_NOT_OK_TUPLE(st, nullopt);

  return {Status::Ok(), std::move(fragment_meta_uri.value())};
}

Status ArrayDirectory::load_array_meta_uris() {
  // Load the URIs in the array metadata directory
  std::vector<URI> array_meta_dir_uris;
  auto array_meta_uri = uri_.join_path(constants::array_metadata_dir_name);
  RETURN_NOT_OK(vfs_->ls(array_meta_uri, &array_meta_dir_uris));

  // Compute and array metadata URIs and the vacuum file URIs to vacuum. */
  auto&& [st1, array_meta_uris_to_vacuum, array_meta_vac_uris_to_vacuum] =
      compute_uris_to_vacuum(array_meta_dir_uris);
  RETURN_NOT_OK(st1);
  array_meta_uris_to_vacuum_ = std::move(array_meta_uris_to_vacuum.value());
  array_meta_vac_uris_to_vacuum_ =
      std::move(array_meta_vac_uris_to_vacuum.value());

  // Compute filtered array metadata URIs
  auto&& [st2, array_meta_filtered_uris] =
      compute_filtered_uris(array_meta_dir_uris, array_meta_uris_to_vacuum_);
  RETURN_NOT_OK(st2);
  array_meta_uris_ = std::move(array_meta_filtered_uris.value());
  array_meta_dir_uris.clear();

  return Status::Ok();
}

Status ArrayDirectory::load_array_schema_uris() {
  // Load the URIs from the array schema directory
  std::vector<URI> array_schema_dir_uris;
  auto schema_dir_uri = uri_.join_path(constants::array_schema_dir_name);
  RETURN_NOT_OK(vfs_->ls(schema_dir_uri, &array_schema_dir_uris));

  // Compute all the array schema URIs plus the latest array schema URI
  RETURN_NOT_OK(compute_array_schema_uris(array_schema_dir_uris));

  return Status::Ok();
}

tuple<Status, optional<std::vector<URI>>>
ArrayDirectory::compute_fragment_uris_v1_v11(
    const std::vector<URI>& array_dir_uris) {
  std::vector<URI> fragment_uris;

  // That fragments are "committed" for versions >= 5
  std::set<URI> ok_uris;
  for (size_t i = 0; i < array_dir_uris.size(); ++i) {
    if (stdx::string::ends_with(
            array_dir_uris[i].to_string(), constants::ok_file_suffix)) {
      auto name = array_dir_uris[i].to_string();
      name = name.substr(0, name.size() - constants::ok_file_suffix.size());
      ok_uris.emplace(URI(name));
    }
  }

  // Get only the committed fragment uris
  std::vector<uint8_t> is_fragment(array_dir_uris.size(), 0);
  auto status = parallel_for(tp_, 0, array_dir_uris.size(), [&](size_t i) {
    if (stdx::string::starts_with(array_dir_uris[i].last_path_part(), "."))
      return Status::Ok();
    int32_t flag;
    RETURN_NOT_OK(this->is_fragment(array_dir_uris[i], ok_uris, &flag));
    is_fragment[i] = (uint8_t)flag;
    return Status::Ok();
  });
  RETURN_NOT_OK_TUPLE(status, nullopt);

  for (size_t i = 0; i < array_dir_uris.size(); ++i) {
    if (is_fragment[i]) {
      fragment_uris.emplace_back(array_dir_uris[i]);
    } else if (is_vacuum_file(array_dir_uris[i])) {
      fragment_uris.emplace_back(array_dir_uris[i]);
    }
  }

  return {Status::Ok(), fragment_uris};
}

tuple<Status, optional<URI>> ArrayDirectory::compute_latest_fragment_meta_uri(
    const std::vector<URI>& array_dir_uris) {
  URI ret;

  // Get the consolidated fragment metadata URIs
  uint64_t t_latest = 0;
  std::pair<uint64_t, uint64_t> timestamp_range;
  for (const auto& uri : array_dir_uris) {
    if (stdx::string::ends_with(uri.to_string(), constants::meta_file_suffix)) {
      fragment_meta_uris_.emplace_back(uri);
      RETURN_NOT_OK_TUPLE(
          utils::parse::get_timestamp_range(uri, &timestamp_range), nullopt);
      if (timestamp_range.second > t_latest) {
        t_latest = timestamp_range.second;
        ret = uri;
      }
    }
  }

  return {Status::Ok(), ret};
}

tuple<Status, optional<std::vector<URI>>, optional<std::vector<URI>>>
ArrayDirectory::compute_uris_to_vacuum(const std::vector<URI>& uris) {
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
ArrayDirectory::compute_filtered_uris(
    const std::vector<URI>& uris, const std::vector<URI>& to_ignore) {
  std::vector<TimestampedURI> filtered_uris;

  // Do nothing if there are not enough URIs
  if (uris.empty())
    return {Status::Ok(), filtered_uris};

  // Get the URIs that must be ignored
  std::set<URI> to_ignore_set;
  for (const auto& uri : to_ignore)
    to_ignore_set.emplace(uri);

  // Filter based on vacuumed URIs and timestamp
  for (auto& uri : uris) {
    // Ignore vacuumed URIs
    if (to_ignore_set.find(uri) != to_ignore_set.end())
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

Status ArrayDirectory::compute_array_schema_uris(
    const std::vector<URI>& array_schema_dir_uris) {
  // Optionally add the old array schema from the root array folder
  auto old_schema_uri = uri_.join_path(constants::array_schema_filename);
  bool has_file = false;
  RETURN_NOT_OK(vfs_->is_file(old_schema_uri, &has_file));
  if (has_file) {
    array_schema_uris_.push_back(old_schema_uri);
  }

  // Optionally add the new array schemas from the array schema directory
  if (!array_schema_dir_uris.empty()) {
    array_schema_uris_.reserve(
        array_schema_uris_.size() + array_schema_dir_uris.size());
    std::copy(
        array_schema_dir_uris.begin(),
        array_schema_dir_uris.end(),
        std::back_inserter(array_schema_uris_));
  }

  // Error check
  if (array_schema_uris_.empty()) {
    return LOG_STATUS(Status_ArrayDirectoryError(
        "Cannot compute array schemas; No array schemas found."));
  }

  // Set the latest array schema URI
  latest_array_schema_uri_ = array_schema_uris_.back();
  assert(!latest_array_schema_uri_.is_invalid());

  return Status::Ok();
}

bool ArrayDirectory::is_vacuum_file(const URI& uri) const {
  if (utils::parse::ends_with(uri.to_string(), constants::vacuum_file_suffix))
    return true;

  return false;
}

Status ArrayDirectory::is_fragment(
    const URI& uri, const std::set<URI>& ok_uris, int* is_fragment) const {
  // If the URI name has a suffix, then it is not a fragment
  auto name = uri.remove_trailing_slash().last_path_part();
  if (name.find_first_of('.') != std::string::npos) {
    *is_fragment = 0;
    return Status::Ok();
  }

  // Check set membership in ok_uris
  if (ok_uris.find(uri) != ok_uris.end()) {
    *is_fragment = 1;
    return Status::Ok();
  }

  // If the format version is >= 5, then the above suffices to check if
  // the URI is indeed a fragment
  uint32_t version;
  RETURN_NOT_OK(utils::parse::get_fragment_version(name, &version));
  if (version != UINT32_MAX && version >= 5) {
    *is_fragment = false;
    return Status::Ok();
  }

  // Versions < 5
  bool is_file;
  RETURN_NOT_OK(vfs_->is_file(
      uri.join_path(constants::fragment_metadata_filename), &is_file));
  *is_fragment = (int)is_file;
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
