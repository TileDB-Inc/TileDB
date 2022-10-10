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
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/storage_format/uri/parse_uri.h"

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
    ArrayDirectoryMode mode)
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

const URI& ArrayDirectory::uri() const {
  return uri_;
}

const std::vector<URI>& ArrayDirectory::array_schema_uris() const {
  return array_schema_uris_;
}

const URI& ArrayDirectory::latest_array_schema_uri() const {
  return latest_array_schema_uri_;
}

const std::vector<URI>& ArrayDirectory::unfiltered_fragment_uris() const {
  return unfiltered_fragment_uris_;
}

const std::vector<URI>& ArrayDirectory::array_meta_uris_to_vacuum() const {
  return array_meta_uris_to_vacuum_;
}

const std::vector<URI>& ArrayDirectory::array_meta_vac_uris_to_vacuum() const {
  return array_meta_vac_uris_to_vacuum_;
}

const std::vector<URI>& ArrayDirectory::commit_uris_to_consolidate() const {
  return commit_uris_to_consolidate_;
}

const std::vector<URI>& ArrayDirectory::commit_uris_to_vacuum() const {
  return commit_uris_to_vacuum_;
}

const std::unordered_set<std::string>&
ArrayDirectory::consolidated_commit_uris_set() const {
  return consolidated_commit_uris_set_;
}

const std::vector<URI>& ArrayDirectory::consolidated_commits_uris_to_vacuum()
    const {
  return consolidated_commits_uris_to_vacuum_;
}

const std::vector<TimestampedURI>& ArrayDirectory::array_meta_uris() const {
  return array_meta_uris_;
}

Status ArrayDirectory::load() {
  assert(!loaded_);

  std::vector<ThreadPool::Task> tasks;
  std::vector<URI> root_dir_uris;
  std::vector<URI> commits_dir_uris;
  std::vector<URI> fragment_meta_uris_v12_or_higher;

  // Lists all directories in parallel. Skipping for schema only.
  // Some processing is also done here for things that don't depend on others.
  if (mode_ != ArrayDirectoryMode::SCHEMA_ONLY) {
    // List (in parallel) the root directory URIs
    tasks.emplace_back(tp_->execute([&]() {
      auto&& [st, uris] = list_root_dir_uris();
      RETURN_NOT_OK(st);

      root_dir_uris = std::move(uris.value());

      return Status::Ok();
    }));

    // List (in parallel) the commits directory URIs
    tasks.emplace_back(tp_->execute([&]() {
      auto&& [st, uris] = list_commits_dir_uris();
      RETURN_NOT_OK(st);

      commits_dir_uris = std::move(uris.value());

      return Status::Ok();
    }));

    // For commits mode, no need to load fragment/array metadata as they
    // are not used for commits consolidation/vacuuming.
    if (mode_ != ArrayDirectoryMode::COMMITS) {
      // Load (in parallel) the fragment metadata directory URIs
      tasks.emplace_back(tp_->execute([&]() {
        auto&& [st, fragment_meta_uris] =
            load_fragment_metadata_dir_uris_v12_or_higher();
        RETURN_NOT_OK(st);
        fragment_meta_uris_v12_or_higher =
            std::move(fragment_meta_uris.value());

        return Status::Ok();
      }));

      // Load (in parallel) the array metadata URIs
      tasks.emplace_back(
          tp_->execute([&]() { return load_array_meta_uris(); }));
    }
  }

  // No need to load array schemas for commits mode as they are not used for
  // commits consolidation/vacuuming.
  if (mode_ != ArrayDirectoryMode::COMMITS) {
    // Load (in parallel) the array schema URIs
    tasks.emplace_back(
        tp_->execute([&]() { return load_array_schema_uris(); }));
  }

  // Wait for all tasks to complete
  RETURN_NOT_OK(tp_->wait_all(tasks));

  if (mode_ != ArrayDirectoryMode::COMMITS) {
    // Add old array schema, if required.
    if (mode_ != ArrayDirectoryMode::SCHEMA_ONLY) {
      auto old_schema_uri = uri_.join_path(constants::array_schema_filename);
      for (auto& uri : root_dir_uris) {
        if (uri == old_schema_uri) {
          array_schema_uris_.insert(array_schema_uris_.begin(), old_schema_uri);
        }
      }
    }

    // Error check
    if (array_schema_uris_.empty()) {
      return LOG_STATUS(Status_ArrayDirectoryError(
          "Cannot open array; Array does not exist."));
    }

    // Set the latest array schema URI
    latest_array_schema_uri_ = array_schema_uris_.back();
    assert(!latest_array_schema_uri_.is_invalid());
  }

  // Process the rest of the data that has dependencies between each other
  // sequentially. Again skipping for schema only.
  if (mode_ != ArrayDirectoryMode::SCHEMA_ONLY) {
    // Load consolidated commit URIs.
    auto&& [st1, consolidated_commit_uris, consolidated_commit_uris_set] =
        load_consolidated_commit_uris(commits_dir_uris);
    RETURN_NOT_OK(st1);
    consolidated_commit_uris_set_ =
        std::move(consolidated_commit_uris_set.value());

    // For consolidation/vacuuming of commits file, we only need to load
    // the files to be consolidated/vacuumed.
    if (mode_ == ArrayDirectoryMode::COMMITS) {
      load_commits_uris_to_consolidate(
          root_dir_uris,
          commits_dir_uris,
          consolidated_commit_uris.value(),
          consolidated_commit_uris_set_);
    } else {
      // Process root dir.
      auto&& [st2, fragment_uris_v1_v11] =
          load_root_dir_uris_v1_v11(root_dir_uris);
      RETURN_NOT_OK(st2);

      // Process commit dir.
      auto&& [st3, fragment_uris_v12_or_higher] =
          load_commits_dir_uris_v12_or_higher(
              commits_dir_uris, consolidated_commit_uris.value());
      RETURN_NOT_OK(st3);

      // Append the two fragment URI vectors together
      unfiltered_fragment_uris_ = std::move(fragment_uris_v1_v11.value());
      unfiltered_fragment_uris_.insert(
          unfiltered_fragment_uris_.end(),
          fragment_uris_v12_or_higher.value().begin(),
          fragment_uris_v12_or_higher.value().end());

      // Merge the fragment meta URIs.
      std::copy(
          fragment_meta_uris_v12_or_higher.begin(),
          fragment_meta_uris_v12_or_higher.end(),
          std::back_inserter(fragment_meta_uris_));

      // Sort the delete/update commits by timestamp. Delete and update tiles
      // locations come from both the consolidated file and the directory
      // listing, and they might have interleaved times, so we need to sort.
      std::sort(
          delete_and_update_tiles_location_.begin(),
          delete_and_update_tiles_location_.end());
    }
  }

  // The URI manager has been loaded successfully
  loaded_ = true;

  return Status::Ok();
}

const ArrayDirectory::FilteredFragmentUris
ArrayDirectory::filtered_fragment_uris(const bool full_overlap_only) const {
  // Compute fragment URIs and the vacuum file URIs to vacuum
  auto&& [st, fragment_uris_to_vacuum, fragment_vac_uris_to_vacuum] =
      compute_uris_to_vacuum(full_overlap_only, unfiltered_fragment_uris_);
  if (!st.ok()) {
    throw std::logic_error(st.message());
  }

  // Compute commit URIs to vacuum, which only need to be done for fragment
  // vacuuming mode.
  std::vector<URI> commit_uris_to_vacuum;
  std::vector<URI> commit_uris_to_ignore;
  if (mode_ == ArrayDirectoryMode::VACUUM_FRAGMENTS) {
    for (auto& uri : fragment_uris_to_vacuum.value()) {
      auto commit_uri = get_commit_uri(uri);
      if (consolidated_commit_uris_set_.count(commit_uri.c_str()) == 0) {
        commit_uris_to_vacuum.emplace_back(commit_uri);
      } else {
        commit_uris_to_ignore.emplace_back(commit_uri);
      }
    }
  }

  // Compute filtered fragment URIs
  auto&& [st2, fragment_filtered_uris] = compute_filtered_uris(
      full_overlap_only,
      unfiltered_fragment_uris_,
      fragment_uris_to_vacuum.value());
  if (!st2.ok()) {
    throw std::logic_error(st.message());
  }

  return FilteredFragmentUris(
      std::move(fragment_uris_to_vacuum.value()),
      std::move(commit_uris_to_vacuum),
      std::move(commit_uris_to_ignore),
      std::move(fragment_vac_uris_to_vacuum.value()),
      std::move(fragment_filtered_uris.value()));
}

const std::vector<URI>& ArrayDirectory::fragment_meta_uris() const {
  return fragment_meta_uris_;
}

const std::vector<ArrayDirectory::DeleteAndUpdateTileLocation>&
ArrayDirectory::delete_and_update_tiles_location() const {
  return delete_and_update_tiles_location_;
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

  return uri_.join_path(constants::array_commits_dir_name);
}

URI ArrayDirectory::get_commit_uri(const URI& fragment_uri) const {
  auto name = fragment_uri.remove_trailing_slash().last_path_part();
  uint32_t version;
  throw_if_not_ok(utils::parse::get_fragment_version(name, &version));

  if (version == UINT32_MAX || version < 12) {
    return URI(fragment_uri.to_string() + constants::ok_file_suffix);
  }

  auto temp_uri =
      uri_.join_path(constants::array_commits_dir_name).join_path(name);
  return URI(temp_uri.to_string() + constants::write_file_suffix);
}

URI ArrayDirectory::get_vacuum_uri(const URI& fragment_uri) const {
  auto name = fragment_uri.remove_trailing_slash().last_path_part();
  uint32_t version;
  throw_if_not_ok(utils::parse::get_fragment_version(name, &version));

  if (version == UINT32_MAX || version < 12) {
    return URI(fragment_uri.to_string() + constants::vacuum_file_suffix);
  }

  auto temp_uri =
      uri_.join_path(constants::array_commits_dir_name).join_path(name);
  return URI(temp_uri.to_string() + constants::vacuum_file_suffix);
}

tuple<Status, optional<std::string>> ArrayDirectory::compute_new_fragment_name(
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

bool ArrayDirectory::loaded() const {
  return loaded_;
}

/* ********************************* */
/*         PRIVATE METHODS           */
/* ********************************* */

tuple<Status, optional<std::vector<URI>>> ArrayDirectory::list_root_dir_uris() {
  // List the array directory URIs
  std::vector<URI> array_dir_uris;
  RETURN_NOT_OK_TUPLE(vfs_->ls(uri_, &array_dir_uris), nullopt);

  return {Status::Ok(), array_dir_uris};
}

tuple<Status, optional<std::vector<URI>>>
ArrayDirectory::load_root_dir_uris_v1_v11(
    const std::vector<URI>& root_dir_uris) {
  // Compute the fragment URIs
  auto&& [st1, fragment_uris] = compute_fragment_uris_v1_v11(root_dir_uris);
  RETURN_NOT_OK_TUPLE(st1, nullopt);

  fragment_meta_uris_ = compute_fragment_meta_uris(root_dir_uris);

  return {Status::Ok(), fragment_uris.value()};
}

tuple<Status, optional<std::vector<URI>>>
ArrayDirectory::list_commits_dir_uris() {
  // List the commits folder array directory URIs
  auto commits_uri = uri_.join_path(constants::array_commits_dir_name);
  std::vector<URI> commits_dir_uris;
  RETURN_NOT_OK_TUPLE(vfs_->ls(commits_uri, &commits_dir_uris), nullopt);

  return {Status::Ok(), commits_dir_uris};
}

tuple<Status, optional<std::vector<URI>>>
ArrayDirectory::load_commits_dir_uris_v12_or_higher(
    const std::vector<URI>& commits_dir_uris,
    const std::vector<URI>& consolidated_uris) {
  std::vector<URI> fragment_uris;
  // Find the commited fragments from consolidated commits URIs
  for (size_t i = 0; i < consolidated_uris.size(); ++i) {
    if (stdx::string::ends_with(
            consolidated_uris[i].to_string(), constants::write_file_suffix)) {
      auto name = consolidated_uris[i].last_path_part();
      name = name.substr(0, name.size() - constants::write_file_suffix.size());
      fragment_uris.emplace_back(
          uri_.join_path(constants::array_fragments_dir_name).join_path(name));
    }
  }

  // Find the commited fragments
  for (size_t i = 0; i < commits_dir_uris.size(); ++i) {
    if (stdx::string::ends_with(
            commits_dir_uris[i].to_string(), constants::write_file_suffix)) {
      if (consolidated_commit_uris_set_.count(commits_dir_uris[i].c_str()) ==
          0) {
        auto name = commits_dir_uris[i].last_path_part();
        name =
            name.substr(0, name.size() - constants::write_file_suffix.size());
        fragment_uris.emplace_back(
            uri_.join_path(constants::array_fragments_dir_name)
                .join_path(name));
      }
    } else if (is_vacuum_file(commits_dir_uris[i])) {
      fragment_uris.emplace_back(commits_dir_uris[i]);
    } else if (
        stdx::string::ends_with(
            commits_dir_uris[i].to_string(), constants::delete_file_suffix) ||
        stdx::string::ends_with(
            commits_dir_uris[i].to_string(), constants::update_file_suffix)) {
      // Get the start and end timestamp for this delete/update
      std::pair<uint64_t, uint64_t> timestamp_range;
      RETURN_NOT_OK_TUPLE(
          utils::parse::get_timestamp_range(
              commits_dir_uris[i], &timestamp_range),
          nullopt);

      // Add the delete tile location if it overlaps the open start/end times
      if (timestamps_overlap(timestamp_range, false)) {
        if (consolidated_commit_uris_set_.count(commits_dir_uris[i].c_str()) ==
            0) {
          const auto base_uri_size = uri_.to_string().size();
          delete_and_update_tiles_location_.emplace_back(
              commits_dir_uris[i],
              commits_dir_uris[i].to_string().substr(base_uri_size),
              0);
        }
      }
    }
  }

  return {Status::Ok(), fragment_uris};
}

tuple<Status, optional<std::vector<URI>>>
ArrayDirectory::load_fragment_metadata_dir_uris_v12_or_higher() {
  // List the fragment metadata directory URIs
  auto fragment_metadata_uri =
      uri_.join_path(constants::array_fragment_meta_dir_name);

  std::vector<URI> ret;
  RETURN_NOT_OK_TUPLE(vfs_->ls(fragment_metadata_uri, &ret), nullopt);

  return {Status::Ok(), ret};
}

tuple<
    Status,
    optional<std::vector<URI>>,
    optional<std::unordered_set<std::string>>>
ArrayDirectory::load_consolidated_commit_uris(
    const std::vector<URI>& commits_dir_uris) {
  // Load the commit URIs to ignore
  std::unordered_set<std::string> ignore_set;
  for (auto& uri : commits_dir_uris) {
    if (stdx::string::ends_with(
            uri.to_string(), constants::ignore_file_suffix)) {
      uint64_t size = 0;
      RETURN_NOT_OK_TUPLE(vfs_->file_size(uri, &size), nullopt, nullopt);
      std::string names;
      names.resize(size);
      RETURN_NOT_OK_TUPLE(
          vfs_->read(uri, 0, &names[0], size), nullopt, nullopt);
      std::stringstream ss(names);
      for (std::string uri_str; std::getline(ss, uri_str);) {
        ignore_set.emplace(uri_str);
      }
    }
  }

  // Load all commit URIs
  std::unordered_set<std::string> uris_set;
  std::vector<std::pair<URI, std::string>> meta_files;
  for (uint64_t i = 0; i < commits_dir_uris.size(); i++) {
    auto& uri = commits_dir_uris[i];
    if (stdx::string::ends_with(
            uri.to_string(), constants::con_commits_file_suffix)) {
      uint64_t size = 0;
      RETURN_NOT_OK_TUPLE(vfs_->file_size(uri, &size), nullopt, nullopt);
      meta_files.emplace_back(uri, std::string());

      auto& names = meta_files.back().second;
      names.resize(size);
      RETURN_NOT_OK_TUPLE(
          vfs_->read(uri, 0, &names[0], size), nullopt, nullopt);
      std::stringstream ss(names);
      for (std::string condition_marker; std::getline(ss, condition_marker);) {
        if (ignore_set.count(condition_marker) == 0) {
          uris_set.emplace(uri_.to_string() + condition_marker);
        }

        // If we have a delete, process the condition tile
        if (stdx::string::ends_with(
                condition_marker, constants::delete_file_suffix)) {
          storage_size_t size = 0;
          ss.read(
              static_cast<char*>(static_cast<void*>(&size)),
              sizeof(storage_size_t));
          auto pos = ss.tellg();

          // Get the start and end timestamp for this delete
          std::pair<uint64_t, uint64_t> delete_timestamp_range;
          RETURN_NOT_OK_TUPLE(
              utils::parse::get_timestamp_range(
                  URI(condition_marker), &delete_timestamp_range),
              nullopt,
              nullopt);

          // Add the delete tile location if it overlaps the open start/end
          // times
          if (timestamps_overlap(delete_timestamp_range, false)) {
            delete_and_update_tiles_location_.emplace_back(
                uri, condition_marker, pos);
          }
          pos += size;
          ss.seekg(pos);
        }
      }
    }
  }

  // Make a sorted vector from the set
  std::vector<URI> uris;
  uris.reserve(uris_set.size());
  for (auto& uri : uris_set) {
    uris.emplace_back(uri);
  }
  std::sort(uris.begin(), uris.end());

  // See if we have a file that contains all URIs, which means we can vacuum.
  if (mode_ == ArrayDirectoryMode::COMMITS) {
    for (auto& meta_file : meta_files) {
      std::stringstream ss(meta_file.second);
      uint64_t count = 0;
      for (std::string uri_str; std::getline(ss, uri_str);) {
        count += uris_set.count(uri_.to_string() + uri_str);
      }

      if (count == uris_set.size()) {
        for (auto& uri : commits_dir_uris) {
          if (stdx::string::ends_with(
                  uri.to_string(), constants::con_commits_file_suffix)) {
            if (uri != meta_file.first) {
              consolidated_commits_uris_to_vacuum_.emplace_back(uri);
            }
          }

          if (stdx::string::ends_with(
                  uri.to_string(), constants::ignore_file_suffix)) {
            consolidated_commits_uris_to_vacuum_.emplace_back(uri);
          }
        }
        break;
      }
    }
  }

  return {Status::Ok(), uris, uris_set};
}

Status ArrayDirectory::load_array_meta_uris() {
  // Load the URIs in the array metadata directory
  std::vector<URI> array_meta_dir_uris;
  auto array_meta_uri = uri_.join_path(constants::array_metadata_dir_name);
  RETURN_NOT_OK(vfs_->ls(array_meta_uri, &array_meta_dir_uris));

  // Compute and array metadata URIs and the vacuum file URIs to vacuum. */
  auto&& [st1, array_meta_uris_to_vacuum, array_meta_vac_uris_to_vacuum] =
      compute_uris_to_vacuum(true, array_meta_dir_uris);
  RETURN_NOT_OK(st1);
  array_meta_uris_to_vacuum_ = std::move(array_meta_uris_to_vacuum.value());
  array_meta_vac_uris_to_vacuum_ =
      std::move(array_meta_vac_uris_to_vacuum.value());

  // Compute filtered array metadata URIs
  auto&& [st2, array_meta_filtered_uris] = compute_filtered_uris(
      true, array_meta_dir_uris, array_meta_uris_to_vacuum_);
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

void ArrayDirectory::load_commits_uris_to_consolidate(
    const std::vector<URI>& array_dir_uris,
    const std::vector<URI>& commits_dir_uris,
    const std::vector<URI>& consolidated_uris,
    const std::unordered_set<std::string>& consolidated_uris_set) {
  // Make a set of existing commit URIs.
  std::unordered_set<std::string> uris_set;
  for (auto& uri : array_dir_uris) {
    uris_set.emplace(uri.to_string());
  }
  for (auto& uri : commits_dir_uris) {
    uris_set.emplace(uri.to_string());
  }

  // Save the commit files to vacuum.
  for (auto& uri : consolidated_uris) {
    if (uris_set.count(uri.c_str()) != 0) {
      commit_uris_to_vacuum_.emplace_back(uri);
    }
  }

  // Start the list with values in the meta files.
  commit_uris_to_consolidate_ = consolidated_uris;

  // Add the ok file URIs not already in the list.
  for (auto& uri : array_dir_uris) {
    if (stdx::string::ends_with(uri.to_string(), constants::ok_file_suffix)) {
      if (consolidated_uris_set.count(uri.c_str()) == 0) {
        commit_uris_to_consolidate_.emplace_back(uri);
      }
    }
  }

  // Add the wrt file URIs not already in the list.
  for (auto& uri : commits_dir_uris) {
    if (stdx::string::ends_with(
            uri.to_string(), constants::write_file_suffix) ||
        stdx::string::ends_with(
            uri.to_string(), constants::delete_file_suffix)) {
      if (consolidated_uris_set.count(uri.c_str()) == 0) {
        commit_uris_to_consolidate_.emplace_back(uri);
      }
    }
  }
}

tuple<Status, optional<std::vector<URI>>>
ArrayDirectory::compute_fragment_uris_v1_v11(
    const std::vector<URI>& array_dir_uris) const {
  std::vector<URI> fragment_uris;

  // That fragments are "committed" for versions >= 5
  std::unordered_set<std::string> ok_uris;
  for (size_t i = 0; i < array_dir_uris.size(); ++i) {
    if (stdx::string::ends_with(
            array_dir_uris[i].to_string(), constants::ok_file_suffix)) {
      auto name = array_dir_uris[i].to_string();
      name = name.substr(0, name.size() - constants::ok_file_suffix.size());
      ok_uris.emplace(name);
    }
  }

  // Get only the committed fragment uris
  std::vector<uint8_t> is_fragment(array_dir_uris.size(), 0);
  auto status = parallel_for(tp_, 0, array_dir_uris.size(), [&](size_t i) {
    if (stdx::string::starts_with(array_dir_uris[i].last_path_part(), "."))
      return Status::Ok();
    int32_t flag;
    RETURN_NOT_OK(this->is_fragment(
        array_dir_uris[i], ok_uris, consolidated_commit_uris_set_, &flag));
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

std::vector<URI> ArrayDirectory::compute_fragment_meta_uris(
    const std::vector<URI>& array_dir_uris) {
  std::vector<URI> ret;

  // Get the consolidated fragment metadata URIs
  for (const auto& uri : array_dir_uris) {
    if (stdx::string::ends_with(uri.to_string(), constants::meta_file_suffix)) {
      ret.emplace_back(uri);
    }
  }

  return ret;
}

bool ArrayDirectory::timestamps_overlap(
    const std::pair<uint64_t, uint64_t> fragment_timestamp_range,
    const bool consolidation_with_timestamps) const {
  auto fragment_timestamp_start = fragment_timestamp_range.first;
  auto fragment_timestamp_end = fragment_timestamp_range.second;

  if (!consolidation_with_timestamps) {
    // True if the fragment falls fully within start and end times
    auto full_overlap = fragment_timestamp_start >= timestamp_start_ &&
                        fragment_timestamp_end <= timestamp_end_;
    return full_overlap;
  } else {
    // When consolidated fragment has timestamps, true if there is even partial
    // overlap
    auto any_overlap = fragment_timestamp_start <= timestamp_end_ &&
                       timestamp_start_ <= fragment_timestamp_end;
    return any_overlap;
  }
}

tuple<Status, optional<std::vector<URI>>, optional<std::vector<URI>>>
ArrayDirectory::compute_uris_to_vacuum(
    const bool full_overlap_only, const std::vector<URI>& uris) const {
  // Get vacuum URIs
  std::vector<URI> vac_files;
  std::unordered_set<std::string> non_vac_uris_set;
  std::unordered_map<std::string, size_t> uris_map;
  for (size_t i = 0; i < uris.size(); ++i) {
    // Get the start and end timestamp for this fragment
    std::pair<uint64_t, uint64_t> fragment_timestamp_range;
    RETURN_NOT_OK_TUPLE(
        utils::parse::get_timestamp_range(uris[i], &fragment_timestamp_range),
        nullopt,
        nullopt);
    if (is_vacuum_file(uris[i])) {
      if (timestamps_overlap(
              fragment_timestamp_range,
              !full_overlap_only &&
                  consolidation_with_timestamps_supported(uris[i]))) {
        vac_files.emplace_back(uris[i]);
      }
    } else {
      if (!timestamps_overlap(
              fragment_timestamp_range,
              !full_overlap_only &&
                  consolidation_with_timestamps_supported(uris[i]))) {
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
    const bool full_overlap_only,
    const std::vector<URI>& uris,
    const std::vector<URI>& to_ignore) const {
  std::vector<TimestampedURI> filtered_uris;

  // Do nothing if there are not enough URIs
  if (uris.empty())
    return {Status::Ok(), filtered_uris};

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

    // Get the start and end timestamp for this fragment
    std::pair<uint64_t, uint64_t> fragment_timestamp_range;
    RETURN_NOT_OK_TUPLE(
        utils::parse::get_timestamp_range(uri, &fragment_timestamp_range),
        nullopt);
    if (timestamps_overlap(
            fragment_timestamp_range,
            !full_overlap_only &&
                consolidation_with_timestamps_supported(uri))) {
      filtered_uris.emplace_back(uri, fragment_timestamp_range);
    }
  }

  // Sort the names based on the timestamps
  std::sort(filtered_uris.begin(), filtered_uris.end());

  return {Status::Ok(), filtered_uris};
}

Status ArrayDirectory::compute_array_schema_uris(
    const std::vector<URI>& array_schema_dir_uris) {
  if (mode_ == ArrayDirectoryMode::SCHEMA_ONLY) {
    // If not in schema only mode, this is done using the listing from the root
    // dir.
    // Optionally add the old array schema from the root array folder
    auto old_schema_uri = uri_.join_path(constants::array_schema_filename);
    bool has_file = false;
    RETURN_NOT_OK(vfs_->is_file(old_schema_uri, &has_file));
    if (has_file) {
      array_schema_uris_.push_back(old_schema_uri);
    }
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

  return Status::Ok();
}

bool ArrayDirectory::is_vacuum_file(const URI& uri) const {
  if (utils::parse::ends_with(uri.to_string(), constants::vacuum_file_suffix))
    return true;

  return false;
}

Status ArrayDirectory::is_fragment(
    const URI& uri,
    const std::unordered_set<std::string>& ok_uris_set,
    const std::unordered_set<std::string>& consolidated_uris_set,
    int* is_fragment) const {
  // If the URI name has a suffix, then it is not a fragment
  auto name = uri.remove_trailing_slash().last_path_part();
  if (name.find_first_of('.') != std::string::npos) {
    *is_fragment = 0;
    return Status::Ok();
  }

  // Exclude all possible known folders
  if (name == constants::array_schema_dir_name ||
      name == constants::array_commits_dir_name ||
      name == constants::array_metadata_dir_name ||
      name == constants::array_fragments_dir_name ||
      name == constants::array_fragment_meta_dir_name) {
    *is_fragment = 0;
    return Status::Ok();
  }

  // Check set membership in ok_uris
  if (ok_uris_set.count(uri.c_str()) != 0) {
    *is_fragment = 1;
    return Status::Ok();
  }

  // Check set membership in consolidated uris
  if (consolidated_uris_set.count(
          uri.to_string() + constants::ok_file_suffix) != 0) {
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

bool ArrayDirectory::consolidation_with_timestamps_supported(
    const URI& uri) const {
  // Get the fragment version from the uri
  uint32_t version;
  auto name = uri.remove_trailing_slash().last_path_part();
  utils::parse::get_fragment_version(name, &version);

  // get_fragment_version returns UINT32_MAX for versions <= 2 so we should
  // explicitly exclude this case when checking if consolidation with timestamps
  // is supported on a fragment
  return mode_ == ArrayDirectoryMode::READ &&
         version >= constants::consolidation_with_timestamps_min_version &&
         version != UINT32_MAX;
}
}  // namespace sm
}  // namespace tiledb
