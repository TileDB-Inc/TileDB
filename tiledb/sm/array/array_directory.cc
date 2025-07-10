/**
 * @file   array_directory.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/assert.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/storage_format/uri/generate_uri.h"

#include <numeric>

using namespace tiledb::common;

namespace tiledb::sm {

/** Class for ArrayDirectory status exceptions. */
class ArrayDirectoryException : public StatusException {
 public:
  explicit ArrayDirectoryException(const std::string& msg)
      : StatusException("ArrayDirectory", msg) {
  }
};

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

ArrayDirectory::ArrayDirectory(ContextResources& resources, const URI& uri)
    : resources_(resources)
    , uri_(uri.add_trailing_slash()) {
}

ArrayDirectory::ArrayDirectory(
    ContextResources& resources,
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    ArrayDirectoryMode mode)
    : resources_(resources)
    , uri_(uri.add_trailing_slash())
    , stats_(resources_.get().vfs().stats()->create_child("ArrayDirectory"))
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

shared_ptr<ArraySchema> ArrayDirectory::load_array_schema_from_uri(
    ContextResources& resources,
    const URI& schema_uri,
    const EncryptionKey& encryption_key,
    shared_ptr<MemoryTracker> memory_tracker) {
  auto timer_se =
      resources.stats().start_timer("sm_load_array_schema_from_uri");

  auto tile = GenericTileIO::load(
      resources, schema_uri, 0, encryption_key, memory_tracker);

  resources.stats().add_counter("read_array_schema_size", tile->size());

  // Deserialize
  Deserializer deserializer(tile->data(), tile->size());
  return ArraySchema::deserialize(deserializer, schema_uri, memory_tracker);
}

shared_ptr<ArraySchema> ArrayDirectory::load_array_schema_latest(
    const EncryptionKey& encryption_key,
    shared_ptr<MemoryTracker> memory_tracker) const {
  auto timer_se =
      resources_.get().stats().start_timer("sm_load_array_schema_latest");

  if (uri_.is_invalid()) {
    throw ArrayDirectoryException(
        "Cannot load array schema; Invalid array URI");
  }

  // Load schema from URI
  const URI& schema_uri = latest_array_schema_uri();
  auto&& array_schema = load_array_schema_from_uri(
      resources_.get(), schema_uri, encryption_key, memory_tracker);

  array_schema->set_array_uri(uri_.remove_trailing_slash());

  return std::move(array_schema);
}

tuple<
    shared_ptr<ArraySchema>,
    std::unordered_map<std::string, shared_ptr<ArraySchema>>>
ArrayDirectory::load_array_schemas(
    const EncryptionKey& encryption_key,
    shared_ptr<MemoryTracker> memory_tracker) const {
  // Load all array schemas
  auto&& array_schemas = load_all_array_schemas(encryption_key, memory_tracker);

  // Locate the latest array schema
  const auto& array_schema_latest_name =
      latest_array_schema_uri().last_path_part();
  auto it = array_schemas.find(array_schema_latest_name);
  passert(
      it != array_schemas.end(),
      "Cannot locate array schema '{}'",
      array_schema_latest_name);

  return {it->second, array_schemas};
}

std::unordered_map<std::string, shared_ptr<ArraySchema>>
ArrayDirectory::load_all_array_schemas(
    const EncryptionKey& encryption_key,
    shared_ptr<MemoryTracker> memory_tracker) const {
  auto timer_se =
      resources_.get().stats().start_timer("sm_load_all_array_schemas");

  if (uri_.is_invalid()) {
    throw ArrayDirectoryException(
        "Cannot load all array schemas; Invalid array URI");
  }

  const std::vector<URI>& schema_uris = array_schema_uris();
  if (schema_uris.empty()) {
    throw ArrayDirectoryException(
        "Cannot get the array schema vector; No array schemas found.");
  }

  std::vector<shared_ptr<ArraySchema>> schema_vector;
  auto schema_num = schema_uris.size();
  schema_vector.resize(schema_num);

  auto status = parallel_for(
      &resources_.get().compute_tp(), 0, schema_num, [&](size_t schema_ith) {
        auto& schema_uri = schema_uris[schema_ith];
        try {
          auto&& array_schema = load_array_schema_from_uri(
              resources_.get(), schema_uri, encryption_key, memory_tracker);
          array_schema->set_array_uri(uri_);
          schema_vector[schema_ith] = array_schema;
        } catch (std::exception& e) {
          // TODO: We could throw a nested exception, but converting exceptions
          // to statuses loses the inner exception messages. We can revisit this
          // when Status gets removed from this module.
          throw ArrayDirectoryException(e.what());
        }

        return Status::Ok();
      });
  throw_if_not_ok(status);

  std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas;
  for (const auto& schema : schema_vector) {
    array_schemas[schema->name()] = schema;
  }

  return array_schemas;
}

std::vector<shared_ptr<const Enumeration>>
ArrayDirectory::load_enumerations_from_paths(
    const std::vector<std::string>& enumeration_paths,
    const EncryptionKey& encryption_key,
    shared_ptr<MemoryTracker> memory_tracker) const {
  // This should never be called with an empty list of enumeration paths, but
  // there's no reason to not check an early return case here given that code
  // changes.
  if (enumeration_paths.size() == 0) {
    return {};
  }

  std::vector<shared_ptr<const Enumeration>> ret(enumeration_paths.size());
  auto& tp = resources_.get().io_tp();
  throw_if_not_ok(parallel_for(&tp, 0, enumeration_paths.size(), [&](size_t i) {
    ret[i] =
        load_enumeration(enumeration_paths[i], encryption_key, memory_tracker);
    return Status::Ok();
  }));
  return ret;
}

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

const uint64_t& ArrayDirectory::timestamp_start() const {
  return timestamp_start_;
}

const uint64_t& ArrayDirectory::timestamp_end() const {
  return timestamp_end_;
}

void ArrayDirectory::write_commit_ignore_file(
    const std::vector<URI>& commit_uris_to_ignore) {
  auto name = storage_format::generate_consolidated_fragment_name(
      commit_uris_to_ignore.front(),
      commit_uris_to_ignore.back(),
      constants::format_version);

  // Write URIs, relative to the array URI.
  std::stringstream ss;
  auto base_uri_size = uri().to_string().size();
  for (const auto& uri : commit_uris_to_ignore) {
    ss << uri.to_string().substr(base_uri_size) << "\n";
  }

  // Write an ignore file to ensure consolidated WRT files still work
  auto data = ss.str();
  URI ignore_file_uri = get_commits_dir(constants::format_version)
                            .join_path(name + constants::ignore_file_suffix);
  throw_if_not_ok(
      resources_.get().vfs().write(ignore_file_uri, data.c_str(), data.size()));
  throw_if_not_ok(resources_.get().vfs().close_file(ignore_file_uri));
}

void ArrayDirectory::delete_fragments_list(
    const std::vector<URI>& fragment_uris) {
  // Get fragment URIs from the array that match the input fragment_uris
  auto filtered_uris = filtered_fragment_uris(true);
  auto uris = filtered_uris.fragment_uris(fragment_uris);

  // Retrieve commit uris to delete and ignore
  std::vector<URI> commit_uris_to_delete;
  std::vector<URI> commit_uris_to_ignore;
  for (auto& timestamped_uri : uris) {
    auto commit_uri = get_commit_uri(timestamped_uri);
    commit_uris_to_delete.emplace_back(commit_uri);
    if (consolidated_commit_uris_set().count(commit_uri.c_str()) != 0) {
      commit_uris_to_ignore.emplace_back(commit_uri);
    }
  }

  // Write ignore file
  if (commit_uris_to_ignore.size() != 0) {
    write_commit_ignore_file(commit_uris_to_ignore);
  }

  // Delete fragments and commits
  throw_if_not_ok(parallel_for(
      &resources_.get().compute_tp(), 0, uris.size(), [&](size_t i) {
        auto& vfs = resources_.get().vfs();
        throw_if_not_ok(vfs.remove_dir(uris[i]));
        bool is_file = false;
        throw_if_not_ok(vfs.is_file(commit_uris_to_delete[i], &is_file));
        if (is_file) {
          throw_if_not_ok(vfs.remove_file(commit_uris_to_delete[i]));
        }
        return Status::Ok();
      }));
}

Status ArrayDirectory::load() {
  passert(!loaded_);

  std::vector<ThreadPool::Task> tasks;
  std::vector<URI> root_dir_uris;
  std::vector<URI> commits_dir_uris;
  std::vector<URI> fragment_meta_uris_v12_or_higher;

  // Lists all directories in parallel. Skipping for schema only.
  // Some processing is also done here for things that don't depend on others.
  if (mode_ != ArrayDirectoryMode::SCHEMA_ONLY) {
    // List (in parallel) the root directory URIs
    tasks.emplace_back(resources_.get().compute_tp().execute([&]() {
      root_dir_uris = list_root_dir_uris();
      return Status::Ok();
    }));

    // List (in parallel) the commits directory URIs
    tasks.emplace_back(resources_.get().compute_tp().execute([&]() {
      commits_dir_uris = list_commits_dir_uris();
      return Status::Ok();
    }));

    // For commits mode, no need to load fragment/array metadata as they
    // are not used for commits consolidation/vacuuming.
    if (mode_ != ArrayDirectoryMode::COMMITS) {
      // Load (in parallel) the fragment metadata directory URIs
      tasks.emplace_back(resources_.get().compute_tp().execute([&]() {
        fragment_meta_uris_v12_or_higher =
            list_fragment_metadata_dir_uris_v12_or_higher();
        return Status::Ok();
      }));

      // Load (in parallel) the array metadata URIs
      tasks.emplace_back(resources_.get().compute_tp().execute([&]() {
        load_array_meta_uris();
        return Status::Ok();
      }));
    }
  }

  // No need to load array schemas for commits mode as they are not used for
  // commits consolidation/vacuuming.
  if (mode_ != ArrayDirectoryMode::COMMITS) {
    // Load (in parallel) the array schema URIs
    tasks.emplace_back(resources_.get().compute_tp().execute([&]() {
      load_array_schema_uris();
      return Status::Ok();
    }));
  }

  // Wait for all tasks to complete
  RETURN_NOT_OK(resources_.get().compute_tp().wait_all(tasks));

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

    latest_array_schema_uri_ = select_latest_array_schema_uri();
    passert(
        !latest_array_schema_uri_.is_invalid(),
        "uri = {}",
        latest_array_schema_uri_.to_string());
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

URI ArrayDirectory::generate_fragment_dir_uri(
    uint32_t write_version, URI array_uri) {
  if (write_version < 12) {
    return array_uri;
  }

  return array_uri.join_path(constants::array_fragments_dir_name);
}

URI ArrayDirectory::get_fragments_dir(uint32_t write_version) const {
  return generate_fragment_dir_uri(write_version, uri_);
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
  FragmentID fragment_id{fragment_uri};
  if (fragment_id.array_format_version() < 12) {
    return URI(fragment_uri.to_string() + constants::ok_file_suffix);
  }

  auto temp_uri = uri_.join_path(constants::array_commits_dir_name)
                      .join_path(fragment_id.name());
  return URI(temp_uri.to_string() + constants::write_file_suffix);
}

URI ArrayDirectory::get_vacuum_uri(const URI& fragment_uri) const {
  FragmentID fragment_id{fragment_uri};
  if (fragment_id.array_format_version() < 12) {
    return URI(fragment_uri.to_string() + constants::vacuum_file_suffix);
  }

  auto temp_uri = uri_.join_path(constants::array_commits_dir_name)
                      .join_path(fragment_id.name());
  return URI(temp_uri.to_string() + constants::vacuum_file_suffix);
}

bool ArrayDirectory::loaded() const {
  return loaded_;
}

/* ********************************* */
/*         PRIVATE METHODS           */
/* ********************************* */

const std::set<std::string>& ArrayDirectory::dir_names() {
  static const std::set<std::string> dir_names = {
      constants::array_schema_dir_name,
      constants::array_metadata_dir_name,
      constants::array_fragment_meta_dir_name,
      constants::array_fragments_dir_name,
      constants::array_commits_dir_name,
      constants::array_dimension_labels_dir_name};

  return dir_names;
}

std::vector<URI> ArrayDirectory::ls(const URI& uri) const {
  auto dir_entries = resources_.get().vfs().ls_with_sizes(uri);
  auto& dirs = dir_names();
  std::vector<URI> uris(dir_entries.size());

  for (auto entry : dir_entries) {
    auto entry_uri = URI(entry.path().native());

    // Always list directories
    if (entry.is_directory()) {
      uris.emplace_back(entry_uri);
      continue;
    }

    // Filter out empty files of the same name as the directory
    if (entry_uri.remove_trailing_slash() == uri.remove_trailing_slash() &&
        entry.file_size() == 0) {
      continue;
    }

    // List non-known (user-added) directory names and non-empty files
    auto iter = dirs.find(entry_uri.last_path_part());
    if (iter == dirs.end() || entry.file_size() > 0) {
      uris.emplace_back(entry_uri);
    } else {
      // Handle MinIO-based s3 implementation limitation. If an object exists
      // with the same name as a directory, the objects under the directory are
      // masked and cannot be listed. See
      // https://github.com/minio/minio/issues/7335
      throw ArrayDirectoryException(
          "Cannot list given uri; File '" + entry_uri.to_string() +
          "' may be masking a non-empty directory by the same name. Removing "
          "that file might fix this.");
    }
  }

  return uris;
}

std::vector<URI> ArrayDirectory::list_root_dir_uris() {
  // List the array directory URIs
  auto timer_se = stats_->start_timer("list_root_uris");
  return ls(uri_);
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

std::vector<URI> ArrayDirectory::list_commits_dir_uris() {
  // List the commits folder array directory URIs
  auto timer_se = stats_->start_timer("list_commit_uris");
  return ls(uri_.join_path(constants::array_commits_dir_name));
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
      FragmentID fragment_id{commits_dir_uris[i]};
      auto timestamp_range{fragment_id.timestamp_range()};

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

std::vector<URI>
ArrayDirectory::list_fragment_metadata_dir_uris_v12_or_higher() {
  // List the fragment metadata directory URIs
  auto timer_se = stats_->start_timer("list_fragment_meta_uris");
  return ls(uri_.join_path(constants::array_fragment_meta_dir_name));
}

tuple<
    Status,
    optional<std::vector<URI>>,
    optional<std::unordered_set<std::string>>>
ArrayDirectory::load_consolidated_commit_uris(
    const std::vector<URI>& commits_dir_uris) {
  auto timer_se = stats_->start_timer("load_consolidated_commit_uris");
  // Load the commit URIs to ignore. This is done in serial for now as it can be
  // optimized by vacuuming.
  std::unordered_set<std::string> ignore_set;
  for (auto& uri : commits_dir_uris) {
    if (stdx::string::ends_with(
            uri.to_string(), constants::ignore_file_suffix)) {
      uint64_t size = 0;
      RETURN_NOT_OK_TUPLE(
          resources_.get().vfs().file_size(uri, &size), nullopt, nullopt);
      std::string names;
      names.resize(size);
      RETURN_NOT_OK_TUPLE(
          resources_.get().vfs().read(uri, 0, &names[0], size, false),
          nullopt,
          nullopt);
      std::stringstream ss(names);
      for (std::string uri_str; std::getline(ss, uri_str);) {
        ignore_set.emplace(uri_str);
      }
    }
  }

  // Load all commit URIs. This is done in serial for now as it can be optimized
  // by vacuuming.
  std::unordered_set<std::string> uris_set;
  std::vector<std::pair<URI, std::string>> meta_files;
  for (uint64_t i = 0; i < commits_dir_uris.size(); i++) {
    auto& uri = commits_dir_uris[i];
    if (stdx::string::ends_with(
            uri.to_string(), constants::con_commits_file_suffix)) {
      uint64_t size = 0;
      RETURN_NOT_OK_TUPLE(
          resources_.get().vfs().file_size(uri, &size), nullopt, nullopt);
      meta_files.emplace_back(uri, std::string());

      auto& names = meta_files.back().second;
      names.resize(size);
      RETURN_NOT_OK_TUPLE(
          resources_.get().vfs().read(uri, 0, &names[0], size, false),
          nullopt,
          nullopt);
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
          FragmentID fragment_id{URI(condition_marker)};
          auto delete_timestamp_range{fragment_id.timestamp_range()};

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
      bool all_in_set = true;
      for (std::string uri_str; std::getline(ss, uri_str);) {
        if (uris_set.count(uri_.to_string() + uri_str) > 0) {
          count++;
        } else {
          all_in_set = false;
          break;
        }
      }

      if (all_in_set && count == uris_set.size()) {
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

void ArrayDirectory::load_array_meta_uris() {
  // Load the URIs in the array metadata directory
  std::vector<URI> array_meta_dir_uris;
  {
    auto timer_se = stats_->start_timer("list_array_meta_uris");
    array_meta_dir_uris =
        ls(uri_.join_path(constants::array_metadata_dir_name));
  }

  // Compute array metadata URIs and vacuum URIs to vacuum. */
  auto&& [st1, array_meta_uris_to_vacuum, array_meta_vac_uris_to_vacuum] =
      compute_uris_to_vacuum(true, array_meta_dir_uris);
  throw_if_not_ok(st1);
  array_meta_uris_to_vacuum_ = std::move(array_meta_uris_to_vacuum.value());
  array_meta_vac_uris_to_vacuum_ =
      std::move(array_meta_vac_uris_to_vacuum.value());

  // Compute filtered array metadata URIs
  auto&& [st2, array_meta_filtered_uris] = compute_filtered_uris(
      true, array_meta_dir_uris, array_meta_uris_to_vacuum_);
  throw_if_not_ok(st2);
  array_meta_uris_ = std::move(array_meta_filtered_uris.value());
  array_meta_dir_uris.clear();
}

void ArrayDirectory::load_array_schema_uris() {
  // Load the URIs from the array schema directory
  std::vector<URI> array_schema_dir_uris;
  {
    auto timer_se = stats_->start_timer("list_array_schema_uris");
    array_schema_dir_uris =
        ls(uri_.join_path(constants::array_schema_dir_name));
  }

  // Compute all the array schema URIs plus the latest array schema URI
  throw_if_not_ok(compute_array_schema_uris(array_schema_dir_uris));
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
  auto& tp = resources_.get().compute_tp();
  auto status = parallel_for(&tp, 0, array_dir_uris.size(), [&](size_t i) {
    if (stdx::string::starts_with(array_dir_uris[i].last_path_part(), "."))
      return Status::Ok();
    int32_t flag;
    throw_if_not_ok(this->is_fragment(
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

std::string ArrayDirectory::get_full_vac_uri(
    std::string base, std::string vac_uri) {
  size_t frag_pos = vac_uri.find(constants::array_fragments_dir_name);
  if (frag_pos != std::string::npos) {
    vac_uri = vac_uri.substr(frag_pos);
  } else if (
      vac_uri.find(constants::array_metadata_dir_name) != std::string::npos) {
    vac_uri = vac_uri.substr(vac_uri.find(constants::array_metadata_dir_name));
  } else {
    size_t last_slash_pos = vac_uri.find_last_of('/');
    if (last_slash_pos == std::string::npos) {
      throw std::logic_error("Invalid URI: " + vac_uri);
    }

    vac_uri = vac_uri.substr(last_slash_pos + 1);
  }

  return base + vac_uri;
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
  std::vector<uint8_t> vac_file_bitmap(uris.size());
  std::vector<uint8_t> overlapping_vac_file_bitmap(uris.size());
  std::vector<uint8_t> non_vac_uri_bitmap(uris.size());
  throw_if_not_ok(parallel_for(
      &resources_.get().compute_tp(), 0, uris.size(), [&](size_t i) {
        auto& uri = uris[i];

        // Get the start and end timestamp for this fragment
        FragmentID fragment_id{uri};
        auto fragment_timestamp_range{fragment_id.timestamp_range()};
        if (is_vacuum_file(uri)) {
          vac_file_bitmap[i] = 1;
          if (timestamps_overlap(
                  fragment_timestamp_range,
                  !full_overlap_only &&
                      consolidation_with_timestamps_supported(uri))) {
            overlapping_vac_file_bitmap[i] = 1;
          }
        } else {
          if (!timestamps_overlap(
                  fragment_timestamp_range,
                  !full_overlap_only &&
                      consolidation_with_timestamps_supported(uri))) {
            non_vac_uri_bitmap[i] = 1;
          }
        }

        return Status::Ok();
      }));

  auto num_vac_files =
      std::accumulate(vac_file_bitmap.begin(), vac_file_bitmap.end(), 0);
  std::vector<URI> vac_files;
  std::unordered_set<std::string> non_vac_uris_set;
  std::unordered_map<std::string, size_t> uris_map;

  if (num_vac_files > 0) {
    vac_files.reserve(num_vac_files);
    auto num_non_vac_uris = std::accumulate(
        non_vac_uri_bitmap.begin(), non_vac_uri_bitmap.end(), 0);
    non_vac_uris_set.reserve(num_non_vac_uris);
    for (uint64_t i = 0; i < uris.size(); i++) {
      if (overlapping_vac_file_bitmap[i] != 0) {
        vac_files.emplace_back(uris[i]);
      }

      if (non_vac_uri_bitmap[i] != 0) {
        non_vac_uris_set.emplace(uris[i].to_string());
      }

      if (vac_file_bitmap[i] == 0 && non_vac_uri_bitmap[i] == 0) {
        uris_map[uris[i].to_string()] = i;
      }
    }
  }

  // Compute fragment URIs to vacuum as a bitmap vector
  // Also determine which vac files to vacuum
  std::vector<int32_t> to_vacuum_vec(uris.size(), 0);
  std::vector<int32_t> to_vacuum_vac_files_vec(vac_files.size(), 0);
  auto& tp = resources_.get().compute_tp();
  auto status = parallel_for(&tp, 0, vac_files.size(), [&](size_t i) {
    uint64_t size = 0;
    auto& vfs = resources_.get().vfs();
    throw_if_not_ok(vfs.file_size(vac_files[i], &size));
    std::string names;
    names.resize(size);
    throw_if_not_ok(vfs.read(vac_files[i], 0, &names[0], size, false));
    std::stringstream ss(names);
    bool vacuum_vac_file = true;
    for (std::string uri_str; std::getline(ss, uri_str);) {
      uri_str =
          get_full_vac_uri(uri_.add_trailing_slash().to_string(), uri_str);
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
  if (uris.empty()) {
    return {Status::Ok(), filtered_uris};
  }

  // Get the URIs that must be ignored
  std::unordered_set<std::string> to_ignore_set;
  auto base_uri_size = uri_.to_string().size();
  for (const auto& uri : to_ignore) {
    to_ignore_set.emplace(uri.to_string().substr(base_uri_size).c_str());
  }

  // Filter based on vacuumed URIs and timestamp
  std::vector<uint8_t> overlaps_bitmap(uris.size());
  std::vector<std::pair<uint64_t, uint64_t>> fragment_timestamp_ranges(
      uris.size());
  throw_if_not_ok(parallel_for(
      &resources_.get().compute_tp(), 0, uris.size(), [&](size_t i) {
        auto& uri = uris[i];
        std::string short_uri = uri.to_string().substr(base_uri_size);
        if (to_ignore_set.count(short_uri.c_str()) != 0) {
          return Status::Ok();
        }

        // Also ignore any vac uris
        if (is_vacuum_file(uri)) {
          return Status::Ok();
        }

        // Get the start and end timestamp for this fragment
        FragmentID fragment_id{uri};
        fragment_timestamp_ranges[i] = fragment_id.timestamp_range();
        if (timestamps_overlap(
                fragment_timestamp_ranges[i],
                !full_overlap_only &&
                    consolidation_with_timestamps_supported(uri))) {
          overlaps_bitmap[i] = 1;
        }
        return Status::Ok();
      }));

  auto count =
      std::accumulate(overlaps_bitmap.begin(), overlaps_bitmap.end(), 0);
  filtered_uris.reserve(count);
  for (uint64_t i = 0; i < uris.size(); i++) {
    if (overlaps_bitmap[i]) {
      filtered_uris.emplace_back(uris[i], fragment_timestamp_ranges[i]);
    }
  };

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
    RETURN_NOT_OK(resources_.get().vfs().is_file(old_schema_uri, &has_file));
    if (has_file) {
      array_schema_uris_.push_back(old_schema_uri);
    }
  }

  // Optionally add the new array schemas from the array schema directory
  if (!array_schema_dir_uris.empty()) {
    array_schema_uris_.reserve(
        array_schema_uris_.size() + array_schema_dir_uris.size());
    for (auto& uri : array_schema_dir_uris) {
      if (uri.last_path_part() == constants::array_enumerations_dir_name) {
        continue;
      }
      array_schema_uris_.push_back(uri);
    }
  }

  return Status::Ok();
}

URI ArrayDirectory::select_latest_array_schema_uri() {
  // Set the latest array schema URI. When in READ mode, the latest array
  // schema URI is the schema with the largest timestamp less than or equal
  // to the current timestamp_end_. If no schema meets this definition, we
  // use the first schema available.
  //
  // The reason for choosing the oldest array schema URI even when time
  // traveling before it existed is to first, not break any arrays that have
  // fragments written before the first schema existed. The second reason is
  // to not break old arrays that only have the old `__array_schema.tdb`
  // URI which does not have timestamps.
  if (mode_ != ArrayDirectoryMode::READ) {
    return array_schema_uris_.back();
  }

  optional<URI> latest_uri = nullopt;

  for (auto& uri : array_schema_uris_) {
    FragmentID fragment_id{uri};
    // Skip the old schema URI name since it doesn't have timestamps
    if (fragment_id.name() == constants::array_schema_filename) {
      continue;
    }

    auto ts_range{fragment_id.timestamp_range()};
    if (ts_range.second <= timestamp_end_) {
      latest_uri = uri;
    }
  }

  return latest_uri.value_or(array_schema_uris_.front());
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
  // If the fragment ID does not have a name, ignore it.
  if (!FragmentID::has_fragment_name(uri)) {
    *is_fragment = 0;
    return Status::Ok();
  }

  FragmentID fragment_id{uri};
  auto name = fragment_id.name();
  // If the URI name has a suffix, then it is not a fragment
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

  // If the array format version is >= 5, then the above suffices to check if
  // the URI is indeed a fragment
  if (fragment_id.array_format_version() >= 5) {
    *is_fragment = false;
    return Status::Ok();
  }

  // Versions < 5
  bool is_file;
  RETURN_NOT_OK(resources_.get().vfs().is_file(
      uri.join_path(constants::fragment_metadata_filename), &is_file));
  *is_fragment = (int)is_file;
  return Status::Ok();
}

bool ArrayDirectory::consolidation_with_timestamps_supported(
    const URI& uri) const {
  // FragmentID::array_format_version() returns UINT32_MAX for versions <= 2
  // so we should explicitly exclude this case when checking if consolidation
  // with timestamps is supported on a fragment
  FragmentID fragment_id{uri};
  return mode_ == ArrayDirectoryMode::READ &&
         fragment_id.array_format_version() >=
             constants::consolidation_with_timestamps_min_version;
}

shared_ptr<const Enumeration> ArrayDirectory::load_enumeration(
    const std::string& enumeration_path,
    const EncryptionKey& encryption_key,
    shared_ptr<MemoryTracker> memory_tracker) const {
  auto timer_se = resources_.get().stats().start_timer("sm_load_enumeration");

  auto enmr_uri = uri_.join_path(constants::array_schema_dir_name)
                      .join_path(constants::array_enumerations_dir_name)
                      .join_path(enumeration_path);

  auto tile = GenericTileIO::load(
      resources_, enmr_uri, 0, encryption_key, memory_tracker);
  resources_.get().stats().add_counter("read_enumeration_size", tile->size());

  if (!memory_tracker->take_memory(tile->size(), MemoryType::ENUMERATION)) {
    throw ArrayDirectoryException(
        "Error loading enumeration; Insufficient memory budget; Needed " +
        std::to_string(tile->size()) + " but only had " +
        std::to_string(memory_tracker->get_memory_available()) +
        " from budget " + std::to_string(memory_tracker->get_memory_budget()));
  }

  Deserializer deserializer(tile->data(), tile->size());
  return Enumeration::deserialize(deserializer, memory_tracker);
}

}  // namespace tiledb::sm
