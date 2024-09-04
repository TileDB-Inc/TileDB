/**
 * @file   consolidator.cc
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
 * This file implements the Consolidator class.
 */

#include "tiledb/sm/consolidator/consolidator.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/consolidator/array_meta_consolidator.h"
#include "tiledb/sm/consolidator/commits_consolidator.h"
#include "tiledb/sm/consolidator/fragment_consolidator.h"
#include "tiledb/sm/consolidator/fragment_meta_consolidator.h"
#include "tiledb/sm/consolidator/group_meta_consolidator.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/object/object.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/job.h"
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ********************************* */
/*          FACTORY METHODS          */
/* ********************************* */

/**
 * Factory function to create the consolidator depending on mode.
 */
shared_ptr<Consolidator> Consolidator::create(
    JobParent& parent, const ConsolidationMode mode, const Config& config) {
  switch (mode) {
    case ConsolidationMode::FRAGMENT_META: {
      auto job{make_shared<FragmentMetaConsolidator>(HERE(), parent)};
      job->register_shared_ptr(job);
      return job;
    }
    case ConsolidationMode::FRAGMENT: {
      auto job{make_shared<FragmentConsolidator>(HERE(), parent, config)};
      job->register_shared_ptr(job);
      return job;
    }
    case ConsolidationMode::ARRAY_META: {
      auto job{make_shared<ArrayMetaConsolidator>(HERE(), parent, config)};
      job->register_shared_ptr(job);
      return job;
    }
    case ConsolidationMode::COMMITS: {
      auto job{make_shared<CommitsConsolidator>(HERE(), parent)};
      job->register_shared_ptr(job);
      return job;
    }
    case ConsolidationMode::GROUP_META: {
      auto job{make_shared<GroupMetaConsolidator>(HERE(), parent, config)};
      job->register_shared_ptr(job);
      return job;
    }
    default:
      return nullptr;
  }
}

ConsolidationMode Consolidator::mode_from_config(
    const Config& config, const bool vacuum_mode) {
  bool found = false;
  const std::string mode = vacuum_mode ?
                               config.get("sm.vacuum.mode", &found) :
                               config.get("sm.consolidation.mode", &found);
  if (!found) {
    throw std::logic_error(
        "Cannot consolidate; Consolidation mode cannot be null");
  }

  if (mode == "fragment_meta")
    return ConsolidationMode::FRAGMENT_META;
  else if (mode == "fragments")
    return ConsolidationMode::FRAGMENT;
  else if (mode == "array_meta")
    return ConsolidationMode::ARRAY_META;
  else if (mode == "commits")
    return ConsolidationMode::COMMITS;
  else if (mode == "group_meta")
    return ConsolidationMode::GROUP_META;

  throw std::logic_error("Cannot consolidate; invalid configuration mode");
}

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Consolidator::Consolidator(JobParent& parent)
    : JobBranch(parent)
    , resources_(parent.resources())
    , consolidator_memory_tracker_(resources_.create_memory_tracker())
    , stats_(resources_.stats().create_child("Consolidator"))
    , logger_(resources_.logger()->clone("Consolidator", ++logger_id_)) {
  consolidator_memory_tracker_->set_type(MemoryTrackerType::CONSOLIDATOR);
}

Consolidator::~Consolidator() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

Status Consolidator::consolidate(
    [[maybe_unused]] const char* array_name,
    [[maybe_unused]] EncryptionType encryption_type,
    [[maybe_unused]] const void* encryption_key,
    [[maybe_unused]] uint32_t key_length) {
  throw ConsolidatorException("Cannot consolidate; Invalid object");
}

void Consolidator::vacuum([[maybe_unused]] const char* array_name) {
  throw ConsolidatorException("Cannot vacuum; Invalid object");
}

void Consolidator::array_consolidate(
    JobParent& parent,
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const Config& config) {
  auto& resources{parent.resources()};
  // Check array URI
  URI array_uri(array_name);
  if (array_uri.is_invalid()) {
    throw ConsolidatorException("Cannot consolidate array; Invalid URI");
  }

  // Check if array exists
  if (object_type(resources, array_uri) != ObjectType::ARRAY) {
    throw ConsolidatorException(
        "Cannot consolidate array; Array does not exist");
  }

  if (array_uri.is_tiledb()) {
    throw_if_not_ok(
        resources.rest_client()->post_consolidation_to_rest(array_uri, config));
  } else {
    // Get encryption key from config
    std::string encryption_key_from_cfg;
    if (!encryption_key) {
      bool found = false;
      encryption_key_from_cfg = config.get("sm.encryption_key", &found);
      assert(found);
    }

    if (!encryption_key_from_cfg.empty()) {
      encryption_key = encryption_key_from_cfg.c_str();
      key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
      std::string encryption_type_from_cfg;
      bool found = false;
      encryption_type_from_cfg = config.get("sm.encryption_type", &found);
      assert(found);
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

    // Consolidate
    auto mode = Consolidator::mode_from_config(config);
    auto consolidator = Consolidator::create(parent, mode, config);
    throw_if_not_ok(consolidator->consolidate(
        array_name, encryption_type, encryption_key, key_length));
  }
}

void Consolidator::fragments_consolidate(
    JobParent& parent,
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const std::vector<std::string> fragment_uris,
    const Config& config) {
  auto& resources{parent.resources()};
  // Check array URI
  URI array_uri(array_name);
  if (array_uri.is_invalid()) {
    throw ConsolidatorException("Cannot consolidate array; Invalid URI");
  }

  // Check if array exists
  if (object_type(resources, array_uri) != ObjectType::ARRAY) {
    throw ConsolidatorException(
        "Cannot consolidate array; Array does not exist");
  }

  // Get encryption key from config
  std::string encryption_key_from_cfg;
  if (!encryption_key) {
    bool found = false;
    encryption_key_from_cfg = config.get("sm.encryption_key", &found);
    assert(found);
  }

  if (!encryption_key_from_cfg.empty()) {
    encryption_key = encryption_key_from_cfg.c_str();
    key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
    std::string encryption_type_from_cfg;
    bool found = false;
    encryption_type_from_cfg = config.get("sm.encryption_type", &found);
    assert(found);
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

  // Consolidate
  auto consolidator =
      Consolidator::create(parent, ConsolidationMode::FRAGMENT, config);
  auto fragment_consolidator =
      dynamic_cast<FragmentConsolidator*>(consolidator.get());
  throw_if_not_ok(fragment_consolidator->consolidate_fragments(
      array_name, encryption_type, encryption_key, key_length, fragment_uris));
}

void Consolidator::write_consolidated_commits_file(
    format_version_t write_version,
    ArrayDirectory array_dir,
    const std::vector<URI>& commit_uris,
    ContextResources& resources) {
  // Compute the file name.
  auto name = storage_format::generate_consolidated_fragment_name(
      commit_uris.front(), commit_uris.back(), write_version);

  // Compute size of consolidated file. Save the sizes of the files to re-use
  // below.
  storage_size_t total_size = 0;
  const auto base_uri_size = array_dir.uri().to_string().size();
  std::vector<storage_size_t> file_sizes(commit_uris.size());
  for (uint64_t i = 0; i < commit_uris.size(); i++) {
    const auto& uri = commit_uris[i];
    total_size += uri.to_string().size() - base_uri_size + 1;

    // If the file is a delete, add the file size to the count and the size of
    // the size variable.
    if (stdx::string::ends_with(
            uri.to_string(), constants::delete_file_suffix)) {
      throw_if_not_ok(resources.vfs().file_size(uri, &file_sizes[i]));
      total_size += file_sizes[i];
      total_size += sizeof(storage_size_t);
    }
  }

  // Write consolidated file, URIs are relative to the array URI.
  std::vector<uint8_t> data(total_size);
  storage_size_t file_index = 0;
  for (uint64_t i = 0; i < commit_uris.size(); i++) {
    // Add the uri.
    const auto& uri = commit_uris[i];
    std::string relative_uri = uri.to_string().substr(base_uri_size) + "\n";
    memcpy(&data[file_index], relative_uri.data(), relative_uri.size());
    file_index += relative_uri.size();

    // For deletes, read the delete condition to the output file.
    if (stdx::string::ends_with(
            uri.to_string(), constants::delete_file_suffix)) {
      memcpy(&data[file_index], &file_sizes[i], sizeof(storage_size_t));
      file_index += sizeof(storage_size_t);
      throw_if_not_ok(
          resources.vfs().read(uri, 0, &data[file_index], file_sizes[i]));
      file_index += file_sizes[i];
    }
  }

  // Write the file to storage.
  URI consolidated_commits_uri =
      array_dir.get_commits_dir(write_version)
          .join_path(name + constants::con_commits_file_suffix);
  throw_if_not_ok(resources.vfs().write(
      consolidated_commits_uri, data.data(), data.size()));
  throw_if_not_ok(resources.vfs().close_file(consolidated_commits_uri));
}

void Consolidator::array_vacuum(
    JobParent& parent, const char* array_name, const Config& config) {
  auto& resources{parent.resources()};
  URI array_uri(array_name);
  if (array_uri.is_tiledb()) {
    throw_if_not_ok(
        resources.rest_client()->post_vacuum_to_rest(array_uri, config));
    return;
  }

  auto mode = Consolidator::mode_from_config(config, true);
  auto consolidator = Consolidator::create(parent, mode, config);
  consolidator->vacuum(array_name);
}

void Consolidator::check_array_uri(const char* array_name) {
  if (URI(array_name).is_tiledb()) {
    throw ConsolidatorException(
        "Consolidation is not supported for remote arrays.");
  }
}

}  // namespace tiledb::sm
