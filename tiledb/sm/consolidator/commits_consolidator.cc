/**
 * @file   commits_consolidator.cc
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
 * This file implements the CommitsConsolidator class.
 */

#include "tiledb/sm/consolidator/commits_consolidator.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTOR           */
/* ****************************** */

CommitsConsolidator::CommitsConsolidator(StorageManager* storage_manager)
    : Consolidator(storage_manager) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status CommitsConsolidator::consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer_se = stats_->start_timer("consolidate_commits");

  // Open array for writing
  auto array_uri = URI(array_name);
  Array array_for_writes(array_uri, storage_manager_);
  RETURN_NOT_OK(array_for_writes.open(
      QueryType::WRITE, encryption_type, encryption_key, key_length));

  // Ensure write version is at least 12.
  auto write_version = array_for_writes.array_schema_latest().write_version();
  RETURN_NOT_OK(array_for_writes.close());
  if (write_version < 12) {
    return logger_->status(Status_ConsolidatorError(
        "Array version should be at least 12 to consolidate commits."));
  }

  // Get the array uri to consolidate from the array directory.
  auto array_dir = ArrayDirectory(
      storage_manager_->vfs(),
      storage_manager_->compute_tp(),
      URI(array_name),
      0,
      utils::time::timestamp_now_ms(),
      ArrayDirectoryMode::COMMITS);

  // Don't try to consolidate empty array
  if (array_dir.commit_uris_to_consolidate().size() == 0) {
    return Status::Ok();
  }

  // Get the file name.
  auto& to_consolidate = array_dir.commit_uris_to_consolidate();
  auto&& [st1, name] = array_dir.compute_new_fragment_name(
      to_consolidate.front(), to_consolidate.back(), write_version);
  RETURN_NOT_OK(st1);

  // Compute size of consolidated file. Save the sizes of the files to re-use
  // below.
  storage_size_t total_size = 0;
  const auto base_uri_size = array_dir.uri().to_string().size();
  std::vector<storage_size_t> file_sizes(to_consolidate.size());
  for (uint64_t i = 0; i < to_consolidate.size(); i++) {
    const auto& uri = to_consolidate[i];
    total_size += uri.to_string().size() - base_uri_size + 1;

    // If the file is a delete, add the file size to the count and the size of
    // the size variable.
    if (stdx::string::ends_with(
            uri.to_string(), constants::delete_file_suffix)) {
      RETURN_NOT_OK(storage_manager_->vfs()->file_size(uri, &file_sizes[i]));
      total_size += file_sizes[i];
      total_size += sizeof(storage_size_t);
    }
  }

  // Write consolidated file, URIs are relative to the array URI.
  std::vector<uint8_t> data(total_size);
  storage_size_t file_index = 0;
  for (uint64_t i = 0; i < to_consolidate.size(); i++) {
    // Add the uri.
    const auto& uri = to_consolidate[i];
    std::string relative_uri = uri.to_string().substr(base_uri_size) + "\n";
    memcpy(&data[file_index], relative_uri.data(), relative_uri.size());
    file_index += relative_uri.size();

    // For deletes, read the delete condition to the output file.
    if (stdx::string::ends_with(
            uri.to_string(), constants::delete_file_suffix)) {
      memcpy(&data[file_index], &file_sizes[i], sizeof(storage_size_t));
      file_index += sizeof(storage_size_t);
      RETURN_NOT_OK(storage_manager_->vfs()->read(
          uri, 0, &data[file_index], file_sizes[i]));
      file_index += file_sizes[i];
    }
  }

  // Write the file to storage.
  URI consolidated_commits_uri =
      array_dir.get_commits_dir(write_version)
          .join_path(name.value() + constants::con_commits_file_suffix);
  RETURN_NOT_OK(storage_manager_->vfs()->write(
      consolidated_commits_uri, data.data(), data.size()));
  RETURN_NOT_OK(storage_manager_->vfs()->close_file(consolidated_commits_uri));

  return Status::Ok();
}

Status CommitsConsolidator::vacuum(const char* array_name) {
  if (array_name == nullptr)
    return logger_->status(Status_StorageManagerError(
        "Cannot vacuum array metadata; Array name cannot be null"));

  // Get the array metadata URIs and vacuum file URIs to be vacuum
  auto vfs = storage_manager_->vfs();
  auto compute_tp = storage_manager_->compute_tp();
  ArrayDirectory array_dir;
  array_dir = ArrayDirectory(
      vfs,
      compute_tp,
      URI(array_name),
      0,
      utils::time::timestamp_now_ms(),
      ArrayDirectoryMode::COMMITS);

  const auto& commits_uris_to_vacuum = array_dir.commit_uris_to_vacuum();
  const auto& consolidated_commits_uris_to_vacuum =
      array_dir.consolidated_commits_uris_to_vacuum();

  // Delete the commits files
  auto status =
      parallel_for(compute_tp, 0, commits_uris_to_vacuum.size(), [&](size_t i) {
        RETURN_NOT_OK(vfs->remove_file(commits_uris_to_vacuum[i]));

        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Delete vacuum files
  status = parallel_for(
      compute_tp, 0, consolidated_commits_uris_to_vacuum.size(), [&](size_t i) {
        RETURN_NOT_OK(vfs->remove_file(consolidated_commits_uris_to_vacuum[i]));
        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
