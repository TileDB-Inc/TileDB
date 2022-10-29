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

  check_array_uri(array_name);

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
  storage_manager_->write_consolidated_commits_file(
      write_version, array_dir, to_consolidate);

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
