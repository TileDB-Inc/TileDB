/**
 * @file   group_meta_consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
 * This file implements the GroupMetaConsolidator class.
 */

#include "tiledb/sm/consolidator/group_meta_consolidator.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/group/group_details_v1.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ****************************** */
/*          CONSTRUCTOR           */
/* ****************************** */

GroupMetaConsolidator::GroupMetaConsolidator(
    ContextResources& resources,
    const Config& config,
    StorageManager* storage_manager)
    : Consolidator(resources, storage_manager) {
  auto st = set_config(config);
  if (!st.ok()) {
    throw std::logic_error(st.message());
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status GroupMetaConsolidator::consolidate(
    const char* group_name, EncryptionType, const void*, uint32_t) {
  auto timer_se = stats_->start_timer("consolidate_group_meta");

  // Open group for reading
  auto group_uri = URI(group_name);
  Group group_for_reads(resources_, group_uri);
  group_for_reads.open(
      QueryType::READ, config_.timestamp_start_, config_.timestamp_end_);

  // Open group for writing
  Group group_for_writes(resources_, group_uri);

  try {
    group_for_writes.open(QueryType::WRITE);
  } catch (...) {
    group_for_reads.close();
  }

  // Check if there's actually more than 1 file to consolidate
  auto metadata_r = group_for_reads.metadata();
  if (metadata_r->loaded_metadata_uris().size() <= 1) {
    return Status::Ok();
  }

  // Copy-assign the read metadata into the metadata of the group for writes
  *(group_for_writes.metadata()) = *metadata_r;
  URI new_uri = metadata_r->get_uri(group_uri);
  const auto& to_vacuum = metadata_r->loaded_metadata_uris();

  // Prepare vacuum file
  URI vac_uri = URI(new_uri.to_string() + constants::vacuum_file_suffix);
  std::stringstream ss;
  for (const auto& uri : to_vacuum) {
    ss << uri.to_string() << "\n";
  }
  auto data = ss.str();

  // Close groups
  group_for_reads.close();
  group_for_writes.close();

  // Write vacuum file
  resources_.vfs().write(vac_uri, data.c_str(), data.size());
  throw_if_not_ok(resources_.vfs().close_file(vac_uri));

  return Status::Ok();
}

void GroupMetaConsolidator::vacuum(const char* group_name) {
  if (group_name == nullptr) {
    throw std::invalid_argument(
        "Cannot vacuum group metadata; Group name cannot be null");
  }

  // Get the group metadata URIs and vacuum file URIs to be vacuumed
  auto& vfs = resources_.vfs();
  auto& compute_tp = resources_.compute_tp();
  GroupDirectory group_dir(
      vfs,
      compute_tp,
      URI(group_name),
      0,
      std::numeric_limits<uint64_t>::max());

  // Delete the group metadata and vacuum files
  vfs.remove_files(&compute_tp, group_dir.group_meta_uris_to_vacuum());
  vfs.remove_files(&compute_tp, group_dir.group_meta_vac_uris_to_vacuum());
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status GroupMetaConsolidator::set_config(const Config& config) {
  // Set the consolidation config for ease of use
  Config merged_config = resources_.config();
  merged_config.inherit(config);
  config_.timestamp_start_ = merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_start", Config::must_find);
  config_.timestamp_end_ = merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_end", Config::must_find);

  return Status::Ok();
}

}  // namespace tiledb::sm
