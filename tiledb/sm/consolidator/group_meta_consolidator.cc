/**
 * @file   group_meta_consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
    const Config& config, StorageManager* storage_manager)
    : Consolidator(storage_manager) {
  auto st = set_config(config);
  if (!st.ok()) {
    throw std::logic_error(st.message());
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status GroupMetaConsolidator::consolidate(
    const char* group_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  (void)encryption_type;
  (void)encryption_key;
  (void)key_length;
  auto timer_se = stats_->start_timer("consolidate_group_meta");

  check_array_uri(group_name);

  // Open group for reading
  auto group_uri = URI(group_name);
  AutoCloseGroup group_for_reads(
      group_uri,
      storage_manager_,
      QueryType::READ,
      config_.timestamp_start_,
      config_.timestamp_end_);

  // Open group for writing
  AutoCloseGroup group_for_writes(
      group_uri, storage_manager_, QueryType::WRITE);

  // Swap the in-memory metadata between the two groups.
  // After that, the group for writes will store the (consolidated by
  // the way metadata loading works) metadata of the group for reads
  Metadata* metadata_r{group_for_reads->metadata()};
  Metadata* metadata_w{group_for_writes->metadata()};
  metadata_r->swap(metadata_w);

  // Metadata uris to delete
  const auto to_vacuum = metadata_w->loaded_metadata_uris();

  // Generate new name for consolidated metadata
  throw_if_not_ok(metadata_w->generate_uri(group_uri));

  // Get the new URI name
  URI new_uri;
  throw_if_not_ok(metadata_w->get_uri(group_uri, &new_uri));

  // Write vacuum file
  URI vac_uri = URI(new_uri.to_string() + constants::vacuum_file_suffix);

  std::stringstream ss;
  for (const auto& uri : to_vacuum)
    ss << uri.to_string() << "\n";

  auto data = ss.str();
  throw_if_not_ok(
      storage_manager_->vfs()->write(vac_uri, data.c_str(), data.size()));
  throw_if_not_ok(storage_manager_->vfs()->close_file(vac_uri));

  return Status::Ok();
}

void GroupMetaConsolidator::vacuum(const char* group_name) {
  if (group_name == nullptr) {
    throw Status_StorageManagerError(
        "Cannot vacuum group metadata; Group name cannot be null");
  }

  // Get the group metadata URIs and vacuum file URIs to be vacuumed
  auto vfs = storage_manager_->vfs();
  auto compute_tp = storage_manager_->compute_tp();
  GroupDirectory group_dir(
      vfs,
      compute_tp,
      URI(group_name),
      0,
      std::numeric_limits<uint64_t>::max());

  // Delete the group metadata and vacuum files
  vfs->remove_files(compute_tp, group_dir.group_meta_uris_to_vacuum());
  vfs->remove_files(compute_tp, group_dir.group_meta_vac_uris_to_vacuum());
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status GroupMetaConsolidator::set_config(const Config& config) {
  // Set the consolidation config for ease of use
  Config merged_config = storage_manager_->config();
  merged_config.inherit(config);
  bool found = false;
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_start", &config_.timestamp_start_, &found));
  assert(found);
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_end", &config_.timestamp_end_, &found));
  assert(found);

  return Status::Ok();
}

}  // namespace tiledb::sm
