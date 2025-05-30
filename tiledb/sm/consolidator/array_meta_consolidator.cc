/**
 * @file   array_meta_consolidator.cc
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
 * This file implements the ArrayMetaConsolidator class.
 */

#include "tiledb/sm/consolidator/array_meta_consolidator.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ****************************** */
/*          CONSTRUCTOR           */
/* ****************************** */

ArrayMetaConsolidator::ArrayMetaConsolidator(
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

Status ArrayMetaConsolidator::consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer_se = stats_->start_timer("consolidate_array_meta");
  check_array_uri(array_name);

  // Open array for reading
  auto array_uri = URI(array_name);
  Array array_for_reads(resources_, array_uri);
  throw_if_not_ok(array_for_reads.open(
      QueryType::READ,
      config_.timestamp_start_,
      config_.timestamp_end_,
      encryption_type,
      encryption_key,
      key_length));

  // Check if there's actually more than 1 file to consolidate
  auto& metadata_r = array_for_reads.metadata();
  if (metadata_r.loaded_metadata_uris().size() <= 1) {
    return Status::Ok();
  }

  // Open array for writing
  Array array_for_writes(resources_, array_uri);
  RETURN_NOT_OK_ELSE(
      array_for_writes.open(
          QueryType::WRITE, encryption_type, encryption_key, key_length),
      throw_if_not_ok(array_for_reads.close()));

  // Copy-assign the read metadata into the metadata of the array for writes
  array_for_writes.opened_array()->metadata() = metadata_r;
  URI new_uri = metadata_r.get_uri(array_uri);
  const auto& to_vacuum = metadata_r.loaded_metadata_uris();

  // Write vac files relative to the array URI. This was fixed for reads in
  // version 19 so only do this for arrays starting with version 19.
  size_t base_uri_size = 0;
  if (array_for_reads.array_schema_latest_ptr() == nullptr ||
      array_for_reads.array_schema_latest().write_version() >= 19) {
    base_uri_size = array_for_reads.array_uri().to_string().size();
  }

  // Prepare vacuum file
  URI vac_uri = URI(new_uri.to_string() + constants::vacuum_file_suffix);
  std::stringstream ss;
  for (const auto& uri : to_vacuum) {
    ss << uri.to_string().substr(base_uri_size) << "\n";
  }
  auto data = ss.str();

  // Close arrays
  throw_if_not_ok(array_for_reads.close());
  throw_if_not_ok(array_for_writes.close());

  // Write vacuum file
  throw_if_not_ok(resources_.vfs().write(vac_uri, data.c_str(), data.size()));
  throw_if_not_ok(resources_.vfs().close_file(vac_uri));

  return Status::Ok();
}

void ArrayMetaConsolidator::vacuum(const char* array_name) {
  if (array_name == nullptr) {
    throw std::invalid_argument(
        "Cannot vacuum array metadata; Array name cannot be null");
  }

  // Get the array metadata URIs and vacuum file URIs to be vacuum
  auto& vfs = resources_.vfs();
  auto& compute_tp = resources_.compute_tp();
  auto array_dir = ArrayDirectory(
      resources_, URI(array_name), 0, std::numeric_limits<uint64_t>::max());

  // Delete the array metadata and vacuum files
  vfs.remove_files(&compute_tp, array_dir.array_meta_uris_to_vacuum());
  vfs.remove_files(&compute_tp, array_dir.array_meta_vac_uris_to_vacuum());
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status ArrayMetaConsolidator::set_config(const Config& config) {
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
