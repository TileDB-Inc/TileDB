/**
 * @file   deletes_and_updates.cc
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
 * This file implements class Deletes.
 */

#include "tiledb/sm/query/deletes_and_updates/deletes_and_updates.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/query/deletes_and_updates/serialization.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

class DeleteAndUpdateStatusException : public StatusException {
 public:
  explicit DeleteAndUpdateStatusException(const std::string& message)
      : StatusException("Deletes", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

DeletesAndUpdates::DeletesAndUpdates(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition,
    std::vector<UpdateValue>& update_values,
    bool skip_checks_serialization)
    : StrategyBase(
          stats,
          logger->clone("Deletes", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout)
    , condition_(condition)
    , update_values_(update_values) {
  // Sanity checks
  if (storage_manager_ == nullptr) {
    throw DeleteAndUpdateStatusException(
        "Cannot initialize query; Storage manager not set");
  }

  if (!buffers_.empty()) {
    throw DeleteAndUpdateStatusException(
        "Cannot initialize deletes; Buffers are set");
  }

  if (array_schema_.dense()) {
    throw DeleteAndUpdateStatusException(
        "Cannot initialize deletes; Only supported for sparse arrays");
  }

  if (subarray_.is_set()) {
    throw DeleteAndUpdateStatusException(
        "Cannot initialize deletes; Subarrays are not supported");
  }

  if (!skip_checks_serialization && condition_.empty()) {
    throw DeleteAndUpdateStatusException(
        "Cannot initialize deletes; One condition is needed");
  }
}

DeletesAndUpdates::~DeletesAndUpdates() {
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status DeletesAndUpdates::finalize() {
  return Status::Ok();
}

Status DeletesAndUpdates::initialize_memory_budget() {
  return Status::Ok();
}

Status DeletesAndUpdates::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  // Check that the update values are valid.
  for (auto& update_value : update_values_) {
    update_value.check(array_schema_);
  }

  // Get a new fragment name for the delete.
  uint64_t timestamp = array_->timestamp_end_opened_at();
  auto write_version = array_->array_schema_latest().write_version();
  auto new_fragment_str =
      storage_format::generate_fragment_name(timestamp, write_version);

  // Check that the delete or update isn't in the middle of a fragment
  // consolidated without timestamps.
  auto& frag_uris = array_->array_directory().unfiltered_fragment_uris();
  for (auto& uri : frag_uris) {
    uint32_t version;
    auto name = uri.remove_trailing_slash().last_path_part();
    RETURN_NOT_OK(utils::parse::get_fragment_version(name, &version));
    if (version < constants::consolidation_with_timestamps_min_version) {
      std::pair<uint64_t, uint64_t> fragment_timestamp_range;
      RETURN_NOT_OK(
          utils::parse::get_timestamp_range(uri, &fragment_timestamp_range));
      if (timestamp >= fragment_timestamp_range.first &&
          timestamp <= fragment_timestamp_range.second) {
        throw DeleteAndUpdateStatusException(
            "Cannot write a delete in the middle of a fragment consolidated "
            "without timestamps.");
      }
    }
  }

  // Create the commit URI if needed.
  auto& array_dir = array_->array_directory();
  auto commit_uri = array_dir.get_commits_dir(write_version);
  RETURN_NOT_OK(storage_manager_->vfs()->create_dir(commit_uri));

  // Serialize the negated condition (aud update values if they are not empty)
  // and write to disk.
  std::vector<uint8_t> serialized_condition;
  if (update_values_.empty()) {
    new_fragment_str += constants::delete_file_suffix;
    serialized_condition =
        tiledb::sm::deletes_and_updates::serialization::serialize_condition(
            condition_.negated_condition());
  } else {
    new_fragment_str += constants::update_file_suffix;
    serialized_condition = tiledb::sm::deletes_and_updates::serialization::
        serialize_update_condition_and_values(
            condition_.negated_condition(), update_values_);
  }

  auto uri = commit_uri.join_path(new_fragment_str);
  RETURN_NOT_OK(storage_manager_->store_data_to_generic_tile(
      serialized_condition.data(),
      serialized_condition.size(),
      uri,
      *array_->encryption_key()));

  return Status::Ok();
}

void DeletesAndUpdates::reset() {
}

}  // namespace sm
}  // namespace tiledb
