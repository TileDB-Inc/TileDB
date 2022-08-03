/**
 * @file   deletes.cc
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

#include "tiledb/sm/query/deletes_and_updates/deletes.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/query/deletes_and_updates/serialization.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

class DeleteStatusException : public StatusException {
 public:
  explicit DeleteStatusException(const std::string& message)
      : StatusException("Deletes", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Deletes::Deletes(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition)
    : StrategyBase(
          stats,
          logger->clone("Deletes", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout)
    , condition_(condition) {
  // Sanity checks
  if (storage_manager_ == nullptr) {
    throw DeleteStatusException(
        "Cannot initialize query; Storage manager not set");
  }

  if (!buffers_.empty()) {
    throw DeleteStatusException("Cannot initialize deletes; Buffers are set");
  }

  if (array_schema_.dense()) {
    throw DeleteStatusException(
        "Cannot initialize deletes; Only supported for sparse arrays");
  }

  if (subarray_.is_set()) {
    throw DeleteStatusException(
        "Cannot initialize deletes; Subarrays are not supported");
  }

  if (condition_.empty()) {
    throw DeleteStatusException(
        "Cannot initialize deletes; One condition is needed");
  }
}

Deletes::~Deletes() {
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status Deletes::finalize() {
  return Status::Ok();
}

Status Deletes::initialize_memory_budget() {
  return Status::Ok();
}

Status Deletes::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  // Get a new fragment name for the delete.
  uint64_t timestamp = array_->timestamp_end_opened_at();
  auto write_version = array_->array_schema_latest().write_version();
  auto&& [st, new_fragment_name_opt] =
      new_fragment_name(timestamp, write_version);
  RETURN_NOT_OK(st);
  std::string new_fragment_str =
      *new_fragment_name_opt + constants::delete_file_suffix;

  // Get the delete URI.
  auto& array_dir = array_->array_directory();
  auto commit_uri = array_dir.get_commits_dir(write_version);
  RETURN_NOT_OK(storage_manager_->vfs()->create_dir(commit_uri));
  auto uri = commit_uri.join_path(new_fragment_str);

  // Serialize the negated condition and write to disk.
  auto serialized_condition =
      tiledb::sm::deletes_and_updates::serialization::serialize_condition(
          condition_.negated_condition());
  RETURN_NOT_OK(storage_manager_->store_data_to_generic_tile(
      serialized_condition.data(),
      serialized_condition.size(),
      uri,
      *array_->encryption_key()));

  return Status::Ok();
}

void Deletes::reset() {
}

}  // namespace sm
}  // namespace tiledb
