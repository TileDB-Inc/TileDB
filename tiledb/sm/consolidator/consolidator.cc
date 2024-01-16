/**
 * @file   consolidator.cc
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
 * This file implements the Consolidator class.
 */

#include "tiledb/sm/consolidator/consolidator.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/consolidator/array_meta_consolidator.h"
#include "tiledb/sm/consolidator/commits_consolidator.h"
#include "tiledb/sm/consolidator/fragment_consolidator.h"
#include "tiledb/sm/consolidator/fragment_meta_consolidator.h"
#include "tiledb/sm/consolidator/group_meta_consolidator.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ********************************* */
/*          FACTORY METHODS          */
/* ********************************* */

/** Factory function to create the consolidator depending on mode. */
shared_ptr<Consolidator> Consolidator::create(
    const ConsolidationMode mode,
    const Config& config,
    StorageManager* storage_manager) {
  switch (mode) {
    case ConsolidationMode::FRAGMENT_META:
      return make_shared<FragmentMetaConsolidator<context_bypass_RM>>(
          HERE(), storage_manager);
    case ConsolidationMode::FRAGMENT:
      return make_shared<FragmentConsolidator<context_bypass_RM>>(
          HERE(), config, storage_manager);
    case ConsolidationMode::ARRAY_META:
      return make_shared<ArrayMetaConsolidator<context_bypass_RM>>(
          HERE(), config, storage_manager);
    case ConsolidationMode::COMMITS:
      return make_shared<CommitsConsolidator<context_bypass_RM>>(
          HERE(), storage_manager);
    case ConsolidationMode::GROUP_META:
      return make_shared<GroupMetaConsolidator<context_bypass_RM>>(
          HERE(), config, storage_manager);
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

Consolidator::Consolidator(StorageManager* storage_manager)
    : storage_manager_(storage_manager)
    , stats_(storage_manager_->stats()->create_child("Consolidator"))
    , logger_(storage_manager_->logger()->clone("Consolidator", ++logger_id_)) {
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
  return logger_->status(
      Status_ConsolidatorError("Cannot consolidate; Invalid object"));
}

void Consolidator::vacuum([[maybe_unused]] const char* array_name) {
  throw Status_ConsolidatorError("Cannot vacuum; Invalid object");
}

void Consolidator::check_array_uri(const char* array_name) {
  if (URI(array_name).is_tiledb()) {
    throw std::logic_error("Consolidation is not supported for remote arrays.");
  }
}

}  // namespace tiledb::sm
