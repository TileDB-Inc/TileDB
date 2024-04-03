/**
 * @file   ondemand_metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file implements the OndemandMetadata class.
 */

#include "tiledb/common/common.h"

#include <cassert>
#include <iostream>
#include <numeric>
#include <string>
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/fragment/ondemand_metadata.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/readers/aggregators/tile_metadata.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/storage_format/serialization/serializers.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

OndemandMetadata::OndemandMetadata(
    FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker)
    : parent_fragment_(parent)
    , memory_tracker_(memory_tracker)
    , tile_offsets_(memory_tracker->get_resource(MemoryType::TILE_OFFSETS)) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

uint64_t OndemandMetadata::persisted_tile_size(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!parent_fragment_.loaded_metadata_.tile_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access persisted tile offsets metadata that's not present");
  }

  auto tile_num = parent_fragment_.tile_num();

  auto tile_size =
      (tile_idx != tile_num - 1) ?
          tile_offsets_[idx][tile_idx + 1] - tile_offsets_[idx][tile_idx] :
          parent_fragment_.file_sizes_[idx] - tile_offsets_[idx][tile_idx];
  return tile_size;
}

void OndemandMetadata::load_tile_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (parent_fragment_.version_ <= 2) {
    return;
  }

  // If the tile offset is already loaded, exit early to avoid the lock
  if (parent_fragment_.loaded_metadata_.tile_offsets_[idx]) {
    return;
  }

  std::lock_guard<std::mutex> lock(tile_offsets_mtx_[idx]);

  if (parent_fragment_.loaded_metadata_.tile_offsets_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_offsets_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_tile_offsets(idx, deserializer);

  parent_fragment_.loaded_metadata_.tile_offsets_[idx] = true;
}

void OndemandMetadata::load_tile_offsets(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_offsets_num = 0;

  // Get number of tile offsets
  tile_offsets_num = deserializer.read<uint64_t>();

  // Get tile offsets
  if (tile_offsets_num != 0) {
    auto size = tile_offsets_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr &&
        !memory_tracker_->take_memory(size, MemoryType::TILE_OFFSETS)) {
      throw FragmentMetadataStatusException(
          "Cannot load tile offsets; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    tile_offsets_[idx].resize(tile_offsets_num);
    deserializer.read(&tile_offsets_[idx][0], size);
  }
}

void OndemandMetadata::load_tile_offsets(
    const EncryptionKey& encryption_key, std::vector<std::string>& names) {
  // Sort 'names' in ascending order of their index. The
  // motivation is to load the offsets in order of their
  // layout for sequential reads to the file.
  std::sort(
      names.begin(),
      names.end(),
      [&](const std::string& lhs, const std::string& rhs) {
        assert(parent_fragment_.idx_map_.count(lhs) > 0);
        assert(parent_fragment_.idx_map_.count(rhs) > 0);
        return parent_fragment_.idx_map_[lhs] < parent_fragment_.idx_map_[rhs];
      });

  // The fixed offsets are located before the
  // var offsets. Load all of the fixed offsets
  // first.
  for (const auto& name : names) {
    load_tile_offsets(encryption_key, parent_fragment_.idx_map_[name]);
  }

  // Load all of the var offsets.
  for (const auto& name : names) {
    if (parent_fragment_.array_schema_->var_size(name)) {
      parent_fragment_.load_tile_var_offsets(
          encryption_key, parent_fragment_.idx_map_[name]);
    }
  }

  // Load all of the var offsets.
  for (const auto& name : names) {
    if (parent_fragment_.array_schema_->is_nullable(name)) {
      parent_fragment_.load_tile_validity_offsets(
          encryption_key, parent_fragment_.idx_map_[name]);
    }
  }
}

// Applicable only to versions 1 and 2
void OndemandMetadata::load_tile_offsets(Deserializer& deserializer) {
  uint64_t tile_offsets_num = 0;
  unsigned int attribute_num = parent_fragment_.array_schema_->attribute_num();

  // Allocate tile offsets
  tile_offsets_.resize(attribute_num + 1);
  tile_offsets_mtx_.resize(attribute_num + 1);

  // For all attributes, get the tile offsets
  for (unsigned int i = 0; i < attribute_num + 1; ++i) {
    // Get number of tile offsets
    tile_offsets_num = deserializer.read<uint64_t>();

    if (tile_offsets_num == 0)
      continue;

    auto size = tile_offsets_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr &&
        !memory_tracker_->take_memory(size, MemoryType::TILE_OFFSETS)) {
      throw FragmentMetadataStatusException(
          "Cannot load tile offsets; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    // Get tile offsets
    tile_offsets_[i].resize(tile_offsets_num);
    deserializer.read(&tile_offsets_[i][0], size);
  }

  parent_fragment_.loaded_metadata_.tile_offsets_.resize(
      parent_fragment_.array_schema_->attribute_num() + 1, true);
}

void OndemandMetadata::free_tile_offsets() {
  for (uint64_t i = 0; i < tile_offsets_.size(); i++) {
    std::lock_guard<std::mutex> lock(tile_offsets_mtx_[i]);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          tile_offsets_[i].size() * sizeof(uint64_t), MemoryType::TILE_OFFSETS);
    }
    tile_offsets_[i].clear();
    parent_fragment_.loaded_metadata_.tile_offsets_[i] = false;
  }

  for (uint64_t i = 0; i < parent_fragment_.tile_var_offsets_.size(); i++) {
    std::lock_guard<std::mutex> lock(parent_fragment_.tile_var_offsets_mtx_[i]);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          parent_fragment_.tile_var_offsets_[i].size() * sizeof(uint64_t),
          MemoryType::TILE_OFFSETS);
    }
    parent_fragment_.tile_var_offsets_[i].clear();
    parent_fragment_.loaded_metadata_.tile_var_offsets_[i] = false;
  }

  for (uint64_t i = 0; i < tile_offsets_.size(); i++) {
    std::lock_guard<std::mutex> lock(tile_offsets_mtx_[i]);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          tile_offsets_[i].size() * sizeof(uint64_t), MemoryType::TILE_OFFSETS);
    }
    tile_offsets_[i].clear();
    parent_fragment_.loaded_metadata_.tile_offsets_[i] = false;
  }

  for (uint64_t i = 0; i < parent_fragment_.tile_validity_offsets_.size();
       i++) {
    std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          parent_fragment_.tile_validity_offsets_[i].size() * sizeof(uint64_t),
          MemoryType::TILE_OFFSETS);
    }
    parent_fragment_.tile_validity_offsets_[i].clear();
    parent_fragment_.loaded_metadata_.tile_validity_offsets_[i] = false;
  }

  for (uint64_t i = 0; i < parent_fragment_.tile_var_sizes_.size(); i++) {
    std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          parent_fragment_.tile_var_sizes_[i].size() * sizeof(uint64_t),
          MemoryType::TILE_OFFSETS);
    }
    parent_fragment_.tile_var_sizes_[i].clear();
    parent_fragment_.loaded_metadata_.tile_var_sizes_[i] = false;
  }
}

uint64_t OndemandMetadata::file_offset(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!parent_fragment_.loaded_metadata_.tile_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access tile offsets metadata that's not loaded");
  }

  return tile_offsets_[idx][tile_idx];
}

void OndemandMetadata::resize_tile_offsets_vectors(uint64_t size) {
  tile_offsets_mtx_.resize(size);
  tile_offsets_.resize(size);
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

}  // namespace tiledb::sm
