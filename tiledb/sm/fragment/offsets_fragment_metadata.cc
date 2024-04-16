/**
 * @file   offsets_fragment_metadata.cc
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
 * This file implements the OffsetsFragmentMetadata class.
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
#include "tiledb/sm/fragment/offsets_fragment_metadata.h"
#include "tiledb/sm/fragment/ondemand_fragment_metadata.h"
#include "tiledb/sm/fragment/v1v2preloaded_fragment_metadata.h"
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

OffsetsFragmentMetadata::OffsetsFragmentMetadata(
    FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker)
    : parent_fragment_(parent)
    , memory_tracker_(memory_tracker)
    , rtree_(RTree(
          &parent.array_schema()->domain(),
          constants::rtree_fanout,
          memory_tracker_))
    , tile_offsets_(memory_tracker->get_resource(MemoryType::TILE_OFFSETS))
    , tile_var_offsets_(memory_tracker_->get_resource(MemoryType::TILE_OFFSETS))
    , tile_var_sizes_(memory_tracker_->get_resource(MemoryType::TILE_OFFSETS))
    , tile_validity_offsets_(
          memory_tracker_->get_resource(MemoryType::TILE_OFFSETS))
    , tile_min_buffer_(memory_tracker_->get_resource(MemoryType::TILE_MIN_VALS))
    , tile_min_var_buffer_(
          memory_tracker_->get_resource(MemoryType::TILE_MIN_VALS))
    , tile_max_buffer_(memory_tracker_->get_resource(MemoryType::TILE_MAX_VALS))
    , tile_max_var_buffer_(
          memory_tracker_->get_resource(MemoryType::TILE_MAX_VALS))
    , tile_sums_(memory_tracker_->get_resource(MemoryType::TILE_SUMS))
    , tile_null_counts_(
          memory_tracker_->get_resource(MemoryType::TILE_NULL_COUNTS)) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

OffsetsFragmentMetadata* OffsetsFragmentMetadata::create(
    FragmentMetadata& parent,
    shared_ptr<MemoryTracker> memory_tracker,
    format_version_t version) {
  if (version <= 2) {
    return new V1V2PreloadedFragmentMetadata(parent, memory_tracker);
  }

  return new OndemandFragmentMetadata(parent, memory_tracker);
}

uint64_t OffsetsFragmentMetadata::persisted_tile_size(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_offsets_[idx]) {
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

void OffsetsFragmentMetadata::load_tile_offsets(
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
      load_tile_var_offsets(encryption_key, parent_fragment_.idx_map_[name]);
    }
  }

  // Load all of the validity offsets.
  for (const auto& name : names) {
    if (parent_fragment_.array_schema_->is_nullable(name)) {
      load_tile_validity_offsets(
          encryption_key, parent_fragment_.idx_map_[name]);
    }
  }
}

void OffsetsFragmentMetadata::free_tile_offsets() {
  for (uint64_t i = 0; i < tile_offsets_.size(); i++) {
    std::lock_guard<std::mutex> lock(tile_offsets_mtx_[i]);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          tile_offsets_[i].size() * sizeof(uint64_t), MemoryType::TILE_OFFSETS);
    }
    tile_offsets_[i].clear();
    loaded_metadata_.tile_offsets_[i] = false;
  }

  for (uint64_t i = 0; i < tile_var_offsets_.size(); i++) {
    std::lock_guard<std::mutex> lock(tile_var_offsets_mtx_[i]);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          tile_var_offsets_[i].size() * sizeof(uint64_t),
          MemoryType::TILE_OFFSETS);
    }
    tile_var_offsets_[i].clear();
    loaded_metadata_.tile_var_offsets_[i] = false;
  }

  for (uint64_t i = 0; i < tile_offsets_.size(); i++) {
    std::lock_guard<std::mutex> lock(tile_offsets_mtx_[i]);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          tile_offsets_[i].size() * sizeof(uint64_t), MemoryType::TILE_OFFSETS);
    }
    tile_offsets_[i].clear();
    loaded_metadata_.tile_offsets_[i] = false;
  }

  for (uint64_t i = 0; i < tile_validity_offsets_.size(); i++) {
    std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          tile_validity_offsets_[i].size() * sizeof(uint64_t),
          MemoryType::TILE_OFFSETS);
    }
    tile_validity_offsets_[i].clear();
    loaded_metadata_.tile_validity_offsets_[i] = false;
  }

  for (uint64_t i = 0; i < tile_var_sizes_.size(); i++) {
    std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);
    if (memory_tracker_ != nullptr) {
      memory_tracker_->release_memory(
          tile_var_sizes_[i].size() * sizeof(uint64_t),
          MemoryType::TILE_OFFSETS);
    }
    tile_var_sizes_[i].clear();
    loaded_metadata_.tile_var_sizes_[i] = false;
  }
}

uint64_t OffsetsFragmentMetadata::file_offset(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access tile offsets metadata that's not loaded");
  }

  return tile_offsets_[idx][tile_idx];
}

void OffsetsFragmentMetadata::resize_tile_offsets_vectors(uint64_t size) {
  tile_offsets_mtx_.resize(size);
  tile_offsets_.resize(size);
}

void OffsetsFragmentMetadata::resize_tile_var_offsets_vectors(uint64_t size) {
  tile_var_offsets_mtx_.resize(size);
  tile_var_offsets_.resize(size);
}

void OffsetsFragmentMetadata::resize_offsets(uint64_t size) {
  resize_tile_offsets_vectors(size);
  resize_tile_var_offsets_vectors(size);
  resize_tile_var_sizes_vectors(size);
  tile_validity_offsets().resize(size);
  tile_min_buffer().resize(size);
  tile_min_var_buffer().resize(size);
  tile_max_buffer().resize(size);
  tile_max_var_buffer().resize(size);
  tile_sums().resize(size);
  tile_null_counts().resize(size);
  fragment_mins_.resize(size);
  fragment_maxs_.resize(size);
  fragment_sums_.resize(size);
  fragment_null_counts_.resize(size);
  loaded_metadata_.tile_offsets_.resize(size, false);
  loaded_metadata_.tile_var_offsets_.resize(size, false);
  loaded_metadata_.tile_var_sizes_.resize(size, false);
  loaded_metadata_.tile_validity_offsets_.resize(size, false);
  loaded_metadata_.tile_min_.resize(size, false);
  loaded_metadata_.tile_max_.resize(size, false);
  loaded_metadata_.tile_sum_.resize(size, false);
  loaded_metadata_.tile_null_count_.resize(size, false);
}

uint64_t OffsetsFragmentMetadata::file_var_offset(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_var_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access tile var offsets metadata that's not loaded");
  }

  return tile_var_offsets_[idx][tile_idx];
}

uint64_t OffsetsFragmentMetadata::persisted_tile_var_size(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;

  if (!loaded_metadata_.tile_var_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access persisted tile var offsets metadata that's not "
        "present");
  }

  auto tile_num = parent_fragment_.tile_num();

  auto tile_size = (tile_idx != tile_num - 1) ?
                       tile_var_offsets_[idx][tile_idx + 1] -
                           tile_var_offsets_[idx][tile_idx] :
                       parent_fragment_.file_var_sizes_[idx] -
                           tile_var_offsets_[idx][tile_idx];
  return tile_size;
}

uint64_t OffsetsFragmentMetadata::tile_var_size(
    const std::string& name, uint64_t tile_idx) {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_var_sizes_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile var size metadata that's not loaded");
  }

  return tile_var_sizes_[idx][tile_idx];
}

void OffsetsFragmentMetadata::load_tile_var_sizes(
    const EncryptionKey& encryption_key, const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  load_tile_var_sizes(encryption_key, idx);
}

void OffsetsFragmentMetadata::resize_tile_var_sizes_vectors(uint64_t size) {
  tile_var_sizes_.resize(size);
}

void OffsetsFragmentMetadata::load_tile_min_values(
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

  // Load all the min values.
  for (const auto& name : names) {
    load_tile_min_values(encryption_key, parent_fragment_.idx_map_[name]);
  }
}

void OffsetsFragmentMetadata::load_tile_max_values(
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

  // Load all the max values.
  for (const auto& name : names) {
    load_tile_max_values(encryption_key, parent_fragment_.idx_map_[name]);
  }
}

void OffsetsFragmentMetadata::load_tile_sum_values(
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

  // Load all the sum values.
  for (const auto& name : names) {
    load_tile_sum_values(encryption_key, parent_fragment_.idx_map_[name]);
  }
}

void OffsetsFragmentMetadata::load_tile_null_count_values(
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

  // Load all the null count values.
  for (const auto& name : names) {
    load_tile_null_count_values(
        encryption_key, parent_fragment_.idx_map_[name]);
  }
}

std::vector<std::string>& OffsetsFragmentMetadata::get_processed_conditions() {
  if (!loaded_metadata_.processed_conditions_) {
    throw std::logic_error(
        "Trying to access processed conditions metadata that's not present");
  }

  return processed_conditions_;
}

std::unordered_set<std::string>&
OffsetsFragmentMetadata::get_processed_conditions_set() {
  if (!loaded_metadata_.processed_conditions_) {
    throw std::logic_error(
        "Trying to access processed condition set metadata that's not present");
  }

  return processed_conditions_set_;
}

std::vector<uint8_t>& OffsetsFragmentMetadata::get_min(
    const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.fragment_min_max_sum_null_count_) {
    throw FragmentMetadataStatusException(
        "Trying to access fragment min metadata that's not loaded");
  }

  const auto type = parent_fragment_.array_schema_->type(name);
  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto var_size = parent_fragment_.array_schema_->var_size(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access fragment min metadata that's not present");
  }

  return fragment_mins_[idx];
}

std::vector<uint8_t>& OffsetsFragmentMetadata::get_max(
    const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.fragment_min_max_sum_null_count_) {
    throw FragmentMetadataStatusException(
        "Trying to access fragment max metadata that's not loaded");
  }

  const auto type = parent_fragment_.array_schema_->type(name);
  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto var_size = parent_fragment_.array_schema_->var_size(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access fragment max metadata that's not present");
  }

  return fragment_maxs_[idx];
}

void* OffsetsFragmentMetadata::get_sum(const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.fragment_min_max_sum_null_count_) {
    throw FragmentMetadataStatusException(
        "Trying to access fragment sum metadata that's not loaded");
  }

  const auto type = parent_fragment_.array_schema_->type(name);
  const auto var_size = parent_fragment_.array_schema_->var_size(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_sum_metadata(type, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access fragment sum metadata that's not present");
  }

  return &fragment_sums_[idx];
}

uint64_t OffsetsFragmentMetadata::get_null_count(const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.fragment_min_max_sum_null_count_) {
    throw FragmentMetadataStatusException(
        "Trying to access fragment null count metadata that's not loaded");
  }

  if (!parent_fragment_.array_schema_->is_nullable(name)) {
    throw FragmentMetadataStatusException(
        "Trying to access fragment null count metadata that's not present");
  }

  return fragment_null_counts_[idx];
}

uint64_t OffsetsFragmentMetadata::get_tile_null_count(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_null_count_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile null count metadata that's not loaded");
  }

  if (!parent_fragment_.array_schema_->is_nullable(name)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile null count metadata that's not present");
  }

  return tile_null_counts_[idx][tile_idx];
}

const void* OffsetsFragmentMetadata::get_tile_sum(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_sum_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile sum metadata that's not loaded");
  }

  auto type = parent_fragment_.array_schema_->type(name);
  auto var_size = parent_fragment_.array_schema_->var_size(name);
  auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_sum_metadata(type, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile sum metadata that's not present");
  }

  const void* sum = &tile_sums_[idx][tile_idx * sizeof(uint64_t)];
  return sum;
}

void OffsetsFragmentMetadata::resize_tile_validity_offsets_vectors(
    uint64_t size) {
  tile_validity_offsets().resize(size);
}

uint64_t OffsetsFragmentMetadata::file_validity_offset(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_validity_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access tile validity offsets metadata that's not loaded");
  }

  return tile_validity_offsets_[idx][tile_idx];
}

uint64_t OffsetsFragmentMetadata::persisted_tile_validity_size(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_validity_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access persisted tile validity offsets metadata that's not "
        "present");
  }

  auto tile_num = parent_fragment_.tile_num();

  auto tile_size = (tile_idx != tile_num - 1) ?
                       tile_validity_offsets_[idx][tile_idx + 1] -
                           tile_validity_offsets_[idx][tile_idx] :
                       parent_fragment_.file_validity_sizes_[idx] -
                           tile_validity_offsets_[idx][tile_idx];
  return tile_size;
}

void OffsetsFragmentMetadata::get_tile_overlap(
    const NDRange& range,
    std::vector<bool>& is_default,
    TileOverlap* tile_overlap) {
  if (rtree_.domain() == nullptr) {
    // For v1_v2 domain on rtree is empty
    *tile_overlap = TileOverlap();
    return;
  }

  assert(loaded_metadata_.rtree_);
  *tile_overlap = rtree_.get_tile_overlap(range, is_default);
}

void OffsetsFragmentMetadata::compute_tile_bitmap(
    const Range& range, unsigned d, std::vector<uint8_t>* tile_bitmap) {
  if (rtree_.domain() == nullptr) {
    // For v1_v2 domain on rtree is empty
    return;
  }

  assert(loaded_metadata_.rtree_);

  rtree_.compute_tile_bitmap(range, d, tile_bitmap);
}

// TODO: maybe remove, this is unused at the moment
void OffsetsFragmentMetadata::free_rtree() {
  auto freed = rtree_.free_memory();
  if (memory_tracker_ != nullptr) {
    memory_tracker_->release_memory(freed, MemoryType::RTREE);
  }
  loaded_metadata_.rtree_ = false;
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

}  // namespace tiledb::sm
