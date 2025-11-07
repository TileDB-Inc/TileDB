/**
 * @file   loaded_fragment_metadata.cc
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
 * This file implements the LoadedFragmentMetadata class.
 */

#include "tiledb/common/assert.h"
#include "tiledb/common/common.h"

#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/fragment/ondemand_fragment_metadata.h"
#include "tiledb/sm/fragment/v1v2preloaded_fragment_metadata.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

LoadedFragmentMetadata::LoadedFragmentMetadata(
    FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker)
    : parent_fragment_(parent)
    , memory_tracker_(memory_tracker)
    , rtree_(RTree(
          parent.array_schema() ? &parent.array_schema()->domain() : nullptr,
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

shared_ptr<LoadedFragmentMetadata> LoadedFragmentMetadata::create(
    FragmentMetadata& parent,
    shared_ptr<MemoryTracker> memory_tracker,
    format_version_t version) {
  if (version <= 2) {
    return make_shared<V1V2PreloadedFragmentMetadata>(
        HERE(), parent, memory_tracker);
  }

  return make_shared<OndemandFragmentMetadata>(HERE(), parent, memory_tracker);
}

uint64_t LoadedFragmentMetadata::persisted_tile_size(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

void LoadedFragmentMetadata::load_tile_offsets(
    const EncryptionKey& encryption_key, std::vector<std::string>& names) {
  sort_names_by_index(names);

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

void LoadedFragmentMetadata::free_tile_offsets() {
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

uint64_t LoadedFragmentMetadata::file_offset(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access tile offsets metadata that's not loaded");
  }

  return tile_offsets_[idx][tile_idx];
}

void LoadedFragmentMetadata::resize_tile_offsets_vectors(uint64_t size) {
  tile_offsets_mtx_.resize(size);
  tile_offsets_.resize(size);
}

void LoadedFragmentMetadata::resize_tile_var_offsets_vectors(uint64_t size) {
  tile_var_offsets_mtx_.resize(size);
  tile_var_offsets_.resize(size);
}

void LoadedFragmentMetadata::resize_offsets(uint64_t size) {
  resize_tile_offsets_vectors(size);
  resize_tile_var_offsets_vectors(size);
  resize_tile_var_sizes_vectors(size);
  tile_validity_offsets_.resize(size);
  tile_min_buffer_.resize(size);
  tile_min_var_buffer_.resize(size);
  tile_max_buffer_.resize(size);
  tile_max_var_buffer_.resize(size);
  tile_sums_.resize(size);
  tile_null_counts_.resize(size);
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

uint64_t LoadedFragmentMetadata::file_var_offset(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_var_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access tile var offsets metadata that's not loaded");
  }

  return tile_var_offsets_[idx][tile_idx];
}

uint64_t LoadedFragmentMetadata::persisted_tile_var_size(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

uint64_t LoadedFragmentMetadata::tile_var_size(
    const std::string& name, uint64_t tile_idx) {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_var_sizes_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile var size metadata that's not loaded");
  }

  return tile_var_sizes_[idx][tile_idx];
}

void LoadedFragmentMetadata::load_tile_var_sizes(
    const EncryptionKey& encryption_key, const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  load_tile_var_sizes(encryption_key, idx);
}

void LoadedFragmentMetadata::resize_tile_var_sizes_vectors(uint64_t size) {
  tile_var_sizes_.resize(size);
}

void LoadedFragmentMetadata::sort_names_by_index(
    std::vector<std::string>& names) {
  // Sort 'names' in ascending order of their index. The
  // motivation is to load the offsets in order of their
  // layout for sequential reads to the file.
  std::sort(
      names.begin(),
      names.end(),
      [&](const std::string& lhs, const std::string& rhs) {
        iassert(parent_fragment_.idx_map_.count(lhs) > 0);
        iassert(parent_fragment_.idx_map_.count(rhs) > 0);
        return parent_fragment_.idx_map_[lhs] < parent_fragment_.idx_map_[rhs];
      });
}

void LoadedFragmentMetadata::load_tile_min_values(
    const EncryptionKey& encryption_key, std::vector<std::string>& names) {
  // Load all the min values.
  for (const auto& name : names) {
    load_tile_min_values(encryption_key, parent_fragment_.idx_map_[name]);
  }
}

void LoadedFragmentMetadata::load_tile_max_values(
    const EncryptionKey& encryption_key, std::vector<std::string>& names) {
  sort_names_by_index(names);

  // Load all the max values.
  for (const auto& name : names) {
    load_tile_max_values(encryption_key, parent_fragment_.idx_map_[name]);
  }
}

void LoadedFragmentMetadata::load_tile_sum_values(
    const EncryptionKey& encryption_key, std::vector<std::string>& names) {
  sort_names_by_index(names);

  // Load all the sum values.
  for (const auto& name : names) {
    load_tile_sum_values(encryption_key, parent_fragment_.idx_map_[name]);
  }
}

void LoadedFragmentMetadata::load_tile_null_count_values(
    const EncryptionKey& encryption_key, std::vector<std::string>& names) {
  sort_names_by_index(names);

  // Load all the null count values.
  for (const auto& name : names) {
    load_tile_null_count_values(
        encryption_key, parent_fragment_.idx_map_[name]);
  }
}

std::vector<std::string>& LoadedFragmentMetadata::get_processed_conditions() {
  if (!loaded_metadata_.processed_conditions_) {
    throw std::logic_error(
        "Trying to access processed conditions metadata that's not present");
  }

  return processed_conditions_;
}

std::unordered_set<std::string>&
LoadedFragmentMetadata::get_processed_conditions_set() {
  if (!loaded_metadata_.processed_conditions_) {
    throw std::logic_error(
        "Trying to access processed condition set metadata that's not present");
  }

  return processed_conditions_set_;
}

std::vector<uint8_t>& LoadedFragmentMetadata::get_min(const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

std::vector<uint8_t>& LoadedFragmentMetadata::get_max(const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

void* LoadedFragmentMetadata::get_sum(const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

uint64_t LoadedFragmentMetadata::get_null_count(const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

uint64_t LoadedFragmentMetadata::get_tile_null_count(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

const void* LoadedFragmentMetadata::get_tile_sum(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

void LoadedFragmentMetadata::resize_tile_validity_offsets_vectors(
    uint64_t size) {
  tile_validity_offsets_.resize(size);
}

uint64_t LoadedFragmentMetadata::file_validity_offset(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_validity_offsets_[idx]) {
    throw std::logic_error(
        "Trying to access tile validity offsets metadata that's not loaded");
  }

  return tile_validity_offsets_[idx][tile_idx];
}

uint64_t LoadedFragmentMetadata::persisted_tile_validity_size(
    const std::string& name, uint64_t tile_idx) const {
  auto it = parent_fragment_.idx_map_.find(name);
  iassert(it != parent_fragment_.idx_map_.end());
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

// TODO: maybe remove, this is unused at the moment
void LoadedFragmentMetadata::free_rtree() {
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
