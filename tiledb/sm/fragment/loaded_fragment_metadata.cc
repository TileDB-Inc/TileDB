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
  assert(it != parent_fragment_.idx_map_.end());
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

uint64_t LoadedFragmentMetadata::file_var_offset(
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

uint64_t LoadedFragmentMetadata::persisted_tile_var_size(
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

uint64_t LoadedFragmentMetadata::tile_var_size(
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

void LoadedFragmentMetadata::load_tile_var_sizes(
    const EncryptionKey& encryption_key, const std::string& name) {
  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
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
        assert(parent_fragment_.idx_map_.count(lhs) > 0);
        assert(parent_fragment_.idx_map_.count(rhs) > 0);
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

std::vector<uint8_t>& LoadedFragmentMetadata::get_max(const std::string& name) {
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

void* LoadedFragmentMetadata::get_sum(const std::string& name) {
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

uint64_t LoadedFragmentMetadata::get_null_count(const std::string& name) {
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

uint64_t LoadedFragmentMetadata::get_tile_null_count(
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

const void* LoadedFragmentMetadata::get_tile_sum(
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

void LoadedFragmentMetadata::resize_tile_validity_offsets_vectors(
    uint64_t size) {
  tile_validity_offsets().resize(size);
}

uint64_t LoadedFragmentMetadata::file_validity_offset(
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

uint64_t LoadedFragmentMetadata::persisted_tile_validity_size(
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

// TODO: maybe remove, this is unused at the moment
void LoadedFragmentMetadata::free_rtree() {
  auto freed = rtree_.free_memory();
  if (memory_tracker_ != nullptr) {
    memory_tracker_->release_memory(freed, MemoryType::RTREE);
  }
  loaded_metadata_.rtree_ = false;
}

void LoadedFragmentMetadata::set_mbr(
    uint64_t base, uint64_t tile, const NDRange& mbr) {
  rtree_.set_leaf(tile + base, mbr);
}

template <typename T>
T LoadedFragmentMetadata::get_tile_min_as(
    const std::string& name, uint64_t tile_idx) const {
  const auto var_size = parent_fragment_.array_schema_->var_size(name);
  if (var_size) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata as wrong type");
  }

  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_min_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata that's not loaded");
  }

  const auto type = parent_fragment_.array_schema_->type(name);
  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata that's not present");
  }

  auto size = parent_fragment_.array_schema_->cell_size(name);
  const void* min = &tile_min_buffer()[idx][tile_idx * size];
  if constexpr (std::is_same_v<T, const void*>) {
    return min;
  } else {
    return *static_cast<const T*>(min);
  }
}

template <>
std::string_view LoadedFragmentMetadata::get_tile_min_as<std::string_view>(
    const std::string& name, uint64_t tile_idx) const {
  const auto type = parent_fragment_.array_schema_->type(name);
  const auto var_size = parent_fragment_.array_schema_->var_size(name);
  if (!var_size && type != Datatype::STRING_ASCII && type != Datatype::CHAR) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata as wrong type");
  }

  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_min_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata that's not loaded");
  }

  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile min metadata that's not present");
  }

  using sv_size_cast = std::string_view::size_type;
  if (var_size) {
    auto tile_num = this->tile_num();
    auto offsets = (uint64_t*)tile_min_buffer_[idx].data();
    auto min_offset = offsets[tile_idx];
    auto size =
        tile_idx == tile_num - 1 ?
            static_cast<sv_size_cast>(
                tile_min_var_buffer_()[idx].size() - min_offset) :
            static_cast<sv_size_cast>(offsets[tile_idx + 1] - min_offset);
    if (size == 0) {
      return {};
    }

    const char* min = &tile_min_var_buffer_[idx][min_offset];
    return {min, size};
  } else {
    auto size = static_cast<sv_size_cast>(
        parent_fragment_.array_schema_->cell_size(name));
    const void* min = &tile_min_buffer_[idx][tile_idx * size];
    return {static_cast<const char*>(min), size};
  }
}

TileMetadata LoadedFragmentMetadata::get_tile_metadata(
    const std::string& name, const uint64_t tile_idx) const {
  auto var_size = parent_fragment_.array_schema_->var_size(name);
  auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  auto count = parent_fragment_.cell_num(tile_idx);

  if (name == constants::count_of_rows) {
    return {count, 0, nullptr, 0, nullptr, 0, nullptr};
  }

  uint64_t null_count = 0;
  if (parent_fragment_.array_schema_->is_nullable(name)) {
    null_count = get_tile_null_count(name, tile_idx);
  }

  unsigned dim_idx = 0;
  const NDRange* mbr = nullptr;
  if (is_dim) {
    dim_idx =
        parent_fragment_.array_schema_->domain().get_dimension_index(name);
    mbr = &rtree().leaf(tile_idx);
  }

  if (var_size) {
    std::string_view min =
        is_dim ? mbr->at(dim_idx).start_str() :
                 get_tile_min_as<std::string_view>(name, tile_idx);
    std::string_view max =
        is_dim ? mbr->at(dim_idx).end_str() :
                 get_tile_max_as<std::string_view>(name, tile_idx);
    return {
        count,
        null_count,
        min.data(),
        min.size(),
        max.data(),
        max.size(),
        nullptr};
  } else {
    auto cell_size = parent_fragment_.array_schema_->cell_size(name);
    const void* min = is_dim ? mbr->at(dim_idx).start_fixed() :
                               get_tile_min_as<const void*>(name, tile_idx);
    const void* max = is_dim ? mbr->at(dim_idx).end_fixed() :
                               get_tile_max_as<const void*>(name, tile_idx);

    const auto type = parent_fragment_.array_schema_->type(name);
    const auto cell_val_num =
        parent_fragment_.array_schema_->cell_val_num(name);
    const void* sum = nullptr;
    if (TileMetadataGenerator::has_sum_metadata(type, false, cell_val_num)) {
      sum = get_tile_sum(name, tile_idx);
    }

    return {count, null_count, min, cell_size, max, cell_size, sum};
  }
}
template <typename T>
T LoadedFragmentMetadata::get_tile_max_as(
    const std::string& name, uint64_t tile_idx) const {
  const auto var_size = parent_fragment_.array_schema_->var_size(name);
  if (var_size) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata as wrong type");
  }

  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (loaded_metadata_.tile_max_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata that's not loaded");
  }

  const auto type = parent_fragment_.array_schema_->type(name);
  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata that's not present");
  }

  auto size = parent_fragment_.array_schema_->cell_size(name);
  const void* max = &tile_max_buffer_[idx][tile_idx * size];
  if constexpr (std::is_same_v<T, const void*>) {
    return max;
  } else {
    return *static_cast<const T*>(max);
  }
}

template <>
std::string_view LoadedFragmentMetadata::get_tile_max_as<std::string_view>(
    const std::string& name, uint64_t tile_idx) const {
  const auto type = parent_fragment_.array_schema_->type(name);
  const auto var_size = parent_fragment_.array_schema_->var_size(name);
  if (!var_size && type != Datatype::STRING_ASCII && type != Datatype::CHAR) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata as wrong type");
  }

  auto it = parent_fragment_.idx_map_.find(name);
  assert(it != parent_fragment_.idx_map_.end());
  auto idx = it->second;
  if (!loaded_metadata_.tile_max_[idx]) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata that's not loaded");
  }

  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  if (!TileMetadataGenerator::has_min_max_metadata(
          type, is_dim, var_size, cell_val_num)) {
    throw FragmentMetadataStatusException(
        "Trying to access tile max metadata that's not present");
  }

  using sv_size_cast = std::string_view::size_type;
  if (var_size) {
    auto tile_num = parent_fragment_.tile_num();
    auto offsets = (uint64_t*)tile_max_buffer_[idx].data();
    auto max_offset = offsets[tile_idx];
    auto size =
        tile_idx == tile_num - 1 ?
            static_cast<sv_size_cast>(
                tile_max_var_buffer_[idx].size() - max_offset) :
            static_cast<sv_size_cast>(offsets[tile_idx + 1] - max_offset);
    if (size == 0) {
      return {};
    }

    const char* max = &tile_max_var_buffer_[idx][max_offset];
    return {max, size};
  } else {
    auto size = static_cast<sv_size_cast>(
        parent_fragment_.array_schema_->cell_size(name));
    const void* max = &tile_max_buffer_[idx][tile_idx * size];
    return {static_cast<const char*>(max), size};
  }
}

void LoadedFragmentMetadata::set_processed_conditions(
    std::vector<std::string>& processed_conditions) {
  processed_conditions_ = processed_conditions;
  processed_conditions_set_ = std::unordered_set<std::string>(
      processed_conditions.begin(), processed_conditions.end());
}

template <class T>
void LoadedMetadataFragmentMetadata::compute_fragment_min_max_sum(
    const std::string& name) {
  // For easy reference.
  const auto& idx = parent_fragment_.idx_map_[name];
  const auto nullable = parent_fragment_.array_schema_->is_nullable(name);
  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto type = parent_fragment_.array_schema_->type(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);

  // No metadata for dense coords
  if (!parent_fragment_.array_schema_->dense() || !is_dim) {
    const auto has_min_max = TileMetadataGenerator::has_min_max_metadata(
        type, is_dim, false, cell_val_num);
    const auto has_sum =
        TileMetadataGenerator::has_sum_metadata(type, false, cell_val_num);

    if (has_min_max) {
      // Initialize defaults.
      T min = metadata_generator_type_data<T>::min;
      T max = metadata_generator_type_data<T>::max;

      // Get data and tile num.
      auto min_values =
          static_cast<T*>(static_cast<void*>(tile_min_buffer()[idx].data()));
      auto max_values =
          static_cast<T*>(static_cast<void*>(tile_max_buffer()[idx].data()));
      auto& null_count_values = tile_null_counts()[idx];
      auto tile_num = this->tile_num();

      // Process tile by tile.
      for (uint64_t t = 0; t < tile_num; t++) {
        const bool is_null = nullable && null_count_values[t] == cell_num(t);
        if (!is_null) {
          min = min < min_values[t] ? min : min_values[t];
          max = max > max_values[t] ? max : max_values[t];
        }
      }

      // Copy min max values.
      fragment_mins()[idx].resize(sizeof(T));
      fragment_maxs()[idx].resize(sizeof(T));
      memcpy(fragment_mins()[idx].data(), &min, sizeof(T));
      memcpy(fragment_maxs()[idx].data(), &max, sizeof(T));
    }

    if (has_sum) {
      compute_fragment_sum<typename metadata_generator_type_data<T>::sum_type>(
          idx, nullable);
    }
  }
}

template <>
void LoadedMetadataFragmentMetadata::compute_fragment_min_max_sum<char>(
    const std::string& name) {
  // For easy reference.
  const auto idx = parent_fragment_.idx_map_[name];
  const auto nullable = parent_fragment_.array_schema_->is_nullable(name);
  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto type = parent_fragment_.array_schema_->type(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);

  // Return if there's no min/max.
  const auto has_min_max = TileMetadataGenerator::has_min_max_metadata(
      type, is_dim, false, cell_val_num);
  if (!has_min_max)
    return;

  // Initialize to null.
  void* min = nullptr;
  void* max = nullptr;

  // Get data and tile num.
  auto min_values = tile_min_buffer()[idx].data();
  auto max_values = tile_max_buffer()[idx].data();
  auto& null_count_values = tile_null_counts()[idx];
  auto tile_num = parent_fragment_.tile_num();

  // Process tile by tile.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      min = (min == nullptr ||
             strncmp((const char*)min, (const char*)min_values, cell_val_num) >
                 0) ?
                min_values :
                min;
      min_values += cell_val_num;
      max = (max == nullptr ||
             strncmp((const char*)max, (const char*)max_values, cell_val_num) <
                 0) ?
                max_values :
                max;
      max_values += cell_val_num;
    }
  }

  // Copy values.
  if (min != nullptr) {
    fragment_mins()[idx].resize(cell_val_num);
    memcpy(fragment_mins()[idx].data(), min, cell_val_num);
  }

  if (max != nullptr) {
    fragment_maxs()[idx].resize(cell_val_num);
    memcpy(fragment_maxs()[idx].data(), max, cell_val_num);
  }
}

template <>
void LoadedMetadataFragmentMetadata::compute_fragment_sum<int64_t>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  int64_t sum_data = 0;

  // Get data and tile num.
  auto values =
      static_cast<int64_t*>(static_cast<void*>(tile_sums()[idx].data()));
  auto& null_count_values = tile_null_counts()[idx];
  auto tile_num = parent_fragment_.tile_num();

  // Process tile by tile, swallowing overflow exception.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      if (sum_data > 0 && values[t] > 0 &&
          (sum_data > std::numeric_limits<int64_t>::max() - values[t])) {
        sum_data = std::numeric_limits<int64_t>::max();
        break;
      }

      if (sum_data < 0 && values[t] < 0 &&
          (sum_data < std::numeric_limits<int64_t>::min() - values[t])) {
        sum_data = std::numeric_limits<int64_t>::min();
        break;
      }

      sum_data += values[t];
    }
  }

  // Copy value.
  memcpy(&fragment_sums()[idx], &sum_data, sizeof(int64_t));
}

template <>
void LoadedMetadataFragmentMetadata::compute_fragment_sum<uint64_t>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  uint64_t sum_data = 0;

  // Get data and tile num.
  auto values =
      static_cast<uint64_t*>(static_cast<void*>(tile_sums()[idx].data()));
  auto& null_count_values = tile_null_counts()[idx];
  auto tile_num = parent_fragment_.tile_num();

  // Process tile by tile, swallowing overflow exception.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      if (sum_data > std::numeric_limits<uint64_t>::max() - values[t]) {
        sum_data = std::numeric_limits<uint64_t>::max();
        break;
      }

      sum_data += values[t];
    }
  }

  // Copy value.
  memcpy(&fragment_sums()[idx], &sum_data, sizeof(uint64_t));
}

template <>
void LoadedMetadataFragmentMetadata::compute_fragment_sum<double>(
    const uint64_t idx, const bool nullable) {
  // Zero sum.
  double sum_data = 0;

  // Get data and tile num.
  auto values =
      static_cast<double*>(static_cast<void*>(tile_sums()[idx].data()));
  auto& null_count_values = tile_null_counts()[idx];
  auto tile_num = parent_fragment_.tile_num();

  // Process tile by tile, swallowing overflow exception.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      if ((sum_data < 0.0) == (values[t] < 0.0) &&
          std::abs(sum_data) >
              std::numeric_limits<double>::max() - std::abs(values[t])) {
        sum_data = sum_data < 0.0 ? std::numeric_limits<double>::lowest() :
                                    std::numeric_limits<double>::max();
        break;
      }

      sum_data += values[t];
    }
  }

  // Copy value.
  memcpy(&fragment_sums()[idx], &sum_data, sizeof(double));
}

template <>
void LoadedMetadataFragmentMetadata::compute_fragment_min_max_sum<char>(
    const std::string& name);

void LoadedMetadataFragmentMetadata::compute_fragment_min_max_sum_null_count() {
  std::vector<std::string> names;
  names.reserve(idx_map_.size());
  for (auto& it : parent_fragment_.idx_map_) {
    names.emplace_back(it.first);
  }

  // Process all attributes in parallel.
  throw_if_not_ok(parallel_for(
      &resources_->compute_tp(),
      0,
      parent_fragment_.idx_map_.size(),
      [&](uint64_t n) {
        // For easy reference.
        const auto& name = names[n];
        const auto& idx = parent_fragment_.idx_map_[name];
        const auto var_size = parent_fragment_.array_schema_->var_size(name);
        const auto type = parent_fragment_.array_schema_->type(name);

        // Compute null count.
        fragment_null_counts()[idx] = std::accumulate(
            tile_null_counts()[idx].begin(), tile_null_counts()[idx].end(), 0);

        if (var_size) {
          min_max_var(name);
        } else {
          // Switch depending on datatype.
          switch (type) {
            case Datatype::INT8:
              compute_fragment_min_max_sum<int8_t>(name);
              break;
            case Datatype::INT16:
              compute_fragment_min_max_sum<int16_t>(name);
              break;
            case Datatype::INT32:
              compute_fragment_min_max_sum<int32_t>(name);
              break;
            case Datatype::INT64:
              compute_fragment_min_max_sum<int64_t>(name);
              break;
            case Datatype::BOOL:
            case Datatype::UINT8:
              compute_fragment_min_max_sum<uint8_t>(name);
              break;
            case Datatype::UINT16:
              compute_fragment_min_max_sum<uint16_t>(name);
              break;
            case Datatype::UINT32:
              compute_fragment_min_max_sum<uint32_t>(name);
              break;
            case Datatype::UINT64:
              compute_fragment_min_max_sum<uint64_t>(name);
              break;
            case Datatype::FLOAT32:
              compute_fragment_min_max_sum<float>(name);
              break;
            case Datatype::FLOAT64:
              compute_fragment_min_max_sum<double>(name);
              break;
            case Datatype::DATETIME_YEAR:
            case Datatype::DATETIME_MONTH:
            case Datatype::DATETIME_WEEK:
            case Datatype::DATETIME_DAY:
            case Datatype::DATETIME_HR:
            case Datatype::DATETIME_MIN:
            case Datatype::DATETIME_SEC:
            case Datatype::DATETIME_MS:
            case Datatype::DATETIME_US:
            case Datatype::DATETIME_NS:
            case Datatype::DATETIME_PS:
            case Datatype::DATETIME_FS:
            case Datatype::DATETIME_AS:
            case Datatype::TIME_HR:
            case Datatype::TIME_MIN:
            case Datatype::TIME_SEC:
            case Datatype::TIME_MS:
            case Datatype::TIME_US:
            case Datatype::TIME_NS:
            case Datatype::TIME_PS:
            case Datatype::TIME_FS:
            case Datatype::TIME_AS:
              compute_fragment_min_max_sum<int64_t>(name);
              break;
            case Datatype::STRING_ASCII:
            case Datatype::CHAR:
              compute_fragment_min_max_sum<char>(name);
              break;
            case Datatype::BLOB:
            case Datatype::GEOM_WKB:
            case Datatype::GEOM_WKT:
              compute_fragment_min_max_sum<std::byte>(name);
              break;
            default:
              break;
          }
        }

        return Status::Ok();
      }));
}

void LoadedFragmentMetadata::min_max_var(const std::string& name) {
  // For easy reference.
  const auto nullable = parent_fragment_.array_schema_->is_nullable(name);
  const auto is_dim = parent_fragment_.array_schema_->is_dim(name);
  const auto type = parent_fragment_.array_schema_->type(name);
  const auto cell_val_num = parent_fragment_.array_schema_->cell_val_num(name);
  const auto idx = parent_fragment_.idx_map_[name];

  // Return if there's no min/max.
  const auto has_min_max = TileMetadataGenerator::has_min_max_metadata(
      type, is_dim, true, cell_val_num);
  if (!has_min_max)
    return;

  // Initialize to null.
  void* min = nullptr;
  void* max = nullptr;
  uint64_t min_size = 0;
  uint64_t max_size = 0;

  // Get data and tile num.
  auto min_offsets =
      static_cast<uint64_t*>(static_cast<void*>(tile_min_buffer()[idx].data()));
  auto max_offsets =
      static_cast<uint64_t*>(static_cast<void*>(tile_max_buffer()[idx].data()));
  auto min_values = tile_min_var_buffer()[idx].data();
  auto max_values = tile_max_var_buffer()[idx].data();
  auto& null_count_values = tile_null_counts()[idx];
  auto tile_num = this->tile_num();

  // Process tile by tile.
  for (uint64_t t = 0; t < tile_num; t++) {
    if (!nullable || null_count_values[t] != cell_num(t)) {
      auto min_value = min_values + min_offsets[t];
      auto min_value_size =
          t == tile_num - 1 ?
              tile_min_var_buffer()[idx].size() - min_offsets[t] :
              min_offsets[t + 1] - min_offsets[t];
      auto max_value = max_values + max_offsets[t];
      auto max_value_size =
          t == tile_num - 1 ?
              tile_max_var_buffer()[idx].size() - max_offsets[t] :
              max_offsets[t + 1] - max_offsets[t];
      if (min == nullptr && max == nullptr) {
        min = min_value;
        min_size = min_value_size;
        max = max_value;
        max_size = max_value_size;
      } else {
        // Process min.
        size_t min_cmp_size = std::min<size_t>(min_size, min_value_size);
        int cmp =
            strncmp(static_cast<const char*>(min), min_value, min_cmp_size);
        if (cmp != 0) {
          if (cmp > 0) {
            min = min_value;
            min_size = min_value_size;
          }
        } else {
          if (min_value_size < min_size) {
            min = min_value;
            min_size = min_value_size;
          }
        }

        // Process max.
        size_t max_cmp_size = std::min<size_t>(max_size, max_value_size);
        cmp = strncmp(static_cast<const char*>(max), max_value, max_cmp_size);
        if (cmp != 0) {
          if (cmp < 0) {
            max = max_value;
            max_size = max_value_size;
          }
        } else {
          if (max_value_size > max_size) {
            max = max_value;
            max_size = max_value_size;
          }
        }
      }
    }
  }

  // Copy values.
  if (min != nullptr) {
    fragment_mins()[idx].resize(min_size);
    memcpy(fragment_mins()[idx].data(), min, min_size);
  }

  if (max != nullptr) {
    fragment_maxs()[idx].resize(max_size);
    memcpy(fragment_maxs()[idx].data(), max, max_size);
  }
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

// Explicit template instantiations
template int8_t LoadedFragmentMetadata::get_tile_min_as<int8_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint8_t LoadedFragmentMetadata::get_tile_min_as<uint8_t>(
    const std::string& name, uint64_t tile_idx) const;
template int16_t LoadedFragmentMetadata::get_tile_min_as<int16_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint16_t LoadedFragmentMetadata::get_tile_min_as<uint16_t>(
    const std::string& name, uint64_t tile_idx) const;
template int32_t LoadedFragmentMetadata::get_tile_min_as<int32_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint32_t LoadedFragmentMetadata::get_tile_min_as<uint32_t>(
    const std::string& name, uint64_t tile_idx) const;
template int64_t LoadedFragmentMetadata::get_tile_min_as<int64_t>(
    const std::string& name, uint64_t tile_idx) const;
template char LoadedFragmentMetadata::get_tile_min_as<char>(
    const std::string& name, uint64_t tile_idx) const;
template uint64_t LoadedFragmentMetadata::get_tile_min_as<uint64_t>(
    const std::string& name, uint64_t tile_idx) const;
template float LoadedFragmentMetadata::get_tile_min_as<float>(
    const std::string& name, uint64_t tile_idx) const;
template double LoadedFragmentMetadata::get_tile_min_as<double>(
    const std::string& name, uint64_t tile_idx) const;
template std::byte LoadedFragmentMetadata::get_tile_min_as<std::byte>(
    const std::string& name, uint64_t tile_idx) const;
template int8_t LoadedFragmentMetadata::get_tile_max_as<int8_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint8_t LoadedFragmentMetadata::get_tile_max_as<uint8_t>(
    const std::string& name, uint64_t tile_idx) const;
template int16_t LoadedFragmentMetadata::get_tile_max_as<int16_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint16_t LoadedFragmentMetadata::get_tile_max_as<uint16_t>(
    const std::string& name, uint64_t tile_idx) const;
template int32_t LoadedFragmentMetadata::get_tile_max_as<int32_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint32_t LoadedFragmentMetadata::get_tile_max_as<uint32_t>(
    const std::string& name, uint64_t tile_idx) const;
template int64_t LoadedFragmentMetadata::get_tile_max_as<int64_t>(
    const std::string& name, uint64_t tile_idx) const;
template uint64_t LoadedFragmentMetadata::get_tile_max_as<uint64_t>(
    const std::string& name, uint64_t tile_idx) const;
template float LoadedFragmentMetadata::get_tile_max_as<float>(
    const std::string& name, uint64_t tile_idx) const;
template double LoadedFragmentMetadata::get_tile_max_as<double>(
    const std::string& name, uint64_t tile_idx) const;
template std::byte LoadedFragmentMetadata::get_tile_max_as<std::byte>(
    const std::string& name, uint64_t tile_idx) const;
template char LoadedFragmentMetadata::get_tile_max_as<char>(
    const std::string& name, uint64_t tile_idx) const;

}  // namespace tiledb::sm
