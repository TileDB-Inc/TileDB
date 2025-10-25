/**
 * @file   ondemand_fragment_metadata.cc
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
 * This file implements the OndemandFragmentMetadata class.
 */

#include "tiledb/common/assert.h"
#include "tiledb/common/common.h"

#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/fragment/ondemand_fragment_metadata.h"
#include "tiledb/sm/query/readers/aggregators/tile_metadata.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

OndemandFragmentMetadata::OndemandFragmentMetadata(
    FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker)
    : LoadedFragmentMetadata(parent, memory_tracker) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

void OndemandFragmentMetadata::load_rtree(const EncryptionKey& encryption_key) {
  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  if (loaded_metadata_.rtree_ || parent_fragment_.dense()) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.rtree_);
  parent_fragment_.resources_->stats().add_counter(
      "read_rtree_size", tile->size());

  // Use the serialized buffer size to approximate memory usage of the rtree.
  if (memory_tracker_ != nullptr &&
      !memory_tracker_->take_memory(tile->size(), MemoryType::RTREE)) {
    throw FragmentMetadataStatusException(
        "Cannot load R-tree; Insufficient memory budget; Needed " +
        std::to_string(tile->size()) + " but only had " +
        std::to_string(memory_tracker_->get_memory_available()) +
        " from budget " + std::to_string(memory_tracker_->get_memory_budget()));
  }

  Deserializer deserializer(tile->data(), tile->size());
  rtree_.deserialize(
      deserializer,
      &parent_fragment_.array_schema_->domain(),
      parent_fragment_.version_);

  loaded_metadata_.rtree_ = true;
}

void OndemandFragmentMetadata::load_fragment_min_max_sum_null_count(
    const EncryptionKey& encryption_key) {
  if (loaded_metadata_.fragment_min_max_sum_null_count_) {
    return;
  }

  if (parent_fragment_.version_ <= 11) {
    return;
  }

  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key,
      parent_fragment_.gt_offsets_.fragment_min_max_sum_null_count_offset_);
  parent_fragment_.resources_->stats().add_counter(
      "read_fragment_min_max_sum_null_count_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_fragment_min_max_sum_null_count(deserializer);

  loaded_metadata_.fragment_min_max_sum_null_count_ = true;
}

void OndemandFragmentMetadata::load_processed_conditions(
    const EncryptionKey& encryption_key) {
  if (loaded_metadata_.processed_conditions_) {
    return;
  }

  if (parent_fragment_.version_ <= 15) {
    return;
  }

  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key,
      parent_fragment_.gt_offsets_.processed_conditions_offsets_);
  parent_fragment_.resources_->stats().add_counter(
      "read_processed_conditions_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_processed_conditions(deserializer);

  loaded_metadata_.processed_conditions_ = true;
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

void OndemandFragmentMetadata::load_tile_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  // If the tile offset is already loaded, exit early to avoid the lock
  if (loaded_metadata_.tile_offsets_[idx]) {
    return;
  }

  std::lock_guard<std::mutex> lock(tile_offsets_mtx_[idx]);

  if (loaded_metadata_.tile_offsets_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_offsets_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_tile_offsets(idx, deserializer);

  loaded_metadata_.tile_offsets_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_offsets(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_offsets_num = 0;

  // Get number of tile offsets
  tile_offsets_num = deserializer.read<uint64_t>();

  // Get tile offsets
  if (tile_offsets_num != 0) {
    auto size = tile_offsets_num * sizeof(uint64_t);

    // Get tile offsets
    tile_offsets_[idx].resize(tile_offsets_num);
    deserializer.read(&tile_offsets_[idx][0], size);
  }
}

void OndemandFragmentMetadata::load_tile_var_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  // If the tile var offset is already loaded, exit early to avoid the lock
  if (loaded_metadata_.tile_var_offsets_[idx]) {
    return;
  }

  std::lock_guard<std::mutex> lock(tile_var_offsets_mtx_[idx]);

  if (loaded_metadata_.tile_var_offsets_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_var_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_var_offsets_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_tile_var_offsets(idx, deserializer);

  loaded_metadata_.tile_var_offsets_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_var_offsets(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_var_offsets_num = 0;

  // Get number of tile offsets
  tile_var_offsets_num = deserializer.read<uint64_t>();

  // Get variable tile offsets
  if (tile_var_offsets_num != 0) {
    auto size = tile_var_offsets_num * sizeof(uint64_t);

    // Get tile var offsets
    tile_var_offsets_[idx].resize(tile_var_offsets_num);
    deserializer.read(&tile_var_offsets_[idx][0], size);
  }
}

void OndemandFragmentMetadata::load_tile_var_sizes(
    const EncryptionKey& encryption_key, unsigned idx) {
  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  if (loaded_metadata_.tile_var_sizes_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_var_sizes_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_var_sizes_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_tile_var_sizes(idx, deserializer);

  loaded_metadata_.tile_var_sizes_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_var_sizes(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_var_sizes_num = 0;

  // Get number of tile sizes
  tile_var_sizes_num = deserializer.read<uint64_t>();

  // Get variable tile sizes
  if (tile_var_sizes_num != 0) {
    auto size = tile_var_sizes_num * sizeof(uint64_t);

    // Get tile var sizes
    tile_var_sizes_[idx].resize(tile_var_sizes_num);
    deserializer.read(&tile_var_sizes_[idx][0], size);
  }
}

void OndemandFragmentMetadata::load_tile_validity_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (parent_fragment_.version_ <= 6) {
    return;
  }

  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  if (loaded_metadata_.tile_validity_offsets_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_validity_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_validity_offsets_size", tile->size());

  ConstBuffer cbuff(tile->data(), tile->size());
  load_tile_validity_offsets(idx, &cbuff);

  loaded_metadata_.tile_validity_offsets_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_validity_offsets(
    unsigned idx, ConstBuffer* buff) {
  uint64_t tile_validity_offsets_num = 0;

  // Get number of tile offsets
  if (!buff->read(&tile_validity_offsets_num, sizeof(uint64_t)).ok()) {
    throw FragmentMetadataStatusException(
        "Cannot load fragment metadata; Reading number of validity tile "
        "offsets failed");
  }

  // Get tile offsets
  if (tile_validity_offsets_num != 0) {
    auto size = tile_validity_offsets_num * sizeof(uint64_t);

    // Get tile validity offsets
    tile_validity_offsets_[idx].resize(tile_validity_offsets_num);
    if (!buff->read(&tile_validity_offsets_[idx][0], size).ok()) {
      throw FragmentMetadataStatusException(
          "Cannot load fragment metadata; Reading validity tile offsets "
          "failed");
    }
  }
}

// ===== FORMAT =====
// tile_min_values#0_size_buffer (uint64_t)
// tile_min_values#0_size_buffer_var (uint64_t)
// tile_min_values#0_buffer
// tile_min_values#0_buffer_var
// ...
// tile_min_values#<attribute_num-1>_size_buffer (uint64_t)
// tile_min_values#<attribute_num-1>_size_buffer_var (uint64_t)
// tile_min_values#<attribute_num-1>_buffer
// tile_min_values#<attribute_num-1>_buffer_var
void OndemandFragmentMetadata::load_tile_min_values(
    unsigned idx, Deserializer& deserializer) {
  uint64_t buffer_size = 0;
  uint64_t var_buffer_size = 0;

  // Get buffer size
  buffer_size = deserializer.read<uint64_t>();

  // Get var buffer size
  var_buffer_size = deserializer.read<uint64_t>();

  // Get tile mins
  if (buffer_size != 0) {
    auto size = buffer_size + var_buffer_size;
    if (memory_tracker_ != nullptr &&
        !memory_tracker_->take_memory(size, MemoryType::TILE_MIN_VALS)) {
      throw FragmentMetadataStatusException(
          "Cannot load min values; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    tile_min_buffer_[idx].resize(buffer_size);
    deserializer.read(&tile_min_buffer_[idx][0], buffer_size);

    if (var_buffer_size) {
      tile_min_var_buffer_[idx].resize(var_buffer_size);
      deserializer.read(&tile_min_var_buffer_[idx][0], var_buffer_size);
    }
  }
}

// ===== FORMAT =====
// tile_max_values#0_size_buffer (uint64_t)
// tile_max_values#0_size_buffer_var (uint64_t)
// tile_max_values#0_buffer
// tile_max_values#0_buffer_var
// ...
// tile_max_values#<attribute_num-1>_size_buffer (uint64_t)
// tile_max_values#<attribute_num-1>_size_buffer_var (uint64_t)
// tile_max_values#<attribute_num-1>_buffer
// tile_max_values#<attribute_num-1>_buffer_var
void OndemandFragmentMetadata::load_tile_max_values(
    unsigned idx, Deserializer& deserializer) {
  uint64_t buffer_size = 0;
  uint64_t var_buffer_size = 0;

  // Get buffer size
  buffer_size = deserializer.read<uint64_t>();

  // Get var buffer size
  var_buffer_size = deserializer.read<uint64_t>();

  // Get tile maxs
  if (buffer_size != 0) {
    auto size = buffer_size + var_buffer_size;
    if (memory_tracker_ != nullptr &&
        !memory_tracker_->take_memory(size, MemoryType::TILE_MAX_VALS)) {
      throw FragmentMetadataStatusException(
          "Cannot load max values; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    tile_max_buffer_[idx].resize(buffer_size);
    deserializer.read(&tile_max_buffer_[idx][0], buffer_size);

    if (var_buffer_size) {
      tile_max_var_buffer_[idx].resize(var_buffer_size);
      deserializer.read(&tile_max_var_buffer_[idx][0], var_buffer_size);
    }
  }
}

// ===== FORMAT =====
// tile_sum_values_attr#0_num (uint64_t)
// tile_sum_value_attr#0_#1 (uint64_t) tile_sum_value_attr#0_#2 (uint64_t)
// ...
// ...
// tile_sum_values_attr#<attribute_num-1>_num (uint64_t)
// tile_sum_value_attr#<attribute_num-1>_#1 (uint64_t)
//     tile_sum_value_attr#<attribute_num-1>_#2 (uint64_t) ...
void OndemandFragmentMetadata::load_tile_sum_values(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_sum_num = 0;

  // Get number of tile sums
  tile_sum_num = deserializer.read<uint64_t>();

  // Get tile sums
  if (tile_sum_num != 0) {
    auto size = tile_sum_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr &&
        !memory_tracker_->take_memory(size, MemoryType::TILE_SUMS)) {
      throw FragmentMetadataStatusException(
          "Cannot load sum values; Insufficient memory budget; Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    tile_sums_[idx].resize(size);
    deserializer.read(tile_sums_[idx].data(), size);
  }
}

// ===== FORMAT =====
// tile_nc_values_attr#0_num (uint64_t)
// tile_nc_value_attr#0_#1 (uint64_t) tile_nc_value_attr#0_#2 (uint64_t) ...
// ...
// tile_nc_values_attr#<attribute_num-1>_num (uint64_t)
// tile_nc_value_attr#<attribute_num-1>_#1 (uint64_t)
//     tile_nc_value_attr#<attribute_num-1>_#2 (uint64_t) ...
void OndemandFragmentMetadata::load_tile_null_count_values(
    unsigned idx, Deserializer& deserializer) {
  uint64_t tile_null_count_num = 0;

  // Get number of tile null counts
  tile_null_count_num = deserializer.read<uint64_t>();

  // Get tile null count
  if (tile_null_count_num != 0) {
    auto size = tile_null_count_num * sizeof(uint64_t);
    if (memory_tracker_ != nullptr &&
        !memory_tracker_->take_memory(size, MemoryType::TILE_NULL_COUNTS)) {
      throw FragmentMetadataStatusException(
          "Cannot load null count values; Insufficient memory budget; "
          "Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    tile_null_counts_[idx].resize(tile_null_count_num);
    deserializer.read(&tile_null_counts_[idx][0], size);
  }
}

// ===== FORMAT =====
// condition_num (uint64_t)
// processed_condition_size#0 (uint64_t)
// processed_condition#0
// ...
// processed_condition_size#<condition_num-1> (uint64_t)
// processed_condition#<condition_num-1>
void OndemandFragmentMetadata::load_processed_conditions(
    Deserializer& deserializer) {
  // Get num conditions.
  uint64_t num;
  num = deserializer.read<uint64_t>();

  processed_conditions_.reserve(num);
  for (uint64_t i = 0; i < num; i++) {
    uint64_t size;
    size = deserializer.read<uint64_t>();

    std::string condition;
    condition.resize(size);
    deserializer.read(condition.data(), size);

    processed_conditions_.emplace_back(condition);
  }

  processed_conditions_set_ = std::unordered_set<std::string>(
      processed_conditions_.begin(), processed_conditions_.end());
}

// ===== FORMAT =====
// fragment_min_size_attr#0 (uint64_t)
// fragment_min_attr#0 (min_size)
// fragment_max_size_attr#0 (uint64_t)
// fragment_max_attr#0 (max_size)
// fragment_sum_attr#0 (uint64_t)
// fragment_null_count_attr#0 (uint64_t)
// ...
// fragment_min_size_attr#<attribute_num-1> (uint64_t)
// fragment_min_attr#<attribute_num-1> (min_size)
// fragment_max_size_attr#<attribute_num-1> (uint64_t)
// fragment_max_attr#<attribute_num-1> (max_size)
// fragment_sum_attr#<attribute_num-1> (uint64_t)
// fragment_null_count_attr#<attribute_num-1> (uint64_t)
void OndemandFragmentMetadata::load_fragment_min_max_sum_null_count(
    Deserializer& deserializer) {
  auto num = parent_fragment_.num_dims_and_attrs();

  for (unsigned int i = 0; i < num; ++i) {
    // Get min.
    uint64_t min_size;
    min_size = deserializer.read<uint64_t>();

    fragment_mins_[i].resize(min_size);
    deserializer.read(fragment_mins_[i].data(), min_size);

    // Get max.
    uint64_t max_size;
    max_size = deserializer.read<uint64_t>();

    fragment_maxs_[i].resize(max_size);
    deserializer.read(fragment_maxs_[i].data(), max_size);

    // Get sum.
    fragment_sums_[i] = deserializer.read<uint64_t>();

    // Get null count.
    fragment_null_counts_[i] = deserializer.read<uint64_t>();
  }
}

void OndemandFragmentMetadata::load_tile_min_values(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (parent_fragment_.version_ < constants::tile_metadata_min_version) {
    return;
  }

  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  if (loaded_metadata_.tile_min_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_min_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_min_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_tile_min_values(idx, deserializer);

  loaded_metadata_.tile_min_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_max_values(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (parent_fragment_.version_ < constants::tile_metadata_min_version) {
    return;
  }

  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  if (loaded_metadata_.tile_max_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_max_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_max_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_tile_max_values(idx, deserializer);

  loaded_metadata_.tile_max_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_sum_values(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (parent_fragment_.version_ < constants::tile_metadata_min_version) {
    return;
  }

  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  if (loaded_metadata_.tile_sum_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_sum_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_sum_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_tile_sum_values(idx, deserializer);

  loaded_metadata_.tile_sum_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_null_count_values(
    const EncryptionKey& encryption_key, unsigned idx) {
  if (parent_fragment_.version_ < constants::tile_metadata_min_version) {
    return;
  }

  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  if (loaded_metadata_.tile_null_count_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key,
      parent_fragment_.gt_offsets_.tile_null_count_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_null_count_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  load_tile_null_count_values(idx, deserializer);

  loaded_metadata_.tile_null_count_[idx] = true;
}

void OndemandFragmentMetadata::get_tile_overlap(
    const NDRange& range,
    std::vector<bool>& is_default,
    TileOverlap* tile_overlap) {
  passert(loaded_metadata_.rtree_);
  *tile_overlap = rtree_.get_tile_overlap(range, is_default);
}

void OndemandFragmentMetadata::compute_tile_bitmap(
    const Range& range, unsigned d, std::vector<uint8_t>* tile_bitmap) {
  passert(loaded_metadata_.rtree_);
  rtree_.compute_tile_bitmap(range, d, tile_bitmap);
}

}  // namespace tiledb::sm
