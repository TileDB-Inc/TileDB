/**
 * @file   v1v2preloaded_fragment_metadata.cc
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
 * This file implements the V1V2PreloadedFragmentMetadata class.
 */

#include "tiledb/common/common.h"

#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/fragment/v1v2preloaded_fragment_metadata.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

V1V2PreloadedFragmentMetadata::V1V2PreloadedFragmentMetadata(
    FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker)
    : LoadedFragmentMetadata(parent, memory_tracker) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

// Applicable only to versions 1 and 2
void V1V2PreloadedFragmentMetadata::load_tile_offsets(
    Deserializer& deserializer) {
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

    // Get tile offsets
    tile_offsets_[i].resize(tile_offsets_num);
    deserializer.read(&tile_offsets_[i][0], size);
  }

  loaded_metadata_.tile_offsets_.resize(
      parent_fragment_.array_schema_->attribute_num() + 1, true);
}

// ===== FORMAT =====
// tile_var_offsets_attr#0_num (uint64_t)
// tile_var_offsets_attr#0_#1 (uint64_t) tile_var_offsets_attr#0_#2
// (uint64_t)
// ...
// ...
// tile_var_offsets_attr#<attribute_num-1>_num(uint64_t)
// tile_var_offsets_attr#<attribute_num-1>_#1 (uint64_t)
//     tile_ver_offsets_attr#<attribute_num-1>_#2 (uint64_t) ...
void V1V2PreloadedFragmentMetadata::load_tile_var_offsets(
    Deserializer& deserializer) {
  unsigned int attribute_num = parent_fragment_.array_schema_->attribute_num();
  uint64_t tile_var_offsets_num = 0;

  // Allocate tile offsets
  tile_var_offsets_.resize(attribute_num);
  tile_var_offsets_mtx_.resize(attribute_num);

  // For all attributes, get the variable tile offsets
  for (unsigned int i = 0; i < attribute_num; ++i) {
    // Get number of tile offsets
    tile_var_offsets_num = deserializer.read<uint64_t>();

    if (tile_var_offsets_num == 0)
      continue;

    auto size = tile_var_offsets_num * sizeof(uint64_t);

    // Get variable tile offsets
    tile_var_offsets_[i].resize(tile_var_offsets_num);
    deserializer.read(&tile_var_offsets_[i][0], size);
  }

  loaded_metadata_.tile_var_offsets_.resize(
      parent_fragment_.array_schema_->attribute_num(), true);
}

// ===== FORMAT =====
// tile_var_sizes_attr#0_num (uint64_t)
// tile_var_sizes_attr#0_#1 (uint64_t) tile_sizes_attr#0_#2 (uint64_t) ...
// ...
// tile_var_sizes_attr#<attribute_num-1>_num(uint64_t)
// tile_var_sizes__attr#<attribute_num-1>_#1 (uint64_t)
//     tile_var_sizes_attr#<attribute_num-1>_#2 (uint64_t) ...
void V1V2PreloadedFragmentMetadata::load_tile_var_sizes(
    Deserializer& deserializer) {
  unsigned int attribute_num = parent_fragment_.array_schema_->attribute_num();
  uint64_t tile_var_sizes_num = 0;

  // Allocate tile sizes
  tile_var_sizes_.resize(attribute_num);

  // For all attributes, get the variable tile sizes
  for (unsigned int i = 0; i < attribute_num; ++i) {
    // Get number of tile sizes
    tile_var_sizes_num = deserializer.read<uint64_t>();

    if (tile_var_sizes_num == 0)
      continue;

    auto size = tile_var_sizes_num * sizeof(uint64_t);

    // Get variable tile sizes
    tile_var_sizes_[i].resize(tile_var_sizes_num);
    deserializer.read(&tile_var_sizes_[i][0], size);
  }

  loaded_metadata_.tile_var_sizes_.resize(
      parent_fragment_.array_schema_->attribute_num(), true);
}

void V1V2PreloadedFragmentMetadata::load_rtree(const EncryptionKey&) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_fragment_min_max_sum_null_count(
    const EncryptionKey&) {
  // N/A for v1_v2 preloaded meta
  return;
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

void V1V2PreloadedFragmentMetadata::load_tile_offsets(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_tile_var_offsets(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_tile_var_sizes(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_tile_validity_offsets(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_tile_min_values(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_tile_max_values(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_tile_sum_values(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_tile_null_count_values(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::load_processed_conditions(
    const EncryptionKey&) {
  // N/A for v1_v2 preloaded meta
  return;
}

void V1V2PreloadedFragmentMetadata::get_tile_overlap(
    const NDRange& range,
    std::vector<bool>& is_default,
    TileOverlap* tile_overlap) {
  *tile_overlap = rtree_.get_tile_overlap(range, is_default);
}

void V1V2PreloadedFragmentMetadata::compute_tile_bitmap(
    const Range& range, unsigned d, std::vector<uint8_t>* tile_bitmap) {
  rtree_.compute_tile_bitmap(range, d, tile_bitmap);
}

}  // namespace tiledb::sm
