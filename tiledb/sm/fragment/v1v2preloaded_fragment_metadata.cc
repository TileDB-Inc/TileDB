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

V1V2PreloadedFragmentMetadata::V1V2PreloadedFragmentMetadata(
    FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker)
    : OffsetsFragmentMetadata(parent, memory_tracker) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

void V1V2PreloadedFragmentMetadata::load_tile_offsets(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
}

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

void V1V2PreloadedFragmentMetadata::load_tile_var_offsets(
    const EncryptionKey&, unsigned) {
  // N/A for v1_v2 preloaded meta
  return;
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
    if (memory_tracker_ != nullptr &&
        !memory_tracker_->take_memory(size, MemoryType::TILE_OFFSETS)) {
      throw FragmentMetadataStatusException(
          "Cannot load tile var offsets; Insufficient memory budget; "
          "Needed " +
          std::to_string(size) + " but only had " +
          std::to_string(memory_tracker_->get_memory_available()) +
          " from budget " +
          std::to_string(memory_tracker_->get_memory_budget()));
    }

    // Get variable tile offsets
    tile_var_offsets_[i].resize(tile_var_offsets_num);
    deserializer.read(&tile_var_offsets_[i][0], size);
  }

  parent_fragment_.loaded_metadata_.tile_var_offsets_.resize(
      parent_fragment_.array_schema_->attribute_num(), true);
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

}  // namespace tiledb::sm
