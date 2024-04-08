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
#include "tiledb/sm/fragment/ondemand_fragment_metadata.h"
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

OndemandFragmentMetadata::OndemandFragmentMetadata(
    FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker)
    : OffsetsFragmentMetadata(parent, memory_tracker) {
}

/* ********************************* */
/*                API                */
/* ********************************* */

void OndemandFragmentMetadata::load_tile_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
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
  OffsetsFragmentMetadata::load_tile_offsets(idx, deserializer);

  parent_fragment_.loaded_metadata_.tile_offsets_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_var_offsets(
    const EncryptionKey& encryption_key, unsigned idx) {
  // If the tile var offset is already loaded, exit early to avoid the lock
  if (parent_fragment_.loaded_metadata_.tile_var_offsets_[idx]) {
    return;
  }

  std::lock_guard<std::mutex> lock(tile_var_offsets_mtx_[idx]);

  if (parent_fragment_.loaded_metadata_.tile_var_offsets_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_var_offsets_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_var_offsets_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  OffsetsFragmentMetadata::load_tile_var_offsets(idx, deserializer);

  parent_fragment_.loaded_metadata_.tile_var_offsets_[idx] = true;
}

void OndemandFragmentMetadata::load_tile_var_sizes(
    const EncryptionKey& encryption_key, unsigned idx) {
  std::lock_guard<std::mutex> lock(parent_fragment_.mtx_);

  if (parent_fragment_.loaded_metadata_.tile_var_sizes_[idx]) {
    return;
  }

  auto tile = parent_fragment_.read_generic_tile_from_file(
      encryption_key, parent_fragment_.gt_offsets_.tile_var_sizes_[idx]);
  parent_fragment_.resources_->stats().add_counter(
      "read_tile_var_sizes_size", tile->size());

  Deserializer deserializer(tile->data(), tile->size());
  OffsetsFragmentMetadata::load_tile_var_sizes(idx, deserializer);

  parent_fragment_.loaded_metadata_.tile_var_sizes_[idx] = true;
}

/* ********************************* */
/*           PRIVATE METHODS         */
/* ********************************* */

}  // namespace tiledb::sm
