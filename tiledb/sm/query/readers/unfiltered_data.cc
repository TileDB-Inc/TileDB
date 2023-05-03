/**
 * @file   unfiltered_data.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file implements class UnfilteredData.
 */

#include "tiledb/sm/query/readers/unfiltered_data.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

void FragmentUnfilteredData::add_tiles(
    const unsigned f,
    const ReaderBase& reader,
    const uint64_t batch_size,
    const shared_ptr<FragmentMetadata>& fragment_metadata,
    const std::vector<ResultTile*>& result_tiles,
    const std::string& name,
    const bool var_sized,
    const bool nullable,
    const bool dups,
    size_t& rt_idx) {
  size_t current_block_size = 0;
  for (; rt_idx < result_tiles.size(); rt_idx++) {
    // Exit once we reach a tile that's not in this fragment.
    if (result_tiles[rt_idx]->frag_idx() != f) {
      break;
    }

    // Skip fields that don't have data.
    if (reader.skip_field(f, name)) {
      continue;
    }

    // Get the sizes for this tile.
    uint64_t t{result_tiles[rt_idx]->tile_idx()};
    uint64_t fixed_tile_size{fragment_metadata->tile_size(name, t)};
    uint64_t var_tile_size{
        var_sized ? fragment_metadata->tile_var_size(name, t) : 0};
    uint64_t validity_tile_size{
        nullable ?
            fragment_metadata->cell_num(t) * constants::cell_validity_size :
            0};
    uint64_t tile_size{fixed_tile_size + var_tile_size + validity_tile_size};

    // The last tile will get special treatment if it's in a no dups array and
    // this is a fragment consolidated with timestamps.
    const bool last_tile = rt_idx == result_tiles.size() - 1 ||
                           result_tiles[rt_idx + 1]->frag_idx() != f;
    const bool force_last_tile_single_block =
        last_tile && fragment_metadata->has_timestamps() && dups;

    // Start a new block if required.
    uint64_t data_block_index{data_blocks_.size()};
    uint64_t data_block_offset{current_block_size};
    if (force_last_tile_single_block ||
        current_block_size + tile_size > batch_size) {
      if (current_block_size == 0) {
        // Make a block with this tile only and start a new empty one.
        data_blocks_.emplace_back(tile_size);
      } else {
        // Push the currently full block and start a new one for this tile.
        data_blocks_.emplace_back(current_block_size);
        data_block_index++;
        current_block_size = tile_size;
        data_block_offset = 0;
      }
    } else {
      // Add the tile to the current block.
      current_block_size += tile_size;
    }

    // Add the index for this tile.
    unfiltered_data_offsets_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(t),
        std::forward_as_tuple(
            data_block_index,
            data_block_offset,
            fixed_tile_size,
            var_tile_size));
  }

  // Add the last data block.
  if (current_block_size != 0) {
    data_blocks_.emplace_back(current_block_size);
  }
}

}  // namespace sm
}  // namespace tiledb
