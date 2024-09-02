/**
 * @file filtered_tile_checker.cc
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
 */

#include "filtered_tile_checker.h"
#include <algorithm>

using namespace tiledb::common;

namespace tiledb::sm {

void ChunkChecker::check(
    const FilteredBuffer& buffer,
    const FilteredTileChunkInfo& buffer_info,
    uint64_t chunk_index) const {
  auto chunk_info = buffer_info.chunk_info(chunk_index);
  auto chunk_offset = buffer_info.chunk_offset(chunk_index);

  // Check the value of the original chunk length.
  CHECK(
      chunk_info.original_chunk_length() ==
      expected_chunk_info().original_chunk_length());

  // Check the metadata.
  check_metadata(buffer, chunk_info, chunk_offset);

  // Check the filtered chunk data.
  check_filtered_data(buffer, chunk_info, chunk_offset);
}

void FilteredTileChecker::check(const FilteredBuffer& buffer) const {
  // Get a summary of all chunk information.
  FilteredTileChunkInfo buffer_chunk_info{buffer};

  // Check the size of the filtered buffer matches the expected total size.
  CHECK(buffer.size() == buffer_chunk_info.size());

  // Check the number of chunks.
  auto nchunks_actual = buffer.value_at_as<uint64_t>(0);
  auto nchunks_expected = chunk_checkers_.size();
  CHECK(nchunks_actual == nchunks_expected);

  // Check each individual chunk.
  auto nchunks_check = std::min<uint64_t>(nchunks_actual, nchunks_expected);
  for (uint64_t chunk_index = 0; chunk_index < nchunks_check; ++chunk_index) {
    INFO("Chunk number: " << chunk_index);
    chunk_checkers_[chunk_index]->check(buffer, buffer_chunk_info, chunk_index);
  }
}

}  // namespace tiledb::sm
