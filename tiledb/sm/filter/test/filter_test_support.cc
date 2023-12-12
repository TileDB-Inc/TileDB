/**
 * @file filter_test_support.cc
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

#include "filter_test_support.h"

using namespace tiledb::common;

namespace tiledb::sm {

ChunkInfo::ChunkInfo(
    uint32_t original_chunk_length,
    uint32_t filtered_chunk_length,
    uint32_t metadata_length)
    : original_chunk_length_{original_chunk_length}
    , filtered_chunk_length_{filtered_chunk_length}
    , metadata_length_{metadata_length} {
}

ChunkInfo::ChunkInfo(const FilteredBuffer& buffer, uint32_t chunk_offset)
    : original_chunk_length_{buffer.value_at_as<uint32_t>(chunk_offset)}
    , filtered_chunk_length_{buffer.value_at_as<uint32_t>(
          chunk_offset + sizeof(uint32_t))}
    , metadata_length_{
          buffer.value_at_as<uint32_t>(chunk_offset + 2 * sizeof(uint32_t))} {
}

FilteredBufferChunkInfo::FilteredBufferChunkInfo(const FilteredBuffer& buffer)
    : nchunks_{buffer.value_at_as<uint64_t>(0)}
    , chunk_info_{}
    , offsets_{} {
  chunk_info_.reserve(nchunks_);
  offsets_.reserve(nchunks_);
  uint64_t current_offset = sizeof(uint64_t);
  for (uint64_t index = 0; index < nchunks_; ++index) {
    chunk_info_.emplace_back(buffer, current_offset);
    offsets_.push_back(current_offset);
    current_offset += chunk_info_[index].size();
  }
  size_ = current_offset;
}

void ChunkCheckerBase::check(
    const FilteredBuffer& buffer,
    const FilteredBufferChunkInfo& buffer_info,
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

}  // namespace tiledb::sm
