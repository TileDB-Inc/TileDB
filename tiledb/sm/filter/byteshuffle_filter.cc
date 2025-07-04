/**
 * @file   byteshuffle_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines class ByteshuffleFilter.
 */

#include "tiledb/sm/filter/byteshuffle_filter.h"
#include "tiledb/common/assert.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/tile/tile.h"

#include "blosc/tiledb-shuffle.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

ByteshuffleFilter::ByteshuffleFilter(Datatype filter_data_type)
    : Filter(FilterType::FILTER_BYTESHUFFLE, filter_data_type) {
}

ByteshuffleFilter* ByteshuffleFilter::clone_impl() const {
  return tdb_new(ByteshuffleFilter, filter_data_type_);
}

std::ostream& ByteshuffleFilter::output(std::ostream& os) const {
  os << "ByteShuffle";
  return os;
}

void ByteshuffleFilter::run_forward(
    const WriterTile& tile,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Output size does not change with this filter.
  throw_if_not_ok(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  passert(output_buf != nullptr);

  // Write the metadata
  auto parts = input->buffers();
  auto num_parts = (uint32_t)parts.size();
  uint32_t metadata_size = sizeof(uint32_t) + num_parts * sizeof(uint32_t);
  throw_if_not_ok(output_metadata->append_view(input_metadata));
  throw_if_not_ok(output_metadata->prepend_buffer(metadata_size));
  throw_if_not_ok(output_metadata->write(&num_parts, sizeof(uint32_t)));

  // Shuffle all parts
  for (const auto& part : parts) {
    auto part_size = (uint32_t)part.size();
    throw_if_not_ok(output_metadata->write(&part_size, sizeof(uint32_t)));

    throw_if_not_ok(shuffle_part(tile, &part, output_buf));

    if (output_buf->owns_data())
      output_buf->advance_size(part.size());
    output_buf->advance_offset(part.size());
  }
}

Status ByteshuffleFilter::shuffle_part(
    const WriterTile&, const ConstBuffer* part, Buffer* output) const {
  auto tile_type_size = static_cast<uint8_t>(datatype_size(filter_data_type_));

  blosc::shuffle(
      tile_type_size,
      part->size(),
      (uint8_t*)part->data(),
      (uint8_t*)output->cur_data());

  return Status::Ok();
}

Status ByteshuffleFilter::run_reverse(
    const Tile& tile,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  // Get number of parts
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  passert(output_buf != nullptr);

  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    RETURN_NOT_OK(unshuffle_part(tile, &part, output_buf));

    if (output_buf->owns_data())
      output_buf->advance_size(part_size);
    output_buf->advance_offset(part_size);
    input->advance_offset(part_size);
  }

  // Output metadata is a view on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

Status ByteshuffleFilter::unshuffle_part(
    const Tile&, const ConstBuffer* part, Buffer* output) const {
  auto tile_type_size = static_cast<uint8_t>(datatype_size(filter_data_type_));

  blosc::unshuffle(
      tile_type_size,
      part->size(),
      (uint8_t*)part->data(),
      (uint8_t*)output->cur_data());

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
