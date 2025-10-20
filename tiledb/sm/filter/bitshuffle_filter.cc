/**
 * @file   bitshuffle_filter.cc
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
 * This file defines class BitshuffleFilter.
 */

#include "tiledb/sm/filter/bitshuffle_filter.h"
#include "tiledb/common/assert.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/tile/tile.h"

#include <blosc2.h>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

BitshuffleFilter::BitshuffleFilter(Datatype filter_data_type)
    : Filter(FilterType::FILTER_BITSHUFFLE, filter_data_type) {
}

BitshuffleFilter* BitshuffleFilter::clone_impl() const {
  return tdb_new(BitshuffleFilter, filter_data_type_);
}

std::ostream& BitshuffleFilter::output(std::ostream& os) const {
  os << "BitShuffle";
  return os;
}

void BitshuffleFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type_size = static_cast<uint8_t>(datatype_size(filter_data_type_));

  // Output size does not change with this filter.
  throw_if_not_ok(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  passert(output_buf != nullptr);

  // Compute the list of parts to shuffle
  std::vector<ConstBuffer> parts;
  throw_if_not_ok(compute_parts(input, &parts));

  // Write the metadata
  auto num_parts = (uint32_t)parts.size();
  uint32_t metadata_size = sizeof(uint32_t) + num_parts * sizeof(uint32_t);
  throw_if_not_ok(output_metadata->append_view(input_metadata));
  throw_if_not_ok(output_metadata->prepend_buffer(metadata_size));
  throw_if_not_ok(output_metadata->write(&num_parts, sizeof(uint32_t)));

  // Shuffle all parts
  for (const auto& part : parts) {
    auto part_size = (uint32_t)part.size();
    throw_if_not_ok(output_metadata->write(&part_size, sizeof(uint32_t)));

    if (part_size % tile_type_size != 0 || part_size % 8 != 0) {
      // Can't shuffle: just copy.
      std::memcpy(output_buf->cur_data(), part.data(), part_size);
    } else {
      throw_if_not_ok(shuffle_part(filter_data_type_, part, *output_buf));
    }

    if (output_buf->owns_data())
      output_buf->advance_size(part.size());
    output_buf->advance_offset(part.size());
  }
}

Status BitshuffleFilter::compute_parts(
    FilterBuffer* input, std::vector<ConstBuffer>* parts) const {
  auto input_parts = input->buffers();
  parts->reserve(input_parts.size() * 2);
  for (const auto& input_part : input_parts) {
    auto part_nbytes = (uint32_t)input_part.size();
    auto rem = part_nbytes % 8;
    if (rem == 0) {
      parts->push_back(input_part);
    } else {
      // Split into 2 subparts with the first one divisible by 8.
      const uint32_t first_size = part_nbytes - rem;
      const uint32_t last_size = part_nbytes - first_size;
      parts->emplace_back((char*)input_part.data(), first_size);
      parts->emplace_back((char*)input_part.data() + first_size, last_size);
    }
  }

  return Status::Ok();
}

Status BitshuffleFilter::shuffle_part(
    Datatype filter_data_type, const ConstBuffer& part, Buffer& output) {
  auto tile_type_size = static_cast<uint8_t>(datatype_size(filter_data_type));
  auto bytes_processed = blosc2_bitshuffle(
      tile_type_size,
      part.size(),
      (uint8_t*)part.data(),
      (uint8_t*)output.cur_data());

  if (bytes_processed < 0) {
    return LOG_STATUS(Status_FilterError(
        std::string("Bitshuffle error; ") +
        blosc2_error_string(bytes_processed)));
  }

  if (bytes_processed != (int64_t)part.size())
    return LOG_STATUS(Status_FilterError(
        "Bitshuffle error; Unhandled internal error code " +
        std::to_string(bytes_processed)));

  return Status::Ok();
}

Status BitshuffleFilter::run_reverse(
    const Tile&,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  auto tile_type_size = static_cast<uint8_t>(datatype_size(filter_data_type_));

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

    if (part_size % tile_type_size != 0 || part_size % 8 != 0) {
      // Part was not shuffled; just copy.
      std::memcpy(output_buf->cur_data(), part.data(), part_size);
    } else {
      RETURN_NOT_OK(unshuffle_part(filter_data_type_, part, *output_buf));
    }

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

Status BitshuffleFilter::unshuffle_part(
    Datatype filter_data_type, const ConstBuffer& part, Buffer& output) {
  auto tile_type_size = static_cast<uint8_t>(datatype_size(filter_data_type));
  auto bytes_processed = blosc2_bitunshuffle(
      tile_type_size,
      part.size(),
      (uint8_t*)part.data(),
      (uint8_t*)output.cur_data());

  if (bytes_processed < 0) {
    return LOG_STATUS(Status_FilterError(
        std::string("Bitunshuffle error; ") +
        blosc2_error_string(bytes_processed)));
  }

  if (bytes_processed != (int64_t)part.size())
    return LOG_STATUS(Status_FilterError(
        "Bitshuffle error; Unhandled internal error code " +
        std::to_string(bytes_processed)));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
