/**
 * @file   bitshuffle_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "bitshuffle_core.h"

#include "tiledb/sm/filter/bitshuffle_filter.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

BitshuffleFilter::BitshuffleFilter()
    : Filter(FilterType::FILTER_BITSHUFFLE) {
}

BitshuffleFilter* BitshuffleFilter::clone_impl() const {
  return new BitshuffleFilter;
}

Status BitshuffleFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type = pipeline_->current_tile()->type();
  auto tile_type_size = static_cast<uint8_t>(datatype_size(tile_type));

  // Output size does not change with this filter.
  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  // Compute the list of parts to shuffle
  std::vector<ConstBuffer> parts;
  RETURN_NOT_OK(compute_parts(input, &parts));

  // Write the metadata
  auto num_parts = (uint32_t)parts.size();
  uint32_t metadata_size = sizeof(uint32_t) + num_parts * sizeof(uint32_t);
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_parts, sizeof(uint32_t)));

  // Shuffle all parts
  for (const auto& part : parts) {
    auto part_size = (uint32_t)part.size();
    RETURN_NOT_OK(output_metadata->write(&part_size, sizeof(uint32_t)));

    if (part_size % tile_type_size != 0 || part_size % 8 != 0) {
      // Can't shuffle: just copy.
      std::memcpy(output_buf->cur_data(), part.data(), part_size);
    } else {
      RETURN_NOT_OK(shuffle_part(&part, output_buf));
    }

    if (output_buf->owns_data())
      output_buf->advance_size(part.size());
    output_buf->advance_offset(part.size());
  }

  return Status::Ok();
}

Status BitshuffleFilter::compute_parts(
    FilterBuffer* input, std::vector<ConstBuffer>* parts) const {
  auto input_parts = input->buffers();
  for (const auto& input_part : input_parts) {
    auto part_nbytes = (uint32_t)input_part.size();
    auto rem = part_nbytes % 8;
    if (rem == 0) {
      parts->push_back(input_part);
    } else {
      // Split into 2 subparts with the first one divisible by 8.
      uint32_t first_size = part_nbytes - rem;
      uint32_t last_size = part_nbytes - first_size;
      parts->emplace_back((char*)input_part.data(), first_size);
      parts->emplace_back((char*)input_part.data() + first_size, last_size);
    }
  }

  return Status::Ok();
}

Status BitshuffleFilter::shuffle_part(
    const ConstBuffer* part, Buffer* output) const {
  auto tile_type = pipeline_->current_tile()->type();
  auto tile_type_size = static_cast<uint8_t>(datatype_size(tile_type));
  auto part_nelts = part->size() / tile_type_size;
  auto bytes_processed = bshuf_bitshuffle(
      part->data(), output->cur_data(), part_nelts, tile_type_size, 0);

  switch (bytes_processed) {
    case -1:
      return LOG_STATUS(
          Status::FilterError("Bitshuffle error; Failed to allocate memory."));
    case -11:
      return LOG_STATUS(Status::FilterError("Bitshuffle error; Missing SSE."));
    case -12:
      return LOG_STATUS(Status::FilterError("Bitshuffle error; Missing AVX."));
    case -80:
      return LOG_STATUS(Status::FilterError(
          "Bitshuffle error; Input size not a multiple of 8."));
    case -81:
      return LOG_STATUS(Status::FilterError(
          "Bitshuffle error; Block size not a multiple of 8."));
    case -91:
      return LOG_STATUS(
          Status::FilterError("Bitshuffle error; Decompression error, wrong "
                              "number of bytes processed."));
    default: {
      if (bytes_processed != (int64_t)part->size())
        return LOG_STATUS(Status::FilterError(
            "Bitshuffle error; Unhandled internal error code " +
            std::to_string(bytes_processed)));
      break;
    }
  }

  return Status::Ok();
}

Status BitshuffleFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type = pipeline_->current_tile()->type();
  auto tile_type_size = static_cast<uint8_t>(datatype_size(tile_type));

  // Get number of parts
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    if (part_size % tile_type_size != 0 || part_size % 8 != 0) {
      // Part was not shuffled; just copy.
      std::memcpy(output_buf->cur_data(), part.data(), part_size);
    } else {
      RETURN_NOT_OK(unshuffle_part(&part, output_buf));
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
    const ConstBuffer* part, Buffer* output) const {
  auto tile_type = pipeline_->current_tile()->type();
  auto tile_type_size = static_cast<uint8_t>(datatype_size(tile_type));
  auto part_nelts = part->size() / tile_type_size;
  auto bytes_processed = bshuf_bitunshuffle(
      part->data(), output->cur_data(), part_nelts, tile_type_size, 0);

  switch (bytes_processed) {
    case -1:
      return LOG_STATUS(
          Status::FilterError("Bitshuffle error; Failed to allocate memory."));
    case -11:
      return LOG_STATUS(Status::FilterError("Bitshuffle error; Missing SSE."));
    case -12:
      return LOG_STATUS(Status::FilterError("Bitshuffle error; Missing AVX."));
    case -80:
      return LOG_STATUS(Status::FilterError(
          "Bitshuffle error; Input size not a multiple of 8."));
    case -81:
      return LOG_STATUS(Status::FilterError(
          "Bitshuffle error; Block size not a multiple of 8."));
    case -91:
      return LOG_STATUS(
          Status::FilterError("Bitshuffle error; Decompression error, wrong "
                              "number of bytes processed."));
    default: {
      if (bytes_processed != (int64_t)part->size())
        return LOG_STATUS(Status::FilterError(
            "Bitshuffle error; Unhandled internal error code " +
            std::to_string(bytes_processed)));
      break;
    }
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb