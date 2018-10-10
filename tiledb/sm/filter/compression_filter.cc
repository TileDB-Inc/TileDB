/**
 * @file   compression_filter.cc
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
 * This file defines class CompressionFilter.
 */

#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/compressors/bzip_compressor.h"
#include "tiledb/sm/compressors/dd_compressor.h"
#include "tiledb/sm/compressors/gzip_compressor.h"
#include "tiledb/sm/compressors/lz4_compressor.h"
#include "tiledb/sm/compressors/rle_compressor.h"
#include "tiledb/sm/compressors/zstd_compressor.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

CompressionFilter::CompressionFilter(FilterType compressor, int level)
    : Filter(compressor) {
  compressor_ = filter_to_compressor(compressor);
  level_ = level;
}

CompressionFilter::CompressionFilter(Compressor compressor, int level)
    : Filter(FilterType::FILTER_NONE) {
  compressor_ = compressor;
  level_ = level;
  type_ = compressor_to_filter(compressor);
}

Compressor CompressionFilter::compressor() const {
  return compressor_;
}

int CompressionFilter::compression_level() const {
  return level_;
}

CompressionFilter* CompressionFilter::clone_impl() const {
  return new CompressionFilter(compressor_, level_);
}

void CompressionFilter::set_compressor(Compressor compressor) {
  compressor_ = compressor;
  type_ = compressor_to_filter(compressor);
}

void CompressionFilter::set_compression_level(int compressor_level) {
  level_ = compressor_level;
}

FilterType CompressionFilter::compressor_to_filter(Compressor compressor) {
  switch (compressor) {
    case Compressor::NO_COMPRESSION:
      return FilterType::FILTER_NONE;
    case Compressor::GZIP:
      return FilterType::FILTER_GZIP;
    case Compressor::ZSTD:
      return FilterType::FILTER_ZSTD;
    case Compressor::LZ4:
      return FilterType::FILTER_LZ4;
    case Compressor::RLE:
      return FilterType::FILTER_RLE;
    case Compressor::BZIP2:
      return FilterType::FILTER_BZIP2;
    case Compressor::DOUBLE_DELTA:
      return FilterType::FILTER_DOUBLE_DELTA;
    default:
      assert(false);
      return FilterType::FILTER_NONE;
  }
}

Compressor CompressionFilter::filter_to_compressor(FilterType type) {
  switch (type) {
    case FilterType::FILTER_NONE:
      return Compressor::NO_COMPRESSION;
    case FilterType::FILTER_GZIP:
      return Compressor::GZIP;
    case FilterType::FILTER_ZSTD:
      return Compressor::ZSTD;
    case FilterType::FILTER_LZ4:
      return Compressor::LZ4;
    case FilterType::FILTER_RLE:
      return Compressor::RLE;
    case FilterType::FILTER_BZIP2:
      return Compressor::BZIP2;
    case FilterType::FILTER_DOUBLE_DELTA:
      return Compressor::DOUBLE_DELTA;
    default:
      assert(false);
      return Compressor::NO_COMPRESSION;
  }
}

Status CompressionFilter::set_option_impl(
    FilterOption option, const void* value) {
  if (value == nullptr)
    return LOG_STATUS(
        Status::FilterError("Compression filter error; invalid option value"));

  switch (option) {
    case FilterOption::COMPRESSION_LEVEL:
      level_ = *(int*)value;
      return Status::Ok();
    default:
      return LOG_STATUS(
          Status::FilterError("Compression filter error; unknown option"));
  }
}

Status CompressionFilter::get_option_impl(
    FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::COMPRESSION_LEVEL:
      *(int*)value = level_;
      return Status::Ok();
    default:
      return LOG_STATUS(
          Status::FilterError("Compression filter error; unknown option"));
  }
}

Status CompressionFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Easy case: no compression
  if (compressor_ == Compressor::NO_COMPRESSION) {
    RETURN_NOT_OK(output->append_view(input));
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));
    return Status::Ok();
  }

  if (input->size() > std::numeric_limits<uint32_t>::max())
    return LOG_STATUS(
        Status::FilterError("Input is too large to be compressed."));

  // Compute the upper bound on the size of the output.
  std::vector<ConstBuffer> data_parts = input->buffers(),
                           metadata_parts = input_metadata->buffers();
  auto num_data_parts = (uint32_t)data_parts.size(),
       num_metadata_parts = (uint32_t)metadata_parts.size(),
       total_num_parts = num_data_parts + num_metadata_parts;
  uint64_t output_size_ub = 0;
  for (const auto& part : metadata_parts)
    output_size_ub += part.size() + overhead(part.size());
  for (const auto& part : data_parts)
    output_size_ub += part.size() + overhead(part.size());

  // Ensure space in output buffer for worst case.
  RETURN_NOT_OK(output->prepend_buffer(output_size_ub));
  Buffer* buffer_ptr = output->buffer_ptr(0);
  assert(buffer_ptr != nullptr);
  buffer_ptr->reset_offset();

  // Allocate a buffer for this filter's metadata and write the number of parts.
  auto metadata_size =
      2 * sizeof(uint32_t) + total_num_parts * 2 * sizeof(uint32_t);
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_metadata_parts, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata->write(&num_data_parts, sizeof(uint32_t)));

  // Compress all parts.
  for (auto& part : metadata_parts)
    RETURN_NOT_OK(compress_part(&part, buffer_ptr, output_metadata));
  for (auto& part : data_parts)
    RETURN_NOT_OK(compress_part(&part, buffer_ptr, output_metadata));

  return Status::Ok();
}

Status CompressionFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Easy case: no compression
  if (compressor_ == Compressor::NO_COMPRESSION) {
    RETURN_NOT_OK(output->append_view(input));
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));
    return Status::Ok();
  }

  // Read the number of parts from input metadata.
  uint32_t num_metadata_parts, num_data_parts;
  RETURN_NOT_OK(input_metadata->read(&num_metadata_parts, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata->read(&num_data_parts, sizeof(uint32_t)));

  // Get a buffer for output.
  RETURN_NOT_OK(output->prepend_buffer(0));
  Buffer* data_buffer = output->buffer_ptr(0);
  assert(data_buffer != nullptr);
  RETURN_NOT_OK(output_metadata->prepend_buffer(0));
  Buffer* metadata_buffer = output_metadata->buffer_ptr(0);
  assert(metadata_buffer != nullptr);

  for (uint32_t i = 0; i < num_metadata_parts; i++)
    RETURN_NOT_OK(decompress_part(input, metadata_buffer, input_metadata));
  for (uint32_t i = 0; i < num_data_parts; i++)
    RETURN_NOT_OK(decompress_part(input, data_buffer, input_metadata));

  return Status::Ok();
}

Status CompressionFilter::compress_part(
    ConstBuffer* part, Buffer* output, FilterBuffer* output_metadata) const {
  // Create const buffer
  ConstBuffer input_buffer(part->data(), part->size());

  auto tile = pipeline_->current_tile();
  auto cell_size = tile->cell_size();
  auto type = tile->type();

  // Invoke the proper compressor
  uint32_t orig_size = (uint32_t)output->size();
  switch (compressor_) {
    case Compressor::GZIP:
      RETURN_NOT_OK(GZip::compress(level_, &input_buffer, output));
      break;
    case Compressor::ZSTD:
      RETURN_NOT_OK(ZStd::compress(level_, &input_buffer, output));
      break;
    case Compressor::LZ4:
      RETURN_NOT_OK(LZ4::compress(level_, &input_buffer, output));
      break;
    case Compressor::RLE:
      RETURN_NOT_OK(RLE::compress(cell_size, &input_buffer, output));
      break;
    case Compressor::BZIP2:
      RETURN_NOT_OK(BZip::compress(level_, &input_buffer, output));
      break;
    case Compressor::DOUBLE_DELTA:
      RETURN_NOT_OK(DoubleDelta::compress(type, &input_buffer, output));
      break;
    default:
      assert(0);
  }

  if (output->size() > std::numeric_limits<uint32_t>::max())
    return LOG_STATUS(
        Status::FilterError("Compressed output exceeds uint32 max."));

  // Write part original and compressed size to metadata
  uint32_t input_size = (uint32_t)part->size(),
           compressed_size = (uint32_t)output->size() - orig_size;
  RETURN_NOT_OK(output_metadata->write(&input_size, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata->write(&compressed_size, sizeof(uint32_t)));

  return Status::Ok();
}

Status CompressionFilter::decompress_part(
    FilterBuffer* input, Buffer* output, FilterBuffer* input_metadata) const {
  auto tile = pipeline_->current_tile();
  auto cell_size = tile->cell_size();
  auto type = tile->type();

  // Read the part metadata
  uint32_t compressed_size, uncompressed_size;
  RETURN_NOT_OK(input_metadata->read(&uncompressed_size, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata->read(&compressed_size, sizeof(uint32_t)));

  // Ensure space in the output buffer if possible.
  if (output->owns_data()) {
    RETURN_NOT_OK(output->realloc(output->alloced_size() + uncompressed_size));
  } else if (output->offset() + uncompressed_size > output->size()) {
    return LOG_STATUS(Status::FilterError(
        "CompressionFilter error; output buffer too small."));
  }

  ConstBuffer input_buffer(nullptr, 0);
  RETURN_NOT_OK(input->get_const_buffer(compressed_size, &input_buffer));

  PreallocatedBuffer output_buffer(output->cur_data(), uncompressed_size);

  // Invoke the proper decompressor
  Status st = Status::Ok();
  switch (compressor_) {
    case Compressor::NO_COMPRESSION:
      assert(0);
      break;
    case Compressor::GZIP:
      st = GZip::decompress(&input_buffer, &output_buffer);
      break;
    case Compressor::ZSTD:
      st = ZStd::decompress(&input_buffer, &output_buffer);
      break;
    case Compressor::LZ4:
      st = LZ4::decompress(&input_buffer, &output_buffer);
      break;
    case Compressor::RLE:
      st = RLE::decompress(cell_size, &input_buffer, &output_buffer);
      break;
    case Compressor::BZIP2:
      st = BZip::decompress(&input_buffer, &output_buffer);
      break;
    case Compressor::DOUBLE_DELTA:
      st = DoubleDelta::decompress(type, &input_buffer, &output_buffer);
      break;
  }

  if (output->owns_data())
    output->advance_size(uncompressed_size);
  output->advance_offset(uncompressed_size);
  input->advance_offset(compressed_size);

  return st;
}

uint64_t CompressionFilter::overhead(uint64_t nbytes) const {
  auto tile = pipeline_->current_tile();
  auto cell_size = tile->cell_size();

  switch (compressor_) {
    case Compressor::GZIP:
      return GZip::overhead(nbytes);
    case Compressor::ZSTD:
      return ZStd::overhead(nbytes);
    case Compressor::LZ4:
      return LZ4::overhead(nbytes);
    case Compressor::RLE:
      return RLE::overhead(nbytes, cell_size);
    case Compressor::BZIP2:
      return BZip::overhead(nbytes);
    case Compressor::DOUBLE_DELTA:
      return DoubleDelta::overhead(nbytes);
    default:
      // No compression
      return 0;
  }
}

Status CompressionFilter::serialize_impl(Buffer* buff) const {
  auto compressor_char = static_cast<uint8_t>(compressor_);
  RETURN_NOT_OK(buff->write(&compressor_char, sizeof(uint8_t)));
  RETURN_NOT_OK(buff->write(&level_, sizeof(int32_t)));

  return Status::Ok();
}

Status CompressionFilter::deserialize_impl(ConstBuffer* buff) {
  uint8_t compressor_char;
  RETURN_NOT_OK(buff->read(&compressor_char, sizeof(uint8_t)));
  compressor_ = static_cast<Compressor>(compressor_char);
  RETURN_NOT_OK(buff->read(&level_, sizeof(int32_t)));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb