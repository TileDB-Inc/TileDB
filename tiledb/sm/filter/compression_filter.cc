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
#include "tiledb/sm/compressors/blosc_compressor.h"
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
    case Compressor::BLOSC_LZ:
      return FilterType::FILTER_BLOSC_LZ;
    case Compressor::BLOSC_LZ4:
      return FilterType::FILTER_BLOSC_LZ4;
    case Compressor::BLOSC_LZ4HC:
      return FilterType::FILTER_BLOSC_LZ4HC;
    case Compressor::BLOSC_SNAPPY:
      return FilterType::FILTER_BLOSC_SNAPPY;
    case Compressor::BLOSC_ZLIB:
      return FilterType::FILTER_BLOSC_ZLIB;
    case Compressor::BLOSC_ZSTD:
      return FilterType::FILTER_BLOSC_ZSTD;
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
    case FilterType::FILTER_BLOSC_LZ:
      return Compressor::BLOSC_LZ;
    case FilterType::FILTER_BLOSC_LZ4:
      return Compressor::BLOSC_LZ4;
    case FilterType::FILTER_BLOSC_LZ4HC:
      return Compressor::BLOSC_LZ4HC;
    case FilterType::FILTER_BLOSC_SNAPPY:
      return Compressor::BLOSC_SNAPPY;
    case FilterType::FILTER_BLOSC_ZLIB:
      return Compressor::BLOSC_ZLIB;
    case FilterType::FILTER_BLOSC_ZSTD:
      return Compressor::BLOSC_ZSTD;
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
    FilterBuffer* input, FilterBuffer* output) const {
  // Easy case: no compression
  if (compressor_ == Compressor::NO_COMPRESSION) {
    RETURN_NOT_OK(output->append_view(input, 0, input->size()));
    return Status::Ok();
  }

  if (input->size() > std::numeric_limits<uint32_t>::max())
    return LOG_STATUS(
        Status::FilterError("Input is too large to be compressed."));

  // Compute the upper bound on the size of the output.
  std::vector<ConstBuffer> parts = input->buffers();
  auto num_parts = (uint32_t)parts.size();
  uint64_t output_size_ub = sizeof(uint32_t);
  for (unsigned i = 0; i < num_parts; i++) {
    auto part = &parts[i];
    output_size_ub +=
        part->size() + overhead(part->size()) + 2 * sizeof(uint32_t);
  }

  // Ensure space in output buffer for worst case.
  RETURN_NOT_OK(output->prepend_buffer(output_size_ub));
  Buffer* buffer_ptr = output->buffer_ptr(0);
  assert(buffer_ptr != nullptr);

  // Write the header value
  buffer_ptr->reset_offset();
  RETURN_NOT_OK(buffer_ptr->write(&num_parts, sizeof(uint32_t)));

  // Compress all parts.
  for (unsigned i = 0; i < num_parts; i++) {
    RETURN_NOT_OK(compress_part(&parts[i], buffer_ptr));
  }

  return Status::Ok();
}

Status CompressionFilter::run_reverse(
    FilterBuffer* input, FilterBuffer* output) const {
  // Easy case: no compression
  if (compressor_ == Compressor::NO_COMPRESSION) {
    RETURN_NOT_OK(output->append_view(input, 0, input->size()));
    return Status::Ok();
  }

  // Read the compressed and uncompressed (result) size
  uint32_t num_parts;
  input->reset_offset();
  RETURN_NOT_OK(input->read(&num_parts, sizeof(uint32_t)));

  // Get a buffer for output.
  RETURN_NOT_OK(output->prepend_buffer(0));
  Buffer* buffer_ptr = output->buffer_ptr(0);
  assert(buffer_ptr != nullptr);

  for (uint32_t i = 0; i < num_parts; i++) {
    RETURN_NOT_OK(decompress_part(input, buffer_ptr));
  }

  return Status::Ok();
}

Status CompressionFilter::compress_part(
    ConstBuffer* part, Buffer* output) const {
  // Create const buffer
  ConstBuffer input_buffer(part->data(), part->size());

  auto tile = pipeline_->current_tile();
  auto cell_size = tile->cell_size();
  auto type = tile->type();
  auto type_size = datatype_size(type);

  // Write part header
  uint32_t input_size = (uint32_t)part->size(), compressed_size;
  RETURN_NOT_OK(output->write(&input_size, sizeof(uint32_t)));
  uint64_t compressed_size_offset = output->offset();  // Will be used later
  RETURN_NOT_OK(output->write(&compressed_size, sizeof(uint32_t)));

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
    case Compressor::BLOSC_LZ:
      RETURN_NOT_OK(
          Blosc::compress("blosclz", type_size, level_, &input_buffer, output));
      break;
#undef BLOSC_LZ4
    case Compressor::BLOSC_LZ4:
      RETURN_NOT_OK(
          Blosc::compress("lz4", type_size, level_, &input_buffer, output));
      break;
#undef BLOSC_LZ4HC
    case Compressor::BLOSC_LZ4HC:
      RETURN_NOT_OK(
          Blosc::compress("lz4hc", type_size, level_, &input_buffer, output));
      break;
#undef BLOSC_SNAPPY
    case Compressor::BLOSC_SNAPPY:
      RETURN_NOT_OK(
          Blosc::compress("snappy", type_size, level_, &input_buffer, output));
      break;
#undef BLOSC_ZLIB
    case Compressor::BLOSC_ZLIB:
      RETURN_NOT_OK(
          Blosc::compress("zlib", type_size, level_, &input_buffer, output));
      break;
#undef BLOSC_ZSTD
    case Compressor::BLOSC_ZSTD:
      RETURN_NOT_OK(
          Blosc::compress("zstd", type_size, level_, &input_buffer, output));
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

  // Overwrite the header value with the real compressed size.
  compressed_size = (uint32_t)output->size() - orig_size;
  std::memcpy(
      output->data(compressed_size_offset), &compressed_size, sizeof(uint32_t));

  return Status::Ok();
}

Status CompressionFilter::decompress_part(
    FilterBuffer* input, Buffer* output) const {
  auto tile = pipeline_->current_tile();
  auto cell_size = tile->cell_size();
  auto type = tile->type();

  // Read the part header
  uint32_t compressed_size, uncompressed_size;
  RETURN_NOT_OK(input->read(&uncompressed_size, sizeof(uint32_t)));
  RETURN_NOT_OK(input->read(&compressed_size, sizeof(uint32_t)));

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
    case Compressor::BLOSC_LZ:
#undef BLOSC_LZ4
    case Compressor::BLOSC_LZ4:
#undef BLOSC_LZ4HC
    case Compressor::BLOSC_LZ4HC:
#undef BLOSC_SNAPPY
    case Compressor::BLOSC_SNAPPY:
#undef BLOSC_ZLIB
    case Compressor::BLOSC_ZLIB:
#undef BLOSC_ZSTD
    case Compressor::BLOSC_ZSTD:
      st = Blosc::decompress(&input_buffer, &output_buffer);
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
    case Compressor::BLOSC_LZ:
#undef BLOSC_LZ4
    case Compressor::BLOSC_LZ4:
#undef BLOSC_LZ4HC
    case Compressor::BLOSC_LZ4HC:
#undef BLOSC_SNAPPY
    case Compressor::BLOSC_SNAPPY:
#undef BLOSC_ZLIB
    case Compressor::BLOSC_ZLIB:
#undef BLOSC_ZSTD
    case Compressor::BLOSC_ZSTD:
      return Blosc::overhead(nbytes);
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
  auto compressor_char = static_cast<char>(compressor_);
  RETURN_NOT_OK(buff->write(&compressor_char, sizeof(char)));
  RETURN_NOT_OK(buff->write(&level_, sizeof(int)));

  return Status::Ok();
}

Status CompressionFilter::deserialize_impl(ConstBuffer* buff) {
  char compressor_char;
  RETURN_NOT_OK(buff->read(&compressor_char, sizeof(char)));
  compressor_ = static_cast<Compressor>(compressor_char);
  RETURN_NOT_OK(buff->read(&level_, sizeof(int)));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb