/**
 * @file   compression_filter.cc
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
 * This file defines class CompressionFilter.
 */

#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/compressors/bzip_compressor.h"
#include "tiledb/sm/compressors/dd_compressor.h"
#include "tiledb/sm/compressors/gzip_compressor.h"
#include "tiledb/sm/compressors/lz4_compressor.h"
#include "tiledb/sm/compressors/rle_compressor.h"
#include "tiledb/sm/compressors/zstd_compressor.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

CompressionFilter::CompressionFilter(
    FilterType compressor, int level, const uint32_t version)
    : Filter(compressor)
    , compressor_(filter_to_compressor(compressor))
    , level_(level)
    , version_(version)
    , zstd_compress_ctx_pool_(nullptr)
    , zstd_decompress_ctx_pool_(nullptr) {
}

CompressionFilter::CompressionFilter(
    Compressor compressor, int level, const uint32_t version)
    : Filter(compressor_to_filter(compressor))
    , compressor_(compressor)
    , level_(level)
    , version_(version)
    , zstd_compress_ctx_pool_(nullptr)
    , zstd_decompress_ctx_pool_(nullptr) {
}

Compressor CompressionFilter::compressor() const {
  return compressor_;
}

int CompressionFilter::compression_level() const {
  return level_;
}

void CompressionFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  std::string compressor_str;
  switch (compressor_) {
    case Compressor::NO_COMPRESSION:
      compressor_str = "NO_COMPRESSION";
      break;
    case Compressor::GZIP:
      compressor_str = "GZIP";
      break;
    case Compressor::ZSTD:
      compressor_str = "ZSTD";
      break;
    case Compressor::LZ4:
      compressor_str = "LZ4";
      break;
    case Compressor::RLE:
      compressor_str = "RLE";
      break;
    case Compressor::BZIP2:
      compressor_str = "BZIP2";
      break;
    case Compressor::DOUBLE_DELTA:
      compressor_str = "DOUBLE_DELTA";
      break;
    default:
      compressor_str = "NO_COMPRESSION";
  }

  fprintf(out, "%s: COMPRESSION_LEVEL=%i", compressor_str.c_str(), level_);
}

CompressionFilter* CompressionFilter::clone_impl() const {
  return tdb_new(CompressionFilter, compressor_, level_, version_);
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
        Status_FilterError("Compression filter error; invalid option value"));

  switch (option) {
    case FilterOption::COMPRESSION_LEVEL:
      level_ = *(int*)value;
      return Status::Ok();
    default:
      return LOG_STATUS(
          Status_FilterError("Compression filter error; unknown option"));
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
          Status_FilterError("Compression filter error; unknown option"));
  }
}

size_t CompressionFilter::calculate_output_metadata_size(
    const Tile& tile,
    const std::vector<ConstBuffer>& data_parts,
    const std::vector<ConstBuffer>& metadata_parts) const {
  auto total_num_parts = data_parts.size() + metadata_parts.size();
  auto metadata_size =
      2 * sizeof(uint32_t) + total_num_parts * 2 * sizeof(uint32_t);

  if (compressor_ == Compressor::RLE) {
    if (datatype_is_string(tile.type())) {
      // Add two extra metadata bytes that store run length datasize and
      // string length datasize, and output offsets size
      metadata_size += 2 * sizeof(uint8_t) + sizeof(uint32_t);
    }
  }

  return metadata_size;
}

Status CompressionFilter::run_forward(
    const Tile& tile,
    Tile* const offsets_tile,
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
        Status_FilterError("Input is too large to be compressed."));

  std::vector<ConstBuffer> data_parts = input->buffers(),
                           metadata_parts = input_metadata->buffers();

  // Allocate output metadata
  auto metadata_size =
      calculate_output_metadata_size(tile, data_parts, metadata_parts);
  auto num_metadata_parts = static_cast<uint32_t>(metadata_parts.size());
  auto num_data_parts = static_cast<uint32_t>(data_parts.size());
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_metadata_parts, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata->write(&num_data_parts, sizeof(uint32_t)));

  if (compressor_ == Compressor::RLE && tile.type() == Datatype::STRING_ASCII &&
      offsets_tile) {
    // String RLE is supported but only allowed on single filter
    assert(num_data_parts == 1);
    return compress_var_string_coords(
        *input, offsets_tile, *output, *output_metadata);
  }

  // Allocate output data
  uint64_t output_size_ub = 0;
  for (const auto& part : metadata_parts)
    output_size_ub += part.size() + overhead(tile, part.size());
  for (const auto& part : data_parts)
    output_size_ub += part.size() + overhead(tile, part.size());

  // Ensure space in output buffer for worst case.
  RETURN_NOT_OK(output->prepend_buffer(output_size_ub));
  Buffer* buffer_ptr = output->buffer_ptr(0);
  assert(buffer_ptr != nullptr);
  buffer_ptr->reset_offset();

  // Compress all parts.
  for (auto& part : metadata_parts)
    RETURN_NOT_OK(compress_part(tile, &part, buffer_ptr, output_metadata));
  for (auto& part : data_parts)
    RETURN_NOT_OK(compress_part(tile, &part, buffer_ptr, output_metadata));

  return Status::Ok();
}

Status CompressionFilter::run_reverse(
    const Tile& tile,
    Tile* const offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  (void)config;

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

  if (compressor_ == Compressor::RLE && tile.type() == Datatype::STRING_ASCII &&
      version_ >= 12 && offsets_tile) {
    // String RLE is only allowed on first/single filter
    assert(num_data_parts == 1);
    return decompress_var_string_coords(
        *input, *input_metadata, offsets_tile, *output);
  }

  for (uint32_t i = 0; i < num_metadata_parts; i++)
    RETURN_NOT_OK(
        decompress_part(tile, input, metadata_buffer, input_metadata));
  for (uint32_t i = 0; i < num_data_parts; i++)
    RETURN_NOT_OK(decompress_part(tile, input, data_buffer, input_metadata));

  return Status::Ok();
}

Status CompressionFilter::compress_part(
    const Tile& tile,
    ConstBuffer* part,
    Buffer* output,
    FilterBuffer* output_metadata) const {
  // Create const buffer
  ConstBuffer input_buffer(part->data(), part->size());

  auto cell_size = tile.cell_size();
  auto type = tile.type();

  // Invoke the proper compressor
  uint32_t orig_size = (uint32_t)output->size();
  switch (compressor_) {
    case Compressor::GZIP:
      RETURN_NOT_OK(GZip::compress(level_, &input_buffer, output));
      break;
    case Compressor::ZSTD:
      RETURN_NOT_OK(ZStd::compress(
          level_, zstd_compress_ctx_pool_, &input_buffer, output));
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
        Status_FilterError("Compressed output exceeds uint32 max."));

  // Write part original and compressed size to metadata
  uint32_t input_size = (uint32_t)part->size(),
           compressed_size = (uint32_t)output->size() - orig_size;
  RETURN_NOT_OK(output_metadata->write(&input_size, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata->write(&compressed_size, sizeof(uint32_t)));

  return Status::Ok();
}

Status CompressionFilter::decompress_part(
    const Tile& tile,
    FilterBuffer* input,
    Buffer* output,
    FilterBuffer* input_metadata) const {
  auto cell_size = tile.cell_size();
  auto type = tile.type();

  // Read the part metadata
  uint32_t compressed_size, uncompressed_size;
  RETURN_NOT_OK(input_metadata->read(&uncompressed_size, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata->read(&compressed_size, sizeof(uint32_t)));

  // Ensure space in the output buffer if possible.
  if (output->owns_data()) {
    RETURN_NOT_OK(output->realloc(output->alloced_size() + uncompressed_size));
  } else if (output->offset() + uncompressed_size > output->size()) {
    return LOG_STATUS(Status_FilterError(
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
      st = ZStd::decompress(
          zstd_decompress_ctx_pool_, &input_buffer, &output_buffer);
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

std::vector<std::string_view> CompressionFilter::create_input_view(
    const FilterBuffer& input, Tile* const offsets_tile) {
  auto input_buf = static_cast<const char*>(input.buffers()[0].data());
  auto offsets_data = static_cast<uint64_t*>(offsets_tile->data());
  auto offsets_size = offsets_tile->size() / constants::cell_var_offset_size;
  std::vector<std::string_view> input_view(offsets_size);

  size_t i = 0;
  for (i = 0; i < offsets_size - 1; i++) {
    input_view[i] = std::string_view(
        input_buf + offsets_data[i], offsets_data[i + 1] - offsets_data[i]);
  }

  // special case for the last string
  input_view[i] = std::string_view(
      input_buf + offsets_data[i], input.size() - offsets_data[i]);

  return input_view;
}

Status CompressionFilter::compress_var_string_coords(
    const FilterBuffer& input,
    Tile* const offsets_tile,
    FilterBuffer& output,
    FilterBuffer& output_metadata) const {
  assert(offsets_tile);

  if (input.num_buffers() != 1) {
    return LOG_STATUS(
        Status_FilterError("Var-sized string input has to be in single "
                           "buffer format to be compressed with RLE"));
  }

  // Construct string view of input
  auto input_view = create_input_view(input, offsets_tile);

  // Estimate and allocate output size
  auto
      [rle_len_bytesize,
       string_len_bytesize,
       num_of_runs,
       output_strings_size] = RLE::calculate_compression_params(input_view);
  uint64_t output_size_ub =
      num_of_runs * (rle_len_bytesize + string_len_bytesize) +
      output_strings_size;
  uint32_t input_offsets_size = offsets_tile->size();

  // Allocate output data buffer
  RETURN_NOT_OK(output.prepend_buffer(output_size_ub));
  Buffer* data_buffer = output.buffer_ptr(0);
  assert(data_buffer != nullptr);
  data_buffer->reset_offset();
  auto output_view = span<std::byte>(
      reinterpret_cast<std::byte*>(data_buffer->data()), output_size_ub);

  Status st = RLE::compress(
      input_view, rle_len_bytesize, string_len_bytesize, output_view);
  RETURN_NOT_OK(st);

  data_buffer->set_size(output_size_ub);

  // Note: assumes single buffer (holds when RLE is the first/only filter)
  auto input_size = input.buffers()[0].size();
  RETURN_NOT_OK(output_metadata.write(&input_size, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata.write(&output_size_ub, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata.write(&input_offsets_size, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata.write(&rle_len_bytesize, sizeof(uint8_t)));
  RETURN_NOT_OK(output_metadata.write(&string_len_bytesize, sizeof(uint8_t)));

  return Status::Ok();
}

Status CompressionFilter::decompress_var_string_coords(
    FilterBuffer& input,
    FilterBuffer& input_metadata,
    Tile* offsets_tile,
    FilterBuffer& output) const {
  if (input.num_buffers() != 1) {
    return LOG_STATUS(
        Status_FilterError("Var-sized string input has to be in single "
                           "buffer format to get decompressed with RLE"));
  }

  // Read the part metadata
  uint32_t compressed_size, uncompressed_size, uncompressed_offsets_size;
  RETURN_NOT_OK(input_metadata.read(&uncompressed_size, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata.read(&compressed_size, sizeof(uint32_t)));
  RETURN_NOT_OK(
      input_metadata.read(&uncompressed_offsets_size, sizeof(uint32_t)));

  uint8_t rle_len_bytesize, string_len_bytesize;
  RETURN_NOT_OK(input_metadata.read(&rle_len_bytesize, sizeof(uint8_t)));
  RETURN_NOT_OK(input_metadata.read(&string_len_bytesize, sizeof(uint8_t)));

  // Get views of input and output
  auto input_buffer = input.buffers()[0];
  auto input_view = span<const std::byte>(
      reinterpret_cast<const std::byte*>(input_buffer.data()), compressed_size);
  Buffer* output_buffer = output.buffer_ptr(0);
  auto output_view = span<std::byte>(
      reinterpret_cast<std::byte*>(output_buffer->data()), uncompressed_size);
  auto offsets_view = span<uint64_t>(
      reinterpret_cast<std::uint64_t*>(offsets_tile->data()),
      uncompressed_offsets_size);

  switch (compressor_) {
    case Compressor::RLE: {
      RLE::decompress(
          input_view,
          rle_len_bytesize,
          string_len_bytesize,
          output_view,
          offsets_view);
      break;
    }
    default:
      break;
  }

  if (output_buffer->owns_data())
    output_buffer->advance_size(uncompressed_size);
  output_buffer->advance_offset(uncompressed_size);
  input.advance_offset(compressed_size);

  return Status::Ok();
}

uint64_t CompressionFilter::overhead(const Tile& tile, uint64_t nbytes) const {
  auto cell_size = tile.cell_size();

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
  if (compressor_ == Compressor::NO_COMPRESSION) {
    return Status::Ok();
  }
  auto compressor_char = static_cast<uint8_t>(compressor_);
  RETURN_NOT_OK(buff->write(&compressor_char, sizeof(uint8_t)));
  RETURN_NOT_OK(buff->write(&level_, sizeof(int32_t)));

  return Status::Ok();
}

void CompressionFilter::init_compression_resource_pool(uint64_t size) {
  std::lock_guard g(zstd_compress_ctx_pool_mtx_);
  if (zstd_compress_ctx_pool_ == nullptr) {
    zstd_compress_ctx_pool_ =
        tdb::make_shared<BlockingResourcePool<ZStd::ZSTD_Compress_Context>>(
            HERE(), size);
  }
}

void CompressionFilter::init_decompression_resource_pool(uint64_t size) {
  std::lock_guard g(zstd_decompress_ctx_pool_mtx_);
  if (zstd_decompress_ctx_pool_ == nullptr) {
    zstd_decompress_ctx_pool_ =
        tdb::make_shared<BlockingResourcePool<ZStd::ZSTD_Decompress_Context>>(
            HERE(), size);
  }
}

}  // namespace sm
}  // namespace tiledb
