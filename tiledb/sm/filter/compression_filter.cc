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
#include "tiledb/sm/compressors/delta_compressor.h"
#include "tiledb/sm/compressors/dict_compressor.h"
#include "tiledb/sm/compressors/gzip_compressor.h"
#include "tiledb/sm/compressors/lz4_compressor.h"
#include "tiledb/sm/compressors/rle_compressor.h"
#include "tiledb/sm/compressors/zstd_compressor.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb::sm {

class CompressionFilterStatusException : public StatusException {
 public:
  explicit CompressionFilterStatusException(const std::string& msg)
      : StatusException("CompressionFilter", msg) {
  }
};

CompressionFilter::CompressionFilter(
    FilterType compressor,
    int level,
    Datatype filter_data_type,
    Datatype reinterpret_type,
    const format_version_t version)
    : Filter(compressor, filter_data_type)
    , compressor_(filter_to_compressor(compressor))
    , level_(level)
    , version_(version)
    , zstd_compress_ctx_pool_(nullptr)
    , zstd_decompress_ctx_pool_(nullptr)
    , reinterpret_datatype_(reinterpret_type) {
}

CompressionFilter::CompressionFilter(
    Compressor compressor,
    int level,
    Datatype filter_data_type,
    Datatype reinterpret_type,
    const format_version_t version)
    : Filter(compressor_to_filter(compressor), filter_data_type)
    , compressor_(compressor)
    , level_(level)
    , version_(version)
    , zstd_compress_ctx_pool_(nullptr)
    , zstd_decompress_ctx_pool_(nullptr)
    , reinterpret_datatype_(reinterpret_type) {
}

Compressor CompressionFilter::compressor() const {
  return compressor_;
}

bool CompressionFilter::accepts_input_datatype(Datatype input_type) const {
  auto this_filter_type = compressor_to_filter(compressor_);
  if (this_filter_type == FilterType::FILTER_DOUBLE_DELTA ||
      this_filter_type == FilterType::FILTER_DELTA) {
    // Delta filters do not accept floating point types.
    if (datatype_is_real(
            reinterpret_datatype_ != Datatype::ANY ? reinterpret_datatype_ :
                                                     input_type)) {
      return false;
    }
    // We must receive an integral number of units of the reinterpret datatype
    else if (
        reinterpret_datatype_ != Datatype::ANY &&
        datatype_size(input_type) % datatype_size(reinterpret_datatype_) != 0) {
      return false;
    }
  }

  return true;
}

int CompressionFilter::compression_level() const {
  return level_;
}

std::ostream& CompressionFilter::output(std::ostream& os) const {
  std::string compressor_str = tiledb::sm::compressor_str(compressor_);
  os << compressor_str << ": COMPRESSION_LEVEL=" << level_;
  if (compressor_ == Compressor::DELTA ||
      compressor_ == Compressor::DOUBLE_DELTA)
    os << ", REINTERPRET_DATATYPE=" << datatype_str(reinterpret_datatype_);

  return os;
}

CompressionFilter* CompressionFilter::clone_impl() const {
  return tdb_new(
      CompressionFilter,
      compressor_,
      level_,
      filter_data_type_,
      reinterpret_datatype_,
      version_);
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
    case Compressor::DICTIONARY_ENCODING:
      return FilterType::FILTER_DICTIONARY;
    case Compressor::DELTA:
      return FilterType::FILTER_DELTA;
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
    case FilterType::FILTER_DICTIONARY:
      return Compressor::DICTIONARY_ENCODING;
    case FilterType::FILTER_DELTA:
      return Compressor::DELTA;
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
      break;
    case FilterOption::COMPRESSION_REINTERPRET_DATATYPE:
      reinterpret_datatype_ = *(Datatype*)value;
      break;
    default:
      return LOG_STATUS(
          Status_FilterError("Compression filter error; unknown option"));
  }

  return Status::Ok();
}

Status CompressionFilter::get_option_impl(
    FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::COMPRESSION_LEVEL:
      *(int*)value = level_;
      break;
    case FilterOption::COMPRESSION_REINTERPRET_DATATYPE:
      *(Datatype*)value = reinterpret_datatype_;
      break;
    default:
      return LOG_STATUS(
          Status_FilterError("Compression filter error; unknown option"));
  }

  return Status::Ok();
}

void CompressionFilter::run_forward(
    const WriterTile& tile,
    WriterTile* const offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Easy case: no compression
  if (compressor_ == Compressor::NO_COMPRESSION) {
    throw_if_not_ok(output->append_view(input));
    throw_if_not_ok(output_metadata->append_view(input_metadata));
    return;
  }

  if (input->size() > std::numeric_limits<uint32_t>::max()) {
    throw FilterStatusException("Input is too large to be compressed.");
  }

  if ((filter_data_type_ == Datatype::STRING_ASCII ||
       filter_data_type_ == Datatype::STRING_UTF8) &&
      offsets_tile) {
    if (compressor_ == Compressor::RLE ||
        compressor_ == Compressor::DICTIONARY_ENCODING) {
      throw_if_not_ok(compress_var_string_coords(
          *input, offsets_tile, *output, *output_metadata));
      return;
    }
  }

  std::vector<ConstBuffer> data_parts =
                               input->buffers_as(reinterpret_datatype_),
                           metadata_parts = input_metadata->buffers();
  // Allocate output metadata
  auto total_num_parts = data_parts.size() + metadata_parts.size();
  auto metadata_size =
      2 * sizeof(uint32_t) + total_num_parts * 2 * sizeof(uint32_t);
  auto num_metadata_parts = static_cast<uint32_t>(metadata_parts.size());
  auto num_data_parts = static_cast<uint32_t>(data_parts.size());
  throw_if_not_ok(output_metadata->prepend_buffer(metadata_size));
  throw_if_not_ok(
      output_metadata->write(&num_metadata_parts, sizeof(uint32_t)));
  throw_if_not_ok(output_metadata->write(&num_data_parts, sizeof(uint32_t)));

  // Allocate output data
  uint64_t output_size_ub = 0;
  for (const auto& part : metadata_parts)
    output_size_ub += part.size() + overhead(tile, part.size());
  for (const auto& part : data_parts)
    output_size_ub += part.size() + overhead(tile, part.size());

  // Ensure space in output buffer for worst case.
  throw_if_not_ok(output->prepend_buffer(output_size_ub));
  Buffer* buffer_ptr = output->buffer_ptr(0);
  assert(buffer_ptr != nullptr);
  buffer_ptr->reset_offset();

  // Compress all parts.
  for (auto& part : metadata_parts)
    throw_if_not_ok(compress_part(tile, &part, buffer_ptr, output_metadata));
  for (auto& part : data_parts)
    throw_if_not_ok(compress_part(tile, &part, buffer_ptr, output_metadata));
}

Status CompressionFilter::run_reverse(
    const Tile& tile,
    Tile* const offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
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

  if ((filter_data_type_ == Datatype::STRING_ASCII ||
       filter_data_type_ == Datatype::STRING_UTF8) &&
      version_ >= 12 && offsets_tile) {
    if (compressor_ == Compressor::RLE ||
        compressor_ == Compressor::DICTIONARY_ENCODING)
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
    const WriterTile& tile,
    ConstBuffer* part,
    Buffer* output,
    FilterBuffer* output_metadata) const {
  // Create const buffer
  ConstBuffer input_buffer(part->data(), part->size());

  auto cell_size = tile.cell_size();

  // Invoke the proper compressor
  uint32_t orig_size = (uint32_t)output->size();
  switch (compressor_) {
    case Compressor::GZIP:
      GZip::compress(level_, &input_buffer, output);
      break;
    case Compressor::ZSTD:
      ZStd::compress(level_, zstd_compress_ctx_pool_, &input_buffer, output);
      break;
    case Compressor::LZ4:
      LZ4::compress(level_, &input_buffer, output);
      break;
    case Compressor::RLE:
      RLE::compress(cell_size, &input_buffer, output);
      break;
    case Compressor::BZIP2:
      BZip::compress(level_, &input_buffer, output);
      break;
    case Compressor::DOUBLE_DELTA:
      DoubleDelta::compress(
          reinterpret_datatype_ == Datatype::ANY ? filter_data_type_ :
                                                   reinterpret_datatype_,
          &input_buffer,
          output);
      break;
    case Compressor::DICTIONARY_ENCODING:
      return LOG_STATUS(
          Status_FilterError("CompressionFilter error; Dictionary encoding "
                             "only applies to variable length strings"));
    case Compressor::DELTA:
      // Use schema type if REINTERPRET_TYPE option is not set.
      Delta::compress(
          reinterpret_datatype_ == Datatype::ANY ? filter_data_type_ :
                                                   reinterpret_datatype_,
          &input_buffer,
          output);
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
      GZip::decompress(&input_buffer, &output_buffer);
      break;
    case Compressor::ZSTD:
      ZStd::decompress(
          zstd_decompress_ctx_pool_, &input_buffer, &output_buffer);
      break;
    case Compressor::LZ4:
      LZ4::decompress(&input_buffer, &output_buffer);
      break;
    case Compressor::RLE:
      RLE::decompress(cell_size, &input_buffer, &output_buffer);
      break;
    case Compressor::BZIP2:
      BZip::decompress(&input_buffer, &output_buffer);
      break;
    case Compressor::DELTA:
      Delta::decompress(
          reinterpret_datatype_ == Datatype::ANY ? filter_data_type_ :
                                                   reinterpret_datatype_,
          &input_buffer,
          &output_buffer);
      break;
    case Compressor::DOUBLE_DELTA:
      DoubleDelta::decompress(
          reinterpret_datatype_ == Datatype::ANY ? filter_data_type_ :
                                                   reinterpret_datatype_,
          &input_buffer,
          &output_buffer);
      break;
    case Compressor::DICTIONARY_ENCODING:
      return LOG_STATUS(
          Status_FilterError("CompressionFilter error; Dictionary encoding "
                             "only applies to variable length strings"));
  }

  if (output->owns_data())
    output->advance_size(uncompressed_size);
  output->advance_offset(uncompressed_size);
  input->advance_offset(compressed_size);

  return st;
}

tuple<std::vector<std::string_view>, uint64_t>
CompressionFilter::create_input_view(
    const FilterBuffer& input, WriterTile* const offsets_tile) {
  auto input_buf = static_cast<const char*>(input.buffers()[0].data());
  auto offsets_data = offsets_tile->data_as<offsets_t>();
  auto offsets_size = offsets_tile->size_as<offsets_t>();
  std::vector<std::string_view> input_view(offsets_size);

  size_t i = 0;
  uint64_t max_str_len = 0;
  for (i = 0; i < offsets_size - 1; i++) {
    auto length = offsets_data[i + 1] - offsets_data[i];
    input_view[i] = std::string_view(input_buf + offsets_data[i], length);
    max_str_len = std::max(length, max_str_len);
  }

  // special case for the last string
  input_view[i] = std::string_view(
      input_buf + offsets_data[i], input.size() - offsets_data[i]);
  max_str_len = std::max(input.size() - offsets_data[i], max_str_len);

  return {input_view, max_str_len};
}

uint8_t CompressionFilter::compute_bytesize(uint64_t param_length) {
  if (param_length <= std::numeric_limits<uint8_t>::max()) {
    return 1;
  } else if (param_length <= std::numeric_limits<uint16_t>::max()) {
    return 2;
  } else if (param_length <= std::numeric_limits<uint32_t>::max()) {
    return 4;
  } else {
    return 8;
  }
}

Status CompressionFilter::compress_var_string_coords(
    const FilterBuffer& input,
    WriterTile* const offsets_tile,
    FilterBuffer& output,
    FilterBuffer& output_metadata) const {
  if (input.num_buffers() != 1) {
    throw CompressionFilterStatusException(
        "Var-sized string input has to be in single "
        "buffer format to be compressed with RLE or Dictionary encoding");
  }

  // Construct string view of input
  auto [input_view, max_string_len] = create_input_view(input, offsets_tile);

  // Estimate metadata and output size
  size_t output_size_ub = 0, metadata_size = 0;
  uint8_t rle_len_bytesize = 0, string_len_bytesize = 0, max_id_bytesize = 0,
          max_strlen_bytesize = 0;
  uint32_t dict_size = 0;
  if (compressor_ == Compressor::RLE) {
    // Two extra metadata bytes to store run length datasize and string length
    // datasize
    metadata_size += 2 * sizeof(uint8_t);
    auto [rle_len, string_len, num_of_runs, output_strings_size] =
        RLE::calculate_compression_params(input_view);
    rle_len_bytesize = rle_len;
    string_len_bytesize = string_len;
    output_size_ub = num_of_runs * (rle_len_bytesize + string_len_bytesize) +
                     output_strings_size;
  } else if (compressor_ == Compressor::DICTIONARY_ENCODING) {
    auto num_strings = offsets_tile->size_as<offsets_t>();
    max_id_bytesize = compute_bytesize(num_strings);
    max_strlen_bytesize = compute_bytesize(max_string_len);
    // Allocate for worst case dict_size when all strings unique, in format:
    // [num_of_strings|size_str1|str1|...|size_strN|strN]
    dict_size =
        max_strlen_bytesize * static_cast<uint32_t>(num_strings) + input.size();
    // Extra metadata bytes to store the dictionary and string length datasize,
    // id size, and dict size
    metadata_size += 2 * sizeof(uint8_t) + sizeof(uint32_t) + dict_size;
    // Allocate for the worst case, assume all strings are unique and allocate a
    // datatype that can fit the ids for each unique string.
    output_size_ub = num_strings * max_id_bytesize;
  }

  // Allocate output metadata
  metadata_size += 5 * sizeof(uint32_t);
  uint32_t num_metadata_parts = 0;
  uint32_t num_data_parts = 1;
  // Note: assumes single buffer
  uint32_t input_size = input.buffers()[0].size();
  uint32_t input_offsets_size = offsets_tile->size();
  RETURN_NOT_OK(output_metadata.prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata.write(&num_metadata_parts, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata.write(&num_data_parts, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata.write(&input_size, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata.write(&output_size_ub, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata.write(&input_offsets_size, sizeof(uint32_t)));

  // Allocate output data buffer
  RETURN_NOT_OK(output.prepend_buffer(output_size_ub));
  Buffer* data_buffer = output.buffer_ptr(0);
  assert(data_buffer != nullptr);
  data_buffer->reset_offset();
  data_buffer->set_size(output_size_ub);
  auto output_view = span<std::byte>(
      reinterpret_cast<std::byte*>(data_buffer->data()), output_size_ub);

  if (compressor_ == Compressor::RLE) {
    RLE::compress(
        input_view, rle_len_bytesize, string_len_bytesize, output_view);
    RETURN_NOT_OK(output_metadata.write(&rle_len_bytesize, sizeof(uint8_t)));
    RETURN_NOT_OK(output_metadata.write(&string_len_bytesize, sizeof(uint8_t)));
  } else if (compressor_ == Compressor::DICTIONARY_ENCODING) {
    auto dict =
        DictEncoding::compress(input_view, max_id_bytesize, output_view);
    RETURN_NOT_OK(output_metadata.write(&max_id_bytesize, sizeof(uint8_t)));
    RETURN_NOT_OK(output_metadata.write(&max_strlen_bytesize, sizeof(uint8_t)));
    std::vector<std::byte> flattened_dict = DictEncoding::serialize_dictionary(
        dict, max_strlen_bytesize, dict_size);
    auto dict_size = flattened_dict.size();
    RETURN_NOT_OK(output_metadata.write(&dict_size, sizeof(uint32_t)));
    RETURN_NOT_OK(output_metadata.write(flattened_dict.data(), dict_size));
  }

  return Status::Ok();
}

Status CompressionFilter::decompress_var_string_coords(
    FilterBuffer& input,
    FilterBuffer& input_metadata,
    Tile* offsets_tile,
    FilterBuffer& output) const {
  if (input.num_buffers() != 1) {
    throw CompressionFilterStatusException(
        "Var-sized string input has to be in single "
        "buffer format to be decompressed with RLE or Dictionary encoding");
  }

  // Read the part metadata
  uint32_t compressed_size, uncompressed_size, uncompressed_offsets_size;
  RETURN_NOT_OK(input_metadata.read(&uncompressed_size, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata.read(&compressed_size, sizeof(uint32_t)));
  RETURN_NOT_OK(
      input_metadata.read(&uncompressed_offsets_size, sizeof(uint32_t)));

  // Get views of input and output
  auto input_buffer = input.buffers()[0];
  auto input_view = span<const std::byte>(
      reinterpret_cast<const std::byte*>(input_buffer.data()), compressed_size);
  Buffer* output_buffer = output.buffer_ptr(0);
  auto output_view = span<std::byte>(
      reinterpret_cast<std::byte*>(output_buffer->data()), uncompressed_size);
  auto offsets_view = span<uint64_t>(
      offsets_tile->data_as<offsets_t>(), uncompressed_offsets_size);

  if (compressor_ == Compressor::RLE) {
    uint8_t rle_len_bytesize, string_len_bytesize;
    RETURN_NOT_OK(input_metadata.read(&rle_len_bytesize, sizeof(uint8_t)));
    RETURN_NOT_OK(input_metadata.read(&string_len_bytesize, sizeof(uint8_t)));
    RLE::decompress(
        input_view,
        rle_len_bytesize,
        string_len_bytesize,
        output_view,
        offsets_view);
  } else if (compressor_ == Compressor::DICTIONARY_ENCODING) {
    uint8_t ids_bytesize = 0, string_len_bytesize = 0;
    uint32_t dict_size = 0;
    RETURN_NOT_OK(input_metadata.read(&ids_bytesize, sizeof(uint8_t)));
    RETURN_NOT_OK(input_metadata.read(&string_len_bytesize, sizeof(uint8_t)));
    RETURN_NOT_OK(input_metadata.read(&dict_size, sizeof(uint32_t)));
    std::vector<std::byte> flattened_dict(dict_size);
    RETURN_NOT_OK(input_metadata.read(flattened_dict.data(), dict_size));
    std::vector<std::string> dict = DictEncoding::deserialize_dictionary(
        flattened_dict, string_len_bytesize);
    DictEncoding::decompress(
        input_view, dict, ids_bytesize, output_view, offsets_view);
  }

  if (output_buffer->owns_data())
    output_buffer->advance_size(uncompressed_size);
  output_buffer->advance_offset(uncompressed_size);
  input.advance_offset(compressed_size);

  return Status::Ok();
}

uint64_t CompressionFilter::overhead(
    const WriterTile& tile, uint64_t nbytes) const {
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
    case Compressor::DELTA:
      return Delta::OVERHEAD;
    case Compressor::DICTIONARY_ENCODING:
    default:
      // No compression
      return 0;
  }
}

void CompressionFilter::serialize_impl(Serializer& serializer) const {
  if (compressor_ == Compressor::NO_COMPRESSION) {
    return;
  }
  auto compressor_char = static_cast<uint8_t>(compressor_);
  serializer.write<uint8_t>(compressor_char);
  serializer.write<int32_t>(level_);
  if (compressor_ == Compressor::DELTA ||
      compressor_ == Compressor::DOUBLE_DELTA) {
    serializer.write<uint8_t>(static_cast<uint8_t>(reinterpret_datatype_));
  }
}

void CompressionFilter::init_compression_resource_pool(uint64_t size) {
  std::lock_guard g(zstd_compress_ctx_pool_mtx_);
  if (zstd_compress_ctx_pool_ == nullptr) {
    zstd_compress_ctx_pool_ =
        make_shared<BlockingResourcePool<ZStd::ZSTD_Compress_Context>>(
            HERE(), size);
  }
}

void CompressionFilter::init_decompression_resource_pool(uint64_t size) {
  std::lock_guard g(zstd_decompress_ctx_pool_mtx_);
  if (zstd_decompress_ctx_pool_ == nullptr) {
    zstd_decompress_ctx_pool_ =
        make_shared<BlockingResourcePool<ZStd::ZSTD_Decompress_Context>>(
            HERE(), size);
  }
}

Datatype CompressionFilter::output_datatype(Datatype datatype) const {
  switch (compressor_) {
    case Compressor::DOUBLE_DELTA:
    case Compressor::DELTA:
      return reinterpret_datatype_ == Datatype::ANY ? datatype :
                                                      reinterpret_datatype_;
    default:
      return datatype;
  }
}

}  // namespace tiledb::sm
