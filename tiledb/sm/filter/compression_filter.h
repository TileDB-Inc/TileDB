/**
 * @file   compression_filter.h
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
 * This file declares class CompressionFilter.
 */

#ifndef TILEDB_COMPRESSION_FILTER_H
#define TILEDB_COMPRESSION_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/compressors/zstd_compressor.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/resource_pool.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

enum class Compressor : uint8_t;

/**
 * A filter that compresses/decompresses its input data. The FilterBuffer input
 * to a filter may contain multiple buffers. Each input buffer is termed a
 * "part", and is compressed separately by this filter. Input metadata is
 * compressed as well.
 *
 * The forward (compress) output metadata has the format:
 *   uint32_t - Number of compressed metadata parts
 *   uint32_t - Number of compressed data parts
 *   metadata_part0
 *   ...
 *   metadata_partN
 *   data_part0
 *   ...
 *   data_partN
 * Where each metadata_part/data_part has the format:
 *   uint32_t - part uncompressed length
 *   uint32_t - part compressed length
 *
 * The forward output data format is just the concatenated compressed bytes:
 *   uint8_t[] - metadata_part0's array of compressed bytes
 *   ...
 *   uint8_t[] - metadata_partN's array of compressed bytes
 *   uint8_t[] - data_part0's array of compressed bytes
 *   ...
 *   uint8_t[] - data_partN's array of compressed bytes
 *
 * The reverse (decompress) output format is simply:
 *   uint8_t[] - Array of uncompressed bytes
 */
class CompressionFilter : public Filter {
 public:
  /**
   * Constructor.
   *
   * @param compressor Compressor to use
   * @param level Compression level to use
   * @param version Format version
   */
  CompressionFilter(
      Compressor compressor,
      int level,
      const uint32_t version = constants::format_version);

  /**
   * Constructor.
   *
   * @param compressor Compressor to use
   * @param level Compression level to use
   * @param version Format version
   */
  CompressionFilter(
      FilterType compressor,
      int level,
      const uint32_t version = constants::format_version);

  /** Return the compressor used by this filter instance. */
  Compressor compressor() const;

  /** Return the compression level used by this filter instance. */
  int compression_level() const;

  /** Dumps the filter details in ASCII format in the selected output. */
  void dump(FILE* out) const override;

  /**
   * Compress the given input into the given output.
   */
  Status run_forward(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Decompress the given input into the given output.
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /** Set the compressor used by this filter instance. */
  void set_compressor(Compressor compressor);

  /** Set the compression level used by this filter instance. */
  void set_compression_level(int compressor_level);

 private:
  /** The compressor. */
  Compressor compressor_;

  /** The compression level. */
  int level_;

  /** The format version. */
  int version_;

  /** The default filter compression level. */
  static constexpr int default_level_ = -30000;

  /** Mutex guarding zstd_compress_ctx_pool */
  std::mutex zstd_compress_ctx_pool_mtx_;

  /** Mutex guarding zstd_decompress_ctx_pool */
  std::mutex zstd_decompress_ctx_pool_mtx_;

  /** A resource pool to be used in ZStd compressor for improved performance */
  std::shared_ptr<BlockingResourcePool<ZStd::ZSTD_Compress_Context>>
      zstd_compress_ctx_pool_;

  /** A resource pool to be used in ZStd decompressor for improved performance
   */
  shared_ptr<BlockingResourcePool<ZStd::ZSTD_Decompress_Context>>
      zstd_decompress_ctx_pool_;

  /** Returns a new clone of this filter. */
  CompressionFilter* clone_impl() const override;

  /** Helper function to compress a single contiguous buffer (part). */
  Status compress_part(
      const Tile& tile,
      ConstBuffer* part,
      Buffer* output,
      FilterBuffer* output_metadata) const;

  /** Return the FilterType corresponding to the given Compressor. */
  static FilterType compressor_to_filter(Compressor compressor);

  /**
   * Helper function to decompress a single contiguous buffer (part), appending
   * onto the single output buffer.
   */
  Status decompress_part(
      const Tile& tile,
      FilterBuffer* input,
      Buffer* output,
      FilterBuffer* input_metadata) const;

  /** Calculate the size of the output metadata to allocate */
  size_t calculate_output_metadata_size(
      const Tile& tile,
      const std::vector<ConstBuffer>& data_parts,
      const std::vector<ConstBuffer>& metadata_parts) const;

  /**
   * Helper function to compress a buffer of variable-sized strings for certain
   * algorithms where this is a special case
   */
  Status compress_var_string_coords(
      const FilterBuffer& input,
      Tile* const offsets_tile,
      FilterBuffer& output,
      FilterBuffer& output_metadata) const;

  /**
   * Helper function to decompress a buffer of variable-sized strings for
   * certain algorithms where this is a special case
   */
  Status decompress_var_string_coords(
      FilterBuffer& input,
      FilterBuffer& input_metadata,
      Tile* offsets_tile,
      FilterBuffer& output) const;

  /** Gets an option from this filter. */
  Status get_option_impl(FilterOption option, void* value) const override;

  /** Return the Compressor corresponding to the given FilterType. */
  static Compressor filter_to_compressor(FilterType type);

  /** Computes the compression overhead on nbytes of the input data. */
  uint64_t overhead(const Tile& tile, uint64_t nbytes) const;

  /** Sets an option on this filter. */
  Status set_option_impl(FilterOption option, const void* value) override;

  /** Serializes this filter's metadata to the given buffer. */
  Status serialize_impl(Buffer* buff) const override;

  /** Initializes the compression resource pool */
  void init_compression_resource_pool(uint64_t size) override;

  /** Initializes the decompression resource pool */
  void init_decompression_resource_pool(uint64_t size) override;

  /** Creates a vector of views of the input strings */
  static std::vector<std::string_view> create_input_view(
      const FilterBuffer& input, Tile* const offsets_tile);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_COMPRESSION_FILTER_H
