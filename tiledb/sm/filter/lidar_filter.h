/**
 * @file   lidar_filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines the lidar compressor class.
 */

#ifndef TILEDB_LIDAR_FILTER_H
#define TILEDB_LIDAR_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/compressors/bzip_compressor.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/xor_filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;

/** Handles compression/decompression of lidar data (similar to LASzip).
 * TODO: comment more
 */
class LidarFilter : public Filter {
 public:
  /**
   * Default constructor.
   */
  LidarFilter()
      : Filter(FilterType::FILTER_LIDAR)
      , compressor_filter_(Compressor::BZIP2, BZip::default_level()) {
  }

  /** Dumps the filter details in ASCII format in the selected output. */
  void dump(FILE* out) const override;

  /**
   * Run forward. TODO: COMMENT
   */
  Status run_forward(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Run reverse. TODO: comment
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /** Returns a new clone of this filter. */
  LidarFilter* clone_impl() const override;

 private:
  /**
   * Run forward, templated on the tile type.
   */

  template <typename T>
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run reverse, templated on the tile type.
   */
  template <typename T>
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const;

  /**
   * shuffle part
   */
  template <typename T>
  Status shuffle_part(
      ConstBuffer* input_buffer, FilterBuffer* output_buffer) const;

  /**
   * TODO: comment
   */
  template <typename T>
  Status unshuffle_part(
      ConstBuffer* input_buffer,
      FilterBuffer* output_buffer,
      const Config& config) const;

  XORFilter xor_filter_;
  CompressionFilter compressor_filter_;
};

};  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_LIDAR_FILTER_H