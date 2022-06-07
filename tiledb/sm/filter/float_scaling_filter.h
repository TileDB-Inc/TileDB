/**
 * @file   float_scaling_filter.h
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
 * This file declares class FloatScalingFilter.
 */

#ifndef TILEDB_FLOAT_SCALING_FILTER_H
#define TILEDB_FLOAT_SCALING_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A filter that does the float scaly thingo.
 * TODO: COMMENT
 */
class FloatScalingFilter : public Filter {
 public:
  /** Struct used for serialization and deserialization from disk. */
  struct Metadata {
    double s;    // scale
    double o;    // offset
    uint64_t b;  // bit width
  };
  /**
   * Constructor.
   */
  FloatScalingFilter()
      : Filter(FilterType::FILTER_SCALE_FLOAT) {
  }

  FloatScalingFilter(uint64_t bit_width, double scale, double offset)
      : Filter(FilterType::FILTER_SCALE_FLOAT)
      , scale_(scale)
      , offset_(offset)
      , bit_width_(bit_width) {
  }

  /** Dumps the filter details in ASCII format in the selected output. */
  void dump(FILE* out) const override;

  /** Serializes this filter's metadata to the given buffer. */
  Status serialize_impl(Buffer* buff) const override;

  /**
   * Run forward.
   */
  Status run_forward(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Run reverse.
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /** Sets an option on this filter. */
  Status set_option_impl(FilterOption option, const void* value) override;

  /** Gets an option from this filter. */
  Status get_option_impl(FilterOption option, void* value) const override;

 private:
  /** The scale factor. */
  double scale_;

  /** The offset factor. */
  double offset_;

  /** The bit width of the compressed representation. */
  uint64_t bit_width_;

  /** Returns a new clone of this filter. */
  FloatScalingFilter* clone_impl() const override;

  /**
   * Run forward, templated on the size of the input type.
   */
  template <typename T>
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run forward, templated on the size of the input type and bit width.
   */
  template <typename T, typename W>
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run reverse, templated on the size of the input type.
   */
  template <typename T>
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run reverse, templated on the size of the input type and bit width.
   */
  template <typename T, typename W>
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FLOAT_SCALING_FILTER_H