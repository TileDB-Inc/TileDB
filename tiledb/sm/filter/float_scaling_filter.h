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
 * The float scaling filter stores the input data with floating point type
 * as an integer type for more compressed storage. The float scaling filter
 * takes three parameters: the scale, the offset, and the byte width.
 *
 * On write, the float scaling filter applies the scale factor and offset,
 * and stores the value of round((raw_float - offset) / scale) as an
 * integer with the specified byte width.
 *
 * On read, the float scaling filter will reverse the scale factor and offset,
 * and returns the floating point data, with a potential loss of precision.
 *
 */
class FloatScalingFilter : public Filter {
 public:
  /** Struct used for serialization and deserialization from disk. */
  struct FilterConfig {
    double scale;
    double offset;
    uint64_t byte_width;
  };
  /**
   * Default constructor. Default settings for Float Scaling Filter are
   * scale = 1.0f, offset = 0.0f, and byte_width = 8.
   *
   * @param filter_data_type Datatype the filter will operate on.
   */
  FloatScalingFilter(Datatype filter_data_type)
      : Filter(FilterType::FILTER_SCALE_FLOAT, filter_data_type)
      , scale_(1.0f)
      , offset_(0.0f)
      , byte_width_(8) {
  }

  /**
   * Full constructor.
   *
   * @param byte_width The byte width of the compressed representation.
   * @param scale The scale factor.
   * @param offset The offset factor.
   * @param filter_data_type Datatype the filter will operate on.
   */
  FloatScalingFilter(
      uint64_t byte_width,
      double scale,
      double offset,
      Datatype filter_data_type)
      : Filter(FilterType::FILTER_SCALE_FLOAT, filter_data_type)
      , scale_(scale)
      , offset_(offset)
      , byte_width_(byte_width) {
  }

  /** Serializes this filter's metadata to the given buffer. */
  void serialize_impl(Serializer& serializer) const override;

  /**
   * Run forward. Takes input data in floating point representation and
   * stores it as integers with the value round((raw_float - offset) / scale)
   * with the pre-specified byte width.
   */
  void run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Run reverse. Takes input data in integer representation and returns the
   * values as floating point numbers with the value (stored_int * scale) +
   * offset with the original type of the floating point data.
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /** Sets an option on this filter. */
  Status set_option_impl(FilterOption option, const void* value) override;

  /** Gets an option from this filter. */
  Status get_option_impl(FilterOption option, void* value) const override;

  /**
   * Checks if the filter is applicable to the input datatype.
   *
   * @param type Input datatype to check filter compatibility.
   */
  bool accepts_input_datatype(Datatype datatype) const override;

  /**
   * Returns the filter output type
   *
   * @param input_type Expected type used for input. Used for filters which
   * change output type based on input data. e.g. XORFilter output type is
   * based on byte width of input type.
   */
  Datatype output_datatype(Datatype input_type) const override;

 protected:
  /** Dumps the filter details in ASCII format in the selected output string. */
  std::ostream& output(std::ostream& os) const override;

 private:
  /** The scale factor. */
  double scale_;

  /** The offset factor. */
  double offset_;

  /** The byte width of the compressed representation. */
  uint64_t byte_width_;

  /** Returns a new clone of this filter. */
  FloatScalingFilter* clone_impl() const override;

  /**
   * Run forward, templated on the size of the input type.
   */
  template <typename T>
  void run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run forward, templated on the size of the input type and byte width.
   */
  template <typename T, typename W>
  void run_forward(
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
   * Run reverse, templated on the size of the input type and byte width.
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
