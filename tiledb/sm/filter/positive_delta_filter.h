/**
 * @file   positive_delta_filter.h
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
 * This file declares class PositiveDeltaFilter.
 */

#ifndef TILEDB_POSITIVE_DELTA_FILTER_H
#define TILEDB_POSITIVE_DELTA_FILTER_H

#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * A filter that encodes an array of integers with delta encoding where the
 * deltas must be positive. An error is returned when filtering data that has
 * negative deltas.
 *
 * The input is encoded within windows of size N bytes. If the input comes in
 * multiple FilterBuffer parts, each part is broken up into windows separately
 * in the forward direction. The rest of the elements in the window are stored
 * relative to the first value in the window (the delta offset).
 *
 * Input metadata is not compressed or modified.
 *
 * The forward output metadata has the format:
 *   uint32_t - Number of windows
 *   window0_md
 *   ...
 *   windowN_md
 * Where each window*_md has the fixed format:
 *   T - Window value delta offset
 *   uint32_t - Size of window in bytes
 *
 * The forward output data format is the concatenated window data:
 *   T[] - Window0 delta-encoded data
 *   T[] - Window1 delta-encoded data
 *   ...
 *   T[] - WindowN delta-encoded data
 *
 * The reverse output format is simply:
 *   T[] - Array of original elements
 */
class PositiveDeltaFilter : public Filter {
 public:
  /** Constructor. */
  PositiveDeltaFilter();

  /** Return the max window size used by the filter. */
  uint32_t max_window_size() const;

  /**
   * Perform positive-delta encoding of the given input into the given output.
   */
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Perform positive-delta decoding of the given input into the given output.
   */
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /** Set the max window size (in bytes) to use. */
  void set_max_window_size(uint32_t max_window_size);

 private:
  /** Maximum size, in bytes, of a window of input elements to compress. */
  uint32_t max_window_size_;

  /** Returns a new clone of this filter. */
  PositiveDeltaFilter* clone_impl() const override;

  /**
   * Encode a part of the filter input.
   *
   * @tparam T Tile cell datatype
   * @param input Buffer to encode
   * @param output Buffer to store encoded output.
   * @param output_metadata Buffer to store output metadata.
   * @return Status
   */
  template <typename T>
  Status encode_part(
      ConstBuffer* input,
      FilterBuffer* output,
      FilterBuffer* output_metadata) const;

  /** Deserializes this filter's metadata from the given buffer. */
  Status deserialize_impl(ConstBuffer* buff) override;

  /** Gets an option from this filter. */
  Status get_option_impl(FilterOption option, void* value) const override;

  /** Run_forward method templated on the tile cell datatype. */
  template <typename T>
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /** Run_reverse method templated on the tile cell datatype. */
  template <typename T>
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /** Sets an option on this filter. */
  Status set_option_impl(FilterOption option, const void* value) override;

  /** Serializes this filter's metadata to the given buffer. */
  Status serialize_impl(Buffer* buff) const override;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_POSITIVE_DELTA_FILTER_H
