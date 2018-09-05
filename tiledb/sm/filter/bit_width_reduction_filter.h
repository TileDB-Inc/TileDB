/**
 * @file   reduce_type_width_filter.h
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
 * This file declares class BitWidthReductionFilter.
 */

#ifndef TILEDB_BIT_WIDTH_REDUCTION_FILTER_H
#define TILEDB_BIT_WIDTH_REDUCTION_FILTER_H

#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * A filter that compresses an array of unsigned integers by reducing the number
 * of bits per element if possible.
 *
 * When compressing, the filter determines the min and max values of the input
 * elements within a window of size N. If the range of values can be represented
 * by an integer of a smaller width, the input elements in the window are
 * transformed to the smaller width and written to the output. Else, the input
 * elements are written to the output unmodified.
 *
 * Within a window, all elements are treated relative to the first input
 * element, which helps in cases of e.g. values that are large but involve
 * relatively small value changes over the window.
 *
 * If the input comes in multiple FilterBuffer parts, each part is broken up
 * into windows separately in the forward direction.
 *
 * Input metadata is not compressed or modified.
 *
 * The forward output metadata has the format:
 *   uint32_t - Original input number of bytes
 *   uint32_t - Number of windows
 *   window0_md
 *   ...
 *   windowN_md
 * Where each window*_md has the fixed format:
 *   T - Window value offset
 *   uint8_t - Bit width of reduced element type T'
 *   uint32_t - Number of bytes in window data
 *
 * The forward output data format is the concatenated window data:
 *   uint8_t[] - Window0 data (possibly-reduced width elements)
 *   uint8_t[] - Window1 data (possibly-reduced width elements)
 *   ...
 *   uint8_t[] - WindowN data (possibly-reduced width elements)
 *
 * The reverse output format is simply:
 *   T[] - Array of original elements
 */
class BitWidthReductionFilter : public Filter {
 public:
  /** Constructor. */
  BitWidthReductionFilter();

  /** Return the max window size used by the filter. */
  uint32_t max_window_size() const;

  /**
   * Reduce the bit size of the given input into the given output.
   */
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Restore the bit size the given input into the given output.
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
  BitWidthReductionFilter* clone_impl() const override;

  /**
   * Compress a part of the filter input.
   *
   * @tparam T Tile cell datatype
   * @param input Buffer to compress
   * @param output Buffer to store compressed output.
   * @param output_metadata Buffer to store output metadata.
   * @return Status
   */
  template <typename T>
  Status compress_part(
      ConstBuffer* input,
      FilterBuffer* output,
      FilterBuffer* output_metadata) const;

  /**
   * Computes the number of bits required to represent elements of type T in the
   * given buffer when the element values are normalized to 0.
   *
   * @tparam T Tile cell datatype
   * @param buffer Buffer to consider
   * @param num_elements Number of elements in the window to consider from
   *    buffer
   * @param min_value Will be set to the minimum element value in the window.
   * @return Number of bits (8, 16, 32, or 64).
   */
  template <typename T>
  uint8_t compute_bits_required(
      ConstBuffer* buffer, uint32_t num_elements, T* min_value) const;

  /** Deserializes this filter's metadata from the given buffer. */
  Status deserialize_impl(ConstBuffer* buff) override;

  /** Gets an option from this filter. */
  Status get_option_impl(FilterOption option, void* value) const override;

  /**
   * Reads a compressed value of type T from the given buffer after
   * decompressing from a value of the given bit width.
   *
   * @tparam T Tile cell datatype
   * @param buffer Buffer to read from
   * @param compressed_bits Bit width of compressed value to read
   * @param value Will be set to the decompressed value
   * @return Status
   */
  template <typename T>
  Status read_compressed_value(
      FilterBuffer* buffer, uint8_t compressed_bits, T* value) const;

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

  /**
   * Writes the given value of type T to the given buffer after compressing
   * (casting) to a value of the given bit width.
   *
   * @param buffer Buffer to write to
   * @param value Uncompressed value to write
   * @param num_bits Bit width of compressed value to write
   * @return Status
   */
  template <typename T>
  Status write_compressed_value(
      FilterBuffer* buffer, T value, uint8_t num_bits) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BIT_WIDTH_REDUCTION_FILTER_H
