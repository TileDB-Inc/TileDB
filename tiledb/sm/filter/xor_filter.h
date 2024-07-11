/**
 * @file   xor_filter.h
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
 * This file declares class XORFilter.
 */

#ifndef TILEDB_XOR_FILTER_H
#define TILEDB_XOR_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * The XOR filter stores the input data passed into it as the starting
 * element and all the differences between the consecutive pairs of
 * elements.
 *
 * On write, the XOR filter stores an array of integers (with size n) by storing
 * the starting element and then storing the XOR between the next
 * n-1 consecutive pairs of elements.
 *
 * On read, the XOR filter reverses this transformation and returns the values
 * of the original elements.
 *
 */
class XORFilter : public Filter {
 public:
  /**
   * Constructor.
   *
   * @param filter_data_type Datatype the filter will operate on.
   */
  XORFilter(Datatype filter_data_type)
      : Filter(FilterType::FILTER_XOR, filter_data_type) {
  }

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

  /**
   * Run forward. Takes input data parts, and per part it stores the first
   * element in the part, and then the differences of each consecutive pair
   * of elements.
   */
  void run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Run reverse. Takes input data parts, and per part it reverses the
   * transformation in run_forward, returning the original input array passed in
   * run_forward.
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

 protected:
  /** Dumps the filter details in ASCII format in the selected output string. */
  std::ostream& output(std::ostream& os) const override;

 private:
  /**
   * Run forward, templated on the tile type.
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  void run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run reverse, templated on the tile type.
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * XORs the input buffer by storing the first element, then
   * storing the XORed value between each consecutive element pair.
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  Status xor_part(const ConstBuffer* part, Buffer* output) const;

  /**
   * Un-XORs the input buffer by restoring the input buffer (which
   * contains the starting element and the XORs between each
   * consecutive element pair) to the original array.
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  Status unxor_part(const ConstBuffer* part, Buffer* output) const;

  /** Returns a new clone of this filter. */
  XORFilter* clone_impl() const override;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_XOR_FILTER_H
