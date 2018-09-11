/**
 * @file   bitshuffle_filter.h
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
 * This file declares class BitshuffleFilter.
 */

#ifndef TILEDB_BITSHUFFLE_FILTER_H
#define TILEDB_BITSHUFFLE_FILTER_H

#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/buffer/preallocated_buffer.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * A filter that performs a "bit shuffle" of the input data into the output data
 * buffer.
 *
 * If the input comes in multiple FilterBuffer parts, each part is shuffled
 * independently in the forward direction.
 *
 * Additionally, because the bitshuffling function used requires the input
 * length to be divisible by 8, input parts may each be broken into two parts:
 * the first divisible by 8, and the second the remaining bytes (which are not
 * shuffled, just copied).
 *
 * Input metadata is not modified.
 *
 * The forward output metadata has the format:
 *   uint32_t - Number of parts
 *   uint32_t - Number of bytes of part0
 *   ...
 *   uint32_t - Number of bytes of partN
 *
 * The forward output data is the concatenated shuffled bytes:
 *   uint8_t[] - Shuffled bits of part0
 *   ...
 *   uint8_t[] - Shuffled bits of partN
 *
 * The reverse output data format is simply:
 *   uint8_t[] - Original input data
 */
class BitshuffleFilter : public Filter {
 public:
  /**
   * Constructor.
   */
  BitshuffleFilter();

  /**
   * Shuffle the bits of the input data into the output data buffer.
   */
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Unshuffle the bits of the input data into the output data buffer.
   */
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

 private:
  /** Returns a new clone of this filter. */
  BitshuffleFilter* clone_impl() const override;

  /**
   * Computes the parts to be shuffled in the given input, accounting for
   * rounding lengths to multiple of 8.
   *
   * @param input Input to break into parts
   * @param parts Set to the list of parts
   * @return Status
   */
  Status compute_parts(
      FilterBuffer* input, std::vector<ConstBuffer>* parts) const;

  /**
   * Perform bit shuffling on the given input buffer.
   *
   * @param part Buffer containing data to be shuffled.
   * @param output Buffer to hold shuffled data.
   * @return Status
   */
  Status shuffle_part(const ConstBuffer* part, Buffer* output) const;

  /**
   * Perform bit unshuffling on the given input buffer.
   *
   * @param part Buffer containing shuffled data.
   * @param output Buffer to hold unshuffled data.
   * @return Status
   */
  Status unshuffle_part(const ConstBuffer* input, Buffer* output) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BITSHUFFLE_FILTER_H
