/**
 * @file   byteshuffle_filter.h
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
 * This file declares class ByteshuffleFilter.
 */

#ifndef TILEDB_BYTESHUFFLE_FILTER_H
#define TILEDB_BYTESHUFFLE_FILTER_H

#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/buffer/preallocated_buffer.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * A filter that performs a "byte shuffle" of the input data into the output
 * data buffer.
 *
 * If the input comes in multiple FilterBuffer parts, each part is shuffled
 * independently in the forward direction.
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
 *   uint8_t[] - Shuffled bytes of part0
 *   ...
 *   uint8_t[] - Shuffled bytes of partN
 *
 * The reverse output data format is simply:
 *   uint8_t[] - Original input data
 */
class ByteshuffleFilter : public Filter {
 public:
  /**
   * Constructor.
   */
  ByteshuffleFilter();

  /**
   * Shuffle the bytes of the input data into the output data buffer.
   */
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Unshuffle the bytes of the input data into the output data buffer.
   */
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

 private:
  /** Returns a new clone of this filter. */
  ByteshuffleFilter* clone_impl() const override;

  /**
   * Perform byte shuffling on the given input buffer.
   *
   * @param part Buffer containing data to be shuffled.
   * @param output Buffer to hold shuffled data.
   * @return Status
   */
  Status shuffle_part(const ConstBuffer* part, Buffer* output) const;

  /**
   * Perform byte unshuffling on the given input buffer.
   *
   * @param part Buffer containing shuffled data.
   * @param output Buffer to hold unshuffled data.
   * @return Status
   */
  Status unshuffle_part(const ConstBuffer* input, Buffer* output) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BYTESHUFFLE_FILTER_H
