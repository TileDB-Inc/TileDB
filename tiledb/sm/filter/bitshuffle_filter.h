/**
 * @file   bitshuffle_filter.h
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
 * This file declares class BitshuffleFilter.
 */

#ifndef TILEDB_BITSHUFFLE_FILTER_H
#define TILEDB_BITSHUFFLE_FILTER_H

#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

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
   *
   * @param filter_data_type Datatype the filter will operate on.
   */
  BitshuffleFilter(Datatype filter_data_type);

  /**
   * Shuffle the bits of the input data into the output data buffer.
   */
  void run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Unshuffle the bits of the input data into the output data buffer.
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /**
   * Perform bit shuffling on the given input buffer.
   *
   * @param filter_data_type Datatype of the data to be shuffled.
   * @param part Buffer containing data to be shuffled.
   * @param output Buffer to hold shuffled data.
   * @param invert If true, performs a bit unshuffle instead of a bit shuffle.
   * @return Status
   */
  static Status shuffle_part(
      Datatype filter_data_type, const ConstBuffer& part, Buffer& output, bool invert);

 protected:
  /** Dumps the filter details in ASCII format in the selected output string. */
  std::ostream& output(std::ostream& os) const override;

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
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BITSHUFFLE_FILTER_H
