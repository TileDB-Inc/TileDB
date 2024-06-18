/**
 * @file   byteshuffle_filter.h
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
 * This file declares class ByteshuffleFilter.
 */

#ifndef TILEDB_BYTESHUFFLE_FILTER_H
#define TILEDB_BYTESHUFFLE_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

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
   *
   * @param filter_data_type Datatype the filter will operate on.
   */
  ByteshuffleFilter(Datatype filter_data_type);

  /**
   * Shuffle the bytes of the input data into the output data buffer.
   */
  void run_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Unshuffle the bytes of the input data into the output data buffer.
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
  /** Returns a new clone of this filter. */
  ByteshuffleFilter* clone_impl() const override;

  /**
   * Perform byte shuffling on the given input buffer.
   *
   * @param tile Current tile on which the filter is being run
   * @param part Buffer containing data to be shuffled.
   * @param output Buffer to hold shuffled data.
   * @return Status
   */
  Status shuffle_part(
      const WriterTile& tile, const ConstBuffer* part, Buffer* output) const;

  /**
   * Perform byte unshuffling on the given input buffer.
   *
   * @param tile Current tile on which the filter is being run
   * @param part Buffer containing shuffled data.
   * @param output Buffer to hold unshuffled data.
   * @return Status
   */
  Status unshuffle_part(
      const Tile& tile, const ConstBuffer* part, Buffer* output) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BYTESHUFFLE_FILTER_H
