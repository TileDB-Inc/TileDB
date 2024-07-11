/**
 * @file add_1_out_of_place_filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Simple filter that increments every element of the input stream, writing the
 * output to another buffer. Does not modify the input stream.
 *
 * This filter is for use in running filter pipeline tests.
 */

#ifndef TILEDB_ADD_1_OUT_OF_PLACE_FILTER_H
#define TILEDB_ADD_1_OUT_OF_PLACE_FILTER_H

#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * Simple filter that increments every element of the input stream, writing the
 * output to another buffer. Does not modify the input stream.
 */
class Add1OutOfPlace : public tiledb::sm::Filter {
 public:
  Add1OutOfPlace(Datatype filter_data_type);

  void run_forward(
      const WriterTile&,
      WriterTile* const,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  Status run_reverse(
      const Tile&,
      Tile*,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const tiledb::sm::Config& config) const override;

  Add1OutOfPlace* clone_impl() const override;

 protected:
  inline std::ostream& output(std::ostream& os) const override {
    return os;
  };
};

}  // namespace tiledb::sm

#endif
