/**
 * @file   whitebox_aggregate_buffer.h
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
 * This file defines class IAggregator.
 */

#ifndef TILEDB_WHITEBOX_AGGREGATE_BUFFER_H
#define TILEDB_WHITEBOX_AGGREGATE_BUFFER_H

namespace tiledb::sm {
class WhiteboxAggregateBuffer {
 public:
  static AggregateBuffer make_aggregate_buffer(
      const uint64_t min_cell,
      const uint64_t max_cell,
      const uint64_t cell_num,
      const void* fixed_data,
      const optional<char*> var_data,
      const uint64_t var_data_size,
      const optional<uint8_t*> validity_data,
      const bool count_bitmap,
      const optional<void*> bitmap_data) {
    return AggregateBuffer(
        min_cell,
        max_cell,
        cell_num,
        fixed_data,
        var_data,
        var_data_size,
        validity_data,
        count_bitmap,
        bitmap_data);
  }
};
}  // namespace tiledb::sm

#endif  // TILEDB_WHITEBOX_AGGREGATE_BUFFER_H