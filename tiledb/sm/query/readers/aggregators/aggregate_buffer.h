/**
 * @file   aggregate_buffer.h
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
 * This file defines class AggregateBuffer.
 */

#ifndef TILEDB_AGGREGATE_BUFFER_H
#define TILEDB_AGGREGATE_BUFFER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/layout.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class AggregateBuffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param min_cell Min cell position to aggregate.
   * @param max_cell Max cell position to aggregate.
   * @param cell_num Cell num for the tile.
   * @param fixed_data Fixed data buffer.
   * @param var_data Var data buffer.
   * @param validity_data Validity data buffer.
   * @param count_bitmap Is the bitmap a count bitmap?
   * @param name Name of the field for the buffer.
   * @param var_sized Is the field var sized?
   * @param nullable Is the field nullable?
   * @param rt Result tile containing the data.
   * @param bitmap_data Bitmap data.
   */
  AggregateBuffer(
      const uint64_t min_cell,
      const uint64_t max_cell,
      const uint64_t cell_num,
      const void* fixed_data,
      const optional<char*> var_data,
      const uint64_t var_data_size,
      const optional<uint8_t*> validity_data,
      const bool count_bitmap,
      const optional<void*> bitmap_data)
      : includes_last_var_cell_(var_data.has_value() && max_cell == cell_num)
      , min_cell_(min_cell)
      , max_cell_(max_cell)
      , fixed_data_(fixed_data)
      , var_data_(var_data)
      , var_data_size_(var_data_size)
      , validity_data_(validity_data)
      , count_bitmap_(count_bitmap)
      , bitmap_data_(bitmap_data) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(AggregateBuffer);
  DISABLE_MOVE_AND_MOVE_ASSIGN(AggregateBuffer);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns a typed fixed data buffer. */
  template <class T>
  const T* fixed_data_as() const {
    return static_cast<const T*>(fixed_data_);
  }

  /** Returns the var data. */
  char* var_data() const {
    return var_data_.value();
  }

  /** Returns the validity buffer. */
  uint8_t* validity_data() const {
    return validity_data_.value();
  }

  /** Returns if the bitmap is a count bitmap. */
  bool is_count_bitmap() const {
    return count_bitmap_;
  }

  /** Returns wether this buffer has a bitmap or not. */
  bool has_bitmap() const {
    return bitmap_data_.has_value();
  }

  /** Returns types bitmap data. */
  template <class BitmapType>
  BitmapType* bitmap_data_as() const {
    return static_cast<BitmapType*>(bitmap_data_.value());
  }

  /** Returns the min cell position to aggregate. */
  uint64_t min_cell() const {
    return min_cell_;
  }

  /** Returns the max cell position to aggregate. */
  uint64_t max_cell() const {
    return max_cell_;
  }

  /** Returns if this buffer includes the last cell of a tile. */
  bool includes_last_var_cell() const {
    return includes_last_var_cell_;
  }

  /**
   * Returns the var data size. Will only be non 0 if the buffers include the
   * last cell of a var data input.
   */
  uint64_t var_data_size() const {
    return var_data_size_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Does this buffer include the last var cell of the tile. */
  const bool includes_last_var_cell_;

  /** Min cell to aggregate. */
  const uint64_t min_cell_;

  /** Max cell to aggregate. */
  const uint64_t max_cell_;

  /** Pointer to the fixed data. */
  const void* fixed_data_;

  /** Pointer to the var data. */
  const optional<char*> var_data_;

  /** Var data size. */
  uint64_t var_data_size_;

  /** Pointer to the validity data. */
  const optional<uint8_t*> validity_data_;

  /** Is the bitmap a count bitmap? */
  const bool count_bitmap_;

  /** Pointer to the bitmap data. */
  const optional<void*> bitmap_data_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_AGGREGATE_BUFFER_H
