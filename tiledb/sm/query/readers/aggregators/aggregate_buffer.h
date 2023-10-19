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

#include "tiledb/common/common.h"

namespace tiledb::sm {

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
   * @param fixed_data Fixed data buffer.
   * @param var_data Var data buffer.
   * @param validity_data Validity data buffer.
   * @param count_bitmap Is the bitmap a count bitmap?
   * @param bitmap_data Bitmap data.
   * @param cell_size Cell size.
   */
  AggregateBuffer(
      const uint64_t min_cell,
      const uint64_t max_cell,
      const void* fixed_data,
      const optional<char*> var_data,
      const optional<uint8_t*> validity_data,
      const bool count_bitmap,
      const optional<void*> bitmap_data,
      const uint64_t cell_size)
      : min_cell_(min_cell)
      , max_cell_(max_cell)
      , fixed_data_(fixed_data)
      , var_data_(var_data)
      , validity_data_(validity_data)
      , count_bitmap_(count_bitmap)
      , bitmap_data_(bitmap_data)
      , cell_size_(cell_size) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(AggregateBuffer);
  DISABLE_MOVE_AND_MOVE_ASSIGN(AggregateBuffer);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns if the bitmap is a count bitmap. */
  bool is_count_bitmap() const {
    return count_bitmap_;
  }

  /** Returns wether this buffer has a bitmap or not. */
  bool has_bitmap() const {
    return bitmap_data_.has_value();
  }

  /** Returns the number of cells to aggregate. */
  uint64_t size() const {
    return max_cell_ - min_cell_;
  }

  /**
   * Get the value at a certain cell index.
   *
   * @tparam VALUE_T Value type.
   * @tparam VALUE_T Returned value type.
   * @param cell_idx Cell index.
   *
   * @return Value.
   */
  template <typename T>
  inline T value_at(uint64_t cell_idx) const {
    cell_idx += min_cell_;
    if constexpr (std::is_same_v<T, std::string_view>) {
      if (var_data_.has_value()) {
        auto offsets = static_cast<const uint64_t*>(fixed_data_);
        // Return the var sized string.
        uint64_t offset = offsets[cell_idx];
        uint64_t next_offset = offsets[cell_idx + 1];
        return {var_data_.value() + offset, next_offset - offset};
      } else {
        // Return the fixed size string.
        return {
            static_cast<const char*>(fixed_data_) + cell_size_ * cell_idx,
            cell_size_};
      }
    } else {
      return static_cast<const T*>(fixed_data_)[cell_idx];
    }
  }

  /**
   * Get the validity value at a certain cell index.
   *
   * @param cell_idx Cell index.
   *
   * @return Validity value.
   */
  inline uint8_t validity_at(const uint64_t cell_idx) const {
    return validity_data_.value()[cell_idx + min_cell_];
  }

  /**
   * Get the bitmap value at a certain cell index.
   *
   * @param cell_idx Cell index.
   *
   * @return Bitmap value.
   */
  template <class BitmapType>
  inline BitmapType bitmap_at(const uint64_t cell_idx) const {
    return static_cast<BitmapType*>(bitmap_data_.value())[cell_idx + min_cell_];
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Min cell to aggregate. */
  const uint64_t min_cell_;

  /** Max cell to aggregate. */
  const uint64_t max_cell_;

  /** Pointer to the fixed data. */
  const void* fixed_data_;

  /** Pointer to the var data. */
  const optional<char*> var_data_;

  /** Pointer to the validity data. */
  const optional<uint8_t*> validity_data_;

  /** Is the bitmap a count bitmap? */
  const bool count_bitmap_;

  /** Pointer to the bitmap data. */
  const optional<void*> bitmap_data_;

  /** Cell size. */
  const unsigned cell_size_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_AGGREGATE_BUFFER_H
