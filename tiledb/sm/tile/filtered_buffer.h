/**
 * @file   filtered_buffer.h
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
 * This file defines class FilteredBuffer.
 */

#ifndef TILEDB_FILTERED_BUFFER_H
#define TILEDB_FILTERED_BUFFER_H

#include <vector>

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Handles filtered buffer information.
 */
class FilteredBuffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  FilteredBuffer(uint64_t size) {
    if (size != 0) {
      filtered_buffer_.resize(size);
    }
  }

  /**
   * Copy constructor.
   *
   * Only used in the tile cache. Should be removed when the tile cache is
   * removed.
   */
  FilteredBuffer(const FilteredBuffer& other) {
    filtered_buffer_ = other.filtered_buffer_;
  }

  /** Move constructor. */
  FilteredBuffer(FilteredBuffer&& other) {
    // Swap with the argument
    swap(other);
  }

  /** Move-assign operator. */
  FilteredBuffer& operator=(FilteredBuffer&& other) {
    // Swap with the argument
    swap(other);

    return *this;
  }

  DISABLE_COPY_ASSIGN(FilteredBuffer);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the size. */
  inline size_t size() const {
    return filtered_buffer_.size();
  }

  /** Returns the data. */
  inline uint8_t* data() {
    return filtered_buffer_.data();
  }

  /** Returns the data. */
  inline const uint8_t* data() const {
    return filtered_buffer_.data();
  }

  /** Expands the size of the underlying container. */
  inline void expand(size_t size) {
    assert(size >= filtered_buffer_.size());
    filtered_buffer_.resize(size);
  }

  /** Clears the data. */
  inline void clear() {
    filtered_buffer_.clear();
  }

  /**
   * Swaps the contents (all field values) of this filtered buffer with the
   * given filtered buffer.
   */
  void swap(FilteredBuffer& other) {
    std::swap(filtered_buffer_, other.filtered_buffer_);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Storing container for the filtered buffer. */
  std::vector<uint8_t> filtered_buffer_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTERED_BUFFER_H
