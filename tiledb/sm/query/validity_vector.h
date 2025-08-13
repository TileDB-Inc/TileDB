/**
 * @file   validity_vector.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
 * This file defines class ValidityVector.
 */

#ifndef TILEDB_VALIDITY_VECTOR_H
#define TILEDB_VALIDITY_VECTOR_H

#include <cstdint>
#include <utility>
#include <vector>

#include "tiledb/common/macros.h"

namespace tiledb {
namespace sm {

class ValidityVector {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  ValidityVector()
      : buffer_(nullptr)
      , buffer_size_(nullptr) {
  }

  /** Constructor. */
  ValidityVector(uint8_t* buffer, uint64_t* buffer_size)
      : buffer_(buffer)
      , buffer_size_(buffer_size) {
  }

  /** Copy constructor. */
  ValidityVector(const ValidityVector& rhs)
      : buffer_(rhs.buffer_)
      , buffer_size_(rhs.buffer_size_) {
  }

  /** Move constructor. */
  ValidityVector(ValidityVector&& rhs)
      : buffer_(std::exchange(rhs.buffer_, nullptr))
      , buffer_size_(std::exchange(rhs.buffer_size_, nullptr)) {
  }

  /** Destructor. */
  ~ValidityVector() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Move-assignment operator. */
  ValidityVector& operator=(ValidityVector&& rhs) {
    if (&rhs == this)
      return *this;

    std::swap(buffer_, rhs.buffer_);
    std::swap(buffer_size_, rhs.buffer_size_);

    return *this;
  }

  DISABLE_COPY_ASSIGN(ValidityVector);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the bytemap that this instance was initialized with. */
  uint8_t* bytemap() const {
    return buffer_;
  }

  /**
   * Returns the size of the bytemap that this instance was initialized
   * with.
   */
  uint64_t* bytemap_size() const {
    return buffer_size_;
  }

  /**
   * Returns the internal buffer. This is currently a byte map, but
   * will change to a bitmap in the future.
   *
   * @return a pointer to the internal buffer.
   */
  uint8_t* buffer() const {
    return buffer_;
  }

  /**
   * Returns the size of the internal buffer.
   *
   * @return a pointer to the internal buffer size.
   */
  uint64_t* buffer_size() const {
    return buffer_size_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * Contains a byte-map, where each non-zero byte represents
   * a valid (non-null) attribute value and a zero byte represents
   * a null (non-valid) attribute value.
   */
  uint8_t* buffer_;

  /** The size of `buffer_size_`. */
  uint64_t* buffer_size_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_VALIDITY_VECTOR_H
