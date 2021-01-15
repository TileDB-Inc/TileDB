/**
 * @file   query_buffer.h
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
 * This file defines class QueryBuffer.
 */

#ifndef TILEDB_QUERY_BUFFER_H
#define TILEDB_QUERY_BUFFER_H

#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/macros.h"
#include "tiledb/sm/query/validity_vector.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Contains the buffer(s) and buffer size(s) for some attribute/dimension. */
class QueryBuffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  QueryBuffer()
      : buffer_(nullptr)
      , buffer_var_(nullptr)
      , buffer_size_(nullptr)
      , buffer_var_size_(nullptr)
      , original_buffer_size_(0)
      , original_buffer_var_size_(0)
      , original_validity_vector_size_(0) {
  }

  /** Value Constructor. */
  QueryBuffer(
      void* const buffer,
      void* const buffer_var,
      uint64_t* const buffer_size,
      uint64_t* const buffer_var_size)
      : buffer_(buffer)
      , buffer_var_(buffer_var)
      , buffer_size_(buffer_size)
      , buffer_var_size_(buffer_var_size) {
    original_buffer_size_ = *buffer_size;
    original_buffer_var_size_ =
        (buffer_var_size_ != nullptr) ? *buffer_var_size_ : 0;
    original_validity_vector_size_ = 0;
  }

  /** Value Constructor. */
  QueryBuffer(
      void* const buffer,
      void* const buffer_var,
      uint64_t* const buffer_size,
      uint64_t* const buffer_var_size,
      ValidityVector&& validity_vector)
      : buffer_(buffer)
      , buffer_var_(buffer_var)
      , buffer_size_(buffer_size)
      , buffer_var_size_(buffer_var_size)
      , validity_vector_(std::move(validity_vector)) {
    original_buffer_size_ = *buffer_size;
    original_buffer_var_size_ =
        (buffer_var_size_ != nullptr) ? *buffer_var_size_ : 0;
    original_validity_vector_size_ =
        (validity_vector_.buffer_size() != nullptr) ?
            *validity_vector_.buffer_size() :
            0;
  }

  /** Destructor. */
  ~QueryBuffer() = default;

  /** Copy constructor. */
  QueryBuffer(const QueryBuffer& rhs)
      : buffer_(rhs.buffer_)
      , buffer_var_(rhs.buffer_var_)
      , buffer_size_(rhs.buffer_size_)
      , buffer_var_size_(rhs.buffer_var_size_)
      , original_buffer_size_(rhs.original_buffer_size_)
      , original_buffer_var_size_(rhs.original_buffer_var_size_)
      , original_validity_vector_size_(rhs.original_validity_vector_size_)
      , validity_vector_(rhs.validity_vector_) {
  }

  /** Move constructor. */
  QueryBuffer(QueryBuffer&& rhs)
      : buffer_(rhs.buffer_)
      , buffer_var_(rhs.buffer_var_)
      , buffer_size_(rhs.buffer_size_)
      , buffer_var_size_(rhs.buffer_var_size_)
      , original_buffer_size_(rhs.original_buffer_size_)
      , original_buffer_var_size_(rhs.original_buffer_var_size_)
      , original_validity_vector_size_(rhs.original_validity_vector_size_)
      , validity_vector_(std::move(rhs.validity_vector_)) {
  }

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Move-assignment Operator. */
  QueryBuffer& operator=(QueryBuffer&& rhs) {
    if (&rhs == this)
      return *this;

    std::swap(buffer_, rhs.buffer_);
    std::swap(buffer_var_, rhs.buffer_var_);
    std::swap(buffer_size_, rhs.buffer_size_);
    std::swap(buffer_var_size_, rhs.buffer_var_size_);
    std::swap(original_buffer_size_, rhs.original_buffer_size_);
    std::swap(original_buffer_var_size_, rhs.original_buffer_var_size_);
    std::swap(
        original_validity_vector_size_, rhs.original_validity_vector_size_);
    validity_vector_ = std::move(rhs.validity_vector_);

    return *this;
  }

  DISABLE_COPY_ASSIGN(QueryBuffer);

  /* ********************************* */
  /*          PUBLIC ATTRIBUTES        */
  /* ********************************* */

  /**
   * The attribute/dimension buffer. In case the attribute/dimension is
   * var-sized, this is the offsets buffer.
   */
  void* buffer_;

  /**
   * For a var-sized attribute/dimension, this is the data buffer. It is
   * `nullptr` for fixed-sized attributes/dimensions.
   */
  void* buffer_var_;

  /**
   * The size (in bytes) of `buffer_`. Note that this size may be altered by
   * a read query to reflect the useful data written in the buffer.
   */
  uint64_t* buffer_size_;

  /**
   * The size (in bytes) of `buffer_var_`. Note that this size may be altered
   * by a read query to reflect the useful data written in the buffer.
   */
  uint64_t* buffer_var_size_;

  /**
   * This is the original size (in bytes) of `buffer_` (before
   * potentially altered by the query).
   */
  uint64_t original_buffer_size_;

  /**
   * This is the original size (in bytes) of `buffer_var_` (before
   * potentially altered by the query).
   */
  uint64_t original_buffer_var_size_;

  /**
   * This is the original size (in bytes) of `validity_vector_.buffer()` (before
   * potentially altered by the query).
   */
  uint64_t original_validity_vector_size_;

  /**
   * The validity vector, which wraps a uint8_t* bytemap buffer and a
   * uint64_t* bytemap buffer size. These will be null for non-nullable
   * attributes.
   */
  ValidityVector validity_vector_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_BUFFER_H
