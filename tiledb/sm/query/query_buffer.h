/**
 * @file   query_buffer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2022 TileDB, Inc.
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

#include "tiledb/common/common.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/types/dynamic_typed_datum.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/dimension.h"
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
    original_buffer_size_ = (buffer_size != nullptr) ? *buffer_size : 0;
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
    original_buffer_size_ = (buffer_size_ != nullptr) ? *buffer_size_ : 0;
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
  QueryBuffer(QueryBuffer&& rhs) noexcept
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
  QueryBuffer& operator=(QueryBuffer&& rhs) noexcept {
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

  /* ********************************* */
  /*           PUBLIC METHODS         */
  /* ********************************* */

  void set_data_buffer(void* data_buffer, uint64_t* size) {
    buffer_ = data_buffer;
    buffer_size_ = size;
    original_buffer_size_ = *size;
  }

  void set_data_var_buffer(void* data_var_buffer, uint64_t* size) {
    buffer_var_ = data_var_buffer;
    buffer_var_size_ = size;
    original_buffer_var_size_ = *size;
  }

  void set_offsets_buffer(void* offsets_buffer, uint64_t* size) {
    buffer_ = offsets_buffer;
    buffer_size_ = size;
    original_buffer_size_ = *size;
  }

  void set_validity_buffer(ValidityVector&& validity_vector) {
    validity_vector_ = std::move(validity_vector);
    original_validity_vector_size_ = *validity_vector_.buffer_size();
  }

  /** Returns a const pointer to the data buffer as the requested type. */
  template <typename T>
  inline const T* data_buffer_as() const {
    if (buffer_var_) {
      return static_cast<T*>(buffer_var_);
    } else {
      return buffer_ ? static_cast<T*>(buffer_) : nullptr;
    }
  }

  /** Returns a const pointer to the offset buffer. */
  inline const uint64_t* offsets_buffer() const {
    return buffer_ ? static_cast<uint64_t*>(buffer_) : nullptr;
  }

  /** Checks if fixed length buffer is sorted. */
  template <
      typename T,
      typename Op,
      typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
  bool is_sorted() const {
    auto data = data_buffer_as<T>();
    uint64_t num_values = *buffer_size_ / sizeof(T);
    Op compare{};
    for (uint64_t index{0}; index < num_values - 1; ++index) {
      if (compare(data[index + 1], data[index])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the input buffer is sorted for variable length string data.
   */
  template <typename Op>
  bool is_sorted_str() const {
    // Set typed buffers and compute number of elements in them.
    auto offsets = offsets_buffer();
    auto data = data_buffer_as<char>();
    uint64_t num_offset_values = *buffer_size_ / sizeof(uint64_t);
    uint64_t last_offset_value = *buffer_var_size_ / sizeof(char);

    // Check the sort.
    Op compare{};
    for (uint64_t index{0}; index < num_offset_values - 1; ++index) {
      uint64_t i0 = offsets[index];
      uint64_t i1 = offsets[index + 1];
      uint64_t i2 = index + 2 < num_offset_values ? offsets[index + 2] :
                                                    last_offset_value;
      if (compare(
              std::string_view(&data[i1], i2 - i1),
              std::string_view(&data[i0], i1 - i0))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Treat this buffer as containing an array of data of fixed size `datum_size`
   * and retrieve the datum at array position `index`.
   *
   * @param datum_size sizeof(T), were this function strongly typed
   * @param index The index into an array of T
   * @return A pointer to the retrieved datum
   */
  tdb::UntypedDatumView fixed_size_datum_at(
      size_t index, size_t datum_size) const {
    return {static_cast<char*>(buffer_) + datum_size * index, datum_size};
  }

  /**
   * Treat this buffer as containing a sequence of data of varying sizes and
   * retrieve the datum at sequence position `index`.
   *
   * @param index The index into a sequence of data of varying sizes.
   * @return a view of the datum at the given index
   */
  tdb::UntypedDatumView varying_size_datum_at(size_t index) const {
    using buffer_type = uint64_t;
    auto* offsets = static_cast<buffer_type*>(buffer_);
    size_t start_offset = offsets[index];
    size_t next_offset = (*buffer_size_ > (index + 1) * sizeof(buffer_type)) ?
                             offsets[index + 1] :
                             *buffer_var_size_;
    return {
        static_cast<char*>(buffer_var_) + start_offset,
        next_offset - start_offset};
  }

  tdb::DynamicTypedDatumView dimension_datum_at(
      const Dimension& dim, size_t index) const {
    auto type = dim.type();
    return dim.var_size() ?
               tdb::DynamicTypedDatumView{varying_size_datum_at(index), type} :
               tdb::DynamicTypedDatumView{
                   fixed_size_datum_at(index, dim.coord_size()), type};
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_BUFFER_H
