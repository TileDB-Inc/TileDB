/**
 * @file tiledb/api/c_api/buffer/buffer_api_internal.h
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
 * This file declares the internal buffer section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_BUFFER_API_INTERNAL_H
#define TILEDB_CAPI_BUFFER_API_INTERNAL_H

#include "../../c_api_support/handle/handle.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"

struct tiledb_buffer_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"buffer"};

 private:
  shared_ptr<tiledb::sm::MemoryTracker> memory_tracker_;
  tiledb::sm::SerializationBuffer buffer_;
  tiledb::sm::Datatype datatype_;

 public:
  explicit tiledb_buffer_handle_t(
      shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
      : memory_tracker_(std::move(memory_tracker))
      , buffer_(memory_tracker_->get_resource(
            tiledb::sm::MemoryType::SERIALIZATION_BUFFER))
      , datatype_(tiledb::sm::Datatype::UINT8) {
  }

  explicit tiledb_buffer_handle_t(
      size_t size, shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
      : memory_tracker_(std::move(memory_tracker))
      , buffer_(
            size,
            memory_tracker_->get_resource(
                tiledb::sm::MemoryType::SERIALIZATION_BUFFER))
      , datatype_(tiledb::sm::Datatype::UINT8) {
  }

  explicit tiledb_buffer_handle_t(
      const void* data,
      uint64_t size,
      shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
      : memory_tracker_(std::move(memory_tracker))
      , buffer_(memory_tracker_->get_resource(
            tiledb::sm::MemoryType::SERIALIZATION_BUFFER))
      , datatype_(tiledb::sm::Datatype::UINT8) {
    buffer_.assign(
        tiledb::sm::SerializationBuffer::NonOwned,
        span(static_cast<const char*>(data), size));
  }

  inline void set_datatype(tiledb::sm::Datatype datatype) {
    datatype_ = datatype;
  }

  inline tiledb::sm::Datatype datatype() const {
    return datatype_;
  }

  [[nodiscard]] inline tiledb::sm::SerializationBuffer& buffer() {
    return buffer_;
  }

  [[nodiscard]] inline const tiledb::sm::SerializationBuffer& buffer() const {
    return buffer_;
  }
};

namespace tiledb::api {

/**
 * Returns if the argument is a valid buffer: non-null, valid as a handle
 *
 * @param buffer A buffer of unknown validity
 */
inline void ensure_buffer_is_valid(const tiledb_buffer_handle_t* buffer) {
  ensure_handle_is_valid(buffer);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_BUFFER_API_INTERNAL_H
