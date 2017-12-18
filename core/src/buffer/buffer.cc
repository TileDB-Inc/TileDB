/**
 * @file   buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements class Buffer.
 */

#include "buffer.h"
#include "const_buffer.h"
#include "logger.h"

#include <iostream>

namespace tiledb {

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MAX(a, b) ((a) > (b) ? (a) : (b))

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Buffer::Buffer() {
  alloced_size_ = 0;
  data_ = nullptr;
  size_ = 0;
  offset_ = 0;
  owns_data_ = true;
}

Buffer::Buffer(void* data, uint64_t size, bool owns_data)
    : data_(data)
    , owns_data_(owns_data)
    , size_(size) {
  offset_ = 0;
  alloced_size_ = 0;
  owns_data_ = false;
}

Buffer::~Buffer() {
  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

void Buffer::advance_offset(uint64_t nbytes) {
  offset_ += nbytes;
}

void Buffer::advance_size(uint64_t nbytes) {
  size_ += nbytes;
}

uint64_t Buffer::alloced_size() const {
  return alloced_size_;
}

void Buffer::clear() {
  if (data_ != nullptr && owns_data_)
    std::free(data_);

  data_ = nullptr;
  offset_ = 0;
  size_ = 0;
  alloced_size_ = 0;
}

void* Buffer::cur_data() const {
  return (char*)data_ + offset_;
}

void* Buffer::data() const {
  return data_;
}

void* Buffer::data(uint64_t offset) const {
  return (char*)data_ + offset;
}

void Buffer::disown_data() {
  owns_data_ = false;
}

uint64_t Buffer::free_space() const {
  assert(alloced_size_ >= size_);
  return alloced_size_ - size_;
}

uint64_t Buffer::offset() const {
  return offset_;
}

Status Buffer::read(void* buffer, uint64_t nbytes) {
  if (nbytes + offset_ > size_) {
    return LOG_STATUS(
        Status::BufferError("Read failed; Trying to read beyond buffer size"));
  }
  std::memcpy(buffer, (char*)data_ + offset_, nbytes);
  offset_ += nbytes;
  return Status::Ok();
}

Status Buffer::realloc(uint64_t nbytes) {
  if (!owns_data_) {
    return LOG_STATUS(Status::BufferError(
        "Cannot reallocate buffer; Buffer does not own data"));
  }

  if (data_ == nullptr) {
    data_ = std::malloc(nbytes);
  } else {
    if (nbytes > alloced_size_)
      data_ = std::realloc(data_, nbytes);
  }

  if (data_ == nullptr) {
    alloced_size_ = 0;
    return LOG_STATUS(Status::BufferError(
        "Cannot reallocate buffer; Memory allocation failed"));
  }

  alloced_size_ = nbytes;

  return Status::Ok();
}

void Buffer::reset_offset() {
  offset_ = 0;
}

void Buffer::reset_size() {
  offset_ = 0;
  size_ = 0;
}

void Buffer::set_offset(uint64_t offset) {
  offset_ = offset;
}

void Buffer::set_size(uint64_t size) {
  size_ = size;
}

uint64_t Buffer::size() const {
  return size_;
}

Status Buffer::write(ConstBuffer* buff) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status::BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  uint64_t bytes_left_to_write = alloced_size_ - offset_;
  uint64_t bytes_left_to_read = buff->nbytes_left_to_read();
  uint64_t bytes_to_copy = std::min(bytes_left_to_write, bytes_left_to_read);

  buff->read((char*)data_ + offset_, bytes_to_copy);
  offset_ += bytes_to_copy;
  size_ = offset_;

  return Status::Ok();
}

Status Buffer::write(ConstBuffer* buff, uint64_t nbytes) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status::BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  while (offset_ + nbytes > alloced_size_)
    RETURN_NOT_OK(realloc(MAX(nbytes, 2 * alloced_size_)));

  buff->read((char*)data_ + offset_, nbytes);
  offset_ += nbytes;
  size_ = offset_;

  return Status::Ok();
}

Status Buffer::write(const void* buffer, uint64_t nbytes) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status::BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  while (offset_ + nbytes > alloced_size_)
    RETURN_NOT_OK(realloc(MAX(nbytes, 2 * alloced_size_)));

  std::memcpy((char*)data_ + offset_, buffer, nbytes);
  offset_ += nbytes;
  size_ = offset_;

  return Status::Ok();
}

Status Buffer::write_with_shift(ConstBuffer* buff, uint64_t offset) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status::BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  uint64_t bytes_left_to_write = alloced_size_ - offset_;
  uint64_t bytes_left_to_read = buff->nbytes_left_to_read();
  uint64_t bytes_to_copy = std::min(bytes_left_to_write, bytes_left_to_read);

  buff->read_with_shift(static_cast<uint64_t*>(data_), bytes_to_copy, offset);
  offset_ += bytes_to_copy;
  size_ = offset_;

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb
