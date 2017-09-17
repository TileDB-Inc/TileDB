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
#include "logger.h"

#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Buffer::Buffer() {
  data_ = nullptr;
  size_ = 0;
  offset_ = 0;
}

Buffer::Buffer(uint64_t size) {
  data_ = std::malloc(size);
  size_ = (data_ != nullptr) ? size : 0;
  offset_ = 0;
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

void Buffer::clear() {
  if (data_ != nullptr)
    std::free(data_);

  data_ = nullptr;
  offset_ = 0;
  size_ = 0;
}

void* Buffer::data() const {
  return data_;
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
  if (nbytes <= size_)
    return Status::Ok();

  if (data_ == nullptr)
    data_ = std::malloc(nbytes);
  else
    data_ = std::realloc(data_, nbytes);

  if (data_ == nullptr) {
    size_ = 0;
    return LOG_STATUS(Status::BufferError(
        "Cannot reallocate buffer; Memory allocation failed"));
  }

  size_ = nbytes;

  return Status::Ok();
}

void Buffer::reset_offset() {
  offset_ = 0;
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

void Buffer::write(ConstBuffer* buff) {
  uint64_t bytes_left_to_write = size_ - offset_;
  uint64_t bytes_left_to_read = buff->nbytes_left_to_read();
  uint64_t bytes_to_copy = std::min(bytes_left_to_write, bytes_left_to_read);

  buff->read(data_, bytes_to_copy);
  offset_ += bytes_to_copy;
}

Status Buffer::write(ConstBuffer* buff, uint64_t nbytes) {
  if (size_ == 0) {
    RETURN_NOT_OK(realloc(nbytes));
  } else {
    while (offset_ + nbytes > size_) {
      RETURN_NOT_OK(realloc(2 * size_));
    }
  }

  buff->read(data_, nbytes);
  offset_ += nbytes;

  return Status::Ok();
}

Status Buffer::write(const void* buffer, uint64_t nbytes) {
  if (size_ == 0) {
    RETURN_NOT_OK(realloc(nbytes));
  } else {
    while (offset_ + nbytes > size_) {
      RETURN_NOT_OK(realloc(2 * size_));
    }
  }

  std::memcpy((char*)data_ + offset_, buffer, nbytes);
  offset_ += nbytes;

  return Status::Ok();
}

void Buffer::write_with_shift(ConstBuffer* buff, uint64_t offset) {
  uint64_t bytes_left_to_write = size_ - offset_;
  uint64_t bytes_left_to_read = buff->nbytes_left_to_read();
  uint64_t bytes_to_copy = std::min(bytes_left_to_write, bytes_left_to_read);

  buff->read_with_shift(static_cast<uint64_t*>(data_), bytes_to_copy, offset);
  offset_ += bytes_to_copy;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb
