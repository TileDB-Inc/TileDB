/**
 * @file   buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/misc/logger.h"

#include <iostream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MAX(a, b) ((a) > (b) ? (a) : (b))

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Buffer::Buffer()
    : Buffer(nullptr, 0) {
  owns_data_ = true;
}

Buffer::Buffer(void* data, const uint64_t size) {
  offset_ = 0;
  alloced_size_ = 0;
  owns_data_ = false;
  data_ = data;
  size_ = size;
}

Buffer::Buffer(const Buffer& buff) {
  alloced_size_ = buff.alloced_size_;
  offset_ = buff.offset_;
  owns_data_ = buff.owns_data_;
  size_ = buff.size_;

  if (buff.owns_data_) {
    if (buff.data_ == nullptr) {
      data_ = nullptr;
    } else {
      data_ = std::malloc(alloced_size_);
      assert(data_);
      std::memcpy(data_, buff.data_, buff.alloced_size_);
    }
  } else {
    data_ = buff.data_;
  }
}

Buffer::Buffer(Buffer&& buff)
    : Buffer() {
  swap(buff);
}

Buffer::~Buffer() {
  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

void Buffer::advance_offset(const uint64_t nbytes) {
  offset_ += nbytes;
}

void Buffer::advance_size(const uint64_t nbytes) {
  assert(owns_data_);
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
  if (data_ == nullptr)
    return nullptr;
  return (char*)data_ + offset_;
}

void* Buffer::data() const {
  return data_;
}

void* Buffer::data(const uint64_t offset) const {
  if (data_ == nullptr)
    return nullptr;
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

bool Buffer::owns_data() const {
  return owns_data_;
}

Status Buffer::read(void* buffer, const uint64_t nbytes) {
  if (nbytes + offset_ > size_) {
    return LOG_STATUS(
        Status::BufferError("Read failed; Trying to read beyond buffer size"));
  }
  std::memcpy(buffer, (char*)data_ + offset_, nbytes);
  offset_ += nbytes;
  return Status::Ok();
}

Status Buffer::realloc(const uint64_t nbytes) {
  if (!owns_data_) {
    return LOG_STATUS(Status::BufferError(
        "Cannot reallocate buffer; Buffer does not own data"));
  }

  if (data_ == nullptr) {
    data_ = std::malloc(nbytes);
    if (data_ == nullptr) {
      return LOG_STATUS(Status::BufferError(
          "Cannot allocate buffer; Memory allocation failed"));
    }
    alloced_size_ = nbytes;
  } else if (nbytes > alloced_size_) {
    auto new_data = std::realloc(data_, nbytes);
    if (new_data == nullptr) {
      return LOG_STATUS(Status::BufferError(
          "Cannot reallocate buffer; Memory allocation failed"));
    }
    data_ = new_data;
    alloced_size_ = nbytes;
  }

  return Status::Ok();
}

void Buffer::reset_offset() {
  offset_ = 0;
}

void Buffer::reset_size() {
  offset_ = 0;
  size_ = 0;
}

void Buffer::set_offset(const uint64_t offset) {
  offset_ = offset;
}

void Buffer::set_size(const uint64_t size) {
  size_ = size;
}

uint64_t Buffer::size() const {
  return size_;
}

Status Buffer::swap(Buffer& other) {
  std::swap(alloced_size_, other.alloced_size_);
  std::swap(data_, other.data_);
  std::swap(offset_, other.offset_);
  std::swap(owns_data_, other.owns_data_);
  std::swap(size_, other.size_);
  return Status::Ok();
}

Status Buffer::write(ConstBuffer* buff) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status::BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  const uint64_t bytes_left_to_write = alloced_size_ - offset_;
  const uint64_t bytes_left_to_read = buff->nbytes_left_to_read();
  const uint64_t bytes_to_copy =
      std::min(bytes_left_to_write, bytes_left_to_read);

  buff->read((char*)data_ + offset_, bytes_to_copy);
  offset_ += bytes_to_copy;
  size_ = offset_;

  return Status::Ok();
}

Status Buffer::write(ConstBuffer* buff, const uint64_t nbytes) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status::BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  RETURN_NOT_OK(ensure_alloced_size(offset_ + nbytes));

  buff->read((char*)data_ + offset_, nbytes);
  offset_ += nbytes;
  size_ = offset_;

  return Status::Ok();
}

Status Buffer::write(const void* buffer, const uint64_t nbytes) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status::BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  RETURN_NOT_OK(ensure_alloced_size(offset_ + nbytes));

  std::memcpy((char*)data_ + offset_, buffer, nbytes);
  offset_ += nbytes;
  size_ = offset_;

  return Status::Ok();
}

Status Buffer::write_with_shift(ConstBuffer* buff, const uint64_t offset) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status::BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  const uint64_t bytes_left_to_write = alloced_size_ - offset_;
  const uint64_t bytes_left_to_read = buff->nbytes_left_to_read();
  const uint64_t bytes_to_copy =
      std::min(bytes_left_to_write, bytes_left_to_read);
  buff->read_with_shift(
      (uint64_t*)(((char*)data_ + offset_)), bytes_to_copy, offset);

  offset_ += bytes_to_copy;
  size_ = offset_;

  return Status::Ok();
}

Buffer& Buffer::operator=(const Buffer& buff) {
  // Clear any existing allocation.
  clear();

  // Create a copy and swap with the copy.
  Buffer tmp(buff);
  swap(tmp);

  return *this;
}

Buffer& Buffer::operator=(Buffer&& buff) {
  swap(buff);
  return *this;
}

Status Buffer::ensure_alloced_size(const uint64_t nbytes) {
  if (alloced_size_ >= nbytes)
    return Status::Ok();

  auto new_alloc_size = alloced_size_ == 0 ? nbytes : alloced_size_;
  while (new_alloc_size < nbytes)
    new_alloc_size *= 2;

  return realloc(new_alloc_size);
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace sm
}  // namespace tiledb
