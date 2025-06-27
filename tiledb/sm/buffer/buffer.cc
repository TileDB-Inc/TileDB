/**
 * @file   buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements classes BufferBase, Buffer, ConstBuffer, and
 * PreallocatedBuffer.
 */

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/common/assert.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"

#include <algorithm>
#include <iostream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {
/* ****************************** */
/*          BufferBase            */
/* ****************************** */

BufferBase::BufferBase()
    : data_(nullptr)
    , size_(0)
    , offset_(0) {};

BufferBase::BufferBase(void* data, const uint64_t size)
    : data_(data)
    , size_(size)
    , offset_(0) {};

BufferBase::BufferBase(const void* data, const uint64_t size)
    // const_cast is safe here because BufferBase methods do not modify storage
    : data_(const_cast<void*>(data))
    , size_(size)
    , offset_(0) {};

uint64_t BufferBase::size() const {
  return size_;
}

void* BufferBase::nonconst_data() const {
  return data_;
}

const void* BufferBase::data() const {
  // const_cast is safe here because BufferBase methods do not modify storage
  return const_cast<const void*>(data_);
}

void* BufferBase::nonconst_unread_data() const {
  if (data_ == nullptr) {
    return nullptr;
  }
  // Cast to byte type because offset is measured in bytes
  return static_cast<int8_t*>(data_) + offset_;
}

const void* BufferBase::cur_data() const {
  // const_cast is safe here because BufferBase methods do not modify storage
  return const_cast<const void*>(nonconst_unread_data());
}

uint64_t BufferBase::offset() const {
  return offset_;
}

void BufferBase::reset_offset() {
  offset_ = 0;
}

void BufferBase::set_offset(const uint64_t offset) {
  assert_offset_is_valid(offset);
  offset_ = offset;
}

void BufferBase::advance_offset(const uint64_t nbytes) {
  if (nbytes >= size_ - offset_) {
    // The argument puts us at the end or past it, which is still at the end.
    offset_ = size_;
  } else {
    offset_ += nbytes;
  }
}

bool BufferBase::end() const {
  return offset_ == size_;
}

Status BufferBase::read(void* destination, const uint64_t nbytes) {
  if (nbytes > size_ - offset_) {
    return LOG_STATUS(Status_BufferError(
        "Read buffer overflow; may not read beyond buffer size"));
  }
  std::memcpy(destination, static_cast<char*>(data_) + offset_, nbytes);
  offset_ += nbytes;
  return Status::Ok();
}

Status BufferBase::read(
    void* destination, const uint64_t offset, const uint64_t nbytes) {
  if (nbytes > size_ - offset) {
    return LOG_STATUS(Status_BufferError(
        "Read buffer overflow; may not read beyond buffer size"));
  }
  std::memcpy(destination, static_cast<char*>(data_) + offset, nbytes);
  return Status::Ok();
}

BufferBase::operator span<const char>() const& {
  return {static_cast<const char*>(data()), size_};
}

span<const char> BufferBase::cur_span() const& {
  return static_cast<span<const char>>(*this).subspan(offset_);
}

void BufferBase::assert_offset_is_valid(uint64_t offset) const {
  if (offset > size_) {
    throw std::out_of_range("BufferBase::set_offset");
  }
}

/* ****************************** */
/*            Buffer              */
/* ****************************** */

Buffer::Buffer()
    : BufferBase()
    , owns_data_(true)
    , alloced_size_(0) {
}

Buffer::Buffer(uint64_t size)
    : BufferBase((void*)nullptr, size)
    , owns_data_(true)
    , alloced_size_(0) {
  throw_if_not_ok(ensure_alloced_size(size_));
  preallocated_ = true;
  size_ = 0;
}

Buffer::Buffer(void* data, const uint64_t size)
    : BufferBase(data, size)
    , owns_data_(false)
    , alloced_size_(0) {
}

Buffer::Buffer(const Buffer& buff)
    : BufferBase(buff.data_, buff.size_) {
  offset_ = buff.offset_;
  owns_data_ = buff.owns_data_;
  alloced_size_ = buff.alloced_size_;
  preallocated_ = buff.preallocated_;

  if (buff.owns_data_ && buff.data_ != nullptr) {
    data_ = tdb_malloc(alloced_size_);
    passert(data_);
    std::memcpy(data_, buff.data_, buff.alloced_size_);
  }
}

Buffer::Buffer(Buffer&& buff) noexcept
    : Buffer() {
  swap(buff);
}

Buffer::~Buffer() {
  clear();
}

void* Buffer::data() const {
  return nonconst_data();
}

void Buffer::advance_size(const uint64_t nbytes) {
  iassert(owns_data_);
  size_ += nbytes;
}

uint64_t Buffer::alloced_size() const {
  return alloced_size_;
}

void Buffer::clear() {
  if (data_ != nullptr && owns_data_)
    tdb_free(data_);

  data_ = nullptr;
  offset_ = 0;
  size_ = 0;
  alloced_size_ = 0;
}

void* Buffer::cur_data() const {
  return nonconst_unread_data();
}

void* Buffer::data(const uint64_t offset) const {
  auto data = static_cast<char*>(nonconst_data());
  if (data == nullptr) {
    return nullptr;
  }
  return data + offset;
}

uint64_t Buffer::free_space() const {
  iassert(alloced_size_ >= size_);
  return alloced_size_ - size_;
}

bool Buffer::owns_data() const {
  return owns_data_;
}

Status Buffer::realloc(const uint64_t nbytes) {
  if (!owns_data_) {
    return LOG_STATUS(Status_BufferError(
        "Cannot reallocate buffer; Buffer does not own data"));
  }

  if (data_ == nullptr) {
    data_ = tdb_malloc(nbytes);
    if (data_ == nullptr) {
      return LOG_STATUS(Status_BufferError(
          "Cannot allocate buffer; Memory allocation failed"));
    }
    alloced_size_ = nbytes;
  } else if (nbytes > alloced_size_) {
    auto new_data = tdb_realloc(data_, nbytes);
    if (new_data == nullptr) {
      return LOG_STATUS(Status_BufferError(
          "Cannot reallocate buffer; Memory allocation failed"));
    }
    data_ = new_data;
    alloced_size_ = nbytes;
  }

  return Status::Ok();
}

void Buffer::reset_size() {
  offset_ = 0;
  size_ = 0;
}

void Buffer::set_size(const uint64_t size) {
  size_ = size;
}

void Buffer::swap(Buffer& other) {
  std::swap(alloced_size_, other.alloced_size_);
  std::swap(data_, other.data_);
  std::swap(offset_, other.offset_);
  std::swap(owns_data_, other.owns_data_);
  std::swap(size_, other.size_);
  std::swap(preallocated_, other.preallocated_);
}

Status Buffer::write(ConstBuffer* buff) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status_BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  const uint64_t bytes_left_to_write = alloced_size_ - offset_;
  const uint64_t bytes_left_to_read = buff->nbytes_left_to_read();
  const uint64_t bytes_to_copy =
      std::min(bytes_left_to_write, bytes_left_to_read);

  RETURN_NOT_OK(buff->read((char*)data_ + offset_, bytes_to_copy));
  offset_ += bytes_to_copy;
  size_ = std::max(offset_, size_);

  return Status::Ok();
}

Status Buffer::write(ConstBuffer* buff, const uint64_t nbytes) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status_BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  RETURN_NOT_OK(ensure_alloced_size(offset_ + nbytes));

  RETURN_NOT_OK(buff->read((char*)data_ + offset_, nbytes));
  offset_ += nbytes;
  size_ = std::max(offset_, size_);

  return Status::Ok();
}

Status Buffer::write(const void* buffer, const uint64_t nbytes) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status_BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  RETURN_NOT_OK(ensure_alloced_size(offset_ + nbytes));

  std::memcpy((char*)data_ + offset_, buffer, nbytes);
  offset_ += nbytes;
  size_ = std::max(offset_, size_);

  return Status::Ok();
}

Status Buffer::write(
    const void* buffer, const uint64_t offset, const uint64_t nbytes) {
  // Sanity check
  if (!owns_data_)
    return LOG_STATUS(Status_BufferError(
        "Cannot write to buffer; Buffer does not own the already stored data"));

  RETURN_NOT_OK(ensure_alloced_size(offset + nbytes));

  std::memcpy((char*)data_ + offset, buffer, nbytes);
  size_ = std::max(offset + nbytes, size_);

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
  if (preallocated_ && nbytes > alloced_size_) {
    throw BufferStatusException(
        "Failed to reallocate. Buffer is preallocated to a fixed size.");
  } else if (preallocated_ || alloced_size_ >= nbytes) {
    return Status::Ok();
  }

  auto new_alloc_size = alloced_size_ == 0 ? nbytes : alloced_size_;
  while (new_alloc_size < nbytes)
    new_alloc_size *= 2;

  return this->realloc(new_alloc_size);
}

/* ****************************** */
/*          ConstBuffer           */
/* ****************************** */

ConstBuffer::ConstBuffer(Buffer* buff)
    : ConstBuffer(buff->data(), buff->size()) {
}

ConstBuffer::ConstBuffer(const void* data, const uint64_t size)
    : BufferBase(data, size) {
}

uint64_t ConstBuffer::nbytes_left_to_read() const {
  return size_ - offset_;
}

/* ****************************** */
/*       PreallocatedBuffer       */
/* ****************************** */

PreallocatedBuffer::PreallocatedBuffer(const void* data, const uint64_t size)
    : BufferBase(data, size) {
}

void* PreallocatedBuffer::cur_data() const {
  return nonconst_unread_data();
}

uint64_t PreallocatedBuffer::free_space() const {
  return size_ - offset_;
}

Status PreallocatedBuffer::write(const void* buffer, const uint64_t nbytes) {
  if (nbytes > size_ - offset_)
    return Status_PreallocatedBufferError("Write would overflow buffer.");

  std::memcpy((char*)data_ + offset_, buffer, nbytes);
  offset_ += nbytes;

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
