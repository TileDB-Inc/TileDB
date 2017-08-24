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
#include "../../include/vfs/filesystem.h"
#include "logger.h"

#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Buffer::Buffer() {
  data_ = nullptr;
  size_ = 0;
  mmap_data_ = nullptr;
  mmap_size_ = 0;
  offset_ = 0;
}

Buffer::Buffer(uint64_t size) {
  data_ = std::malloc(size);
  mmap_data_ = nullptr;
  mmap_size_ = 0;
  size_ = (data_ != nullptr) ? size : 0;
  offset_ = 0;
}

Buffer::~Buffer() {
  Status st = clear();
  if (!st.ok())
    LOG_STATUS(st);
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status Buffer::clear() {
  offset_ = 0;

  if (data_ != nullptr) {
    if (mmap_data_ != nullptr) {
      return munmap();
    } else {
      free(data_);
      data_ = nullptr;
      size_ = 0;
    }
  }

  return Status::Ok();
}

Status Buffer::mmap(
    const uri::URI& filename, uint64_t size, uint64_t offset, bool read_only) {
  // Clean buffer
  RETURN_NOT_OK(clear());

  // Alignment and sizes
  size_t page_size = sysconf(_SC_PAGE_SIZE);
  off_t start_offset = (offset / page_size) * page_size;
  size_t extra_offset = offset - start_offset;
  size_ = size;
  mmap_size_ = size + extra_offset;

  RETURN_NOT_OK(
      vfs::mmap(filename, mmap_size_, start_offset, &mmap_data_, read_only));

  data_ = static_cast<char*>(mmap_data_) + extra_offset;

  return Status::Ok();
}

Status Buffer::munmap() {
  if (mmap_data_ == nullptr)
    return Status::Ok();

  RETURN_NOT_OK(vfs::munmap(mmap_data_, mmap_size_));

  mmap_data_ = nullptr;
  mmap_size_ = 0;
  data_ = nullptr;
  size_ = 0;

  return Status::Ok();
}

Status Buffer::read(void* buffer, uint64_t bytes) {
  if (bytes + offset_ > size_) {
    return LOG_STATUS(
        Status::BufferError("Read failed; Trying to read beyond buffer size"));
  }
  std::memcpy(buffer, (char*)data_ + offset_, bytes);
  offset_ += bytes;
  return Status::Ok();
}

Status Buffer::realloc(uint64_t size) {
  if (size <= size_) {
    return Status::Ok();
  }
  if (data_ == nullptr) {
    data_ = std::malloc(size);
  } else {
    data_ = std::realloc(data_, size);
  }
  if (data_ == nullptr) {
    size_ = 0;
    return LOG_STATUS(Status::BufferError(
        "Cannot reallocate buffer; Memory allocation failed"));
  }
  size_ = size;
  return Status::Ok();
}

void Buffer::write(ConstBuffer* buf) {
  uint64_t bytes_left_to_write = size_ - offset_;
  uint64_t bytes_left_to_read = buf->nbytes_left_to_read();
  uint64_t bytes_to_copy = std::min(bytes_left_to_write, bytes_left_to_read);

  buf->read(data_, bytes_to_copy);
  offset_ += bytes_to_copy;
}

Status Buffer::write(ConstBuffer* buf, uint64_t nbytes) {
  if (size_ == 0) {
    RETURN_NOT_OK(realloc(nbytes));
  } else {
    while (offset_ + nbytes > size_) {
      RETURN_NOT_OK(realloc(2 * size_));
    }
  }
  buf->read(data_, nbytes);
  offset_ += nbytes;
  return Status::Ok();
}

Status Buffer::write(const void* buf, uint64_t nbytes) {
  if (size_ == 0) {
    RETURN_NOT_OK(realloc(nbytes));
  } else {
    while (offset_ + nbytes > size_) {
      RETURN_NOT_OK(realloc(2 * size_));
    }
  }
  std::memcpy((char*)data_ + offset_, buf, nbytes);
  offset_ += nbytes;
  return Status::Ok();
}

void Buffer::write_with_shift(ConstBuffer* buf, uint64_t offset) {
  uint64_t bytes_left_to_write = size_ - offset_;
  uint64_t bytes_left_to_read = buf->nbytes_left_to_read();
  uint64_t bytes_to_copy = std::min(bytes_left_to_write, bytes_left_to_read);

  buf->read_with_shift(static_cast<uint64_t*>(data_), bytes_to_copy, offset);
  offset_ += bytes_to_copy;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb
