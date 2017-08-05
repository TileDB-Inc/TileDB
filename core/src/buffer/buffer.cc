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

#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Buffer::Buffer() {
  data_ = nullptr;
  size_ = 0;
  size_alloced_ = 0;
  mmap_data_ = nullptr;
  mmap_size_ = 0;
  offset_ = 0;
}

Buffer::Buffer(uint64_t size) {
  data_ = malloc(size);
  mmap_data_ = nullptr;
  mmap_size_ = 0;
  size_ = (data_ != nullptr) ? size : 0;
  size_alloced_ = size_;
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
    if (!mmap_data_) {
      free(data_);
      data_ = nullptr;
      size_ = 0;
      size_alloced_ = 0;
    } else {
      return munmap();
    }
  }

  return Status::Ok();
}

Status Buffer::mmap(int fd, uint64_t size, uint64_t offset, bool read_only) {
  // Clean buffer
  RETURN_NOT_OK(clear());

  // Alignment and sizes
  size_t page_size = sysconf(_SC_PAGE_SIZE);
  off_t start_offset = (offset / page_size) * page_size;
  size_t extra_offset = offset - start_offset;
  size_ = size;
  mmap_size_ = size + extra_offset;

  // MMap flags
  int prot = read_only ? PROT_READ : (PROT_READ | PROT_WRITE);
  int flags = read_only ? MAP_SHARED : MAP_PRIVATE;

  // Map
  // TODO: perhaps move to filesystem (along with open and fd)?
  mmap_data_ = ::mmap(mmap_data_, mmap_size_, prot, flags, fd, start_offset);
  if (mmap_data_ == MAP_FAILED) {
    size_ = 0;
    mmap_size_ = 0;
    return LOG_STATUS(Status::BufferError("Memory map failed"));
  }

  data_ = static_cast<char*>(mmap_data_) + extra_offset;

  return Status::Ok();
}

Status Buffer::munmap() {
  if (mmap_data_ == nullptr)
    return Status::Ok();

  // TODO: perhaps move to filesystem?
  if (::munmap(mmap_data_, mmap_size_))
    return LOG_STATUS(Status::BufferError("Memory unmap failed"));

  mmap_data_ = nullptr;
  mmap_size_ = 0;
  data_ = nullptr;
  size_ = 0;

  return Status::Ok();
}

Status Buffer::read(void* buffer, uint64_t bytes) {
  if (bytes + offset_ > size_)
    return LOG_STATUS(
        Status::BufferError("Read failed; Trying to read beyond buffer size"));

  memcpy(buffer, (char*)data_ + offset_, bytes);
  offset_ += bytes;

  return Status::Ok();
}

// TODO: this may fail - return Status
void Buffer::realloc(uint64_t size) {
  data_ = ::realloc(data_, size);
  size_alloced_ = size;
}

void Buffer::write(ConstBuffer* buf) {
  uint64_t bytes_left_to_write = size_ - offset_;
  uint64_t bytes_left_to_read = buf->bytes_left_to_read();
  uint64_t bytes_to_copy = std::min(bytes_left_to_write, bytes_left_to_read);

  buf->read(data_, bytes_to_copy);
  offset_ += bytes_to_copy;
}

// TODO: this may fail - return Status
void Buffer::write(ConstBuffer* buf, uint64_t bytes) {
  while (offset_ + bytes > size_alloced_)
    realloc(2 * size_alloced_);

  buf->read(data_, bytes);
  offset_ += bytes;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb
