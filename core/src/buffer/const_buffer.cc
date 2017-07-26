/**
 * @file   const_buffer.cc
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
 * This file implements class ConstBuffer.
 */

#include "const_buffer.h"

#include <algorithm>
#include <cstring>
#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ConstBuffer::ConstBuffer(const void* buffer, uint64_t buffer_size)
    : buffer_(buffer)
    , buffer_size_(buffer_size) {
  buffer_offset_ = 0;
}

ConstBuffer::~ConstBuffer() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

bool ConstBuffer::end() const {
  return buffer_offset_ == buffer_size_;
}

uint64_t ConstBuffer::read(void* buffer, uint64_t buffer_size) {
  uint64_t remaining_bytes = buffer_size_ - buffer_offset_;
  uint64_t bytes_to_copy = std::min(remaining_bytes, buffer_size);
  memcpy(buffer, (char*)buffer_ + buffer_offset_, bytes_to_copy);
  buffer_offset_ += bytes_to_copy;

  return bytes_to_copy;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb
