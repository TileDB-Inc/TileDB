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

#include <cstring>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ConstBuffer::ConstBuffer(const void* data, uint64_t size)
    : data_(data)
    , size_(size) {
  offset_ = 0;
}

/* ****************************** */
/*               API              */
/* ****************************** */

void ConstBuffer::read(void* buffer, uint64_t nbytes) {
  memcpy(buffer, (char*)data_ + offset_, nbytes);
  offset_ += nbytes;
}

void ConstBuffer::read_with_shift(
    uint64_t* buffer, uint64_t nbytes, uint64_t offset) {
  // For easy reference
  uint64_t buffer_cell_num = nbytes / sizeof(uint64_t);
  const void* data_c = static_cast<const char*>(data_) + offset_;
  auto data = static_cast<const uint64_t*>(data_c);

  // Write shifted offsets
  for (uint64_t i = 0; i < buffer_cell_num; ++i)
    buffer[i] = offset + data[i];

  offset_ += nbytes;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb
