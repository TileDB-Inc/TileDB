/**
 * @file   preallocated_buffer.cc
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
 * This file implements class PreallocatedBuffer.
 */

#include "tiledb/sm/buffer/preallocated_buffer.h"
#include "tiledb/common/logger.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

PreallocatedBuffer::PreallocatedBuffer(const void* data, const uint64_t size)
    : BufferBase(data, size) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

void* PreallocatedBuffer::cur_data() const {
  return nonconst_unread_data();
}

const void* PreallocatedBuffer::data() const {
  return const_data();
}

uint64_t PreallocatedBuffer::free_space() const {
  return size_ - offset_;
}

Status PreallocatedBuffer::write(const void* buffer, const uint64_t nbytes) {
  if (offset_ + nbytes > size_)
    return Status::PreallocatedBufferError("Write would overflow buffer.");

  std::memcpy((char*)data_ + offset_, buffer, nbytes);
  offset_ += nbytes;

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace sm
}  // namespace tiledb
