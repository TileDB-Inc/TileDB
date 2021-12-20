/**
 * @file   buffer_list.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 TileDB, Inc.
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
 * This file implements class BufferList.
 */

#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"

#include <algorithm>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

BufferList::BufferList()
    : current_buffer_index_(0)
    , current_relative_offset_(0)
    , offset_(0) {
}

Status BufferList::add_buffer(Buffer&& buffer) {
  buffers_.emplace_back(std::move(buffer));
  return Status::Ok();
}

Status BufferList::get_buffer(uint64_t index, Buffer** buffer) {
  if (index >= buffers_.size())
    return LOG_STATUS(Status_BufferError(
        "Cannot get buffer " + std::to_string(index) +
        " from buffer list; index out of bounds."));

  *buffer = &buffers_[index];

  return Status::Ok();
}

uint64_t BufferList::num_buffers() const {
  return buffers_.size();
}

Status BufferList::read(void* dest, uint64_t nbytes) {
  uint64_t bytes_read = 0;
  RETURN_NOT_OK(read(dest, nbytes, &bytes_read));

  if (bytes_read != nbytes)
    return LOG_STATUS(Status_BufferError(
        "BufferList error; could not read requested byte count."));

  return Status::Ok();
}

Status BufferList::read_at_most(
    void* dest, uint64_t nbytes, uint64_t* bytes_read) {
  return read(dest, nbytes, bytes_read);
}

Status BufferList::read(void* dest, uint64_t nbytes, uint64_t* bytes_read) {
  uint64_t bytes_left = nbytes;
  uint64_t dest_offset = 0;
  auto dest_ptr = static_cast<char*>(dest);

  // Starting from current buffer, read bytes until finished.
  for (size_t idx = current_buffer_index_;
       idx < buffers_.size() && bytes_left > 0;
       ++idx) {
    Buffer& src = buffers_[idx];
    src.set_offset(current_relative_offset_);

    // Read from buffer
    const uint64_t bytes_in_src = src.size() - current_relative_offset_;
    const uint64_t bytes_from_src = std::min(bytes_in_src, bytes_left);
    // If the destination pointer is not null, then read into it
    // if it is null then we are just seeking
    if (dest_ptr != nullptr)
      RETURN_NOT_OK(src.read(dest_ptr + dest_offset, bytes_from_src));
    bytes_left -= bytes_from_src;
    dest_offset += bytes_from_src;

    // Always keep current buffer and offset in sync.
    current_buffer_index_ = idx;
    current_relative_offset_ += bytes_from_src;

    if (bytes_left > 0) {
      // Moving to next buffer; set relative offset to 0.
      current_relative_offset_ = 0;
    }
  }

  if (bytes_read != nullptr)
    *bytes_read = nbytes - bytes_left;

  return Status::Ok();
}

Status BufferList::seek(off_t offset, int whence) {
  switch (whence) {
    case SEEK_SET:
      // We just reset the offsets to 0/start, then fall through to seek_current
      reset_offset();
      // fall through
    case SEEK_CUR:
      return read(nullptr, offset);
    case SEEK_END:
      return Status_BufferError(
          "SEEK_END operation not supported for BufferList");
    default:
      return Status_BufferError("Invalid seek operation for BufferList");
  }

  return Status::Ok();
}

void BufferList::reset_offset() {
  offset_ = 0;
  current_buffer_index_ = 0;
  current_relative_offset_ = 0;
}

uint64_t BufferList::total_size() const {
  uint64_t size = 0;
  for (const auto& b : buffers_)
    size += b.size();
  return size;
}

}  // namespace sm
}  // namespace tiledb
