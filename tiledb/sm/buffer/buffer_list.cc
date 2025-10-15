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

namespace tiledb::sm {

BufferList::BufferList(shared_ptr<sm::MemoryTracker> memory_tracker)
    : buffers_(memory_tracker, MemoryType::SERIALIZATION_BUFFER)
    , current_buffer_index_(0)
    , current_relative_offset_(0)
    , offset_(0) {
}

const SerializationBuffer& BufferList::get_buffer(uint64_t index) const {
  if (index >= buffers_.size())
    throw BufferStatusException(
        "Cannot get buffer " + std::to_string(index) +
        " from buffer list; index out of bounds.");

  return buffers_[index];
}

uint64_t BufferList::num_buffers() const {
  return buffers_.size();
}

void BufferList::read(void* dest, uint64_t nbytes) {
  uint64_t bytes_read = read_at_most(dest, nbytes);

  if (bytes_read != nbytes)
    throw BufferStatusException(
        "BufferList error; could not read requested byte count.");
}

uint64_t BufferList::read_at_most(void* dest, uint64_t nbytes) {
  uint64_t bytes_left = nbytes;
  uint64_t dest_offset = 0;
  auto dest_ptr = static_cast<char*>(dest);

  // Starting from current buffer, read bytes until finished.
  for (size_t idx = current_buffer_index_;
       idx < buffers_.size() && bytes_left > 0;
       ++idx) {
    span<const char> src = buffers_[idx];

    // Read from buffer
    const uint64_t bytes_in_src = src.size() - current_relative_offset_;
    const uint64_t bytes_from_src = std::min(bytes_in_src, bytes_left);
    // If the destination pointer is not null, then read into it
    // if it is null then we are just seeking
    if (dest_ptr != nullptr)
      memcpy(
          dest_ptr + dest_offset,
          src.data() + current_relative_offset_,
          bytes_from_src);
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

  return nbytes - bytes_left;
}

void BufferList::seek(off_t offset, int whence) {
  switch (whence) {
    case SEEK_SET:
      // We just reset the offsets to 0/start, then fall through to seek_current
      reset_offset();
      [[fallthrough]];
    case SEEK_CUR:
      read(nullptr, offset);
      break;
    case SEEK_END:
      throw BufferStatusException(
          "SEEK_END operation not supported for BufferList");
    default:
      throw BufferStatusException("Invalid seek operation for BufferList");
  }
}

void BufferList::reset_offset() {
  offset_ = 0;
  current_buffer_index_ = 0;
  current_relative_offset_ = 0;
}

void BufferList::set_offset(
    const size_t current_buffer_index, const uint64_t current_relative_offset) {
  current_buffer_index_ = current_buffer_index;
  current_relative_offset_ = current_relative_offset;
}

std::tuple<size_t, uint64_t> BufferList::get_offset() const {
  return {current_buffer_index_, current_relative_offset_};
}

uint64_t BufferList::total_size() const {
  uint64_t size = 0;
  for (const auto& b : buffers_)
    size += b.size();
  return size;
}

}  // namespace tiledb::sm
