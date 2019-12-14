/**
 * @file   filter_buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * This file defines class FilterBuffer.
 */

#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/misc/logger.h"

namespace tiledb {
namespace sm {

FilterBuffer::BufferOrView::BufferOrView(
    const std::shared_ptr<Buffer>& buffer) {
  underlying_buffer_ = buffer;
  is_view_ = false;
}

FilterBuffer::BufferOrView::BufferOrView(
    const std::shared_ptr<Buffer>& buffer, uint64_t offset, uint64_t nbytes) {
  is_view_ = true;
  underlying_buffer_ = buffer;
  view_ = std::unique_ptr<Buffer>(
      new Buffer((char*)buffer->data() + offset, nbytes));
}

FilterBuffer::BufferOrView::BufferOrView(BufferOrView&& other) {
  underlying_buffer_.swap(other.underlying_buffer_);
  view_.swap(other.view_);
  std::swap(is_view_, other.is_view_);
}

Buffer* FilterBuffer::BufferOrView::buffer() const {
  return is_view_ ? view_.get() : underlying_buffer_.get();
}

std::shared_ptr<Buffer> FilterBuffer::BufferOrView::underlying_buffer() const {
  return underlying_buffer_;
}

bool FilterBuffer::BufferOrView::is_view() const {
  return is_view_;
}

FilterBuffer::BufferOrView FilterBuffer::BufferOrView::get_view(
    uint64_t offset, uint64_t nbytes) const {
  if (is_view_) {
    BufferOrView new_view(underlying_buffer_);
    new_view.is_view_ = true;
    new_view.view_ = std::unique_ptr<Buffer>(
        new Buffer((char*)view_->data() + offset, nbytes));
    return new_view;
  } else {
    return BufferOrView(underlying_buffer_, offset, nbytes);
  }
}

FilterBuffer::FilterBuffer()
    : FilterBuffer(nullptr) {
}

FilterBuffer::FilterBuffer(FilterStorage* storage) {
  offset_ = 0;
  current_buffer_ = buffers_.cend();
  current_relative_offset_ = 0;
  fixed_allocation_data_ = nullptr;
  fixed_allocation_op_allowed_ = false;
  read_only_ = false;
  storage_ = storage;
}

Status FilterBuffer::swap(FilterBuffer& other) {
  if (read_only_ || other.read_only_)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot swap read-only buffers."));

  buffers_.swap(other.buffers_);
  std::swap(offset_, other.offset_);
  std::swap(current_buffer_, other.current_buffer_);
  std::swap(current_relative_offset_, other.current_relative_offset_);
  std::swap(fixed_allocation_data_, other.fixed_allocation_data_);
  std::swap(fixed_allocation_op_allowed_, other.fixed_allocation_op_allowed_);
  std::swap(read_only_, other.read_only_);
  std::swap(storage_, other.storage_);

  return Status::Ok();
}

Status FilterBuffer::init(void* data, uint64_t nbytes) {
  if (!buffers_.empty())
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot init buffer: not empty."));
  else if (data == nullptr)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot init buffer: nullptr given."));
  else if (read_only_)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot init buffer: read-only."));

  std::shared_ptr<Buffer> buffer(new Buffer(data, nbytes));
  offset_ = 0;
  buffers_.emplace_back(std::move(buffer));
  current_relative_offset_ = 0;
  current_buffer_ = --(buffers_.end());
  return Status::Ok();
}

Status FilterBuffer::set_fixed_allocation(void* buffer, uint64_t nbytes) {
  if (!buffers_.empty() || fixed_allocation_data_ != nullptr)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot set fixed allocation: not empty."));
  else if (read_only_)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot set fixed allocation: read-only."));

  RETURN_NOT_OK(init(buffer, nbytes));
  fixed_allocation_data_ = buffer;
  fixed_allocation_op_allowed_ = true;

  return Status::Ok();
}

Status FilterBuffer::copy_to(Buffer* dest) const {
  for (auto it = buffers_.cbegin(), ite = buffers_.cend(); it != ite; ++it) {
    Buffer* src = it->buffer();
    src->reset_offset();
    RETURN_NOT_OK(dest->write(src->data(), src->size()));
  }

  return Status::Ok();
}

Status FilterBuffer::copy_to(void* dest) const {
  uint64_t dest_offset = 0;
  for (auto it = buffers_.cbegin(), ite = buffers_.cend(); it != ite; ++it) {
    Buffer* src = it->buffer();
    std::memcpy((char*)dest + dest_offset, src->data(), src->size());
    dest_offset += src->size();
  }

  return Status::Ok();
}

Status FilterBuffer::get_const_buffer(
    uint64_t nbytes, ConstBuffer* buffer) const {
  if (current_buffer_ == buffers_.end())
    return LOG_STATUS(
        Status::FilterError("FilterBuffer error; no current buffer."));

  Buffer* buf = current_buffer_->buffer();
  uint64_t bytes_in_buf = buf->size() - current_relative_offset_;
  if (bytes_in_buf < nbytes)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; ConstBuffer would span multiple regions."));

  *buffer = ConstBuffer(buf->data(current_relative_offset_), nbytes);

  return Status::Ok();
}

std::vector<ConstBuffer> FilterBuffer::buffers() const {
  std::vector<ConstBuffer> result;

  for (auto it = buffers_.cbegin(), ite = buffers_.cend(); it != ite; ++it) {
    Buffer* buffer = it->buffer();
    result.emplace_back(buffer->data(), buffer->size());
  }

  return result;
}

uint64_t FilterBuffer::num_buffers() const {
  return buffers_.size();
}

Buffer* FilterBuffer::buffer_ptr(unsigned index) const {
  assert(!read_only_);  // Just to be safe

  for (auto it = buffers_.cbegin(), ite = buffers_.cend(); it != ite; ++it) {
    if (index == 0) {
      auto b = it->buffer();
      b->reset_offset();
      return b;
    }
    index--;
  }
  return nullptr;
}

Status FilterBuffer::read(void* buffer, uint64_t nbytes) {
  uint64_t bytes_left = nbytes;
  uint64_t dest_offset = 0;
  for (auto it = current_buffer_, ite = buffers_.cend(); it != ite; ++it) {
    Buffer* src = it->buffer();
    uint64_t bytes_in_src = src->size() - current_relative_offset_;
    uint64_t bytes_from_src = std::min(bytes_in_src, bytes_left);
    src->set_offset(current_relative_offset_);
    RETURN_NOT_OK(src->read((char*)buffer + dest_offset, bytes_from_src));

    bytes_left -= bytes_from_src;
    dest_offset += bytes_from_src;

    // Keep current buffer in sync.
    current_buffer_ = it;

    if (bytes_left == 0) {
      current_relative_offset_ += bytes_from_src;
      break;
    } else {
      current_relative_offset_ = 0;
    }
  }

  if (bytes_left > 0)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; could not read requested byte count."));

  // Adjust the offset and advance to next buffer if we're at the end of it.
  offset_ += nbytes;

  if (current_buffer_ != buffers_.end()) {
    if (current_relative_offset_ == current_buffer_->buffer()->size()) {
      ++current_buffer_;
      current_relative_offset_ = 0;
    }
  }

  return Status::Ok();
}

bool FilterBuffer::read_only() const {
  return read_only_;
}

Status FilterBuffer::write(const void* buffer, uint64_t nbytes) {
  if (read_only_)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot set write: read-only."));

  uint64_t bytes_left = nbytes;
  uint64_t src_offset = 0;
  for (auto it = current_buffer_, ite = buffers_.cend(); it != ite; ++it) {
    Buffer* dest = it->buffer();
    uint64_t dest_buffer_size =
        dest->owns_data() ? dest->alloced_size() : dest->size();
    uint64_t bytes_avail_in_dest = dest_buffer_size - current_relative_offset_;
    if (bytes_avail_in_dest == 0)
      return LOG_STATUS(Status::FilterError(
          "FilterBuffer error; could not write: buffer is full."));

    // Write to the destination. We can't use Buffer::write() here as the
    // buffer may not own its data, and write() only works on owned buffers.
    uint64_t bytes_to_dest = std::min(bytes_avail_in_dest, bytes_left);
    uint64_t old_size = dest->size();
    std::memcpy(
        dest->value_ptr(current_relative_offset_),
        (char*)buffer + src_offset,
        bytes_to_dest);

    // Writing past the end (but still within the allocation) should update
    // the size accordingly.
    if (current_relative_offset_ + bytes_to_dest > old_size) {
      auto appended = (current_relative_offset_ + bytes_to_dest) - old_size;
      dest->set_size(old_size + appended);
    }

    bytes_left -= bytes_to_dest;
    src_offset += bytes_to_dest;

    // Keep current buffer in sync.
    current_buffer_ = it;

    if (bytes_left == 0) {
      current_relative_offset_ += bytes_to_dest;
      break;
    } else {
      current_relative_offset_ = 0;
    }
  }

  if (bytes_left > 0)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; could not write requested byte count."));

  // Adjust the offset and advance to next buffer if we're at the end of it.
  offset_ += nbytes;

  if (current_buffer_ != buffers_.end()) {
    auto* curr = current_buffer_->buffer();
    uint64_t size = curr->owns_data() ? curr->alloced_size() : curr->size();
    if (current_relative_offset_ == size) {
      ++current_buffer_;
      current_relative_offset_ = 0;
    }
  }

  return Status::Ok();
}

Status FilterBuffer::write(FilterBuffer* other, uint64_t nbytes) {
  if (read_only_)
    return LOG_STATUS(
        Status::FilterError("FilterBuffer error; cannot write: read-only."));

  auto list_node = other->current_buffer_;
  uint64_t relative_offset = other->current_relative_offset_;
  uint64_t bytes_left = nbytes;
  for (auto it = list_node, ite = other->buffers_.cend(); it != ite; ++it) {
    if (bytes_left == 0)
      break;

    Buffer* buf = it->buffer();
    uint64_t bytes_in_buf = buf->size() - relative_offset;
    uint64_t bytes_from_buf = std::min(bytes_left, bytes_in_buf);

    // Use bare pointer write function to copy the data.
    RETURN_NOT_OK(this->write(buf->data(relative_offset), bytes_from_buf));

    bytes_left -= bytes_from_buf;
    relative_offset = 0;
  }

  if (bytes_left > 0)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; could not write requested byte count."));

  return Status::Ok();
}

Status FilterBuffer::get_relative_offset(
    uint64_t offset,
    std::list<BufferOrView>::const_iterator* list_node,
    uint64_t* relative_offset) const {
  uint64_t rel_offset = offset;
  for (auto it = buffers_.begin(), ite = buffers_.end(); it != ite; ++it) {
    Buffer* buf = it->buffer();
    uint64_t buffer_size = buf->owns_data() ? buf->alloced_size() : buf->size();
    if (rel_offset < buffer_size) {
      *list_node = it;
      *relative_offset = rel_offset;
      return Status::Ok();
    }
    rel_offset -= buffer_size;
  }

  return LOG_STATUS(Status::FilterError(
      "FilterBuffer error; cannot determine relative offset."));
}

uint64_t FilterBuffer::offset() const {
  return offset_;
}

uint64_t FilterBuffer::size() const {
  uint64_t size = 0;
  for (auto& b : buffers_)
    size += b.buffer()->size();
  return size;
}

void FilterBuffer::reset_offset() {
  offset_ = 0;
  current_buffer_ = buffers_.begin();
  current_relative_offset_ = 0;
}

void FilterBuffer::set_offset(uint64_t offset) {
  if (offset == 0) {
    reset_offset();
    return;
  }

  auto list_node = buffers_.cend();
  uint64_t relative_offset = 0;
  auto st = get_relative_offset(offset, &list_node, &relative_offset);
  assert(st.ok());
  offset_ = offset;
  current_buffer_ = list_node;
  current_relative_offset_ = relative_offset;
}

void FilterBuffer::set_read_only(bool read_only) {
  read_only_ = read_only;
}

void FilterBuffer::advance_offset(uint64_t nbytes) {
  Buffer* buf = current_buffer_->buffer();
  if (current_relative_offset_ + nbytes < buf->size()) {
    // Fast path: within the current buffer
    current_relative_offset_ += nbytes;
    offset_ += nbytes;
  } else if (current_relative_offset_ + nbytes == buf->size()) {
    // Fast path: exactly to the end of the current buffer.
    current_relative_offset_ = 0;
    current_buffer_ = std::next(current_buffer_, 1);
    offset_ += nbytes;
  } else if (offset_ + nbytes <= size()) {
    // Slower path: across buffers.
    auto list_node = buffers_.cend();
    uint64_t relative_offset = 0;
    auto st =
        get_relative_offset(offset_ + nbytes, &list_node, &relative_offset);
    assert(st.ok());
    current_buffer_ = list_node;
    current_relative_offset_ = relative_offset;
    offset_ += nbytes;
  }
}

Status FilterBuffer::prepend_buffer(uint64_t nbytes) {
  if (read_only_)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot prepend buffer: read-only."));

  if (fixed_allocation_data_ == nullptr) {
    // Normal case: realloc and prepend a new Buffer.
    auto buf_ptr = storage_->get_buffer();
    RETURN_NOT_OK(buf_ptr->realloc(nbytes));
    buf_ptr->reset_offset();
    buf_ptr->reset_size();
    buffers_.emplace_front(buf_ptr);
    // Keep the offset in the same global place it was.
    offset_ += nbytes;
  } else {
    // Fixed allocation case: prepend is a no-op because the fixed allocation
    // memory region must be used. That is why only one prepend/append is
    // allowed.
    assert(!buffers_.empty());

    // Check for errors
    if (!fixed_allocation_op_allowed_)
      return LOG_STATUS(
          Status::FilterError("FilterBuffer error; cannot prepend buffer: "
                              "fixed allocation is set."));
    else if (nbytes > buffers_.front().buffer()->size())
      return LOG_STATUS(
          Status::FilterError("FilterBuffer error; cannot prepend buffer: "
                              "fixed allocation not large enough."));

    // Disallow further operations
    fixed_allocation_op_allowed_ = false;
  }

  reset_offset();

  return Status::Ok();
}

Status FilterBuffer::append_view(
    const FilterBuffer* other, uint64_t offset, uint64_t nbytes) {
  if (read_only_)
    return LOG_STATUS(Status::FilterError(
        "FilterBuffer error; cannot append view: read-only."));

  // Empty views can be skipped.
  if (nbytes == 0)
    return Status::Ok();

  // Check for fixed-allocation errors first.
  if (fixed_allocation_data_ != nullptr) {
    assert(!buffers_.empty());
    if (!fixed_allocation_op_allowed_)
      return LOG_STATUS(Status::FilterError(
          "FilterBuffer error; cannot append view: fixed allocation set."));
    else if (nbytes > buffers_.front().buffer()->size())
      return LOG_STATUS(
          Status::FilterError("FilterBuffer error; cannot append view: "
                              "fixed allocation not large enough."));

    // Disallow further operations
    fixed_allocation_op_allowed_ = false;
  }

  auto list_node = other->buffers_.cend();
  uint64_t relative_offset = 0;
  RETURN_NOT_OK(
      other->get_relative_offset(offset, &list_node, &relative_offset));

  uint64_t bytes_left = nbytes;
  for (auto it = list_node, ite = other->buffers_.cend(); it != ite; ++it) {
    if (bytes_left == 0)
      break;

    Buffer* buf = it->buffer();
    buf->reset_offset();
    uint64_t bytes_in_buf = buf->size() - relative_offset;
    uint64_t bytes_from_buf = std::min(bytes_left, bytes_in_buf);

    auto view = it->get_view(relative_offset, bytes_from_buf);

    if (fixed_allocation_data_ == nullptr) {
      // Normal case: append the view to the list of buffers.
      buffers_.push_back(std::move(view));
    } else {
      // When fixed allocation is set, copy the data instead.
      std::memcpy(
          buffers_.front().buffer()->data(),
          view.buffer()->data(),
          view.buffer()->size());
    }

    bytes_left -= bytes_from_buf;
    relative_offset = 0;
  }

  reset_offset();

  return Status::Ok();
}

Status FilterBuffer::append_view(const FilterBuffer* other) {
  return append_view(other, 0, other->size());
}

Status FilterBuffer::clear() {
  if (read_only_)
    return LOG_STATUS(
        Status::FilterError("FilterBuffer error; cannot clear: read-only."));

  offset_ = 0;

  // Save the raw pointers to the underlying buffers and clear the list, which
  // decrements the ref counts.
  std::vector<Buffer*> buffer_ptrs;
  for (const auto& buf : buffers_)
    buffer_ptrs.push_back(buf.underlying_buffer().get());

  buffers_.clear();
  current_relative_offset_ = 0;
  current_buffer_ = buffers_.cend();

  fixed_allocation_data_ = nullptr;
  fixed_allocation_op_allowed_ = false;

  // Mark all of the buffers as available.
  for (Buffer* b : buffer_ptrs)
    RETURN_NOT_OK(storage_->reclaim(b));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
