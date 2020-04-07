/**
 * @file chunked_buffer.cc
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
 * This file implements class ChunkedBuffer.
 */

#include "tiledb/sm/tile/chunked_buffer.h"

#include <cstdlib>
#include <iostream>

namespace tiledb {
namespace sm {

ChunkedBuffer::ChunkedBuffer()
    : buffer_addressing_(BufferAddressing::DISCRETE)
    , chunk_size_(0)
    , last_chunk_size_(0)
    , capacity_(0)
    , size_(0) {
}

ChunkedBuffer::ChunkedBuffer(const ChunkedBuffer& rhs) {
  deep_copy(rhs);
}

ChunkedBuffer& ChunkedBuffer::operator=(const ChunkedBuffer& rhs) {
  deep_copy(rhs);
  return *this;
}

ChunkedBuffer::~ChunkedBuffer() {
}

void ChunkedBuffer::deep_copy(const ChunkedBuffer& rhs) {
  buffers_.reserve(rhs.buffers_.size());
  for (size_t i = 0; i < rhs.buffers_.size(); ++i) {
    void* const buffer = rhs.buffers_[i];
    if (buffer == nullptr) {
      buffers_.emplace_back(nullptr);
    } else {
      const uint32_t buffer_size = rhs.get_chunk_capacity(i);
      void* const buffer_copy = std::malloc(buffer_size);
      std::memcpy(buffer_copy, buffer, buffer_size);
      buffers_.emplace_back(buffer_copy);
    }
  }

  buffer_addressing_ = rhs.buffer_addressing_;
  chunk_size_ = rhs.chunk_size_;
  last_chunk_size_ = rhs.last_chunk_size_;
  var_chunk_sizes_ = rhs.var_chunk_sizes_;
  capacity_ = rhs.capacity_;
  size_ = rhs.size_;
}

ChunkedBuffer ChunkedBuffer::shallow_copy() const {
  ChunkedBuffer copy;
  copy.buffer_addressing_ = buffer_addressing_;
  copy.buffers_ = buffers_;
  copy.chunk_size_ = chunk_size_;
  copy.last_chunk_size_ = last_chunk_size_;
  copy.var_chunk_sizes_ = var_chunk_sizes_;
  copy.capacity_ = capacity_;
  copy.size_ = size_;
  return copy;
}

void ChunkedBuffer::swap(ChunkedBuffer* const rhs) {
  std::swap(buffer_addressing_, rhs->buffer_addressing_);
  std::swap(buffers_, rhs->buffers_);
  std::swap(chunk_size_, rhs->chunk_size_);
  std::swap(last_chunk_size_, rhs->last_chunk_size_);
  std::swap(var_chunk_sizes_, rhs->var_chunk_sizes_);
  std::swap(capacity_, rhs->capacity_);
  std::swap(size_, rhs->size_);
}

void ChunkedBuffer::free() {
  if (buffer_addressing_ == BufferAddressing::CONTIGIOUS) {
    if (!buffers_.empty() && buffers_[0] != nullptr) {
      free_contigious();
    }
  } else {
    for (size_t i = 0; i < buffers_.size(); ++i) {
      void* const buffer = buffers_[i];
      if (buffer != nullptr) {
        auto st = free_discrete(i);
        if (!st.ok()) {
          LOG_FATAL(st.message());
        }
      }
    }
  }

  clear();
}

void ChunkedBuffer::clear() {
  buffers_.clear();
  buffer_addressing_ = BufferAddressing::DISCRETE;
  chunk_size_ = 0;
  last_chunk_size_ = 0;
  var_chunk_sizes_.clear();
  capacity_ = 0;
  size_ = 0;
}

uint64_t ChunkedBuffer::size() const {
  return size_;
}

Status ChunkedBuffer::set_size(const uint64_t size) {
  if (size > capacity_) {
    return Status::ChunkedBufferError("Cannot set size; size exceeds capacity");
  }

  size_ = size;

  return Status::Ok();
}

uint64_t ChunkedBuffer::capacity() const {
  return capacity_;
}

size_t ChunkedBuffer::nchunks() const {
  return buffers_.size();
}

ChunkedBuffer::BufferAddressing ChunkedBuffer::buffer_addressing() const {
  return buffer_addressing_;
}

Status ChunkedBuffer::init_fixed_size(
    const BufferAddressing buffer_addressing,
    const uint64_t total_size,
    const uint32_t chunk_size) {
  if (!buffers_.empty()) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot init chunk buffers; Chunk buffers non-empty."));
  }

  if (total_size == 0) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot init chunk buffers; Total size must be non-zero."));
  }

  if (chunk_size == 0) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot init chunk buffers; Chunk size must be non-zero."));
  }

  buffer_addressing_ = buffer_addressing;
  chunk_size_ = chunk_size;

  // Calculate the last chunk size.
  last_chunk_size_ = total_size % chunk_size_;
  if (last_chunk_size_ == 0) {
    last_chunk_size_ = chunk_size_;
  }

  // Calculate the number of chunks required.
  const size_t nchunks = last_chunk_size_ == chunk_size_ ?
                             total_size / chunk_size_ :
                             total_size / chunk_size_ + 1;

  buffers_.resize(nchunks);

  capacity_ = chunk_size_ * (buffers_.size() - 1) + last_chunk_size_;

  return Status::Ok();
}

Status ChunkedBuffer::init_var_size(
    const BufferAddressing buffer_addressing,
    std::vector<uint32_t>&& var_chunk_sizes) {
  if (!buffers_.empty()) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot init chunk buffers; Chunk buffers non-empty."));
  }

  if (var_chunk_sizes.empty()) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot init chunk buffers; Var chunk sizes must be non-empty."));
  }

  buffer_addressing_ = buffer_addressing;
  var_chunk_sizes_ = std::move(var_chunk_sizes);
  buffers_.resize(var_chunk_sizes_.size());

  capacity_ = 0;
  for (const auto& var_chunk_size : var_chunk_sizes_) {
    if (var_chunk_size == 0) {
      clear();
      return LOG_STATUS(Status::ChunkedBufferError(
          "Cannot init chunk buffers; Var chunk size must be non-empty."));
    }

    capacity_ += var_chunk_size;
  }

  return Status::Ok();
}

Status ChunkedBuffer::alloc_discrete(
    const size_t chunk_idx, void** const buffer) {
  if (buffer_addressing_ != BufferAddressing::DISCRETE) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot alloc discrete internal chunk buffer; Chunk buffers are not "
        "discretely allocated"));
  }

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot alloc internal chunk buffer; Chunk index out of bounds"));
  }

  buffers_[chunk_idx] = std::malloc(get_chunk_capacity(chunk_idx));
  if (!buffers_[chunk_idx]) {
    LOG_FATAL("malloc() failed");
  }

  if (buffer) {
    *buffer = buffers_[chunk_idx];
  }

  return Status::Ok();
}

Status ChunkedBuffer::free_discrete(const size_t chunk_idx) {
  if (buffer_addressing_ != BufferAddressing::DISCRETE) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot free discrete internal chunk buffer; Chunk buffers are not "
        "discretely allocated"));
  }

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot free internal chunk buffer; Chunk index out of bounds"));
  }

  ::free(buffers_[chunk_idx]);

  return Status::Ok();
}

Status ChunkedBuffer::set_contigious(void* const buffer) {
  if (buffer == nullptr) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot set contigious chunk buffers; Input buffer is null."));
  }

  if (buffer_addressing_ != BufferAddressing::CONTIGIOUS) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot alloc discrete internal chunk buffer; Chunk buffers are not "
        "contigiously allocated."));
  }

  if (buffers_.empty()) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot set contigious chunk buffers; Chunk buffers uninitialized."));
  }

  uint64_t offset = 0;
  for (size_t i = 0; i < buffers_.size(); ++i) {
    buffers_[i] = static_cast<char*>(buffer) + offset;
    offset += get_chunk_capacity(i);
  }

  return Status::Ok();
}

Status ChunkedBuffer::get_contigious(void** const buffer) const {
  if (buffer_addressing_ != BufferAddressing::CONTIGIOUS) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot get contigious internal chunk buffer; Chunk buffers are not "
        "contigiouly allocated"));
  }

  return internal_buffer(0, buffer);
}

Status ChunkedBuffer::free_contigious() {
  if (buffers_[0] == nullptr) {
    return Status::ChunkedBufferError(
        "Cannot free contigious internal chunk buffer; The internal chunk "
        "buffer is unallocated");
  }

  // This asssumes buffers set with the set_contigious interface
  // were allocated with malloc().
  ::free(buffers_[0]);

  return Status::Ok();
}

Status ChunkedBuffer::internal_buffer_from_offset(
    const uint64_t offset, void** const buffer) const {
  if (offset >= size_) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot get internal chunk buffer; Offset out of bounds"));
  }

  if (buffer_addressing_ == BufferAddressing::CONTIGIOUS) {
    RETURN_NOT_OK(get_contigious(buffer));
    *buffer = static_cast<char*>(*buffer) + offset;
    return Status::Ok();
  }

  size_t chunk_idx;
  size_t chunk_offset;
  RETURN_NOT_OK(translate_logical_offset(offset, &chunk_idx, &chunk_offset));
  RETURN_NOT_OK(internal_buffer(chunk_idx, buffer));
  *buffer = static_cast<char*>(*buffer) + chunk_offset;

  return Status::Ok();
}

Status ChunkedBuffer::internal_buffer(
    const size_t chunk_idx, void** const buffer) const {
  assert(buffer);

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot get internal chunk buffer; Chunk index out of bounds"));
  }

  *buffer = buffers_[chunk_idx];
  return Status::Ok();
}

Status ChunkedBuffer::internal_buffer_capacity(
    const size_t chunk_idx, uint32_t* const capacity) const {
  assert(capacity);

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(
        Status::ChunkedBufferError("Cannot get internal chunk buffer capacity; "
                                   "Chunk index out of bounds"));
  }

  *capacity = get_chunk_capacity(chunk_idx);
  return Status::Ok();
}

Status ChunkedBuffer::internal_buffer_size(
    const size_t chunk_idx, uint32_t* const size) const {
  assert(size);

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(Status::ChunkedBufferError(
        "Cannot get internal chunk buffer size; Chunk index out of bounds"));
  }

  *size = get_chunk_size(chunk_idx);
  return Status::Ok();
}

Status ChunkedBuffer::read(
    void* const buffer, const uint64_t nbytes, const uint64_t offset) {
  if ((offset + nbytes) > size()) {
    return Status::ChunkedBufferError("Chunk read error; read out of bounds");
  }

  // As an optimization, we can directly copy the entire requested number
  // of bytes if the chunked buffers are contigiously allocated.
  if (buffer_addressing_ == BufferAddressing::CONTIGIOUS) {
    void* chunk_buffer;
    RETURN_NOT_OK(get_contigious(&chunk_buffer));
    std::memcpy(
        static_cast<char*>(buffer),
        static_cast<char*>(chunk_buffer) + offset,
        nbytes);
    return Status::Ok();
  }

  size_t chunk_idx;
  size_t chunk_offset;
  RETURN_NOT_OK(translate_logical_offset(offset, &chunk_idx, &chunk_offset));

  uint64_t nbytes_read = 0;
  while (nbytes_read < nbytes) {
    const void* const chunk_buffer = buffers_[chunk_idx];
    if (chunk_buffer == nullptr) {
      return Status::ChunkedBufferError("Chunk read error; chunk unallocated");
    }

    const uint64_t nbytes_remaining = nbytes - nbytes_read;
    const uint64_t cbytes_remaining =
        get_chunk_capacity(chunk_idx) - chunk_offset;
    const uint64_t bytes_to_read = std::min(nbytes_remaining, cbytes_remaining);

    std::memcpy(
        static_cast<char*>(buffer) + nbytes_read,
        static_cast<const char*>(chunk_buffer) + chunk_offset,
        bytes_to_read);
    nbytes_read += bytes_to_read;

    chunk_offset = 0;
    ++chunk_idx;
  }

  return Status::Ok();
}

Status ChunkedBuffer::ensure_capacity(const uint64_t requested_capacity) {
  if (requested_capacity <= capacity_) {
    return Status::Ok();
  }

  if (capacity_ == 0) {
    return Status::ChunkedBufferError(
        "Ensure capacity failed; Chunk buffers uninitialized");
  }

  if (fixed_chunk_sizes()) {
    // Calculate the new last chunk size.
    const uint32_t orig_last_chunk_size = last_chunk_size_;
    last_chunk_size_ = requested_capacity % chunk_size_;
    if (last_chunk_size_ == 0) {
      last_chunk_size_ = chunk_size_;
    }

    // Calculate the new number of chunks required.
    const size_t nchunks = last_chunk_size_ == chunk_size_ ?
                               requested_capacity / chunk_size_ :
                               requested_capacity / chunk_size_ + 1;

    assert(buffers_.size() <= nchunks);

    // For contigiously allocated buffers, reallocate to the capacity
    // if set. For discretely allocated buffers, reallocate the last
    // chunk size if it was previously allocated.
    if (buffer_addressing_ == BufferAddressing::CONTIGIOUS) {
      void* buffer;
      RETURN_NOT_OK(get_contigious(&buffer));
      if (buffer) {
        uint64_t realloc_size = capacity_;
        while (realloc_size < requested_capacity)
          realloc_size *= 2;
        void* const realloced_buffer = std::realloc(buffer, realloc_size);
        if (!realloced_buffer) {
          return Status::ChunkedBufferError(
              "Ensure capacity failed; realloc() failed.");
        }
        RETURN_NOT_OK(set_contigious(realloced_buffer));
      }
    } else {
      assert(buffer_addressing_ == BufferAddressing::DISCRETE);
      const uint64_t realloc_size =
          buffers_.size() == nchunks ? last_chunk_size_ : chunk_size_;
      if (realloc_size != orig_last_chunk_size) {
        assert(realloc_size > orig_last_chunk_size);
        const size_t last_chunk_idx = buffers_.size() - 1;
        void* buffer;
        RETURN_NOT_OK(internal_buffer(last_chunk_idx, &buffer));
        if (buffer) {
          void* const realloced_buffer = std::realloc(buffer, realloc_size);
          if (!realloced_buffer) {
            return Status::ChunkedBufferError(
                "Ensure capacity failed; realloc() failed.");
          }
          buffers_[last_chunk_idx] = realloced_buffer;
        }
      }
    }

    // Update the capacity.
    buffers_.resize(nchunks);
    capacity_ = chunk_size_ * (buffers_.size() - 1) + last_chunk_size_;
  } else {
    // There is not a curent use-case for reallocating var-sized chunk
    // buffers.
    assert(false);
    return Status::ChunkedBufferError(
        "Ensure capacity failed; realloc unsupported for var-sized chunk "
        "buffers.");
  }

  return Status::Ok();
}

Status ChunkedBuffer::write(
    const void* const buffer, const uint64_t nbytes, const uint64_t offset) {
  RETURN_NOT_OK(ensure_capacity(offset + nbytes));

  if ((offset + nbytes) > capacity()) {
    return Status::ChunkedBufferError("Chunk write error; write out of bounds");
  }

  // As an optimization, we can directly copy the entire number of
  // bytes to write if the chunked buffers are contigiously allocated.
  if (buffer_addressing_ == BufferAddressing::CONTIGIOUS) {
    void* chunk_buffer;
    RETURN_NOT_OK(get_contigious(&chunk_buffer));
    if (!chunk_buffer) {
      return Status::ChunkedBufferError(
          "Chunk write error; unset contigious buffer");
    }

    std::memcpy(
        static_cast<char*>(chunk_buffer) + offset,
        static_cast<const char*>(buffer),
        nbytes);
    return Status::Ok();
  }

  size_t chunk_idx;
  size_t chunk_offset;
  RETURN_NOT_OK(translate_logical_offset(offset, &chunk_idx, &chunk_offset));

  uint64_t nbytes_written = 0;
  while (nbytes_written < nbytes) {
    void* chunk_buffer = buffers_[chunk_idx];
    if (!chunk_buffer) {
      if (buffer_addressing_ == BufferAddressing::CONTIGIOUS) {
        return Status::ChunkedBufferError(
            "Chunk write error; unset contigious buffer");
      } else {
        RETURN_NOT_OK(alloc_discrete(chunk_idx, &chunk_buffer));
        buffers_[chunk_idx] = chunk_buffer;
      }
    }
    const uint64_t nbytes_remaining = nbytes - nbytes_written;
    const uint64_t cbytes_remaining =
        get_chunk_capacity(chunk_idx) - chunk_offset;
    const uint64_t bytes_to_write =
        std::min(nbytes_remaining, cbytes_remaining);

    std::memcpy(
        static_cast<char*>(chunk_buffer) + chunk_offset,
        static_cast<const char*>(buffer) + nbytes_written,
        bytes_to_write);
    nbytes_written += bytes_to_write;

    chunk_offset = 0;
    ++chunk_idx;

    // Update the size cursor if necessary.
    if (offset + nbytes_written > size_) {
      size_ = offset + nbytes_written;
    }
  }

  assert(nbytes_written == nbytes);

  return Status::Ok();
}

uint32_t ChunkedBuffer::get_chunk_capacity(const size_t chunk_idx) const {
  assert(chunk_idx < buffers_.size());
  if (fixed_chunk_sizes()) {
    return chunk_idx == (buffers_.size() - 1) ? last_chunk_size_ : chunk_size_;
  } else {
    return var_chunk_sizes_[chunk_idx];
  }
}

uint32_t ChunkedBuffer::get_chunk_size(const size_t chunk_idx) const {
  assert(chunk_idx < buffers_.size());

  // Calculate the total capacity leading up to this chunk.
  uint64_t leading_capacity = 0;
  for (size_t i = 0; i < chunk_idx; ++i) {
    leading_capacity += get_chunk_capacity(i);
  }

  const uint32_t chunk_capacity = get_chunk_capacity(chunk_idx);

  if (size() <= leading_capacity) {
    return 0;
  }

  if (size() >= leading_capacity + chunk_capacity) {
    return chunk_capacity;
  }

  return size() - leading_capacity;
}

bool ChunkedBuffer::fixed_chunk_sizes() const {
  return var_chunk_sizes_.empty();
}

Status ChunkedBuffer::translate_logical_offset(
    const uint64_t logical_offset,
    size_t* const chunk_idx,
    size_t* const chunk_offset) const {
  assert(chunk_idx);
  assert(chunk_offset);

  // Optimize for the common case.
  if (logical_offset == 0) {
    *chunk_idx = 0;
    *chunk_offset = 0;
    return Status::Ok();
  }

  if (fixed_chunk_sizes()) {
    *chunk_idx = logical_offset / chunk_size_;
    *chunk_offset = logical_offset % chunk_size_;
  } else {
    // Lookup the index of the chunk that the logical offset
    // intersects and compute the chunk offset to reach the
    // logical offset.
    *chunk_idx = 0;
    uint64_t i = 0;
    while (i <= logical_offset) {
      if (*chunk_idx >= buffers_.size()) {
        return Status::ChunkedBufferError("Out of bounds logical offset");
      }
      i += var_chunk_sizes_[(*chunk_idx)++];
    }
    i -= var_chunk_sizes_[--(*chunk_idx)];
    *chunk_offset = logical_offset - i;
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
