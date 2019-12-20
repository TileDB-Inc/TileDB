/**
 * @file chunk_buffers.cc
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
 * This file implements class ChunkBuffers.
 */

#include "tiledb/sm/tile/chunk_buffers.h"

#include <cstdlib>
#include <iostream>

namespace tiledb {
namespace sm {

ChunkBuffers::ChunkBuffers()
    : contigious_(false)
    , chunk_size_(0)
    , last_chunk_size_(0)
    , cached_size_(0) {
}

ChunkBuffers::ChunkBuffers(const ChunkBuffers& rhs) {
  deep_copy(rhs);
}

ChunkBuffers& ChunkBuffers::operator=(const ChunkBuffers& rhs) {
  deep_copy(rhs);
  return *this;
}

ChunkBuffers::~ChunkBuffers() {
}

void ChunkBuffers::deep_copy(const ChunkBuffers& rhs) {
  buffers_.reserve(rhs.buffers_.size());
  for (size_t i = 0; i < rhs.buffers_.size(); ++i) {
    const uint32_t buffer_size = rhs.get_chunk_size(i);
    void* const buffer_copy = malloc(buffer_size);
    memcpy(buffer_copy, rhs.buffers_[i], buffer_size);
    buffers_.emplace_back(buffer_copy);
  }

  contigious_ = rhs.contigious_;
  chunk_size_ = rhs.chunk_size_;
  last_chunk_size_ = rhs.last_chunk_size_;
  var_chunk_sizes_ = rhs.var_chunk_sizes_;
  cached_size_ = rhs.cached_size_;
}

ChunkBuffers ChunkBuffers::shallow_copy() const {
  ChunkBuffers copy;
  copy.contigious_ = contigious_;
  copy.buffers_ = buffers_;
  copy.chunk_size_ = chunk_size_;
  copy.last_chunk_size_ = last_chunk_size_;
  copy.var_chunk_sizes_ = var_chunk_sizes_;
  copy.cached_size_ = cached_size_;
  return copy;
}

void ChunkBuffers::swap(ChunkBuffers* const rhs) {
  std::swap(contigious_, rhs->contigious_);
  std::swap(buffers_, rhs->buffers_);
  std::swap(chunk_size_, rhs->chunk_size_);
  std::swap(last_chunk_size_, rhs->last_chunk_size_);
  std::swap(var_chunk_sizes_, rhs->var_chunk_sizes_);
  std::swap(cached_size_, rhs->cached_size_);
}

void ChunkBuffers::free() {
  if (contigious_) {
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

void ChunkBuffers::clear() {
  buffers_.clear();
  contigious_ = false;
  chunk_size_ = 0;
  last_chunk_size_ = 0;
  var_chunk_sizes_.clear();
  cached_size_ = 0;
}

uint64_t ChunkBuffers::size() const {
  return cached_size_;
}

bool ChunkBuffers::empty() const {
  return buffers_.empty();
}

size_t ChunkBuffers::nchunks() const {
  return buffers_.size();
}

bool ChunkBuffers::contigious() const {
  return contigious_;
}

Status ChunkBuffers::init_fixed_size(
    const bool contigious,
    const uint64_t total_size,
    const uint32_t chunk_size) {
  if (!buffers_.empty()) {
    return LOG_STATUS(Status::TileError(
        "Cannot init chunk buffers; Chunk buffers non-empty."));
  }

  contigious_ = contigious;
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
  cached_size_ = chunk_size_ * (buffers_.size() - 1) + last_chunk_size_;

  return Status::Ok();
}

Status ChunkBuffers::init_var_size(
    const bool contigious, std::vector<uint32_t>&& var_chunk_sizes) {
  if (!buffers_.empty()) {
    return LOG_STATUS(Status::TileError(
        "Cannot init chunk buffers; Chunk buffers non-empty."));
  }

  contigious_ = contigious;
  var_chunk_sizes_ = std::move(var_chunk_sizes);
  buffers_.resize(var_chunk_sizes_.size());

  assert(cached_size_ == 0);
  for (const auto& var_chunk_size : var_chunk_sizes_) {
    cached_size_ += var_chunk_size;
  }

  return Status::Ok();
}

Status ChunkBuffers::alloc_discrete(
    const size_t chunk_idx, void** const buffer) {
  if (contigious_) {
    return LOG_STATUS(
        Status::TileError("Cannot alloc discrete internal chunk buffer; Chunk "
                          "buffers are contigiously allocated"));
  }

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(Status::TileError(
        "Cannot alloc internal chunk buffer; Chunk index out of bounds"));
  }

  buffers_[chunk_idx] = malloc(get_chunk_size(chunk_idx));
  if (!buffers_[chunk_idx]) {
    LOG_FATAL("malloc() failed");
  }

  if (buffer) {
    *buffer = buffers_[chunk_idx];
  }

  return Status::Ok();
}

Status ChunkBuffers::free_discrete(const size_t chunk_idx) {
  if (contigious_) {
    return LOG_STATUS(
        Status::TileError("Cannot free discrete internal chunk buffer; Chunk "
                          "buffers are contigiously allocated"));
  }

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(Status::TileError(
        "Cannot free internal chunk buffer; Chunk index out of bounds"));
  }

  ::free(buffers_[chunk_idx]);
  return Status::Ok();
}

Status ChunkBuffers::set_contigious(void* const buffer) {
  if (!contigious_) {
    return LOG_STATUS(
        Status::TileError("Cannot alloc discrete internal chunk buffer; Chunk "
                          "buffers are discretely allocated"));
  }

  if (buffers_.empty()) {
    return LOG_STATUS(Status::TileError(
        "Cannot set chunk buffers; Chunk buffers uninitialized."));
  }

  uint64_t offset = 0;
  for (size_t i = 0; i < buffers_.size(); ++i) {
    buffers_[i] = static_cast<char*>(buffer) + offset;
    offset += get_chunk_size(i);
  }

  return Status::Ok();
}

Status ChunkBuffers::free_contigious() {
  // This asssumes buffers set with the set_contigious interface
  // were allocated with malloc().
  ::free(buffers_[0]);

  return Status::Ok();
}

Status ChunkBuffers::internal_buffer(
    const size_t chunk_idx, void** const buffer) const {
  assert(buffer);

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(Status::TileError(
        "Cannot get internal chunk buffer; Chunk index out of bounds"));
  }

  *buffer = buffers_[chunk_idx];
  return Status::Ok();
}

Status ChunkBuffers::internal_buffer_size(
    const size_t chunk_idx, uint32_t* const size) const {
  assert(size);

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(Status::TileError(
        "Cannot get internal chunk buffer size; Chunk index out of bounds"));
  }

  *size = get_chunk_size(chunk_idx);
  return Status::Ok();
}

Status ChunkBuffers::read(
    void* const buffer, const uint64_t nbytes, const uint64_t offset) const {
  if ((offset + nbytes) > size()) {
    return Status::TileError("Chunk read error; read out of bounds");
  }

  size_t chunk_idx;
  size_t chunk_offset;
  RETURN_NOT_OK(translate_logical_offset(offset, &chunk_idx, &chunk_offset));

  uint64_t nbytes_read = 0;
  while (nbytes_read < nbytes) {
    const void* const chunk_buffer = buffers_[chunk_idx];
    if (!chunk_buffer) {
      return Status::TileError("Chunk read error; chunk unallocated");
    }

    const uint64_t nbytes_remaining = nbytes - nbytes_read;
    const uint64_t cbytes_remaining = get_chunk_size(chunk_idx) - chunk_offset;
    const uint64_t bytes_to_read = std::min(nbytes_remaining, cbytes_remaining);

    memcpy(
        static_cast<char*>(buffer) + nbytes_read,
        static_cast<const char*>(chunk_buffer) + chunk_offset,
        bytes_to_read);
    nbytes_read += bytes_to_read;

    chunk_offset = 0;
    ++chunk_idx;
  }

  return Status::Ok();
}

Status ChunkBuffers::write(
    const void* const buffer, const uint64_t nbytes, const uint64_t offset) {
  if ((offset + nbytes) > size()) {
    return Status::TileError("Chunk write error; write out of bounds");
  }

  size_t chunk_idx;
  size_t chunk_offset;
  RETURN_NOT_OK(translate_logical_offset(offset, &chunk_idx, &chunk_offset));

  uint64_t nbytes_written = 0;
  while (nbytes_written < nbytes) {
    void* chunk_buffer = buffers_[chunk_idx];
    if (!chunk_buffer) {
      if (contigious_) {
        return Status::TileError("Chunk write error; unset contigious buffer");
      } else {
        RETURN_NOT_OK(alloc_discrete(chunk_idx, &chunk_buffer));
        buffers_[chunk_idx] = chunk_buffer;
      }
    }

    const uint64_t nbytes_remaining = nbytes - nbytes_written;
    const uint64_t cbytes_remaining = get_chunk_size(chunk_idx) - chunk_offset;
    const uint64_t bytes_to_write =
        std::min(nbytes_remaining, cbytes_remaining);

    memcpy(
        static_cast<char*>(chunk_buffer) + chunk_offset,
        static_cast<const char*>(buffer) + nbytes_written,
        bytes_to_write);
    nbytes_written += bytes_to_write;

    chunk_offset = 0;
    ++chunk_idx;
  }

  return Status::Ok();
}

Status ChunkBuffers::write(const ChunkBuffers& rhs, const uint64_t offset) {
  LOG_FATAL(
      "ChunkBuffers::write(const ChunkBuffers &, uint64_t) unimplemented");

  // TODO REMOVE: bypass compiler warning
  if (offset || rhs.size()) {
  }

  return Status::Ok();
}

uint32_t ChunkBuffers::get_chunk_size(const size_t chunk_idx) const {
  assert(chunk_idx < buffers_.size());
  if (fixed_chunk_sizes()) {
    return chunk_idx == (buffers_.size() - 1) ? last_chunk_size_ : chunk_size_;
  } else {
    return var_chunk_sizes_[chunk_idx];
  }
}

bool ChunkBuffers::fixed_chunk_sizes() const {
  return var_chunk_sizes_.empty();
}

Status ChunkBuffers::translate_logical_offset(
    const uint64_t logical_offset,
    size_t* const chunk_idx,
    size_t* const chunk_offset) const {
  assert(chunk_idx);
  assert(chunk_offset);

  // Optimize for the common case.
  if (logical_offset == 0) {
    *chunk_idx = 0;
    *chunk_offset = 0;
  }

  if (fixed_chunk_sizes()) {
    *chunk_idx = logical_offset / chunk_size_;
    *chunk_offset = logical_offset % chunk_size_;
  } else {
    // The expectation is that the number of chunks is
    // sufficiently small that we can perform an O(N)
    // lookup. If the number of chunks is abnormally large,
    // this could become a performance bottleneck. Let's
    // assert() here that the number of chunks is less than
    // 32k. That ensures we are operating on a 'var_chunk_sizes_'
    // object that is roughly half the maximum size of a 256KB
    // L1 cache.
    assert(var_chunk_sizes_.size() < 32000);

    // Lookup the index of the chunk that the logical offset
    // intersects and compute the chunk offset to reach the
    // logical offset.
    *chunk_idx = 0;
    uint64_t i = 0;
    while (i < logical_offset) {
      if (*chunk_idx >= buffers_.size()) {
        return Status::TileError("Out of bounds logical offset");
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