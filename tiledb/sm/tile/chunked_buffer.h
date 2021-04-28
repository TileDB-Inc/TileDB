/**
 * @file chunked_buffer.h
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
 * This file defines class ChunkedBuffer.
 *
 * The ChunkedBuffer class is used to represent a logically contiguous buffer
 * as a vector of individual buffers. These individual buffers are referred to
 * as "chunk buffers". Each chunk buffer may be allocated individually, which
 * will save memory in scenarios where the logically contiguous buffer is
 * sparsley allocated.
 *
 * After construction, the class must be initialized before performing IO. The
 * initialization determines the following, independent usage paradigms:
 *
 * #1: Chunk Sizes: Fixed/Variable
 * The chunk sizes must be either fixed or variable. An instance with fixed
 * chunk sizes ensures that all chunk buffers are of equal size. The size of the
 * last chunk buffer may be equal-to or less-than the other chunk sizes.
 * Instances with fixed size chunks have a smaller memory footprint and have a
 * smaller algorithmic complexity when performing IO. For variable sized chunks,
 * each chunk size is independent from the others.
 *
 * #2: Chunk Buffer Addressing: Discrete/Contigious
 * The addresses of the individual chunk buffers may or may not be virtually
 * contiguous. For example, the chunk addresses within a virtually contiguous
 * instance may be allocated at address 1024 and 1028, where the first chunk is
 * of size 4. Non-contiguous chunks (referred to as "discrete") may be allocated
 * at any address. The trade-off is that the memory of each discrete chunk is
 * managed individually, where contiguous chunk buffers can be managed by the
 * first chunk alone.
 *
 * #3: Memory Management: Internal/External
 * The chunk buffers may be allocated and freed internally or externally.
 * Internal memory management is exposed through the alloc_*() and free_*()
 * routines. External memory management is exposed through the set_*() routines.
 * Currently, this only supports external memory management for contiguously
 * addressed buffers and internal memory management for discretely addressed
 * buffers.
 *
 * Note that the ChunkedBuffer class does NOT support any concept of ownership.
 * It is up to the caller to free the instance before destruction.
 */

#ifndef TILEDB_CHUNKED_BUFFER_H
#define TILEDB_CHUNKED_BUFFER_H

#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"

#include <cinttypes>
#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ChunkedBuffer {
 public:
  enum BufferAddressing { CONTIGUOUS, DISCRETE };

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ChunkedBuffer();

  /** Copy constructor. */
  ChunkedBuffer(const ChunkedBuffer& rhs);

  /** Destructor. */
  ~ChunkedBuffer();

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Assignment operator. */
  ChunkedBuffer& operator=(const ChunkedBuffer& rhs);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * ChunkedBuffer fixed-size initializer. The last chunk size may be
   * equal-to or less-than *chunk_size*.
   *
   * @param buffer_addressing The internal buffer addressing type.
   * @param total_size The total byte size of all chunks.
   * @param chunk_size The byte size of each individual chunk.
   * @return Status
   */
  Status init_fixed_size(
      BufferAddressing buffer_addressing,
      uint64_t total_size,
      uint32_t chunk_size);

  /**
   * ChunkedBuffer variable-sized initializer.
   *
   * @param buffer_addressing The internal buffer addressing type.
   * @param chunk_sizes The size for each individual chunk.
   * @return Status
   */
  Status init_var_size(
      BufferAddressing buffer_addressing, std::vector<uint32_t>&& chunk_sizes);

  /** Resets the state. Must be reinitialized before performing IO. */
  void clear();

  /**
   * Resets the state and frees the internal buffers. Must be
   * reinitialized before performing IO.
   */
  void free();

  /** Returns a shallow copy of the current instance. */
  ChunkedBuffer shallow_copy() const;

  /** Swaps the current instance with 'rhs'. */
  void swap(ChunkedBuffer* rhs);

  /**
   * Returns the logical size. The returned size does not guarantee that
   * all bytes within the range [0, size] are allocated. This is a logical
   * cursor to the index immediately after the last written byte.
   */
  uint64_t size() const;

  /**
   * Sets the logical size. This must be less than or equal to the
   * capacity. This does not perform any additional allocations.
   */
  Status set_size(uint64_t size);

  /**
   * Returns the summation of all chunk sizes. This does not consider
   * whether an individual chunk is allocated. The returned value may
   * be interpretted as the maximum number of bytes that may be allocated
   * within this instance.
   */
  uint64_t capacity() const;

  /**
   * Returns the number of initialized chunks. This does not imply
   * the number of allocated chunks.
   */
  size_t nchunks() const;

  /**
   * Returns the internal buffer addressing type.
   */
  BufferAddressing buffer_addressing() const;

  /**
   * Allocates the chunk at *chunk_idx* with the internal memory manager.
   *
   * @param chunk_idx The index of the chunk to allocate.
   * @param buffer If non-NULL, this will be mutated to the address
   *   to the newly allocated buffer. This is used as an optimization
   *   to allow the caller to avoid calling *ChunkedBuffer::internal_buffer()*
   *   to fetch this address.
   * @return Status
   */
  Status alloc_discrete(size_t chunk_idx, void** buffer = nullptr);

  /**
   * Frees the chunk at *chunk_idx* with the internal memory manager.
   *
   * @param chunk_idx The index of the chunk to free.
   * @return Status
   */
  Status free_discrete(size_t chunk_idx);

  /**
   * Sets the contiguous buffer to represent all chunks. This
   * must be pf size equal to the total size of the logical
   * buffer that this instance represents. Assumes *buffer*
   * was allocated with *malloc*.
   *
   * @param buffer The buffer to represent all chunks.
   * @return Status
   */
  Status set_contiguous(void* buffer);

  /**
   * Returns the address of the first chunk, which is guaranteed
   * to be contiguous in the range of [0, this->capacity()). Returns
   * an error for instances with non-contiguous buffer addressing.
   */
  Status get_contiguous(void** buffer) const;

  /**
   * This is a variant of `get_contiguous` that does not check if
   * the buffer addressing is `CONTIGUOUS` or if a contiguous buffer
   * has been set. This exists for use in performance-critical paths
   * where the caller can guarantee the contiguous buffer exists.
   */
  inline void* get_contiguous_unsafe() {
    return buffers_[0];
  }

  /**
   * Frees the contiguous buffer set with *ChunkedBuffer::set_contiguous()*.
   * This assumes the buffer was allocated with *malloc*.
   *
   * @return Status
   */
  Status free_contiguous();

  /**
   * Returns a pointer to an internal chunked buffer from a logical offset.
   * For example, if there are two chunked buffers of size 10 and the logical
   * offset is 15, this will return the address of the second chunked buffer
   * + 5.
   *
   * @param offset The logical offset into
   * @param buffer The buffer address to mutate to the chunk buffer address.
   * @return Status
   */
  Status internal_buffer_from_offset(uint64_t offset, void** buffer) const;

  /**
   * Returns the internal buffer at 'chunk_idx'. A nullptr buffer
   * indicates that the internal buffer is unallocated.
   *
   * @param chunk_idx The index of the chunk buffer to fetch.
   * @param buffer The buffer address to mutate to the chunk buffer address.
   * @return Status
   */
  Status internal_buffer(size_t chunk_idx, void** buffer) const;

  /**
   * Returns the capacity of internal buffer at 'chunk_idx'.
   *
   * @param chunk_idx The index of the chunk buffer to fetch.
   * @param capacity The capacity to mutate to the chunk buffer capacity.
   * @return Status
   */
  Status internal_buffer_capacity(size_t chunk_idx, uint32_t* capacity) const;

  /**
   * Returns the size of the internal buffer at 'chunk_idx'.
   *
   * @param chunk_idx The index of the chunk buffer to fetch.
   * @param size The size to mutate to the chunk buffer size.
   * @return Status
   */
  Status internal_buffer_size(size_t chunk_idx, uint32_t* size) const;

  /**
   * Reads from the offset of the logical buffer that the chunk
   * buffers represent. This makes a copy and will return a non-OK
   * status if any subset of the region to read contains an unallocated
   * chunk buffer.
   *
   * @param buffer The buffer to copy the output to.
   * @param nbytes The number of bytes to read.
   * @param offset The offset to read from.
   * @return Status
   */
  Status read(void* buffer, uint64_t nbytes, uint64_t offset);

  /**
   * Writes a buffer into the the logical buffer that the
   * chunk buffers represent. This will make as many copies
   * as chunk buffers it writes to. For discretely addressed
   * chunk buffers, they will be allocated as necessary. For
   * contiguously addressed chunk buffers, this will return a
   * non-OK status if attempting to write to an unallocated
   * chunk buffer.
   *
   * @param buffer The buffer to write.
   * @param nbytes The number of bytes to write.
   * @param offset The offset to write from.
   * @return Status
   */
  Status write(const void* buffer, uint64_t nbytes, uint64_t offset);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The internal buffer addressing type. */
  BufferAddressing buffer_addressing_;

  /** The internal chunk buffers. */
  std::vector<void*> buffers_;

  /** The chunk size for fixed-size chunks. */
  uint32_t chunk_size_;

  /** The last chunk size for fixed-size chunks. */
  uint32_t last_chunk_size_;

  /** The chunk size for variable-sized chunks. */
  std::vector<uint32_t> var_chunk_sizes_;

  /**
   * The summation of all chunk sizes. Recomputed when the
   * chunk sizes change.
   */
  uint64_t capacity_;

  /**
   * The logical size reflecting the highest written byte.
   */
  uint64_t size_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Mutates the current instance to a deep copy of *rhs*. */
  void deep_copy(const ChunkedBuffer& rhs);

  /**
   * Assigns 'buffer' as the internal contiguous buffer
   * within 'buffers_'. This is a performance-critical
   * routine within the write path.
   */
  inline void set_contiguous_internal(void* buffer);

  /**
   * Ensures that the capacity is greater than or equal to
   * 'requested_capacity'. This expects that the caller has
   * verified that `requested_capacity` > `capacity_`.
   *
   * @param chunk_idx The index of the chunk.
   * @return uint32_t
   */
  Status ensure_capacity(uint64_t requested_capacity);

  /**
   * Returns the chunk capacity at the given index.
   *
   * @param chunk_idx The index of the chunk.
   * @return uint32_t
   */
  uint32_t get_chunk_capacity(size_t chunk_idx) const;

  /**
   * Returns the chunk size at the given index.
   *
   * @param chunk_idx The index of the chunk.
   * @return uint32_t
   */
  uint32_t get_chunk_size(size_t chunk_idx) const;

  /** Returns true if chunks are of a fixed size. */
  bool fixed_chunk_sizes() const;

  /**
   * Returns the chunk index and offset within the chunk
   * that point to the given offset of the logical buffer
   * that the chunks represent.
   * Runs in O(1) for fixed size chunks and O(N) for
   * variable-sized chunks.
   *
   * @param logical_offset The offset of the logical buffer.
   * @param chunk_idx Mutated to the translated chunk index.
   * @param chunk_offset Mutated to the translated chunk offset.
   * @return Status
   */
  Status translate_logical_offset(
      uint64_t logical_offset, size_t* chunk_idx, size_t* chunk_offset) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CHUNKED_BUFFER_H
