/**
 * @file chunk_buffers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file defines class ChunkBuffers.
 *
 * The ChunkBuffers class is used to represent a logically contigious buffer
 * as a vector of individual buffers. These individual buffers are referred to
 * as "chunk buffers". Each chunk buffer may be allocated individually, which
 * will save memory in scenarios where the logically contigious buffer is
 * sparsley allocated.
 *
 * After construction, the class must be initialized before performing IO. The
 * initialization determines the following, independent usage paradigms:
 *
 * #1: Chunk Sizes: Fixed/Variable
 * The chunk sizes must be either fixed or variable. An instance with fixed chunk
 * sizes ensures that all chunk buffers are of equal size. The size of the last
 * chunk buffer may be equal-to or less-than the other chunk sizes. Instances with
 * fixed size chunks have a smaller memory footprint and have a smaller algorithmic
 * complexity when performing IO. For variable sized chunks, each chunk size is
 * independent from the others.
 *
 * #2: Chunk Buffer Addressing: Discrete/Contigious
 * The addresses of the individual chunk buffers may or may not be virtually
 * contigious. For example, the chunk addresses within a virtually contigious
 * instance may be allocated at address 1024 and 1028, where the first chunk is of
 * size 4. Non-contigious chunks (referred to as "discrete") may be allocated
 * at any address. The trade-off is that the memory of each discrete chunk is
 * managed individually, where contigious chunk buffers can be managed by the
 * first chunk alone.
 *
 * #3: Memory Management: Internal/External
 * The chunk buffers may be allocated and freed internally or externally. Internal memory
 * management is exposed through the alloc_*() and free_*() routines. External memory
 * management is exposed through the set_*() routines. Currently, this only supports external
 * memory management for contigiously addressed buffers and internal memory management for
 * discretely addressed buffers.
 *
 * Note that the ChunkBuffers class does NOT support any concept of ownership. It is
 * up to the caller to free the instance before destruction.
 */

#ifndef TILEDB_CHUNK_BUFFERS_H
#define TILEDB_CHUNK_BUFFERS_H

#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/logger.h"

#include <cinttypes>
#include <vector>

namespace tiledb {
namespace sm {

class ChunkBuffers {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ChunkBuffers();

  /** Copy constructor. */
  ChunkBuffers(const ChunkBuffers& rhs);

  /** Destructor. */
  ~ChunkBuffers();

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Assignment operator. */
  ChunkBuffers& operator=(const ChunkBuffers &rhs);


  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * ChunkBuffers fixed-size initializer. The last chunk size may be
   * equal-to or less-than *chunk_size*.
   *
   * @param contigious Whether the internal chunk buffers are contigiously
   *   or discretely addressed.
   * @param total_size The total byte size of all chunks.
   * @param chunk_size The byte size of each individual chunk.
   * @return Status
   */
  Status init_fixed_size(
    bool contigious,
    uint64_t total_size,
    uint32_t chunk_size);

  /**
   * ChunkBuffers variable-sized initializer.
   *
   * @param contigious Whether the internal chunk buffers are contigiously
   *   or discretely addressed.
   * @param chunk_sizes The size for each individual chunk.
   * @return Status
   */
  Status init_var_size(
    bool contigious,
    std::vector<uint32_t>&& chunk_sizes);

  /** Resets the state. Must be reinitialized before performing IO. */
  void clear();

  /**
   * Resets the state and frees the internal buffers. Must be
   * reinitialized before performing IO.
   */
  void free();

  /** Returns a shallow copy of the current instance. */
  ChunkBuffers shallow_copy() const;

  /** Swaps the current instance with 'rhs'. */
  void swap(ChunkBuffers *rhs);

  /**
   * Returns summation of each chunk size. If one or more chunks
   * are unallocated, this number will be greater than the summation
   * of each allocated buffer.
   */
  uint64_t size() const;

  /** Returns true if there 0 initialized chunks. */
  bool empty() const;

  /**
   * Returns the number of initialized chunks. This does not imply
   * the number of allocated chunks.
   */
  size_t nchunks() const;

  /**
   * Returns true if the chunk buffers are contigiously addressed when
   * allocated.
   */
  bool contigious() const;

  /**
   * Allocates the chunk at *chunk_idx* with the internal memory manager.
   *
   * @param chunk_idx The index of the chunk to allocate.
   * @param buffer If non-NULL, this will be mutated to the address
   *   to the newly allocated buffer. This is used as an optimization
   *   to allow the caller to avoid calling *ChunkBuffers::internal_buffer()*
   *   to fetch this address.
   * @return Status
   */
  Status alloc_discrete(size_t chunk_idx, void **buffer = nullptr);

  /**
   * Frees the chunk at *chunk_idx* with the internal memory manager.
   *
   * @param chunk_idx The index of the chunk to free.
   * @return Status
   */
  Status free_discrete(size_t chunk_idx);

  /**
   * Sets the contigious buffer to represent all chunks. This
   * must be pf size equal to the total size of the logical
   * buffer that this instance represents. Assumes *buffer*
   * was allocated with *malloc*.
   *
   * @param buffer The buffer to represent all chunks.
   * @return Status
   */
  Status set_contigious(void *buffer);

  /**
   * Frees the contigious buffer set with *ChunkBuffers::set_contigious()*.
   * This assumes the buffer was allocated with *malloc*.
   *
   * @return Status
   */
  Status free_contigious();

  /**
   * Returns the internal buffer at 'chunk_idx'. A nullptr buffer
   * indicates that the internal buffer is unallocated.
   *
   * @param chunk_idx The index of the chunk buffer to fetch.
   * @param buffer The buffer address to mutate to the chunk buffer address.
   * @return Status
   */
  Status internal_buffer(
  	size_t chunk_idx, void **buffer) const;

  /**
   * Returns the size of internal buffer at 'chunk_idx'. Returns a non-OK
   * status if the chunk buffer is unallocated.
   *
   * @param chunk_idx The index of the chunk buffer to fetch.
   * @param size The size to mutate to the chunk buffer size.
   * @return Status
   */
  Status internal_buffer_size(
    size_t chunk_idx, uint32_t *size) const;

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
  Status read(
    void* buffer,
    uint64_t nbytes,
    uint64_t offset) const;

  /**
   * Writes a buffer into the the logical buffer that the
   * chunk buffers represent. This will make as many copies
   * as chunk buffers it writes to. For discretely addressed
   * chunk buffers, they will be allocated as necessary. For
   * contigiously addressed chunk buffers, this will return a
   * non-OK status if attempting to write to an unallocated
   * chunk buffer.
   *
   * @param buffer The buffer to write.
   * @param nbytes The number of bytes to write.
   * @param offset The offset to write from.
   * @return Status
   */
  Status write(
    const void* buffer,
    uint64_t nbytes,
    uint64_t offset);

  // TODO: unimplemented
  Status write(
    const ChunkBuffers &rhs,
    uint64_t offset);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Whether the chunk buffers are contigiously or discretely allocated. */
  bool contigious_;

  /** The internal chunk buffers. */
  std::vector<void *> buffers_;

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
  uint64_t cached_size_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Mutates the current instance to a deep copy of *rhs*. */
  void deep_copy(const ChunkBuffers &rhs);

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
    uint64_t logical_offset,
    size_t *chunk_idx,
    size_t *chunk_offset) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CHUNK_BUFFERS_H
