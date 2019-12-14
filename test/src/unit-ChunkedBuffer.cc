/**
 * @file unit-ChunkedBuffer.cc
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
 * Tests the `ChunkedBuffer` class.
 */

#include "tiledb/sm/tile/chunked_buffer.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "ChunkedBuffer: Test default constructor",
    "[ChunkedBuffer][default_constructor]") {
  // Test the default constructor and verify the empty state.
  ChunkedBuffer chunked_buffer;
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == 0);
  void* buffer = nullptr;
  CHECK(!chunked_buffer.internal_buffer(0, &buffer).ok());
  CHECK(!buffer);
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, fixed size IO",
    "[ChunkedBuffer][discrete_fixed_io]") {
  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Attempt a write before initializing the test ChunkedBuffer.
  uint64_t write_offset = 0;
  CHECK(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  // Attempt a read before initializing the test ChunkedBuffer.
  uint64_t read_offset = 0;
  uint64_t read_buffer[buffer_len];
  CHECK(!chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());

  // Attempt an alloc before initializing the test ChunkedBuffer.
  size_t chunk_idx = 0;
  void* chunk_buffer = nullptr;
  CHECK(!chunked_buffer.alloc_discrete(chunk_idx, &chunk_buffer).ok());
  CHECK(!chunk_buffer);

  // Attempt a set before initializing the test ChunkedBuffer.
  chunk_buffer = nullptr;
  CHECK(!chunked_buffer.set_contigious(chunk_buffer).ok());
  CHECK(!chunk_buffer);

  // Initialize the ChunkedBuffer.
  CHECK(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  CHECK(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  CHECK(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  CHECK(chunked_buffer.capacity() == buffer_size);
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == nchunks);

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    void* chunk_buffer;
    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(!chunk_buffer);
  }

  // Write the entire buffer. This will allocate all of the chunks.
  write_offset = 0;
  CHECK(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  // Verify all chunks are allocated and that they do not overlap
  // 'buffer' because they have been deep-copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    uint32_t internal_size = 0;
    CHECK(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      CHECK(internal_size == chunk_size);
    } else {
      CHECK(internal_size == last_chunk_size);
    }

    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(chunk_buffer);
    CHECK(chunk_buffer != write_buffer);
    if (chunk_buffer < write_buffer) {
      CHECK(
          static_cast<char*>(chunk_buffer) + chunk_size <=
          reinterpret_cast<char*>(write_buffer));
    } else {
      CHECK(
          reinterpret_cast<char*>(write_buffer) + buffer_size <=
          static_cast<char*>(chunk_buffer));
    }
  }
  // Read the third element, this will be of value '2'.
  read_offset = 2 * sizeof(uint64_t);
  uint64_t two = 0;
  CHECK(chunked_buffer.read(&two, sizeof(uint64_t), read_offset).ok());
  CHECK(two == 2);

  // Read the 10th element, this will be of value '9'.
  read_offset = 9 * sizeof(uint64_t);
  uint64_t nine = 0;
  CHECK(chunked_buffer.read(&nine, sizeof(uint64_t), read_offset).ok());
  CHECK(nine == 9);

  // Read the 100th element, this will be of value '99'.
  read_offset = 99 * sizeof(uint64_t);
  uint64_t ninety_nine = 0;
  CHECK(chunked_buffer.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
  CHECK(ninety_nine == 99);

  // Overwrite the 100th element with value '900'.
  write_offset = 99 * sizeof(uint64_t);
  uint64_t nine_hundred = 900;
  CHECK(
      chunked_buffer.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

  // Read the 100th element, this will be of value '900'.
  read_offset = 99 * sizeof(uint64_t);
  nine_hundred = 0;
  CHECK(chunked_buffer.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
  CHECK(nine_hundred == 900);

  // Overwrite the 100th element back to value '99'.
  write_offset = 99 * sizeof(uint64_t);
  ninety_nine = 99;
  CHECK(
      chunked_buffer.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

  // Read the entire written buffer.
  read_offset = 0;
  CHECK(chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  // Free the chunk buffer, which will free all of the allocated
  // buffers and return chunked_buffer into an uninitialized state.
  chunked_buffer.free();
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == 0);

  // Reinitialize the chunk buffers.
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  CHECK(chunked_buffer.capacity() == buffer_size);
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == nchunks);

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    uint32_t internal_size = 0;
    CHECK(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    CHECK(internal_size == 0);

    uint32_t internal_capacity = 0;
    CHECK(chunked_buffer.internal_buffer_capacity(i, &internal_capacity).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      CHECK(internal_capacity == chunk_size);
    } else {
      CHECK(internal_capacity == last_chunk_size);
    }
  }

  // Allocate all chunks.
  std::vector<void*> internal_chunked_buffer(chunked_buffer.nchunks());
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    CHECK(chunked_buffer.alloc_discrete(i, &chunk_buffer).ok());
    CHECK(chunk_buffer);
    internal_chunked_buffer[i] = chunk_buffer;
  }

  // Verify all chunks are allocated and that they do not overlap
  // 'buffer' because they have been deep-copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    uint32_t internal_size = 0;
    CHECK(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    CHECK(internal_size == 0);

    uint32_t internal_capacity = 0;
    CHECK(chunked_buffer.internal_buffer_capacity(i, &internal_capacity).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      CHECK(internal_capacity == chunk_size);
    } else {
      CHECK(internal_capacity == last_chunk_size);
    }

    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(chunk_buffer);
    CHECK(chunk_buffer != write_buffer);
    if (chunk_buffer < write_buffer) {
      CHECK(
          static_cast<char*>(chunk_buffer) + chunk_size <=
          reinterpret_cast<char*>(write_buffer));
    } else {
      CHECK(
          reinterpret_cast<char*>(write_buffer) + buffer_size <=
          static_cast<char*>(chunk_buffer));
    }
  }

  // Write to all chunks.
  write_offset = 0;
  CHECK(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  // Read the entire written buffer.
  read_offset = 0;
  CHECK(chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  // Clear the ChunkedBuffer. This will reset to an uninitialized state
  // but will NOT free the underlying buffers.
  chunked_buffer.clear();
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == 0);

  // Free the internal buffers to prevent a memory leak.
  for (const auto& internal_buffer : internal_chunked_buffer) {
    free(internal_buffer);
  }
}

TEST_CASE(
    "ChunkedBuffer: Test contigious, fixed size IO",
    "[ChunkedBuffer][contigious_fixed_io]") {
  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Initialize the ChunkedBuffer.
  CHECK(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  CHECK(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  CHECK(chunk_size != last_chunk_size);
  chunked_buffer.init_fixed_size(
      ChunkedBuffer::BufferAddressing::CONTIGIOUS, buffer_size, chunk_size);
  CHECK(chunked_buffer.capacity() == buffer_size);
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == nchunks);

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    void* chunk_buffer;
    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(!chunk_buffer);
  }

  // Write the entire buffer. This will fail because a contigious buffer
  // has not been set or allocated.
  uint64_t write_offset = 0;
  CHECK(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  // Attempt to allocate a chunk. This will fail because contigious
  // ChunkedBuffer instances can not use the 'alloc_discrete' routine.
  CHECK(!chunked_buffer.alloc_discrete(chunked_buffer.nchunks() / 2).ok());

  // Set the contigious buffer.
  CHECK(chunked_buffer.set_contigious(write_buffer).ok());
  CHECK(chunked_buffer.set_size(buffer_size).ok());

  // Verify all chunks are allocated and that they're addressed match
  // 'write_buffer' to ensure 'write_buffer' was not copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    uint32_t internal_size = 0;
    CHECK(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    if (i < chunked_buffer.nchunks() - 1) {
      CHECK(internal_size == chunk_size);
    } else {
      CHECK(internal_size == last_chunk_size);
    }

    void* chunk_buffer = nullptr;
    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(chunk_buffer);
    CHECK(
        chunk_buffer ==
        reinterpret_cast<char*>(write_buffer) + (i * chunk_size));
  }

  // Read the third element, this will be of value '2'.
  uint64_t read_offset = 2 * sizeof(uint64_t);
  uint64_t two = 0;
  CHECK(chunked_buffer.read(&two, sizeof(uint64_t), read_offset).ok());
  CHECK(two == 2);

  // Read the 10th element, this will be of value '9'.
  read_offset = 9 * sizeof(uint64_t);
  uint64_t nine = 0;
  CHECK(chunked_buffer.read(&nine, sizeof(uint64_t), read_offset).ok());
  CHECK(nine == 9);

  // Read the 100th element, this will be of value '99'.
  read_offset = 99 * sizeof(uint64_t);
  uint64_t ninety_nine = 0;
  CHECK(chunked_buffer.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
  CHECK(ninety_nine == 99);

  // Overwrite the 100th element with value '900'.
  write_offset = 99 * sizeof(uint64_t);
  uint64_t nine_hundred = 900;
  CHECK(
      chunked_buffer.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

  // Read the 100th element, this will be of value '900'.
  read_offset = 99 * sizeof(uint64_t);
  nine_hundred = 0;
  CHECK(chunked_buffer.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
  CHECK(nine_hundred == 900);

  // Overwrite the 100th element back to value '99'.
  write_offset = 99 * sizeof(uint64_t);
  ninety_nine = 99;
  CHECK(
      chunked_buffer.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

  // Read the entire written buffer.
  uint64_t read_buffer[buffer_len];
  read_offset = 0;
  CHECK(chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  // Clear the chunk buffer. This will not free the underlying buffer.
  chunked_buffer.clear();
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == 0);
}

TEST_CASE(
    "ChunkedBuffer: Test discrete, variable sized IO",
    "[ChunkedBuffer][discrete_var_io]") {
  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Initialize the ChunkedBuffer with variable sized chunks.
  CHECK(buffer_size % sizeof(uint64_t) == 0);
  std::vector<uint32_t> var_chunk_sizes;
  uint64_t remaining_bytes = buffer_size;
  uint32_t chunk_size = sizeof(uint64_t);
  while (remaining_bytes > 0) {
    if (chunk_size > remaining_bytes) {
      chunk_size = remaining_bytes;
    }
    var_chunk_sizes.emplace_back(chunk_size);
    remaining_bytes -= chunk_size;
    chunk_size += sizeof(uint64_t);
  }
  chunked_buffer.init_var_size(
      ChunkedBuffer::BufferAddressing::DISCRETE,
      std::vector<uint32_t>(var_chunk_sizes));
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.capacity() == buffer_size);
  CHECK(chunked_buffer.nchunks() == var_chunk_sizes.size());

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    void* chunk_buffer;
    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(!chunk_buffer);
  }

  // Write the entire buffer. This will allocate all of the chunks.
  uint64_t write_offset = 0;
  CHECK(chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  // Verify all chunks are allocated and that they do not overlap
  // 'buffer' because they have been deep-copied.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    uint32_t internal_size = 0;
    CHECK(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    CHECK(internal_size == var_chunk_sizes[i]);

    void* chunk_buffer = nullptr;
    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(chunk_buffer);
    CHECK(chunk_buffer != write_buffer);
    if (chunk_buffer < write_buffer) {
      CHECK(
          static_cast<char*>(chunk_buffer) + internal_size <=
          reinterpret_cast<char*>(write_buffer));
    } else {
      CHECK(
          reinterpret_cast<char*>(write_buffer) + buffer_size <=
          static_cast<char*>(chunk_buffer));
    }
  }

  // Read the third element, this will be of value '2'.
  uint64_t read_offset = 2 * sizeof(uint64_t);
  uint64_t two = 0;
  CHECK(chunked_buffer.read(&two, sizeof(uint64_t), read_offset).ok());
  CHECK(two == 2);

  // Read the 10th element, this will be of value '9'.
  read_offset = 9 * sizeof(uint64_t);
  uint64_t nine = 0;
  CHECK(chunked_buffer.read(&nine, sizeof(uint64_t), read_offset).ok());
  CHECK(nine == 9);

  // Read the 100th element, this will be of value '99'.
  read_offset = 99 * sizeof(uint64_t);
  uint64_t ninety_nine = 0;
  CHECK(chunked_buffer.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
  CHECK(ninety_nine == 99);

  // Overwrite the 100th element with value '900'.
  write_offset = 99 * sizeof(uint64_t);
  uint64_t nine_hundred = 900;
  CHECK(
      chunked_buffer.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

  // Read the 100th element, this will be of value '900'.
  read_offset = 99 * sizeof(uint64_t);
  nine_hundred = 0;
  CHECK(chunked_buffer.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
  CHECK(nine_hundred == 900);

  // Overwrite the 100th element back to value '99'.
  write_offset = 99 * sizeof(uint64_t);
  ninety_nine = 99;
  CHECK(
      chunked_buffer.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

  // Read the entire written buffer.
  uint64_t read_buffer[buffer_len];
  read_offset = 0;
  CHECK(chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  // Free the chunk buffer, which will free all of the allocated
  // buffers and return chunked_buffer into an uninitialized state.
  chunked_buffer.free();
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == 0);
}

TEST_CASE(
    "ChunkedBuffer: Test contigious variable sized IO",
    "[ChunkedBuffer][contigious_var_io]") {
  // Instantiate a test ChunkedBuffer.
  ChunkedBuffer chunked_buffer;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Initialize the ChunkedBuffer with variable sized chunks.
  CHECK(buffer_size % sizeof(uint64_t) == 0);
  std::vector<uint32_t> var_chunk_sizes;
  uint64_t remaining_bytes = buffer_size;
  uint32_t chunk_size = sizeof(uint64_t);
  while (remaining_bytes > 0) {
    if (chunk_size > remaining_bytes) {
      chunk_size = remaining_bytes;
    }
    var_chunk_sizes.emplace_back(chunk_size);
    remaining_bytes -= chunk_size;
    chunk_size += sizeof(uint64_t);
  }
  chunked_buffer.init_var_size(
      ChunkedBuffer::BufferAddressing::CONTIGIOUS,
      std::vector<uint32_t>(var_chunk_sizes));
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.capacity() == buffer_size);
  CHECK(chunked_buffer.nchunks() == var_chunk_sizes.size());

  // Verify all chunks are unallocated.
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    void* chunk_buffer;
    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(!chunk_buffer);
  }

  // Write the entire buffer. This will fail because a contigious buffer
  // has not been set or allocated.
  uint64_t write_offset = 0;
  CHECK(!chunked_buffer.write(write_buffer, buffer_size, write_offset).ok());

  // Attempt to allocate a chunk. This will fail because contigious
  // ChunkedBuffer instances can not use the 'alloc_discrete' routine.
  CHECK(!chunked_buffer.alloc_discrete(chunked_buffer.nchunks() / 2).ok());

  // Set the contigious buffer.
  CHECK(chunked_buffer.set_contigious(write_buffer).ok());
  CHECK(chunked_buffer.set_size(buffer_size).ok());

  // Verify all chunks are allocated and that they're addressed match
  // 'write_buffer' to ensure 'write_buffer' was not copied.
  uint64_t offset = 0;
  for (size_t i = 0; i < chunked_buffer.nchunks(); ++i) {
    uint32_t internal_size = 0;
    CHECK(chunked_buffer.internal_buffer_size(i, &internal_size).ok());
    CHECK(internal_size == var_chunk_sizes[i]);

    void* chunk_buffer = nullptr;
    CHECK(chunked_buffer.internal_buffer(i, &chunk_buffer).ok());
    CHECK(chunk_buffer);
    CHECK(chunk_buffer == reinterpret_cast<char*>(write_buffer) + offset);
    offset += internal_size;
  }

  // Read the third element, this will be of value '2'.
  uint64_t read_offset = 2 * sizeof(uint64_t);
  uint64_t two = 0;
  CHECK(chunked_buffer.read(&two, sizeof(uint64_t), read_offset).ok());
  CHECK(two == 2);

  // Read the 10th element, this will be of value '9'.
  read_offset = 9 * sizeof(uint64_t);
  uint64_t nine = 0;
  CHECK(chunked_buffer.read(&nine, sizeof(uint64_t), read_offset).ok());
  CHECK(nine == 9);

  // Read the 100th element, this will be of value '99'.
  read_offset = 99 * sizeof(uint64_t);
  uint64_t ninety_nine = 0;
  CHECK(chunked_buffer.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
  CHECK(ninety_nine == 99);

  // Overwrite the 100th element with value '900'.
  write_offset = 99 * sizeof(uint64_t);
  uint64_t nine_hundred = 900;
  CHECK(
      chunked_buffer.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

  // Read the 100th element, this will be of value '900'.
  read_offset = 99 * sizeof(uint64_t);
  nine_hundred = 0;
  CHECK(chunked_buffer.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
  CHECK(nine_hundred == 900);

  // Overwrite the 100th element back to value '99'.
  write_offset = 99 * sizeof(uint64_t);
  ninety_nine = 99;
  CHECK(
      chunked_buffer.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

  // Read the entire written buffer.
  uint64_t read_buffer[buffer_len];
  read_offset = 0;
  CHECK(chunked_buffer.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  // Clear the chunk buffer. This will not free the underlying buffer.
  chunked_buffer.clear();
  CHECK(chunked_buffer.size() == 0);
  CHECK(chunked_buffer.nchunks() == 0);
}

TEST_CASE(
    "ChunkedBuffer: Test copy constructor",
    "[ChunkedBuffer][copy_constructor]") {
  // Instantiate the first test ChunkedBuffer.
  ChunkedBuffer chunked_buffer1;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Initialize the ChunkedBuffer.
  CHECK(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  CHECK(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  CHECK(chunk_size != last_chunk_size);
  chunked_buffer1.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  CHECK(chunked_buffer1.size() == 0);
  CHECK(chunked_buffer1.capacity() == buffer_size);
  CHECK(chunked_buffer1.nchunks() == nchunks);

  // Write the entire buffer. This will allocate all of the chunks.
  uint64_t write_offset = 0;
  CHECK(chunked_buffer1.write(write_buffer, buffer_size, write_offset).ok());

  // Instantiate a second test ChunkedBuffer with the copy constructor.
  ChunkedBuffer chunked_buffer2(chunked_buffer1);

  // Verify all public attributes are identical.
  CHECK(chunked_buffer2.nchunks() == chunked_buffer1.nchunks());
  CHECK(
      chunked_buffer2.buffer_addressing() ==
      chunked_buffer1.buffer_addressing());
  CHECK(chunked_buffer2.capacity() == chunked_buffer1.capacity());
  CHECK(chunked_buffer2.size() == chunked_buffer1.size());

  // Read the entire written buffer from the second instance and verify
  // the contents match the first instance.
  uint64_t read_buffer[buffer_len];
  uint64_t read_offset = 0;
  CHECK(chunked_buffer2.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  // Ensure the internal data was deep-copied:
  void* chunked_buffer1_chunk_0;
  void* chunked_buffer2_chunk_0;
  CHECK(chunked_buffer1.internal_buffer(0, &chunked_buffer1_chunk_0).ok());
  CHECK(chunked_buffer2.internal_buffer(0, &chunked_buffer2_chunk_0).ok());
  CHECK(chunked_buffer1_chunk_0 != chunked_buffer2_chunk_0);
}

TEST_CASE("ChunkedBuffer: Test assignment", "[ChunkedBuffer][assignment]") {
  // Instantiate the first test ChunkedBuffer.
  ChunkedBuffer chunked_buffer1;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Initialize the ChunkedBuffer.
  CHECK(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  CHECK(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  CHECK(chunk_size != last_chunk_size);
  chunked_buffer1.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  CHECK(chunked_buffer1.size() == 0);
  CHECK(chunked_buffer1.capacity() == buffer_size);
  CHECK(chunked_buffer1.nchunks() == nchunks);

  // Write the entire buffer. This will allocate all of the chunks.
  uint64_t write_offset = 0;
  CHECK(chunked_buffer1.write(write_buffer, buffer_size, write_offset).ok());

  // Instantiate a second test ChunkedBuffer with the assignment operator.
  ChunkedBuffer chunked_buffer2 = chunked_buffer1;

  // Verify all public attributes are identical.
  CHECK(chunked_buffer2.nchunks() == chunked_buffer1.nchunks());
  CHECK(
      chunked_buffer2.buffer_addressing() ==
      chunked_buffer1.buffer_addressing());
  CHECK(chunked_buffer2.capacity() == chunked_buffer1.capacity());
  CHECK(chunked_buffer2.size() == chunked_buffer1.size());

  // Read the entire written buffer from the second instance and verify
  // the contents match the first instance.
  uint64_t read_buffer[buffer_len];
  uint64_t read_offset = 0;
  CHECK(chunked_buffer2.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  // Ensure the internal data was deep-copied:
  void* chunked_buffer1_chunk_0;
  void* chunked_buffer2_chunk_0;
  CHECK(chunked_buffer1.internal_buffer(0, &chunked_buffer1_chunk_0).ok());
  CHECK(chunked_buffer2.internal_buffer(0, &chunked_buffer2_chunk_0).ok());
  CHECK(chunked_buffer1_chunk_0 != chunked_buffer2_chunk_0);
}

TEST_CASE("ChunkedBuffer: Test shallow copy", "[ChunkedBuffer][shallow_copy]") {
  // Instantiate the first test ChunkedBuffer.
  ChunkedBuffer chunked_buffer1;

  // Create a buffer to write to the test ChunkedBuffer.
  const int64_t buffer_size = 1024 * 1024 * 3;
  uint64_t* const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
  const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Initialize the ChunkedBuffer.
  CHECK(buffer_size % sizeof(uint64_t) == 0);
  const size_t chunk_size = 1024 * 100;
  CHECK(buffer_size % chunk_size != 0);
  const size_t nchunks = (buffer_size / chunk_size) + 1;
  const size_t last_chunk_size = buffer_size % chunk_size;
  CHECK(chunk_size != last_chunk_size);
  chunked_buffer1.init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, buffer_size, chunk_size);
  CHECK(chunked_buffer1.size() == 0);
  CHECK(chunked_buffer1.capacity() == buffer_size);
  CHECK(chunked_buffer1.nchunks() == nchunks);

  // Write the entire buffer. This will allocate all of the chunks.
  uint64_t write_offset = 0;
  CHECK(chunked_buffer1.write(write_buffer, buffer_size, write_offset).ok());

  // Instantiate a second test ChunkedBuffer with the shallow_copy routine.
  ChunkedBuffer chunked_buffer2 = chunked_buffer1.shallow_copy();

  // Verify all public attributes are identical.
  CHECK(chunked_buffer2.nchunks() == chunked_buffer1.nchunks());
  CHECK(
      chunked_buffer2.buffer_addressing() ==
      chunked_buffer1.buffer_addressing());
  CHECK(chunked_buffer2.capacity() == chunked_buffer1.capacity());
  CHECK(chunked_buffer2.size() == chunked_buffer1.size());

  // Read the entire written buffer from the second instance and verify
  // the contents match the first instance.
  uint64_t read_buffer[buffer_len];
  uint64_t read_offset = 0;
  CHECK(chunked_buffer2.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

  // Ensure the internal data was shallow-copied:
  void* chunked_buffer1_chunk_0;
  void* chunked_buffer2_chunk_0;
  CHECK(chunked_buffer1.internal_buffer(0, &chunked_buffer1_chunk_0).ok());
  CHECK(chunked_buffer2.internal_buffer(0, &chunked_buffer2_chunk_0).ok());
  CHECK(chunked_buffer1_chunk_0 == chunked_buffer2_chunk_0);
}