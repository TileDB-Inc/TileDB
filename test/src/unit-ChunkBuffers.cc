/**
 * @file unit-ChunkBuffers.cc
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
 * Tests the `ChunkBuffers` class.
 */

#include "tiledb/sm/tile/chunk_buffers.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("ChunkBuffers: Test default constructor", "[ChunkBuffers][default_constructor]") {
	// Test the default constructor and verify the empty state.
	ChunkBuffers chunk_buffers;
	CHECK(chunk_buffers.size() == 0);
	CHECK(chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == 0);
	void *buffer = nullptr;
	CHECK(!chunk_buffers.internal_buffer(0, &buffer).ok());
	CHECK(!buffer);
}

TEST_CASE("ChunkBuffers: Test discrete, fixed size IO", "[ChunkBuffers][discrete_fixed_io]") {
	// Instantiate a test ChunkBuffers.
	ChunkBuffers chunk_buffers;

	// Create a buffer to write to the test ChunkBuffers.
	const int64_t buffer_size = 1024 * 1024 * 3;
	uint64_t *const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
	const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
	for (uint64_t i = 0; i < buffer_len; ++i) {
		write_buffer[i] = i;
	}

	// Attempt a write before initializing the test ChunkBuffers.
	uint64_t write_offset = 0;
	CHECK(!chunk_buffers.write(write_buffer, buffer_size, write_offset).ok());

	// Attempt a read before initializing the test ChunkBuffers.
	uint64_t read_offset = 0;
	uint64_t read_buffer[buffer_len];
	CHECK(!chunk_buffers.read(read_buffer, buffer_size, read_offset).ok());

	// Attempt an alloc before initializing the test ChunkBuffers.
	size_t chunk_idx = 0;
	void *chunk_buffer = nullptr;
	CHECK(!chunk_buffers.alloc_discrete(chunk_idx, &chunk_buffer).ok());
	CHECK(!chunk_buffer);

	// Attempt a set before initializing the test ChunkBuffers.
	chunk_buffer = nullptr;
	CHECK(!chunk_buffers.set_contigious(chunk_buffer).ok());
	CHECK(!chunk_buffer);

	// Initialize the ChunkBuffers.
	CHECK(buffer_size % sizeof(uint64_t) == 0);
	const size_t chunk_size = 1024 * 100;
	CHECK(buffer_size % chunk_size != 0);
	const size_t nchunks = (buffer_size / chunk_size) + 1;
	const size_t last_chunk_size = buffer_size % chunk_size;
	CHECK(chunk_size != last_chunk_size);
	chunk_buffers.init_fixed_size(false, buffer_size, chunk_size);
	CHECK(chunk_buffers.size() == buffer_size);
	CHECK(!chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == nchunks);

	// Verify all chunks are unallocated.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		void *chunk_buffer;
		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(!chunk_buffer);
	}

	// Write the entire buffer. This will allocate all of the chunks.
	write_offset = 0;
	CHECK(chunk_buffers.write(write_buffer, buffer_size, write_offset).ok());

	// Verify all chunks are allocated and that they do not overlap
	// 'buffer' because they have been deep-copied.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		uint32_t internal_size = 0;
		CHECK(chunk_buffers.internal_buffer_size(i, &internal_size).ok());
		if (i < chunk_buffers.nchunks() - 1) {
			CHECK(internal_size == chunk_size);
		} else {
			CHECK(internal_size == last_chunk_size);
		}

		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(chunk_buffer);
		CHECK(chunk_buffer != write_buffer);
		if (chunk_buffer < write_buffer) {
			CHECK(
				static_cast<char*>(chunk_buffer) + chunk_size <=
					reinterpret_cast<char *>(write_buffer));
		} else {
			CHECK(
				reinterpret_cast<char *>(write_buffer) + buffer_size <=
					static_cast<char*>(chunk_buffer));
		}
	}
	// Read the third element, this will be of value '2'.
	read_offset = 2 * sizeof(uint64_t);
	uint64_t two = 0;
	CHECK(chunk_buffers.read(&two, sizeof(uint64_t), read_offset).ok());
	CHECK(two == 2);

	// Read the 10th element, this will be of value '9'.
	read_offset = 9 * sizeof(uint64_t);
	uint64_t nine = 0;
	CHECK(chunk_buffers.read(&nine, sizeof(uint64_t), read_offset).ok());
	CHECK(nine == 9);

	// Read the 100th element, this will be of value '99'.
	read_offset = 99 * sizeof(uint64_t);
	uint64_t ninety_nine = 0;
	CHECK(chunk_buffers.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
	CHECK(ninety_nine == 99);

	// Overwrite the 100th element with value '900'.
	write_offset = 99 * sizeof(uint64_t);
	uint64_t nine_hundred = 900;
	CHECK(chunk_buffers.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

	// Read the 100th element, this will be of value '900'.
	read_offset = 99 * sizeof(uint64_t);
	nine_hundred = 0;
	CHECK(chunk_buffers.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
	CHECK(nine_hundred == 900);

	// Overwrite the 100th element back to value '99'.
	write_offset = 99 * sizeof(uint64_t);
	ninety_nine = 99;
	CHECK(chunk_buffers.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

	// Read the entire written buffer.
	read_offset = 0;
	CHECK(chunk_buffers.read(read_buffer, buffer_size, read_offset).ok());
	CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

	// Free the chunk buffer, which will free all of the allocated
	// buffers and return chunk_buffers into an uninitialized state.
	chunk_buffers.free();
	CHECK(chunk_buffers.size() == 0);
	CHECK(chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == 0);

	// Reinitialize the chunk buffers.
	chunk_buffers.init_fixed_size(false, buffer_size, chunk_size);
	CHECK(chunk_buffers.size() == buffer_size);
	CHECK(!chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == nchunks);

	// Verify all chunks are unallocated.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		uint32_t internal_size = 0;
		CHECK(chunk_buffers.internal_buffer_size(i, &internal_size).ok());
		if (i < chunk_buffers.nchunks() - 1) {
			CHECK(internal_size == chunk_size);
		} else {
			CHECK(internal_size == last_chunk_size);
		}
	}

	// Allocate all chunks.
	std::vector<void*> internal_chunk_buffers(chunk_buffers.nchunks());
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		CHECK(chunk_buffers.alloc_discrete(i, &chunk_buffer).ok());
		CHECK(chunk_buffer);
		internal_chunk_buffers[i] = chunk_buffer;
	}

	// Verify all chunks are allocated and that they do not overlap
	// 'buffer' because they have been deep-copied.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		uint32_t internal_size = 0;
		CHECK(chunk_buffers.internal_buffer_size(i, &internal_size).ok());
		if (i < chunk_buffers.nchunks() - 1) {
			CHECK(internal_size == chunk_size);
		} else {
			CHECK(internal_size == last_chunk_size);
		}

		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(chunk_buffer);
		CHECK(chunk_buffer != write_buffer);
		if (chunk_buffer < write_buffer) {
			CHECK(
				static_cast<char*>(chunk_buffer) + chunk_size <=
					reinterpret_cast<char *>(write_buffer));
		} else {
			CHECK(
				reinterpret_cast<char *>(write_buffer) + buffer_size <=
					static_cast<char*>(chunk_buffer));
		}
	}

	// Write to all chunks.
	write_offset = 0;
	CHECK(chunk_buffers.write(write_buffer, buffer_size, write_offset).ok());

	// Read the entire written buffer.
	read_offset = 0;
	CHECK(chunk_buffers.read(read_buffer, buffer_size, read_offset).ok());
	CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

	// Clear the ChunkBuffers. This will reset to an uninitialized state
	// but will NOT free the underlying buffers.
	chunk_buffers.clear();
	CHECK(chunk_buffers.size() == 0);
	CHECK(chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == 0);

	// Free the internal buffers to prevent a memory leak.
	for (const auto& internal_buffer : internal_chunk_buffers) {
		free(internal_buffer);
	}
}

TEST_CASE("ChunkBuffers: Test contigious, fixed size IO", "[ChunkBuffers][contigious_fixed_io]") {
	// Instantiate a test ChunkBuffers.
	ChunkBuffers chunk_buffers;

	// Create a buffer to write to the test ChunkBuffers.
	const int64_t buffer_size = 1024 * 1024 * 3;
	uint64_t *const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
	const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
	for (uint64_t i = 0; i < buffer_len; ++i) {
		write_buffer[i] = i;
	}

	// Initialize the ChunkBuffers.
	CHECK(buffer_size % sizeof(uint64_t) == 0);
	const size_t chunk_size = 1024 * 100;
	CHECK(buffer_size % chunk_size != 0);
	const size_t nchunks = (buffer_size / chunk_size) + 1;
	const size_t last_chunk_size = buffer_size % chunk_size;
	CHECK(chunk_size != last_chunk_size);
	chunk_buffers.init_fixed_size(true, buffer_size, chunk_size);
	CHECK(chunk_buffers.size() == buffer_size);
	CHECK(!chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == nchunks);

	// Verify all chunks are unallocated.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		void *chunk_buffer;
		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(!chunk_buffer);
	}

	// Write the entire buffer. This will fail because a contigious buffer
	// has not been set or allocated.
	uint64_t write_offset = 0;
	CHECK(!chunk_buffers.write(write_buffer, buffer_size, write_offset).ok());

	// Attempt to allocate a chunk. This will fail because contigious
	// ChunkBuffers instances can not use the 'alloc_discrete' routine.
	CHECK(!chunk_buffers.alloc_discrete(chunk_buffers.nchunks() / 2).ok());

	// Set the contigious buffer.
	CHECK(chunk_buffers.set_contigious(write_buffer).ok());

	// Verify all chunks are allocated and that they're addressed match
	// 'write_buffer' to ensure 'write_buffer' was not copied.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		uint32_t internal_size = 0;
		CHECK(chunk_buffers.internal_buffer_size(i, &internal_size).ok());
		if (i < chunk_buffers.nchunks() - 1) {
			CHECK(internal_size == chunk_size);
		} else {
			CHECK(internal_size == last_chunk_size);
		}

		void *chunk_buffer = nullptr;
		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(chunk_buffer);
		CHECK(chunk_buffer == reinterpret_cast<char*>(write_buffer) + (i * chunk_size));
	}

	// Read the third element, this will be of value '2'.
	uint64_t read_offset = 2 * sizeof(uint64_t);
	uint64_t two = 0;
	CHECK(chunk_buffers.read(&two, sizeof(uint64_t), read_offset).ok());
	CHECK(two == 2);

	// Read the 10th element, this will be of value '9'.
	read_offset = 9 * sizeof(uint64_t);
	uint64_t nine = 0;
	CHECK(chunk_buffers.read(&nine, sizeof(uint64_t), read_offset).ok());
	CHECK(nine == 9);

	// Read the 100th element, this will be of value '99'.
	read_offset = 99 * sizeof(uint64_t);
	uint64_t ninety_nine = 0;
	CHECK(chunk_buffers.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
	CHECK(ninety_nine == 99);

	// Overwrite the 100th element with value '900'.
	write_offset = 99 * sizeof(uint64_t);
	uint64_t nine_hundred = 900;
	CHECK(chunk_buffers.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

	// Read the 100th element, this will be of value '900'.
	read_offset = 99 * sizeof(uint64_t);
	nine_hundred = 0;
	CHECK(chunk_buffers.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
	CHECK(nine_hundred == 900);

	// Overwrite the 100th element back to value '99'.
	write_offset = 99 * sizeof(uint64_t);
	ninety_nine = 99;
	CHECK(chunk_buffers.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

	// Read the entire written buffer.
	uint64_t read_buffer[buffer_len];
	read_offset = 0;
	CHECK(chunk_buffers.read(read_buffer, buffer_size, read_offset).ok());
	CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

	// Clear the chunk buffer. This will not free the underlying buffer.
	chunk_buffers.clear();
	CHECK(chunk_buffers.size() == 0);
	CHECK(chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == 0);
}

TEST_CASE("ChunkBuffers: Test discrete, variable sized IO", "[ChunkBuffers][discrete_var_io]") {
	// Instantiate a test ChunkBuffers.
	ChunkBuffers chunk_buffers;

	// Create a buffer to write to the test ChunkBuffers.
	const int64_t buffer_size = 1024 * 1024 * 3;
	uint64_t *const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
	const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
	for (uint64_t i = 0; i < buffer_len; ++i) {
		write_buffer[i] = i;
	}

	// Initialize the ChunkBuffers with variable sized chunks.
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
	chunk_buffers.init_var_size(false, std::vector<uint32_t>(var_chunk_sizes));
	CHECK(chunk_buffers.size() == buffer_size);
	CHECK(!chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == var_chunk_sizes.size());

	// Verify all chunks are unallocated.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		void *chunk_buffer;
		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(!chunk_buffer);
	}

	// Write the entire buffer. This will allocate all of the chunks.
	uint64_t write_offset = 0;
	CHECK(chunk_buffers.write(write_buffer, buffer_size, write_offset).ok());

	// Verify all chunks are allocated and that they do not overlap
	// 'buffer' because they have been deep-copied.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		uint32_t internal_size = 0;
		CHECK(chunk_buffers.internal_buffer_size(i, &internal_size).ok());
		CHECK(internal_size == var_chunk_sizes[i]);

		void *chunk_buffer = nullptr;
		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(chunk_buffer);
		CHECK(chunk_buffer != write_buffer);
		if (chunk_buffer < write_buffer) {
			CHECK(
				static_cast<char*>(chunk_buffer) + internal_size <=
					reinterpret_cast<char *>(write_buffer));
		} else {
			CHECK(
				reinterpret_cast<char *>(write_buffer) + buffer_size <=
					static_cast<char*>(chunk_buffer));
		}
	}

	// Read the third element, this will be of value '2'.
	uint64_t read_offset = 2 * sizeof(uint64_t);
	uint64_t two = 0;
	CHECK(chunk_buffers.read(&two, sizeof(uint64_t), read_offset).ok());
	CHECK(two == 2);

	// Read the 10th element, this will be of value '9'.
	read_offset = 9 * sizeof(uint64_t);
	uint64_t nine = 0;
	CHECK(chunk_buffers.read(&nine, sizeof(uint64_t), read_offset).ok());
	CHECK(nine == 9);

	// Read the 100th element, this will be of value '99'.
	read_offset = 99 * sizeof(uint64_t);
	uint64_t ninety_nine = 0;
	CHECK(chunk_buffers.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
	CHECK(ninety_nine == 99);

	// Overwrite the 100th element with value '900'.
	write_offset = 99 * sizeof(uint64_t);
	uint64_t nine_hundred = 900;
	CHECK(chunk_buffers.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

	// Read the 100th element, this will be of value '900'.
	read_offset = 99 * sizeof(uint64_t);
	nine_hundred = 0;
	CHECK(chunk_buffers.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
	CHECK(nine_hundred == 900);

	// Overwrite the 100th element back to value '99'.
	write_offset = 99 * sizeof(uint64_t);
	ninety_nine = 99;
	CHECK(chunk_buffers.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

	// Read the entire written buffer.
	uint64_t read_buffer[buffer_len];
	read_offset = 0;
	CHECK(chunk_buffers.read(read_buffer, buffer_size, read_offset).ok());
	CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

	// Free the chunk buffer, which will free all of the allocated
	// buffers and return chunk_buffers into an uninitialized state.
	chunk_buffers.free();
	CHECK(chunk_buffers.size() == 0);
	CHECK(chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == 0);
}

TEST_CASE("ChunkBuffers: Test contigious variable sized IO", "[ChunkBuffers][contigious_var_io]") {
	// Instantiate a test ChunkBuffers.
	ChunkBuffers chunk_buffers;

	// Create a buffer to write to the test ChunkBuffers.
	const int64_t buffer_size = 1024 * 1024 * 3;
	uint64_t *const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
	const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
	for (uint64_t i = 0; i < buffer_len; ++i) {
		write_buffer[i] = i;
	}

	// Initialize the ChunkBuffers with variable sized chunks.
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
	chunk_buffers.init_var_size(true, std::vector<uint32_t>(var_chunk_sizes));
	CHECK(chunk_buffers.size() == buffer_size);
	CHECK(!chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == var_chunk_sizes.size());

	// Verify all chunks are unallocated.
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		void *chunk_buffer;
		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(!chunk_buffer);
	}

	// Write the entire buffer. This will fail because a contigious buffer
	// has not been set or allocated.
	uint64_t write_offset = 0;
	CHECK(!chunk_buffers.write(write_buffer, buffer_size, write_offset).ok());

	// Attempt to allocate a chunk. This will fail because contigious
	// ChunkBuffers instances can not use the 'alloc_discrete' routine.
	CHECK(!chunk_buffers.alloc_discrete(chunk_buffers.nchunks() / 2).ok());

	// Set the contigious buffer.
	CHECK(chunk_buffers.set_contigious(write_buffer).ok());

	// Verify all chunks are allocated and that they're addressed match
	// 'write_buffer' to ensure 'write_buffer' was not copied.
	uint64_t offset = 0;
	for (size_t i = 0; i < chunk_buffers.nchunks(); ++i) {
		uint32_t internal_size = 0;
		CHECK(chunk_buffers.internal_buffer_size(i, &internal_size).ok());
		CHECK(internal_size == var_chunk_sizes[i]);

		void *chunk_buffer = nullptr;
		CHECK(chunk_buffers.internal_buffer(i, &chunk_buffer).ok());
		CHECK(chunk_buffer);
		CHECK(chunk_buffer == reinterpret_cast<char*>(write_buffer) + offset);
		offset += internal_size;
	}

	// Read the third element, this will be of value '2'.
	uint64_t read_offset = 2 * sizeof(uint64_t);
	uint64_t two = 0;
	CHECK(chunk_buffers.read(&two, sizeof(uint64_t), read_offset).ok());
	CHECK(two == 2);

	// Read the 10th element, this will be of value '9'.
	read_offset = 9 * sizeof(uint64_t);
	uint64_t nine = 0;
	CHECK(chunk_buffers.read(&nine, sizeof(uint64_t), read_offset).ok());
	CHECK(nine == 9);

	// Read the 100th element, this will be of value '99'.
	read_offset = 99 * sizeof(uint64_t);
	uint64_t ninety_nine = 0;
	CHECK(chunk_buffers.read(&ninety_nine, sizeof(uint64_t), read_offset).ok());
	CHECK(ninety_nine == 99);

	// Overwrite the 100th element with value '900'.
	write_offset = 99 * sizeof(uint64_t);
	uint64_t nine_hundred = 900;
	CHECK(chunk_buffers.write(&nine_hundred, sizeof(uint64_t), write_offset).ok());

	// Read the 100th element, this will be of value '900'.
	read_offset = 99 * sizeof(uint64_t);
	nine_hundred = 0;
	CHECK(chunk_buffers.read(&nine_hundred, sizeof(uint64_t), read_offset).ok());
	CHECK(nine_hundred == 900);

	// Overwrite the 100th element back to value '99'.
	write_offset = 99 * sizeof(uint64_t);
	ninety_nine = 99;
	CHECK(chunk_buffers.write(&ninety_nine, sizeof(uint64_t), write_offset).ok());

	// Read the entire written buffer.
	uint64_t read_buffer[buffer_len];
	read_offset = 0;
	CHECK(chunk_buffers.read(read_buffer, buffer_size, read_offset).ok());
	CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

	// Clear the chunk buffer. This will not free the underlying buffer.
	chunk_buffers.clear();
	CHECK(chunk_buffers.size() == 0);
	CHECK(chunk_buffers.empty());
	CHECK(chunk_buffers.nchunks() == 0);
}

TEST_CASE("ChunkBuffers: Test copy constructor", "[ChunkBuffers][copy_constructor]") {
	// Instantiate the first test ChunkBuffers.
	ChunkBuffers chunk_buffers1;

	// Create a buffer to write to the test ChunkBuffers.
	const int64_t buffer_size = 1024 * 1024 * 3;
	uint64_t *const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
	const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
	for (uint64_t i = 0; i < buffer_len; ++i) {
		write_buffer[i] = i;
	}

	// Initialize the ChunkBuffers.
	CHECK(buffer_size % sizeof(uint64_t) == 0);
	const size_t chunk_size = 1024 * 100;
	CHECK(buffer_size % chunk_size != 0);
	const size_t nchunks = (buffer_size / chunk_size) + 1;
	const size_t last_chunk_size = buffer_size % chunk_size;
	CHECK(chunk_size != last_chunk_size);
	chunk_buffers1.init_fixed_size(false, buffer_size, chunk_size);
	CHECK(chunk_buffers1.size() == buffer_size);
	CHECK(!chunk_buffers1.empty());
	CHECK(chunk_buffers1.nchunks() == nchunks);

	// Write the entire buffer. This will allocate all of the chunks.
	uint64_t write_offset = 0;
	CHECK(chunk_buffers1.write(write_buffer, buffer_size, write_offset).ok());

	// Instantiate a second test ChunkBuffers with the copy constructor.
	ChunkBuffers chunk_buffers2(chunk_buffers1);

	// Verify all public attributes are identical.
	CHECK(chunk_buffers2.nchunks() == chunk_buffers1.nchunks());
	CHECK(chunk_buffers2.contigious() == chunk_buffers1.contigious());

	// Read the entire written buffer from the second instance and verify
	// the contents match the first instance.
	uint64_t read_buffer[buffer_len];
	uint64_t read_offset = 0;
	CHECK(chunk_buffers2.read(read_buffer, buffer_size, read_offset).ok());
	CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

	// Ensure the internal data was deep-copied:
	void *chunk_buffers1_chunk_0;
	void *chunk_buffers2_chunk_0;
	CHECK(chunk_buffers1.internal_buffer(0, &chunk_buffers1_chunk_0).ok());
	CHECK(chunk_buffers2.internal_buffer(0, &chunk_buffers2_chunk_0).ok());
	CHECK(chunk_buffers1_chunk_0 != chunk_buffers2_chunk_0);
}

TEST_CASE("ChunkBuffers: Test assignment", "[ChunkBuffers][assignment]") {
	// Instantiate the first test ChunkBuffers.
	ChunkBuffers chunk_buffers1;

	// Create a buffer to write to the test ChunkBuffers.
	const int64_t buffer_size = 1024 * 1024 * 3;
	uint64_t *const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
	const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
	for (uint64_t i = 0; i < buffer_len; ++i) {
		write_buffer[i] = i;
	}

	// Initialize the ChunkBuffers.
	CHECK(buffer_size % sizeof(uint64_t) == 0);
	const size_t chunk_size = 1024 * 100;
	CHECK(buffer_size % chunk_size != 0);
	const size_t nchunks = (buffer_size / chunk_size) + 1;
	const size_t last_chunk_size = buffer_size % chunk_size;
	CHECK(chunk_size != last_chunk_size);
	chunk_buffers1.init_fixed_size(false, buffer_size, chunk_size);
	CHECK(chunk_buffers1.size() == buffer_size);
	CHECK(!chunk_buffers1.empty());
	CHECK(chunk_buffers1.nchunks() == nchunks);

	// Write the entire buffer. This will allocate all of the chunks.
	uint64_t write_offset = 0;
	CHECK(chunk_buffers1.write(write_buffer, buffer_size, write_offset).ok());

	// Instantiate a second test ChunkBuffers with the assignment operator.
	ChunkBuffers chunk_buffers2 = chunk_buffers1;

	// Verify all public attributes are identical.
	CHECK(chunk_buffers2.nchunks() == chunk_buffers1.nchunks());
	CHECK(chunk_buffers2.contigious() == chunk_buffers1.contigious());

	// Read the entire written buffer from the second instance and verify
	// the contents match the first instance.
	uint64_t read_buffer[buffer_len];
	uint64_t read_offset = 0;
	CHECK(chunk_buffers2.read(read_buffer, buffer_size, read_offset).ok());
	CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

	// Ensure the internal data was deep-copied:
	void *chunk_buffers1_chunk_0;
	void *chunk_buffers2_chunk_0;
	CHECK(chunk_buffers1.internal_buffer(0, &chunk_buffers1_chunk_0).ok());
	CHECK(chunk_buffers2.internal_buffer(0, &chunk_buffers2_chunk_0).ok());
	CHECK(chunk_buffers1_chunk_0 != chunk_buffers2_chunk_0);
}

TEST_CASE("ChunkBuffers: Test shallow copy", "[ChunkBuffers][shallow_copy]") {
	// Instantiate the first test ChunkBuffers.
	ChunkBuffers chunk_buffers1;

	// Create a buffer to write to the test ChunkBuffers.
	const int64_t buffer_size = 1024 * 1024 * 3;
	uint64_t *const write_buffer = static_cast<uint64_t*>(malloc(buffer_size));
	const uint64_t buffer_len = buffer_size / sizeof(uint64_t);
	for (uint64_t i = 0; i < buffer_len; ++i) {
		write_buffer[i] = i;
	}

	// Initialize the ChunkBuffers.
	CHECK(buffer_size % sizeof(uint64_t) == 0);
	const size_t chunk_size = 1024 * 100;
	CHECK(buffer_size % chunk_size != 0);
	const size_t nchunks = (buffer_size / chunk_size) + 1;
	const size_t last_chunk_size = buffer_size % chunk_size;
	CHECK(chunk_size != last_chunk_size);
	chunk_buffers1.init_fixed_size(false, buffer_size, chunk_size);
	CHECK(chunk_buffers1.size() == buffer_size);
	CHECK(!chunk_buffers1.empty());
	CHECK(chunk_buffers1.nchunks() == nchunks);

	// Write the entire buffer. This will allocate all of the chunks.
	uint64_t write_offset = 0;
	CHECK(chunk_buffers1.write(write_buffer, buffer_size, write_offset).ok());

	// Instantiate a second test ChunkBuffers with the shallow_copy routine.
	ChunkBuffers chunk_buffers2 = chunk_buffers1.shallow_copy();

	// Verify all public attributes are identical.
	CHECK(chunk_buffers2.nchunks() == chunk_buffers1.nchunks());
	CHECK(chunk_buffers2.contigious() == chunk_buffers1.contigious());

	// Read the entire written buffer from the second instance and verify
	// the contents match the first instance.
	uint64_t read_buffer[buffer_len];
	uint64_t read_offset = 0;
	CHECK(chunk_buffers2.read(read_buffer, buffer_size, read_offset).ok());
	CHECK(memcmp(read_buffer, write_buffer, buffer_size) == 0);

	// Ensure the internal data was shallow-copied:
	void *chunk_buffers1_chunk_0;
	void *chunk_buffers2_chunk_0;
	CHECK(chunk_buffers1.internal_buffer(0, &chunk_buffers1_chunk_0).ok());
	CHECK(chunk_buffers2.internal_buffer(0, &chunk_buffers2_chunk_0).ok());
	CHECK(chunk_buffers1_chunk_0 == chunk_buffers2_chunk_0);
}