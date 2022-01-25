/**
 * @file unit-Tile.cc
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
 * Tests the `Tile` class.
 */

#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/tile/tile.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("Tile: Test basic IO", "[Tile][basic_io]") {
  // Instantiate the test Tile.
  Tile tile;
  CHECK(tile.empty());
  CHECK(!tile.full());
  CHECK(tile.size() == 0);

  // Initialize the test Tile.
  const uint32_t format_version = 0;
  const Datatype data_type = Datatype::UINT32;
  const uint64_t tile_size = 1024 * 1024;
  const uint64_t cell_size = sizeof(uint32_t);
  const unsigned int dim_num = 1;
  CHECK(tile.init_unfiltered(
                format_version, data_type, tile_size, cell_size, dim_num)
            .ok());
  CHECK(tile.empty());
  CHECK(!tile.full());
  CHECK(tile.size() == 0);
  CHECK(tile.buffer()->alloced_size() == tile_size);
  CHECK(tile.owns_buff());

  // Create a buffer to write to the test Tile.
  uint32_t* const write_buffer = static_cast<uint32_t*>(malloc(tile_size));
  const uint32_t buffer_len = tile_size / sizeof(uint32_t);
  for (uint32_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Write the buffer to the test Tile.
  CHECK(tile.offset() == 0);
  CHECK(tile.write(write_buffer, tile_size).ok());
  CHECK(tile.offset() == tile_size);
  CHECK(!tile.empty());
  CHECK(tile.full());
  CHECK(tile.size() == tile_size);

  // Ensure the internal data was deep-copied:
  void* tile_chunk_0 = tile.buffer()->data();
  CHECK(tile_chunk_0 != static_cast<void*>(write_buffer));

  // Test a partial read at offset 8, which should be a uint32_t with
  // a value of two.
  uint32_t two = 0;
  CHECK(tile.read(&two, sizeof(uint32_t), 8).ok());
  CHECK(two == 2);

  // The last read should have not changed the internal offset.
  CHECK(tile.offset() == tile_size);

  // Perform a read at the current offset and verify we read 'two'
  // again and that the offset has changed.
  tile.set_offset(8);
  two = 0;
  CHECK(tile.read(&two, sizeof(uint32_t)).ok());
  CHECK(two == 2);
  CHECK(tile.offset() == 12);

  // Test a full read.
  uint32_t* const read_buffer = static_cast<uint32_t*>(malloc(tile_size));
  memset(read_buffer, 0, tile_size);
  uint64_t read_offset = 0;
  CHECK(tile.read(read_buffer, tile_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer, tile_size) == 0);

  // Verify the internal offset did not change.
  CHECK(tile.offset() == 12);

  // Test a write at a non-zero offset. Overwrite the two at offset 8.
  tile.reset_offset();
  tile.advance_offset(8);
  uint32_t magic = 5234549;
  CHECK(tile.write(&magic, sizeof(uint32_t)).ok());
  CHECK(tile.offset() == 12);

  // Read the magic number to ensure the '2' value was overwritten.
  two = 0;
  CHECK(tile.read(&two, sizeof(uint32_t), 8).ok());
  CHECK(two == magic);

  // Restore the state without the magic number.
  tile.set_offset(8);
  two = 2;
  CHECK(tile.write(&two, sizeof(uint32_t)).ok());
  CHECK(tile.offset() == 12);

  // Test a read at an out-of-bounds offset.
  memset(read_buffer, 0, tile_size);
  read_offset = tile_size;
  CHECK(!tile.read(read_buffer, tile_size, read_offset).ok());

  // Test a read at a valid offset but with a size that
  // exceeds the written buffer size.
  const uint32_t large_buffer_size = tile_size * 2;
  uint32_t* const large_read_buffer =
      static_cast<uint32_t*>(malloc(large_buffer_size));
  memset(large_read_buffer, 0, large_buffer_size);
  read_offset = 0;
  CHECK(!tile.read(large_read_buffer, large_buffer_size, read_offset).ok());

  // Free the write buffer to ensure that it was deep-copied
  // within the initial write.
  uint32_t* const write_buffer_copy = static_cast<uint32_t*>(malloc(tile_size));
  memcpy(write_buffer_copy, write_buffer, tile_size);
  free(write_buffer);
  memset(read_buffer, 0, tile_size);
  read_offset = 0;
  CHECK(tile.read(read_buffer, tile_size, read_offset).ok());
  CHECK(memcmp(read_buffer, write_buffer_copy, tile_size) == 0);

  free(read_buffer);
  free(large_read_buffer);
  free(write_buffer_copy);
}

TEST_CASE("Tile: Test copy constructor", "[Tile][copy_constructor]") {
  // Instantiate and initialize the first test Tile.
  Tile tile1;
  const uint32_t format_version = 0;
  const Datatype data_type = Datatype::UINT32;
  const uint64_t tile_size = 1024 * 1024;
  const uint64_t cell_size = sizeof(uint32_t);
  const unsigned int dim_num = 1;
  CHECK(tile1
            .init_unfiltered(
                format_version, data_type, tile_size, cell_size, dim_num)
            .ok());

  // Create a buffer to write to the first test Tile.
  const uint32_t buffer_len = tile_size / sizeof(uint32_t);
  uint32_t* const buffer = static_cast<uint32_t*>(malloc(tile_size));
  for (uint32_t i = 0; i < buffer_len; ++i) {
    buffer[i] = i;
  }

  // Write the buffer to the first test Tile.
  CHECK(tile1.write(buffer, tile_size).ok());

  // Instantiate a second test tile with the copy constructor.
  Tile tile2(tile1);

  // Verify all public attributes are identical.
  CHECK(tile2.cell_size() == tile1.cell_size());
  CHECK(tile2.cell_num() == tile1.cell_num());
  CHECK(tile2.dim_num() == tile1.dim_num());
  CHECK(tile2.empty() == tile1.empty());
  CHECK(tile2.filtered() == tile1.filtered());
  CHECK(tile2.format_version() == tile1.format_version());
  CHECK(tile2.full() == tile1.full());
  CHECK(tile2.offset() == tile1.offset());
  CHECK(tile2.size() == tile1.size());
  CHECK(tile2.stores_coords() == tile1.stores_coords());
  CHECK(tile2.type() == tile1.type());
  CHECK(tile2.owns_buff() == tile1.owns_buff());

  // Read the second test tile to verify it contains the data
  // written to the first test tile.
  uint32_t* const read_buffer = static_cast<uint32_t*>(malloc(tile_size));
  memset(read_buffer, 0, tile_size);
  uint64_t read_offset = 0;
  CHECK(tile2.read(read_buffer, tile_size, read_offset).ok());
  CHECK(memcmp(read_buffer, buffer, tile_size) == 0);

  // Ensure the internal data was deep-copied:
  CHECK(tile1.buffer());
  CHECK(tile2.buffer());
  void* tile1_chunk_0 = tile1.buffer()->data();
  void* tile2_chunk_0 = tile2.buffer()->data();
  CHECK(tile1_chunk_0 != tile2_chunk_0);

  free(buffer);
  free(read_buffer);
}

TEST_CASE("Tile: Test move constructor", "[Tile][move_constructor]") {
  // Instantiate and initialize the first test Tile.
  Tile tile1;
  const uint32_t format_version = 0;
  const Datatype data_type = Datatype::UINT32;
  const uint64_t tile_size = 1024 * 1024;
  const uint64_t cell_size = sizeof(uint32_t);
  const unsigned int dim_num = 1;
  CHECK(tile1
            .init_unfiltered(
                format_version, data_type, tile_size, cell_size, dim_num)
            .ok());

  // Create a buffer to write to the first test Tile.
  const uint32_t buffer_len = tile_size / sizeof(uint32_t);
  uint32_t* const buffer = static_cast<uint32_t*>(malloc(tile_size));
  for (uint32_t i = 0; i < buffer_len; ++i) {
    buffer[i] = i;
  }

  // Write the buffer to the first test Tile.
  CHECK(tile1.write(buffer, tile_size).ok());

  // Instantiate a second test tile with the copy constructor.
  Tile tile2(tile1);

  // Instantiate a third test tile with the move constructor.
  Tile tile3(std::move(tile1));

  // Verify all public attributes are identical.
  CHECK(tile3.cell_size() == tile2.cell_size());
  CHECK(tile3.cell_num() == tile2.cell_num());
  CHECK(tile3.dim_num() == tile2.dim_num());
  CHECK(tile3.empty() == tile2.empty());
  CHECK(tile3.filtered() == tile2.filtered());
  CHECK(tile3.format_version() == tile2.format_version());
  CHECK(tile3.full() == tile2.full());
  CHECK(tile3.offset() == tile2.offset());
  CHECK(tile3.size() == tile2.size());
  CHECK(tile3.stores_coords() == tile2.stores_coords());
  CHECK(tile3.type() == tile2.type());
  CHECK(tile3.owns_buff() == tile2.owns_buff());

  // Read the second test tile to verify it contains the data
  // written to the first test tile.
  uint32_t* const read_buffer = static_cast<uint32_t*>(malloc(tile_size));
  memset(read_buffer, 0, tile_size);
  uint64_t read_offset = 0;
  CHECK(tile2.read(read_buffer, tile_size, read_offset).ok());
  CHECK(memcmp(read_buffer, buffer, tile_size) == 0);

  free(buffer);
  free(read_buffer);
}

TEST_CASE("Tile: Test assignment", "[Tile][assignment]") {
  // Instantiate and initialize the first test Tile.
  Tile tile1;
  const uint32_t format_version = 0;
  const Datatype data_type = Datatype::UINT32;
  const uint64_t tile_size = 1024 * 1024;
  const uint64_t cell_size = sizeof(uint32_t);
  const unsigned int dim_num = 1;
  CHECK(tile1
            .init_unfiltered(
                format_version, data_type, tile_size, cell_size, dim_num)
            .ok());

  // Create a buffer to write to the first test Tile.
  const uint32_t buffer_len = tile_size / sizeof(uint32_t);
  uint32_t* const buffer = static_cast<uint32_t*>(malloc(tile_size));
  for (uint32_t i = 0; i < buffer_len; ++i) {
    buffer[i] = i;
  }

  // Write the buffer to the first test Tile.
  CHECK(tile1.write(buffer, tile_size).ok());

  // Assign the first test Tile to a second test Tile.
  Tile tile2 = tile1;

  // Verify all public attributes are identical.
  CHECK(tile2.cell_size() == tile1.cell_size());
  CHECK(tile2.cell_num() == tile1.cell_num());
  CHECK(tile2.dim_num() == tile1.dim_num());
  CHECK(tile2.empty() == tile1.empty());
  CHECK(tile2.filtered() == tile1.filtered());
  CHECK(tile2.format_version() == tile1.format_version());
  CHECK(tile2.full() == tile1.full());
  CHECK(tile2.offset() == tile1.offset());
  CHECK(tile2.size() == tile1.size());
  CHECK(tile2.stores_coords() == tile1.stores_coords());
  CHECK(tile2.type() == tile1.type());
  CHECK(tile2.owns_buff() == tile1.owns_buff());

  // Read the second test tile to verify it contains the data
  // written to the first test tile.
  uint32_t* const read_buffer = static_cast<uint32_t*>(malloc(tile_size));
  memset(read_buffer, 0, tile_size);
  uint64_t read_offset = 0;
  CHECK(tile2.read(read_buffer, tile_size, read_offset).ok());
  CHECK(memcmp(read_buffer, buffer, tile_size) == 0);

  // Ensure the internal data was deep-copied:
  CHECK(tile1.buffer());
  CHECK(tile2.buffer());
  void* tile1_chunk_0 = tile1.buffer()->data();
  void* tile2_chunk_0 = tile2.buffer()->data();
  CHECK(tile1_chunk_0 != tile2_chunk_0);

  free(buffer);
  free(read_buffer);
}

TEST_CASE("Tile: Test move-assignment", "[Tile][move_assignment]") {
  // Instantiate and initialize the first test Tile.
  Tile tile1;
  const uint32_t format_version = 0;
  const Datatype data_type = Datatype::UINT32;
  const uint64_t tile_size = 1024 * 1024;
  const uint64_t cell_size = sizeof(uint32_t);
  const unsigned int dim_num = 1;
  CHECK(tile1
            .init_unfiltered(
                format_version, data_type, tile_size, cell_size, dim_num)
            .ok());

  // Create a buffer to write to the first test Tile.
  const uint32_t buffer_len = tile_size / sizeof(uint32_t);
  uint32_t* const buffer = static_cast<uint32_t*>(malloc(tile_size));
  for (uint32_t i = 0; i < buffer_len; ++i) {
    buffer[i] = i;
  }

  // Write the buffer to the first test Tile.
  CHECK(tile1.write(buffer, tile_size).ok());

  // Instantiate a second test tile with the copy constructor.
  Tile tile2(tile1);

  // Instantiate a third test tile with the move constructor.
  Tile tile3 = std::move(tile1);

  // Verify all public attributes are identical.
  CHECK(tile3.cell_size() == tile2.cell_size());
  CHECK(tile3.cell_num() == tile2.cell_num());
  CHECK(tile3.dim_num() == tile2.dim_num());
  CHECK(tile3.empty() == tile2.empty());
  CHECK(tile3.filtered() == tile2.filtered());
  CHECK(tile3.format_version() == tile2.format_version());
  CHECK(tile3.full() == tile2.full());
  CHECK(tile3.offset() == tile2.offset());
  CHECK(tile3.size() == tile2.size());
  CHECK(tile3.stores_coords() == tile2.stores_coords());
  CHECK(tile3.type() == tile2.type());
  CHECK(tile3.owns_buff() == tile2.owns_buff());

  // Read the second test tile to verify it contains the data
  // written to the first test tile.
  uint32_t* const read_buffer = static_cast<uint32_t*>(malloc(tile_size));
  memset(read_buffer, 0, tile_size);
  uint64_t read_offset = 0;
  CHECK(tile2.read(read_buffer, tile_size, read_offset).ok());
  CHECK(memcmp(read_buffer, buffer, tile_size) == 0);

  free(buffer);
  free(read_buffer);
}

TEST_CASE(
    "Tile: Test buffer chunks value constructor",
    "[Tile][buffer_chunks_value_constructor]") {
  // Define test tile attributes.
  const Datatype data_type = Datatype::UINT32;
  const uint64_t tile_size = 1024 * 1024;
  const uint64_t cell_size = sizeof(uint32_t);
  const unsigned int dim_num = 1;

  // Instantiate a Buffer instance. We will use this to test
  // ownership and non-ownership from a Tile value constructor.
  Buffer buffer;
  const uint32_t buffer_len = tile_size / sizeof(uint32_t);
  for (uint32_t i = 0; i < buffer_len; ++i) {
    buffer.write(&i, sizeof(uint32_t));
  }
  CHECK(buffer.size() == tile_size);

  // Instantiate the first test tile that does NOT own 'buffer'.
  bool owns_buff = false;
  std::unique_ptr<Tile> tile1(
      new Tile(data_type, cell_size, dim_num, &buffer, owns_buff));
  CHECK(tile1);
  CHECK(tile1->size() == tile_size);
  CHECK(!tile1->full());
  CHECK(tile1->buffer()->size() == tile_size);
  CHECK(!tile1->owns_buff());

  // Test a full read.
  uint32_t* const read_buffer = static_cast<uint32_t*>(malloc(tile_size));
  memset(read_buffer, 0, tile_size);
  uint64_t read_offset = 0;
  CHECK(tile1->read(read_buffer, tile_size, read_offset).ok());
  CHECK(memcmp(read_buffer, buffer.data(), tile_size) == 0);

  // Free tile1 and ensure that buffer is still valid.
  uint32_t* const buffer_copy = static_cast<uint32_t*>(malloc(tile_size));
  memcpy(buffer_copy, buffer.data(), tile_size);
  tile1.reset();
  CHECK(memcmp(buffer_copy, buffer.data(), tile_size) == 0);

  Buffer* const alloced_buffer = tdb_new(Buffer);
  alloced_buffer->swap(buffer);

  // Instantiate the second test tile that does own 'buffer'.
  owns_buff = true;
  std::unique_ptr<Tile> tile2(
      new Tile(data_type, cell_size, dim_num, alloced_buffer, owns_buff));
  CHECK(tile2);
  CHECK(!tile2->empty());
  CHECK(!tile2->full());
  CHECK(tile2->size() == tile_size);
  CHECK(tile2->owns_buff());

  // Test a full read.
  memset(read_buffer, 0, tile_size);
  read_offset = 0;
  CHECK(tile2->read(read_buffer, tile_size, read_offset).ok());
  CHECK(memcmp(read_buffer, alloced_buffer->data(), tile_size) == 0);

  // Disown 'buffer' because the destructor of 'tile2' will delete it.
  alloced_buffer->disown_data();
  tile2.reset();

  free(buffer_copy);
  free(read_buffer);
}