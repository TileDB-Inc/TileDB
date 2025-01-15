/**
 * @file unit_tile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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

#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/tile/tile.h"

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("Tile: Test basic IO", "[Tile][basic_io]") {
  // Create our test memory tracker.
  auto tracker = tiledb::test::create_test_memory_tracker();

  // Initialize the test Tile.
  const format_version_t format_version = 0;
  const Datatype data_type = Datatype::UINT32;
  const uint64_t tile_size = 1024 * 1024;
  const uint64_t cell_size = sizeof(uint32_t);
  const unsigned int dim_num = 1;
  Tile tile(
      format_version,
      data_type,
      cell_size,
      dim_num,
      tile_size,
      nullptr,
      0,
      tracker,
      ThreadPool::SharedTask(),
      nullptr);
  CHECK(tile.size() == tile_size);

  // Create a buffer to write to the test Tile.
  const uint32_t buffer_len = tile_size / sizeof(uint32_t);
  std::vector<uint32_t> write_buffer(buffer_len);
  for (uint32_t i = 0; i < buffer_len; ++i) {
    write_buffer[i] = i;
  }

  // Write the buffer to the test Tile.
  CHECK_NOTHROW(tile.write(write_buffer.data(), 0, tile_size));
  CHECK(tile.size() == tile_size);

  // Ensure the internal data was deep-copied:
  void* tile_chunk_0 = tile.data();
  CHECK(tile_chunk_0 != write_buffer.data());

  // Test a partial read at offset 8, which should be a uint32_t with
  // a value of two.
  uint32_t two = 0;
  CHECK_NOTHROW(tile.read(&two, 8, sizeof(uint32_t)));
  CHECK(two == 2);

  // Test a full read.
  std::vector<uint32_t> read_buffer(buffer_len);
  uint64_t read_offset = 0;
  CHECK_NOTHROW(tile.read(read_buffer.data(), read_offset, tile_size));
  CHECK(memcmp(read_buffer.data(), write_buffer.data(), tile_size) == 0);

  // Test a write at a non-zero offset. Overwrite the two at offset 8.
  uint32_t magic = 5234549;
  CHECK_NOTHROW(tile.write(&magic, 8, sizeof(uint32_t)));

  // Read the magic number to ensure the '2' value was overwritten.
  two = 0;
  CHECK_NOTHROW(tile.read(&two, 8, sizeof(uint32_t)));
  CHECK(two == magic);

  // Restore the state without the magic number.
  two = 2;
  CHECK_NOTHROW(tile.write(&two, 8, sizeof(uint32_t)));

  // Test a read at an out-of-bounds offset.
  memset(read_buffer.data(), 0, tile_size);
  read_offset = tile_size;
  auto matcher = Catch::Matchers::ContainsSubstring("Read tile overflow");
  CHECK_THROWS_WITH(
      tile.read(read_buffer.data(), read_offset, tile_size), matcher);

  // Test a read at a valid offset but with a size that
  // exceeds the written buffer size.
  const uint32_t large_buffer_size = tile_size * 2;
  std::vector<uint32_t> large_read_buffer(buffer_len * 2);
  read_offset = 0;
  CHECK_THROWS_WITH(
      tile.read(large_read_buffer.data(), read_offset, large_buffer_size),
      matcher);

  // Free the write buffer to ensure that it was deep-copied
  // within the initial write.
  std::vector<uint32_t> write_buffer_copy = write_buffer;
  write_buffer.clear();
  memset(read_buffer.data(), 0, tile_size);
  read_offset = 0;
  CHECK_NOTHROW(tile.read(read_buffer.data(), read_offset, tile_size));
  CHECK(memcmp(read_buffer.data(), write_buffer_copy.data(), tile_size) == 0);
}
