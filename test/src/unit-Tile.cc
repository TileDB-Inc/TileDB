/**
 * @file unit-Tile.cc
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
 * Tests the `Tile` class.
 */

#include "tiledb/sm/tile/tile.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("Tile: Test basic read", "[Tile][basic_read]") {
  // Initialize the test Tile.
  Tile tile;
  const uint32_t format_version = 0;
  const Datatype data_type = Datatype::UINT32;
  const uint64_t cell_size = 0;
  const unsigned int dim_num = 0;
  CHECK(tile.init(format_version, data_type, cell_size, dim_num).ok());

  // Create a buffer to write to the test Tile.
  const uint32_t buffer_len = 128;
  uint32_t buffer[buffer_len];
  for (uint32_t i = 0; i < buffer_len; ++i) {
    buffer[i] = i;
  }

  // Write the buffer to the test Tile.
  const uint64_t buffer_size = buffer_len * sizeof(uint32_t);
  CHECK(tile.write(buffer, buffer_size).ok());

  // Test a partial read at offset 8, which should be a uint32_t with
  // a value of two.
  uint32_t two = 0;
  CHECK(tile.read(&two, sizeof(uint32_t), 8).ok());
  CHECK(two == 2);

  // Test a full read.
  uint32_t read_buffer[buffer_len];
  memset(read_buffer, 0, buffer_size);
  uint64_t read_offset = 0;
  CHECK(tile.read(read_buffer, buffer_size, read_offset).ok());
  CHECK(memcmp(read_buffer, buffer, buffer_size) == 0);

  // Test a read at an out-of-bounds offset.
  memset(read_buffer, 0, buffer_size);
  read_offset = buffer_size;
  CHECK(!tile.read(read_buffer, buffer_size, read_offset).ok());

  // Test a read at a valid offset but with a size that
  // exceeds the written buffer size.
  const uint32_t large_buffer_len = buffer_len * 2;
  uint32_t large_read_buffer[large_buffer_len];
  const uint64_t large_buffer_size = large_buffer_len * sizeof(uint32_t);
  memset(large_read_buffer, 0, large_buffer_size);
  read_offset = 0;
  CHECK(!tile.read(large_read_buffer, large_buffer_size, read_offset).ok());
}