/**
 * @file unit_memory_tracker_types.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file tests the memory tracker to_str functions.
 */

#include <iostream>

#include <catch2/catch_test_macros.hpp>
#include "tiledb/common/memory_tracker.h"

using namespace tiledb::common;

TEST_CASE("memory_type_to_str") {
  auto max = static_cast<int>(tiledb::sm::MemoryType::WRITER_TILE_DATA);
  size_t failures = 0;
  for (int8_t i = 0; i < 127; i++) {
    auto val = static_cast<tiledb::sm::MemoryType>(i);
    if (i <= max) {
      REQUIRE_NOTHROW(tiledb::sm::memory_type_to_str(val));
    } else {
      REQUIRE_THROWS(tiledb::sm::memory_type_to_str(val));
      failures += 1;
    }
  }
  // Technically, we could eventually have more than 127 enumeration values
  // and this test would pass when it shouldn't.
  REQUIRE(failures > 0);
}

TEST_CASE("memory_tracker_type_to_str") {
  auto max = static_cast<int>(tiledb::sm::MemoryTrackerType::SCHEMA_EVOLUTION);
  size_t failures = 0;
  for (int8_t i = 0; i < 127; i++) {
    auto val = static_cast<tiledb::sm::MemoryTrackerType>(i);
    if (i <= max) {
      REQUIRE_NOTHROW(tiledb::sm::memory_tracker_type_to_str(val));
    } else {
      REQUIRE_THROWS(tiledb::sm::memory_tracker_type_to_str(val));
      failures += 1;
    }
  }
  // Technically, we could eventually have more than 127 enumeration values
  // and this test would pass when it shouldn't.
  REQUIRE(failures > 0);
}
