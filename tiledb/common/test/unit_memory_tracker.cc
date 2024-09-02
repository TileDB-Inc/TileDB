/**
 * @file unit_memory_tracker.cc
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
 * This file tests the MemoryTracker class.
 */

#include <iostream>

#include <test/support/tdb_catch.h>
#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/pmr.h"

using namespace tiledb::sm;

TEST_CASE(
    "Memory tracker: Test budget exceeded callback",
    "[memory_tracker][budget_exceeded]") {
  MemoryTrackerManager tracker_manager;
  auto tracker = tracker_manager.create_tracker(
      100, []() { throw std::runtime_error("Budget exceeded"); });
  CHECK_THROWS_WITH(
      [&tracker]() {
        std::ignore = tdb::pmr::vector<uint8_t>(
            1000, tracker->get_resource(MemoryType::TILE_DATA));
      }(),
      "Budget exceeded");
}
