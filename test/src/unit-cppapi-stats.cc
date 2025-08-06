/**
 * @file unit-cppapi-stats.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB Inc.
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
 * Tests the C++ API for stats related functions.
 */

#include "test/support/src/stats.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <test/support/tdb_catch.h>

using namespace tiledb;

TEST_CASE("stats gathering is on by default", "[stats]") {
  CHECK(Stats::is_enabled());
}

TEST_CASE("stats disabled, scoped enable", "[stats]") {
  Stats::disable();
  CHECK(!Stats::is_enabled());
  {
    test::ScopedStats scoped;
    CHECK(Stats::is_enabled());
  }
  CHECK(!Stats::is_enabled());
}

TEST_CASE("stats enabled, scoped enable", "[stats]") {
  Stats::enable();
  CHECK(Stats::is_enabled());

  // outer scope disables when exiting
  {
    test::ScopedStats outer;
    CHECK(Stats::is_enabled());

    // inner scope does not disable since stats was enabled when entering
    {
      test::ScopedStats inner;
      CHECK(Stats::is_enabled());
    }

    CHECK(Stats::is_enabled());
  }

  CHECK(Stats::is_enabled());
}
