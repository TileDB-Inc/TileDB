/**
 * @file   unit_math.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * Tests the various utility functions.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/sm/misc/tdb_math.h"

using namespace tiledb::sm::utils;

TEST_CASE("Utils: Test left_p2_m1", "[utils][left_p2_m1]") {
  CHECK(math::left_p2_m1(0) == 0);
  CHECK(math::left_p2_m1(1) == 1);
  CHECK(math::left_p2_m1(2) == 1);
  CHECK(math::left_p2_m1(3) == 3);
  CHECK(math::left_p2_m1(4) == 3);
  CHECK(math::left_p2_m1(5) == 3);
  CHECK(math::left_p2_m1(6) == 3);
  CHECK(math::left_p2_m1(7) == 7);
  CHECK(math::left_p2_m1(8) == 7);
  CHECK(math::left_p2_m1(9) == 7);
  CHECK(math::left_p2_m1(10) == 7);
  CHECK(math::left_p2_m1(11) == 7);
  CHECK(math::left_p2_m1(12) == 7);
  CHECK(math::left_p2_m1(13) == 7);
  CHECK(math::left_p2_m1(14) == 7);
  CHECK(math::left_p2_m1(15) == 15);
  CHECK(math::left_p2_m1(UINT64_MAX) == UINT64_MAX);
  CHECK(math::left_p2_m1(UINT64_MAX - 1) == (UINT64_MAX >> 1));
}

TEST_CASE("Utils: Test right_p2_m1", "[utils][right_p2_m1]") {
  CHECK(math::right_p2_m1(0) == 0);
  CHECK(math::right_p2_m1(1) == 1);
  CHECK(math::right_p2_m1(2) == 3);
  CHECK(math::right_p2_m1(3) == 3);
  CHECK(math::right_p2_m1(4) == 7);
  CHECK(math::right_p2_m1(5) == 7);
  CHECK(math::right_p2_m1(6) == 7);
  CHECK(math::right_p2_m1(7) == 7);
  CHECK(math::right_p2_m1(8) == 15);
  CHECK(math::right_p2_m1(9) == 15);
  CHECK(math::right_p2_m1(10) == 15);
  CHECK(math::right_p2_m1(11) == 15);
  CHECK(math::right_p2_m1(12) == 15);
  CHECK(math::right_p2_m1(13) == 15);
  CHECK(math::right_p2_m1(14) == 15);
  CHECK(math::right_p2_m1(15) == 15);
  CHECK(math::right_p2_m1(16) == 31);
  CHECK(math::right_p2_m1(UINT64_MAX) == UINT64_MAX);
  CHECK(math::right_p2_m1(UINT64_MAX - 1) == UINT64_MAX);
}
