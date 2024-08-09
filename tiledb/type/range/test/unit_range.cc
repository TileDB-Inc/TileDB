/**
 * @file unit_range.cc
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
 * This file tests the Range class
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

TEMPLATE_TEST_CASE(
    "Test fixed-sized range constructor with "
    "separate start and stop pointers",
    "[range]",
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    float,
    double) {
  TestType start{2};
  TestType end{10};
  Range range{&start, &end, sizeof(TestType)};
  REQUIRE(!range.var_size());
  const auto* data = static_cast<const TestType*>(range.data());
  REQUIRE(data[0] == start);
  REQUIRE(data[1] == end);
}

TEST_CASE(
    "Test variable-sized range constructor with separate start and stop "
    "pointers",
    "[range]") {
  std::string start{"x"};
  std::string end{"zzz"};
  Range range{start.data(), start.size(), end.data(), end.size()};
  REQUIRE(range.var_size());
  REQUIRE(range.start_str() == start);
  REQUIRE(range.end_str() == end);
}
