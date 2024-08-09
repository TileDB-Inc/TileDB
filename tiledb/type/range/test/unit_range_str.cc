/**
 * @file tiledb/type/range/test/unit_range_str.cc
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
 * This file defines unit tests for the Range function for generating strings
 * from ranges.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;
using namespace tiledb::sm;

TEMPLATE_TEST_CASE_SIG(
    "Test range_str for unsigned int types",
    "[range]",
    ((typename T, Datatype D), T, D),
    (uint8_t, Datatype::UINT8),
    (uint16_t, Datatype::UINT16),
    (uint32_t, Datatype::UINT32),
    (uint64_t, Datatype::UINT64)) {
  T data[2]{1, 10};
  Range range{&data[0], 2 * sizeof(T)};
  std::string expected_output{"[1, 10]"};
  REQUIRE(range_str(range, D) == expected_output);
}

TEMPLATE_TEST_CASE_SIG(
    "Test range_str for signed int types",
    "[range]",
    ((typename T, Datatype D), T, D),
    (int8_t, Datatype::INT8),
    (int16_t, Datatype::INT16),
    (int32_t, Datatype::INT32),
    (int64_t, Datatype::INT64)) {
  T data[2]{-4, 4};
  Range range{&data[0], 2 * sizeof(T)};
  std::string expected_output{"[-4, 4]"};
  REQUIRE(range_str(range, D) == expected_output);
}

TEMPLATE_TEST_CASE_SIG(
    "Test range_str for floating-point types",
    "[range][!mayfail]",  // Allow failure for inconsistent float rep.
    ((typename T, Datatype D), T, D),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  T data[2]{-10.5, 10.5};
  Range range{&data[0], 2 * sizeof(T)};
  std::string expected_output{"[-10.5, 10.5]"};
  REQUIRE(range_str(range, D) == expected_output);
}

TEST_CASE("Test range_str for empty range", "[range][!mayfail]") {
  Range range{};
  REQUIRE(range_str(range, Datatype::STRING_ASCII) == constants::null_str);
}

TEST_CASE("Test range_str for string range", "[range][!mayfail]") {
  std::string start{"start"};
  std::string end{"end"};
  Range range{start, end};
  std::string expected_output{"[start, end]"};
  REQUIRE(range_str(range, Datatype::STRING_ASCII) == expected_output);
}
