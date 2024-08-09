/**
 * @file tiledb/type/range/test/unit_check_range_is_valid.cc
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
 * This file defines unit tests for the Range function that checks range
 * validity.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

TEMPLATE_TEST_CASE(
    "RangeOperations: Test is_valid_range for unsigned int types",
    "[range]",
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t) {
  SECTION("Test single point is valid") {
    TestType data[2]{1, 1};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test standard range is valid") {
    TestType data[2]{1, 10};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test full typeset is valid") {
    TestType fullset[2]{
        std::numeric_limits<TestType>::min(),
        std::numeric_limits<TestType>::max()};
    Range range{&fullset[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test empty range is invalid") {
    Range range;
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
  SECTION("Test lower bound larger than upper bound is invalid") {
    TestType data[2]{10, 1};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
}

TEMPLATE_TEST_CASE(
    "RangeOperations: Test is_valid_range for signed int types",
    "[range]",
    int8_t,
    int16_t,
    int32_t,
    int64_t) {
  SECTION("Test single point is valid") {
    TestType data[2]{-1, -1};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test standard range is valid") {
    TestType data[2]{-1, 10};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test full typeset is valid") {
    TestType fullset[2]{
        std::numeric_limits<TestType>::min(),
        std::numeric_limits<TestType>::max()};
    Range range{&fullset[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test empty range is invalid") {
    Range range;
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
  SECTION("Test lower bound larger than upper bound is invalid") {
    TestType data[2]{1, -1};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
}

TEMPLATE_TEST_CASE(
    "RangeOperations: Test is_valid_range for floating-point types",
    "[range]",
    float,
    double) {
  SECTION("Test single point range is valid") {
    TestType data[2]{1.5, 1.5};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test standard range is valid") {
    TestType data[2]{-10.5, 10.5};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test the full typeset is valid") {
    TestType data[2]{
        std::numeric_limits<TestType>::min(),
        std::numeric_limits<TestType>::max()};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test range with lower infinite bound is valid") {
    TestType data[2]{
        -std::numeric_limits<TestType>::infinity(),
        std::numeric_limits<TestType>::infinity()};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test range with upper infinite bound is valid") {
    TestType data[2]{0.0, std::numeric_limits<TestType>::infinity()};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test range with infinite bounds is valid") {
    TestType data[2]{-std::numeric_limits<TestType>::infinity(), 0.0};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_NOTHROW(check_range_is_valid<TestType>(range));
  }
  SECTION("Test empty range is invalid") {
    Range range;
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
  SECTION("Test range with NaN values is invalid") {
    TestType data[2]{
        std::numeric_limits<TestType>::quiet_NaN(),
        std::numeric_limits<TestType>::quiet_NaN()};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
  SECTION("Test range with lower NaN value is invalid") {
    TestType data[2]{0.0, std::numeric_limits<TestType>::quiet_NaN()};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
  SECTION("Test range with upper NaN value is invalid") {
    TestType data[2]{std::numeric_limits<TestType>::quiet_NaN(), 0.0};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
  SECTION("Test lower bound larger than upper bound is invalid") {
    TestType data[2]{1.0, -1.0};
    Range range{&data[0], 2 * sizeof(TestType)};
    REQUIRE_THROWS(check_range_is_valid<TestType>(range));
  }
}

TEST_CASE("RangeOperations: Test is_valid_range for string_view", "[range]") {
  SECTION("Test single point range is valid") {
    Range range{"a", "a"};
    REQUIRE_NOTHROW(check_range_is_valid<std::string_view>(range));
  }
  SECTION("Test standard range is valid") {
    Range range{"abc", "def"};
    REQUIRE_NOTHROW(check_range_is_valid<std::string_view>(range));
  }
  SECTION("Test empty range is invalid") {
    Range range;
    REQUIRE_THROWS(check_range_is_valid<std::string_view>(range));
  }
  SECTION("Test lower bound larger than upper bound is invalid") {
    Range range{"def", "abc"};
    REQUIRE_THROWS(check_range_is_valid<std::string_view>(range));
  }
}
