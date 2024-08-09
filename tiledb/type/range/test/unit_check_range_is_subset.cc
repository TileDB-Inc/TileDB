/**
 * @file tiledb/sm/misc/test/unit_check_range_is_subset.cc
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
 * This file defines unit tests for the Range helper functions used for subset
 * comparisons.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

TEMPLATE_TEST_CASE(
    "Test check_is_subset for unsigned int types",
    "[range]",
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t) {
  TestType superset_data[2]{1, 4};
  Range superset{&superset_data[0], 2 * sizeof(TestType)};
  SECTION("Test full domain is a valid subset") {
    auto status = check_range_is_subset<TestType>(superset, superset);
    REQUIRE(status.ok());
  }
  SECTION("Test simple proper subset is a valid subset") {
    TestType data[2]{2, 3};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(status.ok());
  }
  SECTION("Test a non-valid subset with lower bound less than superset") {
    TestType data[2]{0, 3};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset with upper bound more than superset") {
    TestType data[2]{2, 8};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset that is a proper superset") {
    TestType data[2]{0, 6};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset that is actually the full typeset") {
    TestType data[2]{
        std::numeric_limits<TestType>::min(),
        std::numeric_limits<TestType>::max()};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
}

TEMPLATE_TEST_CASE(
    "Test check_is_subset for signed int types",
    "[range]",
    int8_t,
    int16_t,
    int32_t,
    int64_t) {
  TestType superset_data[2]{-2, 2};
  Range superset{&superset_data[0], 2 * sizeof(TestType)};
  SECTION("Test full domain is a valid subset") {
    auto status = check_range_is_subset<TestType>(superset, superset);
    REQUIRE(status.ok());
  }
  SECTION("Test simple proper subset is a valid subset") {
    TestType data[2]{-1, 1};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(status.ok());
  }
  SECTION("Test a non-valid subset with lower bound less than superset") {
    TestType data[2]{-4, 0};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset with upper bound more than superset") {
    TestType data[2]{0, 8};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset that is a proper superset") {
    TestType data[2]{-8, 8};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset that is actually the full typeset") {
    TestType data[2]{
        std::numeric_limits<TestType>::lowest(),
        std::numeric_limits<TestType>::max()};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
}

TEMPLATE_TEST_CASE(
    "Test check_is_subset for floating-point types", "[range]", float, double) {
  TestType superset_data[2]{-10.5, 3.33f};
  Range superset{&superset_data[0], 2 * sizeof(TestType)};
  SECTION("Test full domain is a valid subset") {
    auto status = check_range_is_subset<TestType>(superset, superset);
    REQUIRE(status.ok());
  }
  SECTION("Test simple proper subset is a valid subset") {
    TestType data[2]{-2.5, 2.5};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(status.ok());
  }
  SECTION("Test a non-valid subset with lower bound less than superset") {
    TestType data[2]{-20.5, 0.0};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset with upper bound more than superset") {
    TestType data[2]{0.0, 20.5};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset that is a proper superset") {
    TestType data[2]{-20.0, 20.0};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset that is actually the full typeset") {
    TestType data[2]{
        std::numeric_limits<TestType>::lowest(),
        std::numeric_limits<TestType>::max()};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
  SECTION("Test a non-valid subset that is actually infinite") {
    TestType data[2]{
        -std::numeric_limits<TestType>::infinity(),
        std::numeric_limits<TestType>::infinity()};
    Range range{&data[0], 2 * sizeof(TestType)};
    auto status = check_range_is_subset<TestType>(superset, range);
    REQUIRE(!status.ok());
  }
}
