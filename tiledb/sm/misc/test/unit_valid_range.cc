/**
 * @file tiledb/sm/misc/test/unit_valid_range.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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

#include <catch.hpp>
#include "tiledb/sm/misc/types.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

template <typename T>
void test_valid_range(T lower, T upper) {
  T data[2]{lower, upper};
  Range range{data, 2 * sizeof(T)};
  auto status = check_typed_range_is_valid<T>(range);
  REQUIRE(status.ok());
}

template <typename T>
void test_invalid_range(T lower, T upper) {
  T data[2]{lower, upper};
  Range range{data, 2 * sizeof(T)};
  auto status = check_typed_range_is_valid<T>(range);
  REQUIRE(!status.ok());
  INFO(status.ok())
}

template <typename T>
void test_full_typeset_is_valid() {
  T fullset[2]{std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
  Range range{fullset, 2 * sizeof(T)};
  auto status = check_typed_range_is_valid<T>(range);
  REQUIRE(status.ok());
}

TEMPLATE_TEST_CASE(
    "RangeOperations: Test is_valid_range for unsigned int types",
    "[range]",
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t) {
  SECTION("Test single point is valid") {
    test_valid_range<TestType>(1, 1);
  }
  SECTION("Test standard range is valid") {
    test_valid_range<TestType>(1, 10);
  }
  SECTION("Test full typeset is valid") {
    test_full_typeset_is_valid<TestType>();
  }
  SECTION("Test lower bound larger than upper bound is invalid") {
    test_invalid_range<TestType>(10, 1);
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
    test_valid_range<TestType>(-1, -1);
  }
  SECTION("Test standard range is valid") {
    test_valid_range<TestType>(-1, 10);
  }
  SECTION("Test full typeset is valid") {
    test_full_typeset_is_valid<TestType>();
  }
  SECTION("Test lower bound larger than upper bound is invalid") {
    test_invalid_range<TestType>(1, -1);
  }
}

TEMPLATE_TEST_CASE(
    "RangeOperations: Test is_valid_range for floating-point types",
    "[range]",
    float,
    double) {
  SECTION("Test single point is valid") {
    test_valid_range<TestType>(1.0, 1.0);
  }
  SECTION("Test standard range is valid") {
    test_valid_range<TestType>(-10.0, 10.0);
  }
  SECTION("Test full typeset is valid") {
    test_full_typeset_is_valid<TestType>();
  }
  SECTION("Test lower bound larger than upper bound is invalid") {
    test_invalid_range<TestType>(1.0, -1.0);
  }
  SECTION("Test infinite values are valid") {
    test_valid_range<TestType>(
        -std::numeric_limits<TestType>::infinity(),
        std::numeric_limits<TestType>::infinity());
    test_valid_range<TestType>(0.0, std::numeric_limits<TestType>::infinity());
    test_valid_range<TestType>(-std::numeric_limits<TestType>::infinity(), 0.0);
  }
  SECTION("Test NaN values are invalid") {
    test_invalid_range<double>(
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::quiet_NaN());
    test_invalid_range<double>(0.0, std::numeric_limits<double>::quiet_NaN());
    test_invalid_range<double>(std::numeric_limits<double>::quiet_NaN(), 0.0);
  }
}
