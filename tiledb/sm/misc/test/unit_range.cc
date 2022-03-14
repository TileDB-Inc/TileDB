/**
 * @file tiledb/sm/misc/test/unit_range.cc
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
 * This file defines unit tests for the Range class and helper functions that
 * operator on Ranges.
 */

#include <catch.hpp>
#include "tiledb/sm/misc/types.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

/**************************/
/* Test Check Valid Range */
/**************************/
template <typename T>
void test_valid_range(T lower, T upper) {
  T data[2]{lower, upper};
  Range range{data, 2 * sizeof(T)};
  auto status = RangeOperations<T>::check_is_valid_range(range);
  REQUIRE(status.ok());
}

template <typename T>
void test_invalid_range(T lower, T upper) {
  T data[2]{lower, upper};
  Range range{data, 2 * sizeof(T)};
  auto status = RangeOperations<T>::check_is_valid_range(range);
  REQUIRE(!status.ok());
  INFO(status.ok())
}

template <typename T>
void test_full_typeset_is_valid() {
  T fullset[2]{std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
  Range range{fullset, 2 * sizeof(T)};
  auto status = RangeOperations<T>::check_is_valid_range(range);
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

/****************************/
/* Test Superset Operations */
/****************************/

template <typename T>
void test_good_subset(const T* domain_data, const T* subset_data) {
  // Create superset.
  Range domain{domain_data, 2 * sizeof(T)};
  RangeSuperset<T> superset{domain};
  Range subset{subset_data, 2 * sizeof(T)};
  Status status = Status::Ok();
  // Verify "is subset" checks pass.
  status = superset.check_is_subset(subset);
  REQUIRE(status.ok());
  // Verify "intersect" returns OK status (no-op).
  status = superset.intersect(subset);
  REQUIRE(status.ok());
  // Verify range is unaltered.
  auto new_range_data = static_cast<const T*>(subset.data());
  REQUIRE(new_range_data[0] == subset_data[0]);
  REQUIRE(new_range_data[1] == subset_data[1]);
}

template <typename T>
void test_bad_subset(const T* domain_data, const T* range_data) {
  // Create superset.
  Range domain{domain_data, 2 * sizeof(T)};
  RangeSuperset<T> superset{domain};
  Range range{range_data, 2 * sizeof(T)};
  Status status = Status::Ok();
  // Verify "is subset" checks fail.
  status = superset.check_is_subset(range);
  REQUIRE(!status.ok());
  // Intersect with superset and verify status fails.
  status = superset.intersect(range);
  REQUIRE(!status.ok());
  // Verify "is subset" checks pass after intersection.
  status = superset.check_is_subset(range);
  REQUIRE(status.ok());
  auto new_range_data = static_cast<const T*>(range.data());
  if (range_data[0] < domain_data[0]) {
    REQUIRE(new_range_data[0] == domain_data[0]);
  } else {
    REQUIRE(new_range_data[0] == range_data[0]);
  }
  if (range_data[1] > domain_data[1]) {
    REQUIRE(new_range_data[1] == domain_data[1]);
  } else {
    REQUIRE(new_range_data[1] == range_data[1]);
  }
}

TEMPLATE_TEST_CASE(
    "RangeSuperset: Test intersect, check_is_subset for unsigned int types",
    "[range]",
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t) {
  TestType domain[2]{1, 4};
  SECTION("Test full domain is a valid subset") {
    test_good_subset<TestType>(domain, domain);
  }
  SECTION("Test simple proper subset is a valid subset") {
    TestType subset[2]{2, 3};
    test_good_subset<TestType>(domain, subset);
  }
  SECTION("Test a non-valid subset with lower bound less than superset") {
    TestType bad_lower[2]{0, 3};
    test_bad_subset<TestType>(domain, bad_lower);
  }
  SECTION("Test a non-valid subset with upper bound more than superset") {
    TestType bad_upper[2]{2, 8};
    test_bad_subset<TestType>(domain, bad_upper);
  }
  SECTION("Test a non-valid subset that is a proper superset") {
    TestType superset[2]{0, 6};
    test_bad_subset<TestType>(domain, superset);
  }
  SECTION("Test a non-valid subset that is actually the full typeset") {
    TestType fullset[2]{std::numeric_limits<TestType>::min(),
                        std::numeric_limits<TestType>::max()};
    test_bad_subset<TestType>(domain, fullset);
  }
}

TEMPLATE_TEST_CASE(
    "RangeSuperset: Test intersect, check_is_subset for signed int types",
    "[range]",
    int8_t,
    int16_t,
    int32_t,
    int64_t) {
  TestType domain[2]{-2, 2};
  SECTION("Test full domain is a valid subset") {
    test_good_subset<TestType>(domain, domain);
  }
  SECTION("Test simple proper subset is a valid subset") {
    TestType subset[2]{-1, 1};
    test_good_subset<TestType>(domain, subset);
  }
  SECTION("Test a non-valid subset with lower bound less than superset") {
    TestType bad_lower[2]{-4, 0};
    test_bad_subset<TestType>(domain, bad_lower);
  }
  SECTION("Test a non-valid subset with upper bound more than superset") {
    TestType bad_upper[2]{0, 8};
    test_bad_subset<TestType>(domain, bad_upper);
  }
  SECTION("Test a non-valid subset that is a proper superset") {
    TestType superset[2]{-8, 8};
    test_bad_subset<TestType>(domain, superset);
  }
  SECTION("Test a non-valid subset that is actually the full typeset") {
    TestType fullset[2]{std::numeric_limits<TestType>::min(),
                        std::numeric_limits<TestType>::max()};
    test_bad_subset<TestType>(domain, fullset);
  }
}

TEMPLATE_TEST_CASE(
    "RangeSuperset: Test intersect, check_is_subset for floating-point types",
    "[range]",
    float,
    double) {
  TestType domain[2]{-10.5, 3.33};
  SECTION("Test full domain is a valid subset") {
    test_good_subset<TestType>(domain, domain);
  }
  SECTION("Test simple proper subset is a valid subset") {
    TestType subset[2]{-2.5, 2.5};
    test_good_subset<TestType>(domain, subset);
  }
  SECTION("Test a non-valid subset with lower bound less than superset") {
    TestType bad_lower[2]{-20.5, 0.0};
    test_bad_subset<TestType>(domain, bad_lower);
  }
  SECTION("Test a non-valid subset with upper bound more than superset") {
    TestType bad_upper[2]{0.0, 20.5};
    test_bad_subset<TestType>(domain, bad_upper);
  }
  SECTION("Test a non-valid subset that is a proper superset") {
    TestType superset[2]{-20.0, 20.0};
    test_bad_subset<TestType>(domain, superset);
  }
  SECTION("Test a non-valid subset that is actually the full typeset") {
    TestType fullset[2]{std::numeric_limits<TestType>::min(),
                        std::numeric_limits<TestType>::max()};
    test_bad_subset<TestType>(domain, fullset);
  }
  SECTION("Test a non-valid subset that is actually infinite") {
    TestType infinite[2]{-std::numeric_limits<TestType>::infinity(),
                         std::numeric_limits<TestType>::infinity()};
    test_bad_subset<TestType>(domain, infinite);
  }
}
