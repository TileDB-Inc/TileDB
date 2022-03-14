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

TEST_CASE(
    "RangeOperations: Test is_valid_range for numeric datatypes", "[range]") {
  SECTION("Test single point is valid") {
    test_valid_range<int8_t>(1, 1);
    test_valid_range<uint8_t>(1, 1);
    test_valid_range<int16_t>(1, 1);
    test_valid_range<uint16_t>(1, 1);
    test_valid_range<int32_t>(1, 1);
    test_valid_range<uint32_t>(1, 1);
    test_valid_range<int64_t>(1, 1);
    test_valid_range<uint64_t>(1, 1);
    test_valid_range<float>(1.0, 1.0);
    test_valid_range<double>(1.0, 1.0);
  }
  SECTION("Test standard range is valid") {
    test_valid_range<int8_t>(-1, 10);
    test_valid_range<uint8_t>(1, 10);
    test_valid_range<int16_t>(-1, 10);
    test_valid_range<uint16_t>(1, 10);
    test_valid_range<int32_t>(-1, 10);
    test_valid_range<uint32_t>(1, 10);
    test_valid_range<int64_t>(-1, 10);
    test_valid_range<uint64_t>(1, 10);
    test_valid_range<float>(-10.0, 10.0);
    test_valid_range<double>(-10.0, 10.0);
  }
  SECTION("Test full typeset is valid") {
    test_full_typeset_is_valid<int8_t>();
    test_full_typeset_is_valid<uint8_t>();
    test_full_typeset_is_valid<int16_t>();
    test_full_typeset_is_valid<uint16_t>();
    test_full_typeset_is_valid<int32_t>();
    test_full_typeset_is_valid<uint32_t>();
    test_full_typeset_is_valid<int64_t>();
    test_full_typeset_is_valid<uint64_t>();
    test_full_typeset_is_valid<float>();
    test_full_typeset_is_valid<double>();
  }
  SECTION("Test lower bound larger than upper bound is invalid") {
    test_invalid_range<int8_t>(1, -1);
    test_invalid_range<uint8_t>(10, 1);
    test_invalid_range<int16_t>(1, -1);
    test_invalid_range<uint16_t>(10, 1);
    test_invalid_range<int32_t>(1, -1);
    test_invalid_range<uint32_t>(10, 1);
    test_invalid_range<int64_t>(1, -1);
    test_invalid_range<uint64_t>(10, 1);
    test_invalid_range<float>(1.0, -1.0);
    test_invalid_range<double>(1.0, -1.0);
  }
  SECTION("Test infinite values are valid") {
    test_valid_range<float>(
        -std::numeric_limits<float>::infinity(),
        std::numeric_limits<float>::infinity());
    test_valid_range<float>(0.0, std::numeric_limits<float>::infinity());
    test_valid_range<float>(-std::numeric_limits<float>::infinity(), 0.0);
    test_valid_range<double>(
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::infinity());
    test_valid_range<double>(0.0, std::numeric_limits<double>::infinity());
    test_valid_range<double>(-std::numeric_limits<double>::infinity(), 0.0);
  }
  SECTION("Test NaN values are invalid") {
    test_invalid_range<float>(
        std::numeric_limits<float>::quiet_NaN(),
        std::numeric_limits<float>::quiet_NaN());
    test_invalid_range<float>(0.0, std::numeric_limits<float>::quiet_NaN());
    test_invalid_range<float>(std::numeric_limits<float>::quiet_NaN(), 0.0);
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

template <typename T>
void test_signed_integer_superset() {
  std::string section_name{"Test RangeSuperset for "};
  SECTION(section_name + typeid(T).name()) {
    T domain[2]{-2, 2};
    T subset[2]{-1, 1};
    T bad_lower[2]{-4, 0};
    T bad_upper[2]{0, 8};
    T superset[2]{-8, 8};
    T fullset[2]{std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
    test_good_subset<T>(domain, domain);
    test_good_subset<T>(domain, subset);
    test_bad_subset<T>(domain, bad_lower);
    test_bad_subset<T>(domain, bad_upper);
    test_bad_subset<T>(domain, superset);
    test_bad_subset<T>(domain, fullset);
  }
}

template <typename T>
void test_unsigned_integer_superset() {
  std::string section_name{"Test RangeSuperset for "};
  SECTION(section_name + typeid(T).name()) {
    T domain[2]{1, 4};
    T subset[2]{2, 3};
    T bad_lower[2]{0, 3};
    T bad_upper[2]{2, 8};
    T superset[2]{0, 6};
    T fullset[2]{std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
    test_good_subset<T>(domain, domain);
    test_good_subset<T>(domain, subset);
    test_bad_subset<T>(domain, bad_lower);
    test_bad_subset<T>(domain, bad_upper);
    test_bad_subset<T>(domain, superset);
    test_bad_subset<T>(domain, fullset);
  }
}

template <typename T>
void test_floating_point_superset() {
  std::string section_name{"Test RangeSuperset for "};
  SECTION(section_name + typeid(T).name()) {
    T domain[2]{-10.5, 3.33};
    T subset[2]{-2.5, 2.5};
    T bad_lower[2]{-20.5, 0.0};
    T bad_upper[2]{0.0, 20.5};
    T superset[2]{-20.0, 20.0};
    T fullset[2]{std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
    T infinite[2]{-std::numeric_limits<T>::infinity(),
                  std::numeric_limits<T>::infinity()};
    test_good_subset<T>(domain, domain);
    test_good_subset<T>(domain, subset);
    test_bad_subset<T>(domain, bad_lower);
    test_bad_subset<T>(domain, bad_upper);
    test_bad_subset<T>(domain, superset);
    test_bad_subset<T>(domain, fullset);
    test_bad_subset<T>(domain, infinite);
  }
}

TEST_CASE("RangeSuperset: Test intersect,check_is_subset", "[range]") {
  test_unsigned_integer_superset<uint8_t>();
  test_unsigned_integer_superset<uint16_t>();
  test_unsigned_integer_superset<uint32_t>();
  test_unsigned_integer_superset<uint64_t>();
  test_signed_integer_superset<int8_t>();
  test_signed_integer_superset<int16_t>();
  test_signed_integer_superset<int32_t>();
  test_signed_integer_superset<int64_t>();
  test_floating_point_superset<float>();
  test_floating_point_superset<double>();
}
