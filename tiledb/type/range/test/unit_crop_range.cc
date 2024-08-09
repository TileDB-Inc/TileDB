/**
 * @file tiledb/sm/misc/test/unit_intersect_range.cc
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
 * This file defines unit tests for the Range helper functions used for
 * intersecting a range.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

template <typename TestType>
void test_crop_range(
    const TestType* bounds_data,
    const TestType* range_data,
    const TestType* expected_result) {
  // Create ranges.
  Range bounds{bounds_data, 2 * sizeof(TestType)};
  Range range{range_data, 2 * sizeof(TestType)};
  Status status = Status::Ok();
  // Crop range.
  crop_range<TestType>(bounds, range);
  // Check output from crop.
  auto new_range_data = static_cast<const TestType*>(range.data());
  CHECK(new_range_data[0] == expected_result[0]);
  REQUIRE(new_range_data[1] == expected_result[1]);
}

TEMPLATE_TEST_CASE(
    "Test intersect_range for unsigned int types",
    "[range]",
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t) {
  TestType bounds[2]{1, 4};
  SECTION("Test crop full bounds is no-op") {
    test_crop_range<TestType>(bounds, bounds, bounds);
  }
  SECTION("Test no-op for range inside bounds") {
    TestType range[2]{2, 3};
    test_crop_range<TestType>(bounds, range, range);
  }
  SECTION("Test crop lower bound") {
    TestType range[2]{0, 3};
    TestType result[2]{1, 3};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop upper bound") {
    TestType range[2]{2, 8};
    TestType result[2]{2, 4};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop both bounds") {
    TestType range[2]{0, 6};
    test_crop_range<TestType>(bounds, range, bounds);
  }
  SECTION("Test crop full typeset") {
    TestType range[2]{
        std::numeric_limits<TestType>::lowest(),
        std::numeric_limits<TestType>::max()};
    test_crop_range<TestType>(bounds, range, bounds);
  }
  SECTION("Test crop outside lower bound") {
    TestType range[2]{0, 0};
    TestType result[2]{1, 1};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop outside upper bound") {
    TestType range[2]{5, 6};
    TestType result[2]{4, 4};
    test_crop_range<TestType>(bounds, range, result);
  }
}

TEMPLATE_TEST_CASE(
    "Test intersect_range for signed int types",
    "[range]",
    int8_t,
    int16_t,
    int32_t,
    int64_t) {
  TestType bounds[2]{-2, 2};
  SECTION("Test crop on bounds is a no-op") {
    test_crop_range<TestType>(bounds, bounds, bounds);
  }
  SECTION("Test crop no-op for range that fits in bounds") {
    TestType range[2]{-1, 1};
    test_crop_range<TestType>(bounds, range, range);
  }
  SECTION("Test crop lower bound") {
    TestType range[2]{-4, 0};
    TestType result[2]{-2, 0};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop upper bound") {
    TestType range[2]{0, 8};
    TestType result[2]{0, 2};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop both bounds") {
    TestType range[2]{-8, 8};
    test_crop_range<TestType>(bounds, range, bounds);
  }
  SECTION("Test crop full typeset") {
    TestType range[2]{
        std::numeric_limits<TestType>::lowest(),
        std::numeric_limits<TestType>::max()};
    test_crop_range<TestType>(bounds, range, bounds);
  }
  SECTION("Test crop outside lower bound") {
    TestType range[2]{-6, -4};
    TestType result[2]{-2, -2};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop outside upper bound") {
    TestType range[2]{5, 6};
    TestType result[2]{2, 2};
    test_crop_range<TestType>(bounds, range, result);
  }
}

TEMPLATE_TEST_CASE(
    "Test intersect_range for floating-point types", "[range]", float, double) {
  TestType bounds[2]{-10.5, 3.33f};
  SECTION("Test crop on bounds is a no-op") {
    test_crop_range<TestType>(bounds, bounds, bounds);
  }
  SECTION("Test crop no-op for range that fits in bounds") {
    TestType range[2]{-2.5, 2.5};
    test_crop_range<TestType>(bounds, range, range);
  }
  SECTION("Test crop lower bound") {
    TestType range[2]{-20.5, 0.0};
    TestType result[2]{-10.5, 0.0};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop upper bound") {
    TestType range[2]{0.0, 20.5};
    TestType result[2]{0.0, 3.33f};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop both bounds") {
    TestType range[2]{-20.0, 20.0};
    test_crop_range<TestType>(bounds, range, bounds);
  }
  SECTION("Test crop full typeset") {
    TestType range[2]{
        std::numeric_limits<TestType>::lowest(),
        std::numeric_limits<TestType>::max()};
    test_crop_range<TestType>(bounds, range, bounds);
  }
  SECTION("Test crop infinite range") {
    TestType range[2]{
        -std::numeric_limits<TestType>::infinity(),
        std::numeric_limits<TestType>::infinity()};
    test_crop_range<TestType>(bounds, range, bounds);
  }
  SECTION("Test crop outside lower bound") {
    TestType range[2]{-60.1f, -40.3f};
    TestType result[2]{-10.5f, -10.5f};
    test_crop_range<TestType>(bounds, range, result);
  }
  SECTION("Test crop outside upper bound") {
    TestType range[2]{5.1f, 6.5f};
    TestType result[2]{3.33f, 3.33f};
    test_crop_range<TestType>(bounds, range, result);
  }
}
