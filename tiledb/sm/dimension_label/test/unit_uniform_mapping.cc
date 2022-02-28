/**
 * @file tiledb/sm/dimension_label/unit_uniform_mapping.cc
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
 * This file provides unit tests for the uniform mapping.
 */

#include <array>
#include <catch.hpp>
#include "tiledb/sm/dimension_label/uniform_mapping.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

template <typename T>
void require_range_is_equal(const Range& expected, const Range& result) {
  if (expected.empty() || result.empty()) {
    REQUIRE((expected.empty() && result.empty()));
    return;
  }
  auto expected_data = static_cast<const T*>(expected.data());
  auto result_data = static_cast<const T*>(result.data());
  CHECK(expected_data[0] == result_data[0]);
  REQUIRE(expected_data[1] == result_data[1]);
}

template <typename T>
void require_range_is_equal(const Range& result, const T start, const T end) {
  if (result.empty()) {
    REQUIRE(false);
    return;
  }
  auto result_data = static_cast<const T*>(result.data());
  CHECK(start == result_data[0]);
  REQUIRE(end == result_data[1]);
}

template <typename T>
Range create_range(const T start, const T stop) {
  std::array<T, 2> array{start, stop};
  return {array.data(), 2 * sizeof(T)};
}

TEMPLATE_TEST_CASE(
    "UniformMap from label [-1.5, 1.5] to index [0, 4]",
    "[dimension_label][uniform_label]",
    float,
    double) {
  const uint64_t n_min{0};
  const uint64_t n_max{4};
  const TestType x_min{-1.5};
  const TestType x_max{1.5};
  UniformMapping<TestType, uint64_t> dim_label{x_min, x_max, n_min, n_max};
  const auto index_domain = create_range<uint64_t>(n_min, n_max);
  const auto label_domain = create_range<TestType>(x_min, x_max);
  SECTION("Convert full data range") {
    auto result = dim_label.index_range(label_domain);
    require_range_is_equal<uint64_t>(result, 0, 4);
  }
  SECTION("Convert [-2.0, -0.5] -> [0, 1]") {
    const auto label_range = create_range<TestType>(-2.0, -0.5);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 0, 1);
  }
  SECTION("Convert [0.0, 0.0] -> [3, 3]") {
    const auto label_range = create_range<TestType>(0.0, 0.0);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 2, 2);
  }
  SECTION("Convert [0.5, 2.0] -> [3, 4]") {
    const auto label_range = create_range<TestType>(0.5, 2.0);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 3, 4);
  }
  SECTION("Exception [-3.5, -3.0] is out of bounds") {
    const auto label_range = create_range<TestType>(-3.5, -3.0);
    REQUIRE_THROWS(dim_label.index_range(label_range));
  }
  SECTION("Exception [3.0, 3.5] is out of bounds") {
    const auto label_range = create_range<TestType>(3.0, 3.5);
    REQUIRE_THROWS(dim_label.index_range(label_range));
  }
}

TEMPLATE_TEST_CASE(
    "UniformMap from label [-1.5, 1.5] to index [1, 5]",
    "[dimension_label][uniform_label]",
    float,
    double) {
  const uint64_t n_min{1};
  const uint64_t n_max{5};
  const TestType x_min{-1.5};
  const TestType x_max{1.5};
  UniformMapping<TestType, uint64_t> dim_label{x_min, x_max, n_min, n_max};
  const auto index_domain = create_range<uint64_t>(n_min, n_max);
  const auto label_domain = create_range<TestType>(x_min, x_max);
  SECTION("Convert full data range") {
    auto result = dim_label.index_range(label_domain);
    require_range_is_equal<uint64_t>(result, 1, 5);
  }
  SECTION("Convert [-2.0, -0.5] -> [0, 1]") {
    const auto label_range = create_range<TestType>(-2.0, -0.5);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 1, 2);
  }
  SECTION("Convert [0.0, 0.0] -> [3, 3]") {
    const auto label_range = create_range<TestType>(0.0, 0.0);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 3, 3);
  }
  SECTION("Convert [0.5, 2.0] -> [0, 1]") {
    const auto label_range = create_range<TestType>(0.5, 2.0);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 4, 5);
  }
  SECTION("Exception [-3.5, -3.0] is out of bounds") {
    const auto label_range = create_range<TestType>(-3.5, -3.0);
    REQUIRE_THROWS(dim_label.index_range(label_range));
  }
  SECTION("Exception [3.0, 3.5] is out of bounds") {
    const auto label_range = create_range<TestType>(3.0, 3.5);
    REQUIRE_THROWS(dim_label.index_range(label_range));
  }
}

TEMPLATE_TEST_CASE(
    "UniformMap on domain [-1.5, -1.5] with 1 grid point",
    "[dimension_label][uniform_label]",
    float,
    double) {
  const uint64_t n_min{1};
  const uint64_t n_max{1};
  const TestType x_min{-1.5};
  const TestType x_max{-1.5};
  UniformMapping<TestType, uint64_t> dim_label{x_min, x_max, n_min, n_max};
  const auto index_domain = create_range<uint64_t>(n_min, n_max);
  const auto label_domain = create_range<TestType>(x_min, x_max);
  SECTION("Convert full data range") {
    auto result = dim_label.index_range(label_domain);
    require_range_is_equal<uint64_t>(result, 1, 1);
  }
  SECTION("Convert larger than full range") {
    const auto label_range = create_range<TestType>(-2.0, 0.0);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 1, 1);
  }
}

TEMPLATE_TEST_CASE(
    "UniformMap on domain [10, 70] -> [0, 5] for all numeric label types",
    "[dimension_label][uniform_label]",
    uint8_t,
    int8_t,
    uint16_t,
    int16_t,
    uint32_t,
    int32_t,
    uint64_t,
    int64_t,
    float,
    double) {
  const uint64_t n_min{0};
  const uint64_t n_max{5};
  const TestType x_min{10};
  const TestType x_max{60};
  UniformMapping<TestType, uint64_t> dim_label{x_min, x_max, n_min, n_max};
  const auto index_domain = create_range<uint64_t>(n_min, n_max);
  const auto label_domain = create_range<TestType>(x_min, x_max);
  SECTION("Convert full data range") {
    auto result = dim_label.index_range(label_domain);
    require_range_is_equal<uint64_t>(result, 0, 5);
  }
  SECTION("Convert [1, 30] -> [0, 2]") {
    const auto label_range = create_range<TestType>(1, 30);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 0, 2);
  }
  SECTION("Convert [40, 40] -> [3, 3]") {
    const auto label_range = create_range<TestType>(40, 40);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 3, 3);
  }
  SECTION("Convert [45, 80] -> [4, 5]") {
    const auto label_range = create_range<TestType>(45, 80);
    const auto result = dim_label.index_range(label_range);
    require_range_is_equal<uint64_t>(result, 4, 5);
  }
  SECTION("Exception [0, 5] is out of bounds") {
    const auto label_range = create_range<TestType>(0, 5);
    REQUIRE_THROWS(dim_label.index_range(label_range));
  }
  SECTION("Exception [70, 75] is out of bounds") {
    const auto label_range = create_range<TestType>(70, 75);
    REQUIRE_THROWS(dim_label.index_range(label_range));
  }
}
