/**
 * @file tiledb/sm/subarray/unit_range_multi_subset.cc
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
 * This file defines unit tests for the RangeMultiSubset classes.
 */

#include <catch.hpp>
#include "../range_multi_subset.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

TEMPLATE_TEST_CASE_SIG(
    "Create RangeMultiSubset with implicitly defined range",
    "[range_multi_subset]",
    ((typename T, Datatype D), T, D),
    (int8_t, Datatype::INT8),
    (uint8_t, Datatype::UINT8),
    (int16_t, Datatype::INT16),
    (uint16_t, Datatype::UINT16),
    (int32_t, Datatype::INT32),
    (uint32_t, Datatype::UINT32),
    (int64_t, Datatype::INT64),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  T bounds[2] = {0, 10};
  Range range{bounds, 2 * sizeof(T)};
  RangeMultiSubset range_subset{D, range, true, false};
  CHECK(range_subset.num_ranges() == 1);
  Range default_range = range_subset[0];
  CHECK(!default_range.empty());
  auto start = (const T*)default_range.start();
  auto end = (const T*)default_range.end();
  CHECK(*start == bounds[0]);
  REQUIRE(*end == bounds[1]);
}

TEMPLATE_TEST_CASE_SIG(
    "RangeMultiSubset::add_range for coalescing types",
    "[range_multi_subset]",
    ((typename T, Datatype D), T, D),
    (int8_t, Datatype::INT8),
    (uint8_t, Datatype::UINT8),
    (int16_t, Datatype::INT16),
    (uint16_t, Datatype::UINT16),
    (int32_t, Datatype::INT32),
    (uint32_t, Datatype::UINT32),
    (int64_t, Datatype::INT64),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::DATETIME_YEAR)) {
  T bounds[2] = {0, 10};
  Range range{bounds, 2 * sizeof(T)};
  RangeMultiSubset range_subset{D, range, false, true};
  CHECK(range_subset.num_ranges() == 0);
  SECTION("Add 2 Overlapping Ranges") {
    T data1[2] = {1, 3};
    T data2[2] = {4, 5};
    Range r1{data1, 2 * sizeof(T)};
    Range r2{data2, 2 * sizeof(T)};
    range_subset.add_subset(r1);
    range_subset.add_subset(r2);
    CHECK(range_subset.num_ranges() == 1);
    auto combined_range = range_subset[0];
    auto start = (const T*)combined_range.start();
    auto end = (const T*)combined_range.end();
    CHECK(*start == data1[0]);
    REQUIRE(*end == data2[1]);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "RangeMultiSubset::add_range for non-coalescing floating-point types",
    "[range_multi_subset]",
    ((typename T, Datatype D), T, D),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  T bounds[2] = {-1.0, 1.0};
  Range range{bounds, 2 * sizeof(T)};
  RangeMultiSubset range_subset{D, range, false, true};
  REQUIRE(range_subset.num_ranges() == 0);
  SECTION("Add 2 Overlapping Ranges") {
    T data1[2] = {-0.5, 0.5};
    T data2[2] = {0.5, 0.75};
    Range r1{data1, 2 * sizeof(T)};
    Range r2{data2, 2 * sizeof(T)};
    range_subset.add_subset(r1);
    range_subset.add_subset(r2);
    REQUIRE(range_subset.num_ranges() == 2);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "Test RangeMultiSubset numeric sort",
    "[range_multi_subset][threadpool]",
    ((typename T, Datatype D), T, D),
    (int8_t, Datatype::INT8),
    (uint8_t, Datatype::UINT8),
    (int16_t, Datatype::INT16),
    (uint16_t, Datatype::UINT16),
    (int32_t, Datatype::INT32),
    (uint32_t, Datatype::UINT32),
    (int64_t, Datatype::INT64),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  T bounds[2] = {0, 10};
  Range range{bounds, 2 * sizeof(T)};
  RangeMultiSubset range_subset{D, range, false, true};
  SECTION("Sort 2 reverse ordered ranges") {
    // Add ranges.
    T data1[2] = {4, 5};
    T data2[2] = {1, 2};
    Range r1{data1, 2 * sizeof(T)};
    Range r2{data2, 2 * sizeof(T)};
    range_subset.add_subset(r1);
    range_subset.add_subset(r2);
    CHECK(range_subset.num_ranges() == 2);
    // Create ThreadPool.
    ThreadPool pool;
    REQUIRE(pool.init(2).ok());
    //  Sort ranges.
    auto sort_status = range_subset.sort_ranges(&pool);
    CHECK(sort_status.ok());
    // Check the numner of ranges.
    CHECK(range_subset.num_ranges() == 2);
    // Check the first range.
    auto result_range = range_subset[0];
    auto start = (const T*)result_range.start();
    auto end = (const T*)result_range.end();
    CHECK(*start == data2[0]);
    REQUIRE(*end == data2[1]);
    // Check the second range.
    result_range = range_subset[1];
    start = (const T*)result_range.start();
    end = (const T*)result_range.end();
    CHECK(*start == data1[0]);
    REQUIRE(*end == data1[1]);
  }
}

TEST_CASE("RangeMultiSubset::sort - STRING_ASCII") {
  Range range{};
  RangeMultiSubset range_subset{Datatype::STRING_ASCII, range, false, false};
  SECTION("Sort 2 reverse ordered non-overlapping ranges") {
    // Set ranges.
    const std::string d1{"cat"};
    const std::string d2{"dog"};
    const std::string d3{"ax"};
    const std::string d4{"bird"};
    Range r1{};
    r1.set_str_range(d1, d2);
    range_subset.add_subset(r1);
    Range r2{};
    r2.set_str_range(d3, d4);
    range_subset.add_subset(r2);
    CHECK(range_subset.num_ranges() == 2);
    // Create ThreadPool.
    ThreadPool pool;
    REQUIRE(pool.init(2).ok());
    // Sort ranges.
    auto sort_status = range_subset.sort_ranges(&pool);
    CHECK(sort_status.ok());
    // Check the number of ranges.
    CHECK(range_subset.num_ranges() == 2);
    // Check the first range.
    auto result_range = range_subset[0];
    auto start = result_range.start_str();
    auto end = result_range.end_str();
    CHECK(start == d3);
    CHECK(end == d4);
    // Check the second range.
    result_range = range_subset[1];
  }
}
