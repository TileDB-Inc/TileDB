/**
 * @file tiledb/sm/subarray/unit_range_subset.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * This file defines unit tests for the RangeSetAndSuperset classes.
 */

#include <test/support/tdb_catch.h>
#include "../range_subset.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/type/range/range.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::type;

/**
 * Check the values of a particular range in a RangeSetAndSuperset object.
 *
 * @param index The index of the range.
 */
template <typename T>
void check_subset_range_values(
    const RangeSetAndSuperset& subset,
    const uint64_t index,
    const T expected_start,
    const T expected_end) {
  REQUIRE(index < subset.num_ranges());
  auto range = subset[index];
  REQUIRE(!range.empty());
  auto result_data = static_cast<const T*>(range.data());
  CHECK(result_data[0] == expected_start);
  CHECK(result_data[1] == expected_end);
}

/**
 * Check the values of a particular range in a RangeSetAndSuperset object.
 *
 * @param index The index of the range.
 */
void check_subset_range_strings(
    const RangeSetAndSuperset& subset,
    const uint64_t index,
    const std::string expected_start,
    const std::string expected_end) {
  REQUIRE(index < subset.num_ranges());
  auto range = subset[index];
  REQUIRE(!range.empty());
  CHECK(range.start_str() == expected_start);
  CHECK(range.end_str() == expected_end);
}

TEMPLATE_TEST_CASE_SIG(
    "Create RangeSetAndSuperset with implicitly defined range",
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
  RangeSetAndSuperset range_subset{D, range, true, false};
  CHECK(range_subset.num_ranges() == 1);
  Range default_range = range_subset[0];
  CHECK(!default_range.empty());
  auto result_data = (const T*)default_range.data();
  CHECK(result_data[0] == bounds[0]);
  REQUIRE(result_data[1] == bounds[1]);
}

TEMPLATE_TEST_CASE_SIG(
    "RangeSetAndSuperset::add_range for coalescing types",
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
  RangeSetAndSuperset range_subset{D, range, false, true};
  CHECK(range_subset.num_ranges() == 0);
  SECTION("Add 2 Overlapping Ranges") {
    T data1[2] = {1, 3};
    T data2[2] = {4, 5};
    Range r1{data1, 2 * sizeof(T)};
    Range r2{data2, 2 * sizeof(T)};
    range_subset.add_range(r1);
    range_subset.add_range(r2);
    CHECK(range_subset.num_ranges() == 1);
    auto combined_range = range_subset[0];
    auto result_data = (const T*)combined_range.data();
    CHECK(result_data[0] == data1[0]);
    REQUIRE(result_data[1] == data2[1]);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "RangeSetAndSuperset::add_range for non-coalescing floating-point types",
    "[range_multi_subset]",
    ((typename T, Datatype D), T, D),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  T bounds[2] = {-1.0, 1.0};
  Range range{bounds, 2 * sizeof(T)};
  RangeSetAndSuperset range_subset{D, range, false, true};
  REQUIRE(range_subset.num_ranges() == 0);
  SECTION("Add 2 Overlapping Ranges") {
    T data1[2] = {-0.5, 0.5};
    T data2[2] = {0.5, 0.75};
    Range r1{data1, 2 * sizeof(T)};
    Range r2{data2, 2 * sizeof(T)};
    range_subset.add_range(r1);
    range_subset.add_range(r2);
    REQUIRE(range_subset.num_ranges() == 2);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "RangeSetAndSuperset::add_range test intersect for unsigned int types",
    "[range]",
    ((typename T, Datatype D), T, D),
    (uint8_t, Datatype::UINT8),
    (uint16_t, Datatype::UINT16),
    (uint32_t, Datatype::UINT32),
    (uint64_t, Datatype::UINT64)) {
  T domain_data[2]{1, 4};
  Range domain{domain_data, 2 * sizeof(T)};
  RangeSetAndSuperset subset{D, domain, false, true};
  SECTION("Test adding subset with lower bound less than superset") {
    T bad_lower[2]{0, 3};
    Range range{bad_lower, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    REQUIRE(warn_message.has_value());
    INFO(warn_message.value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, 1, 3);
  }
  SECTION("Test adding a subset with upper bound more than superset") {
    T bad_upper[2]{2, 8};
    Range range{bad_upper, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    REQUIRE(warn_message.has_value());
    INFO(warn_message.value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, 2, 4);
  }
  SECTION("Test adding the full typeset") {
    T fullset[2]{std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
    Range range{fullset, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    CHECK(warn_message.has_value());
    INFO(warn_message.value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, 1, 4);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "RangeSetAndSuperset::add_range test intersect for signed int types",
    "[range]",
    ((typename T, Datatype D), T, D),
    (int8_t, Datatype::INT8),
    (int16_t, Datatype::INT16),
    (int32_t, Datatype::INT32),
    (int64_t, Datatype::INT64),
    (int64_t, Datatype::DATETIME_MONTH)) {
  T domain_data[2]{-2, 2};
  Range domain{domain_data, 2 * sizeof(T)};
  RangeSetAndSuperset subset{D, domain, false, true};
  SECTION("Test adding subset with lower bound less than superset") {
    T bad_lower[2]{-4, 0};
    Range range{bad_lower, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    CHECK(warn_message.has_value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, -2, 0);
  }
  SECTION("Test adding a subset with upper bound more than superset") {
    T bad_upper[2]{0, 8};
    Range range{bad_upper, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    CHECK(warn_message.has_value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, 0, 2);
  }
  SECTION("Test adding the full typeset") {
    T fullset[2]{std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
    Range range{fullset, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    CHECK(warn_message.has_value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, -2, 2);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "RangeSetAndSuperset::add_range test intersect for floating-point types",
    "[range]",
    ((typename T, Datatype D), T, D),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  T domain_data[2]{-1.5, 4.5};
  Range domain{domain_data, 2 * sizeof(T)};
  RangeSetAndSuperset subset{D, domain, false, true};
  SECTION("Test adding subset with lower bound less than superset") {
    T bad_lower[2]{-2.0, 3.0};
    Range range{bad_lower, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    CHECK(warn_message.has_value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, -1.5, 3.0);
  }
  SECTION("Test adding a subset with upper bound more than superset") {
    T bad_upper[2]{2.0, 8.0};
    Range range{bad_upper, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    CHECK(warn_message.has_value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, 2.0, 4.5);
  }
  SECTION("Test adding the full typeset") {
    T fullset[2]{
        std::numeric_limits<T>::lowest(), std::numeric_limits<T>::max()};
    Range range{fullset, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    CHECK(warn_message.has_value());
    REQUIRE(subset.num_ranges() == 1);
    INFO("DATA: " << fullset[0] << ", " << fullset[1]);
    check_subset_range_values<T>(subset, 0, -1.5, 4.5);
  }
  SECTION("Test adding infinite range") {
    T infinite[2]{
        -std::numeric_limits<T>::infinity(),
        std::numeric_limits<T>::infinity()};
    Range range{infinite, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, false);
    CHECK(error_status.ok());
    CHECK(warn_message.has_value());
    REQUIRE(subset.num_ranges() == 1);
    check_subset_range_values<T>(subset, 0, -1.5, 4.5);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "RangeSetAndSuperset::add_range - error status for invalid ranges",
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
  RangeSetAndSuperset subset{D, range, true, false};
  SECTION("Check error for empty range") {
    Range range{};
    REQUIRE_THROWS(subset.add_range(range, true));
  }
  SECTION("Check error for out-of-order range") {
    T data[2] = {3, 2};
    Range range{data, 2 * sizeof(T)};
    REQUIRE_THROWS(subset.add_range(range, true));
  }
  SECTION("Check error for out-of-bounds range") {
    T data[2] = {0, 11};
    Range range{data, 2 * sizeof(T)};
    auto&& [error_status, warn_message] = subset.add_range(range, true);
    REQUIRE(!error_status.ok());
  }
}

TEMPLATE_TEST_CASE_SIG(
    "RangeSetAndSuperset::sort_and_merge_ranges - numeric",
    "[sort_and_merge_ranges][numeric]",
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
  bool merge = GENERATE(true, false);
  T bounds[2] = {0, 10};
  Range range{bounds, 2 * sizeof(T)};
  RangeSetAndSuperset range_subset{D, range, false, true};

  SECTION("Empty ranges") {
    CHECK(range_subset.num_ranges() == 0);

    // Try to sort and merge ranges.
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results.
    CHECK(range_subset.num_ranges() == 0);
  }

  SECTION("Adjacent, sorted ranges") {
    // Add ranges.
    T data1[2] = {0, 1};
    T data2[2] = {2, 3};
    T data3[2] = {4, 5};
    T data4[2] = {6, 7};
    std::vector<Range> ranges = {
        Range(data1, 2 * sizeof(T)),
        Range(data2, 2 * sizeof(T)),
        Range(data3, 2 * sizeof(T)),
        Range(data4, 2 * sizeof(T))};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }

    // Integer-type ranges will coalesce and needn't be merged.
    // Float-type ranges cannot coalesce and will only be sorted.
    if constexpr (D == Datatype::FLOAT32 || D == Datatype::FLOAT64) {
      CHECK(range_subset.num_ranges() == 4);
      ThreadPool pool{2};
      range_subset.sort_and_merge_ranges(&pool, merge);
      CHECK(range_subset.num_ranges() == 4);
      check_subset_range_values(range_subset, 0, data1[0], data1[1]);
      check_subset_range_values(range_subset, 1, data2[0], data2[1]);
      check_subset_range_values(range_subset, 2, data3[0], data3[1]);
      check_subset_range_values(range_subset, 3, data4[0], data4[1]);
    } else {
      CHECK(range_subset.num_ranges() == 1);
      check_subset_range_values(range_subset, 0, data1[0], data4[1]);
    }
  }

  SECTION("Adjacent, unsorted ranges") {
    // Add ranges.
    T data1[2] = {0, 1};
    T data2[2] = {4, 5};
    T data3[2] = {2, 3};
    T data4[2] = {6, 7};
    std::vector<Range> ranges = {
        Range(data1, 2 * sizeof(T)),
        Range(data2, 2 * sizeof(T)),
        Range(data3, 2 * sizeof(T)),
        Range(data4, 2 * sizeof(T))};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 4);

    // Sort and merge ranges.
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results.
    if constexpr (D == Datatype::FLOAT32 || D == Datatype::FLOAT64) {
      CHECK(range_subset.num_ranges() == 4);
      check_subset_range_values(range_subset, 0, data1[0], data1[1]);
      check_subset_range_values(range_subset, 1, data3[0], data3[1]);
      check_subset_range_values(range_subset, 2, data2[0], data2[1]);
      check_subset_range_values(range_subset, 3, data4[0], data4[1]);
    } else {
      if (merge) {
        CHECK(range_subset.num_ranges() == 1);
        check_subset_range_values(range_subset, 0, data1[0], data4[1]);
      } else {
        CHECK(range_subset.num_ranges() == 4);
        check_subset_range_values(range_subset, 0, data1[0], data1[1]);
        check_subset_range_values(range_subset, 1, data3[0], data3[1]);
        check_subset_range_values(range_subset, 2, data2[0], data2[1]);
        check_subset_range_values(range_subset, 3, data4[0], data4[1]);
      }
    }
  }

  SECTION("Overlapping, sorted ranges") {
    // Add ranges.
    T data1[2] = {0, 2};
    T data2[2] = {1, 4};
    T data3[2] = {3, 6};
    T data4[2] = {5, 8};
    std::vector<Range> ranges = {
        Range(data1, 2 * sizeof(T)),
        Range(data2, 2 * sizeof(T)),
        Range(data3, 2 * sizeof(T)),
        Range(data4, 2 * sizeof(T))};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 4);

    // Sort and merge ranges.
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results.
    if (merge) {
      CHECK(range_subset.num_ranges() == 1);
      check_subset_range_values(range_subset, 0, data1[0], data4[1]);
    } else {
      CHECK(range_subset.num_ranges() == 4);
      check_subset_range_values(range_subset, 0, data1[0], data1[1]);
      check_subset_range_values(range_subset, 1, data2[0], data2[1]);
      check_subset_range_values(range_subset, 2, data3[0], data3[1]);
      check_subset_range_values(range_subset, 3, data4[0], data4[1]);
    }
  }

  SECTION("Overlapping, unsorted ranges") {
    // Add ranges.
    T data1[2] = {0, 2};
    T data2[2] = {5, 8};
    T data3[2] = {3, 6};
    T data4[2] = {1, 4};
    std::vector<Range> ranges = {
        Range(data1, 2 * sizeof(T)),
        Range(data2, 2 * sizeof(T)),
        Range(data3, 2 * sizeof(T)),
        Range(data4, 2 * sizeof(T))};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 4);

    // Sort and merge ranges.
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results.
    if (merge) {
      CHECK(range_subset.num_ranges() == 1);
      check_subset_range_values(range_subset, 0, data1[0], data2[1]);
    } else {
      CHECK(range_subset.num_ranges() == 4);
      check_subset_range_values(range_subset, 0, data1[0], data1[1]);
      check_subset_range_values(range_subset, 1, data4[0], data4[1]);
      check_subset_range_values(range_subset, 2, data3[0], data3[1]);
      check_subset_range_values(range_subset, 3, data2[0], data2[1]);
    }
  }

  SECTION("Partially overlapping") {
    // Add ranges.
    T data1[2] = {0, 2};
    T data2[2] = {1, 4};
    T data3[2] = {7, 9};
    T data4[2] = {3, 5};
    std::vector<Range> ranges = {
        Range(data1, 2 * sizeof(T)),
        Range(data2, 2 * sizeof(T)),
        Range(data3, 2 * sizeof(T)),
        Range(data4, 2 * sizeof(T))};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 4);

    // Sort and merge ranges.
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results.
    if (merge) {
      CHECK(range_subset.num_ranges() == 2);
      check_subset_range_values(range_subset, 0, data1[0], data4[1]);
      check_subset_range_values(range_subset, 1, data3[0], data3[1]);
    } else {
      CHECK(range_subset.num_ranges() == 4);
      check_subset_range_values(range_subset, 0, data1[0], data1[1]);
      check_subset_range_values(range_subset, 1, data2[0], data2[1]);
      check_subset_range_values(range_subset, 2, data4[0], data4[1]);
      check_subset_range_values(range_subset, 3, data3[0], data3[1]);
    }
  }

  SECTION("Overlapping, encompassing ranges") {
    // Note: this test is intended to duplicate regression test sc-53970,
    // validating that a range which is fully encompassed within another
    // will merge as expected.

    // Add ranges.
    T data1[2] = {3, 3};
    T data2[2] = {1, 10};
    std::vector<Range> ranges = {
        Range(data1, 2 * sizeof(T)), Range(data2, 2 * sizeof(T))};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 2);

    // Sort and merge ranges.
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results.
    if (merge) {
      CHECK(range_subset.num_ranges() == 1);
      check_subset_range_values(range_subset, 0, data2[0], data2[1]);
    } else {
      CHECK(range_subset.num_ranges() == 2);
      check_subset_range_values(range_subset, 0, data2[0], data2[1]);
      check_subset_range_values(range_subset, 1, data1[0], data1[1]);
    }
  }
}

TEST_CASE(
    "RangeSetAndSuperset::sort_and_merge_ranges - STRING_ASCII",
    "[sort_and_merge_ranges][string]") {
  bool merge = GENERATE(true, false);
  Range range{};
  RangeSetAndSuperset range_subset{Datatype::STRING_ASCII, range, false, false};

  SECTION("Empty ranges") {
    CHECK(range_subset.num_ranges() == 0);

    // Try to sort and merge ranges.
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results.
    CHECK(range_subset.num_ranges() == 0);
  }

  SECTION("Adjacent, sorted ranges") {
    // Note: string ranges do not coalesce
    std::vector<Range> ranges = {
        Range("a", "b"), Range("c", "d"), Range("ef", "g"), Range("h", "ij")};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 4);
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results
    CHECK(range_subset.num_ranges() == 4);
    check_subset_range_strings(range_subset, 0, "a", "b");
    check_subset_range_strings(range_subset, 1, "c", "d");
    check_subset_range_strings(range_subset, 2, "ef", "g");
    check_subset_range_strings(range_subset, 3, "h", "ij");
  }

  SECTION("Adjacent, unsorted ranges") {
    std::vector<Range> ranges = {Range("c", "d"), Range("a", "b")};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 2);
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results
    CHECK(range_subset.num_ranges() == 2);
    check_subset_range_strings(range_subset, 0, "a", "b");
    check_subset_range_strings(range_subset, 1, "c", "d");
  }

  SECTION("Overlapping, sorted ranges") {
    std::vector<Range> ranges = {Range("a", "c"), Range("b", "d")};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 2);
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results
    if (merge) {
      CHECK(range_subset.num_ranges() == 1);
      check_subset_range_strings(range_subset, 0, "a", "d");
    } else {
      CHECK(range_subset.num_ranges() == 2);
      check_subset_range_strings(range_subset, 0, "a", "c");
      check_subset_range_strings(range_subset, 1, "b", "d");
    }
  }

  SECTION("Overlapping, unsorted ranges") {
    std::vector<Range> ranges = {Range("b", "d"), Range("a", "c")};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 2);
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results
    if (merge) {
      CHECK(range_subset.num_ranges() == 1);
      check_subset_range_strings(range_subset, 0, "a", "d");
    } else {
      CHECK(range_subset.num_ranges() == 2);
      check_subset_range_strings(range_subset, 0, "a", "c");
      check_subset_range_strings(range_subset, 1, "b", "d");
    }
  }

  SECTION("Partially overlapping") {
    std::vector<Range> ranges = {
        Range("a", "c"), Range("b", "d"), Range("h", "j"), Range("e", "f")};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 4);
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results
    if (merge) {
      CHECK(range_subset.num_ranges() == 3);
      check_subset_range_strings(range_subset, 0, "a", "d");
      check_subset_range_strings(range_subset, 1, "e", "f");
      check_subset_range_strings(range_subset, 2, "h", "j");
    } else {
      CHECK(range_subset.num_ranges() == 4);
      check_subset_range_strings(range_subset, 0, "a", "c");
      check_subset_range_strings(range_subset, 1, "b", "d");
      check_subset_range_strings(range_subset, 2, "e", "f");
      check_subset_range_strings(range_subset, 3, "h", "j");
    }
  }

  SECTION("Same first letter") {
    std::vector<Range> ranges = {
        Range("G1", "G1"), Range("G2", "G2"), Range("G59", "G59")};
    for (auto range : ranges) {
      range_subset.add_range(range);
    }
    CHECK(range_subset.num_ranges() == 3);
    ThreadPool pool{2};
    range_subset.sort_and_merge_ranges(&pool, merge);

    // Check range results
    if (merge) {
      CHECK(range_subset.num_ranges() == 3);
      check_subset_range_strings(range_subset, 0, "G1", "G1");
      check_subset_range_strings(range_subset, 1, "G2", "G2");
      check_subset_range_strings(range_subset, 2, "G59", "G59");
    } else {
      CHECK(range_subset.num_ranges() == 3);
      check_subset_range_strings(range_subset, 0, "G1", "G1");
      check_subset_range_strings(range_subset, 1, "G2", "G2");
      check_subset_range_strings(range_subset, 2, "G59", "G59");
    }
  }
}

TEST_CASE("RangeSetAndSuperset test bad constructor args") {
  Range range{};
  REQUIRE_THROWS(RangeSetAndSuperset(Datatype::ANY, range, false, false));
}
