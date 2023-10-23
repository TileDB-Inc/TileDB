/**
 * @file unit_aggregate_with_count.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Tests the `AggregateWithCount` class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_with_count.h"
#include "tiledb/sm/query/readers/aggregators/safe_sum.h"
#include "tiledb/sm/query/readers/aggregators/sum_type.h"
#include "tiledb/sm/query/readers/aggregators/validity_policies.h"

#include <test/support/src/helper_type.h>
#include <test/support/tdb_catch.h>

using namespace tiledb::sm;
using namespace tiledb::test;

typedef tuple<
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double>
    FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Aggregate with count: safe sum",
    "[aggregate-with-count][safe-sum]",
    FixedTypesUnderTest) {
  typedef TestType T;
  AggregateWithCount<T, typename sum_type_data<T>::sum_type, SafeSum, NonNull>
      aggregator(FieldInfo("a1", false, false, 1, tdb_type<T>));
  AggregateWithCount<T, typename sum_type_data<T>::sum_type, SafeSum, NonNull>
      aggregator_nullable(FieldInfo("a2", false, true, 1, tdb_type<T>));

  std::vector<T> fixed_data = {1, 2, 3, 4, 5, 5, 4, 3, 2, 1};
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    // Regular attribute.
    AggregateBuffer input_data{
        2, 10, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};
    auto res = aggregator.template aggregate<uint8_t>(input_data);
    CHECK(std::get<0>(res) == 27);
    CHECK(std::get<1>(res) == 8);

    // Nullable attribute.
    AggregateBuffer input_data2{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        false,
        nullopt,
        0};
    auto res_nullable =
        aggregator_nullable.template aggregate<uint8_t>(input_data2);
    CHECK(std::get<0>(res_nullable) == 14);
    CHECK(std::get<1>(res_nullable) == 4);
  }

  SECTION("Regular bitmap") {
    // Regular attribute.
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data{
        2, 10, fixed_data.data(), nullopt, nullopt, false, bitmap.data(), 0};
    auto res = aggregator.template aggregate<uint8_t>(input_data);
    CHECK(std::get<0>(res) == 11);
    CHECK(std::get<1>(res) == 3);

    AggregateBuffer input_data2{
        0, 2, fixed_data.data(), nullopt, nullopt, false, bitmap.data(), 0};
    auto res2 = aggregator.template aggregate<uint8_t>(input_data2);
    CHECK(std::get<0>(res2) == 3);
    CHECK(std::get<1>(res2) == 2);

    // Nullable attribute.
    AggregateBuffer input_data3{
        0,
        2,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        false,
        nullopt,
        0};
    auto res_nullable =
        aggregator_nullable.template aggregate<uint8_t>(input_data3);
    CHECK(std::get<0>(res_nullable) == 0);
    CHECK(std::get<1>(res_nullable) == 0);

    AggregateBuffer input_data4{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        false,
        bitmap.data(),
        0};
    auto res_nullable2 =
        aggregator_nullable.template aggregate<uint8_t>(input_data4);
    CHECK(std::get<0>(res_nullable2) == 6);
    CHECK(std::get<1>(res_nullable2) == 2);
  }

  SECTION("Count bitmap") {
    // Regular attribute.
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data{
        2,
        10,
        fixed_data.data(),
        nullopt,
        nullopt,
        true,
        bitmap_count.data(),
        0};
    auto res = aggregator.template aggregate<uint64_t>(input_data);
    CHECK(std::get<0>(res) == 29);
    CHECK(std::get<1>(res) == 10);

    AggregateBuffer input_data2{
        0,
        2,
        fixed_data.data(),
        nullopt,
        nullopt,
        true,
        bitmap_count.data(),
        0};
    auto res2 = aggregator.template aggregate<uint64_t>(input_data2);
    CHECK(std::get<0>(res2) == 5);
    CHECK(std::get<1>(res2) == 3);

    // Nullable attribute.
    AggregateBuffer input_data3{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        true,
        bitmap_count.data(),
        0};
    auto res_nullable =
        aggregator_nullable.template aggregate<uint64_t>(input_data3);
    CHECK(std::get<0>(res_nullable) == 22);
    CHECK(std::get<1>(res_nullable) == 7);

    AggregateBuffer input_data4{
        0,
        2,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        true,
        bitmap_count.data(),
        0};
    auto res_nullable2 =
        aggregator_nullable.template aggregate<uint64_t>(input_data4);
    CHECK(std::get<0>(res_nullable2) == 0);
    CHECK(std::get<1>(res_nullable2) == 0);
  }
}
