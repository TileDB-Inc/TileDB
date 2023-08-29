/**
 * @file unit_sum.cc
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
 * Tests the `SumAggregator` class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;

TEST_CASE("Sum aggregator: constructor", "[sum-aggregator][constructor]") {
  SECTION("Var size") {
    CHECK_THROWS_WITH(
        SumAggregator<uint8_t>(FieldInfo("a1", true, false, 1)),
        "SumAggregator: Sum aggregates must not be requested for var sized "
        "attributes.");
  }

  SECTION("Invalid cell val num") {
    CHECK_THROWS_WITH(
        SumAggregator<uint8_t>(FieldInfo("a1", false, false, 2)),
        "SumAggregator: Sum aggregates must not be requested for attributes "
        "with more than one value.");
  }
}

TEST_CASE("Sum aggregator: var sized", "[sum-aggregator][var-sized]") {
  SumAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.var_sized() == false);
}

TEST_CASE(
    "Sum aggregator: need recompute", "[sum-aggregator][need-recompute]") {
  SumAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.need_recompute_on_overflow() == true);
}

TEST_CASE("Sum aggregator: field name", "[sum-aggregator][field-name]") {
  SumAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.field_name() == "a1");
}

TEST_CASE(
    "Sum aggregator: Validate buffer", "[sum-aggregator][validate-buffer]") {
  SumAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  SumAggregator<uint8_t> aggregator_nullable(FieldInfo("a2", false, true, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  SECTION("Doesn't exist") {
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Sum", buffers),
        "SumAggregator: Result buffer doesn't exist.");
  }

  SECTION("Null data buffer") {
    buffers["Sum"].buffer_ = nullptr;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Sum", buffers),
        "SumAggregator: Sum aggregates must have a fixed size buffer.");
  }

  SECTION("Wrong size") {
    uint64_t sum = 0;
    buffers["Sum"].buffer_ = &sum;
    buffers["Sum"].original_buffer_size_ = 1;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Sum", buffers),
        "SumAggregator: Sum aggregates fixed size buffer should be for one "
        "element only.");
  }

  SECTION("With var buffer") {
    uint64_t sum = 0;
    buffers["Sum"].buffer_ = &sum;
    buffers["Sum"].original_buffer_size_ = 8;
    buffers["Sum"].buffer_var_ = &sum;

    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Sum", buffers),
        "SumAggregator: Sum aggregates must not have a var buffer.");
  }

  SECTION("With validity") {
    uint64_t sum = 0;
    buffers["Sum"].buffer_ = &sum;
    buffers["Sum"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Sum"].validity_vector_ = ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Sum", buffers),
        "SumAggregator: Sum aggregates for non nullable attributes must not "
        "have a validity buffer.");
  }

  SECTION("With no validity") {
    uint64_t sum = 0;
    buffers["Sum"].buffer_ = &sum;
    buffers["Sum"].original_buffer_size_ = 8;

    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Sum", buffers),
        "SumAggregator: Sum aggregates for nullable attributes must have a "
        "validity buffer.");
  }

  SECTION("Wrong validity size") {
    uint64_t sum = 0;
    buffers["Sum"].buffer_ = &sum;
    buffers["Sum"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 2;
    buffers["Sum"].validity_vector_ = ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Sum", buffers),
        "SumAggregator: Sum aggregates validity vector should "
        "be for one element only.");
  }

  SECTION("Success") {
    uint64_t sum = 0;
    buffers["Sum"].buffer_ = &sum;
    buffers["Sum"].original_buffer_size_ = 8;
    aggregator.validate_output_buffer("Sum", buffers);
  }

  SECTION("Success nullable") {
    uint64_t sum = 0;
    buffers["Sum"].buffer_ = &sum;
    buffers["Sum"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Sum"].validity_vector_ = ValidityVector(&validity, &validity_size);

    aggregator_nullable.validate_output_buffer("Sum", buffers);
  }
}

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
    "Sum aggregator: Basic aggregation",
    "[sum-aggregator][basic-aggregation]",
    FixedTypesUnderTest) {
  typedef TestType T;
  SumAggregator<T> aggregator(FieldInfo("a1", false, false, 1));
  SumAggregator<T> aggregator_nullable(FieldInfo("a2", false, true, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  typename sum_type_data<T>::sum_type sum = 0;
  buffers["Sum"].buffer_ = &sum;
  buffers["Sum"].original_buffer_size_ = 8;

  typename sum_type_data<T>::sum_type sum2 = 0;
  uint8_t validity = 0;
  uint64_t validity_size = 1;
  buffers["Sum2"].buffer_ = &sum2;
  buffers["Sum2"].original_buffer_size_ = 8;
  buffers["Sum2"].validity_vector_ = ValidityVector(&validity, &validity_size);

  std::vector<T> fixed_data = {1, 2, 3, 4, 5, 5, 4, 3, 2, 1};
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    // Regular attribute.
    AggregateBuffer input_data{
        2, 10, fixed_data.data(), nullopt, nullopt, false, nullopt};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == 27);

    // Nullable attribute.
    AggregateBuffer input_data2{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        false,
        nullopt};
    aggregator_nullable.aggregate_data(input_data2);
    aggregator_nullable.copy_to_user_buffer("Sum2", buffers);
    CHECK(sum2 == 14);
    CHECK(validity == 1);
  }

  SECTION("Regular bitmap") {
    // Regular attribute.
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data{
        2, 10, fixed_data.data(), nullopt, nullopt, false, bitmap.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == 11);

    AggregateBuffer input_data2{
        0, 2, fixed_data.data(), nullopt, nullopt, false, bitmap.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == 14);

    // Nullable attribute.
    AggregateBuffer input_data3{
        0, 2, fixed_data.data(), nullopt, validity_data.data(), false, nullopt};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("Sum2", buffers);
    CHECK(sum2 == 0);
    CHECK(validity == 0);

    AggregateBuffer input_data4{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        false,
        bitmap.data()};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("Sum2", buffers);
    CHECK(sum2 == 6);
    CHECK(validity == 1);
  }

  SECTION("Count bitmap") {
    // Regular attribute.
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data{
        2, 10, fixed_data.data(), nullopt, nullopt, true, bitmap_count.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == 29);

    AggregateBuffer input_data2{
        0, 2, fixed_data.data(), nullopt, nullopt, true, bitmap_count.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == 34);

    // Nullable attribute.
    AggregateBuffer input_data3{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("Sum2", buffers);
    CHECK(sum2 == 22);
    CHECK(validity == 1);

    AggregateBuffer input_data4{
        0,
        2,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("Sum2", buffers);
    CHECK(sum2 == 22);
    CHECK(validity == 1);
  }
}

TEST_CASE(
    "Sum aggregator: signed overflow", "[sum-aggregator][signed-overflow]") {
  SumAggregator<int64_t> aggregator(FieldInfo("a1", false, false, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  int64_t sum = 0;
  buffers["Sum"].buffer_ = &sum;
  buffers["Sum"].original_buffer_size_ = 8;

  std::vector<int64_t> fixed_data = {
      1,
      std::numeric_limits<int64_t>::max() - 2,
      -1,
      std::numeric_limits<int64_t>::min() + 2};

  AggregateBuffer input_data_plus_one{
      0, 1, fixed_data.data(), nullopt, nullopt, false, nullopt};

  AggregateBuffer input_data_minus_one{
      2, 3, fixed_data.data(), nullopt, nullopt, false, nullopt};

  SECTION("Overflow") {
    // First sum doesn't overflow.
    AggregateBuffer input_data{
        0, 2, fixed_data.data(), nullopt, nullopt, false, nullopt};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max() - 1);

    // Reached max but still hasn't overflowed.
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());

    // We can still subtract.
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max() - 1);

    // Now cause an overflow.
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());

    // Once we overflow, the value doesn't change.
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());
  }

  SECTION("Underflow") {
    // First sum doesn't underflow.
    AggregateBuffer input_data{
        2, 4, fixed_data.data(), nullopt, nullopt, false, nullopt};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::min() + 1);

    // Reached min but still hasn't underflowed.
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::min());

    // We can still subtract.
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::min() + 1);

    // Now cause an underflow.
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());

    // Once we underflow, the value doesn't change.
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());
  }
}

TEST_CASE(
    "Sum aggregator: unsigned overflow",
    "[sum-aggregator][unsigned-overflow]") {
  SumAggregator<uint64_t> aggregator(FieldInfo("a1", false, false, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  uint64_t sum = 0;
  buffers["Sum"].buffer_ = &sum;
  buffers["Sum"].original_buffer_size_ = 8;

  std::vector<uint64_t> fixed_data = {
      1, std::numeric_limits<uint64_t>::max() - 2};

  AggregateBuffer input_data_plus_one{
      0, 1, fixed_data.data(), nullopt, nullopt, false, nullopt};

  // First sum doesn't overflow.
  AggregateBuffer input_data{
      0, 2, fixed_data.data(), nullopt, nullopt, false, nullopt};
  aggregator.aggregate_data(input_data);
  aggregator.copy_to_user_buffer("Sum", buffers);
  CHECK(sum == std::numeric_limits<uint64_t>::max() - 1);

  // Reached max but still hasn't overflowed.
  aggregator.aggregate_data(input_data_plus_one);
  aggregator.copy_to_user_buffer("Sum", buffers);
  CHECK(sum == std::numeric_limits<uint64_t>::max());

  // Now cause an overflow.
  aggregator.aggregate_data(input_data_plus_one);
  aggregator.aggregate_data(input_data_plus_one);
  aggregator.copy_to_user_buffer("Sum", buffers);
  CHECK(sum == std::numeric_limits<uint64_t>::max());
}

TEST_CASE(
    "Sum aggregator: double overflow", "[sum-aggregator][double-overflow]") {
  SumAggregator<double> aggregator(FieldInfo("a1", false, false, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  double sum = 0;
  buffers["Sum"].buffer_ = &sum;
  buffers["Sum"].original_buffer_size_ = 8;

  std::vector<double> fixed_data = {
      std::numeric_limits<double>::max(),
      std::numeric_limits<double>::lowest()};

  AggregateBuffer input_data_max{
      0, 1, fixed_data.data(), nullopt, nullopt, false, nullopt};

  AggregateBuffer input_data_lowest{
      1, 2, fixed_data.data(), nullopt, nullopt, false, nullopt};

  SECTION("Overflow") {
    // First sum doesn't overflow.
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<double>::max());

    // Now create an overflow
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<double>::max());

    // Once we overflow, the value doesn't change.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<double>::max());
  }

  SECTION("Underflow") {
    // First sum doesn't underflow.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<double>::lowest());

    // Now cause an underflow.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<double>::max());

    // Once we underflow, the value doesn't change.
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Sum", buffers);
    CHECK(sum == std::numeric_limits<double>::max());
  }
}
