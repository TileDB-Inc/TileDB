/**
 * @file unit_mean.cc
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
 * Tests the `MeanAggregator` class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/mean_aggregator.h"

#include <test/support/tdb_catch.h>
#include <cmath>

using namespace tiledb::sm;

TEST_CASE("Mean aggregator: constructor", "[mean-aggregator][constructor]") {
  SECTION("Var size") {
    CHECK_THROWS_WITH(
        MeanAggregator<uint8_t>(FieldInfo("a1", true, false, 1)),
        "MeanAggregator: Mean aggregates must not be requested for var sized "
        "attributes.");
  }

  SECTION("Invalid cell val num") {
    CHECK_THROWS_WITH(
        MeanAggregator<uint8_t>(FieldInfo("a1", false, false, 2)),
        "MeanAggregator: Mean aggregates must not be requested for attributes "
        "with more than one value.");
  }
}

TEST_CASE("Mean aggregator: var sized", "[mean-aggregator][var-sized]") {
  MeanAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.var_sized() == false);
}

TEST_CASE(
    "Mean aggregator: need recompute", "[mean-aggregator][need-recompute]") {
  MeanAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.need_recompute_on_overflow() == true);
}

TEST_CASE("Mean aggregator: field name", "[mean-aggregator][field-name]") {
  MeanAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.field_name() == "a1");
}

TEST_CASE(
    "Mean aggregator: Validate buffer", "[mean-aggregator][validate-buffer]") {
  MeanAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  MeanAggregator<uint8_t> aggregator_nullable(FieldInfo("a2", false, true, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  SECTION("Doesn't exist") {
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Mean", buffers),
        "MeanAggregator: Result buffer doesn't exist.");
  }

  SECTION("Null data buffer") {
    buffers["Mean"].buffer_ = nullptr;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Mean", buffers),
        "MeanAggregator: Mean aggregates must have a fixed size buffer.");
  }

  SECTION("Wrong size") {
    double mean = 0;
    buffers["Mean"].buffer_ = &mean;
    buffers["Mean"].original_buffer_size_ = 1;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Mean", buffers),
        "MeanAggregator: Mean aggregates fixed size buffer should be for one "
        "element only.");
  }

  SECTION("With var buffer") {
    double mean = 0;
    buffers["Mean"].buffer_ = &mean;
    buffers["Mean"].original_buffer_size_ = 8;
    buffers["Mean"].buffer_var_ = &mean;

    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Mean", buffers),
        "MeanAggregator: Mean aggregates must not have a var buffer.");
  }

  SECTION("With validity") {
    double mean = 0;
    buffers["Mean"].buffer_ = &mean;
    buffers["Mean"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Mean"].validity_vector_ =
        ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Mean", buffers),
        "MeanAggregator: Mean aggregates for non nullable attributes must not "
        "have a validity buffer.");
  }

  SECTION("With no validity") {
    double mean = 0;
    buffers["Mean"].buffer_ = &mean;
    buffers["Mean"].original_buffer_size_ = 8;

    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Mean", buffers),
        "MeanAggregator: Mean aggregates for nullable attributes must have a "
        "validity buffer.");
  }

  SECTION("Wrong validity size") {
    double mean = 0;
    buffers["Mean"].buffer_ = &mean;
    buffers["Mean"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 2;
    buffers["Mean"].validity_vector_ =
        ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Mean", buffers),
        "MeanAggregator: Mean aggregates validity vector should "
        "be for one element only.");
  }

  SECTION("Success") {
    double mean = 0;
    buffers["Mean"].buffer_ = &mean;
    buffers["Mean"].original_buffer_size_ = 8;
    aggregator.validate_output_buffer("Mean", buffers);
  }

  SECTION("Success nullable") {
    double mean = 0;
    buffers["Mean"].buffer_ = &mean;
    buffers["Mean"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Mean"].validity_vector_ =
        ValidityVector(&validity, &validity_size);

    aggregator_nullable.validate_output_buffer("Mean", buffers);
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
    "Mean aggregator: Basic aggregation",
    "[mean-aggregator][basic-aggregation]",
    FixedTypesUnderTest) {
  typedef TestType T;
  MeanAggregator<T> aggregator(FieldInfo("a1", false, false, 1));
  MeanAggregator<T> aggregator_nullable(FieldInfo("a2", false, true, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  double mean = 0;
  buffers["Mean"].buffer_ = &mean;
  buffers["Mean"].original_buffer_size_ = 8;

  double mean2 = 0;
  uint8_t validity = 0;
  uint64_t validity_size = 1;
  buffers["Mean2"].buffer_ = &mean2;
  buffers["Mean2"].original_buffer_size_ = 8;
  buffers["Mean2"].validity_vector_ = ValidityVector(&validity, &validity_size);

  std::vector<T> fixed_data = {1, 2, 3, 4, 5, 5, 4, 3, 2, 1};
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    // Regular attribute.
    AggregateBuffer input_data{
        2, 10, 10, fixed_data.data(), nullopt, 0, nullopt, false, nullopt};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == (27.0 / 8.0));

    // Nullable attribute.
    AggregateBuffer input_data2{
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        false,
        nullopt};
    aggregator_nullable.aggregate_data(input_data2);
    aggregator_nullable.copy_to_user_buffer("Mean2", buffers);
    CHECK(mean2 == (14.0 / 4.0));
    CHECK(validity == 1);
  }

  SECTION("Regular bitmap") {
    // Regular attribute.
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data{
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        nullopt,
        false,
        bitmap.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == (11.0 / 3.0));

    AggregateBuffer input_data2{
        0, 2, 10, fixed_data.data(), nullopt, 0, nullopt, false, bitmap.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == (14.0 / 5.0));

    // Nullable attribute.
    AggregateBuffer input_data3{
        0,
        2,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        false,
        nullopt};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("Mean2", buffers);
    CHECK(std::isnan(mean2));
    CHECK(validity == 0);

    AggregateBuffer input_data4{
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        false,
        bitmap.data()};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("Mean2", buffers);
    CHECK(mean2 == (6.0 / 2.0));
    CHECK(validity == 1);
  }

  SECTION("Count bitmap") {
    // Regular attribute.
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data{
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        nullopt,
        true,
        bitmap_count.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == (29.0 / 10.0));

    AggregateBuffer input_data2{
        0,
        2,
        10,
        fixed_data.data(),
        nullopt,
        0,
        nullopt,
        true,
        bitmap_count.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == (34.0 / 13.0));

    // Nullable attribute.
    AggregateBuffer input_data3{
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("Mean2", buffers);
    CHECK(mean2 == (22.0 / 7.0));
    CHECK(validity == 1);

    AggregateBuffer input_data4{
        0,
        2,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("Mean2", buffers);
    CHECK(mean2 == (22.0 / 7.0));
    CHECK(validity == 1);
  }
}

TEST_CASE("Mean aggregator: overflow", "[mean-aggregator][overflow]") {
  MeanAggregator<double> aggregator(FieldInfo("a1", false, false, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  double mean = 0;
  buffers["Mean"].buffer_ = &mean;
  buffers["Mean"].original_buffer_size_ = 8;

  std::vector<double> fixed_data = {
      std::numeric_limits<double>::max(),
      std::numeric_limits<double>::lowest()};

  AggregateBuffer input_data_max{
      0, 1, 10, fixed_data.data(), nullopt, 0, nullopt, false, nullopt};

  AggregateBuffer input_data_lowest{
      1, 2, 10, fixed_data.data(), nullopt, 0, nullopt, false, nullopt};

  SECTION("Overflow") {
    // First mean doesn't overflow.
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == std::numeric_limits<double>::max());

    // Now create an overflow
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == std::numeric_limits<double>::max());

    // Once we overflow, the value doesn't change.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == std::numeric_limits<double>::max());
  }

  SECTION("Underflow") {
    // First mean doesn't underflow.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == std::numeric_limits<double>::lowest());

    // Now cause an underflow.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == std::numeric_limits<double>::lowest());

    // Once we underflow, the value doesn't change.
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Mean", buffers);
    CHECK(mean == std::numeric_limits<double>::lowest());
  }
}
