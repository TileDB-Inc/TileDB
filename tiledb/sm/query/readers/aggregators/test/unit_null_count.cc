/**
 * @file unit_null_count.cc
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
 * Tests the `NullCountAggregator` class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/null_count_aggregator.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;

TEST_CASE(
    "NullCount aggregator: constructor",
    "[null-count-aggregator][constructor]") {
  SECTION("Non nullable") {
    CHECK_THROWS_WITH(
        NullCountAggregator<uint8_t>(FieldInfo("a1", false, false, 1)),
        "NullCountAggregator: NullCount aggregates must only be requested for "
        "nullable attributes.");
  }
}

TEST_CASE(
    "NullCount aggregator: var sized", "[null-count-aggregator][var-sized]") {
  bool var_sized = GENERATE(true, false);
  NullCountAggregator<uint8_t> aggregator(FieldInfo("a1", var_sized, true, 1));
  CHECK(aggregator.var_sized() == false);
}

TEST_CASE(
    "NullCount aggregator: need recompute",
    "[null-count-aggregator][need-recompute]") {
  NullCountAggregator<uint8_t> aggregator(FieldInfo("a1", false, true, 1));
  CHECK(aggregator.need_recompute_on_overflow() == true);
}

TEST_CASE(
    "NullCount aggregator: field name", "[null-count-aggregator][field-name]") {
  NullCountAggregator<uint8_t> aggregator(FieldInfo("a1", false, true, 1));
  CHECK(aggregator.field_name() == "a1");
}

TEST_CASE(
    "NullCount aggregator: Validate buffer",
    "[null-count-aggregator][validate-buffer]") {
  NullCountAggregator<uint8_t> aggregator(FieldInfo("a1", false, true, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  SECTION("Doesn't exist") {
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("NullCount", buffers),
        "NullCountAggregator: Result buffer doesn't exist.");
  }

  SECTION("Null data buffer") {
    buffers["NullCount"].buffer_ = nullptr;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("NullCount", buffers),
        "NullCountAggregator: NullCount aggregates must have a fixed size "
        "buffer.");
  }

  SECTION("Wrong size") {
    uint64_t count = 0;
    buffers["NullCount"].buffer_ = &count;
    buffers["NullCount"].original_buffer_size_ = 1;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("NullCount", buffers),
        "NullCountAggregator: NullCount aggregates fixed size buffer should be "
        "for one element only.");
  }

  SECTION("With var buffer") {
    uint64_t null_count = 0;
    buffers["NullCount"].buffer_ = &null_count;
    buffers["NullCount"].original_buffer_size_ = 8;
    buffers["NullCount"].buffer_var_ = &null_count;

    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("NullCount", buffers),
        "NullCountAggregator: NullCount aggregates must not have a var "
        "buffer.");
  }

  SECTION("With validity") {
    uint64_t null_count = 0;
    buffers["NullCount"].buffer_ = &null_count;
    buffers["NullCount"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["NullCount"].validity_vector_ =
        ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("NullCount", buffers),
        "NullCountAggregator: NullCount aggregates must not have a validity "
        "buffer.");
  }

  SECTION("Success") {
    uint64_t null_count = 0;
    buffers["NullCount"].buffer_ = &null_count;
    buffers["NullCount"].original_buffer_size_ = 8;
    aggregator.validate_output_buffer("NullCount", buffers);
  }
}

template <class T, class RetT>
std::vector<RetT> make_fixed_data_nc(T) {
  return {1, 2, 3, 4, 5, 5, 4, 3, 2, 1};
}

template <>
std::vector<char> make_fixed_data_nc(std::string) {
  return {'1', '2', '3', '4', '5', '5', '4', '3', '2', '1'};
}

template <typename T>
struct fixed_data_type {
  using type = T;
  typedef T value_type;
};

template <>
struct fixed_data_type<std::string> {
  using type = std::string;
  typedef char value_type;
};

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
    double,
    std::string>
    FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "NullCount aggregator: Basic aggregation",
    "[null-count-aggregator][basic-aggregation]",
    FixedTypesUnderTest) {
  typedef TestType T;
  NullCountAggregator<T> aggregator(FieldInfo("a1", false, true, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  uint64_t null_count = 0;
  buffers["NullCount"].buffer_ = &null_count;
  buffers["NullCount"].original_buffer_size_ = 8;

  auto fixed_data =
      make_fixed_data_nc<T, typename fixed_data_type<T>::value_type>(T());
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    AggregateBuffer input_data{
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        false,
        nullopt};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 4);
  }

  SECTION("Regular bitmap") {
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data{
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        false,
        bitmap.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 1);

    AggregateBuffer input_data2{
        0,
        2,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        false,
        bitmap.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 3);
  }

  SECTION("Count bitmap") {
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data{
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 3);

    AggregateBuffer input_data2{
        0,
        2,
        10,
        fixed_data.data(),
        nullopt,
        0,
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 6);
  }
}

TEST_CASE(
    "NullCount aggregator: Basic string aggregation",
    "[null-count-aggregator][basic-string-aggregation]") {
  NullCountAggregator<std::string> aggregator(
      FieldInfo("a1", true, true, constants::var_num));

  std::unordered_map<std::string, QueryBuffer> buffers;

  uint64_t null_count = 0;
  buffers["NullCount"].buffer_ = &null_count;
  buffers["NullCount"].original_buffer_size_ = 8;

  std::vector<uint64_t> offsets = {0, 2, 3, 6, 8, 11, 15, 16, 18, 22};
  std::string var_data = "11233344555555543322221";
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    AggregateBuffer input_data{
        2,
        10,
        10,
        offsets.data(),
        var_data.data(),
        var_data.size(),
        validity_data.data(),
        false,
        nullopt};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 4);
  }

  SECTION("Regular bitmap") {
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data{
        2,
        10,
        10,
        offsets.data(),
        var_data.data(),
        var_data.size(),
        validity_data.data(),
        false,
        bitmap.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 1);

    AggregateBuffer input_data2{
        0,
        2,
        10,
        offsets.data(),
        var_data.data(),
        var_data.size(),
        validity_data.data(),
        false,
        bitmap.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 3);
  }

  SECTION("Count bitmap") {
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data{
        2,
        10,
        10,
        offsets.data(),
        var_data.data(),
        var_data.size(),
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 3);

    AggregateBuffer input_data2{
        0,
        2,
        10,
        offsets.data(),
        var_data.data(),
        var_data.size(),
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("NullCount", buffers);
    CHECK(null_count == 6);
  }
}