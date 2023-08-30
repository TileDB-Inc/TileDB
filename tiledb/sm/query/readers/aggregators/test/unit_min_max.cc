/**
 * @file unit_min_max.cc
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
 * Tests the `MinMaxAggregator` class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/min_max_aggregator.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;

TEST_CASE(
    "Min max aggregator: constructor", "[min-max-aggregator][constructor]") {
  SECTION("Var size, not string") {
    CHECK_THROWS_WITH(
        MinAggregator<uint8_t>(FieldInfo("a1", true, false, 1)),
        "MinMaxAggregator: Min/max aggregates must not be requested for var "
        "sized non-string attributes.");
  }

  SECTION("Invalid cell val num") {
    CHECK_THROWS_WITH(
        MinAggregator<uint8_t>(FieldInfo("a1", false, false, 2)),
        "MinMaxAggregator: Min/max aggregates must not be requested for "
        "attributes with more than one value.");
  }
}

TEST_CASE("Min max aggregator: var sized", "[min-max-aggregator][var-sized]") {
  MinAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.var_sized() == false);

  MinAggregator<std::string> aggregator_nullable(
      FieldInfo("a1", true, false, 1));
  CHECK(aggregator_nullable.var_sized() == true);
}

TEST_CASE(
    "Min max aggregator: need recompute",
    "[min-max-aggregator][need-recompute]") {
  MinAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.need_recompute_on_overflow() == false);
}

TEST_CASE(
    "Min max aggregator: field name", "[min-max-aggregator][field-name]") {
  MinAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  CHECK(aggregator.field_name() == "a1");
}

TEST_CASE(
    "Min max aggregator: Validate buffer",
    "[min-max-aggregator][validate-buffer]") {
  MinAggregator<uint8_t> aggregator(FieldInfo("a1", false, false, 1));
  MinAggregator<uint8_t> aggregator_nullable(FieldInfo("a2", false, true, 1));
  MinAggregator<std::string> aggregator_var(
      FieldInfo("a1", true, false, constants::var_num));
  MinAggregator<std::string> aggregator_var_wrong_cvn(
      FieldInfo("a1", true, false, 11));
  MinAggregator<std::string> aggregator_fixed_string(
      FieldInfo("a1", false, false, 5));

  std::unordered_map<std::string, QueryBuffer> buffers;

  SECTION("Doesn't exist") {
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Max", buffers),
        "MinMaxAggregator: Result buffer doesn't exist.");
  }

  SECTION("Null data buffer") {
    buffers["Max"].buffer_ = nullptr;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Aggregates must have a fixed size buffer.");
  }

  SECTION("Var, wrong size") {
    uint64_t offset = 0;
    buffers["Max"].buffer_ = &offset;
    buffers["Max"].original_buffer_size_ = 1;
    buffers["Max"].buffer_var_ = &offset;
    buffers["Max"].original_buffer_var_size_ = 8;
    CHECK_THROWS_WITH(
        aggregator_var.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Var sized aggregates offset buffer should be "
        "for one element only.");
  }

  SECTION("Var, no var buffer") {
    uint64_t offset = 0;
    buffers["Max"].buffer_ = &offset;
    buffers["Max"].original_buffer_size_ = 8;

    CHECK_THROWS_WITH(
        aggregator_var.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Var sized aggregates must have a var buffer.");
  }

  SECTION("Var, wrong cell val num") {
    uint64_t offset = 0;
    buffers["Max"].buffer_ = &offset;
    buffers["Max"].original_buffer_size_ = 8;

    uint64_t string = 0;
    buffers["Max"].buffer_var_ = &string;
    buffers["Max"].original_buffer_size_ = 8;

    CHECK_THROWS_WITH(
        aggregator_var_wrong_cvn.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Var sized aggregates should have "
        "TILEDB_VAR_NUM cell val num.");
    ;
  }

  SECTION("Fixed, wrong size") {
    uint64_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 8;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Fixed size aggregates fixed buffer should be "
        "for one element only.");
  }

  SECTION("Fixed, var buffer") {
    uint8_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 1;
    buffers["Max"].buffer_var_ = &value;
    buffers["Max"].original_buffer_var_size_ = 1;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Fixed aggregates must not have a var buffer.");
  }

  SECTION("Fixed string, wrong size") {
    uint64_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 4;
    CHECK_THROWS_WITH(
        aggregator_fixed_string.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Fixed size aggregates fixed buffer should be "
        "for one element only.");
  }

  SECTION("With validity") {
    uint8_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 1;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Max"].validity_vector_ = ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Aggregates for non nullable attributes must "
        "not have a validity buffer.");
  }

  SECTION("With no validity") {
    uint8_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 1;

    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Aggregates for nullable attributes must have a "
        "validity buffer.");
  }

  SECTION("Wrong validity size") {
    uint64_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 1;

    uint8_t validity = 0;
    uint64_t validity_size = 2;
    buffers["Max"].validity_vector_ = ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Max", buffers),
        "OutputBufferValidator: Aggregates validity vector should be "
        "for one element only.");
  }

  SECTION("Success") {
    uint8_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 1;
    aggregator.validate_output_buffer("Max", buffers);
  }

  SECTION("Success nullable") {
    uint64_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 1;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Max"].validity_vector_ = ValidityVector(&validity, &validity_size);

    aggregator_nullable.validate_output_buffer("Max", buffers);
  }

  SECTION("Success var") {
    uint64_t offset = 0;
    buffers["Max"].buffer_ = &offset;
    buffers["Max"].original_buffer_size_ = 8;

    uint64_t string = 0;
    buffers["Max"].buffer_var_ = &string;
    buffers["Max"].original_buffer_size_ = 8;

    aggregator_var.validate_output_buffer("Max", buffers);
  }

  SECTION("Success fixed string") {
    uint64_t string = 0;
    buffers["Max"].buffer_ = &string;
    buffers["Max"].original_buffer_size_ = 5;

    aggregator_fixed_string.validate_output_buffer("Max", buffers);
  }
}

template <class T, class RetT>
std::vector<RetT> make_fixed_data(T) {
  return {1, 2, 3, 4, 5, 5, 4, 3, 2, 1};
}

template <>
std::vector<char> make_fixed_data(std::string) {
  return {'1', '2', '3', '4', '5', '5', '4', '3', '2', '1'};
}

template <class T>
void check_value(
    T, ByteVecValue& min_max, bool min, int64_t min_val, int64_t max_val) {
  CHECK(
      min_max.rvalue_as<T>() ==
      (min ? static_cast<T>(min_val) : static_cast<T>(max_val)));
}

template <>
void check_value(
    std::string,
    ByteVecValue& min_max,
    bool min,
    int64_t min_val,
    int64_t max_val) {
  CHECK(
      min_max.rvalue_as<char>() ==
      (min ? char('0' + min_val) : char('0' + max_val)));
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
    std::pair<uint8_t, MinAggregator<uint8_t>>,
    std::pair<uint16_t, MinAggregator<uint16_t>>,
    std::pair<uint32_t, MinAggregator<uint32_t>>,
    std::pair<uint64_t, MinAggregator<uint64_t>>,
    std::pair<int8_t, MinAggregator<int8_t>>,
    std::pair<int16_t, MinAggregator<int16_t>>,
    std::pair<int32_t, MinAggregator<int32_t>>,
    std::pair<int64_t, MinAggregator<int64_t>>,
    std::pair<float, MinAggregator<float>>,
    std::pair<double, MinAggregator<double>>,
    std::pair<std::string, MinAggregator<std::string>>,
    std::pair<uint8_t, MaxAggregator<uint8_t>>,
    std::pair<uint16_t, MaxAggregator<uint16_t>>,
    std::pair<uint32_t, MaxAggregator<uint32_t>>,
    std::pair<uint64_t, MaxAggregator<uint64_t>>,
    std::pair<int8_t, MaxAggregator<int8_t>>,
    std::pair<int16_t, MaxAggregator<int16_t>>,
    std::pair<int32_t, MaxAggregator<int32_t>>,
    std::pair<int64_t, MaxAggregator<int64_t>>,
    std::pair<float, MaxAggregator<float>>,
    std::pair<double, MaxAggregator<double>>,
    std::pair<std::string, MaxAggregator<std::string>>>
    FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Min max aggregator: Basic aggregation",
    "[min-max-aggregator][basic-aggregation]",
    FixedTypesUnderTest) {
  typedef decltype(TestType::first) T;
  typedef decltype(TestType::second) AGG;
  bool min = std::is_same<AGG, MinAggregator<T>>::value;
  AGG aggregator(FieldInfo("a1", false, false, 1));
  AGG aggregator_nullable(FieldInfo("a2", false, true, 1));

  std::unordered_map<std::string, QueryBuffer> buffers;

  ByteVecValue min_max(8);
  buffers["MinMax"].buffer_ = min_max.data();
  buffers["MinMax"].original_buffer_size_ = 8;

  ByteVecValue min_max2(8);
  uint8_t validity = 0;
  uint64_t validity_size = 1;
  buffers["MinMax2"].buffer_ = min_max2.data();
  buffers["MinMax2"].original_buffer_size_ = 8;
  buffers["MinMax2"].validity_vector_ =
      ValidityVector(&validity, &validity_size);

  auto fixed_data =
      make_fixed_data<T, typename fixed_data_type<T>::value_type>(T());
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    // Regular attribute.
    AggregateBuffer input_data{
        2, 10, fixed_data.data(), nullopt, nullopt, false, nullopt};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 1, 5);

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
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value(T(), min_max2, min, 2, 5);
    CHECK(validity == 1);
  }

  SECTION("Regular bitmap") {
    // Regular attribute.
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data{
        2, 10, fixed_data.data(), nullopt, nullopt, false, bitmap.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 2, 5);

    AggregateBuffer input_data2{
        0, 2, fixed_data.data(), nullopt, nullopt, false, bitmap.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 1, 5);

    // Nullable attribute.
    AggregateBuffer input_data3{
        0, 2, fixed_data.data(), nullopt, validity_data.data(), false, nullopt};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);

    // For fixed size string, the min/max buffer will remain unchanged, set it
    // to the value '0' so the next check will pass.
    if (std::is_same<T, std::string>::value) {
      min_max2.data()[0] = '0';
    }

    check_value(T(), min_max2, min, 0, 0);
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
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value(T(), min_max2, min, 2, 4);
    CHECK(validity == 1);
  }

  SECTION("Count bitmap") {
    // Regular attribute.
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data{
        2, 10, fixed_data.data(), nullopt, nullopt, true, bitmap_count.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 1, 5);

    AggregateBuffer input_data2{
        0, 2, fixed_data.data(), nullopt, nullopt, true, bitmap_count.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 1, 5);

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
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value(T(), min_max2, min, 2, 4);
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
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value(T(), min_max2, min, 2, 4);
    CHECK(validity == 1);
  }
}

void check_value_var(
    uint64_t offset,
    uint64_t min_max_size,
    std::vector<char>& min_max,
    bool min,
    std::string min_val,
    std::string max_val) {
  if (min) {
    CHECK(min_max_size == min_val.length());
    CHECK(0 == memcmp(min_max.data(), min_val.data(), min_val.length()));
  } else {
    CHECK(min_max_size == max_val.length());
    CHECK(0 == memcmp(min_max.data(), max_val.data(), max_val.length()));
  }

  CHECK(offset == 0);
}

typedef tuple<MinAggregator<std::string>, MinAggregator<std::string>>
    AggUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Min max aggregator: Basic string aggregation",
    "[min-max-aggregator][basic-string-aggregation]",
    AggUnderTest) {
  typedef TestType AGG;
  bool min = std::is_same<AGG, MinAggregator<std::string>>::value;
  AGG aggregator(FieldInfo("a1", true, false, constants::var_num));
  AGG aggregator_nullable(FieldInfo("a2", true, true, constants::var_num));

  std::unordered_map<std::string, QueryBuffer> buffers;

  uint64_t offset = 11;
  std::vector<char> min_max(10, 0);
  uint64_t min_max_size = 10;
  buffers["MinMax"].buffer_ = &offset;
  buffers["MinMax"].original_buffer_size_ = 8;
  buffers["MinMax"].buffer_var_ = min_max.data();
  buffers["MinMax"].original_buffer_var_size_ = 10;
  buffers["MinMax"].buffer_var_size_ = &min_max_size;

  uint64_t offset2 = 12;
  std::vector<char> min_max2(10, 0);
  uint64_t min_max_size2 = 10;
  uint8_t validity = 0;
  uint64_t validity_size = 1;
  buffers["MinMax2"].buffer_ = &offset2;
  buffers["MinMax2"].original_buffer_size_ = 8;
  buffers["MinMax2"].buffer_var_ = min_max2.data();
  buffers["MinMax2"].original_buffer_var_size_ = 10;
  buffers["MinMax2"].buffer_var_size_ = &min_max_size2;
  buffers["MinMax2"].validity_vector_ =
      ValidityVector(&validity, &validity_size);

  std::vector<uint64_t> offsets = {0, 2, 3, 6, 8, 11, 15, 16, 18, 22, 23};
  std::string var_data = "11233344555555543322221";
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    // Regular attribute.
    AggregateBuffer input_data{
        2, 10, offsets.data(), var_data.data(), nullopt, false, nullopt};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "1", "5555");

    // Nullable attribute.
    AggregateBuffer input_data2{
        2,
        10,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        false,
        nullopt};
    aggregator_nullable.aggregate_data(input_data2);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "2222", "555");
    CHECK(validity == 1);
  }

  SECTION("Regular bitmap") {
    // Regular attribute.
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data{
        2, 10, offsets.data(), var_data.data(), nullopt, false, bitmap.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "2222", "5555");

    AggregateBuffer input_data2{
        0, 2, offsets.data(), var_data.data(), nullopt, false, bitmap.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "11", "5555");

    // Nullable attribute.
    AggregateBuffer input_data3{
        0,
        2,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        false,
        nullopt};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "", "");
    CHECK(validity == 0);

    AggregateBuffer input_data4{
        2,
        10,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        false,
        bitmap.data()};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "2222", "4");
    CHECK(validity == 1);
  }

  SECTION("Count bitmap") {
    // Regular attribute.
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data{
        2,
        10,
        offsets.data(),
        var_data.data(),
        nullopt,
        true,
        bitmap_count.data()};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "1", "5555");

    AggregateBuffer input_data2{
        0,
        2,
        offsets.data(),
        var_data.data(),
        nullopt,
        true,
        bitmap_count.data()};
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "1", "5555");

    // Nullable attribute.
    AggregateBuffer input_data3{
        2,
        10,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "2222", "4");
    CHECK(validity == 1);

    AggregateBuffer input_data4{
        0,
        2,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        true,
        bitmap_count.data()};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "2222", "4");
    CHECK(validity == 1);
  }
}