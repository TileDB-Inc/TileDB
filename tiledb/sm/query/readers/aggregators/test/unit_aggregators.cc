/**
 * @file unit_aggregators.cc
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
 * Tests the aggregators class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/count_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/min_max_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"

#include <test/support/src/helper_type.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace tiledb::sm;
using namespace tiledb::test;

typedef tuple<
    SumAggregator<uint8_t>,
    MeanAggregator<uint8_t>,
    MinAggregator<uint64_t>>
    AggUnderTestConstructor;
TEMPLATE_LIST_TEST_CASE(
    "Aggregator: constructor",
    "[aggregators][constructor]",
    AggUnderTestConstructor) {
  SECTION("Var size") {
    CHECK_THROWS_WITH(
        TestType(FieldInfo("a1", true, false, 1, Datatype::UINT8)),
        "InputFieldValidator: Aggregate is not supported for var sized "
        "non-string fields.");
  }

  SECTION("Invalid cell val num") {
    CHECK_THROWS_WITH(
        TestType(FieldInfo("a1", false, false, 2, Datatype::UINT8)),
        "InputFieldValidator: Aggregate is not supported for non-string fields "
        "with cell_val_num greater than one.");
  }
}

TEST_CASE(
    "NullCount aggregator: constructor",
    "[null-count-aggregator][constructor]") {
  SECTION("Non nullable") {
    CHECK_THROWS_WITH(
        NullCountAggregator(FieldInfo("a1", false, false, 1, Datatype::UINT8)),
        "InputFieldValidator: Aggregate must only be requested for nullable "
        "fields.");
  }
}

template <class AGGREGATOR>
AGGREGATOR make_aggregator(FieldInfo field_info) {
  return AGGREGATOR(field_info);
}

template <>
CountAggregator make_aggregator(FieldInfo) {
  return CountAggregator();
}

typedef tuple<
    SumAggregator<uint8_t>,
    MeanAggregator<uint8_t>,
    MinAggregator<uint64_t>,
    NullCountAggregator,
    CountAggregator>
    AggUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Aggregator: var sized", "[aggregator][var-sized]", AggUnderTest) {
  auto aggregator{make_aggregator<TestType>(
      FieldInfo("a1", false, true, 1, Datatype::UINT8))};
  CHECK(aggregator.aggregation_var_sized() == false);

  if constexpr (std::is_same<TestType, MinAggregator<uint8_t>>::value) {
    MinAggregator<std::string> aggregator2(
        FieldInfo("a1", true, false, 1, Datatype::UINT8));
    CHECK(aggregator2.aggregation_var_sized() == true);
  }
}

TEMPLATE_LIST_TEST_CASE(
    "Aggregators: need recompute",
    "[aggregators][need-recompute]",
    AggUnderTest) {
  auto aggregator{make_aggregator<TestType>(
      FieldInfo("a1", false, true, 1, Datatype::UINT8))};
  bool need_recompute = true;
  if constexpr (std::is_same<TestType, MinAggregator<uint64_t>>::value) {
    need_recompute = false;
  }
  CHECK(aggregator.need_recompute_on_overflow() == need_recompute);
}

TEMPLATE_LIST_TEST_CASE(
    "Aggregators: field name", "[aggregator][field-name]", AggUnderTest) {
  auto aggregator{make_aggregator<TestType>(
      FieldInfo("a1", false, true, 1, Datatype::UINT8))};
  if (std::is_same<TestType, CountAggregator>::value) {
    CHECK(aggregator.field_name() == constants::count_of_rows);
  } else {
    CHECK(aggregator.field_name() == "a1");
  }
}

typedef tuple<
    SumAggregator<uint8_t>,
    MeanAggregator<uint8_t>,
    MinAggregator<uint64_t>>
    AggUnderTestValidateBuffer;
TEMPLATE_LIST_TEST_CASE(
    "Aggregators: Validate buffer",
    "[aggregators][validate-buffer]",
    AggUnderTestValidateBuffer) {
  TestType aggregator(FieldInfo("a1", false, false, 1, Datatype::UINT8));
  TestType aggregator_nullable(
      FieldInfo("a2", false, true, 1, Datatype::UINT8));
  MinAggregator<std::string> aggregator_var(
      FieldInfo("a1", true, false, constants::var_num, Datatype::UINT8));
  MinAggregator<std::string> aggregator_var_wrong_cvn(
      FieldInfo("a1", true, false, 11, Datatype::UINT8));
  MinAggregator<std::string> aggregator_fixed_string(
      FieldInfo("a1", false, false, 5, Datatype::UINT8));

  std::unordered_map<std::string, QueryBuffer> buffers;

  SECTION("Doesn't exist") {
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Agg", buffers),
        Catch::Matchers::EndsWith("Result buffer doesn't exist."));
  }

  SECTION("Null data buffer") {
    buffers["Agg"].buffer_ = nullptr;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Agg", buffers),
        "OutputBufferValidator: Aggregate must have a fixed size buffer.");
  }

  SECTION("Fixed, wrong size") {
    uint64_t sum = 0;
    buffers["Agg"].buffer_ = &sum;
    buffers["Agg"].original_buffer_size_ = 1;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Agg", buffers),
        "OutputBufferValidator: Aggregate fixed size buffer should be for one "
        "element only.");
  }

  if constexpr (std::is_same<TestType, MinAggregator<uint8_t>>::value) {
    SECTION("Var, wrong size") {
      uint64_t offset = 0;
      buffers["Agg"].buffer_ = &offset;
      buffers["Agg"].original_buffer_size_ = 1;
      buffers["Agg"].buffer_var_ = &offset;
      buffers["Agg"].original_buffer_var_size_ = 8;
      CHECK_THROWS_WITH(
          aggregator_var.validate_output_buffer("Agg", buffers),
          "OutputBufferValidator: Aggregate fixed size buffer should be for "
          "one element only.");
    }

    SECTION("Var, no var buffer") {
      uint64_t offset = 0;
      buffers["Agg"].buffer_ = &offset;
      buffers["Agg"].original_buffer_size_ = 8;

      CHECK_THROWS_WITH(
          aggregator_var.validate_output_buffer("Agg", buffers),
          "OutputBufferValidator: Var sized aggregates must have a var "
          "buffer.");
    }

    SECTION("Var, wrong cell val num") {
      uint64_t offset = 0;
      buffers["Agg"].buffer_ = &offset;
      buffers["Agg"].original_buffer_size_ = 8;

      uint64_t string = 0;
      buffers["Agg"].buffer_var_ = &string;
      buffers["Agg"].original_buffer_size_ = 8;

      CHECK_THROWS_WITH(
          aggregator_var_wrong_cvn.validate_output_buffer("Agg", buffers),
          "OutputBufferValidator: Var sized aggregates should have "
          "TILEDB_VAR_NUM cell val num.");
    }

    SECTION("Fixed string, wrong size") {
      uint64_t value = 0;
      buffers["Agg"].buffer_ = &value;
      buffers["Agg"].original_buffer_size_ = 4;
      CHECK_THROWS_WITH(
          aggregator_fixed_string.validate_output_buffer("Agg", buffers),
          "OutputBufferValidator: Aggregate fixed size buffer should be for "
          "one element only.");
    }
  }

  SECTION("Fixed, with var buffer") {
    uint64_t sum = 0;
    buffers["Agg"].buffer_ = &sum;
    buffers["Agg"].original_buffer_size_ = 8;
    buffers["Agg"].buffer_var_ = &sum;

    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Agg", buffers),
        "OutputBufferValidator: Aggregate must not have a var buffer.");
  }

  SECTION("With validity") {
    uint64_t sum = 0;
    buffers["Agg"].buffer_ = &sum;
    buffers["Agg"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Agg"].validity_vector_ = ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Agg", buffers),
        "OutputBufferValidator: Aggregate for non nullable attributes must not "
        "have a validity buffer.");
  }

  SECTION("With no validity") {
    uint64_t sum = 0;
    buffers["Agg"].buffer_ = &sum;
    buffers["Agg"].original_buffer_size_ = 8;

    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Agg", buffers),
        "OutputBufferValidator: Aggregate for nullable attributes must have a "
        "validity buffer.");
  }

  SECTION("Wrong validity size") {
    uint64_t sum = 0;
    buffers["Agg"].buffer_ = &sum;
    buffers["Agg"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 2;
    buffers["Agg"].validity_vector_ = ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Agg", buffers),
        "OutputBufferValidator: Aggregate validity vector should be for one "
        "element only.");
  }

  SECTION("Success") {
    uint64_t sum = 0;
    buffers["Agg"].buffer_ = &sum;
    buffers["Agg"].original_buffer_size_ = 8;
    aggregator.validate_output_buffer("Agg", buffers);
  }

  SECTION("Success nullable") {
    uint64_t sum = 0;
    buffers["Agg"].buffer_ = &sum;
    buffers["Agg"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Agg"].validity_vector_ = ValidityVector(&validity, &validity_size);

    aggregator_nullable.validate_output_buffer("Agg", buffers);
  }

  if constexpr (std::is_same<TestType, MinAggregator<uint8_t>>::value) {
    SECTION("Success var") {
      uint64_t offset = 0;
      buffers["Agg"].buffer_ = &offset;
      buffers["Agg"].original_buffer_size_ = 8;

      uint64_t string = 0;
      buffers["Agg"].buffer_var_ = &string;
      buffers["Agg"].original_buffer_size_ = 8;

      aggregator_var.validate_output_buffer("Agg", buffers);
    }

    SECTION("Success fixed string") {
      uint64_t string = 0;
      buffers["Agg"].buffer_ = &string;
      buffers["Agg"].original_buffer_size_ = 5;

      aggregator_fixed_string.validate_output_buffer("Agg", buffers);
    }
  }
}

typedef tuple<NullCountAggregator, CountAggregator>
    AggUnderTestValidateBufferCount;
TEMPLATE_LIST_TEST_CASE(
    "Aggregators: Validate buffer count",
    "[aggregators][validate-buffer]",
    AggUnderTestValidateBufferCount) {
  auto aggregator{make_aggregator<TestType>(
      FieldInfo("a1", false, true, 1, Datatype::UINT8))};
  std::unordered_map<std::string, QueryBuffer> buffers;

  SECTION("Doesn't exist") {
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Count", buffers),
        "CountAggregator: Result buffer doesn't exist.");
  }

  SECTION("Null data buffer") {
    buffers["Count"].buffer_ = nullptr;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Count", buffers),
        "OutputBufferValidator: Aggregate must have a fixed size buffer.");
  }

  SECTION("Wrong size") {
    uint64_t count = 0;
    buffers["Count"].buffer_ = &count;
    buffers["Count"].original_buffer_size_ = 1;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Count", buffers),
        "OutputBufferValidator: Aggregate fixed size buffer should be for one "
        "element only.");
  }

  SECTION("With var buffer") {
    uint64_t count = 0;
    buffers["Count"].buffer_ = &count;
    buffers["Count"].original_buffer_size_ = 8;
    buffers["Count"].buffer_var_ = &count;

    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Count", buffers),
        "OutputBufferValidator: Aggregate must not have a var buffer.");
  }

  SECTION("With validity") {
    uint64_t count = 0;
    buffers["Count"].buffer_ = &count;
    buffers["Count"].original_buffer_size_ = 8;

    uint8_t validity = 0;
    uint64_t validity_size = 1;
    buffers["Count"].validity_vector_ =
        ValidityVector(&validity, &validity_size);
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Count", buffers),
        "OutputBufferValidator: Count aggregates must not have a validity "
        "buffer.");
  }

  SECTION("Success") {
    uint64_t count = 0;
    buffers["Count"].buffer_ = &count;
    buffers["Count"].original_buffer_size_ = 8;
    aggregator.validate_output_buffer("Count", buffers);
  }
}

template <typename T>
bool is_nan(T) {
  return false;
}

template <>
bool is_nan<double>(double v) {
  return std::isnan(v);
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
void check_value(T, ByteVecValue& res, double val) {
  CHECK(res.rvalue_as<T>() == static_cast<T>(val));
}

template <>
void check_value(std::string, ByteVecValue& res, double val) {
  CHECK(res.rvalue_as<char>() == char('0' + val));
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

template <typename AGGREGATOR>
void check_validity(uint8_t validity, uint8_t expected) {
  if constexpr (
      !std::is_same<AGGREGATOR, CountAggregator>::value &&
      !std::is_same<AGGREGATOR, NullCountAggregator>::value) {
    CHECK(validity == expected);
  }
}

template <typename T, typename RES, typename AGGREGATOR>
void basic_aggregation_test(std::vector<double> expected_results) {
  // Optionally make the aggregator for non nullable values.
  optional<AGGREGATOR> aggregator;
  if constexpr (!std::is_same<AGGREGATOR, NullCountAggregator>::value) {
    if constexpr (std::is_same<AGGREGATOR, CountAggregator>::value) {
      aggregator.emplace();
    } else {
      aggregator.emplace(FieldInfo("a1", false, false, 1, tdb_type<T>));
    }
  }

  auto aggregator_nullable{make_aggregator<AGGREGATOR>(
      FieldInfo("a1", false, true, 1, tdb_type<T>))};

  std::unordered_map<std::string, QueryBuffer> buffers;

  ByteVecValue res(8);
  buffers["Agg"].buffer_ = res.data();
  buffers["Agg"].original_buffer_size_ = 8;

  ByteVecValue res2(8);
  uint8_t validity = 0;
  uint64_t validity_size = 1;
  buffers["Agg2"].buffer_ = res2.data();
  buffers["Agg2"].original_buffer_size_ = 8;
  buffers["Agg2"].validity_vector_ = ValidityVector(&validity, &validity_size);

  auto fixed_data =
      make_fixed_data<T, typename fixed_data_type<T>::value_type>(T());
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    ByteVecValue zero(8);
    ByteVecValue full_tile_sum(8);
    if constexpr (!std::is_same<RES, std::string>::value) {
      zero.assign_as<typename tiledb::sm::sum_type_data<T>::sum_type>(0);
      full_tile_sum.assign_as<typename tiledb::sm::sum_type_data<T>::sum_type>(
          10);
    }

    auto tile_metadata_all_null = TileMetadata(
        10,
        10,
        &fixed_data[0],
        sizeof(typename fixed_data_type<T>::value_type),
        &fixed_data[0],
        sizeof(typename fixed_data_type<T>::value_type),
        zero.data());
    auto tile_metadata = TileMetadata(
        10,
        5,
        &fixed_data[0],
        sizeof(typename fixed_data_type<T>::value_type),
        &fixed_data[0],
        sizeof(typename fixed_data_type<T>::value_type),
        full_tile_sum.data());
    if (aggregator.has_value()) {
      // Regular attribute.
      AggregateBuffer input_data{
          2, 10, fixed_data.data(), nullopt, nullopt, false, nullopt, 1};
      aggregator->aggregate_data(input_data);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value(RES(), res, expected_results[0]);

      aggregator->aggregate_tile_with_frag_md(tile_metadata_all_null);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value(RES(), res, expected_results[1]);

      aggregator->aggregate_tile_with_frag_md(tile_metadata);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value(RES(), res, expected_results[2]);
    }

    // Nullable attribute.
    AggregateBuffer input_data2{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        false,
        nullopt,
        1};
    aggregator_nullable.aggregate_data(input_data2);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value(RES(), res2, expected_results[3]);
    check_validity<AGGREGATOR>(validity, 1);

    aggregator_nullable.aggregate_tile_with_frag_md(tile_metadata_all_null);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value(RES(), res2, expected_results[4]);
    check_validity<AGGREGATOR>(validity, 1);

    aggregator_nullable.aggregate_tile_with_frag_md(tile_metadata);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value(RES(), res2, expected_results[5]);
    check_validity<AGGREGATOR>(validity, 1);
  }

  SECTION("Regular bitmap") {
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    if (aggregator.has_value()) {
      // Regular attribute.
      AggregateBuffer input_data{
          2, 10, fixed_data.data(), nullopt, nullopt, false, bitmap.data(), 1};
      aggregator->aggregate_data(input_data);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value(RES(), res, expected_results[6]);

      AggregateBuffer input_data2{
          0, 2, fixed_data.data(), nullopt, nullopt, false, bitmap.data(), 1};
      aggregator->aggregate_data(input_data2);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value(RES(), res, expected_results[7]);
    }

    // Nullable attribute.
    AggregateBuffer input_data3{
        0,
        2,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        false,
        nullopt,
        1};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    // For fixed size string, the min/max buffer will remain unchanged, set it
    // to the value '0' so the next check will pass.
    if (std::is_same<RES, std::string>::value) {
      res2.data()[0] = '0';
    }

    if (is_nan(expected_results[8])) {
      CHECK(is_nan(*static_cast<RES*>(static_cast<void*>(res2.data()))));
    } else {
      check_value(RES(), res2, expected_results[8]);
    }
    check_validity<AGGREGATOR>(validity, 0);

    AggregateBuffer input_data4{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        false,
        bitmap.data(),
        1};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value(RES(), res2, expected_results[9]);
    check_validity<AGGREGATOR>(validity, 1);
  }

  SECTION("Count bitmap") {
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    if (aggregator.has_value()) {
      // Regular attribute.
      AggregateBuffer input_data{
          2,
          10,
          fixed_data.data(),
          nullopt,
          nullopt,
          true,
          bitmap_count.data(),
          1};
      aggregator->aggregate_data(input_data);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value(RES(), res, expected_results[10]);

      AggregateBuffer input_data2{
          0,
          2,
          fixed_data.data(),
          nullopt,
          nullopt,
          true,
          bitmap_count.data(),
          1};
      aggregator->aggregate_data(input_data2);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value(RES(), res, expected_results[11]);
    }

    // Nullable attribute.
    AggregateBuffer input_data3{
        2,
        10,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        true,
        bitmap_count.data(),
        1};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value(RES(), res2, expected_results[12]);
    check_validity<AGGREGATOR>(validity, 1);

    AggregateBuffer input_data4{
        0,
        2,
        fixed_data.data(),
        nullopt,
        validity_data.data(),
        true,
        bitmap_count.data(),
        1};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value(RES(), res2, expected_results[13]);
    check_validity<AGGREGATOR>(validity, 1);
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
  basic_aggregation_test<
      T,
      typename sum_type_data<T>::sum_type,
      SumAggregator<T>>({27, 27, 37, 14, 14, 24, 11, 14, 0, 6, 29, 34, 22, 22});
}

TEMPLATE_LIST_TEST_CASE(
    "Mean aggregator: Basic aggregation",
    "[mean-aggregator][basic-aggregation]",
    FixedTypesUnderTest) {
  typedef TestType T;
  basic_aggregation_test<T, double, MeanAggregator<T>>(
      {(27.0 / 8.0),
       (27.0 / 8.0),
       (37.0 / 13.0),
       (14.0 / 4.0),
       (14.0 / 4.0),
       (24.0 / 9.0),
       (11.0 / 3.0),
       (14.0 / 5.0),
       std::numeric_limits<double>::signaling_NaN(),
       (6.0 / 2.0),
       (29.0 / 10.0),
       (34.0 / 13.0),
       (22.0 / 7.0),
       (22.0 / 7.0)});
}

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
    AggUnderTestMinMax;
TEMPLATE_LIST_TEST_CASE(
    "Min max aggregator: Basic aggregation",
    "[min-max-aggregator][basic-aggregation]",
    AggUnderTestMinMax) {
  typedef decltype(TestType::first) T;
  typedef decltype(TestType::second) AGG;
  std::vector<double> res = {1, 1, 1, 2, 2, 1, 2, 1, 0, 2, 1, 1, 2, 2};
  if (std::is_same<AGG, MaxAggregator<T>>::value) {
    res = {5, 5, 5, 5, 5, 5, 5, 5, 0, 4, 5, 5, 4, 4};
  }
  basic_aggregation_test<T, T, AGG>(res);
}

TEST_CASE(
    "Count aggregator: Basic aggregation",
    "[count-aggregator][basic-aggregation]") {
  std::vector<double> res = {8, 18, 28, 8, 18, 28, 3, 5, 2, 5, 10, 13, 10, 13};
  basic_aggregation_test<uint8_t, uint64_t, CountAggregator>(res);
}

TEMPLATE_LIST_TEST_CASE(
    "NullCount aggregator: Basic aggregation",
    "[null-count-aggregator][basic-aggregation]",
    FixedTypesUnderTest) {
  typedef TestType T;
  std::vector<double> res = {0, 0, 0, 4, 14, 19, 0, 0, 2, 3, 0, 0, 3, 6};
  basic_aggregation_test<T, uint64_t, NullCountAggregator>(res);
}

TEST_CASE(
    "Sum aggregator: signed overflow", "[sum-aggregator][signed-overflow]") {
  SumAggregator<int64_t> aggregator(
      FieldInfo("a1", false, false, 1, tdb_type<int64_t>));

  std::unordered_map<std::string, QueryBuffer> buffers;

  int64_t sum = 0;
  buffers["Agg"].buffer_ = &sum;
  buffers["Agg"].original_buffer_size_ = 8;

  std::vector<int64_t> fixed_data = {
      1,
      std::numeric_limits<int64_t>::max() - 2,
      -1,
      std::numeric_limits<int64_t>::min() + 2};

  AggregateBuffer input_data_plus_one{
      0, 1, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};

  AggregateBuffer input_data_minus_one{
      2, 3, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};

  SECTION("Overflow") {
    // First sum doesn't overflow.
    AggregateBuffer input_data{
        0, 2, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max() - 1);

    // Reached max but still hasn't overflowed.
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());

    // We can still subtract.
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max() - 1);

    // Now cause an overflow.
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());

    // Once we overflow, the value doesn't change.
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());
  }

  SECTION("Underflow") {
    // First sum doesn't underflow.
    AggregateBuffer input_data{
        2, 4, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::min() + 1);

    // Reached min but still hasn't underflowed.
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::min());

    // We can still subtract.
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::min() + 1);

    // Now cause an underflow.
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.aggregate_data(input_data_minus_one);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());

    // Once we underflow, the value doesn't change.
    aggregator.aggregate_data(input_data_plus_one);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<int64_t>::max());
  }
}

TEST_CASE(
    "Sum aggregator: unsigned overflow",
    "[sum-aggregator][unsigned-overflow]") {
  SumAggregator<uint64_t> aggregator(
      FieldInfo("a1", false, false, 1, tdb_type<int64_t>));

  std::unordered_map<std::string, QueryBuffer> buffers;

  uint64_t sum = 0;
  buffers["Agg"].buffer_ = &sum;
  buffers["Agg"].original_buffer_size_ = 8;

  std::vector<uint64_t> fixed_data = {
      1, std::numeric_limits<uint64_t>::max() - 2};

  AggregateBuffer input_data_plus_one{
      0, 1, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};

  // First sum doesn't overflow.
  AggregateBuffer input_data{
      0, 2, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};
  aggregator.aggregate_data(input_data);
  aggregator.copy_to_user_buffer("Agg", buffers);
  CHECK(sum == std::numeric_limits<uint64_t>::max() - 1);

  // Reached max but still hasn't overflowed.
  aggregator.aggregate_data(input_data_plus_one);
  aggregator.copy_to_user_buffer("Agg", buffers);
  CHECK(sum == std::numeric_limits<uint64_t>::max());

  // Now cause an overflow.
  aggregator.aggregate_data(input_data_plus_one);
  aggregator.aggregate_data(input_data_plus_one);
  aggregator.copy_to_user_buffer("Agg", buffers);
  CHECK(sum == std::numeric_limits<uint64_t>::max());
}

typedef tuple<SumAggregator<double>, MeanAggregator<double>> DoubleAggUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Sum aggregator: double overflow",
    "[sum-aggregator][double-overflow]",
    DoubleAggUnderTest) {
  TestType aggregator(FieldInfo("a1", false, false, 1, tdb_type<double>));

  std::unordered_map<std::string, QueryBuffer> buffers;

  double sum = 0;
  buffers["Agg"].buffer_ = &sum;
  buffers["Agg"].original_buffer_size_ = 8;

  std::vector<double> fixed_data = {
      std::numeric_limits<double>::max(),
      std::numeric_limits<double>::lowest()};

  AggregateBuffer input_data_max{
      0, 1, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};

  AggregateBuffer input_data_lowest{
      1, 2, fixed_data.data(), nullopt, nullopt, false, nullopt, 0};

  SECTION("Overflow") {
    // First sum doesn't overflow.
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<double>::max());

    // Now create an overflow
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<double>::max());

    // Once we overflow, the value doesn't change.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<double>::max());
  }

  SECTION("Underflow") {
    // First sum doesn't underflow.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<double>::lowest());

    // Now cause an underflow.
    aggregator.aggregate_data(input_data_lowest);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<double>::max());

    // Once we underflow, the value doesn't change.
    aggregator.aggregate_data(input_data_max);
    aggregator.copy_to_user_buffer("Agg", buffers);
    CHECK(sum == std::numeric_limits<double>::max());
  }
}

template <typename RES>
void check_value_string(
    uint64_t fixed_data, uint64_t, std::vector<char>&, RES expected) {
  CHECK(fixed_data == expected);
}

template <>
void check_value_string(
    uint64_t fixed_data,
    uint64_t value_size,
    std::vector<char>& value,
    std::string expected) {
  CHECK(value_size == expected.length());
  CHECK(0 == memcmp(value.data(), expected.data(), expected.length()));

  CHECK(fixed_data == 0);
}

template <typename AGGREGATOR, typename RES>
void basic_string_aggregation_test(std::vector<RES> expected_results) {
  // Optionally make the aggregator for non nullable values.
  optional<AGGREGATOR> aggregator;
  if constexpr (!std::is_same<AGGREGATOR, NullCountAggregator>::value) {
    aggregator.emplace(FieldInfo(
        "a1", true, false, constants::var_num, tdb_type<std::string>));
  }

  AGGREGATOR aggregator_nullable(
      FieldInfo("a2", true, true, constants::var_num, tdb_type<std::string>));

  std::unordered_map<std::string, QueryBuffer> buffers;

  uint64_t fixed_data = 11;
  std::vector<char> value(10, 0);
  uint64_t value_size = 10;
  buffers["Agg"].buffer_ = &fixed_data;
  buffers["Agg"].original_buffer_size_ = 8;
  buffers["Agg"].buffer_var_ = value.data();
  buffers["Agg"].original_buffer_var_size_ = 10;
  buffers["Agg"].buffer_var_size_ = &value_size;

  uint64_t fixed_data2 = 12;
  std::vector<char> value2(10, 0);
  uint64_t value_size2 = 10;
  uint8_t validity = 0;
  uint64_t validity_size = 1;
  buffers["Agg2"].buffer_ = &fixed_data2;
  buffers["Agg2"].original_buffer_size_ = 8;

  if constexpr (!std::is_same<AGGREGATOR, NullCountAggregator>::value) {
    buffers["Agg2"].buffer_var_ = value2.data();
    buffers["Agg2"].original_buffer_var_size_ = 10;
    buffers["Agg2"].buffer_var_size_ = &value_size2;
  }
  buffers["Agg2"].validity_vector_ = ValidityVector(&validity, &validity_size);

  std::vector<uint64_t> offsets = {0, 2, 3, 6, 8, 11, 15, 16, 18, 22, 23};
  std::string var_data = "11233344555555543322221";
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    ByteVecValue unused(8);
    auto tile_metadata_all_null = TileMetadata(
        10,
        10,
        &var_data[offsets[0]],
        offsets[1] - offsets[0],
        &var_data[offsets[5]],
        offsets[6] - offsets[5],
        unused.data());
    auto tile_metadata = TileMetadata(
        10,
        5,
        &var_data[offsets[0]],
        offsets[1] - offsets[0],
        &var_data[offsets[5]],
        offsets[6] - offsets[5],
        unused.data());

    if (aggregator.has_value()) {
      // Regular attribute.
      AggregateBuffer input_data{
          2, 10, offsets.data(), var_data.data(), nullopt, false, nullopt, 1};
      aggregator->aggregate_data(input_data);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value_string(fixed_data, value_size, value, expected_results[0]);

      aggregator->aggregate_tile_with_frag_md(tile_metadata_all_null);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value_string(fixed_data, value_size, value, expected_results[1]);

      aggregator->aggregate_tile_with_frag_md(tile_metadata);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value_string(fixed_data, value_size, value, expected_results[2]);
    }

    // Nullable attribute.
    AggregateBuffer input_data2{
        2,
        10,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        false,
        nullopt,
        1};
    aggregator_nullable.aggregate_data(input_data2);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value_string(fixed_data2, value_size2, value2, expected_results[3]);
    check_validity<AGGREGATOR>(validity, 1);

    aggregator_nullable.aggregate_tile_with_frag_md(tile_metadata_all_null);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value_string(fixed_data2, value_size2, value2, expected_results[4]);

    aggregator_nullable.aggregate_tile_with_frag_md(tile_metadata);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value_string(fixed_data2, value_size2, value2, expected_results[5]);
  }

  SECTION("Regular bitmap") {
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    if (aggregator.has_value()) {
      // Regular attribute.
      AggregateBuffer input_data{
          2,
          10,
          offsets.data(),
          var_data.data(),
          nullopt,
          false,
          bitmap.data(),
          1};
      aggregator->aggregate_data(input_data);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value_string(fixed_data, value_size, value, expected_results[6]);

      AggregateBuffer input_data2{
          0,
          2,
          offsets.data(),
          var_data.data(),
          nullopt,
          false,
          bitmap.data(),
          1};
      aggregator->aggregate_data(input_data2);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value_string(fixed_data, value_size, value, expected_results[7]);
    }

    // Nullable attribute.
    AggregateBuffer input_data3{
        0,
        2,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        false,
        nullopt,
        1};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value_string(fixed_data2, value_size2, value2, expected_results[8]);
    check_validity<AGGREGATOR>(validity, 0);

    AggregateBuffer input_data4{
        2,
        10,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        false,
        bitmap.data(),
        1};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value_string(fixed_data2, value_size2, value2, expected_results[9]);
    check_validity<AGGREGATOR>(validity, 1);
  }

  SECTION("Count bitmap") {
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    if (aggregator.has_value()) {
      // Regular attribute.
      AggregateBuffer input_data{
          2,
          10,
          offsets.data(),
          var_data.data(),
          nullopt,
          true,
          bitmap_count.data(),
          1};
      aggregator->aggregate_data(input_data);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value_string(fixed_data, value_size, value, expected_results[10]);

      AggregateBuffer input_data2{
          0,
          2,
          offsets.data(),
          var_data.data(),
          nullopt,
          true,
          bitmap_count.data(),
          1};
      aggregator->aggregate_data(input_data2);
      aggregator->copy_to_user_buffer("Agg", buffers);
      check_value_string(fixed_data, value_size, value, expected_results[11]);
    }

    // Nullable attribute.
    AggregateBuffer input_data3{
        2,
        10,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        true,
        bitmap_count.data(),
        1};
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value_string(fixed_data2, value_size2, value2, expected_results[12]);
    check_validity<AGGREGATOR>(validity, 1);

    AggregateBuffer input_data4{
        0,
        2,
        offsets.data(),
        var_data.data(),
        validity_data.data(),
        true,
        bitmap_count.data(),
        1};
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("Agg2", buffers);
    check_value_string(fixed_data2, value_size2, value2, expected_results[13]);
    check_validity<AGGREGATOR>(validity, 1);
  }
}

typedef tuple<MinAggregator<std::string>, MaxAggregator<std::string>>
    MinMaxAggUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Min max aggregator: Basic string aggregation",
    "[min-max-aggregator][basic-string-aggregation]",
    MinMaxAggUnderTest) {
  typedef TestType AGGREGATOR;
  std::vector<std::string> res = {
      "1",
      "1",
      "1",
      "2222",
      "2222",
      "11",
      "2222",
      "11",
      "",
      "2222",
      "1",
      "1",
      "2222",
      "2222"};
  if (std::is_same<AGGREGATOR, MaxAggregator<std::string>>::value) {
    res = {
        "5555",
        "5555",
        "5555",
        "555",
        "555",
        "5555",
        "5555",
        "5555",
        "",
        "4",
        "5555",
        "5555",
        "4",
        "4"};
  }

  basic_string_aggregation_test<AGGREGATOR, std::string>(res);
}

TEST_CASE(
    "NullCount aggregator: Basic string aggregation",
    "[null-count-aggregator][basic-string-aggregation]") {
  std::vector<uint64_t> res = {0, 0, 0, 4, 14, 19, 0, 0, 2, 3, 0, 0, 3, 6};
  basic_string_aggregation_test<NullCountAggregator, uint64_t>(res);
}

TEST_CASE(
    "NullCount aggregator: output datatype",
    "[null-count-aggregator][output-datatype]") {
  NullCountAggregator aggregator{
      FieldInfo{"a1", false, true, 1, Datatype::UINT8}};
  CHECK(aggregator.output_datatype() == Datatype::UINT64);
}

TEST_CASE(
    "Count aggregator: output datatype",
    "[count-aggregator][output-datatype]") {
  CountAggregator aggregator;
  CHECK(aggregator.output_datatype() == Datatype::UINT64);
}

TEST_CASE(
    "Sum aggregator: Expected output type",
    "[sum-aggregator][output_datatype]") {
  CHECK(
      SumAggregator<int8_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::INT64);
  CHECK(
      SumAggregator<uint8_t>{FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::UINT64);
  CHECK(
      SumAggregator<int16_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::INT64);
  CHECK(
      SumAggregator<uint16_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::UINT64);
  CHECK(
      SumAggregator<int32_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::INT64);
  CHECK(
      SumAggregator<uint32_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::UINT64);
  CHECK(
      SumAggregator<int64_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::INT64);
  CHECK(
      SumAggregator<uint64_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::UINT64);
  CHECK(
      SumAggregator<float>{FieldInfo("a1", false, false, 1, Datatype::FLOAT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      SumAggregator<double>{FieldInfo("a1", false, false, 1, Datatype::FLOAT64)}
          .output_datatype() == Datatype::FLOAT64);
}

TEST_CASE(
    "Mean aggregator: Expected output type",
    "[mean-aggregator][output_datatype]") {
  CHECK(
      MeanAggregator<int8_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<uint8_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<int16_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<uint16_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<int32_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<uint32_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<int64_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<uint64_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<float>{FieldInfo("a1", false, false, 1, Datatype::FLOAT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MeanAggregator<double>{
          FieldInfo("a1", false, false, 1, Datatype::FLOAT64)}
          .output_datatype() == Datatype::FLOAT64);
}

TEST_CASE(
    "Min max aggregator: Expected output type",
    "[min-max-aggregator][output_datatype]") {
  CHECK(
      MinAggregator<int8_t>{FieldInfo("a1", false, false, 1, Datatype::INT8)}
          .output_datatype() == Datatype::INT8);
  CHECK(
      MinAggregator<uint8_t>{FieldInfo("a1", false, false, 1, Datatype::UINT8)}
          .output_datatype() == Datatype::UINT8);
  CHECK(
      MinAggregator<int16_t>{FieldInfo("a1", false, false, 1, Datatype::INT16)}
          .output_datatype() == Datatype::INT16);
  CHECK(
      MinAggregator<uint16_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT16)}
          .output_datatype() == Datatype::UINT16);
  CHECK(
      MinAggregator<int32_t>{FieldInfo("a1", false, false, 1, Datatype::INT32)}
          .output_datatype() == Datatype::INT32);
  CHECK(
      MinAggregator<uint32_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT32)}
          .output_datatype() == Datatype::UINT32);
  CHECK(
      MinAggregator<int64_t>{FieldInfo("a1", false, false, 1, Datatype::INT64)}
          .output_datatype() == Datatype::INT64);
  CHECK(
      MinAggregator<uint64_t>{
          FieldInfo("a1", false, false, 1, Datatype::UINT64)}
          .output_datatype() == Datatype::UINT64);
  CHECK(
      MinAggregator<float>{FieldInfo("a1", false, false, 1, Datatype::FLOAT32)}
          .output_datatype() == Datatype::FLOAT32);
  CHECK(
      MinAggregator<double>{FieldInfo("a1", false, false, 1, Datatype::FLOAT64)}
          .output_datatype() == Datatype::FLOAT64);
  CHECK(
      MinAggregator<std::string>{
          FieldInfo("a1", false, false, 1, Datatype::STRING_ASCII)}
          .output_datatype() == Datatype::STRING_ASCII);
}
