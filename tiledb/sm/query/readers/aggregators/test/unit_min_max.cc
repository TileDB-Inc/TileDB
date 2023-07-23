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
#include "tiledb/sm/query/readers/aggregators/test/whitebox_aggregate_buffer.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;

TEST_CASE(
    "Min max aggregator: constructor", "[min-max-aggregator][constructor]") {
  SECTION("Var size, not string") {
    CHECK_THROWS_WITH(
        MinMaxAggregator<uint8_t>(false, "a1", true, false, 1),
        "MinMaxAggregator: Min/max aggregates must not be requested for var "
        "sized non-string attributes.");
  }

  SECTION("Invalid cell val num") {
    CHECK_THROWS_WITH(
        MinMaxAggregator<uint8_t>(false, "a1", false, false, 2),
        "MinMaxAggregator: Min/max aggregates must not be requested for "
        "attributes with more than one value.");
  }
}

TEST_CASE("Min max aggregator: var sized", "[min-max-aggregator][var-sized]") {
  MinMaxAggregator<uint8_t> aggregator(false, "a1", false, false, 1);
  CHECK(aggregator.var_sized() == false);

  MinMaxAggregator<std::string> aggregator_nullable(
      false, "a1", true, false, 1);
  CHECK(aggregator_nullable.var_sized() == true);
}

TEST_CASE(
    "Min max aggregator: need recompute",
    "[min-max-aggregator][need-recompute]") {
  MinMaxAggregator<uint8_t> aggregator(false, "a1", false, false, 1);
  CHECK(aggregator.need_recompute_on_overflow() == false);
}

TEST_CASE(
    "Min max aggregator: field name", "[min-max-aggregator][field-name]") {
  MinMaxAggregator<uint8_t> aggregator(false, "a1", false, false, 1);
  CHECK(aggregator.field_name() == "a1");
}

TEST_CASE(
    "Min max aggregator: Validate buffer",
    "[min-max-aggregator][validate-buffer]") {
  MinMaxAggregator<uint8_t> aggregator(false, "a1", false, false, 1);
  MinMaxAggregator<uint8_t> aggregator_nullable(false, "a2", false, true, 1);
  MinMaxAggregator<std::string> aggregator_var(
      false, "a1", true, false, constants::var_num);
  MinMaxAggregator<std::string> aggregator_var_wrong_cvn(
      false, "a1", true, false, 11);
  MinMaxAggregator<std::string> aggregator_fixed_string(
      false, "a1", false, false, 5);

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
        "MinMaxAggregator: Min/max aggregates must have a fixed size buffer.");
  }

  SECTION("Var, wrong size") {
    uint64_t offset = 0;
    buffers["Max"].buffer_ = &offset;
    buffers["Max"].original_buffer_size_ = 1;
    buffers["Max"].buffer_var_ = &offset;
    buffers["Max"].original_buffer_var_size_ = 8;
    CHECK_THROWS_WITH(
        aggregator_var.validate_output_buffer("Max", buffers),
        "MinMaxAggregator: Var sized min/max aggregates offset buffer should "
        "be for one element only.");
  }

  SECTION("Var, no var buffer") {
    uint64_t offset = 0;
    buffers["Max"].buffer_ = &offset;
    buffers["Max"].original_buffer_size_ = 8;

    CHECK_THROWS_WITH(
        aggregator_var.validate_output_buffer("Max", buffers),
        "MinMaxAggregator: Var sized min/max aggregates must have a var "
        "buffer.");
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
        "MinMaxAggregator: Var sized min/max aggregates should have "
        "TILEDB_VAR_NUM cell val num.");
    ;
  }

  SECTION("Fixed, wrong size") {
    uint64_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 8;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Max", buffers),
        "MinMaxAggregator: Fixed size min/max aggregates fixed buffer should "
        "be for one element only.");
  }

  SECTION("Fixed, var buffer") {
    uint8_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 1;
    buffers["Max"].buffer_var_ = &value;
    buffers["Max"].original_buffer_var_size_ = 1;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Max", buffers),
        "MinMaxAggregator: Fixed min/max aggregates must not have a var "
        "buffer.");
  }

  SECTION("Fixed string, wrong size") {
    uint64_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 4;
    CHECK_THROWS_WITH(
        aggregator_fixed_string.validate_output_buffer("Max", buffers),
        "MinMaxAggregator: Fixed size min/max aggregates fixed buffer should "
        "be for one element only.");
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
        "MinMaxAggregator: Min/max aggregates for non nullable attributes must "
        "not have a validity buffer.");
  }

  SECTION("With no validity") {
    uint8_t value = 0;
    buffers["Max"].buffer_ = &value;
    buffers["Max"].original_buffer_size_ = 1;

    CHECK_THROWS_WITH(
        aggregator_nullable.validate_output_buffer("Max", buffers),
        "MinMaxAggregator: Min/max aggregates for nullable attributes must "
        "have a validity buffer.");
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
        "MinMaxAggregator: Min/max aggregates validity vector should be for "
        "one element only.");
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

/** Convert type to a fixed data type. **/
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
    "Min max aggregator: Basic aggregation",
    "[min-max-aggregator][basic-aggregation]",
    FixedTypesUnderTest) {
  bool min = GENERATE(true, false);
  typedef TestType T;
  MinMaxAggregator<T> aggregator(min, "a1", false, false, 1);
  MinMaxAggregator<T> aggregator_nullable(min, "a2", false, true, 1);

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
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2, 10, 10, fixed_data.data(), nullopt, 0, nullopt, false, nullopt);
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 1, 5);

    // Nullable attribute.
    AggregateBuffer input_data2 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            2,
            10,
            10,
            fixed_data.data(),
            nullopt,
            0,
            validity_data.data(),
            false,
            nullopt);
    aggregator_nullable.aggregate_data(input_data2);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value(T(), min_max2, min, 2, 5);
    CHECK(validity == 1);
  }

  SECTION("Regular bitmap") {
    // Regular attribute.
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        nullopt,
        false,
        bitmap.data());
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 2, 5);

    AggregateBuffer input_data2 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0,
            2,
            10,
            fixed_data.data(),
            nullopt,
            0,
            nullopt,
            false,
            bitmap.data());
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 1, 5);

    // Nullable attribute.
    AggregateBuffer input_data3 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0,
            2,
            10,
            fixed_data.data(),
            nullopt,
            0,
            validity_data.data(),
            false,
            nullopt);
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);

    // For fixed size string, the min/max buffer will remain unchanged, set it
    // to the value '0' so the next check will pass.
    if (std::is_same<T, std::string>::value) {
      min_max2.data()[0] = '0';
    }

    check_value(T(), min_max2, min, 0, 0);
    CHECK(validity == 0);

    AggregateBuffer input_data4 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            2,
            10,
            10,
            fixed_data.data(),
            nullopt,
            0,
            validity_data.data(),
            false,
            bitmap.data());
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value(T(), min_max2, min, 2, 4);
    CHECK(validity == 1);
  }

  SECTION("Count bitmap") {
    // Regular attribute.
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2,
        10,
        10,
        fixed_data.data(),
        nullopt,
        0,
        nullopt,
        true,
        bitmap_count.data());
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 1, 5);

    AggregateBuffer input_data2 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0,
            2,
            10,
            fixed_data.data(),
            nullopt,
            0,
            nullopt,
            true,
            bitmap_count.data());
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value(T(), min_max, min, 1, 5);

    // Nullable attribute.
    AggregateBuffer input_data3 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            2,
            10,
            10,
            fixed_data.data(),
            nullopt,
            0,
            validity_data.data(),
            true,
            bitmap_count.data());
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value(T(), min_max2, min, 2, 4);
    CHECK(validity == 1);

    AggregateBuffer input_data4 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0,
            2,
            10,
            fixed_data.data(),
            nullopt,
            0,
            validity_data.data(),
            true,
            bitmap_count.data());
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

TEST_CASE(
    "Min max aggregator: Basic string aggregation",
    "[min-max-aggregator][basic-string-aggregation]") {
  bool min = GENERATE(true, false);
  MinMaxAggregator<std::string> aggregator(
      min, "a1", true, false, constants::var_num);
  MinMaxAggregator<std::string> aggregator_nullable(
      min, "a2", true, true, constants::var_num);

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

  std::vector<uint64_t> offsets = {0, 2, 3, 6, 8, 11, 15, 16, 18, 22};
  std::string var_data = "11233344555555543322221";
  std::vector<uint8_t> validity_data = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0};

  SECTION("No bitmap") {
    // Regular attribute.
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2,
        10,
        10,
        offsets.data(),
        var_data.data(),
        var_data.size(),
        nullopt,
        false,
        nullopt);
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "1", "5555");

    // Nullable attribute.
    AggregateBuffer input_data2 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            2,
            10,
            10,
            offsets.data(),
            var_data.data(),
            var_data.size(),
            validity_data.data(),
            false,
            nullopt);
    aggregator_nullable.aggregate_data(input_data2);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "2222", "555");
    CHECK(validity == 1);
  }

  SECTION("Regular bitmap") {
    // Regular attribute.
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2,
        10,
        10,
        offsets.data(),
        var_data.data(),
        var_data.size(),
        nullopt,
        false,
        bitmap.data());
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "2222", "5555");

    AggregateBuffer input_data2 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0,
            2,
            10,
            offsets.data(),
            var_data.data(),
            var_data.size(),
            nullopt,
            false,
            bitmap.data());
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "11", "5555");

    // Nullable attribute.
    AggregateBuffer input_data3 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0,
            2,
            10,
            offsets.data(),
            var_data.data(),
            var_data.size(),
            validity_data.data(),
            false,
            nullopt);
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "", "");
    CHECK(validity == 0);

    AggregateBuffer input_data4 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            2,
            10,
            10,
            offsets.data(),
            var_data.data(),
            var_data.size(),
            validity_data.data(),
            false,
            bitmap.data());
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "2222", "4");
    CHECK(validity == 1);
  }

  SECTION("Count bitmap") {
    // Regular attribute.
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2,
        10,
        10,
        offsets.data(),
        var_data.data(),
        var_data.size(),
        nullopt,
        true,
        bitmap_count.data());
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "1", "5555");

    AggregateBuffer input_data2 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0,
            2,
            10,
            offsets.data(),
            var_data.data(),
            var_data.size(),
            nullopt,
            true,
            bitmap_count.data());
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("MinMax", buffers);
    check_value_var(offset, min_max_size, min_max, min, "1", "5555");

    // Nullable attribute.
    AggregateBuffer input_data3 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            2,
            10,
            10,
            offsets.data(),
            var_data.data(),
            var_data.size(),
            validity_data.data(),
            true,
            bitmap_count.data());
    aggregator_nullable.aggregate_data(input_data3);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "2222", "4");
    CHECK(validity == 1);

    AggregateBuffer input_data4 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0,
            2,
            10,
            offsets.data(),
            var_data.data(),
            var_data.size(),
            validity_data.data(),
            true,
            bitmap_count.data());
    aggregator_nullable.aggregate_data(input_data4);
    aggregator_nullable.copy_to_user_buffer("MinMax2", buffers);
    check_value_var(offset2, min_max_size2, min_max2, min, "2222", "4");
    CHECK(validity == 1);
  }
}