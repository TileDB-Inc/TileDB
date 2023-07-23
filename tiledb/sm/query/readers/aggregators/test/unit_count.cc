/**
 * @file unit_count.cc
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
 * Tests the `CountAggregator` class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/count_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/test/whitebox_aggregate_buffer.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;

TEST_CASE("Count aggregator: var sized", "[count-aggregator][var-sized]") {
  CountAggregator aggregator;
  CHECK(aggregator.var_sized() == false);
}

TEST_CASE(
    "Count aggregator: need recompute", "[count-aggregator][need-recompute]") {
  CountAggregator aggregator;
  CHECK(aggregator.need_recompute_on_overflow() == true);
}

TEST_CASE("Count aggregator: field name", "[count-aggregator][field-name]") {
  CountAggregator aggregator;
  CHECK(aggregator.field_name() == constants::count_of_rows);
}

TEST_CASE(
    "Count aggregator: Validate buffer",
    "[count-aggregator][validate-buffer]") {
  CountAggregator aggregator;

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
        "CountAggregator: Count aggregates must have a fixed size buffer.");
  }

  SECTION("Wrong size") {
    uint64_t count = 0;
    buffers["Count"].buffer_ = &count;
    buffers["Count"].original_buffer_size_ = 1;
    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Count", buffers),
        "CountAggregator: Count aggregates fixed size buffer should be for one "
        "element only.");
  }

  SECTION("With var buffer") {
    uint64_t count = 0;
    buffers["Count"].buffer_ = &count;
    buffers["Count"].original_buffer_size_ = 8;
    buffers["Count"].buffer_var_ = &count;

    CHECK_THROWS_WITH(
        aggregator.validate_output_buffer("Count", buffers),
        "CountAggregator: Count aggregates must not have a var buffer.");
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
        "CountAggregator: Count aggregates must not have a validity buffer.");
  }

  SECTION("Success") {
    uint64_t count = 0;
    buffers["Count"].buffer_ = &count;
    buffers["Count"].original_buffer_size_ = 8;
    aggregator.validate_output_buffer("Count", buffers);
  }
}

TEST_CASE(
    "Count aggregator: Basic aggregation",
    "[count-aggregator][basic-aggregation]") {
  CountAggregator aggregator;

  std::unordered_map<std::string, QueryBuffer> buffers;

  uint64_t count = 0;
  buffers["Count"].buffer_ = &count;
  buffers["Count"].original_buffer_size_ = 1;

  SECTION("No bitmap") {
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2, 10, 10, nullptr, nullopt, 0, nullopt, false, nullopt);
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Count", buffers);
    CHECK(count == 8);
  }

  SECTION("Regular bitmap") {
    std::vector<uint8_t> bitmap = {1, 1, 0, 0, 0, 1, 1, 0, 1, 0};
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2, 10, 10, nullptr, nullopt, 0, nullopt, false, bitmap.data());
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Count", buffers);
    CHECK(count == 3);

    AggregateBuffer input_data2 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0, 2, 10, nullptr, nullopt, 0, nullopt, false, bitmap.data());
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("Count", buffers);
    CHECK(count == 5);
  }

  SECTION("Count bitmap") {
    std::vector<uint64_t> bitmap_count = {1, 2, 4, 0, 0, 1, 2, 0, 1, 2};
    AggregateBuffer input_data = WhiteboxAggregateBuffer::make_aggregate_buffer(
        2, 10, 10, nullptr, nullopt, 0, nullopt, true, bitmap_count.data());
    aggregator.aggregate_data(input_data);
    aggregator.copy_to_user_buffer("Count", buffers);
    CHECK(count == 10);

    AggregateBuffer input_data2 =
        WhiteboxAggregateBuffer::make_aggregate_buffer(
            0, 2, 10, nullptr, nullopt, 0, nullopt, true, bitmap_count.data());
    aggregator.aggregate_data(input_data2);
    aggregator.copy_to_user_buffer("Count", buffers);
    CHECK(count == 13);
  }
}
