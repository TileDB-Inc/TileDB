/**
 * @file test-capi-sparse-array-dimension-label.cc
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
 * Tests the DimensionLabel API
 */

#include "test/src/experimental_helpers.h"
#include "test/src/helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/c_api/experimental/tiledb_dimension_label.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/encryption_type.h"

#include <test/support/tdb_catch.h>
#include <iostream>
#include <string>

using namespace tiledb::sm;
using namespace tiledb::test;

/**
 * Create a small dense array with a dimension label.
 *
 * Array Summary:
 *  * Array Type: Dense
 *  * Dimensions:
 *    - x: (type=UINT64, domain=[0, 3], tile=4)
 *  * Attributes:
 *    - a: (type=FLOAT64)
 *  * Dimension labels:
 *    - x: (label_order=UNORDERED_LABELS, dim_idx=0, type=STRING_ASCII)
 */
class ArrayExample : public DimensionLabelFixture {
 public:
  ArrayExample()
      : index_domain_{0, 3} {
    // Create an array schema
    uint64_t x_tile_extent{4};
    auto array_schema = create_array_schema(
        ctx,
        TILEDB_DENSE,
        {"x"},
        {TILEDB_UINT64},
        {&index_domain_[0]},
        {&x_tile_extent},
        {"a"},
        {TILEDB_FLOAT64},
        {1},
        {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        4096,
        false);

    // Add dimension label.
    add_dimension_label(
        ctx,
        array_schema,
        "x",
        0,
        TILEDB_UNORDERED_LABELS,
        TILEDB_STRING_ASCII,
        nullptr,
        nullptr,
        &x_tile_extent);
    // Create array
    array_name = create_temporary_array("array_with_label_1", array_schema);
    tiledb_array_schema_free(&array_schema);
  }

  void write_array_with_label(
      std::vector<double>& input_attr_data,
      std::string& input_label_data,
      std::vector<uint64_t>& input_label_offsets) {
    // Open array for writing.
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

    // Define sizes for setting buffers.
    uint64_t attr_data_size{input_attr_data.size() * sizeof(double)};
    uint64_t label_data_size{input_label_data.size() * sizeof(char)};
    uint64_t label_offsets_size{input_label_offsets.size() * sizeof(uint64_t)};

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &index_domain_[0], &index_domain_[1], nullptr));

    // Create write query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    if (attr_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "a", input_attr_data.data(), &attr_data_size));
    }
    if (label_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_label_data_buffer(
          ctx, query, "x", input_label_data.data(), &label_data_size));
      require_tiledb_ok(tiledb_query_set_label_offsets_buffer(
          ctx, query, "x", input_label_offsets.data(), &label_offsets_size));
    }

    // Submit write query.
    require_tiledb_ok(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    require_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_query_free(&query);
    tiledb_array_free(&array);
  }

  /**
   * Read back full array with a data query and check the values.
   *
   * @param expected_label_data A vector of the expected label values.
   */
  void check_values_from_data_reader(
      const std::string& expected_label_data,
      const std::vector<uint64_t>& expected_label_offsets) {
    // Open array for reading.
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &index_domain_[0], &index_domain_[1], nullptr));

    // Define label buffers and size.
    std::vector<char> label_data(20);
    std::vector<uint64_t> label_offsets(4);
    uint64_t label_data_size{label_data.size() * sizeof(char)};
    uint64_t label_offsets_size{label_offsets.size() * sizeof(uint64_t)};

    // Create read query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "x", label_data.data(), &label_data_size));
    require_tiledb_ok(tiledb_query_set_label_offsets_buffer(
        ctx, query, "x", label_offsets.data(), &label_offsets_size));

    // Submit read query.
    require_tiledb_ok(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    require_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_subarray_free(&subarray);
    tiledb_query_free(&query);
    tiledb_array_free(&array);

    // Check results.
    CHECK(label_data_size == sizeof(char) * expected_label_data.size());
    CHECK(
        label_offsets_size == sizeof(uint64_t) * expected_label_offsets.size());
    for (uint64_t ii{0}; ii < expected_label_data.size(); ++ii) {
      CHECK(label_data[ii] == expected_label_data[ii]);
    }
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(label_offsets[ii] == expected_label_offsets[ii]);
    }
  }

 protected:
  /** Name of the example array. */
  std::string array_name;

 private:
  /** Valid range for the index. */
  uint64_t index_domain_[2];
};

TEST_CASE_METHOD(
    ArrayExample,
    "Round trip dimension label and array data for dense 1d array",
    "[capi][query][DimensionLabel]") {
  // Define the input data values.
  std::vector<double> input_attr_data{1.0, 2.5, -1.0, 0.0};
  std::string input_label_data{"redyellowgreenblue"};
  std::vector<uint64_t> input_label_offsets{0, 4, 10, 15};

  // Write the array.
  write_array_with_label(
      input_attr_data, input_label_data, input_label_offsets);
  /**
  // Check the dimension label arrays have the correct data.
  check_indexed_array_data(input_label_data);
  check_labelled_array_data(expected_index_data, expected_label_data_sorted);

  // Check data reader.
  check_values_from_data_reader(input_label_data);
  **/
}
