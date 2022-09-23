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
 * Create a small sparse array with a dimension label.
 *
 * Array Summary:
 *  * Array Type: Sparse
 *  * Dimensions:
 *    - x: (type=UINT64, domain=[1, 4], tile=4)
 *  * Attributes:
 *    - a: (type=FLOAT64)
 *  * Dimension labels:
 *    - x: (label_order=label_order_t, dim_idx=0, type=FLOAT64)
 */
class SparseArrayExample1 : public DimensionLabelFixture {
 public:
  SparseArrayExample1()
      : array_name{}
      , index_domain_{0, 3}
      , label_domain_{-1, 1} {
  }

  void create_example(tiledb_label_order_t label_order) {
    // Create an array schema
    uint64_t x_tile_extent{4};
    auto array_schema = create_array_schema(
        ctx,
        TILEDB_SPARSE,
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
    double label_tile_extent = 2.0;
    add_dimension_label(
        ctx,
        array_schema,
        "x",
        0,
        label_order,
        TILEDB_FLOAT64,
        &label_domain_[0],
        &label_tile_extent,
        &x_tile_extent);
    // Create array
    array_name = create_temporary_array("array_with_label_1", array_schema);
    tiledb_array_schema_free(&array_schema);
  }

  void write_array_with_label(
      std::vector<uint64_t>& input_index_data,
      std::vector<double>& input_attr_data,
      std::vector<double>& input_label_data,
      bool error_on_write = false) {
    // Open array for writing.
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

    // Define sizes for setting buffers.
    uint64_t index_data_size{input_index_data.size() * sizeof(uint64_t)};
    uint64_t attr_data_size{input_attr_data.size() * sizeof(double)};
    uint64_t label_data_size{input_label_data.size() * sizeof(double)};

    // Create write query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED));
    if (index_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "x", input_index_data.data(), &index_data_size));
    }
    if (attr_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "a", input_attr_data.data(), &attr_data_size));
    }
    if (label_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_label_data_buffer(
          ctx, query, "x", input_label_data.data(), &label_data_size));
    }

    // Submit write query.
    if (error_on_write) {
      auto rc = tiledb_query_submit(ctx, query);
      REQUIRE(rc != TILEDB_OK);
    } else {
      require_tiledb_ok(tiledb_query_submit(ctx, query));
      tiledb_query_status_t query_status;
      require_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
      REQUIRE(query_status == TILEDB_COMPLETED);
    }

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
      const std::vector<double>& expected_label_data) {
    // Open array for reading.
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &index_domain_[0], &index_domain_[1], nullptr));

    // Define label buffer and size.
    std::vector<double> label_data(4);
    uint64_t label_data_size{label_data.size() * sizeof(double)};

    // Create read query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "x", label_data.data(), &label_data_size));

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
    for (uint64_t ii{0}; ii < 4; ++ii) {
      INFO("Label data " + std::to_string(ii));
      CHECK(label_data[ii] == expected_label_data[ii]);
    }
  }

  void check_indexed_array_data(
      const std::vector<double>& expected_label_data) {
    // Read back the data.
    auto label_data = DimensionLabelFixture::read_indexed_array<double>(
        URI(array_name).join_path("__labels/l0"),
        4,
        &index_domain_[0],
        &index_domain_[1]);

    // Check results.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      INFO("Label data " + std::to_string(ii));
      CHECK(label_data[ii] == expected_label_data[ii]);
    }
  }

  void check_labelled_array_data(
      const std::vector<uint64_t>& expected_index_data,
      const std::vector<double>& expected_label_data) {
    // Read back the data.
    auto [index_data, label_data] =
        DimensionLabelFixture::read_labelled_array<uint64_t, double>(
            URI(array_name).join_path("__labels/l0"),
            4,
            &label_domain_[0],
            &label_domain_[1]);

    // Check the results.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      INFO("Index data " + std::to_string(ii));
      CHECK(index_data[ii] == expected_index_data[ii]);
    }
    for (uint64_t ii{0}; ii < 4; ++ii) {
      INFO("Label data " + std::to_string(ii));
      CHECK(label_data[ii] == expected_label_data[ii]);
    }
  }

 protected:
  /** Name of the example array. */
  std::string array_name;

 private:
  /** Valid range for the index. */
  uint64_t index_domain_[2];

  /** Valid range for the label. */
  double label_domain_[2];
};

TEST_CASE_METHOD(
    SparseArrayExample1,
    "Round trip dimension label data for sparse 1D array",
    "[capi][query][DimensionLabel]") {
  // Input vectors.
  std::vector<uint64_t> input_index_data{};
  std::vector<double> input_label_data{};
  std::vector<double> input_attr_data{};

  // Expected output vectors.
  std::vector<double> label_data_sorted_by_index{};
  std::vector<double> label_data_sorted_by_label{};
  std::vector<uint64_t> index_data_sorted_by_label{};

  // Dimension label parameters.
  tiledb_label_order_t label_order;

  SECTION("Write increasing labels and array", "[IncreasingLabels]") {
    // Set the label order.
    label_order = TILEDB_INCREASING_LABELS;

    // Set input values.
    input_index_data = {0, 1, 2, 3};
    input_label_data = {-1.0, 0.0, 0.5, 1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};

    // Set expected_output values.
    label_data_sorted_by_index = input_label_data;
    label_data_sorted_by_label = input_label_data;
    index_data_sorted_by_label = input_index_data;
  }

  SECTION("Write increasing labels only", "[IncreasingLabels]") {
    // Set the label order.
    label_order = TILEDB_INCREASING_LABELS;

    // Set input values.
    input_index_data = {0, 1, 2, 3};
    input_label_data = {-1.0, 0.0, 0.5, 1.0};
    input_attr_data = {};

    // Set expected_output values.
    label_data_sorted_by_index = input_label_data;
    label_data_sorted_by_label = input_label_data;
    index_data_sorted_by_label = input_index_data;
  }

  SECTION("Write decreasing labels and array", "[DecreasingLabels]") {
    // Set the label order.
    label_order = TILEDB_DECREASING_LABELS;

    // Set the data values.
    input_index_data = {0, 1, 2, 3};
    input_label_data = {1.0, 0.0, -0.5, -1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};

    // Define expected output data.
    label_data_sorted_by_index = input_label_data;
    label_data_sorted_by_label = {-1.0, -0.5, 0.0, 1.0};
    index_data_sorted_by_label = {3, 2, 1, 0};
  }

  SECTION("Write decreasing labels only", "[DecreasingLabels]") {
    // Set the label order.
    label_order = TILEDB_DECREASING_LABELS;

    // Set the data values.
    input_index_data = {0, 1, 2, 3};
    input_label_data = {1.0, 0.0, -0.5, -1.0};

    // Define expected output data.
    label_data_sorted_by_index = input_label_data;
    label_data_sorted_by_label = {-1.0, -0.5, 0.0, 1.0};
    index_data_sorted_by_label = {3, 2, 1, 0};
  }

  SECTION("Write unordered labels and array", "[UnorderedLabels]") {
    // Set the label order.
    label_order = TILEDB_UNORDERED_LABELS;

    // Set the data values.
    input_index_data = {0, 3, 2, 1};
    input_label_data = {1.0, 0.0, -0.5, -1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};

    // Define expected output data.
    label_data_sorted_by_index = {1.0, -1.0, -0.5, 0.0};
    label_data_sorted_by_label = {-1.0, -0.5, 0.0, 1.0};
    index_data_sorted_by_label = {1, 2, 3, 0};
  }

  SECTION("Write unordered labels only", "[UnorderedLabels]") {
    // Set the label order.
    label_order = TILEDB_UNORDERED_LABELS;

    // Set the data values.
    input_index_data = {0, 3, 2, 1};
    input_label_data = {1.0, 0.0, -0.5, -1.0};

    // Define expected output data.
    label_data_sorted_by_index = {1.0, -1.0, -0.5, 0.0};
    label_data_sorted_by_label = {-1.0, -0.5, 0.0, 1.0};
    index_data_sorted_by_label = {1, 2, 3, 0};
  }

  // Create and Write the array and label.
  create_example(label_order);
  write_array_with_label(input_index_data, input_attr_data, input_label_data);

  // Check the dimension label arrays have the correct data.
  {
    INFO("Check indexed array data");
    check_indexed_array_data(label_data_sorted_by_index);
  }
  {
    INFO("Check labelled array data");
    check_labelled_array_data(
        index_data_sorted_by_label, label_data_sorted_by_label);
  }

  // Check data reader.
  {
    INFO("Check data from data reader");
    check_values_from_data_reader(label_data_sorted_by_index);
  }
}

TEST_CASE_METHOD(
    SparseArrayExample1,
    "Test error on bad dimension label order for sparse array",
    "[capi][query][DimensionLabel]") {
  // Vectors for input data.
  std::vector<uint64_t> input_index_data{};
  std::vector<double> input_label_data{};
  std::vector<double> input_attr_data{};

  // Dimension label parameters.
  tiledb_label_order_t label_order;

  SECTION("Increasing labels with bad order", "[IncreasingLabels]") {
    label_order = TILEDB_INCREASING_LABELS;
    input_index_data = {0, 1, 2, 3};
    input_label_data = {1.0, 0.0, -0.5, -1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Increasing labels with duplicate values", "[IncreasingLabels]") {
    label_order = TILEDB_INCREASING_LABELS;
    input_index_data = {0, 1, 2, 3};
    input_label_data = {-1.0, 0.0, 0.0, 1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Increasing labels with unordered index", "[IncreasingLabels]") {
    label_order = TILEDB_INCREASING_LABELS;
    input_index_data = {2, 0, 1, 3};
    input_label_data = {-1.0, 0.0, 0.5, 1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Decreasing labels with bad order", "[IncreasingLabels]") {
    label_order = TILEDB_DECREASING_LABELS;
    input_index_data = {0, 1, 2, 3};
    input_label_data = {-1.0, -0.5, 0.0, 1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Increasing labels with duplicate values", "[IncreasingLabels]") {
    label_order = TILEDB_DECREASING_LABELS;
    input_index_data = {0, 1, 2, 3};
    input_label_data = {1.0, 0.0, 0.0, -1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Increasing labels with unordered index", "[IncreasingLabels]") {
    label_order = TILEDB_DECREASING_LABELS;
    input_index_data = {2, 1, 0, 3};
    input_label_data = {1.0, 0.0, -0.5, -1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  // Create the array.
  create_example(label_order);
  write_array_with_label(
      input_index_data, input_attr_data, input_label_data, true);
}
