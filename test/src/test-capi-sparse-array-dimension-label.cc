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
 * Test the dimension label API with a sparse array using fixed size dimension
 * labels.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/serialization_wrappers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"

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
 *    - x: (label_order=label_order, dim_idx=0, type=FLOAT64)
 */
class SparseArrayExample1 : public TemporaryDirectoryFixture {
 public:
  /**
   * If true, array schema is serialized before submission, to test the
   * serialization paths.
   */
  bool serialize_ = false;

  SparseArrayExample1()
      : array_name{}
      , index_domain_{0, 3}
      , label_domain_{-1, 1} {
  }

  /**
   * Create the example array with a dimension label.
   *
   * @param label_order Label order for the dimension label.
   */
  void create_example(tiledb_data_order_t label_order) {
    auto ctx = get_ctx();
    // Create an array schema
    uint64_t x_tile_extent{4};
    auto array_schema = create_array_schema(
        ctx,
        TILEDB_SPARSE,
        {"dim"},
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
    tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 0, "x", label_order, TILEDB_FLOAT64);
    // Create array
    array_name =
        create_temporary_array("array_with_label_1", array_schema, serialize_);
    tiledb_array_schema_free(&array_schema);
  }

  /**
   * Write data to the array and dimension label.
   *
   * @param input_index_data Coordinate data for the array. If empty, the
   *     coordinate data is not added to the query.
   * @param input_attr_data Data to write to the array attribute. If empty, the
   *     attribute data is not added to the query..
   * @param input_label_data Data to write to the dimension label. If empty, the
   *     dimension label dadta is not added to the query.
   * @param error_on_write If true, require the query returns a failed status.
   */
  void write_array_with_label(
      std::vector<uint64_t>& input_index_data,
      std::vector<double>& input_attr_data,
      std::vector<double>& input_label_data,
      bool error_on_write = false) {
    auto ctx = get_ctx();
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
          ctx, query, "dim", input_index_data.data(), &index_data_size));
    }
    if (attr_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "a", input_attr_data.data(), &attr_data_size));
    }
    if (label_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
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
      const std::vector<double>& expected_label_data,
      const std::vector<double>& expected_attr_data) {
    auto ctx = get_ctx();
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
    std::vector<double> attr_data(4);
    uint64_t attr_data_size{attr_data.size() * sizeof(double)};

    // Create read query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED));
    require_tiledb_ok(tiledb_query_set_data_buffer(
        ctx, query, "x", label_data.data(), &label_data_size));
    if (!expected_attr_data.empty()) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "a", attr_data.data(), &attr_data_size));
    }

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
    for (uint64_t index{0}; index < 4; ++index) {
      INFO("Label data " + std::to_string(index));
      CHECK(label_data[index] == expected_label_data[index]);
    }
    if (!expected_attr_data.empty()) {
      for (uint64_t index{0}; index < 4; ++index) {
        INFO("Attribute data index=" + std::to_string(index));
        CHECK(attr_data[index] == expected_attr_data[index]);
      }
    }
  }

  /**
   * Check data from a query using label ranges.
   *
   * @param expected_label_data A vector of the expected label values.
   * @param expected_attr_data A vector of the expected attribute values. If
   *     empty, do not read attribute data.
   */
  void check_values_from_range_reader(
      const std::vector<double> ranges,
      const std::vector<double> expected_label_data,
      const std::vector<double> expected_attr_data) {
    auto ctx = get_ctx();
    // Open array for reading.
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    for (uint64_t r{0}; r < ranges.size() / 2; r++) {
      require_tiledb_ok(tiledb_subarray_add_label_range(
          ctx, subarray, "x", &ranges[2 * r], &ranges[2 * r + 1], nullptr));
    }

    if (serialize_) {
      tiledb_subarray_serialize(ctx, array, &subarray);
    }

    // Define label buffer and size.
    std::vector<double> label_data(expected_label_data.size());
    uint64_t label_data_size{label_data.size() * sizeof(double)};

    // Define attribute buffer and size.
    std::vector<double> attr_data(expected_attr_data.size());
    uint64_t attr_data_size{attr_data.size() * sizeof(double)};

    // Create read query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    if (!expected_label_data.empty()) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "x", label_data.data(), &label_data_size));
    }
    if (!expected_attr_data.empty()) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "a", attr_data.data(), &attr_data_size));
    }

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
    for (uint64_t index{0}; index < expected_label_data.size(); ++index) {
      CHECK(label_data[index] == expected_label_data[index]);
    }
    for (uint64_t index{0}; index < expected_attr_data.size(); ++index) {
      CHECK(attr_data[index] == expected_attr_data[index]);
    }
  }

 protected:
  /** Name of the example array. */
  std::string array_name;

  /** Valid range for the index. */
  uint64_t index_domain_[2];

  /** Valid range for the label. */
  double label_domain_[2];
};

TEST_CASE_METHOD(
    SparseArrayExample1,
    "Round trip dimension label data for sparse 1D array",
    "[capi][query][DimensionLabel]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif

  // Input vectors.
  std::vector<uint64_t> input_index_data{4};
  std::vector<double> input_label_data{4};
  std::vector<double> input_attr_data{4};

  // Expected output vectors.
  std::vector<double> label_data_sorted_by_index{4};
  std::vector<double> label_data_sorted_by_label{4};
  std::vector<double> attr_data_sorted_by_index{4};
  std::vector<uint64_t> index_data_sorted_by_label{4};

  // Dimension label parameters.
  tiledb_data_order_t label_order{};

  SECTION("Write increasing labels", "[IncreasingLabels]") {
    // Set the label order.
    label_order = TILEDB_INCREASING_DATA;

    // Set input values.
    input_index_data = {0, 1, 2, 3};
    input_label_data = {-1.0, 0.0, 0.5, 1.0};

    // Set attribute values.
    SECTION("With attribute values") {
      input_attr_data = {0.5, 1.0, 1.5, 2.0};
    }
    SECTION("Without attribute values") {
      input_attr_data = {};
    }

    // Set expected_output values.
    label_data_sorted_by_index = input_label_data;
    label_data_sorted_by_label = input_label_data;
    attr_data_sorted_by_index = input_attr_data;
    index_data_sorted_by_label = input_index_data;
  }

  SECTION("Write decreasing labels and array", "[DecreasingLabels]") {
    // Set the label order.
    label_order = TILEDB_DECREASING_DATA;

    // Set the data values.
    input_index_data = {0, 1, 2, 3};
    input_label_data = {1.0, 0.0, -0.5, -1.0};
    SECTION("With attribute values") {
      input_attr_data = {0.5, 1.0, 1.5, 2.0};
    }
    SECTION("Without attribute values") {
      input_attr_data = {};
    }

    // Define expected output data.
    label_data_sorted_by_index = input_label_data;
    label_data_sorted_by_label = {-1.0, -0.5, 0.0, 1.0};
    attr_data_sorted_by_index = input_attr_data;
    index_data_sorted_by_label = {3, 2, 1, 0};
  }

  INFO(
      "Testing array with label order " +
      data_order_str(static_cast<DataOrder>(label_order)) + ".");

  // Create and Write the array and label.
  create_example(label_order);
  write_array_with_label(input_index_data, input_attr_data, input_label_data);

  // Check values when reading by index ranges.
  {
    INFO("Reading values by index range.");
    check_values_from_data_reader(
        label_data_sorted_by_index, attr_data_sorted_by_index);
  }

  // Check values when reading by label ranges.
  {
    INFO("Reading data by label range.");

    // Check query on full range.
    check_values_from_range_reader(
        {label_domain_[0], label_domain_[1]},
        label_data_sorted_by_index,
        attr_data_sorted_by_index);

    // Check point query on each individual value.
    if (input_attr_data.empty()) {
      for (uint64_t index{0}; index < 4; ++index) {
        check_values_from_range_reader(
            {input_label_data[index], input_label_data[index]},
            {input_label_data[index]},
            {});
      }
    } else {
      for (uint64_t index{0}; index < 4; ++index) {
        check_values_from_range_reader(
            {input_label_data[index], input_label_data[index]},
            {input_label_data[index]},
            {input_attr_data[index]});
      }
    }
  }
}

TEST_CASE_METHOD(
    SparseArrayExample1,
    "Test error on bad dimension label order for sparse array",
    "[capi][query][DimensionLabel]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
#endif

  // Vectors for input data.
  std::vector<uint64_t> input_index_data{4};
  std::vector<double> input_label_data{4};
  std::vector<double> input_attr_data{4};

  // Dimension label parameters.
  tiledb_data_order_t label_order{};

  SECTION("Increasing labels with bad order", "[IncreasingLabels]") {
    label_order = TILEDB_INCREASING_DATA;
    input_index_data = {0, 1, 2, 3};
    input_label_data = {1.0, 0.0, -0.5, -1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Increasing labels with duplicate values", "[IncreasingLabels]") {
    label_order = TILEDB_INCREASING_DATA;
    input_index_data = {0, 1, 2, 3};
    input_label_data = {-1.0, 0.0, 0.0, 1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Increasing labels with unordered index", "[IncreasingLabels]") {
    label_order = TILEDB_INCREASING_DATA;
    input_index_data = {2, 0, 1, 3};
    input_label_data = {-1.0, 0.0, 0.5, 1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Decreasing labels with bad order", "[IncreasingLabels]") {
    label_order = TILEDB_DECREASING_DATA;
    input_index_data = {0, 1, 2, 3};
    input_label_data = {-1.0, -0.5, 0.0, 1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Increasing labels with duplicate values", "[IncreasingLabels]") {
    label_order = TILEDB_DECREASING_DATA;
    input_index_data = {0, 1, 2, 3};
    input_label_data = {1.0, 0.0, 0.0, -1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  SECTION("Increasing labels with unordered index", "[IncreasingLabels]") {
    label_order = TILEDB_DECREASING_DATA;
    input_index_data = {2, 1, 0, 3};
    input_label_data = {1.0, 0.0, -0.5, -1.0};
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }

  // Create the array.
  create_example(label_order);
  write_array_with_label(
      input_index_data, input_attr_data, input_label_data, true);
}
