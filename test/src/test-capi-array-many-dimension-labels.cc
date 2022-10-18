/**
 * @file test-capi-array-many-dimension-labels.cc
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
 * Test the dimension label API by writing many dimension labels to a dense
 * array.
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
 * Create a multi-dimensional array with multiple dimension labels.
 *
 * Array Summary:
 *  * Array Type: Dense
 *  * Dimensions:
 *    - x: (type=UINT64, domain=[1, 4], tile=4)
 *    - y: (type=UINT64, domain=[1, 4], tile=4)
 *    - z: (type=UINT64, domain=[1, 4], tile=4)
 *    - t: (type=UINT64, domain=[1, 8], tile=4)
 *  * Attributes:
 *    - a: (type=FLOAT64)
 *  * Dimension labels:
 *    - t: (label_order=INCREASING, dim_idx=3, type=DATETIME_SEC)
 *    - x: (label_order=INCREASING, dim_idx=0, type=FLOAT64)
 *    - y: (label_order=INCREASING, dim_idx=1, type=FLOAT64)
 *    - z: (label_order=INCREASING, dim_idx=2, type=FLOAT64)
 *    - alpha: (label_order=DECREASING, dim_idx=0, type=FLOAT64)
 *    - beta:  (label_order=DECREASING, dim_idx=1, type=FLOAT64)
 *    - gamma: (label_order=DEACRESING, dim_idx=2, type=FLOAT64)
 *
 */
class ExampleArray : public DimensionLabelFixture {
 public:
  ExampleArray()
      : domain_{1, 4}
      , t_domain_{1, 8}
      , temporal_label_domain_{0, 100}
      , spatial_label1_domain_{-10.0, 10.0}
      , spatial_label2_domain_{0.0, 100.0} {
    // Create an array schema
    uint64_t tile_extent{4};
    auto array_schema = create_array_schema(
        ctx,
        TILEDB_DENSE,
        {"x", "y", "z", "t"},
        {TILEDB_UINT64, TILEDB_UINT64, TILEDB_UINT64, TILEDB_UINT64},
        {&domain_[0], &domain_[0], &domain_[0], &t_domain_[0]},
        {&tile_extent, &tile_extent, &tile_extent, &tile_extent},
        {"a"},
        {TILEDB_FLOAT64},
        {1},
        {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        4096,
        false);

    // Add dimension labels.
    double spatial_label1_tile = 20.0;
    double spatial_label2_tile = 100.0;
    int64_t temporal_tile = 101;

    add_dimension_label(
        ctx,
        array_schema,
        "t",
        3,
        TILEDB_INCREASING_LABELS,
        TILEDB_DATETIME_SEC,
        &temporal_label_domain_[0],
        &temporal_tile,
        &tile_extent);

    add_dimension_label(
        ctx,
        array_schema,
        "x",
        0,
        TILEDB_INCREASING_LABELS,
        TILEDB_FLOAT64,
        &spatial_label1_domain_[0],
        &spatial_label1_tile,
        &tile_extent);

    add_dimension_label(
        ctx,
        array_schema,
        "y",
        1,
        TILEDB_INCREASING_LABELS,
        TILEDB_FLOAT64,
        &spatial_label1_domain_[0],
        &spatial_label1_tile,
        &tile_extent);

    add_dimension_label(
        ctx,
        array_schema,
        "z",
        2,
        TILEDB_INCREASING_LABELS,
        TILEDB_FLOAT64,
        &spatial_label1_domain_[0],
        &spatial_label1_tile,
        &tile_extent);

    add_dimension_label(
        ctx,
        array_schema,
        "x_shifted",
        0,
        TILEDB_DECREASING_LABELS,
        TILEDB_FLOAT64,
        &spatial_label2_domain_[0],
        &spatial_label2_tile,
        &tile_extent);

    add_dimension_label(
        ctx,
        array_schema,
        "y_shifted",
        1,
        TILEDB_DECREASING_LABELS,
        TILEDB_FLOAT64,
        &spatial_label2_domain_[0],
        &spatial_label2_tile,
        &tile_extent);

    add_dimension_label(
        ctx,
        array_schema,
        "z_shifted",
        2,
        TILEDB_DECREASING_LABELS,
        TILEDB_FLOAT64,
        &spatial_label2_domain_[0],
        &spatial_label2_tile,
        &tile_extent);

    // Create array
    array_name =
        create_temporary_array("array_with_multiple_labels", array_schema);
    tiledb_array_schema_free(&array_schema);
  }

  /**
   * Write data to the array and all of the dimensions.
   *
   * @param a1 Data for the array attribute.
   * @param x1 Data for the first x label.
   * @param x2 Data for the second x label.
   * @param y1 Data for the first y label.
   * @param y2 Data for the second y label.
   * @param z1 Data for the first z label.
   * @param z2 Data for the second z label.
   * @param time Data for the time data.
   */
  void write_array_with_labels(
      std::vector<double>& a1,
      std::vector<double>& x1,
      std::vector<double>& x2,
      std::vector<double>& y1,
      std::vector<double>& y2,
      std::vector<double>& z1,
      std::vector<double>& z2,
      std::vector<int64_t>& time) {
    // Open array for writing.
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

    // Define sizes for setting buffers.
    uint64_t a1_size{a1.size() * sizeof(double)};
    uint64_t x1_size{x1.size() * sizeof(double)};
    uint64_t x2_size{x2.size() * sizeof(double)};
    uint64_t y1_size{y1.size() * sizeof(double)};
    uint64_t y2_size{y2.size() * sizeof(double)};
    uint64_t z1_size{z1.size() * sizeof(double)};
    uint64_t z2_size{z2.size() * sizeof(double)};
    uint64_t time_size{time.size() * sizeof(double)};

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &domain_[0], &domain_[1], nullptr));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 1, &domain_[0], &domain_[1], nullptr));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 2, &domain_[0], &domain_[1], nullptr));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 3, &t_domain_[0], &t_domain_[1], nullptr));

    // Create write query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "a", a1.data(), &a1_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "x", x1.data(), &x1_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "x_shifted", x2.data(), &x2_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "y", y1.data(), &y1_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "y_shifted", y2.data(), &y2_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "z", z1.data(), &z1_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "z_shifted", z2.data(), &z2_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "t", time.data(), &time_size));

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
   * Read back and check label values in a data query.
   *
   * @param expected_x1 Expected values for the first x label.
   * @param expected_x2 Expected values for the second x label.
   * @param expected_y1 Expected values for the first y label.
   * @param expected_y2 Expected values for the second y label.
   * @param expected_z1 Expected values for the first z label.
   * @param expected_z2 Expected values for the second z label.
   * @param expected_time Excepted values for the time data.
   */
  void check_values_from_data_reader(
      const std::vector<double>& expected_x1,
      const std::vector<double>& expected_x2,
      const std::vector<double>& expected_y1,
      const std::vector<double>& expected_y2,
      const std::vector<double>& expected_z1,
      const std::vector<double>& expected_z2,
      const std::vector<int64_t>& expected_time) {
    // Open array for reading.
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &domain_[0], &domain_[1], nullptr));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 1, &domain_[0], &domain_[1], nullptr));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 2, &domain_[0], &domain_[1], nullptr));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 3, &t_domain_[0], &t_domain_[1], nullptr));

    // Define label data buffers.
    std::vector<double> x1(4);
    std::vector<double> x2(4);
    std::vector<double> y1(4);
    std::vector<double> y2(4);
    std::vector<double> z1(4);
    std::vector<double> z2(4);
    std::vector<int64_t> time(8);

    // Define sizes for setting buffers.
    uint64_t x1_size{x1.size() * sizeof(double)};
    uint64_t x2_size{x2.size() * sizeof(double)};
    uint64_t y1_size{y1.size() * sizeof(double)};
    uint64_t y2_size{y2.size() * sizeof(double)};
    uint64_t z1_size{z1.size() * sizeof(double)};
    uint64_t z2_size{z2.size() * sizeof(double)};
    uint64_t time_size{time.size() * sizeof(double)};

    // Create read query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "x", x1.data(), &x1_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "x_shifted", x2.data(), &x2_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "y", y1.data(), &y1_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "y_shifted", y2.data(), &y2_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "z", z1.data(), &z1_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "z_shifted", z2.data(), &z2_size));
    require_tiledb_ok(tiledb_query_set_label_data_buffer(
        ctx, query, "t", time.data(), &time_size));

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
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(x1[index] == expected_x1[index]);
    }

    for (uint64_t index{0}; index < 4; ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(x2[index] == expected_x2[index]);
    }

    for (uint64_t index{0}; index < 4; ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(y1[index] == expected_y1[index]);
    }

    for (uint64_t index{0}; index < 4; ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(y2[index] == expected_y2[index]);
    }

    for (uint64_t index{0}; index < 4; ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(z1[index] == expected_z1[index]);
    }

    for (uint64_t index{0}; index < 4; ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(z2[index] == expected_z2[index]);
    }

    for (uint64_t index{0}; index < 8; ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(time[index] == expected_time[index]);
    }
  }

 protected:
  /** Name of the example array. */
  std::string array_name;

 private:
  /** Domain for dimensions x, y, and z. */
  uint64_t domain_[2];

  /** Domain for dimension t. */
  uint64_t t_domain_[2];

  /** Domain for the dimension label on dimension t. */
  int64_t temporal_label_domain_[2];

  /** Domain for the dimension labels x, y, and z. */
  double spatial_label1_domain_[2];

  /** Domain for the dimension labels x_shifted, y_shifted, and z_shifted. */
  double spatial_label2_domain_[2];
};

TEST_CASE_METHOD(
    ExampleArray,
    "Test writing to array with many dimension labels",
    "[capi][query][DimensionLabel]") {
  // Input attribute data.
  std::vector<double> input_a(512);
  for (uint64_t index{0}; index < 512; ++index) {
    input_a[index] = 0.1 * index;
  }

  // Data for x labels.
  std::vector<double> input_x1{-10.0, -4.0, -2.0, 8.0};
  std::vector<double> input_x2{100.0, 70.0, 60.0, 30.0};

  // Data for y labels.
  std::vector<double> input_y1{-9.0, 2.0, 6.0, 10.0};
  std::vector<double> input_y2{95.0, 40.0, 20.0, 0.0};

  // Data for z labels.
  std::vector<double> input_z1{-10.0, -5.0, 5.0, 10.0};
  std::vector<double> input_z2{100, 75, 25, 0};

  // Data for t label.
  std::vector<int64_t> input_t{0, 4, 9, 11, 14, 15, 18, 20};

  // Write data.
  write_array_with_labels(
      input_a,
      input_x1,
      input_x2,
      input_y1,
      input_y2,
      input_z1,
      input_z2,
      input_t);

  // Check the data.
  check_values_from_data_reader(
      input_x1, input_x2, input_y1, input_y2, input_z1, input_z2, input_t);
}
