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

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"

#include <catch2/catch_test_macros.hpp>
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
class ExampleArray : public TemporaryDirectoryFixture {
 public:
  ExampleArray()
      : domain_{1, 4}
      , t_domain_{1, 8} {
    // Create an array schema
    uint64_t tile_extent{4};
    auto array_schema = create_array_schema(
        ctx,
        TILEDB_DENSE,
        {"ix", "iy", "iz", "it"},
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
    tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 3, "t", TILEDB_INCREASING_DATA, TILEDB_DATETIME_SEC);

    tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 0, "x", TILEDB_INCREASING_DATA, TILEDB_FLOAT64);

    tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 1, "y", TILEDB_INCREASING_DATA, TILEDB_FLOAT64);

    tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 2, "z", TILEDB_INCREASING_DATA, TILEDB_FLOAT64);

    tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 0, "alpha", TILEDB_DECREASING_DATA, TILEDB_FLOAT64);

    tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 1, "beta", TILEDB_DECREASING_DATA, TILEDB_FLOAT64);

    tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 2, "gamma", TILEDB_DECREASING_DATA, TILEDB_FLOAT64);

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
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "x", x1.data(), &x1_size));
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "alpha", x2.data(), &x2_size));
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "y", y1.data(), &y1_size));
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "beta", y2.data(), &y2_size));
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "z", z1.data(), &z1_size));
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "gamma", z2.data(), &z2_size));
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "t", time.data(), &time_size));

    // Submit write query.
    require_tiledb_ok(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    require_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_subarray_free(&subarray);
    tiledb_query_free(&query);
    tiledb_array_free(&array);
  }

  /**
   * Read values in the array and dimension labels and check against expected
   * values.
   *
   * @param index_ranges Map from dimension index to ranges to query on.
   * @param label_ranges Map from label name to ranges to query on.
   * @param expected_x1 Expected values for the first x label.
   * @param expected_x2 Expected values for the second x label.
   * @param expected_y1 Expected values for the first y label.
   * @param expected_y2 Expected values for the second y label.
   * @param expected_z1 Expected values for the first z label.
   * @param expected_z2 Expected values for the second z label.
   * @param expected_time Excepted values for the time data.
   */
  void check_values(
      std::unordered_map<uint32_t, std::vector<uint64_t>> index_ranges,
      std::unordered_map<std::string, std::vector<const void*>> label_ranges,
      const std::vector<double>& expected_a,
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
    for (const auto& [dim_idx, ranges] : index_ranges) {
      for (size_t r{0}; r < ranges.size(); r += 2) {
        require_tiledb_ok(tiledb_subarray_add_range(
            ctx, subarray, dim_idx, &ranges[r], &ranges[r + 1], nullptr));
      }
    }
    for (const auto& [label_name, ranges] : label_ranges) {
      for (size_t r{0}; r < ranges.size(); r += 2) {
        require_tiledb_ok(tiledb_subarray_add_label_range(
            ctx,
            subarray,
            label_name.c_str(),
            ranges[r],
            ranges[r + 1],
            nullptr));
      }
    }

    // Define label data buffers.
    std::vector<double> a(expected_a.size());
    std::vector<double> x1(expected_x1.size());
    std::vector<double> x2(expected_x2.size());
    std::vector<double> y1(expected_y1.size());
    std::vector<double> y2(expected_y2.size());
    std::vector<double> z1(expected_z1.size());
    std::vector<double> z2(expected_z2.size());
    std::vector<int64_t> time(expected_time.size());

    // Define sizes for setting buffers.
    uint64_t a_size{a.size() * sizeof(double)};
    uint64_t x1_size{x1.size() * sizeof(double)};
    uint64_t x2_size{x2.size() * sizeof(double)};
    uint64_t y1_size{y1.size() * sizeof(double)};
    uint64_t y2_size{y2.size() * sizeof(double)};
    uint64_t z1_size{z1.size() * sizeof(double)};
    uint64_t z2_size{z2.size() * sizeof(double)};
    uint64_t time_size{time.size() * sizeof(int64_t)};

    // Create read query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    if (a_size > 0) {
      require_tiledb_ok(
          tiledb_query_set_data_buffer(ctx, query, "a", a.data(), &a_size));
    }
    if (x1_size > 0) {
      require_tiledb_ok(
          tiledb_query_set_data_buffer(ctx, query, "x", x1.data(), &x1_size));
    }
    if (x2_size > 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "alpha", x2.data(), &x2_size));
    }
    if (y1_size > 0) {
      require_tiledb_ok(
          tiledb_query_set_data_buffer(ctx, query, "y", y1.data(), &y1_size));
    }
    if (y2_size > 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "beta", y2.data(), &y2_size));
    }
    if (z1_size > 0) {
      require_tiledb_ok(
          tiledb_query_set_data_buffer(ctx, query, "z", z1.data(), &z1_size));
    }
    if (z2_size > 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "gamma", z2.data(), &z2_size));
    }
    if (time_size > 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "t", time.data(), &time_size));
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
    for (uint64_t index{0}; index < expected_a.size(); ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(a[index] == expected_a[index]);
    }

    for (uint64_t index{0}; index < expected_x1.size(); ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(x1[index] == expected_x1[index]);
    }

    for (uint64_t index{0}; index < expected_x2.size(); ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(x2[index] == expected_x2[index]);
    }

    for (uint64_t index{0}; index < expected_y1.size(); ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(y1[index] == expected_y1[index]);
    }

    for (uint64_t index{0}; index < expected_y2.size(); ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(y2[index] == expected_y2[index]);
    }

    for (uint64_t index{0}; index < expected_z1.size(); ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(z1[index] == expected_z1[index]);
    }

    for (uint64_t index{0}; index < expected_z2.size(); ++index) {
      UNSCOPED_INFO("index = " + std::to_string(index));
      CHECK(z2[index] == expected_z2[index]);
    }

    for (uint64_t index{0}; index < expected_time.size(); ++index) {
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

  // Check the data when querying by dimension.
  {
    INFO("Reading values from index ranges.");
    check_values(
        {{0, {1, 4}}, {1, {1, 4}}, {2, {1, 4}}, {3, {1, 8}}},
        {},
        input_a,
        input_x1,
        input_x2,
        input_y1,
        input_y2,
        input_z1,
        input_z2,
        input_t);
  }

  // Check the data when querying by label.
  //  - This checks we can read the values from one dimension label
  //  using the ranges from another dimension label set on the same
  //  dimension.
  {
    INFO("Reading values from label ranges.");
    double x_range[2]{-10.0, 10.0};
    double beta_range[2]{0.0, 100.0};
    double z_range[2]{-10.0, 10.0};
    uint64_t time_range[2]{0, 100};

    check_values(
        {},
        {{"x", {&x_range[0], &x_range[1]}},
         {"beta", {&beta_range[0], &beta_range[1]}},
         {"z", {&z_range[0], &z_range[1]}},
         {"t", {&time_range[0], &time_range[1]}}},
        input_a,
        input_x1,
        input_x2,
        input_y1,
        input_y2,
        input_z1,
        input_z2,
        input_t);
  }
}
