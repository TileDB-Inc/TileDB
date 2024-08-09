/**
 * @file  test-capi-array-write-ordered-attr-fixed.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests sort checks for fixed length ordered attributes.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
using namespace Catch::Matchers;

#include <test/support/src/helpers.h>
#include <test/support/src/vfs_helpers.h>
#include <catch2/catch_test_macros.hpp>
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

#include <numeric>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

template <typename T, Datatype type, DataOrder order>
struct FixedOrderedAttributeArrayFixture {
 public:
  FixedOrderedAttributeArrayFixture()
      : temp_dir_{}
      , ctx{temp_dir_.get_ctx()} {
    // Allocate array schema.
    tiledb_array_schema_t* schema{nullptr};
    require_tiledb_ok(tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &schema));

    // Set the domian.
    uint32_t dim_domain_data[2]{0, 31};
    uint32_t tile{32};
    tiledb_dimension_t* dim;
    tiledb_dimension_alloc(
        ctx, "x", TILEDB_INT32, &dim_domain_data[0], &tile, &dim);
    tiledb_domain_t* domain;
    tiledb_domain_alloc(ctx, &domain);
    tiledb_domain_add_dimension(ctx, domain, dim);
    tiledb_array_schema_set_domain(ctx, schema, domain);
    tiledb_dimension_free(&dim);
    tiledb_domain_free(&domain);

    // Define the attribute: skip C-API since ordered attributes aren't
    // exposed in the C-API yet.
    auto attr = make_shared<sm::Attribute>(HERE(), "a", type, 1, order);
    auto st = schema->array_schema_->add_attribute(attr);
    REQUIRE(st.ok());

    // Create the array and clean-up.
    std::string base_name{
        "array_ordered_attr_" + datatype_str(type) + "_" +
        data_order_str(order)};
    array_name = temp_dir_.create_temporary_array(std::move(base_name), schema);
    tiledb_array_schema_free(&schema);
  }

  /**
   * Read back data from an array and verify it matches the expected data.
   *
   * @param min_index Lower bound for range to read data from.
   * @param max_index Upper bound for range to read data from.
   * @param expected_data The expected output data.
   */
  void check_array_data(
      uint32_t min_index,
      uint32_t max_index,
      const std::vector<T>& expected_data) {
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));

    // Define sizes for setting buffers.
    uint64_t attr_data_size{expected_data.size() * sizeof(T)};
    std::vector<T> output_data(expected_data.size());

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &min_index, &max_index, nullptr));

    // Create write query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_data_buffer(
        ctx, query, "a", output_data.data(), &attr_data_size));

    // Submit read query and verify it is successful.
    check_tiledb_ok(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    check_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
    CHECK(query_status == TILEDB_COMPLETED);

    CHECK(output_data == expected_data);

    // Clean-up.
    tiledb_subarray_free(&subarray);
    tiledb_query_free(&query);
    tiledb_array_free(&array);
  }

  /**
   * Write data to the ordered attribute.
   *
   * @param min_index Lower bound for range to write data to.
   * @param max_index Upper bound for range to write data to.
   * @param data Data to write to the attribute.
   * @param valid If ``true`` the write should succeed, otherwise the write
   *     should fail.
   */
  void write_fragment(
      uint32_t min_index,
      uint32_t max_index,
      std::vector<T>& data,
      bool valid = true) {
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

    // Define sizes for setting buffers.
    uint64_t attr_data_size{data.size() * sizeof(T)};

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &min_index, &max_index, nullptr));

    // Create write query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_data_buffer(
        ctx, query, "a", data.data(), &attr_data_size));

    // Submit query.
    if (valid) {
      // Submit write query and verify it is successful.
      check_tiledb_ok(tiledb_query_submit(ctx, query));
      tiledb_query_status_t query_status;
      check_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
      CHECK(query_status == TILEDB_COMPLETED);
    } else {
      // Submit write query and verify if fails.
      require_tiledb_error_with(
          tiledb_query_submit(ctx, query),
          "WriterBase: The data for attribute 'a' is not in the expected "
          "order.");
    }

    // Clean-up.
    tiledb_subarray_free(&subarray);
    tiledb_query_free(&query);
    tiledb_array_free(&array);
  }

  /** Check a TileDB return code is ok. */
  inline void check_tiledb_ok(int rc) const {
    temp_dir_.check_tiledb_ok(rc);
  }

  /** Require a TileDB return code is ok. */
  inline void require_tiledb_ok(int rc) const {
    temp_dir_.require_tiledb_ok(rc);
  }

  /** Require a TileDB return code is an error with the given message. */
  inline void require_tiledb_error_with(int rc, const std::string& msg) {
    temp_dir_.require_tiledb_error_with(rc, msg);
  }

 protected:
  /** Temporary directory and virtual file system. */
  test::TemporaryDirectoryFixture temp_dir_;

  /**
   * TileDB context.
   *
   * This context in managed by the above temporary directory fixture.
   */
  tiledb_ctx_t* ctx;

  /** Name of the array. */
  std::string array_name;
};

TEMPLATE_TEST_CASE_SIG(
    "Write to increasing attributes with fixed length: valid",
    "[ordered-writer][fixed][ordered-data]",
    ((typename T, sm::Datatype datatype), T, datatype),
    (uint8_t, Datatype::UINT8),
    (int8_t, Datatype::INT8),
    (uint16_t, Datatype::UINT16),
    (int16_t, Datatype::INT16),
    (uint32_t, Datatype::UINT32),
    (int32_t, Datatype::INT32),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::INT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (int64_t, Datatype::DATETIME_MONTH),
    (int64_t, Datatype::DATETIME_WEEK),
    (int64_t, Datatype::DATETIME_DAY),
    (int64_t, Datatype::DATETIME_HR),
    (int64_t, Datatype::DATETIME_MIN),
    (int64_t, Datatype::DATETIME_SEC),
    (int64_t, Datatype::DATETIME_MS),
    (int64_t, Datatype::DATETIME_US),
    (int64_t, Datatype::DATETIME_NS),
    (int64_t, Datatype::DATETIME_PS),
    (int64_t, Datatype::DATETIME_FS),
    (int64_t, Datatype::DATETIME_AS),
    (int64_t, Datatype::TIME_HR),
    (int64_t, Datatype::TIME_MIN),
    (int64_t, Datatype::TIME_SEC),
    (int64_t, Datatype::TIME_MS),
    (int64_t, Datatype::TIME_US),
    (int64_t, Datatype::TIME_NS),
    (int64_t, Datatype::TIME_PS),
    (int64_t, Datatype::TIME_FS),
    (int64_t, Datatype::TIME_AS),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  auto fixture = FixedOrderedAttributeArrayFixture<
      T,
      datatype,
      sm::DataOrder::INCREASING_DATA>{};
  std::vector<T> data{3, 4, 5, 6, 7, 8};
  fixture.write_fragment(2, 7, data);
  fixture.check_array_data(2, 7, data);
}

TEMPLATE_TEST_CASE_SIG(
    "Write to decreasing attributes with fixed length: valid",
    "[ordered-writer][fixed][ordered-data]",
    ((typename T, sm::Datatype datatype), T, datatype),
    (uint8_t, Datatype::UINT8),
    (int8_t, Datatype::INT8),
    (uint16_t, Datatype::UINT16),
    (int16_t, Datatype::INT16),
    (uint32_t, Datatype::UINT32),
    (int32_t, Datatype::INT32),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::INT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (int64_t, Datatype::DATETIME_MONTH),
    (int64_t, Datatype::DATETIME_WEEK),
    (int64_t, Datatype::DATETIME_DAY),
    (int64_t, Datatype::DATETIME_HR),
    (int64_t, Datatype::DATETIME_MIN),
    (int64_t, Datatype::DATETIME_SEC),
    (int64_t, Datatype::DATETIME_MS),
    (int64_t, Datatype::DATETIME_US),
    (int64_t, Datatype::DATETIME_NS),
    (int64_t, Datatype::DATETIME_PS),
    (int64_t, Datatype::DATETIME_FS),
    (int64_t, Datatype::DATETIME_AS),
    (int64_t, Datatype::TIME_HR),
    (int64_t, Datatype::TIME_MIN),
    (int64_t, Datatype::TIME_SEC),
    (int64_t, Datatype::TIME_MS),
    (int64_t, Datatype::TIME_US),
    (int64_t, Datatype::TIME_NS),
    (int64_t, Datatype::TIME_PS),
    (int64_t, Datatype::TIME_FS),
    (int64_t, Datatype::TIME_AS),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  auto fixture = FixedOrderedAttributeArrayFixture<
      T,
      datatype,
      sm::DataOrder::DECREASING_DATA>{};
  std::vector<T> data{8, 7, 6, 5, 4, 3};
  fixture.write_fragment(2, 7, data);
  fixture.check_array_data(2, 7, data);
}

TEMPLATE_TEST_CASE_SIG(
    "Write to increasing attributes with fixed length: Invalid order",
    "[ordered-writer][fixed][ordered-data]",
    ((typename T, sm::Datatype datatype), T, datatype),
    (uint8_t, Datatype::UINT8),
    (int8_t, Datatype::INT8),
    (uint16_t, Datatype::UINT16),
    (int16_t, Datatype::INT16),
    (uint32_t, Datatype::UINT32),
    (int32_t, Datatype::INT32),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::INT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (int64_t, Datatype::DATETIME_MONTH),
    (int64_t, Datatype::DATETIME_WEEK),
    (int64_t, Datatype::DATETIME_DAY),
    (int64_t, Datatype::DATETIME_HR),
    (int64_t, Datatype::DATETIME_MIN),
    (int64_t, Datatype::DATETIME_SEC),
    (int64_t, Datatype::DATETIME_MS),
    (int64_t, Datatype::DATETIME_US),
    (int64_t, Datatype::DATETIME_NS),
    (int64_t, Datatype::DATETIME_PS),
    (int64_t, Datatype::DATETIME_FS),
    (int64_t, Datatype::DATETIME_AS),
    (int64_t, Datatype::TIME_HR),
    (int64_t, Datatype::TIME_MIN),
    (int64_t, Datatype::TIME_SEC),
    (int64_t, Datatype::TIME_MS),
    (int64_t, Datatype::TIME_US),
    (int64_t, Datatype::TIME_NS),
    (int64_t, Datatype::TIME_PS),
    (int64_t, Datatype::TIME_FS),
    (int64_t, Datatype::TIME_AS),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  auto fixture = FixedOrderedAttributeArrayFixture<
      T,
      datatype,
      sm::DataOrder::INCREASING_DATA>{};
  // Write initial data.
  std::vector<T> valid_data{1, 2, 3, 4};
  fixture.write_fragment(4, 7, valid_data);

  // Try writing invalid data.
  std::vector<T> invalid_data{10, 10, 11, 12};
  fixture.write_fragment(4, 7, invalid_data, false);

  // Verify array data is unchanged for bad write.
  fixture.check_array_data(4, 7, valid_data);
}

TEMPLATE_TEST_CASE_SIG(
    "Write to decreasing attributes with fixed length: Invalid order",
    "[ordered-writer][fixed][ordered-data]",
    ((typename T, sm::Datatype datatype), T, datatype),
    (uint8_t, Datatype::UINT8),
    (int8_t, Datatype::INT8),
    (uint16_t, Datatype::UINT16),
    (int16_t, Datatype::INT16),
    (uint32_t, Datatype::UINT32),
    (int32_t, Datatype::INT32),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::INT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (int64_t, Datatype::DATETIME_MONTH),
    (int64_t, Datatype::DATETIME_WEEK),
    (int64_t, Datatype::DATETIME_DAY),
    (int64_t, Datatype::DATETIME_HR),
    (int64_t, Datatype::DATETIME_MIN),
    (int64_t, Datatype::DATETIME_SEC),
    (int64_t, Datatype::DATETIME_MS),
    (int64_t, Datatype::DATETIME_US),
    (int64_t, Datatype::DATETIME_NS),
    (int64_t, Datatype::DATETIME_PS),
    (int64_t, Datatype::DATETIME_FS),
    (int64_t, Datatype::DATETIME_AS),
    (int64_t, Datatype::TIME_HR),
    (int64_t, Datatype::TIME_MIN),
    (int64_t, Datatype::TIME_SEC),
    (int64_t, Datatype::TIME_MS),
    (int64_t, Datatype::TIME_US),
    (int64_t, Datatype::TIME_NS),
    (int64_t, Datatype::TIME_PS),
    (int64_t, Datatype::TIME_FS),
    (int64_t, Datatype::TIME_AS),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  auto fixture = FixedOrderedAttributeArrayFixture<
      T,
      datatype,
      sm::DataOrder::DECREASING_DATA>{};
  // Write initial data.
  std::vector<T> valid_data{4, 3, 2, 1};
  fixture.write_fragment(4, 7, valid_data);

  // Try writing invalid data.
  std::vector<T> invalid_data{12, 11, 10, 10};
  fixture.write_fragment(4, 7, invalid_data, false);

  // Verify array data is unchanged for bad write.
  fixture.check_array_data(4, 7, valid_data);
}
