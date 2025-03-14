/**
 * @file tiledb/api/c_api/query_aggregate/test/unit_capi_query_aggregate.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests the query aggregate C API.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/query_aggregate/query_aggregate_api_external_experimental.h"
#include "tiledb/api/c_api/vfs/vfs_api_internal.h"

using namespace tiledb::test;

struct QueryAggregateFx : TemporaryDirectoryFixture {
  QueryAggregateFx()
      : array_name_(vfs_temp_.temp_dir_ + "queryaggregate_array") {
    rm_array();
    create_sparse_array();
    write_sparse_array();
  }
  ~QueryAggregateFx() {
    rm_array();
  }
  void create_sparse_array();
  void write_sparse_array();
  void rm_array();

  std::string array_name_;
};

void QueryAggregateFx::rm_array() {
  int32_t is_dir = 0;
  tiledb_vfs_is_dir(get_ctx(), vfs_temp_->vfs_c, array_name_.c_str(), &is_dir);
  if (is_dir) {
    if (tiledb_vfs_remove_dir(
            get_ctx(), vfs_temp_->vfs_c, array_name_.c_str()) != TILEDB_OK) {
      throw std::runtime_error("couldn't delete existing array " + array_name_);
    }
  }
}

// Writes simple 2d sparse array to test that query aggregate API
// basic functionality such as summing or counting works.
void QueryAggregateFx::write_sparse_array() {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_WRITE) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query) == TILEDB_OK);

  CHECK(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int32_t a[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int32_t b[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t a_size = 10 * sizeof(int32_t);
  uint64_t b_size = 10 * sizeof(int32_t);
  uint8_t b_validity[] = {1, 0, 0, 0, 0, 0, 0, 0, 0, 1};
  uint64_t b_validity_size = sizeof(b_validity);

  int64_t d1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int64_t d2[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  uint64_t d1_size = 10 * sizeof(int64_t);
  uint64_t d2_size = 10 * sizeof(int64_t);
  char c_data[] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  uint64_t c_size = strlen(c_data);
  uint64_t c_data_offsets[] = {0, 5, 8, 13, 17, 21, 26, 31, 36, 40};
  uint64_t c_offsets_size = sizeof(c_data_offsets);

  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "a", a, &a_size) == TILEDB_OK);

  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "b", b, &b_size) == TILEDB_OK);

  REQUIRE(
      tiledb_query_set_validity_buffer(
          ctx, query, "b", b_validity, &b_validity_size) == TILEDB_OK);

  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "d1", d1, &d1_size) ==
      TILEDB_OK);

  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "d2", d2, &d2_size) ==
      TILEDB_OK);

  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "c", c_data, &c_size) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_query_set_offsets_buffer(
          ctx, query, "c", c_data_offsets, &c_offsets_size) == TILEDB_OK);

  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "d", c_data, &c_size) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_query_set_offsets_buffer(
          ctx, query, "d", c_data_offsets, &c_offsets_size) == TILEDB_OK);

  CHECK(tiledb_query_submit(ctx, query) == TILEDB_OK);

  // Clean up
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void QueryAggregateFx::create_sparse_array() {
  // Create dimensions
  uint64_t tile_extents[] = {2, 2};
  uint64_t dim_domain[] = {1, 10, 1, 10};

  auto ctx = get_ctx();

  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx, "b", TILEDB_INT32, &b);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_nullable(ctx, b, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* c;
  rc = tiledb_attribute_alloc(ctx, "c", TILEDB_STRING_ASCII, &c);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, c, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* d;
  rc = tiledb_attribute_alloc(ctx, "d", TILEDB_STRING_UTF8, &d);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, d, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx, array_schema, 4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, a);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, b);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, c);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, d);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx, array_name_.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_attribute_free(&b);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: argument validation",
    "[capi][query_aggregate][args]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 9, 1, 2};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(tiledb_subarray_set_subarray(ctx, subarray, &dom) == TILEDB_OK);
  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  // nullptr context
  tiledb_query_channel_t* default_channel = nullptr;
  tiledb_channel_operation_t* operation;
  const tiledb_channel_operation_t* const_operation;
  const tiledb_channel_operator_t* ch_operator;

  CHECK(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  CHECK(
      tiledb_channel_operator_sum_get(nullptr, &ch_operator) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(
      tiledb_aggregate_count_get(nullptr, &const_operation) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(
      tiledb_query_get_default_channel(nullptr, query, &default_channel) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(
      tiledb_create_unary_aggregate(
          nullptr, query, tiledb_channel_operator_sum, "a", &operation) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(
      tiledb_channel_apply_aggregate(
          nullptr, default_channel, "Count", operation) ==
      TILEDB_INVALID_CONTEXT);

  // nullptr query
  CHECK(
      tiledb_query_get_default_channel(ctx, nullptr, &default_channel) ==
      TILEDB_ERR);
  CHECK(
      tiledb_create_unary_aggregate(
          ctx, nullptr, tiledb_channel_operator_sum, "a", &operation) ==
      TILEDB_ERR);

  // nullptr operator
  CHECK(
      tiledb_create_unary_aggregate(ctx, query, nullptr, "a", &operation) ==
      TILEDB_ERR);
  CHECK(tiledb_channel_operator_sum_get(ctx, nullptr) == TILEDB_ERR);

  // nullptr input field
  CHECK(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_sum, nullptr, &operation) ==
      TILEDB_ERR);

  // nullptr channel
  CHECK(
      tiledb_channel_apply_aggregate(ctx, nullptr, "Count", operation) ==
      TILEDB_ERR);
  CHECK(tiledb_query_channel_free(ctx, nullptr) == TILEDB_ERR);
  tiledb_query_channel_t* nullchannel = NULL;
  CHECK(tiledb_query_channel_free(ctx, &nullchannel) == TILEDB_ERR);

  // nullptr output field
  CHECK(
      tiledb_channel_apply_aggregate(
          ctx, default_channel, nullptr, operation) == TILEDB_ERR);

  // nullptr operation
  CHECK(
      tiledb_channel_apply_aggregate(ctx, default_channel, "Count", nullptr) ==
      TILEDB_ERR);
  CHECK(tiledb_aggregate_free(ctx, nullptr) == TILEDB_ERR);
  tiledb_channel_operation_t* nullop = NULL;
  CHECK(tiledb_aggregate_free(ctx, &nullop) == TILEDB_ERR);
  CHECK(tiledb_aggregate_count_get(ctx, nullptr) == TILEDB_ERR);

  // duplicate output field
  CHECK(
      tiledb_channel_apply_aggregate(
          ctx, default_channel, "duplicate", tiledb_aggregate_count) ==
      TILEDB_OK);
  CHECK(
      tiledb_channel_apply_aggregate(
          ctx, default_channel, "duplicate", tiledb_aggregate_count) ==
      TILEDB_ERR);

  // Non-existent input field
  CHECK(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_sum, "nonexistent", &operation) ==
      TILEDB_ERR);

  // Clean up
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: Query aggregates COUNT test",
    "[capi][query_aggregate][count]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 9, 1, 2};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  REQUIRE(
      tiledb_channel_apply_aggregate(
          ctx, default_channel, "Count", tiledb_aggregate_count) == TILEDB_OK);

  uint64_t count = 0;
  uint64_t size = 8;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "Count", &count, &size) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_submit(ctx, query) == TILEDB_OK);
  CHECK(count == 9);

  // Clean up
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: Query aggregates SUM test",
    "[capi][query_aggregate][sum]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 10, 1, 1};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  tiledb_channel_operation_t* sum_op;
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_sum, "a", &sum_op) == TILEDB_OK);
  REQUIRE(
      tiledb_channel_apply_aggregate(ctx, default_channel, "Sum", sum_op) ==
      TILEDB_OK);

  uint64_t sum = 0;
  uint64_t size = 8;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "Sum", &sum, &size) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_submit(ctx, query) == TILEDB_OK);
  CHECK(sum == 55);

  // Clean up
  CHECK(tiledb_aggregate_free(ctx, &sum_op) == TILEDB_OK);
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: Query aggregates MEAN test",
    "[capi][query_aggregate][mean]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 10, 1, 1};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  tiledb_channel_operation_t* mean_op;
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_mean, "a", &mean_op) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_channel_apply_aggregate(ctx, default_channel, "Mean", mean_op) ==
      TILEDB_OK);

  double mean = 0;
  uint64_t size = 8;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "Mean", &mean, &size) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_submit(ctx, query) == TILEDB_OK);

  CHECK(mean == 5.5);

  // Clean up
  CHECK(tiledb_aggregate_free(ctx, &mean_op) == TILEDB_OK);
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: Query aggregates MIN test",
    "[capi][query_aggregate][min]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 10, 1, 1};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  tiledb_channel_operation_t* min_op;
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_min, "a", &min_op) == TILEDB_OK);
  REQUIRE(
      tiledb_channel_apply_aggregate(ctx, default_channel, "Min", min_op) ==
      TILEDB_OK);

  uint64_t min = 0;
  uint64_t size = 4;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "Min", &min, &size) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_submit(ctx, query) == TILEDB_OK);

  CHECK(min == 1);

  // Clean up
  CHECK(tiledb_aggregate_free(ctx, &min_op) == TILEDB_OK);
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: Query aggregates MAX test",
    "[capi][query_aggregate][max]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 10, 1, 1};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  tiledb_channel_operation_t* max_op;
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_max, "a", &max_op) == TILEDB_OK);
  REQUIRE(
      tiledb_channel_apply_aggregate(ctx, default_channel, "Max", max_op) ==
      TILEDB_OK);

  uint64_t max = 0;
  uint64_t size = 4;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "Max", &max, &size) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_submit(ctx, query) == TILEDB_OK);

  CHECK(max == 10);

  // Clean up
  CHECK(tiledb_aggregate_free(ctx, &max_op) == TILEDB_OK);
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: Query aggregates NULL_COUNT test",
    "[capi][query_aggregate][null_count]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 10, 1, 1};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  tiledb_channel_operation_t* nullcount_op;
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_null_count, "b", &nullcount_op) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_channel_apply_aggregate(
          ctx, default_channel, "Null", nullcount_op) == TILEDB_OK);

  uint64_t nullcount = 0;
  uint64_t size = 8;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "Null", &nullcount, &size) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_submit(ctx, query) == TILEDB_OK);

  CHECK(nullcount == 8);

  // Clean up
  CHECK(tiledb_aggregate_free(ctx, &nullcount_op) == TILEDB_OK);
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: datatype checks",
    "[capi][query_aggregate][attr_type]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 9, 1, 2};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  tiledb_channel_operation_t* op;
  // SUM only works on numeric and string_ascii types
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_sum, "c", &op) == TILEDB_ERR);
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_sum, "d", &op) == TILEDB_ERR);

  // MIN,MAX only work on numeric and string_ascii types
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_min, "d", &op) == TILEDB_ERR);
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_max, "d", &op) == TILEDB_ERR);
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_min, "c", &op) == TILEDB_OK);
  CHECK(tiledb_aggregate_free(ctx, &op) == TILEDB_OK);
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_max, "c", &op) == TILEDB_OK);

  // Clean up
  CHECK(tiledb_aggregate_free(ctx, &op) == TILEDB_OK);
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: Query aggregates lifetime test",
    "[capi][query_aggregate][lifetime]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);
  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);
  int64_t dom[] = {1, 2, 1, 1};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  std::vector<int32_t> d(4);
  uint64_t size = 1;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "a", d.data(), &size) ==
      TILEDB_OK);

  // Transition the query state
  tiledb_string_handle_t* string_handle;
  REQUIRE(tiledb_query_get_plan(ctx, query, &string_handle) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);
  tiledb_channel_operation_t* op;
  CHECK(
      tiledb_channel_apply_aggregate(
          ctx, default_channel, "Count", tiledb_aggregate_count) == TILEDB_ERR);
  CHECK(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_min, "_", &op) == TILEDB_ERR);

  // Clean up
  REQUIRE(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  REQUIRE(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    QueryAggregateFx,
    "C API: Query aggregates serialization test",
    "[capi][query_aggregate][serialization][incompletes]") {
  auto ctx = get_ctx();

  tiledb_array_t* array;
  REQUIRE(tiledb_array_alloc(ctx, array_name_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);

  int64_t dom[] = {1, 10, 1, 1};
  tiledb_subarray_t* subarray;
  REQUIRE(tiledb_subarray_alloc(ctx, array, &subarray) == TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 0, &dom[0], &dom[1], nullptr) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_subarray_add_range(ctx, subarray, 1, &dom[2], &dom[3], nullptr) ==
      TILEDB_OK);

  REQUIRE(tiledb_query_set_subarray_t(ctx, query, subarray) == TILEDB_OK);

  tiledb_query_channel_t* default_channel;
  REQUIRE(
      tiledb_query_get_default_channel(ctx, query, &default_channel) ==
      TILEDB_OK);

  tiledb_channel_operation_t* sum_op;
  REQUIRE(
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_sum, "a", &sum_op) == TILEDB_OK);
  REQUIRE(
      tiledb_channel_apply_aggregate(ctx, default_channel, "Sum", sum_op) ==
      TILEDB_OK);

  uint64_t sum = 0;
  uint64_t size = 8;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "Sum", &sum, &size) ==
      TILEDB_OK);

  uint64_t a_size = 1;
  int32_t a;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "a", &a, &a_size) == TILEDB_OK);

  REQUIRE(tiledb_query_submit(ctx, query) == TILEDB_OK);

  tiledb_query_status_t status;
  REQUIRE(tiledb_query_get_status(ctx, query, &status) == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Clean up
  CHECK(tiledb_aggregate_free(ctx, &sum_op) == TILEDB_OK);
  CHECK(tiledb_query_channel_free(ctx, &default_channel) == TILEDB_OK);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}
