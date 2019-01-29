/**
 * @file   unit-capi-subarray.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests the C API for subarray.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#include <climits>
#include <cstring>
#include <iostream>

const uint64_t DIM_DOMAIN[4] = {1, 10, 1, 10};

/** Tests for C API subarray. */
struct SubarrayFx {
  // TileDB context
  tiledb_ctx_t* ctx_;

  // Constructors/destructor
  SubarrayFx();
  ~SubarrayFx();

  // Functions
  void create_dense_array(const std::string& array_name);
  void create_sparse_array(
      const std::string& array_name, const uint64_t* dim_domain = DIM_DOMAIN);
  void create_sparse_array_1d(
      const std::string& array_name,
      const uint64_t* dim_domain = DIM_DOMAIN,
      tiledb_layout_t layout = TILEDB_ROW_MAJOR);
  void create_sparse_array_2d(
      const std::string& array_name,
      const uint64_t* dim_domain = DIM_DOMAIN,
      tiledb_layout_t layout = TILEDB_ROW_MAJOR);
  void create_sparse_array_real(const std::string& array_name);
  void write_sparse_array(
      const std::string& array_name,
      const std::vector<uint64_t>& coords,
      const std::vector<int>& a,
      const std::vector<uint64_t>& b_off,
      const std::vector<int>& b_val);
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

SubarrayFx::SubarrayFx() {
  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx_) == TILEDB_OK);
}

SubarrayFx::~SubarrayFx() {
  tiledb_ctx_free(&ctx_);
  CHECK(ctx_ == nullptr);
}

bool SubarrayFx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY || type == TILEDB_KEY_VALUE;
}

void SubarrayFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

void SubarrayFx::create_dense_array(const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 10, 1, 10};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a, 1);
  CHECK(rc == TILEDB_OK);

  // Create array schmea
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void SubarrayFx::create_sparse_array(
    const std::string& array_name, const uint64_t* dim_domain) {
  // Create dimensions
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx_, "b", TILEDB_INT32, &b);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, b, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, b, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);

  // Create array schmea
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, b);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_attribute_free(&b);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void SubarrayFx::create_sparse_array_1d(
    const std::string& array_name,
    const uint64_t* dim_domain,
    tiledb_layout_t layout) {
  // Create dimensions
  uint64_t tile_extents[] = {10};
  tiledb_dimension_t* d;
  int rc = tiledb_dimension_alloc(
      ctx_, "d", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx_, "b", TILEDB_INT32, &b);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, b, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, b, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);

  // Create array schmea
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, layout);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, b);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_attribute_free(&b);
  tiledb_dimension_free(&d);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void SubarrayFx::create_sparse_array_2d(
    const std::string& array_name,
    const uint64_t* dim_domain,
    tiledb_layout_t layout) {
  // Create dimensions
  uint64_t tile_extents[] = {10, 10};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx_, "b", TILEDB_INT32, &b);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, b, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, b, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);

  // Create array schmea
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, layout);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, b);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_attribute_free(&b);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void SubarrayFx::create_sparse_array_real(const std::string& array_name) {
  // Create dimensions
  double dim_domain[] = {1, 10, 1, 10};
  double tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_FLOAT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_FLOAT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a, 1);
  CHECK(rc == TILEDB_OK);

  // Create array schmea
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void SubarrayFx::write_sparse_array(
    const std::string& array_name,
    const std::vector<uint64_t>& coords,
    const std::vector<int>& a,
    const std::vector<uint64_t>& b_off,
    const std::vector<int>& b_val) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  uint64_t coords_size = coords.size() * sizeof(uint64_t);
  uint64_t a_size = a.size() * sizeof(int);
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  uint64_t b_val_size = b_val.size() * sizeof(int);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", (void*)a.data(), &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "b",
      (uint64_t*)b_off.data(),
      &b_off_size,
      (void*)b_val.data(),
      &b_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, (void*)coords.data(), &coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, basic API usage and errors",
    "[capi], [subarray], [subarray-basic]") {
  std::string array_name = "subarray_basic";
  remove_array(array_name);
  create_sparse_array(array_name);

  // Allocate subarray with an unopened array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
  CHECK(rc == TILEDB_ERR);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Allocate subarray with an opened array
  rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
  CHECK(rc == TILEDB_OK);

  // Get layout
  tiledb_layout_t layout;
  rc = tiledb_subarray_get_layout(ctx_, subarray, &layout);
  CHECK(rc == TILEDB_OK);
  CHECK(layout == TILEDB_UNORDERED);

  // Get type and ndim
  tiledb_datatype_t type;
  uint32_t dim_num;
  rc = tiledb_subarray_get_type(ctx_, subarray, &type);
  CHECK(rc == TILEDB_OK);
  CHECK(type == TILEDB_UINT64);
  rc = tiledb_subarray_get_ndim(ctx_, subarray, &dim_num);
  CHECK(rc == TILEDB_OK);
  CHECK(dim_num == 2);

  // Get domain
  const void* dom;
  uint64_t domain[] = {1, 10, 1, 10};
  rc = tiledb_subarray_get_domain(ctx_, subarray, &dom);
  CHECK(rc == TILEDB_OK);
  CHECK(std::memcmp(dom, domain, sizeof(domain)) == 0);

  // Check getting range num from an invalid dimension index
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 2, &range_num);
  CHECK(rc == TILEDB_ERR);

  // Check getting range from an invalid dimension index
  const void* range;
  rc = tiledb_subarray_get_range(ctx_, subarray, 2, 0, &range);
  CHECK(rc == TILEDB_ERR);

  // Check getting range from an invalid range index
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 1, &range);
  CHECK(rc == TILEDB_ERR);

  // Add null range
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, NULL);
  CHECK(rc == TILEDB_ERR);

  // Add ranges outside the subarray domain
  uint64_t inv_r1[] = {0, 0};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &inv_r1[0]);
  CHECK(rc == TILEDB_ERR);
  uint64_t inv_r2[] = {0, 20};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &inv_r2[0]);
  CHECK(rc == TILEDB_ERR);
  uint64_t inv_r3[] = {11, 20};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &inv_r3[0]);
  CHECK(rc == TILEDB_ERR);

  // Add range with invalid end points
  uint64_t inv_r4[] = {5, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &inv_r4[0]);
  CHECK(rc == TILEDB_ERR);

  // Add valid ranges
  uint64_t r1[] = {1, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
  CHECK(rc == TILEDB_OK);
  uint64_t r2[] = {2, 8};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
  CHECK(rc == TILEDB_OK);
  uint64_t r3[] = {2, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r3[0]);
  CHECK(rc == TILEDB_OK);

  // Check range num
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);

  // Check getting range from an invalid range index
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 2, &range);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_get_range(ctx_, subarray, 1, 1, &range);
  CHECK(rc == TILEDB_ERR);

  // Check ranges
  const uint64_t *r00, *r01, *r10;
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 0, (const void**)&r00);
  CHECK(rc == TILEDB_OK);
  CHECK(r00[0] == 1);
  CHECK(r00[1] == 3);
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 1, (const void**)&r01);
  CHECK(rc == TILEDB_OK);
  CHECK(r01[0] == 2);
  CHECK(r01[1] == 8);
  rc = tiledb_subarray_get_range(ctx_, subarray, 1, 0, (const void**)&r10);
  CHECK(rc == TILEDB_OK);
  CHECK(r10[0] == 2);
  CHECK(r10[1] == 2);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, check default (empty) subarray",
    "[capi], [subarray], [subarray-default]") {
  std::string array_name = "subarray_default";
  remove_array(array_name);
  create_sparse_array(array_name);

  // Allocate subarray with an unopened array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Allocate subarray with an opened array
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
  CHECK(rc == TILEDB_OK);

  // Check that there are no ranges initially
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);

  // Check ranges
  const uint64_t *r1, *r2;
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, subarray, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 1);
  CHECK(r2[1] == 10);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, check creating subarray for a dense array",
    "[capi][subarray][subarray-errors][subarray-dense]") {
  std::string array_name = "subarray_dense";
  remove_array(array_name);
  create_dense_array(array_name);

  // Create subarray
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
  CHECK(rc == TILEDB_ERR);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, check NaN ranges",
    "[capi][subarray][subarray-errors][subarray-nan]") {
  std::string array_name = "subarray_nan";
  remove_array(array_name);
  create_sparse_array_real(array_name);

  // Create subarray
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
  CHECK(rc == TILEDB_OK);

  // Add range with NaN
  double range[] = {std::numeric_limits<double>::quiet_NaN(), 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, range);
  CHECK(rc == TILEDB_ERR);
  double range2[] = {1.3, 4.2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, range2);
  CHECK(rc == TILEDB_OK);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, check errors in setting the subarray to query",
    "[capi][subarray][subarray-errors][subarray-errors-query]") {
  std::string array_name = "subarray_query_set";
  std::string array_name_inv = "subarray_query_inv";
  remove_array(array_name);
  remove_array(array_name_inv);
  create_sparse_array(array_name);
  uint64_t dom[] = {1, 2, 1, 3};
  create_sparse_array(array_name_inv, dom);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Open invalid array
  tiledb_array_t* array_inv;
  rc = tiledb_array_alloc(ctx_, array_name_inv.c_str(), &array_inv);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array_inv, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create subarray for invalid array
  tiledb_subarray_t* subarray_inv = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array_inv, TILEDB_UNORDERED, &subarray_inv);
  CHECK(rc == TILEDB_OK);

  // Create query for correct array
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set invalid subarray to correct array
  rc = tiledb_query_set_subarray_2(ctx_, query, subarray_inv);
  CHECK(rc == TILEDB_ERR);

  // Create subarray for correct array
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
  CHECK(rc == TILEDB_OK);

  // Set correct subarray to correct array
  rc = tiledb_query_set_subarray_2(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  rc = tiledb_array_close(ctx_, array_inv);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array_inv);
  CHECK(array_inv == nullptr);
  tiledb_subarray_free(&subarray_inv);
  CHECK(subarray_inv == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);

  remove_array(array_name);
  remove_array(array_name_inv);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, check setters/getters of memory budget",
    "[capi][subarray][subarray-budget]") {
  std::string array_name = "subarray_budget";
  remove_array(array_name);
  create_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
  CHECK(rc == TILEDB_OK);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, sparse result estimation, empty tree",
    "[capi][subarray][subarray-result-estimation]"
    "[subarray-result-estimation-0]") {
  std::string array_name = "subarray_result_estimation_0";
  remove_array(array_name);
  create_sparse_array_1d(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
  CHECK(rc == TILEDB_OK);

  uint64_t size, size_off, size_val;

  // Simple checks
  rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "b", &size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_get_est_result_size_var(
      ctx_, subarray, "a", &size_off, &size_val);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "foo", &size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_get_est_result_size_var(
      ctx_, subarray, "foo", &size_off, &size_val);
  CHECK(rc == TILEDB_ERR);

  // Get estimated result size
  rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 0);
  rc = tiledb_subarray_get_est_result_size_var(
      ctx_, subarray, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 0);
  CHECK(size_val == 0);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, 1D, sparse result estimation, height 2",
    "[capi][subarray][subarray-result-estimation-1d]"
    "[subarray-result-estimation-1d-2]") {
  std::string array_name = "subarray_result_estimation_1d_2";
  remove_array(array_name);

  uint64_t domain[] = {1, 100};
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  tiledb_array_t* array = nullptr;
  tiledb_subarray_t* subarray = nullptr;
  uint64_t size, size_off, size_val;

  SECTION("- row-major") {
    // Create array
    create_sparse_array_1d(array_name, domain, TILEDB_ROW_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 20};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 15 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {20, 30};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 2 ranges") {
      uint64_t r1[] = {1, 1};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {20, 30};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Partial overlap, 1 range") {
      uint64_t r[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t))));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(int))));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(
          size_off == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t))));
      CHECK(
          size_val ==
          (uint64_t)(
              (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (6 * sizeof(int))));
    }

    SECTION("-- Partial overlap, 2 ranges") {
      uint64_t r1[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {10, 12};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      uint64_t coords_size = (uint64_t)ceil(
          (2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size =
          (uint64_t)ceil((2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size = (uint64_t)ceil(
          (2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (3.0 / 6) * (6 * sizeof(int)) +
          (1.0 / 7) * (6 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }
  }

  SECTION("- col-major") {
    // Create array
    create_sparse_array_1d(array_name, domain, TILEDB_COL_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 20};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 15 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {20, 30};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 2 ranges") {
      uint64_t r1[] = {1, 1};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {20, 30};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Partial overlap, 1 range") {
      uint64_t r[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t))));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(int))));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(
          size_off == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t))));
      CHECK(
          size_val ==
          (uint64_t)(
              (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (6 * sizeof(int))));
    }

    SECTION("-- Partial overlap, 2 ranges") {
      uint64_t r1[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {10, 12};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      uint64_t coords_size = (uint64_t)ceil(
          (2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size =
          (uint64_t)ceil((2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size = (uint64_t)ceil(
          (2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (3.0 / 6) * (6 * sizeof(int)) +
          (1.0 / 7) * (6 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }
  }

  // Clean-up
  int rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, 1D, sparse result estimation, height 3",
    "[capi][subarray][subarray-result-estimation-1d]"
    "[subarray-result-estimation-1d-3]") {
  std::string array_name = "subarray_result_estimation_1d_3";
  remove_array(array_name);

  uint64_t domain[] = {1, 100};
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18, 20, 23, 24, 27};
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 5 * sizeof(int),
                                 7 * sizeof(int),
                                 10 * sizeof(int),
                                 14 * sizeof(int),
                                 15 * sizeof(int),
                                 16 * sizeof(int),
                                 18 * sizeof(int)};
  std::vector<int> b_val = {
      1, 2, 2, 3, 3, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 8, 9, 9, 10};
  tiledb_array_t* array = nullptr;
  tiledb_subarray_t* subarray = nullptr;
  uint64_t size, size_off, size_val;

  SECTION("- row-major") {
    // Create array
    create_sparse_array_1d(array_name, domain, TILEDB_ROW_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 27};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 10 * sizeof(uint64_t));
      CHECK(size_val == 19 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {30, 40};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 2 ranges") {
      uint64_t r1[] = {1, 1};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {30, 40};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Overlap only tiles, 1 range") {
      uint64_t r[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      uint64_t coords_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size = (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (4 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Overlap only tiles, 2 ranges") {
      uint64_t r1[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {23, 24};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      uint64_t coords_size = (uint64_t)ceil(
          (2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size = (uint64_t)ceil(
          (2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (4 * sizeof(int)) +
          (1.0 / 4) * (2 * sizeof(int)) + (1.0 / 4) * (3 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Overlap only tile ranges, 1 range") {
      uint64_t r[] = {2, 18};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 14 * sizeof(int));
    }

    SECTION("-- Overlap only tile ranges, 2 ranges") {
      uint64_t r1[] = {2, 18};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {19, 28};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 10 * sizeof(uint64_t));
      CHECK(size_val == 19 * sizeof(int));
    }

    SECTION("-- Overlap tiles and tile ranges") {
      uint64_t r[] = {2, 20};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (6 + (1.0 / 4) * 2) * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (6 + (1.0 / 4) * 2) * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == (6 + (1.0 / 4) * 2) * sizeof(uint64_t));
      CHECK(size_val == 14 * sizeof(int) + (1.0 / 4) * 2 * sizeof(int));
    }
  }

  SECTION("- col-major") {
    // Create array
    create_sparse_array_1d(array_name, domain, TILEDB_COL_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 27};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 10 * sizeof(uint64_t));
      CHECK(size_val == 19 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {30, 40};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 2 ranges") {
      uint64_t r1[] = {1, 1};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {30, 40};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Overlap only tiles, 1 range") {
      uint64_t r[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      uint64_t coords_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size = (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (4 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Overlap only tiles, 2 ranges") {
      uint64_t r1[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {23, 24};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      uint64_t coords_size = (uint64_t)ceil(
          (2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size = (uint64_t)ceil(
          (2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (4 * sizeof(int)) +
          (1.0 / 4) * (2 * sizeof(int)) + (1.0 / 4) * (3 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Overlap only tile ranges, 1 range") {
      uint64_t r[] = {2, 18};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 14 * sizeof(int));
    }

    SECTION("-- Overlap only tile ranges, 2 ranges") {
      uint64_t r1[] = {2, 18};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {19, 28};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 10 * sizeof(uint64_t));
      CHECK(size_val == 19 * sizeof(int));
    }

    SECTION("-- Overlap tiles and tile ranges") {
      uint64_t r[] = {2, 20};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (6 + (1.0 / 4) * 2) * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (6 + (1.0 / 4) * 2) * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == (6 + (1.0 / 4) * 2) * sizeof(uint64_t));
      CHECK(size_val == 14 * sizeof(int) + (1.0 / 4) * 2 * sizeof(int));
    }
  }

  // Clean-up
  int rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayFx,
    "C API: Test subarray, 2D, sparse result estimation, height 2",
    "[capi][subarray][subarray-result-estimation-2d]"
    "[subarray-result-estimation-2d-2]") {
  std::string array_name = "subarray_result_estimation_2d_2";
  remove_array(array_name);

  uint64_t domain[] = {1, 10, 1, 10};
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  tiledb_array_t* array = nullptr;
  tiledb_subarray_t* subarray = nullptr;
  uint64_t size, size_off, size_val;

  SECTION("- row-major") {
    // Create array
    create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 10, 1, 10};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * 2 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 15 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {1, 2, 7, 8};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 4 ranges") {
      uint64_t r11[] = {1, 2};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r11[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r12[] = {5, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r12[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r21[] = {6, 7};
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r21[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r22[] = {9, 10};
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r22[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Partial overlap, 1 range") {
      uint64_t r[] = {2, 3, 5, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      uint64_t coords_size = (uint64_t)ceil(
          (1.0 / 2) * (1.0 / 4) * 4 * sizeof(uint64_t) +
          1.0 * (2.0 / 7) * 4 * sizeof(uint64_t));
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size = (uint64_t)ceil(
          (1.0 / 2) * (1.0 / 4) * 2 * sizeof(int) +
          1.0 * (2.0 / 7) * 2 * sizeof(int));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size = (uint64_t)ceil(
          (1.0 / 2) * (1.0 / 4) * 2 * sizeof(uint64_t) +
          1.0 * (2.0 / 7) * 2 * sizeof(uint64_t));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (1.0 / 2) * (1.0 / 4) * 3 * sizeof(int) +
          1.0 * (2.0 / 7) * 6 * sizeof(int));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Partial overlap, 4 ranges") {
      uint64_t r11[] = {1, 2};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r11[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r12[] = {4, 4};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r12[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r21[] = {1, 2};
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r21[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r22[] = {7, 8};
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r22[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      uint64_t coords_size = (uint64_t)ceil(
          (1.0 / 4) * 4 * sizeof(uint64_t) + (3.0 / 7) * 4 * sizeof(uint64_t));
      CHECK(rc == TILEDB_OK);
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size = (uint64_t)ceil(
          (1.0 / 4) * 2 * sizeof(int) + (3.0 / 7) * 2 * sizeof(int));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size = (uint64_t)ceil(
          (1.0 / 4) * 2 * sizeof(uint64_t) + (3.0 / 7) * 2 * sizeof(uint64_t));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (1.0 / 4) * 3 * sizeof(int) + (3.0 / 7) * 6 * sizeof(int));
      CHECK(size_val == b_val_size);
    }
  }

  SECTION("- col-major") {
    // Create array
    create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, TILEDB_UNORDERED, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 10, 1, 10};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * 2 * sizeof(uint64_t));
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 15 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {1, 2, 7, 8};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 4 ranges") {
      uint64_t r11[] = {1, 2};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r11[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r12[] = {5, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r12[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r21[] = {6, 7};
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r21[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r22[] = {9, 10};
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r22[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Partial overlap, 1 range") {
      uint64_t r[] = {2, 3, 5, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      uint64_t coords_size =
          (uint64_t)ceil(1.0 * (1.0 / 3) * 4 * sizeof(uint64_t));
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size = (uint64_t)ceil(1.0 * (1.0 / 3) * 2 * sizeof(int));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size =
          (uint64_t)ceil(1.0 * (1.0 / 3) * 2 * sizeof(uint64_t));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(1.0 * (1.0 / 3) * 5 * sizeof(int));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Partial overlap, 4 ranges") {
      uint64_t r11[] = {1, 2};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r11[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r12[] = {4, 4};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r12[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r21[] = {1, 2};
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r21[0]);
      CHECK(rc == TILEDB_OK);
      uint64_t r22[] = {7, 8};
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r22[0]);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_get_est_result_size(
          ctx_, subarray, TILEDB_COORDS, &size);
      uint64_t coords_size = (uint64_t)ceil(
          (6.0 / 8) * 4 * sizeof(uint64_t) + (2.0 / 6) * 4 * sizeof(uint64_t));
      CHECK(rc == TILEDB_OK);
      CHECK(size == coords_size);
      rc = tiledb_subarray_get_est_result_size(ctx_, subarray, "a", &size);
      CHECK(rc == TILEDB_OK);
      uint64_t a_size = (uint64_t)ceil(
          (6.0 / 8) * 2 * sizeof(int) + (2.0 / 6) * 2 * sizeof(int));
      CHECK(size == a_size);
      rc = tiledb_subarray_get_est_result_size_var(
          ctx_, subarray, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      uint64_t b_off_size = (uint64_t)ceil(
          (6.0 / 8) * 2 * sizeof(uint64_t) + (2.0 / 6) * 2 * sizeof(uint64_t));
      CHECK(size_off == b_off_size);
      uint64_t b_val_size = (uint64_t)ceil(
          (6.0 / 8) * 3 * sizeof(int) + (2.0 / 6) * 7 * sizeof(int));
      CHECK(size_val == b_val_size);
    }
  }

  // Clean-up
  int rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}
