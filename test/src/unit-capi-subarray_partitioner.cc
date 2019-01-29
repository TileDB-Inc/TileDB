/**
 * @file   unit-capi-subarray_partitioner.cc
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
 * Tests the C API for subarray partitioner.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#include <climits>
#include <cstring>
#include <iostream>

/** Tests for C API subarray partitioner. */
struct SubarrayPartitionerFx {
  // TileDB context
  tiledb_ctx_t* ctx_;

  // Constructors/destructor
  SubarrayPartitionerFx();
  ~SubarrayPartitionerFx();

  // Functions
  void create_sparse_array_1d(
      const std::string& array_name, const uint64_t* dim_domain);
  void create_sparse_array_2d(
      const std::string& array_name,
      const uint64_t* dim_domain,
      tiledb_layout_t layout);
  template <class T>
  void write_sparse_array(
      const std::string& array_name,
      const std::vector<T>& coords,
      const std::vector<int>& a,
      const std::vector<uint64_t>& b_off,
      const std::vector<int>& b_val);
  void create_sparse_array_1d_float(
      const std::string& array_name, const float* dim_domain);
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

SubarrayPartitionerFx::SubarrayPartitionerFx() {
  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx_) == TILEDB_OK);
}

SubarrayPartitionerFx::~SubarrayPartitionerFx() {
  tiledb_ctx_free(&ctx_);
  CHECK(ctx_ == nullptr);
}

bool SubarrayPartitionerFx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY || type == TILEDB_KEY_VALUE;
}

void SubarrayPartitionerFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

void SubarrayPartitionerFx::create_sparse_array_1d(
    const std::string& array_name, const uint64_t* dim_domain) {
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
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
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

void SubarrayPartitionerFx::create_sparse_array_2d(
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

template <class T>
void SubarrayPartitionerFx::write_sparse_array(
    const std::string& array_name,
    const std::vector<T>& coords,
    const std::vector<int>& a,
    const std::vector<uint64_t>& b_off,
    const std::vector<int>& b_val) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  uint64_t coords_size = coords.size() * sizeof(T);
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

void SubarrayPartitionerFx::create_sparse_array_1d_float(
    const std::string& array_name, const float* dim_domain) {
  // Create dimensions
  tiledb_dimension_t* d;
  int rc = tiledb_dimension_alloc(
      ctx_, "d", TILEDB_FLOAT32, &dim_domain[0], nullptr, &d);
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
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
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

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, basic API usage and errors",
    "[capi], [subarray-partitioner], [subarray-partitioner-basic]") {
  std::string array_name = "subarray_partitioner_basic";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);

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

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  uint64_t budget, budget_off, budget_val;

  // Set/get result budget, fixed-sized
  rc = tiledb_subarray_partitioner_get_result_budget(
      ctx_, partitioner, "a", &budget);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, nullptr, 10);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "foo", 10);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "a", 10, 10);
  CHECK(rc == TILEDB_ERR);
  rc =
      tiledb_subarray_partitioner_set_result_budget(ctx_, partitioner, "a", 10);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_get_result_budget(
      ctx_, partitioner, nullptr, &budget);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_get_result_budget(
      ctx_, partitioner, "foo", &budget);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_get_result_budget_var(
      ctx_, partitioner, "a", &budget_off, &budget_val);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_get_result_budget(
      ctx_, partitioner, "a", &budget);
  CHECK(rc == TILEDB_OK);
  CHECK(budget == 10);
  rc = tiledb_subarray_partitioner_get_result_budget(
      ctx_, partitioner, TILEDB_COORDS, &budget);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 10);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_get_result_budget(
      ctx_, partitioner, TILEDB_COORDS, &budget);
  CHECK(rc == TILEDB_OK);
  CHECK(budget == 10);

  // Set/get result budget, var-sized
  rc = tiledb_subarray_partitioner_get_result_budget_var(
      ctx_, partitioner, "b", &budget_off, &budget_val);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, nullptr, 100, 101);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "foo", 100, 101);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "b", 100);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 100, 101);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_get_result_budget_var(
      ctx_, partitioner, nullptr, &budget_off, &budget_val);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_get_result_budget_var(
      ctx_, partitioner, "foo", &budget_off, &budget_val);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_get_result_budget(
      ctx_, partitioner, "b", &budget);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_get_result_budget_var(
      ctx_, partitioner, "b", &budget_off, &budget_val);
  CHECK(rc == TILEDB_OK);
  CHECK(budget_off == 100);
  CHECK(budget_val == 101);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, TILEDB_COORDS, 100, 101);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_partitioner_get_result_budget_var(
      ctx_, partitioner, TILEDB_COORDS, &budget_off, &budget_val);
  CHECK(rc == TILEDB_ERR);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, empty array",
    "[capi], [subarray-partitioner], [subarray-partitioner-empty-array]") {
  std::string array_name = "subarray_partitioner_empty_array";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);

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

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check current (should be empty)
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&partition);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 100);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, whole subarray fits",
    "[capi], [subarray-partitioner], "
    "[subarray-partitioner-whole-subarray-fits]") {
  // Create array
  std::string array_name = "subarray_partitioner_whole_subarray_fits";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // --- WITHOUT BUDGET ---

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 100);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // --- WITH BUDGET ---

  // Clean up
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);

  // Create partitioner
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 100000);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 100000, 100000);
  CHECK(rc == TILEDB_OK);

  // Check done
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 100);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, split once",
    "[capi], [subarray-partitioner], "
    "[subarray-partitioner-split-once]") {
  // Create array
  std::string array_name = "subarray_partitioner_split_once";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Add subarray range
  uint64_t s[] = {3, 11};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 3 * sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 100000, 100000);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 7);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Get next current and check
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 8);
  CHECK(r[1] == 11);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, unsplittable at once",
    "[capi], [subarray-partitioner], "
    "[subarray-partitioner-unsplittable-at-once]") {
  // Create array
  std::string array_name = "subarray_partitioner_unsplittable_at_once";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Add subarray range
  uint64_t s[] = {4, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 3 * sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 1, 1);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, split multiple",
    "[capi], [subarray-partitioner], "
    "[subarray-partitioner-split-multiple]") {
  // Create array
  std::string array_name = "subarray_partitioner_split_multiple";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Add subarray range
  uint64_t s[] = {2, 18};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 2 * sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 100000, 100000);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 4);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Get next current and check
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 5);
  CHECK(r[1] == 6);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Get next current and check
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 7);
  CHECK(r[1] == 10);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Get next current and check
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 11);
  CHECK(r[1] == 18);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, unsplittable after multiple",
    "[capi], [subarray-partitioner], "
    "[subarray-partitioner-unsplittable-after-multiple]") {
  // Create array
  std::string array_name = "subarray_partitioner_unsplittable_after_multiple";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Add subarray range
  uint64_t s[] = {2, 18};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 2 * sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 1, 1);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, unsplittable but ok after budget "
    "reset",
    "[capi], [subarray-partitioner], "
    "[subarray-partitioner-unsplittable-then-ok]") {
  // Create array
  std::string array_name = "subarray_partitioner_unsplittable_then_ok";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Add subarray range
  uint64_t s[] = {2, 18};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 2 * sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 1, 1);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 2);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Set budget again
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 100, 100);
  CHECK(rc == TILEDB_OK);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 3);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, float, split multiple",
    "[capi], [subarray-partitioner],[subarray-partitioner-float] "
    "[subarray-partitioner-float-split-multiple]") {
  // Create array
  std::string array_name = "subarray_partitioner_float_split_multiple";
  remove_array(array_name);
  float domain[] = {1.0f, 100.0f};
  create_sparse_array_1d_float(array_name, domain);
  std::vector<float> coords = {2.0f, 4.0f, 5.0f, 10.0f, 12.0f, 18.0f};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array<float>(array_name, coords, a, b_off, b_val);

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

  // Add subarray range
  float s[] = {2.0f, 18.0f};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 2 * sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 100000, 100000);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const float* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2.0f);
  CHECK(r[1] == 4.0f);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Get next current and check
  auto max = std::numeric_limits<float>::max();
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == std::nextafter(4.0f, max));
  CHECK(r[1] == 6.0f);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Get next current and check
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == std::nextafter(6.0f, max));
  CHECK(r[1] == 10.0f);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Get next current and check
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == std::nextafter(10.0f, max));
  CHECK(r[1] == 18.0f);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, float, unsplittable after multiple",
    "[capi], [subarray-partitioner],[subarray-partitioner-float] "
    "[subarray-partitioner-float-unsplittable-after-multiple]") {
  // Create array
  std::string array_name = "subarray_partitioner_unsplittable_after_multiple";
  remove_array(array_name);
  float domain[] = {1, 100};
  create_sparse_array_1d_float(array_name, domain);
  std::vector<float> coords = {2.0f, 4.0f, 5.0f, 10.0f, 12.0f, 18.0f};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Add subarray range
  float s[] = {2.0f, 18.0f};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 2 * sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 0, 0);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, float, whole subarray fits",
    "[capi], [subarray-partitioner][subarray-partitioner-float] "
    "[subarray-partitioner-float-whole-subarray-fits]") {
  // Create array
  std::string array_name = "subarray_partitioner_float_whole_subarray_fits";
  remove_array(array_name);
  float domain[] = {1.0f, 100.0f};
  create_sparse_array_1d_float(array_name, domain);
  std::vector<float> coords = {2.0f, 4.0f, 5.0f, 10.0f, 12.0f, 18.0f};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 100000);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 100000, 100000);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const float* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1.0f);
  CHECK(r[1] == 100.0f);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, whole subarray fits",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-whole-subarray-fits]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_whole_subarray_fits";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 100000);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_partitioner_set_result_budget_var(
      ctx_, partitioner, "b", 100000, 100000);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r1;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  const uint64_t* r2;
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 1);
  CHECK(r2[1] == 10);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, row-major, split once",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-row-split-once]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_split_once";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 2 * 2 * sizeof(uint64_t));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r1;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 2);
  const uint64_t* r2;
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 1);
  CHECK(r2[1] == 10);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, col-major, split once",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-col-split-once]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_split_once";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 4 * 2 * sizeof(uint64_t));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r1;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  const uint64_t* r2;
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 1);
  CHECK(r2[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, row-major, split multiple",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-row-split-multiple]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_split_multiple";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t s[] = {3, 4, 1, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s[2]);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 2 * sizeof(uint64_t));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r1;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 3);
  CHECK(r1[1] == 3);
  const uint64_t* r2;
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 1);
  CHECK(r2[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 3);
  CHECK(r1[1] == 3);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 6);
  CHECK(r2[1] == 8);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 3);
  CHECK(r1[1] == 3);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 9);
  CHECK(r2[1] == 10);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 4);
  CHECK(r1[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 1);
  CHECK(r2[1] == 3);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 4);
  CHECK(r1[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 4);
  CHECK(r2[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 4);
  CHECK(r1[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 6);
  CHECK(r2[1] == 10);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, col-major, split multiple",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-col-split-multiple]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_col_split_multiple";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t s[] = {1, 10, 1, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s[2]);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 2 * sizeof(uint64_t));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r1;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  const uint64_t* r2;
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 1);
  CHECK(r2[1] == 1);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 2);
  CHECK(r2[1] == 2);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 3);
  CHECK(r2[1] == 3);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 4);
  CHECK(r2[1] == 4);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 5);
  CHECK(r2[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 6);
  CHECK(r2[1] == 7);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 8);
  CHECK(r2[1] == 8);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r1);
  CHECK(rc == TILEDB_OK);
  CHECK(r1[0] == 1);
  CHECK(r1[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r2);
  CHECK(rc == TILEDB_OK);
  CHECK(r2[0] == 9);
  CHECK(r2[1] == 10);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, row-major, unsplittable",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-row-unsplittable]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_unsplittable";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t s[] = {1, 10, 2, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s[2]);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 0);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, col-major, unsplittable",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-col-unsplittable]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_col_unsplittable";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t s[] = {1, 10, 1, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s[2]);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 0);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, multiple ranges, fits",
    "[capi], [subarray-partitioner][subarray-partitioner-1d] "
    "[subarray-partitioner-1d-multiple][subarray-partitioner-1d-multiple-"
    "fit]") {
  // Create array
  std::string array_name = "subarray_partitioner_1d_multiple_fit";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18, 25, 27, 33, 40};
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int),
                                 15 * sizeof(int),
                                 16 * sizeof(int),
                                 17 * sizeof(int),
                                 18 * sizeof(int)};
  std::vector<int> b_val = {
      1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6, 7, 8, 9, 10};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r1[] = {5, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r1);
  CHECK(rc == TILEDB_OK);
  uint64_t r2[] = {25, 27};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r2);
  CHECK(rc == TILEDB_OK);
  uint64_t r3[] = {33, 50};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r3);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 100 * sizeof(int));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 3);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 5);
  CHECK(r[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 25);
  CHECK(r[1] == 27);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 2, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 33);
  CHECK(r[1] == 50);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, multiple ranges, split once",
    "[capi], [subarray-partitioner][subarray-partitioner-1d] "
    "[subarray-partitioner-1d-multiple]"
    "[subarray-partitioner-1d-multiple-split-multiple]") {
  // Create array
  std::string array_name = "subarray_partitioner_1d_multiple_split_once";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18, 25, 27, 33, 40};
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int),
                                 15 * sizeof(int),
                                 16 * sizeof(int),
                                 17 * sizeof(int),
                                 18 * sizeof(int)};
  std::vector<int> b_val = {
      1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6, 7, 8, 9, 10};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r1[] = {5, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r1);
  CHECK(rc == TILEDB_OK);
  uint64_t r2[] = {25, 27};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r2);
  CHECK(rc == TILEDB_OK);
  uint64_t r3[] = {33, 50};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r3);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 4 * sizeof(int));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 5);
  CHECK(r[1] == 10);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 25);
  CHECK(r[1] == 27);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 33);
  CHECK(r[1] == 50);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, multiple ranges, split multiple",
    "[capi], [subarray-partitioner][subarray-partitioner-1d] "
    "[subarray-partitioner-1d-multiple]"
    "[subarray-partitioner-1d-multiple-split-multiple]") {
  // Create array
  std::string array_name = "subarray_partitioner_1d_multiple_split_multiple";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18, 25, 27, 33, 40};
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int),
                                 15 * sizeof(int),
                                 16 * sizeof(int),
                                 17 * sizeof(int),
                                 18 * sizeof(int)};
  std::vector<int> b_val = {
      1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6, 7, 8, 9, 10};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r1[] = {5, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r1);
  CHECK(rc == TILEDB_OK);
  uint64_t r2[] = {25, 27};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r2);
  CHECK(rc == TILEDB_OK);
  uint64_t r3[] = {33, 50};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r3);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 2 * sizeof(int));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 5);
  CHECK(r[1] == 10);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 25);
  CHECK(r[1] == 27);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 33);
  CHECK(r[1] == 50);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, multiple ranges, split mixed",
    "[capi], [subarray-partitioner][subarray-partitioner-1d] "
    "[subarray-partitioner-1d-multiple]"
    "[subarray-partitioner-1d-multiple-split-mixed]") {
  // Create array
  std::string array_name = "subarray_partitioner_1d_multiple_split_mixed";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18, 25, 27, 33, 40};
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int),
                                 15 * sizeof(int),
                                 16 * sizeof(int),
                                 17 * sizeof(int),
                                 18 * sizeof(int)};
  std::vector<int> b_val = {
      1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6, 7, 8, 9, 10};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r1[] = {5, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r1);
  CHECK(rc == TILEDB_OK);
  uint64_t r2[] = {25, 27};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r2);
  CHECK(rc == TILEDB_OK);
  uint64_t r3[] = {33, 40};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r3);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 2 * sizeof(int) - 1);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 5);
  CHECK(r[1] == 7);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 8);
  CHECK(r[1] == 10);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 25);
  CHECK(r[1] == 26);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 27);
  CHECK(r[1] == 27);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 33);
  CHECK(r[1] == 36);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 37);
  CHECK(r[1] == 40);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 1D, multiple ranges, unsplittable",
    "[capi], [subarray-partitioner][subarray-partitioner-1d] "
    "[subarray-partitioner-1d-multiple]"
    "[subarray-partitioner-1d-multiple-unsplittable]") {
  // Create array
  std::string array_name = "subarray_partitioner_1d_multiple_unsplittable";
  remove_array(array_name);
  uint64_t domain[] = {1, 100};
  create_sparse_array_1d(array_name, domain);
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18, 25, 27, 33, 40};
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int),
                                 15 * sizeof(int),
                                 16 * sizeof(int),
                                 17 * sizeof(int),
                                 18 * sizeof(int)};
  std::vector<int> b_val = {
      1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6, 7, 8, 9, 10};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r1[] = {5, 10};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r1);
  CHECK(rc == TILEDB_OK);
  uint64_t r2[] = {25, 27};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r2);
  CHECK(rc == TILEDB_OK);
  uint64_t r3[] = {33, 40};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r3);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(ctx_, partitioner, "a", 1);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Check done
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, row, multiple ranges, fits",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-row]"
    "[subarray-partitioner-2d-multiple-fits]"
    "[subarray-partitioner-2d-multiple-fits-row]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_multiple_fits";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r13[] = {4, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r13);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {2, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {4, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 100000);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 3);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 3);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 2, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 4);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 3);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 4);
  CHECK(r[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, row, multiple ranges, split once",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-row]"
    "[subarray-partitioner-2d-multiple-split-once]"
    "[subarray-partitioner-2d-multiple-split-once-row]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_mulitple_split_once";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r13[] = {4, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r13);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {2, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {6, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 4 * sizeof(int));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 3);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 5);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 6);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 4);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 5);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 6);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, row, multiple ranges, calibrate",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-row]"
    "[subarray-partitioner-2d-multiple-calibrate]"
    "[subarray-partitioner-2d-multiple-calibrate-row]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_multiple_calibrate";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r13[] = {4, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r13);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {2, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {6, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 5 * sizeof(int));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 3);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 5);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 6);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 4);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 5);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 6);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, row, multiple ranges, unsplittable",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-row]"
    "[subarray-partitioner-2d-multiple-unsplittable]"
    "[subarray-partitioner-2d-multiple-unsplittable-row]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_multiple_unsplittable";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r13[] = {4, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r13);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {2, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {6, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 1);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, row, multiple ranges, mixed",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-row]"
    "[subarray-partitioner-2d-multiple-mixed]"
    "[subarray-partitioner-2d-multiple-mixed-row]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_row_multiple_mixed";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r13[] = {4, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r13);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {2, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {6, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 2 * sizeof(uint64_t));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Increase budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 1000000);
  CHECK(rc == TILEDB_OK);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 6);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 3);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 4);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 5);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 6);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, col, multiple ranges, fits",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-col]"
    "[subarray-partitioner-2d-multiple-fits]"
    "[subarray-partitioner-2d-multiple-fits-col]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_col_multiple_fits";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {3, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);
  uint64_t r23[] = {7, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r23);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 100000);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 3);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 5);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 2, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 7);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, col, multiple ranges, split once",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-col]"
    "[subarray-partitioner-2d-multiple-split-once]"
    "[subarray-partitioner-2d-multiple-split-once-col]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_col_mulitple_split_once";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {3, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);
  uint64_t r23[] = {7, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r23);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 4 * sizeof(int));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 7);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, col, multiple ranges, unsplittable",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-col]"
    "[subarray-partitioner-2d-multiple-unsplittable]"
    "[subarray-partitioner-2d-multiple-unsplittable-col]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_col_multiple_unsplittable";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {3, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);
  uint64_t r23[] = {7, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r23);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 1);
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, col, multiple ranges, calibrate",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-col]"
    "[subarray-partitioner-2d-multiple-calibrate]"
    "[subarray-partitioner-2d-multiple-calibrate-col]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_col_multiple_calibrate";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {3, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);
  uint64_t r23[] = {7, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r23);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, "a", 3 * sizeof(int));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 5);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 7);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_free(&partition);
  CHECK(partition == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SubarrayPartitionerFx,
    "C API: Test subarray partitioner, 2D, col, multiple ranges, mixed",
    "[capi], [subarray-partitioner][subarray-partitioner-2d] "
    "[subarray-partitioner-2d-multiple]"
    "[subarray-partitioner-2d-multiple-col]"
    "[subarray-partitioner-2d-multiple-mixed]"
    "[subarray-partitioner-2d-multiple-mixed-col]") {
  // Create array
  std::string array_name = "subarray_partitioner_2d_col_multiple_mixed";
  remove_array(array_name);
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {0,
                                 sizeof(int),
                                 3 * sizeof(int),
                                 6 * sizeof(int),
                                 9 * sizeof(int),
                                 11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  write_sparse_array(array_name, coords, a, b_off, b_val);

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
  uint64_t r11[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r11);
  CHECK(rc == TILEDB_OK);
  uint64_t r12[] = {3, 4};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, r12);
  CHECK(rc == TILEDB_OK);
  uint64_t r21[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r21);
  CHECK(rc == TILEDB_OK);
  uint64_t r22[] = {3, 5};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r22);
  CHECK(rc == TILEDB_OK);
  uint64_t r23[] = {7, 9};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, r23);
  CHECK(rc == TILEDB_OK);

  // Create partitioner
  tiledb_subarray_partitioner_t* partitioner;
  rc = tiledb_subarray_partitioner_alloc(ctx_, subarray, &partitioner);
  CHECK(rc == TILEDB_OK);

  // Set budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 1 * sizeof(uint64_t));
  CHECK(rc == TILEDB_OK);

  // Check done
  int done;
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  int unsplittable;
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  tiledb_subarray_t* partition = nullptr;
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  const uint64_t* r;
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 1);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 2);
  CHECK(r[1] == 2);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Increase budget
  rc = tiledb_subarray_partitioner_set_result_budget(
      ctx_, partitioner, TILEDB_COORDS, 1000000);
  CHECK(rc == TILEDB_OK);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 0);

  // Check next
  rc = tiledb_subarray_partitioner_next(ctx_, partitioner, &unsplittable);
  CHECK(rc == TILEDB_OK);
  CHECK(unsplittable == 0);

  // Get current and check
  rc = tiledb_subarray_partitioner_get_current(ctx_, partitioner, &partition);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, partition, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 1);
  CHECK(r[1] == 2);
  rc = tiledb_subarray_get_range(ctx_, partition, 0, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 4);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 0, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 3);
  CHECK(r[1] == 5);
  rc = tiledb_subarray_get_range(ctx_, partition, 1, 1, (const void**)&r);
  CHECK(rc == TILEDB_OK);
  CHECK(r[0] == 7);
  CHECK(r[1] == 9);

  // Check done again
  rc = tiledb_subarray_partitioner_done(ctx_, partitioner, &done);
  CHECK(rc == TILEDB_OK);
  CHECK(done == 1);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_subarray_partitioner_free(&partitioner);
  CHECK(partitioner == nullptr);

  remove_array(array_name);
}
