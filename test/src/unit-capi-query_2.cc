/**
 * @file   unit-capi-query_2.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests the C API for subarray internal to a query.
 *
 * If changes made here, unit-capi-subarray_2.cc cloned from here should
 * be checked for need of possibly similar changes.
 */

#include <catch2/catch_test_macros.hpp>
#include "test/support/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"

#include <climits>
#include <cmath>
#include <cstring>
#include <iostream>

using namespace tiledb::test;
using std::ceil;

const uint64_t DIM_DOMAIN[4] = {1, 10, 1, 10};

/** Tests for C API subarray. */
struct Query2Fx {
  // TileDB context
  tiledb_ctx_t* ctx_;

  bool serialize_ = false;

  // Constructors/destructor
  Query2Fx();
  ~Query2Fx();

  // Functions
  void create_dense_array(const std::string& array_name, bool anon = false);
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
  void write_dense_array(
      const std::string& array_name,
      const std::vector<uint64_t>& domain,
      const std::vector<int>& a,
      const std::vector<uint64_t>& b_off,
      const std::vector<int>& b_val,
      bool anon = false);
  void write_sparse_array(
      const std::string& array_name,
      const std::vector<uint64_t>& coords,
      const std::vector<int>& a,
      const std::vector<uint64_t>& b_off,
      const std::vector<int>& b_val);
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
  int32_t tiledb_query_get_est_result_size_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      const char* name,
      uint64_t* size);
  int32_t tiledb_query_get_est_result_size_var_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val);
};

Query2Fx::Query2Fx() {
  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx_) == TILEDB_OK);
}

Query2Fx::~Query2Fx() {
  tiledb_ctx_free(&ctx_);
  CHECK(ctx_ == nullptr);
}

bool Query2Fx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

void Query2Fx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

int32_t Query2Fx::tiledb_query_get_est_result_size_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* size) {
  int ret = tiledb_query_get_est_result_size(ctx, query, name, size);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_query_est_result_sizes(
          ctx,
          query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_query_est_result_sizes(
          ctx,
          query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          buff) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return tiledb_query_get_est_result_size(ctx, query, name, size);
}

int32_t Query2Fx::tiledb_query_get_est_result_size_var_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val) {
  int ret = tiledb_query_get_est_result_size_var(
      ctx, query, name, size_off, size_val);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_query_est_result_sizes(
          ctx,
          query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_query_est_result_sizes(
          ctx,
          query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          buff) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return tiledb_query_get_est_result_size_var(
      ctx, query, name, size_off, size_val);
}

void Query2Fx::create_dense_array(const std::string& array_name, bool anon) {
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

  const char* attr_b_name = anon ? "" : "b";

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* b;
  rc = tiledb_attribute_alloc(ctx_, attr_b_name, TILEDB_INT32, &b);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, b, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, b, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);

  // Create array schema
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

void Query2Fx::create_sparse_array(
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

  // Create array schema
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

void Query2Fx::create_sparse_array_1d(
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

  // Create array schema
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

void Query2Fx::create_sparse_array_2d(
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

  // Create array schema
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

void Query2Fx::create_sparse_array_real(const std::string& array_name) {
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

  // Create array schema
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

void Query2Fx::write_dense_array(
    const std::string& array_name,
    const std::vector<uint64_t>& domain,
    const std::vector<int>& a,
    const std::vector<uint64_t>& b_off,
    const std::vector<int>& b_val,
    bool anon) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  uint64_t a_size = a.size() * sizeof(int);
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  uint64_t b_val_size = b_val.size() * sizeof(int);

  const char* attr_b_name = anon ? "" : "b";

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, domain.data());
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", (void*)a.data(), &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attr_b_name, (void*)b_val.data(), &b_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attr_b_name, (uint64_t*)b_off.data(), &b_off_size);
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

void Query2Fx::write_sparse_array(
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
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", (void*)a.data(), &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "b", (void*)b_val.data(), &b_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "b", (uint64_t*)b_off.data(), &b_off_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
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
    Query2Fx,
    "C API: Test subarray, sparse, basic API usage and errors",
    "[capi][query_2][sparse][basic]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_sparse_basic";
  remove_array(array_name);
  create_sparse_array(array_name);

  // Allocate subarray with an unopened array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set config for `sm.read_range_oob` = `error`
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.read_range_oob", "error", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_query_set_config(ctx_, query, config);
  REQUIRE(rc == TILEDB_OK);

  // Set/Get layout
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  tiledb_layout_t layout;
  rc = tiledb_query_get_layout(ctx_, query, &layout);
  CHECK(rc == TILEDB_OK);
  CHECK(layout == TILEDB_UNORDERED);

  tiledb_subarray_t* subarray;
  tiledb_subarray_alloc(ctx_, array, &subarray);

  // Check getting range num from an invalid dimension index
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 2, &range_num);
  CHECK(rc == TILEDB_ERR);

  // Check getting range from an invalid dimension index
  const void *start, *end, *stride;
  rc = tiledb_subarray_get_range(ctx_, subarray, 2, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_ERR);

  // Check getting range from an invalid range index
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 1, &start, &end, &stride);
  CHECK(rc == TILEDB_ERR);

  // Add null range
  uint64_t v = 0;
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &v, nullptr, nullptr);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, nullptr, &v, nullptr);
  CHECK(rc == TILEDB_ERR);

  // Add non-null stride
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &v, &v, &v);
  CHECK(rc == TILEDB_ERR);

  // Variable-sized range
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 0, &v, 1, &v, 1);
  REQUIRE(rc == TILEDB_ERR);

  // Add ranges outside the subarray domain
  uint64_t inv_r1[] = {0, 0};
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 0, &inv_r1[0], &inv_r1[1], nullptr);
  CHECK(rc == TILEDB_ERR);
  uint64_t inv_r2[] = {0, 20};
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 1, &inv_r2[0], &inv_r2[1], nullptr);
  CHECK(rc == TILEDB_ERR);
  uint64_t inv_r3[] = {11, 20};
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 1, &inv_r3[0], &inv_r3[1], nullptr);
  CHECK(rc == TILEDB_ERR);

  // Add range with invalid end points
  uint64_t inv_r4[] = {5, 4};
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 0, &inv_r4[0], &inv_r4[1], nullptr);
  CHECK(rc == TILEDB_ERR);

  // Add valid ranges
  uint64_t r1[] = {1, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
  CHECK(rc == TILEDB_OK);
  uint64_t r2[] = {2, 8};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
  CHECK(rc == TILEDB_OK);
  uint64_t r3[] = {2, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r3[0], &r3[1], nullptr);
  CHECK(rc == TILEDB_OK);

  // Check range num
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);

  // Check getting range from an invalid range index
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 2, &start, &end, &stride);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_get_range(ctx_, subarray, 1, 1, &start, &end, &stride);
  CHECK(rc == TILEDB_ERR);

  // Check ranges
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(uint64_t*)start == 1);
  CHECK(*(uint64_t*)end == 3);
  CHECK(stride == nullptr);
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 1, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(uint64_t*)start == 2);
  CHECK(*(uint64_t*)end == 8);
  CHECK(stride == nullptr);
  rc = tiledb_subarray_get_range(ctx_, subarray, 1, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(uint64_t*)start == 2);
  CHECK(*(uint64_t*)end == 2);
  CHECK(stride == nullptr);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_config_free(&config);
  CHECK(config == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, sparse, check default (empty) subarray",
    "[capi][query_2][sparse][default]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_sparse_default";
  remove_array(array_name);
  create_sparse_array(array_name);

  // Allocate subarray with an unopened array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  // Check that there is a single range initially per dimension
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);

  // Check ranges
  const void *start, *end, *stride;
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(uint64_t*)start == 1);
  CHECK(*(uint64_t*)end == 10);
  CHECK(stride == nullptr);
  rc = tiledb_subarray_get_range(ctx_, subarray, 1, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(uint64_t*)start == 1);
  CHECK(*(uint64_t*)end == 10);
  CHECK(stride == nullptr);

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
    Query2Fx,
    "C API: Test subarray, sparse, check NaN ranges",
    "[capi][query_2][sparse][errors][nan]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_sparse_nan";
  remove_array(array_name);
  create_sparse_array_real(array_name);

  // Create subarray
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  // Add range with NaN
  double range[] = {std::numeric_limits<double>::quiet_NaN(), 10};
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 0, &range[0], &range[1], nullptr);
  CHECK(rc == TILEDB_ERR);
  double range2[] = {1.3, 4.2};
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 0, &range2[0], &range2[0], nullptr);
  CHECK(rc == TILEDB_OK);

  // Set the subarray on the query
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, sparse, result estimation, empty tree",
    "[capi][query_2][sparse][result-estimation][0]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_sparse_result_estimation_0";
  remove_array(array_name);
  create_sparse_array_1d(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);

  uint64_t size, size_off, size_val;

  // Simple checks
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "b", &size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "a", &size_off, &size_val);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "foo", &size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "foo", &size_off, &size_val);
  CHECK(rc == TILEDB_ERR);

  // Get estimated result size
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 0);
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 0);
  CHECK(size_val == 0);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, sparse, 1D, result estimation, height 2",
    "[capi][query_2][sparse][result-estimation][1d][2]") {
  std::string array_name = "subarray_sparse_result_estimation_1d_2";
  remove_array(array_name);

  uint64_t domain[] = {1, 100};
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;
  tiledb_subarray_t* subarray = nullptr;
  uint64_t size, size_off, size_val;

  SECTION("- row-major") {
    SECTION("- No serialization") {
      serialize_ = false;
    }
    SECTION("- Serialization") {
      serialize_ = true;
    }
    // Create array
    create_sparse_array_1d(array_name, domain, TILEDB_ROW_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create query
    rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 20};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 15 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {20, 30};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 2 ranges") {
      uint64_t r1[] = {1, 1};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {20, 30};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Partial overlap, 1 range") {
      uint64_t r[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t))));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(int))));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(
          size_off == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t))));
      CHECK(
          size_val == (uint64_t)((2.0 / 3) * (3 * sizeof(int)) +
                                 (2.0 / 6) * (6 * sizeof(int))));
    }

    SECTION("-- Partial overlap, 2 ranges") {
      uint64_t r1[] = {3, 6};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {10, 12};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      auto coords_size = (uint64_t)ceil(
          (2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size =
          (uint64_t)ceil((2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size = (uint64_t)ceil(
          (2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (3.0 / 6) * (6 * sizeof(int)) +
          (1.0 / 7) * (6 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }
  }

  SECTION("- col-major") {
    SECTION("- No serialization") {
      serialize_ = false;
    }
    SECTION("- Serialization") {
      serialize_ = true;
    }
    // Create array
    create_sparse_array_1d(array_name, domain, TILEDB_COL_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create query
    rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 20};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 15 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {20, 30};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 2 ranges") {
      uint64_t r1[] = {1, 1};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {20, 30};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Partial overlap, 1 range") {
      uint64_t r[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t))));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(int))));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(
          size_off == (uint64_t)((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t))));
      CHECK(
          size_val == (uint64_t)((2.0 / 3) * (3 * sizeof(int)) +
                                 (2.0 / 6) * (6 * sizeof(int))));
    }

    SECTION("-- Partial overlap, 2 ranges") {
      uint64_t r1[] = {3, 6};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {10, 12};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      auto coords_size = (uint64_t)ceil(
          (2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size =
          (uint64_t)ceil((2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size = (uint64_t)ceil(
          (2.0 / 3 + 3.0 / 6 + 1.0 / 7) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
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
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, sparse, 1D, result estimation, height 3",
    "[capi][query_2][sparse][result-estimation][1d][3]") {
  std::string array_name = "subarray_sparse_result_estimation_1d_3";
  remove_array(array_name);

  uint64_t domain[] = {1, 100};
  std::vector<uint64_t> coords = {2, 4, 5, 10, 12, 18, 20, 23, 24, 27};
  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<uint64_t> b_off = {
      0,
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
  tiledb_query_t* query = nullptr;
  tiledb_subarray_t* subarray = nullptr;
  uint64_t size, size_off, size_val;

  SECTION("- row-major") {
    SECTION("- No serialization") {
      serialize_ = false;
    }
    SECTION("- Serialization") {
      serialize_ = true;
    }
    // Create array
    create_sparse_array_1d(array_name, domain, TILEDB_ROW_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 27};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 10 * sizeof(uint64_t));
      CHECK(size_val == 19 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {30, 40};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 2 ranges") {
      uint64_t r1[] = {1, 1};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {30, 40};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Overlap only tiles, 1 range") {
      uint64_t r[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      auto coords_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size = (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (4 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Overlap only tiles, 2 ranges") {
      uint64_t r1[] = {3, 6};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {23, 24};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      auto coords_size = (uint64_t)ceil(
          (2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size = (uint64_t)ceil(
          (2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (4 * sizeof(int)) +
          (1.0 / 4) * (2 * sizeof(int)) + (1.0 / 4) * (3 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Overlap only tile ranges, 1 range") {
      uint64_t r[] = {2, 18};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 14 * sizeof(int));
    }

    SECTION("-- Overlap only tile ranges, 2 ranges") {
      uint64_t r1[] = {2, 18};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {19, 28};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 10 * sizeof(uint64_t));
      CHECK(size_val == 19 * sizeof(int));
    }

    SECTION("-- Overlap tiles and tile ranges") {
      uint64_t r[] = {2, 20};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (6 + (1.0 / 4) * 2) * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (6 + (1.0 / 4) * 2) * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == (6 + (1.0 / 4) * 2) * sizeof(uint64_t));
      CHECK(size_val == 14 * sizeof(int) + (1.0 / 4) * 2 * sizeof(int));
    }
  }

  SECTION("- col-major") {
    SECTION("- No serialization") {
      serialize_ = false;
    }
    SECTION("- Serialization") {
      serialize_ = true;
    }
    // Create array
    create_sparse_array_1d(array_name, domain, TILEDB_COL_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create query
    rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 27};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 10 * sizeof(uint64_t));
      CHECK(size_val == 19 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {30, 40};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 2 ranges") {
      uint64_t r1[] = {1, 1};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {30, 40};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Overlap only tiles, 1 range") {
      uint64_t r[] = {3, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      auto coords_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size = (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (4 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Overlap only tiles, 2 ranges") {
      uint64_t r1[] = {3, 6};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {23, 24};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      auto coords_size = (uint64_t)ceil(
          (2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(uint64_t)));
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size =
          (uint64_t)ceil((2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(int)));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size = (uint64_t)ceil(
          (2.0 / 3 + 2.0 / 6 + 2.0 / 4) * (2 * sizeof(uint64_t)));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
          (2.0 / 3) * (3 * sizeof(int)) + (2.0 / 6) * (4 * sizeof(int)) +
          (1.0 / 4) * (2 * sizeof(int)) + (1.0 / 4) * (3 * sizeof(int)));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Overlap only tile ranges, 1 range") {
      uint64_t r[] = {2, 18};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 14 * sizeof(int));
    }

    SECTION("-- Overlap only tile ranges, 2 ranges") {
      uint64_t r1[] = {2, 18};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r2[] = {19, 28};
      rc =
          tiledb_subarray_add_range(ctx_, subarray, 0, &r2[0], &r2[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 10 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 10 * sizeof(uint64_t));
      CHECK(size_val == 19 * sizeof(int));
    }

    SECTION("-- Overlap tiles and tile ranges") {
      uint64_t r[] = {2, 20};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (6 + (1.0 / 4) * 2) * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == (6 + (1.0 / 4) * 2) * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
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
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, sparse, 2D, result estimation, height 2",
    "[capi][query_2][sparse][result-estimation][2d][2]") {
  std::string array_name = "subarray_sparse_result_estimation_2d_2";
  remove_array(array_name);

  uint64_t domain[] = {1, 10, 1, 10};
  std::vector<uint64_t> coords = {1, 2, 2, 5, 3, 3, 3, 9, 4, 1, 4, 7};
  std::vector<int> a = {1, 2, 3, 4, 5, 6};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 6};
  tiledb_array_t* array = nullptr;
  tiledb_query_t* query = nullptr;
  tiledb_subarray_t* subarray = nullptr;
  uint64_t size, size_off, size_val;

  SECTION("- row-major") {
    SECTION("- No serialization") {
      serialize_ = false;
    }
    SECTION("- Serialization") {
      serialize_ = true;
    }
    // Create array
    create_sparse_array_2d(array_name, domain, TILEDB_ROW_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create query
    rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 10, 1, 10};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2], &r[3], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * 2 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "d1", &size, &size);
      CHECK(rc == TILEDB_ERR);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "d2", &size, &size);
      CHECK(rc == TILEDB_ERR);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 15 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {1, 2, 7, 8};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2], &r[3], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 4 ranges") {
      uint64_t r11[] = {1, 2};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 0, &r11[0], &r11[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r12[] = {5, 6};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 0, &r12[0], &r12[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r21[] = {6, 7};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 1, &r21[0], &r21[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r22[] = {9, 10};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 1, &r22[0], &r22[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Partial overlap, 1 range") {
      uint64_t r[] = {2, 3, 5, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2], &r[3], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      auto coords_size = std::max<uint64_t>(
          (uint64_t)ceil(
              (1.0 / 2) * (1.0 / 4) * 4 * sizeof(uint64_t) +
              1.0 * (2.0 / 7) * 4 * sizeof(uint64_t)),
          2 * sizeof(uint64_t));
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      CHECK(rc == TILEDB_OK);
      auto d1_size = std::max<uint64_t>(
          (uint64_t)ceil(
              (1.0 / 2) * (1.0 / 4) * 2 * sizeof(uint64_t) +
              1.0 * (2.0 / 7) * 2 * sizeof(uint64_t)),
          sizeof(uint64_t));
      CHECK(size == d1_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      CHECK(rc == TILEDB_OK);
      auto d2_size = std::max<uint64_t>(
          (uint64_t)ceil(
              (1.0 / 2) * (1.0 / 4) * 2 * sizeof(uint64_t) +
              1.0 * (2.0 / 7) * 2 * sizeof(uint64_t)),
          sizeof(uint64_t));
      CHECK(size == d2_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size = std::max<uint64_t>(
          (uint64_t)ceil(
              (1.0 / 2) * (1.0 / 4) * 2 * sizeof(int) +
              1.0 * (2.0 / 7) * 2 * sizeof(int)),
          sizeof(int));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size = std::max<uint64_t>(
          (uint64_t)ceil(
              (1.0 / 2) * (1.0 / 4) * 2 * sizeof(uint64_t) +
              1.0 * (2.0 / 7) * 2 * sizeof(uint64_t)),
          sizeof(uint64_t));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
          (1.0 / 2) * (1.0 / 4) * 3 * sizeof(int) +
          1.0 * (2.0 / 7) * 6 * sizeof(int));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Partial overlap, 4 ranges") {
      uint64_t r11[] = {1, 2};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 0, &r11[0], &r11[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r12[] = {4, 4};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 0, &r12[0], &r12[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r21[] = {1, 2};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 1, &r21[0], &r21[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r22[] = {7, 8};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 1, &r22[0], &r22[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      auto coords_size = (uint64_t)ceil(
          (1.0 / 4) * 4 * sizeof(uint64_t) + (3.0 / 7) * 4 * sizeof(uint64_t));
      CHECK(rc == TILEDB_OK);
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      auto d1_size = (uint64_t)ceil(
          (1.0 / 4) * 2 * sizeof(uint64_t) + (3.0 / 7) * 2 * sizeof(uint64_t));
      CHECK(rc == TILEDB_OK);
      CHECK(size == d1_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      auto d2_size = (uint64_t)ceil(
          (1.0 / 4) * 2 * sizeof(uint64_t) + (3.0 / 7) * 2 * sizeof(uint64_t));
      CHECK(rc == TILEDB_OK);
      CHECK(size == d2_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size = (uint64_t)ceil(
          (1.0 / 4) * 2 * sizeof(int) + (3.0 / 7) * 2 * sizeof(int));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size = (uint64_t)ceil(
          (1.0 / 4) * 2 * sizeof(uint64_t) + (3.0 / 7) * 2 * sizeof(uint64_t));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
          (1.0 / 4) * 3 * sizeof(int) + (3.0 / 7) * 6 * sizeof(int));
      CHECK(size_val == b_val_size);
    }
  }

  SECTION("- col-major") {
    SECTION("- No serialization") {
      serialize_ = false;
    }
    SECTION("- Serialization") {
      serialize_ = true;
    }
    // Create array
    create_sparse_array_2d(array_name, domain, TILEDB_COL_MAJOR);
    write_sparse_array(array_name, coords, a, b_off, b_val);

    // Open array
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Create query
    rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
    CHECK(rc == TILEDB_OK);

    // Create subarray
    rc = tiledb_subarray_alloc(ctx_, array, &subarray);
    CHECK(rc == TILEDB_OK);

    SECTION("-- Full overlap") {
      uint64_t r[] = {1, 10, 1, 10};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2], &r[3], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * 2 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(uint64_t));
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 6 * sizeof(int));
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 6 * sizeof(uint64_t));
      CHECK(size_val == 15 * sizeof(int));
    }

    SECTION("-- No overlap, 1 range") {
      uint64_t r[] = {1, 2, 7, 8};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2], &r[3], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- No overlap, 4 ranges") {
      uint64_t r11[] = {1, 2};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 0, &r11[0], &r11[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r12[] = {5, 6};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 0, &r12[0], &r12[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r21[] = {6, 7};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 1, &r21[0], &r21[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r22[] = {9, 10};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 1, &r22[0], &r22[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      CHECK(size == 0);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      CHECK(size_off == 0);
      CHECK(size_val == 0);
    }

    SECTION("-- Partial overlap, 1 range") {
      uint64_t r[] = {2, 3, 5, 6};
      rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[2], &r[3], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      CHECK(rc == TILEDB_OK);
      auto coords_size = std::max<uint64_t>(
          (uint64_t)ceil(1.0 * (1.0 / 3) * 4 * sizeof(uint64_t)),
          2 * sizeof(uint64_t));
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      CHECK(rc == TILEDB_OK);
      auto d1_size = std::max<uint64_t>(
          (uint64_t)ceil(1.0 * (1.0 / 3) * 2 * sizeof(uint64_t)),
          sizeof(uint64_t));
      CHECK(size == d1_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      CHECK(rc == TILEDB_OK);
      auto d2_size = std::max<uint64_t>(
          (uint64_t)ceil(1.0 * (1.0 / 3) * 2 * sizeof(uint64_t)),
          sizeof(uint64_t));
      CHECK(size == d2_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size = std::max<uint64_t>(
          (uint64_t)ceil(1.0 * (1.0 / 3) * 2 * sizeof(int)), sizeof(int));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size = std::max<uint64_t>(
          (uint64_t)ceil(1.0 * (1.0 / 3) * 2 * sizeof(uint64_t)),
          sizeof(uint64_t));
      CHECK(size_off == b_off_size);
      auto b_val_size = std::max<uint64_t>(
          (uint64_t)ceil(1.0 * (1.0 / 3) * 5 * sizeof(int)), sizeof(int));
      CHECK(size_val == b_val_size);
    }

    SECTION("-- Partial overlap, 4 ranges") {
      uint64_t r11[] = {1, 2};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 0, &r11[0], &r11[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r12[] = {4, 4};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 0, &r12[0], &r12[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r21[] = {1, 2};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 1, &r21[0], &r21[1], nullptr);
      CHECK(rc == TILEDB_OK);
      uint64_t r22[] = {7, 8};
      rc = tiledb_subarray_add_range(
          ctx_, subarray, 1, &r22[0], &r22[1], nullptr);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_get_est_result_size_wrapper(
          ctx_, query, TILEDB_COORDS, &size);
      auto coords_size = (uint64_t)ceil(
          (6.0 / 8) * 4 * sizeof(uint64_t) + (2.0 / 6) * 4 * sizeof(uint64_t));
      CHECK(rc == TILEDB_OK);
      CHECK(size == coords_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d1", &size);
      auto d1_size = (uint64_t)ceil(
          (6.0 / 8) * 2 * sizeof(uint64_t) + (2.0 / 6) * 2 * sizeof(uint64_t));
      CHECK(rc == TILEDB_OK);
      CHECK(size == d1_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
      auto d2_size = (uint64_t)ceil(
          (6.0 / 8) * 2 * sizeof(uint64_t) + (2.0 / 6) * 2 * sizeof(uint64_t));
      CHECK(rc == TILEDB_OK);
      CHECK(size == d2_size);
      rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
      CHECK(rc == TILEDB_OK);
      auto a_size = (uint64_t)ceil(
          (6.0 / 8) * 2 * sizeof(int) + (2.0 / 6) * 2 * sizeof(int));
      CHECK(size == a_size);
      rc = tiledb_query_get_est_result_size_var_wrapper(
          ctx_, query, "b", &size_off, &size_val);
      CHECK(rc == TILEDB_OK);
      auto b_off_size = (uint64_t)ceil(
          (6.0 / 8) * 2 * sizeof(uint64_t) + (2.0 / 6) * 2 * sizeof(uint64_t));
      CHECK(size_off == b_off_size);
      auto b_val_size = (uint64_t)ceil(
          (6.0 / 8) * 3 * sizeof(int) + (2.0 / 6) * 7 * sizeof(int));
      CHECK(size_val == b_val_size);
    }
  }

  // Clean-up
  int rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, dense, result estimation, empty array",
    "[capi][query_2][dense][result-estimation][0]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_dense_result_estimation_0";
  remove_array(array_name);
  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  uint64_t size, size_off, size_val;

  // Simple checks
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "b", &size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "a", &size_off, &size_val);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "foo", &size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "foo", &size_off, &size_val);
  CHECK(rc == TILEDB_ERR);

  // Get estimated result size
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 100 * sizeof(int32_t));
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 100 * sizeof(uint64_t));
  CHECK(size_val == 100 * sizeof(int32_t));

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, dense, result estimation, 1 range, full tile",
    "[capi][query_2][dense][est][1r][full-tile]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_dense_est_1r_full_tile";
  remove_array(array_name);

  std::vector<int> a = {1, 2, 3, 4};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
  };
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4};
  uint64_t size, size_off, size_val;
  std::vector<uint64_t> domain = {1, 2, 1, 2};

  // Create array
  create_dense_array(array_name);
  write_dense_array(array_name, domain, a, b_off, b_val);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  uint64_t r[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[0], &r[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 4 * sizeof(int));
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "d2", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 4 * sizeof(uint64_t));
  rc = tiledb_query_get_est_result_size_wrapper(
      ctx_, query, TILEDB_COORDS, &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 4 * 2 * sizeof(uint64_t));
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 4 * sizeof(uint64_t));
  CHECK(size_val == 9 * sizeof(int));

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, dense, result estimation, 1 range, full tile, anon "
    "attribute",
    "[capi][query_2][dense][est][1r][full-tile-anon]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_dense_est_1r_full_tile_anon";
  remove_array(array_name);

  std::vector<int> a = {1, 2, 3, 4};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
  };
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4};
  uint64_t size, size_off, size_val;
  std::vector<uint64_t> domain = {1, 2, 1, 2};

  // Create array with anonymous 2nd attribute
  create_dense_array(array_name, true);
  write_dense_array(array_name, domain, a, b_off, b_val, true);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  uint64_t r[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r[0], &r[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r[0], &r[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 4 * sizeof(int));
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 4 * sizeof(uint64_t));
  CHECK(size_val == 9 * sizeof(int));

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, dense, result estimation, 1 range, 2 full tiles",
    "[capi][query_2][dense][est][1r][2-full-tiles]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_dense_est_1r_2_full_tiles";
  remove_array(array_name);

  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int),
      14 * sizeof(int),
      15 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 8};
  uint64_t size, size_off, size_val;
  std::vector<uint64_t> domain = {1, 2, 1, 4};

  // Create array
  create_dense_array(array_name);
  write_dense_array(array_name, domain, a, b_off, b_val);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  uint64_t r1[] = {1, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
  uint64_t r2[] = {1, 4};
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r2[0], &r2[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 8 * sizeof(int));
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 8 * sizeof(uint64_t));
  CHECK(size_val == 16 * sizeof(int));

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, dense, result estimation, 1 range, partial tiles",
    "[capi][query_2][dense][est][1r][2-full-tiles]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_dense_est_1r_2_full_tiles";
  remove_array(array_name);

  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int),
      14 * sizeof(int),
      15 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 8};
  uint64_t size, size_off, size_val;
  std::vector<uint64_t> domain = {1, 2, 1, 4};

  // Create array
  create_dense_array(array_name);
  write_dense_array(array_name, domain, a, b_off, b_val);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  uint64_t r1[] = {2, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
  uint64_t r2[] = {1, 3};
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r2[0], &r2[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 3 * sizeof(int));
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 3 * sizeof(uint64_t));
  CHECK(size_val == 6 * sizeof(int));

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, dense, result estimation, multiple ranges",
    "[capi][query_2][dense][est][nr]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_dense_est_nr";
  remove_array(array_name);

  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int),
      14 * sizeof(int),
      15 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 8};
  uint64_t size, size_off, size_val;
  std::vector<uint64_t> domain = {1, 2, 1, 4};

  // Create array
  create_dense_array(array_name);
  write_dense_array(array_name, domain, a, b_off, b_val);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  uint64_t r11[] = {1, 1};
  uint64_t r12[] = {2, 2};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r11[0], &r11[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r12[0], &r12[1], nullptr);
  uint64_t r21[] = {1, 1};
  uint64_t r22[] = {3, 4};
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r21[0], &r21[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r22[0], &r22[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 6 * sizeof(int));
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 6 * sizeof(uint64_t));
  CHECK(size_val == 12 * sizeof(int));

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, dense, result estimation, non-coinciding domain",
    "[capi][query_2][dense][est][non-coinciding]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_dense_est_non_coinciding";
  remove_array(array_name);

  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int),
      14 * sizeof(int),
      15 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 8};
  uint64_t size, size_off, size_val;
  std::vector<uint64_t> domain = {2, 3, 1, 4};

  // Create array
  create_dense_array(array_name);
  write_dense_array(array_name, domain, a, b_off, b_val);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  uint64_t r11[] = {2, 2};
  uint64_t r12[] = {3, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r11[0], &r11[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r12[0], &r12[1], nullptr);
  uint64_t r21[] = {1, 1};
  uint64_t r22[] = {3, 4};
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r21[0], &r21[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r22[0], &r22[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 6 * sizeof(int));
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 6 * sizeof(uint64_t));
  CHECK(size_val == (0.25 * 5 + 0.5 * 8 + 0.25 * 7 + 0.5 * 4) * sizeof(int));

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, dense, result estimation, 1 range, 2 dense frags",
    "[capi][query_2][dense][est][1r][2-dense-frags]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_dense_est_1r_2_dense_frags";
  remove_array(array_name);

  std::vector<int> a = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
      9 * sizeof(int),
      11 * sizeof(int),
      14 * sizeof(int),
      15 * sizeof(int)};
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 8};
  uint64_t size, size_off, size_val;
  std::vector<uint64_t> domain1 = {1, 2, 1, 4};
  std::vector<uint64_t> domain2 = {3, 4, 1, 4};

  // Create array
  create_dense_array(array_name);
  write_dense_array(array_name, domain1, a, b_off, b_val);
  write_dense_array(array_name, domain2, a, b_off, b_val);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  uint64_t r1[] = {2, 3};
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &r1[0], &r1[1], nullptr);
  uint64_t r2[] = {1, 3};
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &r2[0], &r2[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_wrapper(ctx_, query, "a", &size);
  CHECK(rc == TILEDB_OK);
  CHECK(size == 6 * sizeof(int));
  rc = tiledb_query_get_est_result_size_var_wrapper(
      ctx_, query, "b", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 6 * sizeof(uint64_t));
  CHECK(size_val == 12 * sizeof(int));

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test written fragments, errors with read queries",
    "[capi][query_2][written-fragments][errors-read]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "query_written_fragments_errors_read";
  remove_array(array_name);

  // Create array
  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  uint32_t num = 0;
  rc = tiledb_query_get_fragment_num(ctx_, query, &num);
  CHECK(rc == TILEDB_ERR);
  const char* uri;
  rc = tiledb_query_get_fragment_uri(ctx_, query, 0, &uri);
  CHECK(rc == TILEDB_ERR);
  uint64_t t1, t2;
  rc = tiledb_query_get_fragment_timestamp_range(ctx_, query, 0, &t1, &t2);
  CHECK(rc == TILEDB_ERR);

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test written fragments",
    "[capi][query_2][written-fragments]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "query_written_fragments";
  remove_array(array_name);

  // Create array
  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  std::vector<int> a = {1, 2, 3, 4};
  std::vector<uint64_t> b_off = {
      0,
      sizeof(int),
      3 * sizeof(int),
      6 * sizeof(int),
  };
  std::vector<int> b_val = {1, 2, 2, 3, 3, 3, 4, 4, 4};
  std::vector<uint64_t> domain = {1, 2, 1, 2};

  uint64_t a_size = a.size() * sizeof(int);
  uint64_t b_off_size = b_off.size() * sizeof(uint64_t);
  uint64_t b_val_size = b_val.size() * sizeof(int);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, domain.data());
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", (void*)a.data(), &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "b", (void*)b_val.data(), &b_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "b", (uint64_t*)b_off.data(), &b_off_size);
  CHECK(rc == TILEDB_OK);

  // No fragments written yet
  uint32_t num = 100;
  rc = tiledb_query_get_fragment_num(ctx_, query, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 0);
  const char* uri;
  rc = tiledb_query_get_fragment_uri(ctx_, query, 0, &uri);
  CHECK(rc == TILEDB_ERR);
  uint64_t t1, t2;
  rc = tiledb_query_get_fragment_timestamp_range(ctx_, query, 0, &t1, &t2);
  CHECK(rc == TILEDB_ERR);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // 1 fragment written
  rc = tiledb_query_get_fragment_num(ctx_, query, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 1);
  rc = tiledb_query_get_fragment_uri(ctx_, query, 0, &uri);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_fragment_timestamp_range(ctx_, query, 0, &t1, &t2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_fragment_uri(ctx_, query, 1, &uri);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_fragment_timestamp_range(ctx_, query, 1, &t1, &t2);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test range by name APIs",
    "[capi][query_2][range][string-dims]") {
  std::string array_name = "query_ranges";
  remove_array(array_name);

  // Create array
  uint64_t dom[] = {1, 10};
  uint64_t extent = 5;
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_STRING_ASCII, TILEDB_UINT64},
      {nullptr, dom},
      {nullptr, &extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Create subarray
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  // set dimensions
  char d1_data[] = "abbccdddd";
  uint64_t d1_data_size = sizeof(d1_data) - 1;  // Ignore '\0'
  uint64_t d1_off[] = {0, 1, 3, 5};
  uint64_t d1_off_size = sizeof(d1_off);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d1", d1_data, &d1_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "d1", d1_off, &d1_off_size);
  REQUIRE(rc == TILEDB_OK);

  // Add 1 range per dimension
  char s1[] = "a";
  char s2[] = "cc";
  // Variable-sized range
  rc =
      tiledb_subarray_add_range_var_by_name(ctx_, subarray, "d1", s1, 1, s2, 2);
  CHECK(rc == TILEDB_OK);
  // fixed-sized range
  uint64_t r[] = {1, 2};
  rc = tiledb_subarray_add_range_by_name(
      ctx_, subarray, "d2", &r[0], &r[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Check number of ranges on each dimension
  uint64_t range_num;
  rc =
      tiledb_subarray_get_range_num_from_name(ctx_, subarray, "d1", &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc =
      tiledb_subarray_get_range_num_from_name(ctx_, subarray, "d2", &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);

  // Check ranges
  const void *start, *end, *stride;
  rc = tiledb_subarray_get_range_from_name(
      ctx_, subarray, "d2", 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const uint64_t*)start == 1);
  CHECK(*(const uint64_t*)end == 2);

  uint64_t start_size = 0, end_size = 0;
  rc = tiledb_subarray_get_range_var_size_from_name(
      ctx_, subarray, "d1", 0, &start_size, &end_size);
  CHECK(rc == TILEDB_OK);
  CHECK(start_size == 1);
  CHECK(end_size == 2);
  std::vector<char> start_data(start_size);
  std::vector<char> end_data(end_size);
  rc = tiledb_subarray_get_range_var_from_name(
      ctx_, subarray, "d1", 0, start_data.data(), end_data.data());
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(start_data.begin(), start_data.end()) == "a");
  CHECK(std::string(end_data.begin(), end_data.end()) == "cc");

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx, "C API: Test query set config", "[capi][config][query]") {
  std::string array_name = "query_set_config";
  remove_array(array_name);

  // Create array
  int32_t dom[] = {1, 6};
  int32_t extent = 2;
  create_array(
      ctx_,
      array_name,
      TILEDB_DENSE,
      {"d1"},
      {TILEDB_INT32},
      {dom},
      {&extent},
      {"a"},
      {TILEDB_INT32},
      {TILEDB_VAR_NUM},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Open array
  tiledb_array_t* array = nullptr;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);

  // Create config
  tiledb_error_t* err;
  tiledb_config_t* config = nullptr;
  rc = tiledb_config_alloc(&config, &err);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "sm.var_offsets.bitsize", "32", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "sm.var_offsets.extra_element", "true", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "sm.var_offsets.mode", "elements", &err);
  CHECK(rc == TILEDB_OK);

  // Test setting config
  rc = tiledb_query_set_config(ctx_, query, config);
  CHECK(rc == TILEDB_OK);

  // Test getting config, it should be identical
  tiledb_config_t* config2;
  rc = tiledb_query_get_config(ctx_, query, &config2);
  CHECK(rc == TILEDB_OK);

  uint8_t equal;
  rc = tiledb_config_compare(config, config2, &equal);
  CHECK(rc == TILEDB_OK);
  CHECK(equal == 1);
  tiledb_config_free(&config2);

  // Test modified behavior
  std::vector<uint32_t> offsets = {0, 1, 2, 4, 7, 9, 10};
  // even in elements mode, we need to pass offsets size as if uint64
  uint64_t offsets_size = offsets.size() * sizeof(uint64_t);
  std::vector<int32_t> data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  uint64_t data_size = data.size() * sizeof(int32_t);

  rc = tiledb_query_set_data_buffer(ctx_, query, "a", data.data(), &data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a", (uint64_t*)offsets.data(), &offsets_size);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);

  // Create read query
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  tiledb_query_t* query2 = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query2);
  CHECK(rc == TILEDB_OK);

  // Set override config
  rc = tiledb_query_set_config(ctx_, query2, config);
  CHECK(rc == TILEDB_OK);

  std::vector<int32_t> data2;
  std::vector<uint32_t> offsets2;
  data2.resize(data.size());
  offsets2.resize(offsets.size());

  rc =
      tiledb_query_set_data_buffer(ctx_, query2, "a", data2.data(), &data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query2, "a", (uint64_t*)offsets2.data(), &offsets_size);
  CHECK(rc == TILEDB_OK);

  tiledb_subarray_t* sub;
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, &dom);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query2, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  rc = tiledb_query_submit(ctx_, query2);
  CHECK(rc == TILEDB_OK);

  CHECK(data == data2);
  CHECK(offsets == offsets2);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query2);
  CHECK(query2 == nullptr);
  tiledb_array_free(&array);
  CHECK(array == nullptr);

  tiledb_config_free(&config);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Query2Fx,
    "C API: Test subarray, sparse, set bulk point ranges",
    "[capi][query_2][sparse][bulk-ranges]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
  SECTION("- Serialization") {
    serialize_ = true;
  }
  std::string array_name = "subarray_sparse_default_bulk_ranges";
  remove_array(array_name);
  create_sparse_array(array_name);

  // Allocate subarray with an unopened array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Add ranges
  uint64_t ranges[] = {1, 3, 7, 10};
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_point_ranges(ctx_, subarray, 0, ranges, 4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_point_ranges(ctx_, subarray, 1, ranges, 4);
  CHECK(rc == TILEDB_OK);

  // Check that there are four ranges
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 4);

  // Check ranges
  for (uint32_t dim_idx = 0; dim_idx < 1; dim_idx++) {
    for (uint32_t idx = 0; idx < 4; idx++) {
      const void *start, *end, *stride;
      rc = tiledb_subarray_get_range(
          ctx_, subarray, dim_idx, idx, &start, &end, &stride);
      CHECK(rc == TILEDB_OK);
      CHECK(*(uint64_t*)start == ranges[idx]);
      CHECK(*(uint64_t*)end == ranges[idx]);
      CHECK(stride == nullptr);
    }
  }

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}
