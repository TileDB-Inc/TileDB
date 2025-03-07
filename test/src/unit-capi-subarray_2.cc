/**
 * @file   unit-capi-subarray_2.cc
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
 * Tests the C API for subarray.
 * (cloned from unit-capi-query_2.cc and modified to prepare subarray's outside
 *  of a query, so changes here may suggest similar changes needed there as
 * well)
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#include <climits>
#include <cstring>
#include <iostream>

using namespace tiledb::test;

const uint64_t DIM_DOMAIN[4] = {1, 10, 1, 10};

/** Tests for C API subarray. */
struct Subarray2Fx {
  // TileDB context
  tiledb_ctx_t* ctx_;

  bool serialize_ = false;

  // Constructors/destructor
  Subarray2Fx();
  ~Subarray2Fx();

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
};

Subarray2Fx::Subarray2Fx() {
  ctx_ = vanilla_context_c();
}

Subarray2Fx::~Subarray2Fx() {
  tiledb_ctx_free(&ctx_);
  CHECK(ctx_ == nullptr);
}

bool Subarray2Fx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

void Subarray2Fx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

#if 0
int32_t Subarray2Fx::tiledb_subarray_get_est_result_size_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* name,
    uint64_t* size,
    tiledb_query_t* serialization_query) {
  REQUIRE(
      tiledb_query_set_subarray_t(ctx, serialization_query, subarray) ==
      TILEDB_OK);
  int ret =
      tiledb_query_get_est_result_size(ctx, serialization_query, name, size);
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
          serialization_query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_query_est_result_sizes(
          ctx,
          serialization_query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          buff) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return tiledb_query_get_est_result_size(ctx, serialization_query, name, size);
}

int32_t Subarray2Fx::tiledb_subarray_get_est_result_size_var_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    tiledb_query_t* serialization_query) {
  int ret = tiledb_query_get_est_result_size_var(
      ctx, serialization_query, name, size_off, size_val);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_)
    return ret;

  REQUIRE(
      tiledb_query_set_subarray_t(ctx, serialization_query, subarray) ==
      TILEDB_OK);

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_query_est_result_sizes(
          ctx,
          serialization_query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_query_est_result_sizes(
          ctx,
          serialization_query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          buff) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return tiledb_query_get_est_result_size_var(
      ctx, serialization_query, name, size_off, size_val);
}
#endif

void Subarray2Fx::create_dense_array(const std::string& array_name, bool anon) {
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

void Subarray2Fx::create_sparse_array(
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

void Subarray2Fx::create_sparse_array_1d(
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

void Subarray2Fx::create_sparse_array_2d(
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

void Subarray2Fx::create_sparse_array_real(const std::string& array_name) {
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

void Subarray2Fx::write_dense_array(
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
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, subarray, &domain[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_layout(ctx_, subarray, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", (void*)a.data(), &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attr_b_name,
      (uint64_t*)b_off.data(),
      &b_off_size,
      (void*)b_val.data(),
      &b_val_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_subarray_free(&subarray);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

void Subarray2Fx::write_sparse_array(
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
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    Subarray2Fx,
    "C API: Test subarray/subarray, sparse, basic API usage and errors",
    "[capi][subarray][subarray][subarray_2][sparse][basic]") {
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

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  // Set/Get layout
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  tiledb_layout_t layout;
  rc = tiledb_query_get_layout(ctx_, query, &layout);
  CHECK(rc == TILEDB_OK);
  CHECK(layout == TILEDB_UNORDERED);

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
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_array_free(&array);
  CHECK(array == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Subarray2Fx,
    "C API: Test subarray/subarray, sparse, check default (empty) subarray",
    "[capi][subarray][subarray_2][sparse][default]") {
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

  // Create query
  tiledb_query_t* query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
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
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_array_free(&array);
  CHECK(array == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Subarray2Fx,
    "C API: Test subarray/subarray, sparse, check NaN ranges",
    "[capi][subarray][subarray_2][sparse][errors][nan]") {
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

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
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

  // Clean-up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_array_free(&array);
  CHECK(array == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Subarray2Fx,
    "C API: Test written subarray/fragments, errors with read queries",
    "[capi][subarray][subarray_2][written-fragments][errors-read]") {
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

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
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
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Subarray2Fx,
    "C API: Test written subarray/fragments",
    "[capi][subarray][subarray_2][written-fragments]") {
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
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_subarray_set_subarray(ctx_, subarray, &domain[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_layout(ctx_, subarray, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
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
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
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
  tiledb_subarray_free(&subarray);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    Subarray2Fx,
    "C API: Test (subarray/subarray) range by name APIs",
    "[capi][subarray][subarray_2][range][string-dims]") {
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

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  // set dimensions
  char d1_data[] = "abbccdddd";
  uint64_t d1_data_size = sizeof(d1_data) - 1;  // Ignore '\0'
  uint64_t d1_off[] = {0, 1, 3, 5};
  uint64_t d1_off_size = sizeof(d1_off);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "d1", d1_off, &d1_off_size, d1_data, &d1_data_size);
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
    Subarray2Fx,
    "C API: Test (subarray/)query set config",
    "[capi][subarray][subarray_2][config][query]") {
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

  // Test modified behavior
  std::vector<uint32_t> offsets = {0, 1, 2, 4, 7, 9, 10};
  // even in elements mode, we need to pass offsets size as if uint64
  uint64_t offsets_size = offsets.size() * sizeof(uint64_t);
  std::vector<int32_t> data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  uint64_t data_size = data.size() * sizeof(int32_t);

  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a",
      (uint64_t*)offsets.data(),
      &offsets_size,
      data.data(),
      &data_size);
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

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  tiledb_layout_t query2_layout;
  rc = tiledb_query_get_layout(ctx_, query2, &query2_layout);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_layout(ctx_, subarray, query2_layout);
  CHECK(rc == TILEDB_OK);

  // Set override config
  rc = tiledb_query_set_config(ctx_, query2, config);
  CHECK(rc == TILEDB_OK);

  std::vector<int32_t> data2;
  std::vector<uint32_t> offsets2;
  data2.resize(data.size());
  offsets2.resize(offsets.size());

  rc = tiledb_query_set_buffer_var(
      ctx_,
      query2,
      "a",
      (uint64_t*)offsets2.data(),
      &offsets_size,
      data2.data(),
      &data_size);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_subarray_set_subarray(ctx_, subarray, &dom);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_subarray_t(ctx_, query2, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query2);
  CHECK(rc == TILEDB_OK);

  CHECK(data == data2);
  CHECK(offsets == offsets2);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query2);
  CHECK(query2 == nullptr);
  tiledb_subarray_free(&subarray);
  CHECK(subarray == nullptr);
  tiledb_array_free(&array);
  CHECK(array == nullptr);

  remove_array(array_name);
}
