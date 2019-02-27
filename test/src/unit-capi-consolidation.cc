/**
 * @file   unit-capi-consolidation.cc
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
 * Tests the C API consolidation.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#include <climits>
#include <cstring>
#include <iostream>

/** Tests for C API consolidation. */
struct ConsolidationFx {
  // Constants
  const char* DENSE_VECTOR_NAME = "test_consolidate_dense_vector";
  const char* DENSE_ARRAY_NAME = "test_consolidate_dense_array";
  const char* SPARSE_ARRAY_NAME = "test_consolidate_sparse_array";
  const char* KV_NAME = "test_consolidate_kv";

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Encryption parameters
  tiledb_encryption_type_t encryption_type_ = TILEDB_NO_ENCRYPTION;
  const char* encryption_key_ = nullptr;

  // Constructors/destructors
  ConsolidationFx();
  ~ConsolidationFx();

  // Functions
  void create_dense_vector();
  void create_dense_array();
  void create_sparse_array();
  void create_kv();
  void write_dense_vector_4_fragments();
  void write_dense_vector_4_fragments_not_coinciding();
  void write_dense_vector_4_fragments_not_coinciding_with_gaps();
  void write_dense_vector_4_fragments_mixed();
  void write_dense_vector_consolidatable_1();
  void write_dense_vector_consolidatable_2();
  void write_dense_vector_del_1();
  void write_dense_vector_del_2();
  void write_dense_vector_del_3();
  void write_dense_full();
  void write_dense_subarray();
  void write_dense_unordered();
  void write_sparse_full();
  void write_sparse_unordered();
  void write_kv_keys_abc();
  void write_kv_keys_acd();
  void read_dense_vector();
  void read_dense_vector_with_gaps();
  void read_dense_vector_mixed();
  void read_dense_vector_consolidatable_1();
  void read_dense_vector_consolidatable_2();
  void read_dense_vector_del_1();
  void read_dense_vector_del_2();
  void read_dense_vector_del_3();
  void read_dense_full_subarray_unordered();
  void read_dense_subarray_full_unordered();
  void read_dense_subarray_unordered_full();
  void read_sparse_full_unordered();
  void read_sparse_unordered_full();
  void read_kv_keys_abc_acd();
  void read_kv_keys_acd_abc();
  void consolidate_dense();
  void consolidate_sparse();
  void consolidate_kv();
  void remove_dense_vector();
  void remove_dense_array();
  void remove_sparse_array();
  void remove_kv();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);

  // Used to get the number of directories of another directory
  struct get_dir_num_struct {
    tiledb_ctx_t* ctx;
    tiledb_vfs_t* vfs;
    int dir_num;
  };

  static int get_dir_num(const char* path, void* data);
};

ConsolidationFx::ConsolidationFx() {
  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx_) == TILEDB_OK);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, nullptr, &vfs_) == TILEDB_OK);
}

ConsolidationFx::~ConsolidationFx() {
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void ConsolidationFx::create_dense_vector() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 410};
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

  // Create attribute
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_create(ctx_, DENSE_VECTOR_NAME, array_schema);
  } else {
    rc = tiledb_array_create_with_key(
        ctx_,
        DENSE_VECTOR_NAME,
        array_schema,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void ConsolidationFx::create_dense_array() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
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
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a1, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a2, TILEDB_FILTER_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a3, TILEDB_FILTER_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_create(ctx_, DENSE_ARRAY_NAME, array_schema);
  } else {
    rc = tiledb_array_create_with_key(
        ctx_,
        DENSE_ARRAY_NAME,
        array_schema,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void ConsolidationFx::create_sparse_array() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
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
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a1, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a2, TILEDB_FILTER_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a3, TILEDB_FILTER_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
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
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_create(ctx_, SPARSE_ARRAY_NAME, array_schema);
  } else {
    rc = tiledb_array_create_with_key(
        ctx_,
        SPARSE_ARRAY_NAME,
        array_schema,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void ConsolidationFx::create_kv() {
  // Create attributes
  tiledb_attribute_t* a1;
  int rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  tiledb_filter_t* filter;
  tiledb_filter_list_t* list;
  rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_BZIP2, &filter);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_filter_list_alloc(ctx_, &list);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, list, filter);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_filter_list(ctx_, a1, list);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);

  // Create key-value schema
  tiledb_kv_schema_t* kv_schema;
  rc = tiledb_kv_schema_alloc(ctx_, &kv_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_add_attribute(ctx_, kv_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_set_capacity(ctx_, kv_schema, 10);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_kv_schema_check(ctx_, kv_schema);
  CHECK(rc == TILEDB_OK);

  // Create key-value store
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_create(ctx_, KV_NAME, kv_schema);
  } else {
    rc = tiledb_kv_create_with_key(
        ctx_,
        KV_NAME,
        kv_schema,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&list);
  tiledb_attribute_free(&a1);
  tiledb_kv_schema_free(&kv_schema);
}

void ConsolidationFx::write_dense_vector_4_fragments() {
  // Prepare cell buffers for 4 writes
  int a_1[200];
  for (int i = 0; i < 200; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[50];
  for (int i = 0; i < 50; ++i)
    a_2[i] = 200 + i;
  uint64_t a_2_size = sizeof(a_2);
  int a_3[60];
  for (int i = 0; i < 60; ++i)
    a_3[i] = 250 + i;
  uint64_t a_3_size = sizeof(a_3);
  int a_4[100];
  for (int i = 0; i < 100; ++i)
    a_4[i] = 310 + i;
  uint64_t a_4_size = sizeof(a_4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 200};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);

  // Submit query #2
  tiledb_query_t* query_2;
  subarray[0] = 201;
  subarray[1] = 250;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_2, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);

  // Submit query #3
  tiledb_query_t* query_3;
  subarray[0] = 251;
  subarray[1] = 310;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_3, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);

  // Submit query #4
  tiledb_query_t* query_4;
  subarray[0] = 311;
  subarray[1] = 410;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_4, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_4, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_4, "a", a_4, &a_4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_4);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query_1);
  tiledb_query_free(&query_2);
  tiledb_query_free(&query_3);
  tiledb_query_free(&query_4);
}

void ConsolidationFx::write_dense_vector_4_fragments_not_coinciding() {
  // Prepare cell buffers for 4 writes
  int a_1[198];
  for (int i = 0; i < 198; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[50];
  for (int i = 0; i < 50; ++i)
    a_2[i] = 198 + i;
  uint64_t a_2_size = sizeof(a_2);
  int a_3[60];
  for (int i = 0; i < 60; ++i)
    a_3[i] = 248 + i;
  uint64_t a_3_size = sizeof(a_3);
  int a_4[102];
  for (int i = 0; i < 102; ++i)
    a_4[i] = 308 + i;
  uint64_t a_4_size = sizeof(a_4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 198};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);

  // Submit query #2
  tiledb_query_t* query_2;
  subarray[0] = 199;
  subarray[1] = 248;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_2, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);

  // Submit query #3
  tiledb_query_t* query_3;
  subarray[0] = 249;
  subarray[1] = 308;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_3, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);

  // Submit query #4
  tiledb_query_t* query_4;
  subarray[0] = 309;
  subarray[1] = 410;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_4, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_4, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_4, "a", a_4, &a_4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_4);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query_1);
  tiledb_query_free(&query_2);
  tiledb_query_free(&query_3);
  tiledb_query_free(&query_4);
}

void ConsolidationFx::
    write_dense_vector_4_fragments_not_coinciding_with_gaps() {
  // Prepare cell buffers for 4 writes
  int a_1[200];
  for (int i = 0; i < 200; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[48];
  for (int i = 0; i < 48; ++i)
    a_2[i] = 202 + i;
  uint64_t a_2_size = sizeof(a_2);
  int a_3[58];
  for (int i = 0; i < 58; ++i)
    a_3[i] = 250 + i;
  uint64_t a_3_size = sizeof(a_3);
  int a_4[100];
  for (int i = 0; i < 100; ++i)
    a_4[i] = 310 + i;
  uint64_t a_4_size = sizeof(a_4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 200};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);

  // Submit query #2
  tiledb_query_t* query_2;
  subarray[0] = 203;
  subarray[1] = 250;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_2, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);

  // Submit query #3
  tiledb_query_t* query_3;
  subarray[0] = 251;
  subarray[1] = 308;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_3, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);

  // Submit query #4
  tiledb_query_t* query_4;
  subarray[0] = 311;
  subarray[1] = 410;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_4, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_4, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_4, "a", a_4, &a_4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_4);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query_1);
  tiledb_query_free(&query_2);
  tiledb_query_free(&query_3);
  tiledb_query_free(&query_4);
}

void ConsolidationFx::write_dense_vector_4_fragments_mixed() {
  // Prepare cell buffers for 4 writes
  int a_1[300];
  for (int i = 0; i < 300; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[] = {1050, 1100, 1150};
  uint64_t a_2_size = sizeof(a_2);
  uint64_t coords_2[] = {50, 100, 150};
  uint64_t coords_2_size = sizeof(coords_2);
  int a_3[] = {1025, 1075, 1125};
  uint64_t a_3_size = sizeof(a_3);
  uint64_t coords_3[] = {25, 75, 125};
  uint64_t coords_3_size = sizeof(coords_3);
  int a_4[110];
  for (int i = 0; i < 110; ++i)
    a_4[i] = 300 + i;
  uint64_t a_4_size = sizeof(a_4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1 - Dense
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 300};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_1);

  // Submit query #2
  tiledb_query_t* query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_2, TILEDB_COORDS, coords_2, &coords_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_2);

  // Submit query #3
  tiledb_query_t* query_3;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_3, TILEDB_COORDS, coords_3, &coords_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_3);

  // Submit query #4
  tiledb_query_t* query_4;
  subarray[0] = 301;
  subarray[1] = 410;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_4, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_4, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_4, "a", a_4, &a_4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_4);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void ConsolidationFx::write_dense_vector_consolidatable_1() {
  // Prepare cell buffers for 4 writes
  int a_1[100];
  for (int i = 0; i < 100; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[] = {190};
  uint64_t a_2_size = sizeof(a_2);
  uint64_t coords_2[] = {90};
  uint64_t coords_2_size = sizeof(coords_2);
  int a_3[] = {100};
  uint64_t a_3_size = sizeof(a_3);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 100};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_1);

  // Submit query #2
  tiledb_query_t* query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_2, TILEDB_COORDS, coords_2, &coords_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_2);

  // Submit query #3
  tiledb_query_t* query_3;
  subarray[0] = 101;
  subarray[1] = 101;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_3, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_3);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void ConsolidationFx::write_dense_vector_consolidatable_2() {
  // Prepare cell buffers for 4 writes
  int a_1[100];
  for (int i = 0; i < 100; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[100];
  uint64_t a_2_size = sizeof(a_2);
  for (int i = 0; i < 100; ++i)
    a_2[i] = 201 + i;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 100};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_1);

  // Submit query #2
  tiledb_query_t* query_2;
  subarray[0] = 201;
  subarray[1] = 300;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_2, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void ConsolidationFx::write_dense_vector_del_1() {
  // Prepare cell buffers for 4 writes
  int a_1[200];
  for (int i = 0; i < 200; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[] = {1250, 1275, 1300};
  uint64_t a_2_size = sizeof(a_2);
  uint64_t coords_2[] = {250, 275, 300};
  uint64_t coords_2_size = sizeof(coords_2);
  int a_3[] = {1251, 1276, 1301};
  uint64_t a_3_size = sizeof(a_3);
  uint64_t coords_3[] = {251, 276, 301};
  uint64_t coords_3_size = sizeof(coords_3);
  int a_4[200];
  for (int i = 0; i < 200; ++i)
    a_4[i] = 200 + i;
  uint64_t a_4_size = sizeof(a_4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1 - Dense
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 200};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_1);

  // Submit query #2
  tiledb_query_t* query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_2, TILEDB_COORDS, coords_2, &coords_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_2);

  // Submit query #3
  tiledb_query_t* query_3;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_3, TILEDB_COORDS, coords_3, &coords_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_3);

  // Submit query #4
  tiledb_query_t* query_4;
  subarray[0] = 201;
  subarray[1] = 400;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_4, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_4, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_4, "a", a_4, &a_4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_4);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void ConsolidationFx::write_dense_vector_del_2() {
  // Prepare cell buffers for 4 writes
  int a_1[200];
  for (int i = 0; i < 200; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[] = {1250, 1275, 1300};
  uint64_t a_2_size = sizeof(a_2);
  uint64_t coords_2[] = {250, 275, 300};
  uint64_t coords_2_size = sizeof(coords_2);
  int a_3[] = {1251, 1276, 1301};
  uint64_t a_3_size = sizeof(a_3);
  uint64_t coords_3[] = {251, 276, 301};
  uint64_t coords_3_size = sizeof(coords_3);
  int a_4[400];
  for (int i = 0; i < 400; ++i)
    a_4[i] = 10000 + i;
  uint64_t a_4_size = sizeof(a_4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1 - Dense
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 200};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_1);

  // Submit query #2
  tiledb_query_t* query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_2, TILEDB_COORDS, coords_2, &coords_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_2);

  // Submit query #3
  tiledb_query_t* query_3;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_3, TILEDB_COORDS, coords_3, &coords_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_3);

  // Submit query #4
  tiledb_query_t* query_4;
  subarray[0] = 1;
  subarray[1] = 400;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_4, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_4, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_4, "a", a_4, &a_4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_4);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void ConsolidationFx::write_dense_vector_del_3() {
  // Prepare cell buffers for 4 writes
  int a_1[200];
  for (int i = 0; i < 200; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[] = {1250, 1275, 1300};
  uint64_t a_2_size = sizeof(a_2);
  uint64_t coords_2[] = {250, 275, 300};
  uint64_t coords_2_size = sizeof(coords_2);
  int a_3[200];
  for (int i = 0; i < 200; ++i)
    a_3[i] = 10200 + i;
  uint64_t a_3_size = sizeof(a_3);
  int a_4[] = {1251, 1276, 1301};
  uint64_t a_4_size = sizeof(a_4);
  uint64_t coords_4[] = {251, 276, 301};
  uint64_t coords_4_size = sizeof(coords_4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Submit query #1 - Dense
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 200};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_1);

  // Submit query #2
  tiledb_query_t* query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_2, TILEDB_COORDS, coords_2, &coords_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_2);

  // Submit query #3
  tiledb_query_t* query_3;
  subarray[0] = 201;
  subarray[1] = 400;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_3, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_3);

  // Submit query #4
  tiledb_query_t* query_4;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_4, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_4, "a", a_4, &a_4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query_4, TILEDB_COORDS, coords_4, &coords_4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query_4);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void ConsolidationFx::write_dense_full() {
  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3"};

  // Prepare cell buffers
  // clang-format off
  int buffer_a1[] = {
      0,  1,  2,  3, 4,  5,  6,  7,
      8,  9,  10, 11, 12, 13, 14, 15
  };
  uint64_t buffer_a2[] = {
      0,  1,  3,  6, 10, 11, 13, 16,
      20, 21, 23, 26, 30, 31, 33, 36
  };
  char buffer_var_a2[] =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  float buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  uint64_t buffer_sizes[] =
  {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3)
  };
  // clang-format on

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
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

void ConsolidationFx::write_dense_subarray() {
  // Prepare cell buffers
  int buffer_a1[] = {112, 113, 114, 115};
  uint64_t buffer_a2[] = {0, 1, 3, 6};
  char buffer_var_a2[] = "MNNOOOPPPP";
  float buffer_a3[] = {
      112.1f, 112.2f, 113.1f, 113.2f, 114.1f, 114.2f, 115.1f, 115.2f};
  void* buffers[] = {buffer_a1, buffer_a2, buffer_var_a2, buffer_a3};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3"};
  uint64_t subarray[] = {3, 4, 3, 4};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
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

void ConsolidationFx::write_dense_unordered() {
  // Prepare buffers
  int buffer_a1[] = {211, 213, 212, 208};
  uint64_t buffer_a2[] = {0, 4, 6, 7};
  char buffer_var_a2[] = "wwwwyyxu";
  float buffer_a3[] = {
      211.1f, 211.2f, 213.1f, 213.2f, 212.1f, 212.2f, 208.1f, 208.2f};
  uint64_t buffer_coords[] = {4, 2, 3, 4, 3, 3, 3, 1};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
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

void ConsolidationFx::write_sparse_full() {
  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {0.1f,
                       0.2f,
                       1.1f,
                       1.2f,
                       2.1f,
                       2.2f,
                       3.1f,
                       3.2f,
                       4.1f,
                       4.2f,
                       5.1f,
                       5.2f,
                       6.1f,
                       6.2f,
                       7.1f,
                       7.2f};
  uint64_t buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1, 4, 2, 3, 3, 3, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
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

void ConsolidationFx::write_sparse_unordered() {
  // Prepare cell buffers
  int buffer_a1[] = {107, 104, 106, 105};
  uint64_t buffer_a2[] = {0, 3, 4, 5};
  char buffer_var_a2[] = "yyyuwvvvv";
  float buffer_a3[] = {
      107.1f, 107.2f, 104.1f, 104.2f, 106.1f, 106.2f, 105.1f, 105.2f};
  uint64_t buffer_coords[] = {3, 4, 3, 2, 3, 3, 4, 1};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
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

void ConsolidationFx::write_kv_keys_abc() {
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, KV_NAME, &kv);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_WRITE);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  const int one = 1, two = 2, three = 3;

  // Add key-value item #1
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, kv_item1, "A", TILEDB_CHAR, 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, "a1", &one, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item1);
  CHECK(rc == TILEDB_OK);

  // Add key-value item #2
  tiledb_kv_item_t* kv_item2;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, kv_item2, "B", TILEDB_CHAR, sizeof(char));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item2, "a1", &two, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item2);
  CHECK(rc == TILEDB_OK);

  // Add key-value item #3
  tiledb_kv_item_t* kv_item3;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, kv_item3, "C", TILEDB_CHAR, sizeof(char));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item3, "a1", &three, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item3);
  CHECK(rc == TILEDB_OK);

  // Flush
  rc = tiledb_kv_flush(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Close kv
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
}

void ConsolidationFx::write_kv_keys_acd() {
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, KV_NAME, &kv);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_WRITE);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  const int four = 4, five = 5, six = 6;

  // Add key-value item #1
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, kv_item1, "A", TILEDB_CHAR, 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, "a1", &four, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item1);
  CHECK(rc == TILEDB_OK);

  // Add key-value item #2
  tiledb_kv_item_t* kv_item2;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, kv_item2, "C", TILEDB_CHAR, sizeof(char));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item2, "a1", &five, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item2);
  CHECK(rc == TILEDB_OK);

  // Add key-value item #3
  tiledb_kv_item_t* kv_item3;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, kv_item3, "D", TILEDB_CHAR, sizeof(char));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item3, "a1", &six, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item3);
  CHECK(rc == TILEDB_OK);

  // Flush
  rc = tiledb_kv_flush(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Close kv
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
}

void ConsolidationFx::read_dense_vector() {
  // Correct buffer
  int c_a[410];
  for (int i = 0; i < 410; ++i)
    c_a[i] = i;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Preparation
  uint64_t subarray[] = {1, 410};
  int a[410];
  uint64_t a_size = sizeof(a);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 410; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_vector_with_gaps() {
  // Correct buffer
  int c_a[410];
  for (int i = 0; i < 410; ++i)
    c_a[i] = i;
  c_a[200] = INT_MIN;
  c_a[201] = INT_MIN;
  c_a[308] = INT_MIN;
  c_a[309] = INT_MIN;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Preparation
  uint64_t subarray[] = {1, 410};
  int a[410];
  uint64_t a_size = sizeof(a);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 410; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_vector_mixed() {
  // Correct buffer
  int c_a[410];
  for (int i = 0; i < 410; ++i)
    c_a[i] = i;
  c_a[49] = 1050;
  c_a[99] = 1100;
  c_a[149] = 1150;
  c_a[24] = 1025;
  c_a[74] = 1075;
  c_a[124] = 1125;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Preparation
  int a[410];
  uint64_t a_size = sizeof(a);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 410; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_vector_consolidatable_1() {
  // Correct buffer
  int c_a[101];
  for (int i = 0; i < 101; ++i)
    c_a[i] = i;
  c_a[89] = 190;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Preparation
  int a[101];
  uint64_t a_size = sizeof(a);
  uint64_t subarray[] = {1, 101};

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 101; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_vector_consolidatable_2() {
  // Correct buffer
  int c_a[300];
  for (int i = 0; i < 100; ++i)
    c_a[i] = i;
  for (int i = 100; i < 200; ++i)
    c_a[i] = INT_MIN;
  for (int i = 200; i < 300; ++i)
    c_a[i] = i + 1;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Preparation
  int a[300];
  uint64_t a_size = sizeof(a);
  uint64_t subarray[] = {1, 300};

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 300; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_vector_del_1() {
  // Correct buffer
  int c_a[400];
  for (int i = 0; i < 400; ++i)
    c_a[i] = i;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Preparation
  int a[400];
  uint64_t a_size = sizeof(a);
  uint64_t subarray[] = {1, 400};

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 400; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_vector_del_2() {
  // Correct buffer
  int c_a[400];
  for (int i = 0; i < 400; ++i)
    c_a[i] = 10000 + i;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Preparation
  int a[400];
  uint64_t a_size = sizeof(a);
  uint64_t subarray[] = {1, 400};

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 400; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_vector_del_3() {
  // Correct buffer
  int c_a[400];
  for (int i = 0; i < 200; ++i)
    c_a[i] = i;
  for (int i = 200; i < 400; ++i)
    c_a[i] = 10000 + i;
  c_a[250] = 1251;
  c_a[275] = 1276;
  c_a[300] = 1301;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Preparation
  int a[400];
  uint64_t a_size = sizeof(a);
  uint64_t subarray[] = {1, 400};

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 400; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_full_subarray_unordered() {
  // Correct buffers
  int c_buffer_a1[] = {
      0, 1, 2, 3, 4, 5, 6, 7, 208, 9, 10, 211, 212, 213, 114, 115};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 3, 6, 10, 11, 13, 16, 20, 21, 23, 26, 30, 31, 33, 36};
  char c_buffer_a2_val[] =
      "abbcccdddd"
      "effggghhhh"
      "ujjkkkwwww"
      "xyyOOOPPPP";
  float c_buffer_a3[] = {
      0.1f,   0.2f,   1.1f,   1.2f,   2.1f,   2.2f,   3.1f,   3.2f,
      4.1f,   4.2f,   5.1f,   5.2f,   6.1f,   6.2f,   7.1f,   7.2f,
      208.1f, 208.2f, 9.1f,   9.2f,   10.1f,  10.2f,  211.1f, 211.2f,
      212.1f, 212.2f, 213.1f, 213.2f, 114.1f, 114.2f, 115.1f, 115.2f,
  };

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
}

void ConsolidationFx::read_dense_subarray_full_unordered() {
  // Correct buffers
  int c_buffer_a1[] = {
      0, 1, 2, 3, 4, 5, 6, 7, 208, 9, 10, 211, 212, 213, 14, 15};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 3, 6, 10, 11, 13, 16, 20, 21, 23, 26, 30, 31, 33, 36};
  char c_buffer_a2_val[] =
      "abbcccdddd"
      "effggghhhh"
      "ujjkkkwwww"
      "xyyooopppp";
  float c_buffer_a3[] = {
      0.1f,   0.2f,   1.1f,   1.2f,   2.1f,  2.2f,  3.1f,   3.2f,
      4.1f,   4.2f,   5.1f,   5.2f,   6.1f,  6.2f,  7.1f,   7.2f,
      208.1f, 208.2f, 9.1f,   9.2f,   10.1f, 10.2f, 211.1f, 211.2f,
      212.1f, 212.2f, 213.1f, 213.2f, 14.1f, 14.2f, 15.1f,  15.2f,
  };

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
}

void ConsolidationFx::read_dense_subarray_unordered_full() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 3, 6, 10, 11, 13, 16, 20, 21, 23, 26, 30, 31, 33, 36};
  char c_buffer_a2_val[] =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  float c_buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
}

void ConsolidationFx::read_sparse_full_unordered() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 105, 5, 106, 107};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 16, 18, 19};
  char c_buffer_a2_val[] = "abbcccddddeuvvvvffwyyy";
  float c_buffer_a3[] = {0.1f, 0.2f, 1.1f,   1.2f,   2.1f,   2.2f,   3.1f,
                         3.2f, 4.1f, 4.2f,   104.1f, 104.2f, 105.1f, 105.2f,
                         5.1f, 5.2f, 106.1f, 106.2f, 107.1f, 107.2f};
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1,
                                3, 2, 4, 1, 4, 2, 3, 3, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords);
}

void ConsolidationFx::read_sparse_unordered_full() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 105, 5, 6, 7};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 16, 18, 21};
  char c_buffer_a2_val[] = "abbcccddddeuvvvvffggghhhh";
  float c_buffer_a3[] = {0.1f, 0.2f, 1.1f, 1.2f,   2.1f,   2.2f,   3.1f,
                         3.2f, 4.1f, 4.2f, 104.1f, 104.2f, 105.1f, 105.2f,
                         5.1f, 5.2f, 6.1f, 6.2f,   7.1f,   7.2f};
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1,
                                3, 2, 4, 1, 4, 2, 3, 3, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords);
}

void ConsolidationFx::read_kv_keys_abc_acd() {
  // Open key-value store
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, KV_NAME, &kv);
  REQUIRE(rc == TILEDB_OK);

  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_READ);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // For retrieving values
  const void *a1, *a2, *a3;
  uint64_t a1_size, a2_size, a3_size;
  tiledb_datatype_t a1_type, a2_type, a3_type;
  int has_key;

  // Get key-value item #1
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_get_item(ctx_, kv, "A", TILEDB_CHAR, sizeof(char), &kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(ctx_, kv_item1, "a1", &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == 4);

  // Get key-value item #2
  tiledb_kv_item_t* kv_item2;
  rc = tiledb_kv_get_item(ctx_, kv, "B", TILEDB_CHAR, sizeof(char), &kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(ctx_, kv_item2, "a1", &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == 2);

  // Get key-value item #3
  tiledb_kv_item_t* kv_item3;
  rc = tiledb_kv_get_item(ctx_, kv, "C", TILEDB_CHAR, sizeof(char), &kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(ctx_, kv_item3, "a1", &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == 5);

  // Get key-value item #4
  tiledb_kv_item_t* kv_item4;
  rc = tiledb_kv_get_item(ctx_, kv, "D", TILEDB_CHAR, sizeof(char), &kv_item4);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(ctx_, kv_item4, "a1", &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == 6);

  // Close key-value store
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
  tiledb_kv_item_free(&kv_item4);
}

void ConsolidationFx::read_kv_keys_acd_abc() {
  // Open key-value store
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, KV_NAME, &kv);
  REQUIRE(rc == TILEDB_OK);

  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_READ);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // For retrieving values
  const void *a1, *a2, *a3;
  uint64_t a1_size, a2_size, a3_size;
  tiledb_datatype_t a1_type, a2_type, a3_type;
  int has_key;

  // Get key-value item #1
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_get_item(ctx_, kv, "A", TILEDB_CHAR, sizeof(char), &kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(ctx_, kv_item1, "a1", &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == 1);

  // Get key-value item #2
  tiledb_kv_item_t* kv_item2;
  rc = tiledb_kv_get_item(ctx_, kv, "B", TILEDB_CHAR, sizeof(char), &kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(ctx_, kv_item2, "a1", &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == 2);

  // Get key-value item #3
  tiledb_kv_item_t* kv_item3;
  rc = tiledb_kv_get_item(ctx_, kv, "C", TILEDB_CHAR, sizeof(char), &kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(ctx_, kv_item3, "a1", &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == 3);

  // Get key-value item #4
  tiledb_kv_item_t* kv_item4;
  rc = tiledb_kv_get_item(ctx_, kv, "D", TILEDB_CHAR, sizeof(char), &kv_item4);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(ctx_, kv_item4, "a1", &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == 6);

  // Close key-value store
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
  tiledb_kv_item_free(&kv_item4);
}

void ConsolidationFx::consolidate_dense() {
  int rc;
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_consolidate(ctx_, DENSE_ARRAY_NAME, nullptr);
  } else {
    rc = tiledb_array_consolidate_with_key(
        ctx_,
        DENSE_ARRAY_NAME,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        nullptr);
  }
  REQUIRE(rc == TILEDB_OK);
}

void ConsolidationFx::consolidate_sparse() {
  int rc;
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_consolidate(ctx_, SPARSE_ARRAY_NAME, nullptr);
  } else {
    rc = tiledb_array_consolidate_with_key(
        ctx_,
        SPARSE_ARRAY_NAME,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        nullptr);
  }
  REQUIRE(rc == TILEDB_OK);
}

void ConsolidationFx::consolidate_kv() {
  int rc;
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_consolidate(ctx_, KV_NAME, nullptr);
  } else {
    rc = tiledb_kv_consolidate_with_key(
        ctx_,
        KV_NAME,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        nullptr);
  }
  REQUIRE(rc == TILEDB_OK);
}

void ConsolidationFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

void ConsolidationFx::remove_dense_vector() {
  remove_array(DENSE_VECTOR_NAME);
}

void ConsolidationFx::remove_dense_array() {
  remove_array(DENSE_ARRAY_NAME);
}

void ConsolidationFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

void ConsolidationFx::remove_kv() {
  remove_array(KV_NAME);
}

bool ConsolidationFx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY || type == TILEDB_KEY_VALUE;
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, dense",
    "[capi], [consolidation], [dense-consolidation]") {
  remove_dense_array();
  create_dense_array();

  SECTION("- write full, subarray, unordered") {
    write_dense_full();
    write_dense_subarray();
    write_dense_unordered();
    consolidate_dense();
    read_dense_full_subarray_unordered();
  }

  SECTION("- write subarray, full, unordered") {
    write_dense_subarray();
    write_dense_full();
    write_dense_unordered();
    consolidate_dense();
    read_dense_subarray_full_unordered();
  }

  SECTION("- write subarray, unordered, full") {
    write_dense_subarray();
    write_dense_unordered();
    write_dense_full();
    consolidate_dense();
    read_dense_subarray_unordered_full();
  }

  SECTION("- write (encrypted) subarray, unordered, full") {
    remove_dense_array();
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
    create_dense_array();
    write_dense_subarray();
    write_dense_unordered();
    write_dense_full();
    consolidate_dense();
    read_dense_subarray_unordered_full();
  }

  remove_dense_array();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, sparse",
    "[capi], [consolidation], [sparse-consolidation]") {
  remove_sparse_array();
  create_sparse_array();

  SECTION("- write full, unordered") {
    write_sparse_full();
    write_sparse_unordered();
    consolidate_sparse();
    read_sparse_full_unordered();
  }

  SECTION("- write unordered, full") {
    write_sparse_unordered();
    write_sparse_full();
    consolidate_sparse();
    read_sparse_unordered_full();
  }

  SECTION("- write (encrypted) unordered, full") {
    remove_sparse_array();
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
    create_sparse_array();
    write_sparse_unordered();
    write_sparse_full();
    consolidate_sparse();
    read_sparse_unordered_full();
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, KV",
    "[capi], [consolidation], [kv], [kv-consolidation]") {
  remove_kv();
  create_kv();

  SECTION("- write key A,B,C; A,C,D") {
    write_kv_keys_abc();
    write_kv_keys_acd();
    consolidate_kv();
    read_kv_keys_abc_acd();
  }

  SECTION("- write keys A,C,D; A,B,C") {
    write_kv_keys_acd();
    write_kv_keys_abc();
    consolidate_kv();
    read_kv_keys_acd_abc();
  }

  SECTION("- write (encrypted) keys A,C,D; A,B,C") {
    remove_kv();
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
    create_kv();
    write_kv_keys_acd();
    write_kv_keys_abc();
    consolidate_kv();
    read_kv_keys_acd_abc();
  }

  remove_kv();
}

int ConsolidationFx::get_dir_num(const char* path, void* data) {
  auto data_struct = (ConsolidationFx::get_dir_num_struct*)data;
  auto ctx = data_struct->ctx;
  auto vfs = data_struct->vfs;
  int is_dir;
  int rc = tiledb_vfs_is_dir(ctx, vfs, path, &is_dir);
  CHECK(rc == TILEDB_OK);
  data_struct->dir_num += is_dir;

  return 1;
}

// Test valid configuration parameters
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, wrong configs",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-config]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Test consolidation steps
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "-1", &error);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(error != nullptr);
  tiledb_error_free(&error);
  rc = tiledb_config_set(config, "sm.consolidation.steps", "1.5", &error);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(error != nullptr);
  tiledb_error_free(&error);
  rc = tiledb_config_set(config, "sm.consolidation.steps", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Test buffer size
  rc = tiledb_config_set(config, "sm.consolidation.buffer_size", "-1", &error);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(error != nullptr);
  tiledb_error_free(&error);
  rc = tiledb_config_set(config, "sm.consolidation.buffer_size", "1.5", &error);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(error != nullptr);
  tiledb_error_free(&error);
  rc = tiledb_config_set(
      config, "sm.consolidation.buffer_size", "10000000", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Test min frags
  rc = tiledb_config_set(
      config, "sm.consolidation.step_min_frags", "-1", &error);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(error != nullptr);
  tiledb_error_free(&error);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_min_frags", "1.5", &error);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(error != nullptr);
  tiledb_error_free(&error);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Test max frags
  rc = tiledb_config_set(
      config, "sm.consolidation.step_max_frags", "-1", &error);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(error != nullptr);
  tiledb_error_free(&error);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_max_frags", "1.5", &error);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(error != nullptr);
  tiledb_error_free(&error);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Test min frags (currently set to 5) > max frags (currently set to 2)
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_config_set(
      config, "sm.consolidation.step_max_frags", "10", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Test size ratio
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "-1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "1.5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Test amplification
  rc =
      tiledb_config_set(config, "sm.consolidation.amplification", "-1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_ERR);

  // Check that there are 4 fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 4);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test whether the min/max parameters work
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #1",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-1]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test whether >1 steps work
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #2",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-2]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test a strict consolidation size ratio that prevents consolidation
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #3",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-3]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "1.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 4);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test consolidation size ratio that leads to consolidation of 2 fragments
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #4",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-4]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test consolidation size ratio 0.5 and two steps
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #5",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-5]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test consolidation size ratio 0.5 and 10 steps
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #6",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-6]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "10", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 1);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Step = 1, Min = 2, Max = 3, Ratio = 0.0
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #7",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-7]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "3", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Step = 1, Min = 2, Max = 8, Ratio = 0.0
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #8",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-8]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "8", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 1);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test selected read/write layout computed internally based
// on fragments whose domain does not coincide the space tiling.
// In this case, the non-empty domain should be expanded.
// This test should not allow any fragment subset consolidation
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, fragments that don't coincide with space tiles "
    "#1",
    "[capi], [consolidation], [consolidation-not-coinciding-1]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments_not_coinciding();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 4);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test selected read/write layout computed internally based
// on fragments whose domain does not coincide the space tiling.
// In this case, the non-empty domain should be expanded.
// This test should allow the two middle fragments to be consolidated.
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, fragments that don't coincide with space tiles "
    "#2",
    "[capi], [consolidation], [consolidation-not-coinciding-2]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments_not_coinciding_with_gaps();
  read_dense_vector_with_gaps();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.amplification", "1.5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_with_gaps();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, small buffer size",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-buffer-size]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.consolidation.buffer_size", "10", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, encrypted array",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-encrypted]") {
  remove_dense_vector();
  encryption_type_ = TILEDB_AES_256_GCM;
  encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate_with_key(
      ctx_,
      DENSE_VECTOR_NAME,
      encryption_type_,
      encryption_key_,
      (uint32_t)strlen(encryption_key_),
      config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, dense array mixing both sparse and "
    "dense fragments",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-dense-mixed]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments_mixed();
  read_dense_vector_mixed();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_mixed();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test deleting overwritten fragments - no deletion
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, overwritten fragments, no deletion",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-overwritten-no-del]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();
  read_dense_vector();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "1.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 4);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, overwritten fragments, deletion #1",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-overwritten-del-1]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_del_1();
  read_dense_vector_del_1();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "1.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_del_1();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, overwritten fragments, deletion #2",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-overwritten-del-2]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_del_2();
  read_dense_vector_del_2();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "1.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_del_2();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 1);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, overwritten fragments, deletion #3",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-overwritten-del-3]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_del_3();
  read_dense_vector_del_3();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "1.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_del_3();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test previous fragments overlap
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, consolidatable #1",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-consolidatable-1]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_consolidatable_1();
  read_dense_vector_consolidatable_1();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_consolidatable_1();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test amplification
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, consolidatable #2",
    "[capi], [consolidation], [consolidation-adv], "
    "[consolidation-adv-consolidatable-2]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_consolidatable_2();
  read_dense_vector_consolidatable_2();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.5", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  bool should_consolidate = false;

  SECTION("- should consolidate") {
    should_consolidate = true;
    rc = tiledb_config_set(
        config, "sm.consolidation.amplification", "2", &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);
  }

  SECTION("- should not consolidate") {
    should_consolidate = false;
    rc = tiledb_config_set(
        config, "sm.consolidation.amplification", "1.1", &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);
  }

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_consolidatable_2();

  // Check number of fragments
  get_dir_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.dir_num == ((should_consolidate) ? 1 : 2));

  tiledb_config_free(&config);
  remove_dense_vector();
}
