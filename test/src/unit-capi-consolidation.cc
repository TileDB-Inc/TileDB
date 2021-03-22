/**
 * @file   unit-capi-consolidation.cc
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
 * Tests the C API consolidation.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <climits>
#include <cstring>
#include <iostream>

using namespace tiledb::test;

/** Tests for C API consolidation. */
struct ConsolidationFx {
  // Constants
  const char* DENSE_VECTOR_NAME = "test_consolidate_dense_vector";
  const char* DENSE_ARRAY_NAME = "test_consolidate_dense_array";
  const char* SPARSE_ARRAY_NAME = "test_consolidate_sparse_array";
  const char* SPARSE_HETEROGENEOUS_ARRAY_NAME =
      "test_consolidate_sparse_heterogeneous_array";
  const char* SPARSE_STRING_ARRAY_NAME = "test_consolidate_sparse_string_array";

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
  void create_sparse_heterogeneous_array();
  void create_sparse_string_array();
  void write_dense_vector_4_fragments(uint64_t timestamp = 0);
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
  void write_sparse_heterogeneous_full();
  void write_sparse_heterogeneous_unordered();
  void write_sparse_string_full();
  void write_sparse_string_unordered();
  void read_dense_vector(uint64_t timestamp = UINT64_MAX);
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
  void read_sparse_heterogeneous_full_unordered();
  void read_sparse_heterogeneous_unordered_full();
  void read_sparse_string_full_unordered();
  void read_sparse_string_unordered_full();
  void consolidate_dense();
  void consolidate_sparse();
  void consolidate_sparse_heterogeneous();
  void consolidate_sparse_string();
  void remove_dense_vector();
  void remove_dense_array();
  void remove_sparse_array();
  void remove_sparse_heterogeneous_array();
  void remove_sparse_string_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);

  // Used to get the number of directories or files of another directory
  struct get_num_struct {
    tiledb_ctx_t* ctx;
    tiledb_vfs_t* vfs;
    int num;
  };

  static int get_dir_num(const char* path, void* data);
  static int get_meta_num(const char* path, void* data);
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

void ConsolidationFx::create_sparse_heterogeneous_array() {
  // Create dimensions
  uint64_t dim1_domain[] = {1, 4};
  uint32_t dim2_domain[] = {1, 4};
  uint64_t dim1_tile_extents = 2;
  uint32_t dim2_tile_extents = 2;
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim1_domain[0], &dim1_tile_extents, &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT32, &dim2_domain[0], &dim2_tile_extents, &d2);
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
    rc = tiledb_array_create(
        ctx_, SPARSE_HETEROGENEOUS_ARRAY_NAME, array_schema);
  } else {
    rc = tiledb_array_create_with_key(
        ctx_,
        SPARSE_HETEROGENEOUS_ARRAY_NAME,
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

void ConsolidationFx::create_sparse_string_array() {
  // Create dimensions
  uint64_t dim1_domain[] = {1, 4};
  uint64_t dim1_tile_extents = 2;
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim1_domain[0], &dim1_tile_extents, &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_STRING_ASCII, nullptr, nullptr, &d2);
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
    rc = tiledb_array_create(ctx_, SPARSE_STRING_ARRAY_NAME, array_schema);
  } else {
    rc = tiledb_array_create_with_key(
        ctx_,
        SPARSE_STRING_ARRAY_NAME,
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

void ConsolidationFx::write_dense_vector_4_fragments(uint64_t timestamp) {
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

  // Set array configuration
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(
      cfg,
      "sm.array.open_timestamp_start",
      std::to_string(timestamp + 1).c_str(),
      &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_array_set_config(ctx_, array, cfg);
  REQUIRE(rc == TILEDB_OK);

  // Open array
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

  // Close and re-open at a new timestamp
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_free(&cfg);
  err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(
      cfg,
      "sm.array.open_timestamp_start",
      std::to_string(timestamp + 2).c_str(),
      &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_array_set_config(ctx_, array, cfg);
  REQUIRE(rc == TILEDB_OK);

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

  // Close and re-open at a new timestamp
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_free(&cfg);
  err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(
      cfg,
      "sm.array.open_timestamp_start",
      std::to_string(timestamp + 3).c_str(),
      &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_array_set_config(ctx_, array, cfg);
  REQUIRE(rc == TILEDB_OK);
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

  // Close and re-open at a new timestamp
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_free(&cfg);
  err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(
      cfg,
      "sm.array.open_timestamp_start",
      std::to_string(timestamp + 4).c_str(),
      &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_array_set_config(ctx_, array, cfg);
  REQUIRE(rc == TILEDB_OK);
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
  tiledb_config_free(&cfg);
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
  rc = tiledb_query_set_buffer(ctx_, query_2, "d", coords_2, &coords_2_size);
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
  rc = tiledb_query_set_buffer(ctx_, query_3, "d", coords_3, &coords_3_size);
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
  rc = tiledb_query_set_buffer(ctx_, query_2, "d", coords_2, &coords_2_size);
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
  rc = tiledb_query_set_buffer(ctx_, query_2, "d", coords_2, &coords_2_size);
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
  rc = tiledb_query_set_buffer(ctx_, query_3, "d", coords_3, &coords_3_size);
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
  rc = tiledb_query_set_buffer(ctx_, query_2, "d", coords_2, &coords_2_size);
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
  rc = tiledb_query_set_buffer(ctx_, query_3, "d", coords_3, &coords_3_size);
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
  rc = tiledb_query_set_buffer(ctx_, query_2, "d", coords_2, &coords_2_size);
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
  rc = tiledb_query_set_buffer(ctx_, query_4, "d", coords_4, &coords_4_size);
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
  uint64_t buffer_coords_dim1[] = {4, 3, 3, 3};
  uint64_t buffer_coords_dim2[] = {2, 4, 3, 1};

  void* buffers[] = {buffer_a1,
                     buffer_a2,
                     buffer_var_a2,
                     buffer_a3,
                     buffer_coords_dim1,
                     buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

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
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
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
  uint64_t buffer_coords_dim1[] = {1, 1, 1, 2, 3, 4, 3, 3};
  uint64_t buffer_coords_dim2[] = {1, 2, 4, 3, 1, 2, 3, 4};

  void* buffers[] = {buffer_a1,
                     buffer_a2,
                     buffer_var_a2,
                     buffer_a3,
                     buffer_coords_dim1,
                     buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

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
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
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
  uint64_t buffer_coords_dim1[] = {3, 3, 3, 4};
  uint64_t buffer_coords_dim2[] = {4, 2, 3, 1};

  void* buffers[] = {buffer_a1,
                     buffer_a2,
                     buffer_var_a2,
                     buffer_a3,
                     buffer_coords_dim1,
                     buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

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
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
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

void ConsolidationFx::write_sparse_heterogeneous_full() {
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
  uint64_t buffer_coords_dim1[] = {1, 1, 1, 2, 3, 4, 3, 3};
  uint32_t buffer_coords_dim2[] = {1, 2, 4, 3, 1, 2, 3, 4};

  void* buffers[] = {buffer_a1,
                     buffer_a2,
                     buffer_var_a2,
                     buffer_a3,
                     buffer_coords_dim1,
                     buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1),
      sizeof(buffer_coords_dim2)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_HETEROGENEOUS_ARRAY_NAME, &array);
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
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[5]);
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

void ConsolidationFx::write_sparse_heterogeneous_unordered() {
  // Prepare cell buffers
  int buffer_a1[] = {107, 104, 106, 105};
  uint64_t buffer_a2[] = {0, 3, 4, 5};
  char buffer_var_a2[] = "yyyuwvvvv";
  float buffer_a3[] = {
      107.1f, 107.2f, 104.1f, 104.2f, 106.1f, 106.2f, 105.1f, 105.2f};
  uint64_t buffer_coords_dim1[] = {3, 3, 3, 4};
  uint32_t buffer_coords_dim2[] = {4, 2, 3, 1};

  void* buffers[] = {buffer_a1,
                     buffer_a2,
                     buffer_var_a2,
                     buffer_a3,
                     buffer_coords_dim1,
                     buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1),
      sizeof(buffer_coords_dim2)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_HETEROGENEOUS_ARRAY_NAME, &array);
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
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[5]);
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

void ConsolidationFx::write_sparse_string_full() {
  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 6, 7, 5};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 14, 18};
  char buffer_var_a2[] = "abbcccddddeggghhhhff";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f,
      5.1f,
      5.2f,
  };
  uint64_t buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  char buffer_coords_dim2[] = {'a', 'b', 'd', 'c', 'a', 'c', 'd', 'b'};
  uint64_t buffer_coords_dim2_off[] = {0, 1, 2, 3, 4, 5, 6, 7};

  void* buffers[] = {buffer_a1,
                     buffer_a2,
                     buffer_var_a2,
                     buffer_a3,
                     buffer_coords_dim1,
                     buffer_coords_dim2_off,
                     buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1),
      sizeof(buffer_coords_dim2_off),
      sizeof(buffer_coords_dim2)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_STRING_ARRAY_NAME, &array);
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
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
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
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[4],
      (uint64_t*)buffers[5],
      &buffer_sizes[5],
      buffers[6],
      &buffer_sizes[6]);
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

void ConsolidationFx::write_sparse_string_unordered() {
  // Prepare cell buffers
  int buffer_a1[] = {107, 104, 106, 105};
  uint64_t buffer_a2[] = {0, 3, 4, 5};
  char buffer_var_a2[] = "yyyuwvvvv";
  float buffer_a3[] = {
      107.1f, 107.2f, 104.1f, 104.2f, 106.1f, 106.2f, 105.1f, 105.2f};
  uint64_t buffer_coords_dim1[] = {3, 3, 3, 4};
  char buffer_coords_dim2[] = {'d', 'b', 'c', 'a'};
  uint64_t buffer_coords_dim2_off[] = {0, 1, 2, 3};

  void* buffers[] = {buffer_a1,
                     buffer_a2,
                     buffer_var_a2,
                     buffer_a3,
                     buffer_coords_dim1,
                     buffer_coords_dim2_off,
                     buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1),
      sizeof(buffer_coords_dim2_off),
      sizeof(buffer_coords_dim2)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_STRING_ARRAY_NAME, &array);
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
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
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
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[4],
      (uint64_t*)buffers[5],
      &buffer_sizes[5],
      buffers[6],
      &buffer_sizes[6]);
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

void ConsolidationFx::read_dense_vector(uint64_t timestamp) {
  // Correct buffer
  int c_a[410];
  for (int i = 0; i < 200; ++i) {
    if (timestamp >= 1)
      c_a[i] = i;
    else
      c_a[i] = -2147483648;
  }
  for (int i = 200; i < 250; ++i) {
    if (timestamp >= 2)
      c_a[i] = i;
    else
      c_a[i] = -2147483648;
  }
  for (int i = 250; i < 310; ++i) {
    if (timestamp >= 3)
      c_a[i] = i;
    else
      c_a[i] = -2147483648;
  }
  for (int i = 310; i < 410; ++i) {
    if (timestamp == 0 || timestamp >= 4)
      c_a[i] = i;
    else
      c_a[i] = -2147483648;
  }

  // Set array configuration
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(
      cfg,
      "sm.array.open_timestamp_start",
      std::to_string(timestamp).c_str(),
      &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_array_set_config(ctx_, array, cfg);
  REQUIRE(rc == TILEDB_OK);

  // Open array
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
  for (int i = 0; i < 410; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_config_free(&cfg);
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
  uint64_t subarray[] = {1, 410};
  int a[410];
  uint64_t a_size = sizeof(a);

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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 128;
  uint64_t buffer_a2_val_size = 114;
  uint64_t buffer_a3_size = 128;

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
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 128;
  uint64_t buffer_a2_val_size = 114;
  uint64_t buffer_a3_size = 128;

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
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 128;
  uint64_t buffer_a2_val_size = 114;
  uint64_t buffer_a3_size = 128;

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
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
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
  uint64_t c_buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 4, 4, 3, 3};
  uint64_t c_buffer_coords_dim2[] = {1, 2, 4, 3, 1, 2, 1, 2, 3, 4};

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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 176;
  uint64_t buffer_a2_val_size = 51;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 128;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint64_t*)malloc(buffer_coords_dim2_size);

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
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
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
  CHECK(!memcmp(
      buffer_coords_dim1, c_buffer_coords_dim1, sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      buffer_coords_dim2, c_buffer_coords_dim2, sizeof(c_buffer_coords_dim2)));

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
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
}

void ConsolidationFx::read_sparse_unordered_full() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 105, 5, 6, 7};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 16, 18, 21};
  char c_buffer_a2_val[] = "abbcccddddeuvvvvffggghhhh";
  float c_buffer_a3[] = {0.1f, 0.2f, 1.1f, 1.2f,   2.1f,   2.2f,   3.1f,
                         3.2f, 4.1f, 4.2f, 104.1f, 104.2f, 105.1f, 105.2f,
                         5.1f, 5.2f, 6.1f, 6.2f,   7.1f,   7.2f};
  uint64_t c_buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 4, 4, 3, 3};
  uint64_t c_buffer_coords_dim2[] = {1, 2, 4, 3, 1, 2, 1, 2, 3, 4};

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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 176;
  uint64_t buffer_a2_val_size = 54;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 128;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint64_t*)malloc(buffer_coords_dim2_size);

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
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
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
  CHECK(!memcmp(
      buffer_coords_dim1, c_buffer_coords_dim1, sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      buffer_coords_dim2, c_buffer_coords_dim2, sizeof(c_buffer_coords_dim2)));

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
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
}

void ConsolidationFx::read_sparse_heterogeneous_full_unordered() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 105, 5, 106, 107};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 16, 18, 19};
  char c_buffer_a2_val[] = "abbcccddddeuvvvvffwyyy";
  float c_buffer_a3[] = {0.1f, 0.2f, 1.1f,   1.2f,   2.1f,   2.2f,   3.1f,
                         3.2f, 4.1f, 4.2f,   104.1f, 104.2f, 105.1f, 105.2f,
                         5.1f, 5.2f, 106.1f, 106.2f, 107.1f, 107.2f};
  uint64_t c_buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 4, 4, 3, 3};
  uint32_t c_buffer_coords_dim2[] = {1, 2, 4, 3, 1, 2, 1, 2, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_HETEROGENEOUS_ARRAY_NAME, &array);
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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 176;
  uint64_t buffer_a2_val_size = 51;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 64;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint32_t*)malloc(buffer_coords_dim2_size);

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
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
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
  CHECK(!memcmp(
      buffer_coords_dim1, c_buffer_coords_dim1, sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      buffer_coords_dim2, c_buffer_coords_dim2, sizeof(c_buffer_coords_dim2)));

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
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
}

void ConsolidationFx::read_sparse_heterogeneous_unordered_full() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 105, 5, 6, 7};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 16, 18, 21};
  char c_buffer_a2_val[] = "abbcccddddeuvvvvffggghhhh";
  float c_buffer_a3[] = {0.1f, 0.2f, 1.1f, 1.2f,   2.1f,   2.2f,   3.1f,
                         3.2f, 4.1f, 4.2f, 104.1f, 104.2f, 105.1f, 105.2f,
                         5.1f, 5.2f, 6.1f, 6.2f,   7.1f,   7.2f};
  uint64_t c_buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 4, 4, 3, 3};
  uint32_t c_buffer_coords_dim2[] = {1, 2, 4, 3, 1, 2, 1, 2, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_HETEROGENEOUS_ARRAY_NAME, &array);
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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 176;
  uint64_t buffer_a2_val_size = 54;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 64;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint32_t*)malloc(buffer_coords_dim2_size);

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
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
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
  CHECK(!memcmp(
      buffer_coords_dim1, c_buffer_coords_dim1, sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      buffer_coords_dim2, c_buffer_coords_dim2, sizeof(c_buffer_coords_dim2)));

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
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
}

void ConsolidationFx::read_sparse_string_full_unordered() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 106, 107, 105, 5};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 13, 16, 20};
  char c_buffer_a2_val[] = "abbcccddddeuwyyyvvvvff";
  float c_buffer_a3[] = {0.1f,   0.2f,   1.1f,   1.2f,   2.1f,   2.2f,   3.1f,
                         3.2f,   4.1f,   4.2f,   104.1f, 104.2f, 106.1f, 106.2f,
                         107.1f, 107.2f, 105.1f, 105.2f, 5.1f,   5.2f};
  uint64_t c_buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 3, 3, 4, 4};
  char c_buffer_coords_dim2[] = {
      'a', 'b', 'd', 'c', 'a', 'b', 'c', 'd', 'a', 'b'};
  uint64_t c_buffer_coords_dim2_off[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_STRING_ARRAY_NAME, &array);
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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 176;
  uint64_t buffer_a2_val_size = 51;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 16;
  uint64_t buffer_coords_dim2_off_size = 176;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (char*)malloc(buffer_coords_dim2_size);
  auto buffer_coords_dim2_off = (uint64_t*)malloc(buffer_coords_dim2_off_size);

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
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "d2",
      buffer_coords_dim2_off,
      &buffer_coords_dim2_off_size,
      buffer_coords_dim2,
      &buffer_coords_dim2_size);
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
  CHECK(!memcmp(
      buffer_coords_dim1, c_buffer_coords_dim1, sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      buffer_coords_dim2_off,
      c_buffer_coords_dim2_off,
      sizeof(c_buffer_coords_dim2_off)));
  CHECK(!memcmp(
      buffer_coords_dim2, c_buffer_coords_dim2, sizeof(c_buffer_coords_dim2)));

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
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
  free(buffer_coords_dim2_off);
}

void ConsolidationFx::read_sparse_string_unordered_full() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 6, 7, 105, 5};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 15, 19, 23};
  char c_buffer_a2_val[] = "abbcccddddeuggghhhhvvvvff";
  float c_buffer_a3[] = {0.1f, 0.2f, 1.1f,   1.2f,   2.1f,   2.2f, 3.1f,
                         3.2f, 4.1f, 4.2f,   104.1f, 104.2f, 6.1f, 6.2f,
                         7.1f, 7.2f, 105.1f, 105.2f, 5.1f,   5.2f};
  uint64_t c_buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 3, 3, 4, 4};
  char c_buffer_coords_dim2[] = {
      'a', 'b', 'd', 'c', 'a', 'b', 'c', 'd', 'a', 'b'};
  uint64_t c_buffer_coords_dim2_off[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_STRING_ARRAY_NAME, &array);
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
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 176;
  uint64_t buffer_a2_val_size = 54;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 16;
  uint64_t buffer_coords_dim2_off_size = 176;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (char*)malloc(buffer_coords_dim2_size);
  auto buffer_coords_dim2_off = (uint64_t*)malloc(buffer_coords_dim2_off_size);

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
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "d2",
      buffer_coords_dim2_off,
      &buffer_coords_dim2_off_size,
      buffer_coords_dim2,
      &buffer_coords_dim2_size);
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
  CHECK(!memcmp(
      buffer_coords_dim1, c_buffer_coords_dim1, sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      buffer_coords_dim2_off,
      c_buffer_coords_dim2_off,
      sizeof(c_buffer_coords_dim2_off)));
  CHECK(!memcmp(
      buffer_coords_dim2, c_buffer_coords_dim2, sizeof(c_buffer_coords_dim2)));

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
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
  free(buffer_coords_dim2_off);
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

void ConsolidationFx::consolidate_sparse_heterogeneous() {
  int rc;
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_consolidate(
        ctx_, SPARSE_HETEROGENEOUS_ARRAY_NAME, nullptr);
  } else {
    rc = tiledb_array_consolidate_with_key(
        ctx_,
        SPARSE_HETEROGENEOUS_ARRAY_NAME,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        nullptr);
  }
  REQUIRE(rc == TILEDB_OK);
}

void ConsolidationFx::consolidate_sparse_string() {
  int rc;
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_consolidate(ctx_, SPARSE_STRING_ARRAY_NAME, nullptr);
  } else {
    rc = tiledb_array_consolidate_with_key(
        ctx_,
        SPARSE_STRING_ARRAY_NAME,
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

void ConsolidationFx::remove_sparse_heterogeneous_array() {
  remove_array(SPARSE_HETEROGENEOUS_ARRAY_NAME);
}

void ConsolidationFx::remove_sparse_string_array() {
  remove_array(SPARSE_STRING_ARRAY_NAME);
}

bool ConsolidationFx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, dense",
    "[capi][consolidation][dense]") {
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
    "[capi][consolidation][sparse]") {
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

int ConsolidationFx::get_dir_num(const char* path, void* data) {
  auto data_struct = (ConsolidationFx::get_num_struct*)data;
  auto ctx = data_struct->ctx;
  auto vfs = data_struct->vfs;
  int is_dir;
  int rc = tiledb_vfs_is_dir(ctx, vfs, path, &is_dir);
  CHECK(rc == TILEDB_OK);
  auto meta_dir =
      std::string("/") + tiledb::sm::constants::array_metadata_folder_name;
  if (!tiledb::sm::utils::parse::ends_with(path, meta_dir)) {
    // Ignoring the meta folder
    data_struct->num += is_dir;
  }

  return 1;
}

int ConsolidationFx::get_meta_num(const char* path, void* data) {
  auto data_struct = (ConsolidationFx::get_num_struct*)data;
  if (tiledb::sm::utils::parse::ends_with(
          path, tiledb::sm::constants::meta_file_suffix))
    ++data_struct->num;

  return 1;
}

// Test valid configuration parameters
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, wrong configs",
    "[capi][consolidation][adv][config]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check that there are 4 fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 4);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test whether the min/max parameters work
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #1",
    "[capi][consolidation][adv][adv-1]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test whether >1 steps work
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #2",
    "[capi][consolidation][adv][adv-2]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test a strict consolidation size ratio that prevents consolidation
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #3",
    "[capi][consolidation][adv][adv-3]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 4);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test consolidation size ratio that leads to consolidation of 2 fragments
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #4",
    "[capi][consolidation][adv][adv-4]") {
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
      config, "sm.consolidation.step_size_ratio", "0.3", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test consolidation size ratio 0.5 and two steps
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #5",
    "[capi][consolidation][adv][adv-5]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test consolidation size ratio 0.4 and 10 steps
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #6",
    "[capi][consolidation][adv][adv-6]") {
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
      config, "sm.consolidation.step_size_ratio", "0.4", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Step = 1, Min = 2, Max = 3, Ratio = 0.0
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #7",
    "[capi][consolidation][adv][adv-7]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Step = 1, Min = 2, Max = 8, Ratio = 0.0
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation #8",
    "[capi][consolidation][adv][adv-8]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test selected read/write layout computed internally based
// on fragments whose domain does not coincide the space tiling.
// In this case, the non-empty domain should be expanded.
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, fragments that don't coincide with space tiles "
    "#1",
    "[cap[consolidation][not-coinciding-1]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 3);

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
    "[capi][consolidation][not-coinciding-2]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_with_gaps();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, small buffer size",
    "[capi][consolidation][adv][buffer-size]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, encrypted array",
    "[capi][consolidation][adv][encryption]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, dense array mixing both sparse and "
    "dense fragments",
    "[capi][consolidation][adv][mixed]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_mixed();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test deleting overwritten fragments - no deletion
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, overwritten fragments, no deletion",
    "[capi][consolidation][adv][overwritten-no-del]") {
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 4);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, overwritten fragments, deletion #1",
    "[capi][consolidation][overwritten-del-1]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_del_1();
  read_dense_vector_del_1();

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Configure test
  int rc = tiledb_config_set(config, "sm.consolidation.steps", "2", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "1", &error);
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
  read_dense_vector_del_1();

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_del_1();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, overwritten fragments, deletion #2",
    "[capi][consolidation][adv][overwritten-del-2]") {
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
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "4", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.consolidation.step_max_frags", "4", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.step_size_ratio", "0.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.amplification", "5.0", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_del_2();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, overwritten fragments, deletion #3",
    "[capi][consolidation][adv][overwritten-del-3]") {
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
      tiledb_config_set(config, "sm.consolidation.step_min_frags", "3", &error);
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

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_del_3();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test previous fragments overlap
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, consolidatable #1",
    "[capi][consolidation][adv][consolidatable-1]") {
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
      config, "sm.consolidation.step_size_ratio", "0.75", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_consolidatable_1();

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 3);

  tiledb_config_free(&config);
  remove_dense_vector();
}

// Test amplification
TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test advanced consolidation, consolidatable #2",
    "[capi][consolidation][adv][consolidatable-2]") {
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
  get_num_struct data = {ctx_, vfs_, 0};

  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == ((should_consolidate) ? 3 : 2));

  // Vacuum
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  read_dense_vector_consolidatable_2();

  // Check number of fragments
  data.num = 0;
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == ((should_consolidate) ? 1 : 2));

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation and time traveling",
    "[capi][consolidation][time-traveling]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();

  // Consolidate
  int rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, NULL);
  CHECK(rc == TILEDB_OK);

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 5);

  // Check number of consolidated metadata files
  data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 0);

  // This will properly test time traveling at timestamps within the
  // consolidated fragment interval
  read_dense_vector(1);
  read_dense_vector(2);
  read_dense_vector(3);
  read_dense_vector();

  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, NULL);
  CHECK(rc == TILEDB_OK);
  read_dense_vector();

  // Open array - before timestamp 4, the array will appear empty
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, DENSE_VECTOR_NAME, &array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_config_set(cfg, "sm.array.open_timestamp_start", "1", &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_array_set_config(ctx_, array, cfg);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
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
  CHECK(a_size == 410 * sizeof(int32_t));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_config_free(&cfg);

  // Clean up
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidating fragment metadata",
    "[capi][consolidation][fragment-meta]") {
  remove_dense_vector();
  create_dense_vector();
  write_dense_vector_4_fragments();

  // Configuration for consolidating fragment metadata
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  int rc = tiledb_config_set(
      config, "sm.consolidation.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate - this will consolidate only the fragment metadata
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 4);

  // Check number of consolidated metadata files
  data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  read_dense_vector(1);
  read_dense_vector(2);
  read_dense_vector(3);
  read_dense_vector();

  // Write the dense fragments again
  write_dense_vector_4_fragments(4);

  // Check
  data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 8);

  read_dense_vector(1);
  read_dense_vector(2);
  read_dense_vector(3);
  read_dense_vector(4);
  read_dense_vector(5);
  read_dense_vector(6);
  read_dense_vector(7);
  read_dense_vector(8);

  // Consolidate - this will consolidate only the fragment metadata
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check number of consolidated metadata files
  data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  // This will vacuum fragments - no effect on consolidated fragment metadata
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, NULL);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  // Check
  data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 8);
  read_dense_vector(1);
  read_dense_vector(2);
  read_dense_vector(3);
  read_dense_vector(4);
  read_dense_vector(5);
  read_dense_vector(6);
  read_dense_vector(7);
  read_dense_vector(8);

  // Test wrong vacuum mode
  rc = tiledb_config_set(config, "sm.vacuum.mode", "foo", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_ERR);

  // Vacuum consolidated fragment metadata
  rc = tiledb_config_set(config, "sm.vacuum.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_vacuum(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check number of consolidated metadata files
  data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, DENSE_VECTOR_NAME, &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  // Read
  read_dense_vector(1);
  read_dense_vector(2);
  read_dense_vector(3);
  read_dense_vector(4);
  read_dense_vector(5);
  read_dense_vector(6);
  read_dense_vector(7);
  read_dense_vector(8);

  // Error for wrong consolidation mode
  rc = tiledb_config_set(config, "sm.consolidation.mode", "foo", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, DENSE_VECTOR_NAME, config);
  CHECK(rc == TILEDB_ERR);

  tiledb_config_free(&config);
  remove_dense_vector();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, sparse heterogeneous",
    "[capi][consolidation][sparse][heter]") {
  remove_sparse_heterogeneous_array();
  create_sparse_heterogeneous_array();

  SECTION("- write full, unordered") {
    write_sparse_heterogeneous_full();
    write_sparse_heterogeneous_unordered();
    consolidate_sparse_heterogeneous();
    read_sparse_heterogeneous_full_unordered();
  }

  SECTION("- write unordered, full") {
    write_sparse_heterogeneous_unordered();
    write_sparse_heterogeneous_full();
    consolidate_sparse_heterogeneous();
    read_sparse_heterogeneous_unordered_full();
  }

  SECTION("- write (encrypted) unordered, full") {
    remove_sparse_heterogeneous_array();
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
    create_sparse_heterogeneous_array();
    write_sparse_heterogeneous_unordered();
    write_sparse_heterogeneous_full();
    consolidate_sparse_heterogeneous();
    read_sparse_heterogeneous_unordered_full();
  }

  remove_sparse_heterogeneous_array();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, sparse string",
    "[capi][consolidation][sparse][string]") {
  remove_sparse_string_array();
  create_sparse_string_array();

  SECTION("- write full, unordered") {
    write_sparse_string_full();
    write_sparse_string_unordered();
    consolidate_sparse_string();
    read_sparse_string_full_unordered();
  }

  SECTION("- write unordered, full") {
    write_sparse_string_unordered();
    write_sparse_string_full();
    consolidate_sparse_string();
    read_sparse_string_unordered_full();
  }

  SECTION("- write (encrypted) unordered, full") {
    remove_sparse_string_array();
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
    create_sparse_string_array();
    write_sparse_string_unordered();
    write_sparse_string_full();
    consolidate_sparse_string();
    read_sparse_string_unordered_full();
  }

  remove_sparse_string_array();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidating fragment metadata, sparse string",
    "[capi][consolidation][fragment-meta][sparse][string]") {
  remove_sparse_string_array();
  create_sparse_string_array();
  write_sparse_string_full();
  write_sparse_string_unordered();

  // Validate read
  read_sparse_string_full_unordered();

  // Configuration for consolidating fragment metadata
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  int rc = tiledb_config_set(
      config, "sm.consolidation.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate - this will consolidate only the fragment metadata
  rc = tiledb_array_consolidate(ctx_, SPARSE_STRING_ARRAY_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check number of fragments
  get_num_struct data = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, SPARSE_STRING_ARRAY_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  // Check number of consolidated metadata files
  data = {ctx_, vfs_, 0};
  rc =
      tiledb_vfs_ls(ctx_, vfs_, SPARSE_STRING_ARRAY_NAME, &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  // Validate read
  read_sparse_string_full_unordered();

  // Vacuum consolidated fragment metadata
  rc = tiledb_config_set(config, "sm.vacuum.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_vacuum(ctx_, SPARSE_STRING_ARRAY_NAME, config);
  CHECK(rc == TILEDB_OK);

  // Check number of consolidated metadata files
  data = {ctx_, vfs_, 0};
  rc =
      tiledb_vfs_ls(ctx_, vfs_, SPARSE_STRING_ARRAY_NAME, &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  // Validate read
  read_sparse_string_full_unordered();

  tiledb_config_free(&config);
  remove_sparse_string_array();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidating fragment metadata, sparse string, pass only "
    "context",
    "[capi][consolidation][fragment-meta][sparse][string]") {
  remove_sparse_string_array();
  create_sparse_string_array();
  write_sparse_string_full();
  write_sparse_string_unordered();

  // Validate read
  read_sparse_string_full_unordered();

  // Configuration for consolidating fragment metadata
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  int rc = tiledb_config_set(
      config, "sm.consolidation.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);

  // Consolidate - this will consolidate only the fragment metadata
  rc = tiledb_array_consolidate(ctx, SPARSE_STRING_ARRAY_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check number of fragments
  get_num_struct data = {ctx, vfs_, 0};
  rc = tiledb_vfs_ls(ctx, vfs_, SPARSE_STRING_ARRAY_NAME, &get_dir_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 2);

  // Check number of consolidated metadata files
  data = {ctx, vfs_, 0};
  rc = tiledb_vfs_ls(ctx, vfs_, SPARSE_STRING_ARRAY_NAME, &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  // Validate read
  read_sparse_string_full_unordered();

  // Vacuum consolidated fragment metadata
  rc = tiledb_config_set(config, "sm.vacuum.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Create new context
  tiledb_ctx_free(&ctx);
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);

  rc = tiledb_array_vacuum(ctx, SPARSE_STRING_ARRAY_NAME, nullptr);
  CHECK(rc == TILEDB_OK);

  // Check number of consolidated metadata files
  data = {ctx, vfs_, 0};
  rc = tiledb_vfs_ls(ctx, vfs_, SPARSE_STRING_ARRAY_NAME, &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  // Validate read
  read_sparse_string_full_unordered();

  tiledb_ctx_free(&ctx);
  tiledb_config_free(&config);
  remove_sparse_string_array();
}