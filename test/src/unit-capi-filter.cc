/**
 * @file   unit-capi-filter.cc
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
 * Tests the C API filter.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/constants.h"

TEST_CASE("C API: Test filter set option", "[capi], [filter]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, nullptr);
  REQUIRE(rc == TILEDB_ERR);

  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, nullptr);
  REQUIRE(rc == TILEDB_ERR);

  level = 0;
  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(level == 5);

  tiledb_filter_type_t type;
  rc = tiledb_filter_get_type(ctx, filter, &type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(type == TILEDB_FILTER_BZIP2);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test filter list", "[capi], [filter]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(rc == TILEDB_OK);

  unsigned nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx, filter_list, &nfilters);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(nfilters == 0);

  tiledb_filter_t* filter_out;
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 0, &filter_out);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(filter_out == nullptr);
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 1, &filter_out);
  REQUIRE(rc == TILEDB_ERR);

  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_filter_list_add_filter(ctx, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_filter_list_get_nfilters(ctx, filter_list, &nfilters);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(nfilters == 1);

  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 0, &filter_out);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(filter_out != nullptr);

  level = 0;
  rc = tiledb_filter_get_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(level == 5);

  tiledb_filter_free(&filter_out);

  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list, 1, &filter_out);
  REQUIRE(rc == TILEDB_ERR);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test filter list on attribute", "[capi], [filter]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_set_max_chunk_size(ctx, filter_list, 1024);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_attribute_set_filter_list(ctx, attr, filter_list);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_list_t* filter_list_out;
  rc = tiledb_attribute_get_filter_list(ctx, attr, &filter_list_out);
  REQUIRE(rc == TILEDB_OK);

  unsigned nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx, filter_list_out, &nfilters);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(nfilters == 1);

  tiledb_filter_t* filter_out;
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list_out, 0, &filter_out);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(filter_out != nullptr);

  level = 0;
  rc = tiledb_filter_get_option(
      ctx, filter_out, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(level == 5);

  uint32_t max_chunk_size;
  rc = tiledb_filter_list_get_max_chunk_size(
      ctx, filter_list_out, &max_chunk_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(max_chunk_size == 1024);

  tiledb_filter_free(&filter_out);
  tiledb_filter_list_free(&filter_list_out);

  // Clean up
  tiledb_attribute_free(&attr);
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_ctx_free(&ctx);
}

TEST_CASE(
    "C API: Test conversion filter for double and int attributes",
    "[capi], [filter], [conversion]") {
  int rc = 0;
  const char* array_name = "conversion_filter_array";

  // Create TileDB context
  tiledb_ctx_t* ctx;
  rc = tiledb_ctx_alloc(NULL, &ctx);
  REQUIRE(rc == TILEDB_OK);

  tiledb_vfs_t* vfs;
  rc = tiledb_vfs_alloc(ctx, NULL, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  int dim_domain[] = {1, 4, 1, 4};
  int tile_extents[] = {4, 4};
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx, "rows", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx, "cols", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_add_dimension(ctx, domain, d1);
  tiledb_domain_add_dimension(ctx, domain, d2);

  // Create two fixed-length attributes "a1" and "a2"
  tiledb_attribute_t *a1, *a2;
  rc = tiledb_attribute_alloc(ctx, "a1", TILEDB_FLOAT64, &a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx, "a2", TILEDB_INT32, &a2);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, a1);
  tiledb_array_schema_add_attribute(ctx, array_schema, a2);

  int is_dir = 0;
  rc = tiledb_vfs_is_dir(ctx, vfs, array_name, &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (is_dir) {
    rc = tiledb_vfs_remove_dir(ctx, vfs, array_name);
    REQUIRE(rc == TILEDB_OK);
  }

  // Create array
  rc = tiledb_array_create(ctx, array_name, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_vfs_free(&vfs);
  tiledb_ctx_free(&ctx);

  // Write array

  // Create TileDB context
  rc = tiledb_ctx_alloc(NULL, &ctx);
  REQUIRE(rc == TILEDB_OK);

  // Open array for writing
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx, array_name, &array);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
  int coords_rows[] = {1, 2, 2};
  int coords_cols[] = {1, 4, 3};
  uint64_t coords_size = sizeof(coords_rows);
  double data_a1[] = {10.1, -12.2, 13.3};
  uint64_t data_a1_size = sizeof(data_a1);
  int32_t data_a2[] = {-21, -22, -23};
  uint64_t data_a2_size = sizeof(data_a2);

  // Create the query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  tiledb_query_set_buffer(ctx, query, "a1", data_a1, &data_a1_size);
  tiledb_query_set_buffer(ctx, query, "a2", data_a2, &data_a2_size);
  tiledb_query_set_buffer(ctx, query, "rows", coords_rows, &coords_size);
  tiledb_query_set_buffer(ctx, query, "cols", coords_cols, &coords_size);

  // Submit query
  rc = tiledb_query_submit(ctx, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);

  // Read array
  rc = tiledb_ctx_alloc(NULL, &ctx);
  REQUIRE(rc == TILEDB_OK);

  // Open array for reading
  rc = tiledb_array_alloc(ctx, array_name, &array);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Slice only rows 1, 2 and cols 2, 3, 4
  int subarray[] = {1, 2, 2, 4};

  // Set maximum buffer sizes
  uint64_t read_coords_size = 12;
  uint64_t data1_size = 3 * sizeof(int);
  uint64_t data2_size = 3 * sizeof(double);

  // Prepare the vector that will hold the result
  int* read_coords_rows = (int*)malloc(read_coords_size);
  int* read_coords_cols = (int*)malloc(read_coords_size);
  int* data1 = (int*)malloc(data1_size);
  double* data2 = (double*)malloc(data2_size);

  // Create query
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_set_subarray(ctx, query, subarray);
  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);

  bool var_length = false;

  // Read double attribute to int buffer
  tiledb_query_set_buffer(ctx, query, "a1", data1, &data1_size);
  tiledb_query_set_query_datatype(
      ctx, query, "a1", tiledb_datatype_t::TILEDB_INT32, &var_length);

  // Read int attribute to double buffer
  tiledb_query_set_buffer(ctx, query, "a2", data2, &data2_size);
  tiledb_query_set_query_datatype(
      ctx, query, "a2", tiledb_datatype_t::TILEDB_FLOAT64, &var_length);

  // Set buffer for corordinates
  tiledb_query_set_buffer(
      ctx, query, "rows", read_coords_rows, &read_coords_size);
  tiledb_query_set_buffer(
      ctx, query, "cols", read_coords_cols, &read_coords_size);

  // Submit query
  rc = tiledb_query_submit(ctx, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  REQUIRE(rc == TILEDB_OK);

  // Cell (2,3)
  // data1 converted from 13.3 to 13
  REQUIRE(data1[0] == ((int)data_a1[2]));
  // data2 converted from -23 to -23.0
  REQUIRE(data2[0] == ((double)data_a2[2]));

  // Cell(2,4)
  // data1 converted from -12.2 to -12
  REQUIRE(data1[1] == ((int)data_a1[1]));
  // data2 converted from -22 to -22.0
  REQUIRE(data2[0] == ((double)data_a2[2]));

  // Delete array
  rc = tiledb_vfs_alloc(ctx, NULL, &vfs);
  REQUIRE(rc == TILEDB_OK);
  is_dir = 0;
  rc = tiledb_vfs_is_dir(ctx, vfs, array_name, &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (is_dir) {
    rc = tiledb_vfs_remove_dir(ctx, vfs, array_name);
    REQUIRE(rc == TILEDB_OK);
  }

  // Clean up
  free((void*)read_coords_rows);
  free((void*)read_coords_cols);
  free((void*)data1);
  free((void*)data2);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}
