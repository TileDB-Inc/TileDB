/**
 * @file   reading_incomplete.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * This example demonstrates the concept of incomplete read queries
 * for a sparse array with two attributes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "reading_incomplete_array";

void create_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  int dim_domain[] = {1, 4, 1, 4};
  int tile_extents[] = {4, 4};
  tiledb_dimension_t* d1;
  tiledb_dimension_alloc(
      ctx, "rows", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
  tiledb_dimension_t* d2;
  tiledb_dimension_alloc(
      ctx, "cols", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);

  // Create domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);
  tiledb_domain_add_dimension(ctx, domain, d1);
  tiledb_domain_add_dimension(ctx, domain, d2);

  // Create two attributes, the first integer and the second string
  tiledb_attribute_t* a1;
  tiledb_attribute_alloc(ctx, "a1", TILEDB_INT32, &a1);
  tiledb_attribute_t* a2;
  tiledb_attribute_alloc(ctx, "a2", TILEDB_CHAR, &a2);
  tiledb_attribute_set_cell_val_num(ctx, a2, TILEDB_VAR_NUM);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, a1);
  tiledb_array_schema_add_attribute(ctx, array_schema, a2);

  // Create array
  tiledb_array_create(ctx, array_name, array_schema);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_ctx_free(&ctx);
}

void write_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for writing
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
  int coords_rows[] = {1, 2, 2};
  int coords_cols[] = {1, 1, 2};
  uint64_t coords_size = sizeof(coords_rows);
  int a1_data[] = {1, 2, 3};
  uint64_t a1_data_size = sizeof(a1_data);
  const char* a2_data = "abbccc";
  uint64_t a2_data_size = strlen(a2_data);
  uint64_t a2_off[] = {0, 1, 3};
  uint64_t a2_off_size = sizeof(a2_off);

  // Create the query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  tiledb_query_set_data_buffer(ctx, query, "a1", a1_data, &a1_data_size);
  tiledb_query_set_data_buffer(ctx, query, "a2", (void*)a2_data, &a2_data_size);
  tiledb_query_set_offsets_buffer(ctx, query, "a2", a2_off, &a2_off_size);
  tiledb_query_set_data_buffer(ctx, query, "rows", coords_rows, &coords_size);
  tiledb_query_set_data_buffer(ctx, query, "cols", coords_cols, &coords_size);

  // Submit query and finalize
  tiledb_query_submit(ctx, query);
  tiledb_query_finalize(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void reallocate_buffers(
    int** coords_rows,
    int** coords_cols,
    uint64_t* coords_size,
    int** a1_data,
    uint64_t* a1_data_size,
    uint64_t** a2_off,
    uint64_t* a2_off_size,
    char** a2_data,
    uint64_t* a2_data_size) {
  printf("Reallocating...\n");

  // Note: this is a naive reallocation - you should handle
  // reallocation properly depending on your application
  free(*coords_rows);
  free(*coords_cols);
  *coords_size *= 2;
  *coords_rows = (int*)malloc(*coords_size);
  *coords_cols = (int*)malloc(*coords_size);
  free(*a1_data);
  *a1_data_size *= 2;
  *a1_data = (int*)malloc(*a1_data_size);
  free(*a2_off);
  *a2_off_size *= 2;
  *a2_off = (uint64_t*)malloc(*a2_off_size);
  free(*a2_data);
  *a2_data_size *= 2;
  *a2_data = (char*)malloc(*a2_data_size);
}

void print_results(
    const int* coords_rows,
    const int* coords_cols,
    const int* a1_data,
    uint64_t a1_data_size,
    const uint64_t* a2_off,
    const char* a2_data,
    uint64_t a2_data_size) {
  printf("Printing results...\n");

  // Print the results
  int result_num = (int)(a1_data_size / sizeof(int));  // For clarity
  for (int r = 0; r < result_num; ++r) {
    int i = coords_rows[r];
    int j = coords_cols[r];
    int a1 = a1_data[r];
    const char* a2 = &a2_data[a2_off[r]];
    int n;
    if (r == result_num - 1)
      n = (int)(a2_data_size - a2_off[r]);
    else
      n = (int)(a2_off[r + 1] - a2_off[r]);
    printf("Cell (%d, %d) a1: %d, a2: %.*s\n", i, j, a1, n, a2);
  }
}

void read_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for reading
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Read entire array
  tiledb_subarray_t* subarray;
  tiledb_subarray_alloc(ctx, array, &subarray);
  int subarray_v[] = {1, 4, 1, 4};
  tiledb_subarray_set_subarray(ctx, subarray, subarray_v);

  // Prepare buffers such that the results **cannot** fit
  int* coords_rows = (int*)malloc(sizeof(int));
  int* coords_cols = (int*)malloc(sizeof(int));
  uint64_t coords_size = sizeof(int);
  int* a1_data = (int*)malloc(sizeof(int));
  uint64_t a1_data_size = sizeof(int);
  char* a2_data = (char*)malloc(sizeof(char));
  uint64_t a2_data_size = sizeof(char);
  uint64_t* a2_off = (uint64_t*)malloc(sizeof(uint64_t));
  uint64_t a2_off_size = sizeof(uint64_t);

  // Allocated sizes
  uint64_t coords_alloced_size = coords_size;
  uint64_t a1_data_alloced_size = a1_data_size;
  uint64_t a2_off_alloced_size = a2_off_size;
  uint64_t a2_data_alloced_size = a2_data_size;

  // Create query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  tiledb_query_set_subarray_t(ctx, query, subarray);
  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(ctx, query, "a1", a1_data, &a1_data_size);
  tiledb_query_set_data_buffer(ctx, query, "a2", a2_data, &a2_data_size);
  tiledb_query_set_offsets_buffer(ctx, query, "a2", a2_off, &a2_off_size);
  tiledb_query_set_data_buffer(ctx, query, "rows", coords_rows, &coords_size);
  tiledb_query_set_data_buffer(ctx, query, "cols", coords_cols, &coords_size);

  // Create a loop
  tiledb_query_status_t status;
  do {
    // Submit query and get status
    tiledb_query_submit(ctx, query);
    tiledb_query_get_status(ctx, query, &status);

    // If any results were retrieved, parse and print them
    int result_num = (int)(a1_data_size / sizeof(int));
    if (status == TILEDB_INCOMPLETE && result_num == 0) {  // VERY IMPORTANT!!
      reallocate_buffers(
          &coords_rows,
          &coords_cols,
          &coords_alloced_size,
          &a1_data,
          &a1_data_alloced_size,
          &a2_off,
          &a2_off_alloced_size,
          &a2_data,
          &a2_data_alloced_size);
      coords_size = coords_alloced_size;
      a1_data_size = a1_data_alloced_size;
      a2_data_size = a2_data_alloced_size;
      a2_off_size = a2_off_alloced_size;
      tiledb_query_set_data_buffer(ctx, query, "a1", a1_data, &a1_data_size);
      tiledb_query_set_data_buffer(ctx, query, "a2", a2_data, &a2_data_size);
      tiledb_query_set_offsets_buffer(ctx, query, "a2", a2_off, &a2_off_size);
      tiledb_query_set_data_buffer(
          ctx, query, "rows", coords_rows, &coords_size);
      tiledb_query_set_data_buffer(
          ctx, query, "cols", coords_cols, &coords_size);
    } else if (result_num > 0) {
      print_results(
          coords_rows,
          coords_cols,
          a1_data,
          a1_data_size,
          a2_off,
          a2_data,
          a2_data_size);
    }
  } while (status == TILEDB_INCOMPLETE);

  // Handle error
  if (status == TILEDB_FAILED) {
    printf("Error in reading\n");
    return;
  }

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  free((void*)coords_rows);
  free((void*)coords_cols);
  free((void*)a1_data);
  free((void*)a2_data);
  free((void*)a2_off);
  tiledb_subarray_free(&subarray);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

int main() {
  // Get object type
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);
  tiledb_object_t type;
  tiledb_object_type(ctx, array_name, &type);
  tiledb_ctx_free(&ctx);

  if (type != TILEDB_ARRAY) {
    create_array();
    write_array();
  }

  read_array();

  return 0;
}
