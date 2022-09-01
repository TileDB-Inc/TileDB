/**
 * @file   variable_length.c
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
 * When run, this program will create a simple 2D dense array with two
 * variable-length attributes, write some data to it, and read a slice of the
 * data back on both attributes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "variable_length_array";

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
  tiledb_attribute_alloc(ctx, "a1", TILEDB_CHAR, &a1);
  tiledb_attribute_set_cell_val_num(ctx, a1, TILEDB_VAR_NUM);
  tiledb_attribute_t* a2;
  tiledb_attribute_alloc(ctx, "a2", TILEDB_INT32, &a2);
  tiledb_attribute_set_cell_val_num(ctx, a2, TILEDB_VAR_NUM);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
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
  const char* a1_data = "abbcccddeeefghhhijjjkklmnoop";
  uint64_t a1_data_size = strlen(a1_data);
  uint64_t a1_off[] = {
      0, 1, 3, 6, 8, 11, 12, 13, 16, 17, 20, 22, 23, 24, 25, 27};
  uint64_t a1_off_size = sizeof(a1_off);
  int a2_data[] = {1, 1, 2, 2,  3,  4,  5,  6,  6,  7,  7,  8,  8,
                   8, 9, 9, 10, 11, 12, 12, 13, 14, 14, 14, 15, 16};
  uint64_t a2_data_size = sizeof(a2_data);
  uint64_t a2_el_off[] = {
      0, 2, 4, 5, 6, 7, 9, 11, 14, 16, 17, 18, 20, 21, 24, 25};
  uint64_t a2_off[16];
  for (int i = 0; i < 16; ++i)
    a2_off[i] = a2_el_off[i] * sizeof(int);
  uint64_t a2_off_size = sizeof(a2_off);

  // Create the query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(ctx, query, "a1", (void*)a1_data, &a1_data_size);
  tiledb_query_set_offsets_buffer(ctx, query, "a1", a1_off, &a1_off_size);
  tiledb_query_set_data_buffer(ctx, query, "a2", (void*)a2_data, &a2_data_size);
  tiledb_query_set_offsets_buffer(ctx, query, "a2", a2_off, &a2_off_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void read_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for writing
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Slice only rows 1, 2 and cols 2, 3, 4
  tiledb_subarray_t* subarray;
  tiledb_subarray_alloc(ctx, array, &subarray);
  int subarray_v[] = {1, 2, 2, 4};
  tiledb_subarray_set_subarray(ctx, subarray, subarray_v);

  // Set maximum buffer sizes
  uint64_t a1_data_size = 34;
  uint64_t a2_data_size = 128;
  uint64_t a1_off_size = 48;
  uint64_t a2_off_size = 48;

  // Prepare the vector that will hold the result
  char* a1_data = (char*)malloc(a1_data_size);
  uint64_t* a1_off = (uint64_t*)malloc(a1_off_size);
  int* a2_data = (int*)malloc(a2_data_size);
  uint64_t* a2_off = (uint64_t*)malloc(a2_off_size);

  // Create query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  tiledb_query_set_subarray_t(ctx, query, subarray);
  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(ctx, query, "a1", (void*)a1_data, &a1_data_size);
  tiledb_query_set_offsets_buffer(ctx, query, "a1", a1_off, &a1_off_size);
  tiledb_query_set_data_buffer(ctx, query, "a2", (void*)a2_data, &a2_data_size);
  tiledb_query_set_offsets_buffer(ctx, query, "a2", a2_off, &a2_off_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Print the results
  int result_num = (int)(a1_off_size / sizeof(uint64_t));  // For clarity
  for (int r = 0; r < result_num; ++r) {
    const char* a1 = &a1_data[a1_off[r]];
    int n;
    if (r == result_num - 1)
      n = (int)(a1_data_size - a1_off[r]);
    else
      n = (int)(a1_off[r + 1] - a1_off[r]);
    printf("a1: %.*s, a2: ", n, a1);
    if (r == result_num - 1)
      n = (int)((a2_data_size - a2_off[r]) / sizeof(int));
    else
      n = (int)(a2_off[r + 1] - a2_off[r]) / sizeof(int);

    int el = (int)(a2_off[r] / sizeof(int));
    for (int i = 0; i < n; ++i)
      printf("%d ", a2_data[el + i]);
    printf("\n");
  }

  // Clean up
  free((void*)a1_data);
  free((void*)a1_off);
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
