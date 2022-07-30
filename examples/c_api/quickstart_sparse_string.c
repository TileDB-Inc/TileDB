/**
 * @file   quickstart_sparse_string.c
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
 * When run, this program will create a 2D sparse array with one dimension a
 * string type, and the other an integer. This models closely what a dataframe
 * looks like. The program will write some data to it, and read a slice of the
 * data back.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "quickstart_sparse_string_array";

void create_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // The array will be 2d array with dimensions "rows" and "cols"
  // "rows" is a string dimension type, so the domain and extent is null
  int32_t dim_domain[] = {1, 4};
  int32_t tile_extents[] = {4};
  tiledb_dimension_t* d1;
  tiledb_dimension_alloc(ctx, "rows", TILEDB_STRING_ASCII, 0, 0, &d1);
  tiledb_dimension_t* d2;
  tiledb_dimension_alloc(
      ctx, "cols", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d2);

  // Create domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);
  tiledb_domain_add_dimension(ctx, domain, d1);
  tiledb_domain_add_dimension(ctx, domain, d2);

  // Create a single attribute "a" so each (i,j) cell can store an integer
  tiledb_attribute_t* a;
  tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, a);

  // Create array
  tiledb_array_create(ctx, array_name, array_schema);

  // Clean up
  tiledb_attribute_free(&a);
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

  // Write some simple data to cells ("a", 1), ("bb", 4) and ("c", 3).
  char rows[] = {'a', 'b', 'b', 'c'};
  uint64_t rows_size = sizeof(rows);
  uint64_t rows_offsets[] = {0, 1, 3};
  uint64_t rows_offsets_size = sizeof(rows_offsets);
  int32_t cols[] = {1, 4, 3};
  uint64_t col_coords_size = sizeof(cols);
  int32_t data[] = {5, 6, 7};
  uint64_t data_size = sizeof(data);

  // Create the query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
  tiledb_query_set_data_buffer(ctx, query, "rows", rows, &rows_size);
  tiledb_query_set_offsets_buffer(
      ctx, query, "rows", rows_offsets, &rows_offsets_size);
  tiledb_query_set_data_buffer(ctx, query, "cols", cols, &col_coords_size);

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

  // Open array for reading
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Slice only rows 1, 2 and cols 2, 3, 4

  // Set maximum buffer sizes
  uint64_t cols_size = sizeof(int32_t) * 3;
  uint64_t rows_size = sizeof(char) * 4;
  uint64_t data_size = sizeof(int32_t) * 3;  //*7; //3;
  uint64_t rows_offsets[3];                  // = {0, 1, 3};
  uint64_t rows_offsets_size = sizeof(rows_offsets);

  // Prepare the vector that will hold the result
  char* rows = (char*)malloc(rows_size);
  int32_t* cols = (int32_t*)malloc(cols_size);
  int32_t* data = (int32_t*)malloc(data_size);

  // Create query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);

  tiledb_subarray_t* subarray;
  tiledb_subarray_alloc(ctx, array, &subarray);

  // Slice only rows "bb", "c" and cols 3, 4
  int cols_start = 2, cols_end = 4;
  tiledb_subarray_add_range_var(
      ctx, subarray, 0, "a", strlen("a"), "c", strlen("c"));
  tiledb_subarray_add_range(
      ctx, subarray, 1, &cols_start, &cols_end, NULL);  //'1', cols
  tiledb_query_set_subarray_t(ctx, query, subarray);

  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);

  tiledb_query_set_data_buffer(ctx, query, "rows", rows, &rows_size);
  tiledb_query_set_offsets_buffer(
      ctx, query, "rows", rows_offsets, &rows_offsets_size);
  tiledb_query_set_data_buffer(ctx, query, "cols", cols, &cols_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Print out the results.
  uint64_t result_num = (uint64_t)(data_size / sizeof(int32_t));
  for (uint64_t r = 0; r < result_num; r++) {
    // For strings we must compute the length based on the offsets
    uint64_t row_start = rows_offsets[r];
    uint64_t row_end =
        r == result_num - 1 ? result_num : rows_offsets[r + 1] - 1;
    char* i = (char*)(rows + row_start);
    int szdata = row_end - row_start + 1;
    int32_t j = cols[r];
    char a = data[r];
    printf("Cell (%*.*s, %i) has data %d\n", szdata, szdata, i, j, a);
  }

  // Clean up
  free((void*)rows);
  free((void*)cols);
  free((void*)data);
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
