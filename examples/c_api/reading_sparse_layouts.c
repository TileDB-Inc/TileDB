/**
 * @file   reading_sparse_layouts.c
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
 * When run, this program will create a simple 2D sparse array, write some data
 * to it, and read a slice of the data back in the layout of the user's choice
 * (passed as an argument to the program: "row", "col", or "global").
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "reading_sparse_layouts_array";

void create_array() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  int dim_domain[] = {1, 4, 1, 4};
  int tile_extents[] = {2, 2};
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

  // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
  int coords_rows[] = {1, 1, 2, 1, 2, 2};
  int coords_cols[] = {1, 2, 2, 4, 3, 4};
  uint64_t coords_size = sizeof(coords_rows);
  int data[] = {1, 2, 3, 4, 5, 6};
  uint64_t data_size = sizeof(data);

  // Create the query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
  tiledb_query_set_data_buffer(ctx, query, "rows", coords_rows, &coords_size);
  tiledb_query_set_data_buffer(ctx, query, "cols", coords_cols, &coords_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Finalize query (IMPORTANT)
  tiledb_query_finalize(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void read_array(tiledb_layout_t layout) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for reading
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Print non-empty domain
  int is_empty = 0;
  int domain[4];
  tiledb_array_get_non_empty_domain(ctx, array, domain, &is_empty);
  printf(
      "Non-empty domain: [%d,%d], [%d,%d]\n",
      domain[0],
      domain[1],
      domain[2],
      domain[3]);

  // Slice only rows 1, 2 and cols 2, 3, 4
  tiledb_subarray_t* subarray;
  tiledb_subarray_alloc(ctx, array, &subarray);
  int subarray_v[] = {1, 2, 2, 4};
  tiledb_subarray_set_subarray(ctx, subarray, subarray_v);

  // Set maximum buffer sizes
  uint64_t coords_size = 24;
  uint64_t data_size = 24;

  // Prepare the vector that will hold the result (6 cells)
  int* coords_rows = (int*)malloc(coords_size);
  int* coords_cols = (int*)malloc(coords_size);
  int* data = (int*)malloc(data_size);

  // Create query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  tiledb_query_set_subarray_t(ctx, query, subarray);
  tiledb_query_set_layout(ctx, query, layout);
  tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
  tiledb_query_set_data_buffer(ctx, query, "rows", coords_rows, &coords_size);
  tiledb_query_set_data_buffer(ctx, query, "cols", coords_cols, &coords_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Print out the results.
  int result_num = (int)(data_size / sizeof(int));
  for (int r = 0; r < result_num; r++) {
    int i = coords_rows[r];
    int j = coords_cols[r];
    int a = data[r];
    printf("Cell (%d, %d) has data %d\n", i, j, a);
  }

  // Clean up
  free((void*)coords_rows);
  free((void*)coords_cols);
  free((void*)data);
  tiledb_subarray_free(&subarray);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

int main(int argc, char* argv[]) {
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

  // Choose a layout (default is row-major)
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;
  if (argc > 1) {
    if (!strcmp(argv[1], "row"))
      layout = TILEDB_ROW_MAJOR;
    else if (!strcmp(argv[1], "col"))
      layout = TILEDB_COL_MAJOR;
    else if (!strcmp(argv[1], "global"))
      layout = TILEDB_GLOBAL_ORDER;
  }

  read_array(layout);
  return 0;
}
