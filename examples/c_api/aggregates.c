/**
 * @file   aggregates.c
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
 * to it in global order, and read the data back with aggregates.
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

// Name of array.
const char* array_name = "aggregates_array";

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

  // Prepare data for first write
  int coords_rows_1[] = {1, 2};
  int coords_cols_1[] = {1, 4};
  uint64_t coords_size_1 = sizeof(coords_rows_1);
  int data_1[] = {1, 2};
  uint64_t data_size_1 = sizeof(data_1);

  // Create the query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  tiledb_query_set_data_buffer(ctx, query, "a", data_1, &data_size_1);
  tiledb_query_set_data_buffer(
      ctx, query, "rows", coords_rows_1, &coords_size_1);
  tiledb_query_set_data_buffer(
      ctx, query, "cols", coords_cols_1, &coords_size_1);

  // Submit first query
  tiledb_query_submit(ctx, query);

  // Prepare data for second write
  int coords_rows_2[] = {3};
  int coords_cols_2[] = {3};
  uint64_t coords_size_2 = sizeof(coords_rows_2);
  int data_2[] = {3};
  uint64_t data_size_2 = sizeof(data_2);

  // Reset buffers
  tiledb_query_set_data_buffer(ctx, query, "a", data_2, &data_size_2);
  tiledb_query_set_data_buffer(
      ctx, query, "rows", coords_rows_2, &coords_size_2);
  tiledb_query_set_data_buffer(
      ctx, query, "cols", coords_cols_2, &coords_size_2);

  // Submit second query
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

  // Calculate maximum buffer sizes
  uint64_t count_size = 8;
  uint64_t sum_size = 8;

  // Prepare the vector that will hold the result (1 cells)
  uint64_t* count = (uint64_t*)malloc(count_size);
  int64_t* sum = (int64_t*)malloc(sum_size);

  // Create query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);

  // Get the default channel from the query
  tiledb_query_channel_t* default_channel;
  tiledb_query_get_default_channel(ctx, query, &default_channel);

  // Apply count aggregate
  const tiledb_channel_operation_t* count_aggregate;
  tiledb_aggregate_count_get(ctx, &count_aggregate);
  tiledb_channel_apply_aggregate(
      ctx, default_channel, "Count", count_aggregate);

  // Apply sum aggregate
  const tiledb_channel_operator_t* operator_sum;
  tiledb_channel_operator_sum_get(ctx, &operator_sum);
  tiledb_channel_operation_t* sum_a;
  tiledb_create_unary_aggregate(ctx, query, operator_sum, "a", &sum_a);
  tiledb_channel_apply_aggregate(ctx, default_channel, "Sum", sum_a);

  tiledb_query_set_subarray_t(ctx, query, subarray);
  tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  tiledb_query_set_data_buffer(ctx, query, "Count", count, &count_size);
  tiledb_query_set_data_buffer(ctx, query, "Sum", sum, &sum_size);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Close array
  tiledb_array_close(ctx, array);

  // Print out the results.
  printf("Count has data %i\n", (int)count[0]);
  printf("Sum has data %i\n", (int)sum[0]);

  // Clean up
  free((void*)count);
  free((void*)sum);
  tiledb_subarray_free(&subarray);
  tiledb_aggregate_free(NULL, &sum_a);
  tiledb_query_channel_free(NULL, &default_channel);
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
