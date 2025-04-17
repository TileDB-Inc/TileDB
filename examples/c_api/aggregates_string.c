/**
 * @file   aggregates_string.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 * string type, and the other an integer. The program will write some data to
 * it, and run a query to select coordinates and compute the min and max values
 * of the string dimension using aggregates.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

#include "tiledb_examples.h"

// Name of array.
const char* array_name = "aggregates_string_array";

void create_array(tiledb_ctx_t* ctx) {
  // The array will be 2d array with dimensions "rows" and "cols"
  // "rows" is a string dimension type, so the domain and extent is null
  int dim_domain[] = {1, 4};
  int tile_extents[] = {4};
  tiledb_dimension_t* d1;
  tiledb_dimension_alloc(ctx, "rows", TILEDB_STRING_ASCII, NULL, NULL, &d1);
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
}

void write_array(tiledb_ctx_t* ctx) {
  // Open array for writing
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  // Prepare data for first write
  char coords_rows_1[] = {"barbazcorgefoo"};
  uint64_t coords_rows_size_1 = sizeof(coords_rows_1);
  uint64_t coords_rows_offsets_1[] = {0, 3, 6, 11};
  uint64_t coords_rows_offsets_size_1 = sizeof(coords_rows_offsets_1);
  int coords_cols_1[] = {1, 2, 3, 4};
  uint64_t coords_cols_size_1 = sizeof(coords_cols_1);
  int data_1[] = {3, 3, 5, 3};
  uint64_t data_size_1 = sizeof(data_1);

  // Create first query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);

  // Global order enables writes in stages to a single fragment
  // but requires input to match global order
  tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);

  // Prepare data for first write
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query, "a", data_1, &data_size_1));
  TRY(ctx,
      tiledb_query_set_data_buffer(
          ctx, query, "rows", coords_rows_1, &coords_rows_size_1));
  TRY(ctx,
      tiledb_query_set_offsets_buffer(
          ctx,
          query,
          "rows",
          coords_rows_offsets_1,
          &coords_rows_offsets_size_1));
  TRY(ctx,
      tiledb_query_set_data_buffer(
          ctx, query, "cols", coords_cols_1, &coords_cols_size_1));

  // Submit first query
  TRY(ctx, tiledb_query_submit(ctx, query));

  // Prepare data for second write
  char coords_rows_2[] = {"garplygraultgubquux"};
  uint64_t coords_rows_size_2 = sizeof(coords_rows_2);
  uint64_t coords_rows_offsets_2[] = {0, 6, 12, 15};
  uint64_t coords_rows_offsets_size_2 = sizeof(coords_rows_offsets_2);
  int coords_cols_2[] = {1, 2, 3, 4};
  uint64_t coords_cols_size_2 = sizeof(coords_cols_2);
  int data_2[] = {6, 6, 3, 4};
  uint64_t data_size_2 = sizeof(data_2);

  // Reset buffers
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query, "a", data_2, &data_size_2));
  TRY(ctx,
      tiledb_query_set_data_buffer(
          ctx, query, "rows", coords_rows_2, &coords_rows_size_2));
  TRY(ctx,
      tiledb_query_set_offsets_buffer(
          ctx,
          query,
          "rows",
          coords_rows_offsets_2,
          &coords_rows_offsets_size_2));
  TRY(ctx,
      tiledb_query_set_data_buffer(
          ctx, query, "cols", coords_cols_2, &coords_cols_size_2));

  // Submit second query
  TRY(ctx, tiledb_query_submit(ctx, query));

  // Finalize query (IMPORTANT)
  TRY(ctx, tiledb_query_finalize(ctx, query));

  // Close array
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void print_cells(
    uint64_t result_num,
    uint64_t* rows_offsets,
    uint64_t rows_data_size,
    char* rows_data,
    int32_t* cols_data,
    int32_t* a_data) {
  for (uint64_t r = 0; r < result_num; r++) {
    // For strings we must compute the length based on the offsets
    uint64_t row_start = rows_offsets[r];
    uint64_t row_end =
        r == result_num - 1 ? rows_data_size : rows_offsets[r + 1];
    const int row_value_size = row_end - row_start;
    const char* row_value = &rows_data[row_start];

    const int32_t col_value = cols_data[r];
    const int32_t a_value = a_data[r];
    printf(
        "Cell (%.*s, %i) has data %d\n",
        row_value_size,
        row_value,
        col_value,
        a_value);
  }
}

void read_array(tiledb_ctx_t* ctx) {
  // Open array for reading
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  // Calculate maximum buffer sizes
#define VAR_BUFFER_SIZE 64
  uint64_t max_size =
      VAR_BUFFER_SIZE;  // variable-length result has unknown size
  uint64_t max_offsets_size = sizeof(uint64_t);
  uint64_t min_size =
      VAR_BUFFER_SIZE;  // variable-length result has unknown size
  uint64_t min_offsets_size = sizeof(uint64_t);

  // Aggregate result buffers (1 cell each of unknown size)
  char max[VAR_BUFFER_SIZE];
  uint64_t max_offsets[1];
  char min[VAR_BUFFER_SIZE];
  uint64_t min_offsets[1];
#undef VAR_BUFFER_SIZE

  // Attribute/dimension buffers
  // (unknown number of cells, buffer sizes are estimates)
#define NUM_CELLS 2
  char rows_data[NUM_CELLS * 16];
  uint64_t rows_data_size = sizeof(rows_data);
  uint64_t rows_offsets[NUM_CELLS];
  uint64_t rows_offsets_size = sizeof(rows_offsets);
  int32_t cols_data[NUM_CELLS];
  uint64_t cols_size = sizeof(cols_data);
  int32_t a_data[NUM_CELLS];
  uint64_t a_size = sizeof(a_data);
#undef NUM_CELLS

  // Create query
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);

  // Query cells with a >= 4
  tiledb_query_condition_t* qc;
  tiledb_query_condition_alloc(ctx, &qc);
  const int32_t a_lower_bound = 4;
  tiledb_query_condition_init(
      ctx, qc, "a", &a_lower_bound, sizeof(int32_t), TILEDB_GE);
  tiledb_query_set_condition(ctx, query, qc);

  // Add attribute/dimension result buffers
  TRY(ctx,
      tiledb_query_set_data_buffer(
          ctx, query, "rows", &rows_data[0], &rows_data_size));
  TRY(ctx,
      tiledb_query_set_offsets_buffer(
          ctx, query, "rows", &rows_offsets[0], &rows_offsets_size));
  TRY(ctx,
      tiledb_query_set_data_buffer(
          ctx, query, "cols", &cols_data[0], &cols_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query, "a", &a_data[0], &a_size));

  // Get the default channel from the query
  tiledb_query_channel_t* default_channel;
  tiledb_query_get_default_channel(ctx, query, &default_channel);

  // Apply min aggregate
  tiledb_channel_operation_t* min_rows;
  TRY(ctx,
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_min, "rows", &min_rows));
  TRY(ctx,
      tiledb_channel_apply_aggregate(
          ctx, default_channel, "Min(rows)", min_rows));

  // Apply max aggregate
  tiledb_channel_operation_t* max_rows;
  TRY(ctx,
      tiledb_create_unary_aggregate(
          ctx, query, tiledb_channel_operator_max, "rows", &max_rows));
  TRY(ctx,
      tiledb_channel_apply_aggregate(
          ctx, default_channel, "Max(rows)", max_rows));

  TRY(ctx, tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED));
  TRY(ctx,
      tiledb_query_set_data_buffer(ctx, query, "Min(rows)", min, &min_size));
  TRY(ctx,
      tiledb_query_set_offsets_buffer(
          ctx, query, "Min(rows)", min_offsets, &min_offsets_size));
  TRY(ctx,
      tiledb_query_set_data_buffer(ctx, query, "Max(rows)", max, &max_size));
  TRY(ctx,
      tiledb_query_set_offsets_buffer(
          ctx, query, "Max(rows)", max_offsets, &max_offsets_size));

  // Submit query
  TRY(ctx, tiledb_query_submit(ctx, query));

  tiledb_query_status_t status;
  TRY(ctx, tiledb_query_get_status(ctx, query, &status));
  while (status == TILEDB_INCOMPLETE) {
    const uint64_t num_results = a_size / sizeof(int32_t);

    // NB: this is not generically a valid assertion
    // (see reading_incomplete.c)
    // but is true by construction in this example
    assert(num_results);

    print_cells(
        num_results,
        rows_offsets,
        rows_data_size,
        rows_data,
        cols_data,
        a_data);

    TRY(ctx, tiledb_query_submit(ctx, query));
    TRY(ctx, tiledb_query_get_status(ctx, query, &status));
  }

  // Close array
  tiledb_array_close(ctx, array);

  // Print out the final results.
  print_cells(
      a_size / sizeof(int32_t),
      rows_offsets,
      rows_data_size,
      rows_data,
      cols_data,
      a_data);
  printf(
      "Min has data %.*s\n",
      (int)(min_size - min_offsets[0]),
      &min[min_offsets[0]]);
  printf(
      "Max has data %.*s\n",
      (int)(max_size - max_offsets[0]),
      &max[max_offsets[0]]);

  // Clean up
  tiledb_aggregate_free(ctx, &min_rows);
  tiledb_aggregate_free(ctx, &max_rows);
  tiledb_query_channel_free(ctx, &default_channel);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

int main() {
  // Get object type
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);
  tiledb_object_t type;
  tiledb_object_type(ctx, array_name, &type);

  if (type != TILEDB_ARRAY) {
    create_array(ctx);
    write_array(ctx);
  }

  read_array(ctx);
  tiledb_ctx_free(&ctx);
  return 0;
}
