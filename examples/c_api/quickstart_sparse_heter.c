/**
 * @file   quickstart_sparse_heter.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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
 * When run, this program will create a 2D sparse array with each dimension
 * having separate datatypes, similar to a dataframe. It will write some data to
 * it, and read a slice of the data back.
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "quickstart_sparse_heter_array";

void check_report_error_from(int32_t tiledb_status_val, char *who) {
   if(tiledb_status_val != TILEDB_OK) {
      printf("error %d%s%s\n",tiledb_status_val, who ? " from " : "", who ? who : "");
   }
}

void check_report_error(int32_t tiledb_status_val) {
   check_report_error_from(tiledb_status_val, "");
}

void create_array() {
  int32_t apistatus;
  // Create TileDB context
  tiledb_ctx_t* ctx;
  apistatus = tiledb_ctx_alloc(NULL, &ctx);
  check_report_error_from(apistatus, "tiledb_ctx_alloc");

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  int32_t dim_domain[] = {1, 4, 1, 4};
  float dim_float_domain[] = {1, 4, 1, 4};
  int32_t tile_int_extents[] = {4, 4};
  float tile_float_extents[] = {4, 4};
  tiledb_dimension_t* d1;
  apistatus = tiledb_dimension_alloc (ctx, "rows", TILEDB_INT32, &dim_domain[0], &tile_int_extents[0], &d1 );
  check_report_error_from  ( apistatus, "tiledb_dimension_alloc"  );

  tiledb_dimension_t* d2;
  //note, type of variable needs to match TILDB_type specified
  apistatus = tiledb_dimension_alloc (ctx, "cols", TILEDB_FLOAT32, &dim_float_domain[2], &tile_float_extents[1], &d2);
  check_report_error_from(  apistatus, "tiledb_dimension_alloc" );

  // Create domain
  tiledb_domain_t* domain;
  apistatus = tiledb_domain_alloc(ctx, &domain);
  check_report_error_from(  apistatus, "tiledb_domain_alloc" );
  apistatus = tiledb_domain_add_dimension(ctx, domain, d1);
  check_report_error_from(  apistatus, "tiledb_domain_add_dimension" );
  apistatus = tiledb_domain_add_dimension(ctx, domain, d2);
  check_report_error_from(  apistatus, "tiledb_domain_dimension" );

  // Create a single attribute "a" so each (i,j) cell can store an integer
  tiledb_attribute_t* a;
  apistatus = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);
  check_report_error_from(  apistatus, "tiledb_attribute_alloc" );

  // Create array schema
  tiledb_array_schema_t* array_schema;
  apistatus = tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  check_report_error_from(  apistatus, "tiledb_array_schema_alloc" );

  apistatus = tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  check_report_error_from(  apistatus, "tiledb_array_schema_set_cell" );

  apistatus = tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  check_report_error_from(  apistatus, "tiledb_schema_set_tile_order" );

  apistatus = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  check_report_error_from(  apistatus, "tiledb_schema_set" );

  apistatus = tiledb_array_schema_add_attribute(ctx, array_schema, a);
  check_report_error_from(  apistatus, "tiledb_schema_add_attribute" );


  // Create array
  apistatus = tiledb_array_create(ctx, array_name, array_schema);
  check_report_error_from(  apistatus, "tiledb_array_create" );

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_ctx_free(&ctx);
}

void write_array() {
  int32_t apistatus;

  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Open array for writing
  tiledb_array_t* array;
  apistatus = tiledb_array_alloc(ctx, array_name, &array);
  check_report_error_from(  apistatus, "tiledb_array_alloc" );
  apistatus = tiledb_array_open(ctx, array, TILEDB_WRITE);
  check_report_error_from(  apistatus, "tiledb_array_open" );

  // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
  int32_t  rows[] = {1, 2, 2};
  float    cols[] = {1.1, 1.2, 1.3};
  uint64_t row_coords_size = sizeof(rows);
  uint64_t col_coords_size = sizeof(cols);
  int32_t  data[] = {4, 5, 6, 7, 8, 9, 10, 11};
  uint64_t data_size = sizeof(data);

  // Create the query
  tiledb_query_t* query;
  apistatus = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  check_report_error_from(  apistatus, "tiledb_query_alloc" );
  apistatus = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  if(apistatus != TILEDB_OK) printf("error %d\n",apistatus) ;
  check_report_error_from(  apistatus, "tiledb_query_set_alloc" );
  apistatus = tiledb_query_set_buffer(ctx, query, "a", data, &data_size);
  check_report_error_from(  apistatus, "tiledb_query_set_buffer" );
  apistatus = tiledb_query_set_buffer(ctx, query, "rows", rows, &row_coords_size);
  check_report_error_from(  apistatus, "tiledb_query_set_buffer" );
  apistatus = tiledb_query_set_buffer(ctx, query, "cols", cols, &col_coords_size);
  check_report_error_from(  apistatus, "tiledb_query_set_buffer" );

  // Submit query
  apistatus = tiledb_query_submit(ctx, query);
  check_report_error_from(  apistatus, "tiledb_query_submit" );

  // Close array
  apistatus = tiledb_array_close(ctx, array);
  check_report_error_from(  apistatus, "tiledb_array_close" );

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void read_array() {
  int32_t apistatus;
  // Create TileDB context
  tiledb_ctx_t* ctx;
  apistatus = tiledb_ctx_alloc(NULL, &ctx);
  check_report_error_from(  apistatus, "tiledb_ctx_alloc" );

  // Open array for reading
  tiledb_array_t* array;
  apistatus = tiledb_array_alloc(ctx, array_name, &array);
  check_report_error_from(  apistatus, "tiledb_array_alloc" );
  apistatus = tiledb_array_open(ctx, array, TILEDB_READ);
  check_report_error_from(  apistatus, "tiledb_array_open" );


  // Set maximum buffer sizes
  uint64_t row_coords_size = sizeof(int32_t)*3;
  uint64_t cols_coords_size = sizeof(float)*3;
  uint64_t data_size = sizeof(int32_t)*3;

  // Prepare the vector that will hold the result
  int32_t* rows = (int32_t*)malloc(row_coords_size);
  float*   cols = (float*)malloc(cols_coords_size);
  int32_t* data = (int32_t*)malloc(data_size);

  // Create query
  tiledb_query_t* query;
  apistatus = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  check_report_error_from(  apistatus, "tiledb_query_alloc" );

  apistatus = tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  check_report_error_from(  apistatus, "tiledb_query_set_layout" );
  apistatus = tiledb_query_set_buffer(ctx, query, "a", data, &data_size);
  check_report_error_from(  apistatus, "tiledb_query_set_buffer" );
  apistatus = tiledb_query_set_buffer(ctx, query, "rows", rows, &row_coords_size);
  check_report_error_from(  apistatus, "tiledb_query_set_buffer" );
  apistatus = tiledb_query_set_buffer(ctx, query, "cols", cols, &cols_coords_size);
  check_report_error_from(  apistatus, "tiledb_query_set_buffer" );

  int row_start = 1, row_end = 2;
  float cols_start = 1, cols_end = 2; //note 'float', type needs to match for item being added
  apistatus = tiledb_query_add_range(ctx, query, 0, &row_start, &row_end, NULL); //'0' dimension, rows
  check_report_error_from(  apistatus, "tiledb_query_add_range" );
  apistatus = tiledb_query_add_range(ctx, query, 1, &cols_start, &cols_end, NULL); //'1' dimension, cols
  check_report_error_from(  apistatus, "tiledb_query_add_range" );

  // Submit query
  apistatus = tiledb_query_submit(ctx, query);
  check_report_error_from(  apistatus, "tiledb_query_submit" );

  // Close array
  apistatus = tiledb_array_close(ctx, array);
  check_report_error_from(  apistatus, "tiledb_array_close" );

  // Print out the results.
  uint64_t result_num = (uint64_t)(data_size / sizeof(int32_t));
  for (uint64_t r = 0; r < result_num; r++) {
    int32_t i = rows[r];
    float j = cols[r];
    int32_t a = data[r];
    printf("Cell (%d, %g) has data %d\n", i, j, a);
  }

  // Clean up
  free((void*)rows);
  free((void*)cols);
  free((void*)data);
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
