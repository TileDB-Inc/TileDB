/**
 * @file   query_condition_dense.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * When run, this program will create a dense 1D array with 4 attributes. It
 * will then run queries with different query conditions to demonstrate how
 * query conditions can be used to filter out results in TileDB arrays.
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of array.
const char* array_name = "query_condition_dense_array";
int32_t num_elems = 10;

// Fill values.
int c_fill_value = -1;
float d_fill_value = 0.0;

/**
 * @brief Function to print the values of all the attributes for one
 * index of this array.
 *
 * @param a Attribute a's value.
 * @param b Attribute b's value.
 * @param c Attribute c's value.
 * @param d Attribute d's value.
 */
void print_elem(int* a, char* b_start, int b_len, int32_t c, float d) {
  if (a == NULL) {
    printf("{null, %.*s, %d, %.1f}\n", b_len, b_start, c, d);
  } else {
    printf("{%d, %.*s, %d, %.1f}\n", *a, b_len, b_start, c, d);
  }
}

/**
 * @brief Function to create the TileDB array used in this example.
 * The array will be 1D with size 1 with dimension "index".
 * The bounds on the index will be 0 through 9, inclusive.
 *
 * The array has two attributes. The two attributes are
 *  - "a" (type int)
 *  - "b" (type ASCII string)
 *  - "c" (type int32_t)
 *  - "d" (type float)
 *
 * @param ctx The context.
 */
void create_array(tiledb_ctx_t* ctx) {
  // Creating the dimension and the domain.
  tiledb_dimension_t* dimension;
  int dim_domain[] = {0, num_elems - 1};
  int tile_extents[] = {1};
  tiledb_dimension_alloc(
      ctx, "index", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &dimension);

  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);
  tiledb_domain_add_dimension(ctx, domain, dimension);

  // The array will be dense.
  tiledb_array_schema_t* schema;
  tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &schema);
  tiledb_array_schema_set_domain(ctx, schema, domain);
  tiledb_array_schema_set_cell_order(ctx, schema, TILEDB_ROW_MAJOR);

  // Adding the attributes of the array to the array schema.
  tiledb_attribute_t* a;
  tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);
  tiledb_attribute_set_nullable(ctx, a, true);
  tiledb_attribute_t* b;
  tiledb_attribute_alloc(ctx, "b", TILEDB_STRING_ASCII, &b);
  tiledb_attribute_set_cell_val_num(ctx, b, TILEDB_VAR_NUM);
  tiledb_attribute_t* c;
  tiledb_attribute_alloc(ctx, "c", TILEDB_INT32, &c);
  tiledb_attribute_set_fill_value(ctx, c, &c_fill_value, sizeof(int));
  tiledb_attribute_t* d;
  tiledb_attribute_alloc(ctx, "d", TILEDB_FLOAT32, &d);
  tiledb_attribute_set_fill_value(ctx, d, &d_fill_value, sizeof(float));

  tiledb_array_schema_add_attribute(ctx, schema, a);
  tiledb_array_schema_add_attribute(ctx, schema, b);
  tiledb_array_schema_add_attribute(ctx, schema, c);
  tiledb_array_schema_add_attribute(ctx, schema, d);

  // Create the (empty) array.
  tiledb_array_create(ctx, array_name, schema);

  // Cleanup.
  tiledb_attribute_free(&d);
  tiledb_attribute_free(&c);
  tiledb_attribute_free(&b);
  tiledb_attribute_free(&a);
  tiledb_array_schema_free(&schema);
  tiledb_domain_free(&domain);
  tiledb_dimension_free(&dimension);
}

/**
 * @brief Execute a write on array query_condition_dense array
 * which then stores the following data in the array. The table
 * is organized by dimension/attribute.
 *
 * index |  a   |   b   | c |  d
 * -------------------------------
 *   0   | null | alice | 0 | 4.1
 *   1   | 2    | bob   | 0 | 3.4
 *   2   | null | craig | 0 | 5.6
 *   3   | 4    | dave  | 0 | 3.7
 *   4   | null | erin  | 0 | 2.3
 *   5   | 6    | frank | 0 | 1.7
 *   6   | null | grace | 1 | 3.8
 *   7   | 8    | heidi | 2 | 4.9
 *   8   | null | ivan  | 3 | 3.2
 *   9   | 10   | judy  | 4 | 3.1
 *
 * @param ctx The context.
 */
void write_array(tiledb_ctx_t* ctx) {
  // Create data buffers that store the values to be written in.
  int32_t a_data[] = {0, 2, 0, 4, 0, 6, 0, 8, 0, 10};
  uint64_t a_size = sizeof(a_data);
  uint8_t a_data_validity[] = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
  uint64_t a_validity_size = sizeof(a_data_validity);
  char* b_data = "alicebobcraigdaveerinfrankgraceheidiivanjudy";
  uint64_t b_size = strlen(b_data);
  uint64_t b_data_offsets[] = {0, 5, 8, 13, 17, 21, 26, 31, 36, 40};
  uint64_t b_offsets_size = sizeof(b_data_offsets);
  int32_t c_data[] = {0, 0, 0, 0, 0, 0, 1, 2, 3, 4};
  uint64_t c_size = sizeof(c_data);
  float d_data[] = {4.1, 3.4, 5.6, 3.7, 2.3, 1.7, 3.8, 4.9, 3.2, 3.1};
  uint64_t d_size = sizeof(d_data);

  tiledb_array_t* array_w;
  tiledb_array_alloc(ctx, array_name, &array_w);
  tiledb_array_open(ctx, array_w, TILEDB_WRITE);

  // Execute the write query.
  tiledb_query_t* query_w;
  tiledb_query_alloc(ctx, array_w, TILEDB_WRITE, &query_w);
  tiledb_query_set_layout(ctx, query_w, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(ctx, query_w, "a", a_data, &a_size);
  tiledb_query_set_validity_buffer(
      ctx, query_w, "a", a_data_validity, &a_validity_size);
  tiledb_query_set_data_buffer(ctx, query_w, "b", b_data, &b_size);
  tiledb_query_set_offsets_buffer(
      ctx, query_w, "b", b_data_offsets, &b_offsets_size);
  tiledb_query_set_data_buffer(ctx, query_w, "c", c_data, &c_size);
  tiledb_query_set_data_buffer(ctx, query_w, "d", d_data, &d_size);
  tiledb_query_submit(ctx, query_w);
  tiledb_query_finalize(ctx, query_w);
  tiledb_array_close(ctx, array_w);

  tiledb_query_free(&query_w);
  tiledb_array_free(&array_w);
}

/**
 * @brief Executes the read query for the array created in write_array.
 *
 * @param ctx The context.
 * @param qc The query condition to execute the query with.
 */
void read_array_with_qc(tiledb_ctx_t* ctx, tiledb_query_condition_t* qc) {
  // Create data buffers to read the values into.
  int a_data[10];
  uint64_t a_size = sizeof(a_data);
  uint8_t a_data_validity[10];
  uint64_t a_validity_size = sizeof(a_data_validity);

  // We initialize the buffer b_data to contain 256 characters.
  char b_data[256];
  memset(b_data, 0, 256);
  uint64_t b_size = sizeof(b_data);
  uint64_t b_data_offsets[10];
  uint64_t b_offsets_size = sizeof(b_data_offsets);

  int32_t c_data[10];
  uint64_t c_size = sizeof(c_data);
  float d_data[10];
  uint64_t d_size = sizeof(d_data);

  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_name, &array);
  tiledb_array_open(ctx, array, TILEDB_READ);

  tiledb_subarray_t* subarray;
  tiledb_subarray_alloc(ctx, array, &subarray);
  int subarray_v[] = {0, num_elems - 1};
  tiledb_subarray_set_subarray(ctx, subarray, subarray_v);

  // Execute the read query.
  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(ctx, query, "a", a_data, &a_size);
  tiledb_query_set_validity_buffer(
      ctx, query, "a", a_data_validity, &a_validity_size);
  tiledb_query_set_data_buffer(ctx, query, "b", b_data, &b_size);
  tiledb_query_set_offsets_buffer(
      ctx, query, "b", b_data_offsets, &b_offsets_size);
  tiledb_query_set_data_buffer(ctx, query, "c", c_data, &c_size);
  tiledb_query_set_data_buffer(ctx, query, "d", d_data, &d_size);
  tiledb_query_set_subarray_t(ctx, query, subarray);

  if (qc) {
    tiledb_query_set_condition(ctx, query, qc);
  }

  tiledb_query_submit(ctx, query);

  // Collect the results of the read query. The number of elements
  // the filtered array contains is in num_elems, since the array
  // is dense. The length of the filtered substring of all the
  // data is in b_data, and all the offsets for filtered
  // individual elements are in b_data_offsets.

  // Here we print all the elements that are returned by the query.
  for (int i = 0; i < num_elems; ++i) {
    if (c_data[i] != c_fill_value) {
      uint64_t element_start = b_data_offsets[i];
      uint64_t element_length = (i == num_elems - 1) ?
                                    (b_size / sizeof(char)) - element_start :
                                    b_data_offsets[i + 1] - element_start;
      print_elem(
          a_data_validity[i] ? &a_data[i] : NULL,
          b_data + element_start,
          element_length,
          c_data[i],
          d_data[i]);
    }
  }

  tiledb_query_finalize(ctx, query);
  tiledb_array_close(ctx, array);

  tiledb_subarray_free(&subarray);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

int main() {
  // Create the context.
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, NULL, &vfs);

  int32_t is_dir = 0;
  tiledb_vfs_is_dir(ctx, vfs, array_name, &is_dir);
  if (is_dir) {
    tiledb_vfs_remove_dir(ctx, vfs, array_name);
  }

  // Create and write data to the array.
  create_array(ctx);
  write_array(ctx);

  // Printing the entire array.
  printf("Printing the entire array...\n");
  read_array_with_qc(ctx, NULL);
  printf("\n");

  // Execute a read query with query condition `a = null`.
  printf("Running read query with query condition `a = null`...\n");
  tiledb_query_condition_t* qc;
  tiledb_query_condition_alloc(ctx, &qc);
  tiledb_query_condition_init(ctx, qc, "a", NULL, 0, TILEDB_EQ);
  read_array_with_qc(ctx, qc);
  tiledb_query_condition_free(&qc);
  printf("\n");

  // Execute a read query with query condition `b < "eve"`.
  printf("Running read query with query condition `b < \"eve\"`...\n");
  tiledb_query_condition_t* qc1;
  tiledb_query_condition_alloc(ctx, &qc1);
  const char* eve = "eve";
  tiledb_query_condition_init(ctx, qc1, "b", eve, strlen(eve), TILEDB_LT);
  read_array_with_qc(ctx, qc1);
  printf("\n");

  // Execute a read query with query condition `c >= 1`.
  printf("Running read query with query condition `c >= 1`...\n");
  tiledb_query_condition_t* qc2;
  tiledb_query_condition_alloc(ctx, &qc2);
  int val = 1;
  tiledb_query_condition_init(ctx, qc2, "c", &val, sizeof(int), TILEDB_GE);
  read_array_with_qc(ctx, qc2);
  tiledb_query_condition_free(&qc2);
  printf("\n");

  // Execute a read query with query condition `3.0f <= d AND d <= 4.0f`.
  printf(
      "Running read query with query condition `3.0f <= d AND d <= "
      "4.0f`...\n");
  float arr[] = {3.0, 4.0};
  tiledb_query_condition_t* qc3;
  tiledb_query_condition_alloc(ctx, &qc3);
  tiledb_query_condition_init(ctx, qc3, "d", &arr[0], sizeof(float), TILEDB_GE);
  tiledb_query_condition_t* qc4;
  tiledb_query_condition_alloc(ctx, &qc4);
  tiledb_query_condition_init(ctx, qc4, "d", &arr[1], sizeof(float), TILEDB_LE);
  tiledb_query_condition_t* qc5;
  tiledb_query_condition_alloc(ctx, &qc5);
  tiledb_query_condition_combine(ctx, qc3, qc4, TILEDB_AND, &qc5);
  read_array_with_qc(ctx, qc5);
  tiledb_query_condition_free(&qc3);
  tiledb_query_condition_free(&qc4);
  printf("\n");

  // Execute a read query with query condition `3.0f <= d AND d <= 4.0f AND a !=
  // null AND b < \"eve\"`.
  printf(
      "Running read query with query condition `3.0f <= d AND d <= "
      "4.0f AND a != null AND b < \"eve\"`...\n");
  tiledb_query_condition_t* qc6;
  tiledb_query_condition_alloc(ctx, &qc6);
  tiledb_query_condition_init(ctx, qc6, "a", NULL, 0, TILEDB_NE);
  tiledb_query_condition_t* qc7;
  tiledb_query_condition_alloc(ctx, &qc7);
  tiledb_query_condition_combine(ctx, qc5, qc6, TILEDB_AND, &qc7);
  tiledb_query_condition_t* qc8;
  tiledb_query_condition_alloc(ctx, &qc8);
  tiledb_query_condition_combine(ctx, qc7, qc1, TILEDB_AND, &qc8);
  read_array_with_qc(ctx, qc8);
  tiledb_query_condition_free(&qc1);
  tiledb_query_condition_free(&qc5);
  tiledb_query_condition_free(&qc6);
  tiledb_query_condition_free(&qc7);
  tiledb_query_condition_free(&qc8);
  printf("\n");

  // Cleanup.
  is_dir = 0;
  tiledb_vfs_is_dir(ctx, vfs, array_name, &is_dir);
  if (is_dir) {
    tiledb_vfs_remove_dir(ctx, vfs, array_name);
  }

  tiledb_ctx_free(&ctx);

  return 0;
}
