/**
 * @file   query_condition_sparse.c
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
 * This example demonstrates using the experimental `tiledb_query_add_predicate`
 * API to add one or more text predicates to a query. This API parses a SQL
 * predicate and uses it to filter results inside of the storage engine
 * before returning them to the user.
 *
 * The array used in this example is identical to that of the
 * `query_condition_sparse` example. The first group of predicates which
 * run are text equivalents of the predicates in that example, and produce
 * the same results.
 *
 * This example also has additional queries which use predicates which
 * combine dimensions and attributes, highlighting a capability which
 * cannot be replicated by just subarrays and query conditions.
 */

#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

// Name of array.
const char* array_name = "array_query_add_predicate";

#define TRY(ctx, action)                 \
  do {                                   \
    const capi_return_t r = (action);    \
    if (r != TILEDB_OK) {                \
      return print_last_error((ctx), r); \
    }                                    \
  } while (0)

#define RETURN_IF_NOT_OK(r)     \
  do {                          \
    const int32_t status = (r); \
    if (status != TILEDB_OK) {  \
      return status;            \
    }                           \
  } while (0)

/**
 * Enumeration variants
 */
static const char* const states[] = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "etc"};

/**
 * @brief Function to print the values of all the attributes for one
 * index of this array.
 *
 * @param a Attribute a's value.
 * @param b Attribute b's value.
 * @param c Attribute c's value.
 * @param d Attribute d's value.
 */
void print_elem(
    int* a, char* b_start, int b_len, int32_t c, float d, uint8_t* e) {
  char print_a[8], print_e[32];
  if (a == NULL) {
    strcpy(&print_a[0], "null");
  } else {
    sprintf(&print_a[0], "%d", *a);
  }
  if (e == NULL) {
    strcpy(&print_e[0], "null");
  } else if (*e < sizeof(states) / sizeof(const char*)) {
    strcpy(&print_e[0], states[*e]);
  } else {
    sprintf(&print_e[0], "(invalid key %hhu)", *e);
  }

  printf("{%s, %.*s, %d, %.1f, %s}\n", print_a, b_len, b_start, c, d, print_e);
}

/**
 * Retrieve and print the last error.
 *
 * @param ctx The context object to get the error from.
 */
int32_t print_last_error(tiledb_ctx_t* ctx, int32_t rc) {
  tiledb_error_t* err = NULL;
  tiledb_ctx_get_last_error(ctx, &err);
  if (err == NULL) {
    fprintf(stderr, "TileDB Error: Error code returned but no error found.");
    return rc;
  }
  const char* msg = NULL;
  tiledb_error_message(err, &msg);
  if (msg == NULL) {
    fprintf(stderr, "TileDB Error");
  } else {
    fprintf(stderr, "%s\n", msg);
  }
  return rc;
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
int32_t create_array(tiledb_ctx_t* ctx) {
  // Creating the dimension and the domain.
  tiledb_dimension_t* dimension;
  int dim_domain[] = {0, 9};
  int tile_extents[] = {1};
  TRY(ctx,
      tiledb_dimension_alloc(
          ctx,
          "index",
          TILEDB_INT32,
          &dim_domain[0],
          &tile_extents[0],
          &dimension));

  tiledb_domain_t* domain;
  TRY(ctx, tiledb_domain_alloc(ctx, &domain));
  TRY(ctx, tiledb_domain_add_dimension(ctx, domain, dimension));

  // The array will be sparse.
  tiledb_array_schema_t* schema;
  TRY(ctx, tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &schema));
  TRY(ctx, tiledb_array_schema_set_domain(ctx, schema, domain));
  TRY(ctx, tiledb_array_schema_set_cell_order(ctx, schema, TILEDB_ROW_MAJOR));

  // Create enumeration
  size_t states_size = 0;
  for (uint64_t i = 0; i < sizeof(states) / sizeof(const char*); i++) {
    states_size += strlen(states[i]);
  }
  const uint64_t states_offsets_size =
      (sizeof(states) / sizeof(const char*)) * sizeof(uint64_t);

  char* states_values = (char*)(malloc(states_size));
  uint64_t* states_offsets = (uint64_t*)(malloc(states_offsets_size));

  states_size = 0;
  for (uint64_t i = 0; i < sizeof(states) / sizeof(const char*); i++) {
    const uint64_t slen = strlen(states[i]);
    memcpy(&states_values[states_size], &states[i][0], slen);
    states_offsets[i] = states_size;
    states_size += slen;
  }
  tiledb_enumeration_t* enumeration_states;
  TRY(ctx,
      tiledb_enumeration_alloc(
          ctx,
          "us_states",
          TILEDB_STRING_ASCII,
          UINT32_MAX,
          false,
          states_values,
          states_size,
          states_offsets,
          states_offsets_size,
          &enumeration_states));
  free(states_offsets);
  free(states_values);

  TRY(ctx,
      tiledb_array_schema_add_enumeration(ctx, schema, enumeration_states));

  // Adding the attributes of the array to the array schema.
  tiledb_attribute_t* a;
  TRY(ctx, tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a));
  TRY(ctx, tiledb_attribute_set_nullable(ctx, a, true));

  tiledb_attribute_t* b;
  TRY(ctx, tiledb_attribute_alloc(ctx, "b", TILEDB_STRING_ASCII, &b));
  TRY(ctx, tiledb_attribute_set_cell_val_num(ctx, b, TILEDB_VAR_NUM));

  tiledb_attribute_t* c;
  TRY(ctx, tiledb_attribute_alloc(ctx, "c", TILEDB_INT32, &c));

  tiledb_attribute_t* d;
  TRY(ctx, tiledb_attribute_alloc(ctx, "d", TILEDB_FLOAT32, &d));

  tiledb_attribute_t* e;
  TRY(ctx, tiledb_attribute_alloc(ctx, "e", TILEDB_UINT8, &e));
  TRY(ctx, tiledb_attribute_set_nullable(ctx, e, true));
  TRY(ctx, tiledb_attribute_set_enumeration_name(ctx, e, "us_states"));

  TRY(ctx, tiledb_array_schema_add_attribute(ctx, schema, a));
  TRY(ctx, tiledb_array_schema_add_attribute(ctx, schema, b));
  TRY(ctx, tiledb_array_schema_add_attribute(ctx, schema, c));
  TRY(ctx, tiledb_array_schema_add_attribute(ctx, schema, d));
  TRY(ctx, tiledb_array_schema_add_attribute(ctx, schema, e));

  // Create the (empty) array.
  TRY(ctx, tiledb_array_create(ctx, array_name, schema));

  // Cleanup.
  tiledb_attribute_free(&e);
  tiledb_attribute_free(&d);
  tiledb_attribute_free(&c);
  tiledb_attribute_free(&b);
  tiledb_attribute_free(&a);
  tiledb_array_schema_free(&schema);
  tiledb_domain_free(&domain);
  tiledb_dimension_free(&dimension);

  return TILEDB_OK;
}

/**
 * @brief Execute a write on array query_condition_sparse array
 * which then stores the following data in the array. The table
 * is organized by dimension/attribute.
 *
 * index |  a   |   b   | c |  d  |     e
 * ------+------+-------+---+-----+------------
 *   0   | null | alice | 0 | 4.1 | arizona
 *   1   | 2    | bob   | 0 | 3.4 | etc
 *   2   | null | craig | 0 | 5.6 | connecticut
 *   3   | 4    | dave  | 0 | 3.7 | colorado
 *   4   | null | erin  | 0 | 2.3 | null
 *   5   | 6    | frank | 0 | 1.7 | arkansas
 *   6   | null | grace | 1 | 3.8 | etc
 *   7   | 8    | heidi | 2 | 4.9 | etc
 *   8   | null | ivan  | 3 | 3.2 | colorado
 *   9   | 10   | judy  | 4 | 3.1 | california
 *
 * @param ctx The context.
 */
int32_t write_array(tiledb_ctx_t* ctx) {
  // Create data buffers that store the values to be written in.
  int dim_data[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  uint64_t dim_size = sizeof(dim_data);
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
  uint8_t e_data[] = {2, 7, 5, 6, 100, 3, 7, 7, 5, 4};
  uint64_t e_size = sizeof(e_data);
  uint8_t e_validity[] = {1, 1, 1, 1, 0, 1, 1, 1, 1, 1};
  uint64_t e_validity_size = sizeof(e_validity);

  tiledb_array_t* array_w;
  TRY(ctx, tiledb_array_alloc(ctx, array_name, &array_w));
  TRY(ctx, tiledb_array_open(ctx, array_w, TILEDB_WRITE));

  // Execute the write query.
  tiledb_query_t* query_w;
  TRY(ctx, tiledb_query_alloc(ctx, array_w, TILEDB_WRITE, &query_w));
  TRY(ctx, tiledb_query_set_layout(ctx, query_w, TILEDB_UNORDERED));
  TRY(ctx,
      tiledb_query_set_data_buffer(ctx, query_w, "index", dim_data, &dim_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query_w, "a", a_data, &a_size));
  TRY(ctx,
      tiledb_query_set_validity_buffer(
          ctx, query_w, "a", a_data_validity, &a_validity_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query_w, "b", b_data, &b_size));
  TRY(ctx,
      tiledb_query_set_offsets_buffer(
          ctx, query_w, "b", b_data_offsets, &b_offsets_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query_w, "c", c_data, &c_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query_w, "d", d_data, &d_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query_w, "e", e_data, &e_size));
  TRY(ctx,
      tiledb_query_set_validity_buffer(
          ctx, query_w, "e", e_validity, &e_validity_size));
  TRY(ctx, tiledb_query_submit(ctx, query_w));
  TRY(ctx, tiledb_query_finalize(ctx, query_w));
  TRY(ctx, tiledb_array_close(ctx, array_w));

  tiledb_query_free(&query_w);
  tiledb_array_free(&array_w);

  return TILEDB_OK;
}

/**
 * @brief Executes the read query for the array created in write_array.
 *
 * @param ctx The context.
 * @param qc The query condition to execute the query with.
 */
int32_t read_array_with_predicates(tiledb_ctx_t* ctx, int num_predicates, ...) {
  // Create data buffers to read the values into.
  int a_data[10];
  uint64_t a_size = sizeof(a_data);
  uint8_t a_data_validity[10];
  uint64_t a_validity_size = sizeof(a_data_validity);

  // We initialize the string b_data to contain 45 characters because
  // that is the combined size of all strings in attribute b.
  char b_data[256];
  memset(b_data, 0, 256);
  uint64_t b_size = sizeof(b_data);
  uint64_t b_data_offsets[10];
  uint64_t b_offsets_size = sizeof(b_data_offsets);

  int32_t c_data[10];
  uint64_t c_size = sizeof(c_data);
  float d_data[10];
  uint64_t d_size = sizeof(d_data);

  uint8_t e_data[10];
  uint64_t e_size = sizeof(e_data);
  uint8_t e_validity[10];
  uint64_t e_validity_size = sizeof(e_validity);

  tiledb_array_t* array;
  TRY(ctx, tiledb_array_alloc(ctx, array_name, &array));
  TRY(ctx, tiledb_array_open(ctx, array, TILEDB_READ));

  // Execute the read query.
  tiledb_query_t* query;
  TRY(ctx, tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
  TRY(ctx, tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query, "a", a_data, &a_size));
  TRY(ctx,
      tiledb_query_set_validity_buffer(
          ctx, query, "a", a_data_validity, &a_validity_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query, "b", b_data, &b_size));
  TRY(ctx,
      tiledb_query_set_offsets_buffer(
          ctx, query, "b", b_data_offsets, &b_offsets_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query, "c", c_data, &c_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query, "d", d_data, &d_size));
  TRY(ctx, tiledb_query_set_data_buffer(ctx, query, "e", e_data, &e_size));
  TRY(ctx,
      tiledb_query_set_validity_buffer(
          ctx, query, "e", e_validity, &e_validity_size));

  va_list predicates;
  va_start(predicates, num_predicates);
  for (int i = 0; i < num_predicates; i++) {
    const char* predicate = va_arg(predicates, const char*);
    TRY(ctx, tiledb_query_add_predicate(ctx, query, predicate));
  }
  va_end(predicates);

  TRY(ctx, tiledb_query_submit(ctx, query));

  // Collect the results of the read query. The number of elements
  // the filtered array contains is calculated by determining the
  // number of valid elements in c_data, since the array is
  // sparse. The length of the filtered substring of all the
  // data is in b_data, and all the offsets for filtered
  // individual elements are in b_data_offsets.

  // Here we print all the elements that are returned by the query.
  uint64_t result_num = c_size / sizeof(int);
  for (uint64_t i = 0; i < result_num; ++i) {
    uint64_t element_start = b_data_offsets[i];
    uint64_t element_length = (i == result_num - 1) ?
                                  (b_size / sizeof(char)) - element_start :
                                  b_data_offsets[i + 1] - element_start;
    print_elem(
        a_data_validity[i] ? &a_data[i] : NULL,
        b_data + element_start,
        element_length,
        c_data[i],
        d_data[i],
        e_validity[i] ? &e_data[i] : NULL);
  }

  TRY(ctx, tiledb_query_finalize(ctx, query));
  TRY(ctx, tiledb_array_close(ctx, array));

  tiledb_query_free(&query);
  tiledb_array_free(&array);

  return TILEDB_OK;
}

int32_t read_array_with_predicate(tiledb_ctx_t* ctx, const char* predicate) {
  return read_array_with_predicates(ctx, 1, predicate);
}

int main() {
  // Create the context.
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, NULL, &vfs);

  int32_t is_dir = 0;
  tiledb_vfs_is_dir(ctx, vfs, array_name, &is_dir);
  if (!is_dir) {
    // Create and write data to the array.
    RETURN_IF_NOT_OK(create_array(ctx));
    RETURN_IF_NOT_OK(write_array(ctx));
  }

  // EXAMPLES FROM query_condition_sparse.c EXAMPLE

  // Execute a read query with no predicate which prints the entire array.
  printf("NO PREDICATE\n");
  RETURN_IF_NOT_OK(read_array_with_predicates(ctx, 0));
  printf("\n");

  // Execute a read query with predicate `TRUE`, which filters no cells and
  // prints the whole array
  printf("WHERE TRUE\n");
  RETURN_IF_NOT_OK(read_array_with_predicate(ctx, "TRUE"));
  printf("\n");

  // Execute a read query with predicate `a = null`.
  printf("WHERE a IS NULL\n");
  RETURN_IF_NOT_OK(read_array_with_predicate(ctx, "a IS NULL"));
  printf("\n");

  // Execute a read query with predicate `b < "eve"`.
  printf("WHERE b < 'eve'\n");
  RETURN_IF_NOT_OK(read_array_with_predicate(ctx, "b < 'eve'"));
  printf("\n");

  // Execute a read query with predicate `c >= 1`.
  printf("WHERE c >= 1\n");
  RETURN_IF_NOT_OK(read_array_with_predicate(ctx, "c >= 1"));
  printf("\n");

  // Execute a read query with predicate `3.0f <= d AND d <= 4.0f`.
  printf("WHERE d BETWEEN 3.0 AND 4.0\n");
  RETURN_IF_NOT_OK(read_array_with_predicate(ctx, "d BETWEEN 3.0 AND 4.0"));
  printf("\n");

  // Execute a read query with predicate `3.0f <= d AND d <= 4.0f AND a != null
  // AND b < \"eve\"`.
  printf("WHERE (d BETWEEN 3.0 AND 4.0) AND a IS NOT NULL AND b < 'eve'\n");
  RETURN_IF_NOT_OK(read_array_with_predicates(
      ctx, 3, "d BETWEEN 3.0 AND 4.0", "a IS NOT NULL", "b < 'eve'"));
  printf("\n");

  // BEGIN EXAMPLES WITH ENUMERATIONS
  printf("WHERE e = 'california'\n");
  {
    // error is expected as enumerations are not supported yet
    const int32_t ret = read_array_with_predicate(ctx, "e = 'california'");
    if (ret != TILEDB_ERR) {
      return TILEDB_ERR;
    }
  }
  printf("\n");

  // BEGIN EXAMPLES WITH NO EQUIVALENT

  // query condition does not have functions, here we use coalesce
  printf("WHERE coalesce(a, 2) + c < index\n");
  RETURN_IF_NOT_OK(
      read_array_with_predicate(ctx, "coalesce(a, 2) + c < index"));
  printf("\n");

  // FIXME: this is query-condition-able, use arithmetic
  printf("WHERE a > 6 OR a IS NULL\n");
  RETURN_IF_NOT_OK(read_array_with_predicate(ctx, "a > 6 OR a IS NULL"));
  printf("\n");

  tiledb_ctx_free(&ctx);

  return 0;
}
