/**
 * @file quickstart_dimension_labels.c
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
 * When run, this program will create a simple 2D dense array with 3 dimension
 * labels, write data to it, and read back data from the array and the labels.
 * If the array already exists, then the writing and creation steps will be
 * skipped.
 *
 * Array Summary:
 *  * Array Type: Dense
 *  * Dimensions:
 *    - x: (type=INT32, domain=[0, 5])
 *    - sample: (type=INT32, domain=[0,3])
 *
 *  * Attributes:
 *    - a: (type=INT16)
 *
 *  * Labels on dimension 'x':
 *    - x (order=INCREASING, type=FLOAT64)
 *    - y (order=INCREASING, type=FLOAT64)
 *
 *  * Labels on dimension 'sample':
 *    - timestamp (order=INCREASING, type=DATETIME_SEC)
 *
 * Data:
 *  a = [ 1,  2,  3,  4,  5,  6,
 *        7,  8,  9, 10, 11, 12,
 *       13, 14, 15, 16, 17, 18,
 *       19, 20, 21, 22, 23, 23]
 *  x = [-1.0, -0.6, -0.2, 0.2, 1.0]
 *  y = [0.0, 2.0, 4.0, 6.0, 8.0, 10.0]
 *  timestamp = [8:52:23, 8:59:40, 9:12:11, 9:13:48]
 */

#include <stdio.h>
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

/** Macro that will end a function on a TileDB error.*/
#define RETURN_IF_ERROR(rc) \
  if (rc != TILEDB_OK) {    \
    return rc;              \
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
    printf("TileDB Error: Error code returned but no error found.");
    return rc;
  }
  const char* msg = NULL;
  tiledb_error_message(err, &msg);
  if (msg == NULL) {
    printf("TileDB Error");
  } else {
    printf("%s\n", msg);
  }
  return rc;
}

/**
 * Print timestamp in form HH:MM:SS.
 *
 * @param timestamp Timestamp to print in raw seconds.
 */
void print_timestamp(int64_t timestamp) {
  int64_t hr = timestamp / 3600;
  int64_t min = (timestamp - (hr * 3600)) / 60;
  int64_t sec = timestamp - hr * 3600 - min * 60;
  printf("%ld:%ld:%ld", (long)hr, (long)min, (long)sec);
}

/**
 * Create a TileDB array with dimension labels.
 *
 * * Array Summary:
 *  * Array Type: Dense
 *  * Dimensions:
 *    - x_index: (type=INT32, domain=[0, 5])
 *    - sample: (type=INT32, domain=[0,3])
 *
 *  * Attributes:
 *    - a: (type=INT16)
 *
 *  * Labels on dimension 'x_index':
 *    - x (order=INCREASING, type=FLOAT64)
 *    - y (order=INCREASING, type=FLOAT64)
 *
 *  * Labels on dimension 'sample':
 *    - timestamp (order=INCREASING, type=DATETIME_SEC)
 *
 * @param ctx TileDB context to call TileDB API with.
 * @param array_uri The URI to create the array at.
 * @returns TileDB return code.
 */
int32_t create_array(tiledb_ctx_t* ctx, const char* array_uri) {
  // Create first dimension.
  int32_t x_domain[] = {0, 5};
  int32_t x_tile = 6;
  tiledb_dimension_t* d1;
  RETURN_IF_ERROR(tiledb_dimension_alloc(
      ctx, "x_index", TILEDB_INT32, &x_domain[0], &x_tile, &d1));

  // Create second dimension.
  int32_t sample_domain[] = {0, 3};
  int32_t sample_tile = 4;
  tiledb_dimension_t* d2;
  RETURN_IF_ERROR(tiledb_dimension_alloc(
      ctx, "sample", TILEDB_INT32, &sample_domain[0], &sample_tile, &d2));

  // Create the domain.
  tiledb_domain_t* domain;
  RETURN_IF_ERROR(tiledb_domain_alloc(ctx, &domain));
  RETURN_IF_ERROR(tiledb_domain_add_dimension(ctx, domain, d1));
  RETURN_IF_ERROR(tiledb_domain_add_dimension(ctx, domain, d2));

  // Create a single attribute.
  tiledb_attribute_t* a;
  RETURN_IF_ERROR(tiledb_attribute_alloc(ctx, "a", TILEDB_INT16, &a));

  // Create array schema.
  tiledb_array_schema_t* array_schema;
  RETURN_IF_ERROR(tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema));
  RETURN_IF_ERROR(
      tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR));
  RETURN_IF_ERROR(
      tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR));
  RETURN_IF_ERROR(tiledb_array_schema_set_domain(ctx, array_schema, domain));
  RETURN_IF_ERROR(tiledb_array_schema_add_attribute(ctx, array_schema, a));
  RETURN_IF_ERROR(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "x", TILEDB_INCREASING_DATA, TILEDB_FLOAT64));
  RETURN_IF_ERROR(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "y", TILEDB_INCREASING_DATA, TILEDB_FLOAT64));
  RETURN_IF_ERROR(tiledb_array_schema_add_dimension_label(
      ctx,
      array_schema,
      1,
      "timestamp",
      TILEDB_INCREASING_DATA,
      TILEDB_DATETIME_SEC));

  // Create the array.
  RETURN_IF_ERROR(tiledb_array_create(ctx, array_uri, array_schema));

  // Clean-up and return ok.
  tiledb_attribute_free(&a);
  tiledb_domain_free(&domain);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_array_schema_free(&array_schema);
  return TILEDB_OK;
}

/**
 * Write TileDB array data and label data on the entire array.
 *
 * Data:
 *  a = [ 1,  2,  3,  4,  5,  6,
 *        7,  8,  9, 10, 11, 12,
 *       13, 14, 15, 16, 17, 18,
 *       19, 20, 21, 22, 23, 23]
 *  x = [-1.0, -0.6, -0.2, 0.2, 1.0]
 *  y = [0.0, 2.0, 4.0, 6.0, 8.0, 10.0]
 *  timestamp = [8:52:23, 8:59:40, 9:12:11, 9:13:48]
 *
 * @param ctx TileDB context to call TileDB API with.
 * @param array_uri URI of the array to write to.
 * @returns TileDB return code.
 */
int32_t write_array_and_labels(tiledb_ctx_t* ctx, const char* array_uri) {
  // Define attribute data.
  int16_t a[24] = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                   13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};

  // Define label data.
  double x[6] = {-1.0, -0.6, -0.2, 0.2, 0.6, 1.0};
  double y[6] = {0.0, 2.0, 4.0, 6.0, 8.0, 10.0};
  int64_t timestamp[4] = {31943, 32380, 33131, 33228};

  // Define sizes for writing buffers.
  uint64_t a_size = sizeof(a);
  uint64_t x_size = sizeof(x);
  uint64_t y_size = sizeof(y);
  uint64_t timestamp_size = sizeof(timestamp);

  // Open array for writing.
  tiledb_array_t* array;
  RETURN_IF_ERROR(tiledb_array_alloc(ctx, array_uri, &array));
  RETURN_IF_ERROR(tiledb_array_open(ctx, array, TILEDB_WRITE));

  // Create the query
  tiledb_query_t* query;
  RETURN_IF_ERROR(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
  RETURN_IF_ERROR(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(ctx, query, "a", a, &a_size));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(ctx, query, "x", x, &x_size));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(ctx, query, "y", y, &y_size));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(
      ctx, query, "timestamp", timestamp, &timestamp_size));

  // Submit query.
  RETURN_IF_ERROR(tiledb_query_submit(ctx, query));

  // Check the query finished.
  tiledb_query_status_t status;
  RETURN_IF_ERROR(tiledb_query_get_status(ctx, query, &status));
  if (status != TILEDB_COMPLETED) {
    printf("Warning: Read query did not complete.\n");
  }

  // Close the array.
  RETURN_IF_ERROR(tiledb_array_close(ctx, array));

  // Clean up and return TILEDB_OK.
  RETURN_IF_ERROR(tiledb_array_close(ctx, array));
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  return TILEDB_OK;
}

/**
 * Example reading data from the array and all dimension labels.
 *
 * @param ctx TileDB context to call TileDB API with.
 * @param array_uri URI of the array to read from.
 * @returns TileDB return code.
 */
int32_t read_array_and_labels(tiledb_ctx_t* ctx, const char* array_uri) {
  printf("\nRead from main array\n");

  // Open array for reading.
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_uri, &array);
  RETURN_IF_ERROR(tiledb_array_open(ctx, array, TILEDB_READ));

  // Create subarray for reading data.
  int32_t x_range[2] = {1, 2};
  int32_t sample_range[2] = {0, 2};
  tiledb_subarray_t* subarray;
  RETURN_IF_ERROR(tiledb_subarray_alloc(ctx, array, &subarray));
  RETURN_IF_ERROR(tiledb_subarray_add_range(
      ctx, subarray, 0, &x_range[0], &x_range[1], NULL));
  RETURN_IF_ERROR(tiledb_subarray_add_range(
      ctx, subarray, 1, &sample_range[0], &sample_range[1], NULL));

  // Create buffers to hold the output data.
  int16_t a[6];
  double x[2];
  double y[2];
  int64_t timestamp[3];

  // Define sizes for writing buffers.
  uint64_t a_size = sizeof(a);
  uint64_t x_size = sizeof(x);
  uint64_t y_size = sizeof(y);
  uint64_t timestamp_size = sizeof(timestamp);

  // Create the query.
  //
  // Note: This example includes getting data from all 3 dimension labels. The
  // data will be returned for any label buffers set. It can be all, some, or
  // none of the possible dimension labels.
  tiledb_query_t* query;
  RETURN_IF_ERROR(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
  RETURN_IF_ERROR(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
  RETURN_IF_ERROR(tiledb_query_set_subarray_t(ctx, query, subarray));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(ctx, query, "a", a, &a_size));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(ctx, query, "x", x, &x_size));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(ctx, query, "y", y, &y_size));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(
      ctx, query, "timestamp", timestamp, &timestamp_size));

  // Submit the query.
  RETURN_IF_ERROR(tiledb_query_submit(ctx, query));

  // Check the query finished.
  tiledb_query_status_t status;
  RETURN_IF_ERROR(tiledb_query_get_status(ctx, query, &status));
  if (status != TILEDB_COMPLETED) {
    printf("Warning: Read query did not complete.\n");
  }

  // Print results.
  for (unsigned i = 0; i < 2; ++i) {
    for (unsigned j = 0; j < 3; ++j) {
      int32_t x_val = i + x_range[0];
      int32_t sample_val = j + sample_range[0];
      printf(" Cell (%d, %d)\n", x_val, sample_val);
      printf("    * a(%d, %d) = %d\n", x_val, sample_val, a[3 * i + j]);
      printf("    * x(%d) = %4.1f\n", x_val, x[i]);
      printf("    * y(%d) = %4.1f\n", x_val, y[i]);
      printf("    * timestamp(%d) = ", sample_val);
      print_timestamp(timestamp[j]);
      printf("\n");
    }
  }

  // Clean-up and return ok.
  RETURN_IF_ERROR(tiledb_array_close(ctx, array));
  tiledb_subarray_free(&subarray);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  return TILEDB_OK;
}

/**
 * Example reading data from one dimension label.
 *
 * @param ctx TileDB context to call TileDB API with.
 * @param array_uri URI of the array to read from.
 * @returns TileDB return code.
 */
int32_t read_timestamp_data(tiledb_ctx_t* ctx, const char* array_uri) {
  printf("\nRead from dimension label\n");

  // Open array for reading.
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_uri, &array);
  RETURN_IF_ERROR(tiledb_array_open(ctx, array, TILEDB_READ));

  // Create subarray for reading data.
  // Note: Since we are only reading a dimension label on dimension 1, any
  // ranges set on dimension 0 will be ignored.
  int32_t sample_range[2] = {1, 3};
  tiledb_subarray_t* subarray;
  RETURN_IF_ERROR(tiledb_subarray_alloc(ctx, array, &subarray));
  RETURN_IF_ERROR(tiledb_subarray_add_range(
      ctx, subarray, 1, &sample_range[0], &sample_range[1], NULL));

  // Create buffers to hold the output data.
  int64_t timestamp[3];

  // Define sizes for writing buffers.
  uint64_t timestamp_size = sizeof(timestamp);

  // Create the query.
  tiledb_query_t* query;
  RETURN_IF_ERROR(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
  RETURN_IF_ERROR(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
  RETURN_IF_ERROR(tiledb_query_set_subarray_t(ctx, query, subarray));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(
      ctx, query, "timestamp", timestamp, &timestamp_size));

  // Submit the query.
  RETURN_IF_ERROR(tiledb_query_submit(ctx, query));

  // Check the query finished.
  tiledb_query_status_t status;
  RETURN_IF_ERROR(tiledb_query_get_status(ctx, query, &status));
  if (status != TILEDB_COMPLETED) {
    printf("Warning: Read query did not complete.\n");
  }

  // Print results.
  for (unsigned j = 0; j < 3; ++j) {
    int32_t sample_val = j + sample_range[0];
    printf(" Cell (--, %d)\n", sample_val);
    printf("    * timestamp(%d) = ", sample_val);
    print_timestamp(timestamp[j]);
    printf("\n");
  }

  // Clean-up and return ok.
  RETURN_IF_ERROR(tiledb_array_close(ctx, array));
  tiledb_subarray_free(&subarray);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  return TILEDB_OK;
}

/**
 * Example reading data from the array by the dimension labels.
 *
 * @param ctx TileDB context to call TileDB API with.
 * @param array_uri URI of the array to read from.
 * @returns TileDB return code.
 */
int32_t read_array_by_label(tiledb_ctx_t* ctx, const char* array_uri) {
  printf("\nRead array from label ranges\n");

  // Open array for reading.
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_uri, &array);
  RETURN_IF_ERROR(tiledb_array_open(ctx, array, TILEDB_READ));

  // Create subarray for reading data.
  double y_range[2] = {3.0, 8.0};
  int64_t timestamp_range[2] = {31943, 32380};
  tiledb_subarray_t* subarray;
  RETURN_IF_ERROR(tiledb_subarray_alloc(ctx, array, &subarray));
  RETURN_IF_ERROR(tiledb_subarray_add_label_range(
      ctx, subarray, "y", &y_range[0], &y_range[1], NULL));
  RETURN_IF_ERROR(tiledb_subarray_add_label_range(
      ctx,
      subarray,
      "timestamp",
      &timestamp_range[0],
      &timestamp_range[1],
      NULL));

  // Create buffers to hold the output data.
  int16_t a[6];
  double y[3];
  int64_t timestamp[2];

  // Define sizes for writing buffers.
  uint64_t a_size = sizeof(a);
  uint64_t y_size = sizeof(y);
  uint64_t timestamp_size = sizeof(timestamp);

  // Create the query.
  // Note: Setting the label buffers is optional. If they are not set, then
  // only data for `a` will be returned.
  tiledb_query_t* query;
  RETURN_IF_ERROR(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
  RETURN_IF_ERROR(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
  RETURN_IF_ERROR(tiledb_query_set_subarray_t(ctx, query, subarray));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(ctx, query, "y", y, &y_size));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(
      ctx, query, "timestamp", timestamp, &timestamp_size));
  RETURN_IF_ERROR(tiledb_query_set_data_buffer(ctx, query, "a", a, &a_size));

  // Submit the query.
  RETURN_IF_ERROR(tiledb_query_submit(ctx, query));

  // Check the query finished.
  tiledb_query_status_t status;
  RETURN_IF_ERROR(tiledb_query_get_status(ctx, query, &status));
  if (status != TILEDB_COMPLETED) {
    printf("Warning: Read query did not complete.\n");
  }

  // Print results.
  for (unsigned i = 0; i < 3; ++i) {
    for (unsigned j = 0; j < 2; ++j) {
      printf(" Cell (%3.1f, ", y[i]);
      print_timestamp(timestamp[j]);
      printf(")\n");
      printf("    * a(%3.1f, ", y[i]);
      print_timestamp(timestamp[j]);
      printf(") = %d\n", a[2 * i + j]);
      printf("\n");
    }
  }

  // Clean-up and return ok.
  RETURN_IF_ERROR(tiledb_array_close(ctx, array));
  tiledb_subarray_free(&subarray);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  return TILEDB_OK;
}

/** Run the example. */
int main() {
  // Define variables.
  const char* array_uri = "quickstart_dimension_labels";
  int rc = TILEDB_OK;

  // Create TileDB context.
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Only create the axes label array if it does not exist.
  tiledb_object_t type;
  tiledb_object_type(ctx, array_uri, &type);
  if (type != TILEDB_ARRAY) {
    // Create the array.
    rc = create_array(ctx, array_uri);
    if (rc != TILEDB_OK) {
      return print_last_error(ctx, rc);
    }

    // Write data to the array and to the dimension labels.
    rc = write_array_and_labels(ctx, array_uri);
    if (rc != TILEDB_OK) {
      return print_last_error(ctx, rc);
    }
  }

  // Read the full data.
  rc = read_array_and_labels(ctx, array_uri);
  if (rc != TILEDB_OK) {
    return print_last_error(ctx, rc);
  }

  // Read data just from a single label.
  rc = read_timestamp_data(ctx, array_uri);
  if (rc != TILEDB_OK) {
    return print_last_error(ctx, rc);
  }

  // Read data from the array using the dimension labels.
  rc = read_array_by_label(ctx, array_uri);
  if (rc != TILEDB_OK) {
    return print_last_error(ctx, rc);
  }

  // Clean-up.
  tiledb_ctx_free(&ctx);
  return rc;
}
