/**
 * @file axes_labels.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2022 TileDB, Inc.
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
 * Example program which shows the use of axes labels stored in a second array
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>

// Name of axes labels array.
const char* axes_labels_array_uri = "axes_labels_labels";

// Name of data array.
const char* data_array_uri = "axes_labels_data";

void create_data_array(tiledb_ctx_t* ctx, const char* array_uri) {
  // The array will be 2d array with dimensions "id" and "timestamp"
  // "id" is a 32bit integer, and timestamp is a datetime with second resolution

  // Create domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);

  // ID
  int32_t id_domain[2] = {1, 100};
  // Set the ID tile extent to 10
  int32_t id_extent = 10;

  // timestamp domain
  int64_t timestamp_domain[2] = {0, 100LL * 365LL * 24LL * 60LL * 60LL};
  // Set the tile extent to 1 Day
  int64_t timestamp_extent = 24 * 60 * 60;

  tiledb_dimension_t* id;
  tiledb_dimension_alloc(ctx, "id", TILEDB_INT32, id_domain, &id_extent, &id);
  tiledb_domain_add_dimension(ctx, domain, id);

  tiledb_dimension_t* timestamp;
  tiledb_dimension_alloc(
      ctx,
      "timestamp",
      TILEDB_DATETIME_SEC,
      timestamp_domain,
      &timestamp_extent,
      &timestamp);
  tiledb_domain_add_dimension(ctx, domain, timestamp);

  // Add a two attributes "weight" and `element` so each each cell will contain
  // two attributes
  tiledb_attribute_t* weight;
  tiledb_attribute_alloc(ctx, "weight", TILEDB_FLOAT32, &weight);

  tiledb_attribute_t* element;
  tiledb_attribute_alloc(ctx, "element", TILEDB_STRING_ASCII, &element);
  tiledb_attribute_set_cell_val_num(ctx, element, TILEDB_VAR_NUM);

  // The array will be sparse.
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, weight);
  tiledb_array_schema_add_attribute(ctx, array_schema, element);

  // For the data array we will not duplicate coordinates
  tiledb_array_schema_set_allows_dups(ctx, array_schema, 0);

  // Create the (empty) array on disk.
  tiledb_array_create(ctx, array_uri, array_schema);
  // Clean up
  tiledb_attribute_free(&weight);
  tiledb_attribute_free(&element);
  tiledb_dimension_free(&timestamp);
  tiledb_dimension_free(&id);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void create_axes_array(tiledb_ctx_t* ctx, const char* array_uri) {
  // The array will be 2d array with dimensions "id" and "timestamp"
  // "id" is a string dimension type, so the domain and extent is null

  // Create domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);
  tiledb_dimension_t* color;

  tiledb_dimension_alloc(ctx, "color", TILEDB_STRING_ASCII, NULL, NULL, &color);
  tiledb_domain_add_dimension(ctx, domain, color);

  // Add a two attributes "id" and `timestamp` so each each cell will contains
  // the effective coordinates of the label.
  tiledb_attribute_t* id;
  tiledb_attribute_alloc(ctx, "id", TILEDB_INT32, &id);

  tiledb_attribute_t* timestamp;
  tiledb_attribute_alloc(ctx, "timestamp", TILEDB_DATETIME_SEC, &timestamp);

  // The array will be sparse.
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, id);
  tiledb_array_schema_add_attribute(ctx, array_schema, timestamp);

  // For the labels we will allow duplicate coordinates
  tiledb_array_schema_set_allows_dups(ctx, array_schema, 1);

  // Create the (empty) array on disk.
  tiledb_array_create(ctx, array_uri, array_schema);
  // Clean up
  tiledb_attribute_free(&id);
  tiledb_attribute_free(&timestamp);
  tiledb_dimension_free(&color);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void write_axes_array(tiledb_ctx_t* ctx, const char* array_uri) {
  // Create label data. Here we will create one giant string
  char* labels = "bluegreengreen";
  uint64_t label_offsets[3] = {0, 4, 9};
  uint64_t labels_size = strlen(labels);
  uint64_t labels_offsets_size = sizeof(label_offsets);

  // Set the attributes of id/timestamp to match the coordinates of the main
  // data array (1, 1588878856), (1, 1588706056), (3, 1577836800)
  int32_t ids[3] = {1, 1, 3};
  uint64_t ids_size = sizeof(ids);
  int64_t timestamps[3] = {1588878856, 1588706056, 1577836800};
  uint64_t timestamps_size = sizeof(timestamps);

  // Open the array for writing and create the query.
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_uri, &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  tiledb_query_set_data_buffer(ctx, query, "color", labels, &labels_size);
  tiledb_query_set_offsets_buffer(
      ctx, query, "color", label_offsets, &labels_offsets_size);
  tiledb_query_set_data_buffer(ctx, query, "id", ids, &ids_size);
  tiledb_query_set_data_buffer(
      ctx, query, "timestamp", timestamps, &timestamps_size);

  // Perform the write and close the array.
  tiledb_query_submit(ctx, query);
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void write_data_array(tiledb_ctx_t* ctx, const char* array_uri) {
  // Create label data. Here we will create one giant string
  int32_t ids[3] = {1, 1, 3};
  uint64_t ids_size = sizeof(ids);
  int64_t timestamps[3] = {1588878856, 1588706056, 1577836800};
  uint64_t timestamps_size = sizeof(timestamps);

  float weights[3] = {1.008f, 4.0026f, 6.94f};
  uint64_t weights_size = sizeof(weights);
  char* elements = "hydrogenheliumlithium";
  uint64_t elements_size = strlen(elements);
  uint64_t element_offsets[3] = {0, 8, 14};
  uint64_t element_offsets_size = sizeof(element_offsets);

  // Open the array for writing and create the query.
  tiledb_array_t* array;
  tiledb_array_alloc(ctx, array_uri, &array);
  tiledb_array_open(ctx, array, TILEDB_WRITE);

  tiledb_query_t* query;
  tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  tiledb_query_set_data_buffer(ctx, query, "element", elements, &elements_size);
  tiledb_query_set_offsets_buffer(
      ctx, query, "element", element_offsets, &element_offsets_size);
  tiledb_query_set_data_buffer(ctx, query, "weight", weights, &weights_size);
  tiledb_query_set_data_buffer(ctx, query, "id", ids, &ids_size);
  tiledb_query_set_data_buffer(
      ctx, query, "timestamp", timestamps, &timestamps_size);

  // Perform the write and close the array.
  tiledb_query_submit(ctx, query);
  tiledb_array_close(ctx, array);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void read_data_array_with_label(
    tiledb_ctx_t* ctx,
    const char* labels_array_uri,
    const char* datas_array_uri,
    const char* label) {
  // Prepare the data array for reading
  tiledb_array_t* data_array;
  tiledb_array_alloc(ctx, datas_array_uri, &data_array);
  tiledb_array_open(ctx, data_array, TILEDB_READ);

  tiledb_query_t* data_query;
  tiledb_query_alloc(ctx, data_array, TILEDB_READ, &data_query);

  // Prepare the label array for reading
  tiledb_array_t* label_array;
  tiledb_array_alloc(ctx, labels_array_uri, &label_array);
  tiledb_array_open(ctx, label_array, TILEDB_READ);

  tiledb_query_t* label_query;
  tiledb_query_alloc(ctx, label_array, TILEDB_READ, &label_query);

  // Slice only the label passed in
  tiledb_subarray_t* label_subarray;
  tiledb_subarray_alloc(ctx, label_array, &label_subarray);
  uint64_t label_size = strlen(label);
  tiledb_subarray_add_range_var(
      ctx, label_subarray, 0, label, label_size, label, label_size);
  tiledb_query_set_subarray_t(ctx, label_query, label_subarray);

  // Prepare the vector that will hold the result.
  // We only will fetch the id/timestamp
  // You can also use est_result_size to get the estimate result size instead of
  // hard coding the size of the vectors
  int32_t ids_coords[4];
  uint64_t ids_coords_size = sizeof(ids_coords);
  int64_t timestamps_coords[4];
  uint64_t timesamp_coords_size = sizeof(timestamps_coords);
  tiledb_query_set_layout(ctx, label_query, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(
      ctx, label_query, "id", ids_coords, &ids_coords_size);
  tiledb_query_set_data_buffer(
      ctx, label_query, "timestamp", timestamps_coords, &timesamp_coords_size);

  // Submit the query and close the array.
  tiledb_query_submit(ctx, label_query);
  // Close array
  tiledb_array_close(ctx, label_array);

  tiledb_subarray_t* data_subarray;
  tiledb_subarray_alloc(ctx, data_array, &data_subarray);
  tiledb_query_set_subarray_t(ctx, data_query, data_subarray);

  // Loop through the label results to set ranges for the data query
  for (uint64_t r = 0; r < ids_coords_size / sizeof(int32_t); r++) {
    int32_t i = ids_coords[r];
    int64_t j = timestamps_coords[r];
    printf("Adding range for point ( %d, %" PRId64 ")\n", i, j);
    tiledb_subarray_add_range(ctx, data_subarray, 0, &i, &i, NULL);
    tiledb_subarray_add_range(ctx, data_subarray, 1, &j, &j, NULL);
  }

  // Setup the data query's buffers
  int32_t ids[10];
  uint64_t ids_size = sizeof(ids);
  int64_t timestamps[10];
  uint64_t timestamps_size = sizeof(timestamps);

  float weights[10];
  uint64_t weights_size = sizeof(weights);
  char elements[256];
  uint64_t elements_size = sizeof(elements);
  uint64_t element_offsets[10];
  uint64_t element_offsets_size = sizeof(element_offsets);
  tiledb_query_set_layout(ctx, data_query, TILEDB_ROW_MAJOR);
  tiledb_query_set_data_buffer(ctx, data_query, "id", ids, &ids_size);
  tiledb_query_set_data_buffer(
      ctx, data_query, "timestamp", timestamps, &timestamps_size);
  tiledb_query_set_data_buffer(
      ctx, data_query, "element", elements, &elements_size);
  tiledb_query_set_offsets_buffer(
      ctx, data_query, "element", element_offsets, &element_offsets_size);
  tiledb_query_set_data_buffer(
      ctx, data_query, "weight", weights, &weights_size);
  tiledb_query_set_subarray_t(ctx, data_query, data_subarray);

  // Submit the query and close the array.
  tiledb_query_submit(ctx, data_query);
  // Close array
  tiledb_array_close(ctx, data_array);

  // Get the results returned
  uint64_t elements_count = element_offsets_size / sizeof(uint64_t);
  for (uint64_t r = 0; r < elements_count; r++) {
    // For strings we must compute the length based on the offsets
    uint64_t element_start = element_offsets[r];
    int32_t element_length =
        (r == elements_count - 1) ?
            (elements_size / sizeof(char)) - element_start :
            element_offsets[r + 1] - element_start;

    printf(
        "%.*s has weight %f for id %d at timestamp %" PRId64 "\n",
        element_length,
        (elements + element_start),
        weights[r],
        ids[r],
        timestamps[r]);
  }

  // Free resources
  tiledb_subarray_free(&label_subarray);
  tiledb_array_free(&label_array);
  tiledb_query_free(&label_query);
  tiledb_subarray_free(&data_subarray);
  tiledb_array_free(&data_array);
  tiledb_query_free(&data_query);
}

int main() {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // Only create the axes label array if it does not exist
  tiledb_object_t type;
  tiledb_object_type(ctx, axes_labels_array_uri, &type);
  if (type != TILEDB_ARRAY) {
    create_axes_array(ctx, axes_labels_array_uri);
    write_axes_array(ctx, axes_labels_array_uri);
  }

  // Only create the data array if it does not exist
  tiledb_object_type(ctx, data_array_uri, &type);
  if (type != TILEDB_ARRAY) {
    create_data_array(ctx, data_array_uri);
    write_data_array(ctx, data_array_uri);
  }

  // Query based on the label "green"
  read_data_array_with_label(
      ctx, axes_labels_array_uri, data_array_uri, "green");

  tiledb_ctx_free(&ctx);
  return 0;
}
