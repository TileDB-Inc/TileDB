/**
 * @file quickstart_dimension_labels.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

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
 */
void create_array(const Context& ctx, const char* array_uri) {
  // Create dimensions.
  auto d1 = Dimension::create<int32_t>(ctx, "x_index", {{0, 5}}, 6);
  auto d2 = Dimension::create<int32_t>(ctx, "sample", {{0, 3}}, 4);

  // Create the domain.
  Domain domain(ctx);
  domain.add_dimensions(d1, d2);

  // Create a single attribute.
  Attribute a = Attribute::create<int16_t>(ctx, "a");

  // Create array schema.
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_domain(domain);
  schema.add_attribute(a);
  ArraySchemaExperimental::add_dimension_label(
      ctx, schema, 0, "x", TILEDB_INCREASING_DATA, TILEDB_FLOAT64);
  ArraySchemaExperimental::add_dimension_label(
      ctx, schema, 0, "y", TILEDB_INCREASING_DATA, TILEDB_FLOAT64);
  ArraySchemaExperimental::add_dimension_label(
      ctx, schema, 1, "timestamp", TILEDB_INCREASING_DATA, TILEDB_DATETIME_SEC);
  Array::create(ctx, array_uri, schema);
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
 */
void write_array_and_labels(const Context& ctx, const char* array_uri) {
  // Define attribute data.
  std::vector<int16_t> a = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                            13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};

  // Define label data.
  std::vector<double> x = {-1.0, -0.6, -0.2, 0.2, 0.6, 1.0};
  std::vector<double> y = {0.0, 2.0, 4.0, 6.0, 8.0, 10.0};
  std::vector<int64_t> timestamp = {31943, 32380, 33131, 33228};

  // Open array for writing.
  Array array(ctx, array_uri, TILEDB_WRITE);

  // Create the query
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_data_buffer("a", a);
  QueryExperimental::set_data_buffer(query, "x", x);
  QueryExperimental::set_data_buffer(query, "y", y);
  QueryExperimental::set_data_buffer(query, "timestamp", timestamp);

  // Submit query and check the query finished.
  auto status = query.submit();

  if (status != Query::Status::COMPLETE) {
    printf("Warning: Read query did not complete.\n");
  }
}

/**
 * Example reading data from the array and all dimension labels.
 *
 * @param ctx TileDB context to call TileDB API with.
 * @param array_uri URI of the array to read from.
 */
void read_array_and_labels(const Context& ctx, const char* array_uri) {
  printf("\nRead from main array\n");
  // Open array for reading.
  Array array(ctx, array_uri, TILEDB_READ);

  // Create subarray for reading data.
  Subarray subarray(ctx, array);
  subarray.add_range(0, 1, 2);
  subarray.add_range(1, 0, 2);

  // Create buffers to hold the output data.
  std::vector<int16_t> a(6);
  std::vector<double> x(2);
  std::vector<double> y(2);
  std::vector<int64_t> timestamp(3);

  // Create the query.
  //
  // Note: This example includes getting data from all 3 dimension labels. The
  // data will be returned for any label buffers set. It can be all, some, or
  // none of the possible dimension labels.
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  query.set_data_buffer("a", a);
  QueryExperimental::set_data_buffer(query, "x", x);
  QueryExperimental::set_data_buffer(query, "y", y);
  QueryExperimental::set_data_buffer(query, "timestamp", timestamp);
  // Submit the query and check if it finished.
  auto status = query.submit();
  if (status != Query::Status::COMPLETE) {
    printf("Warning: Read query did not complete.\n");
  }

  // Print results.
  for (unsigned i = 0; i < 2; ++i) {
    for (unsigned j = 0; j < 3; ++j) {
      int32_t x_val = i + 1;
      int32_t sample_val = j + 0;
      printf(" Cell (%d, %d)\n", x_val, sample_val);
      printf("    * a(%d, %d) = %d\n", x_val, sample_val, a[3 * i + j]);
      printf("    * x(%d) = %4.1f\n", x_val, x[i]);
      printf("    * y(%d) = %4.1f\n", x_val, y[i]);
      printf("    * timestamp(%d) = ", sample_val);
      print_timestamp(timestamp[j]);
      printf("\n");
    }
  }
}

/**
 * Example reading data from one dimension label.
 *
 * @param ctx TileDB context to call TileDB API with.
 * @param array_uri URI of the array to read from.
 */
void read_timestamp_data(const Context& ctx, const char* array_uri) {
  printf("\nRead from dimension label\n");

  // Open array for reading.
  Array array(ctx, array_uri, TILEDB_READ);

  // Create subarray for reading data.
  // Note: Since we are only reading a dimension label on dimension 1, any
  // ranges set on dimension 0 will be ignored.
  Subarray subarray(ctx, array);
  subarray.add_range(1, 1, 3);

  // Create buffers to hold the output data.
  std::vector<int64_t> timestamp(3);

  // Create the query.
  Query query(ctx, array);
  query.set_subarray(subarray);
  query.set_layout(TILEDB_ROW_MAJOR);
  QueryExperimental::set_data_buffer(query, "timestamp", timestamp);
  auto status = query.submit();
  // Check the query finished.
  if (status != Query::Status::COMPLETE) {
    printf("Warning: Read query did not complete.\n");
  }

  // Print results.
  for (unsigned j = 0; j < 3; ++j) {
    int32_t sample_val = j + 1;
    printf(" Cell (--, %d)\n", sample_val);
    printf("    * timestamp(%d) = ", sample_val);
    print_timestamp(timestamp[j]);
    printf("\n");
  }
}

/**
 * Example reading data from the array by the dimension labels.
 *
 * @param ctx TileDB context to call TileDB API with.
 * @param array_uri URI of the array to read from.
 */
void read_array_by_label(const Context& ctx, const char* array_uri) {
  printf("\nRead array from label ranges\n");
  Array array(ctx, array_uri, TILEDB_READ);

  // Create subarray for reading data.
  double y_range[2] = {3.0, 8.0};
  int64_t timestamp_range[2] = {31943, 32380};
  Subarray subarray(ctx, array);
  SubarrayExperimental::add_label_range(
      ctx, subarray, "y", y_range[0], y_range[1]);
  SubarrayExperimental::add_label_range(
      ctx, subarray, "timestamp", timestamp_range[0], timestamp_range[1]);

  // Create buffers to hold the output data.
  std::vector<int16_t> a(6);
  std::vector<double> y(3);
  std::vector<int64_t> timestamp(2);

  // Create the query.
  // Note: Setting the label buffers is optional. If they are not set, then
  // only data for `a` will be returned.
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  QueryExperimental::set_data_buffer(query, "y", y);
  QueryExperimental::set_data_buffer(query, "timestamp", timestamp);
  query.set_data_buffer("a", a);

  // Submit the query.
  auto status = query.submit();
  if (status != Query::Status::COMPLETE) {
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
}

/** Run the example. */
int main() {
  // Define variables.
  const char* array_uri = "quickstart_dimension_labels_array_cpp";

  // Create TileDB context.
  Context ctx;

  // Only create the axes label array if it does not exist.
  Object type = Object::object(ctx, array_uri);
  if (type.type() != Object::Type::Array) {
    // Create the array.
    create_array(ctx, array_uri);

    // Write data to the array and to the dimension labels.
    write_array_and_labels(ctx, array_uri);
  }

  // Read the full data.
  read_array_and_labels(ctx, array_uri);

  // Read data just from a single label.
  read_timestamp_data(ctx, array_uri);

  // Read data from the array using the dimension labels.
  read_array_by_label(ctx, array_uri);
}
