/**
 * @file axes_labels.cpp
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

#include <iostream>
#include <tiledb/tiledb>

// Name of axes labels array.
std::string axes_labels_array_uri("axes_labels_labels");

// Name of data array.
std::string data_array_uri("axes_labels_data");

void create_data_array(tiledb::Context& ctx, const std::string& array_uri) {
  // The array will be 2d array with dimensions "id" and "timestamp"
  // "id" is a 32bit integer, and timestamp is a datetime with second resolution
  tiledb::Domain domain(ctx);
  //
  int64_t timestamp_domain[2] = {0, 100LL * 365LL * 24LL * 60LL * 60LL};
  // Set the tile extent to 1 Day
  int64_t timestamp_extent = 24 * 60 * 60;
  domain
      .add_dimension(
          tiledb::Dimension::create<int32_t>(ctx, "id", {{1, 100}}, 10))
      // Add dimension which is timestamp from unix epoch to about 2070
      .add_dimension(tiledb::Dimension::create(
          ctx,
          "timestamp",
          tiledb_datatype_t::TILEDB_DATETIME_SEC,
          timestamp_domain,
          &timestamp_extent));

  // The array will be sparse.
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a two attributes "weight" and `element` so each each cell will contain
  // two attributes
  schema.add_attribute(tiledb::Attribute::create<float>(ctx, "weight"))
      .add_attribute(tiledb::Attribute::create<std::string>(ctx, "element"));

  // For the data array we will not duplicate coordinates
  schema.set_allows_dups(false);

  // Create the (empty) array on disk.
  tiledb::Array::create(ctx, array_uri, schema);
}

void create_axes_array(tiledb::Context& ctx, const std::string& array_uri) {
  // The array will be 2d array with dimensions "id" and "timestamp"
  // "id" is a string dimension type, so the domain and extent is null
  tiledb::Domain domain(ctx);
  domain.add_dimension(tiledb::Dimension::create(
      ctx, "color", TILEDB_STRING_ASCII, nullptr, nullptr));

  // The array will be sparse.
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a two attributes "id" and `timestamp` so each each cell will contains
  // the effective coordinates of the label.
  auto id_attr = tiledb::Attribute(ctx, "id", TILEDB_INT32);
  auto timestamp_attr =
      tiledb::Attribute(ctx, "timestamp", TILEDB_DATETIME_SEC);
  schema.add_attribute(id_attr).add_attribute(timestamp_attr);

  // Allow duplicate coordinates
  schema.set_allows_dups(true);

  // Create the (empty) array on disk.
  tiledb::Array::create(ctx, array_uri, schema);
}

void write_axes_array(tiledb::Context& ctx, const std::string& array_uri) {
  // Create label data. Here we will create one giant string
  std::string labels = "blue";
  std::vector<uint64_t> label_offsets = {0};
  label_offsets.emplace_back(labels.size());
  labels += "green";
  label_offsets.emplace_back(labels.size());
  labels += "green";

  // You could also create a vector of chars
  //  std::vector<char> labels  = {'b', 'l', 'u', 'e', 'g', 'r', 'e', 'e', 'n',
  //  'g', 'r', 'e', 'e', 'n'}; std::vector<uint64_t> label_offsets = {0, 4, 9}

  // Set the attributes of id/timestamp to match the coordinates of the main
  // data array (1, 1588878856), (1, 1588706056), (3, 1577836800)
  std::vector<int32_t> ids = {1, 1, 3};
  std::vector<int64_t> timestamps = {1588878856, 1588706056, 1577836800};

  // Open the array for writing and create the query.
  tiledb::Array array(ctx, array_uri, TILEDB_WRITE);
  tiledb::Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("color", labels)
      .set_offsets_buffer("color", label_offsets)
      .set_data_buffer("id", ids)
      .set_data_buffer("timestamp", timestamps);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void write_data_array(tiledb::Context& ctx, const std::string& array_uri) {
  // Create label data. Here we will create one giant string
  std::vector<int32_t> ids = {1, 1, 3};
  std::vector<int64_t> timestamps = {1588878856, 1588706056, 1577836800};

  std::vector<float> weights = {1.008, 4.0026, 6.94};
  std::string elements = "hydrogen";
  std::vector<uint64_t> element_offsets = {0};
  element_offsets.emplace_back(elements.size());
  elements += "helium";
  element_offsets.emplace_back(elements.size());
  elements += "lithium";

  // Open the array for writing and create the query.
  tiledb::Array array(ctx, array_uri, TILEDB_WRITE);
  tiledb::Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("id", ids)
      .set_data_buffer("timestamp", timestamps)
      .set_data_buffer("weight", weights)
      .set_data_buffer("element", elements)
      .set_offsets_buffer("element", element_offsets);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void read_data_array_with_label(
    tiledb::Context& ctx,
    const std::string& labels_array_uri,
    const std::string& datas_array_uri,
    const std::string& label) {
  // Prepare the data array for reading
  tiledb::Array data_array(ctx, datas_array_uri, TILEDB_READ);

  // Prepare the data query
  tiledb::Query data_query(ctx, data_array, TILEDB_READ);

  // Prepare the label array for reading
  tiledb::Array label_array(ctx, labels_array_uri, TILEDB_READ);

  // Set the subarray
  tiledb::Subarray subarray(ctx, label_array);
  subarray.add_range(0, label, label);

  // Prepare the query
  tiledb::Query label_query(ctx, label_array, TILEDB_READ);
  // Slice only the label passed in
  label_query.set_subarray(subarray);

  // Prepare the vector that will hold the result.
  // We only will fetch the id/timestamp
  // You can also use est_result_size to get the estimate result size instead of
  // hard coding the size of the vectors
  std::vector<int32_t> ids_coords(4);
  std::vector<int64_t> timestamps_coords(4);
  label_query.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("id", ids_coords)
      .set_data_buffer("timestamp", timestamps_coords);

  // Submit the query and close the array.
  label_query.submit();
  label_array.close();

  tiledb::Subarray data_subarray(ctx, data_array);

  // Loop through the label results to set ranges for the data query
  auto label_result_num = label_query.result_buffer_elements()["id"];
  for (uint64_t r = 0; r < label_result_num.second; r++) {
    int32_t i = ids_coords[r];
    int64_t j = timestamps_coords[r];
    std::cout << "Adding range for point (" << i << ", " << j << ")"
              << std::endl;
    data_subarray.add_range(0, i, i);
    data_subarray.add_range(1, j, j);
  }

  // Setup the data query's buffers
  std::vector<int32_t> ids(10);
  std::vector<int64_t> timestamps(10);

  std::vector<float> weights(10);
  std::vector<char> elements(256);
  std::vector<uint64_t> element_offsets(10);
  data_query.set_data_buffer("id", ids)
      .set_data_buffer("timestamp", timestamps)
      .set_data_buffer("weight", weights)
      .set_data_buffer("element", elements)
      .set_offsets_buffer("element", element_offsets)
      .set_subarray(data_subarray);
  // Submit the query and close the array.
  data_query.submit();
  data_array.close();

  // Get the results returned
  auto result_num = data_query.result_buffer_elements();
  auto element_result_num = result_num["element"];

  for (uint64_t r = 0; r < result_num["id"].second; r++) {
    // For strings we must compute the length based on the offsets
    uint64_t element_start = element_offsets[r];
    uint64_t element_size = (r == element_result_num.first - 1) ?
                                element_result_num.second - element_start :
                                element_offsets[r + 1] - element_start;
    std::string element(elements.data() + element_start, element_size);

    std::cout << element << " has weight " << weights[r] << " for id " << ids[r]
              << " at timestamp " << timestamps[r] << std::endl;
  }
}

int main() {
  tiledb::Context ctx;

  // Only create the axes label array if it does not exist
  if (tiledb::Object::object(ctx, axes_labels_array_uri).type() !=
      tiledb::Object::Type::Array) {
    create_axes_array(ctx, axes_labels_array_uri);
    write_axes_array(ctx, axes_labels_array_uri);
  }

  // Only create the data array if it does not exist
  if (tiledb::Object::object(ctx, data_array_uri).type() !=
      tiledb::Object::Type::Array) {
    create_data_array(ctx, data_array_uri);
    write_data_array(ctx, data_array_uri);
  }

  // Query based on the label "green"
  read_data_array_with_label(
      ctx, axes_labels_array_uri, data_array_uri, "green");

  return 0;
}
