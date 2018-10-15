/**
 * @file   filters.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This is a part of the TileDB filters tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/filters.html
 *
 * When run, this program will create a 2D sparse array with several filters,
 * write some data to it, and read a slice of the data back.
 *
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("filters_array");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Create two fixed-length attributes "a1" and "a2"
  auto a1 = Attribute::create<uint32_t>(ctx, "a1");
  auto a2 = Attribute::create<int32_t>(ctx, "a2");

  // a1 will be filtered by bit width reduction followed by zstd
  // compression.
  Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);
  Filter compression_zstd(ctx, TILEDB_FILTER_ZSTD);
  FilterList a1_filters(ctx);
  a1_filters.add_filter(bit_width_reduction).add_filter(compression_zstd);
  a1.set_filter_list(a1_filters);

  // a2 will just have a single gzip compression filter.
  FilterList a2_filters(ctx);
  a2_filters.add_filter({ctx, TILEDB_FILTER_GZIP});
  a2.set_filter_list(a2_filters);

  // Add the attributes
  schema.add_attribute(a1).add_attribute(a2);

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_array() {
  Context ctx;

  // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
  std::vector<int> coords = {1, 1, 2, 4, 2, 3};
  std::vector<uint32_t> data_a1 = {1, 2, 3};
  std::vector<int32_t> data_a2 = {-1, -2, -3};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_buffer("a1", data_a1)
      .set_buffer("a2", data_a2)
      .set_coordinates(coords);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void read_array() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Slice only rows 1, 2 and cols 2, 3, 4
  const std::vector<int> subarray = {1, 2, 2, 4};

  // Prepare the vector that will hold the result.
  // We take an upper bound on the result size, as we do not
  // know a priori how big it is (since the array is sparse)
  auto max_el = array.max_buffer_elements(subarray);
  std::vector<uint32_t> data_a1(max_el["a1"].second);
  std::vector<int> coords(max_el[TILEDB_COORDS].second);

  // Prepare the query
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a1", data_a1)
      .set_coordinates(coords);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  auto result_num = (int)query.result_buffer_elements()["a1"].second;
  for (int r = 0; r < result_num; r++) {
    int i = coords[2 * r], j = coords[2 * r + 1];
    int a = data_a1[r];
    std::cout << "Cell (" << i << ", " << j << ") has a1 data " << a << "\n";
  }
}

int main() {
  Context ctx;

  if (Object::object(ctx, array_name).type() != Object::Type::Array) {
    create_array();
    write_array();
  }

  read_array();
  return 0;
}