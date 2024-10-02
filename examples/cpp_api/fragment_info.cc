/**
 * @file   fragment_info.cc
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
 * When run, this program will create a simple 2D dense array, write some data
 * with one query (creating a fragment) and collect information on the fragment.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("fragment_info_array");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4]
  // and space tiles 2x2
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 2))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 2));

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(ctx, array_name, schema);
}

void write_array() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Subarray subarray(ctx, array);
  subarray.add_range(0, 1, 2).add_range(1, 1, 4);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data)
      .set_subarray(subarray);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void get_fragment_info() {
  // Create TileDB context
  Context ctx;

  // Create fragment info object
  FragmentInfo fragment_info(ctx, array_name);

  // Load fragment
  fragment_info.load();

  // Get number of written fragments.
  uint32_t num = fragment_info.fragment_num();
  std::cout << "The number of written fragments is " << num << ".\n"
            << std::endl;

  // Get fragment name
  std::string name = fragment_info.fragment_name(0);
  std::cout << "The fragment name is " << name.c_str() << ".\n" << std::endl;

  // Get fragment URI
  std::string uri = fragment_info.fragment_uri(0);
  std::cout << "The fragment URI is " << uri.c_str() << ".\n" << std::endl;

  // Get fragment size
  uint64_t size = fragment_info.fragment_size(0);
  std::cout << "The fragment size is " << size << ".\n" << std::endl;

  // Check if the fragment is dense or sparse.
  bool dense = fragment_info.dense(0);
  if (dense == 1)
    std::cout << "The fragment is dense.\n" << std::endl;
  else
    std::cout << "The fragment is sparse.\n" << std::endl;

  // Get the fragment timestamp range
  std::pair<uint64_t, uint64_t> timestamps = fragment_info.timestamp_range(0);
  std::cout << "The fragment's timestamp range is {" << timestamps.first << " ,"
            << timestamps.second << "}.\n"
            << std::endl;

  // Get the number of cells written to the fragment.
  uint64_t cell_num = fragment_info.cell_num(0);
  std::cout << "The number of cells written to the fragment is " << cell_num
            << ".\n"
            << std::endl;

  // Get the format version of the fragment.
  uint32_t version = fragment_info.version(0);
  std::cout << "The fragment's format version is " << version << ".\n"
            << std::endl;

  // Check if fragment has consolidated metadata.
  // If not, get the number of fragments with unconsolidated metadata
  //  in the fragment info object.
  bool consolidated = fragment_info.has_consolidated_metadata(0);
  if (consolidated != 0) {
    std::cout << "The fragment has consolidated metadata.\n" << std::endl;
  } else {
    uint32_t unconsolidated = fragment_info.unconsolidated_metadata_num();
    std::cout << "The fragment has " << unconsolidated
              << " unconsolidated metadata fragments.\n"
              << std::endl;
  }

  // Get non-empty domain from index
  uint64_t non_empty_dom[2];
  fragment_info.get_non_empty_domain(0, 0, &non_empty_dom[0]);
}

int main() {
  Context ctx;
  if (Object::object(ctx, array_name).type() == Object::Type::Array) {
    tiledb::Object::remove(ctx, array_name);
  }
  create_array();
  write_array();
  get_fragment_info();

  return 0;
}
