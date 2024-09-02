/**
 * @file consolidation_plan.cc
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
 * When run, this program will create a simple 1D sparse array with three
 * fragments and generate the consolidation plan.
 */

#include <iostream>
#include <numeric>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

// Name of array.
std::string array_name("consolidation_plan_example_array");

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // If the array already exists on disk, return immediately.
  if (Object::object(ctx, array_name).type() == Object::Type::Array)
    return;

  // The array will be a vector with one dimension "rows", with domain [1,100].
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 100}}, 4));

  // The array will be sparse.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i) cell can store an integer.
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Create the (empty) array on disk.
  Array::create(array_name, schema);
}

void write_fragment(int min, int max) {
  Context ctx;

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);

  // Set layout to global
  query.set_layout(TILEDB_GLOBAL_ORDER);

  // Write a fragment with cells [min,max] for "rows" and "a".
  std::vector<int> coords_rows_1(max - min);
  std::iota(coords_rows_1.begin(), coords_rows_1.end(), min);
  std::vector<int> data_1(max - min);
  std::iota(data_1.begin(), data_1.end(), min);
  query.set_data_buffer("a", data_1).set_data_buffer("rows", coords_rows_1);
  query.submit();

  // Finalize - IMPORTANT!
  query.finalize();

  // Close the array
  array.close();
}

void write_array() {
  // Write a fragment with cells [1,30] for "rows" and "a".
  write_fragment(1, 30);

  // Write another fragment with cells [15,44] for "rows" and "a".
  write_fragment(15, 44);

  // Write one last fragment with cells [80,89] for "rows" and "a".
  write_fragment(80, 89);
}

void print_consolidation_plan() {
  Context ctx;

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Create the plan
  ConsolidationPlan plan(ctx, array, 1000);

  auto num_nodes = plan.num_nodes();
  std::cout << "Consolidation plan for " << num_nodes << " nodes:" << std::endl;
  for (uint64_t n = 0; n < num_nodes; n++) {
    auto num_fragments = plan.num_fragments(n);
    std::cout << "  Node " << n << " with " << num_fragments
              << " fragments:" << std::endl;
    for (uint64_t f = 0; f < num_fragments; f++) {
      std::cout << "    " << plan.fragment_uri(n, f) << std::endl;
    }
  }
}

int main() {
  Context ctx;
  if (Object::object(ctx, array_name).type() != Object::Type::Array) {
    create_array();
    write_array();
  }

  print_consolidation_plan();
  return 0;
}
