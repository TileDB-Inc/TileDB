/**
 * @file   current_domain.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * When run, this program will create a simple 1D sparse array with a current
 * domain, print it, expand it with array schema evolution, and print it again.
 */

#include <iostream>
#include <numeric>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

// Name of array.
std::string array_name("current_domain_example_array");

void create_array(Context& ctx, const std::string& array_uri) {
  // Create a TileDB domain
  Domain domain(ctx);

  // Add a dimension to the domain
  auto d1 = Dimension::create<int>(ctx, "d1", {{1, 1000}}, 50);
  domain.add_dimension(d1);

  // Create a CurrentDomain object
  CurrentDomain current_domain(ctx);

  // Create an NDRectangle object
  NDRectangle ndrect(ctx, domain);

  // Assign the range [1, 100] to the rectangle's first dimension
  ndrect.set_range<int32_t>("d1", 1, 100);

  // Assign the NDRectangle to the CurrentDomain
  current_domain.set_ndrectangle(ndrect);

  // Create a TileDB sparse array schema
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
      .set_capacity(100)
      .set_cell_order(TILEDB_ROW_MAJOR)
      .set_tile_order(TILEDB_ROW_MAJOR);

  // Create a single attribute
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Assign the current domain to the array schema
  ArraySchemaExperimental::set_current_domain(ctx, schema, current_domain);

  // Create the (empty) array on disk
  Array::create(array_uri, schema);
}

void print_current_domain(Context& ctx) {
  // Get array schema
  ArraySchema schema(ctx, array_name);

  // Get current domain
  CurrentDomain current_domain =
      ArraySchemaExperimental::current_domain(ctx, schema);

  // Check if the current domain is empty
  if (current_domain.is_empty()) {
    std::cout << "Current domain: empty" << std::endl;
    return;
  }

  // Get the current domain type
  tiledb_current_domain_type_t current_domain_type = current_domain.type();

  // Check the current domain type
  if (current_domain_type != TILEDB_NDRECTANGLE) {
    std::cout << "Current domain type: unknown" << std::endl;
    return;
  }

  std::cout << "Current domain type: NDRECTANGLE" << std::endl;

  // Get the current domain's NDRectangle
  NDRectangle ndrect = current_domain.ndrectangle();

  // Get the range of the rectangle's first dimension
  std::array<int32_t, 2> range = ndrect.range<int32_t>("d1");

  // Print the range
  std::cout << "Current domain range: [" << range[0] << ", " << range[1] << "]"
            << std::endl;

  // Print datatype of range 0
  std::cout << "Current domain range 0 datatype: "
            << tiledb::impl::type_to_str(ndrect.range_dtype(0)) << std::endl;

  // Print datatype of range d1
  std::cout << "Current domain range 0 datatype: "
            << tiledb::impl::type_to_str(ndrect.range_dtype("d1")) << std::endl;

  // Print dim num
  std::cout << "Current domain dim num: " << ndrect.dim_num() << std::endl;
}

void expand_current_domain(Context& ctx) {
  // Get the array schema
  ArraySchema schema(ctx, array_name);

  // Get the domain
  Domain domain = schema.domain();

  // Create an ArraySchemaEvolution object
  ArraySchemaEvolution schema_evolution(ctx);

  // Create the new CurrentDomain object
  CurrentDomain new_current_domain(ctx);

  // Create an NDRectangle object
  NDRectangle ndrect(ctx, domain);

  // Assign the range [1, 200] to the rectangle's first dimension
  ndrect.set_range<int32_t>("d1", 1, 200);

  // Set the NDRectangle to the CurrentDomain
  new_current_domain.set_ndrectangle(ndrect);

  // Set the current domain to the array schema evolution
  schema_evolution.expand_current_domain(new_current_domain);

  // Evolve the array
  schema_evolution.array_evolve(array_name);
}

int main() {
  Context ctx;

  // Create a new simple array
  create_array(ctx, array_name);

  // Print the current domain
  print_current_domain(ctx);

  // Expand the current domain
  expand_current_domain(ctx);

  // Print the current domain again
  print_current_domain(ctx);

  return 0;
}
