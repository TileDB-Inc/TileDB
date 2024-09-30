/**
 * @file enumerations.cc
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
 * When run, this program will create a simple 2D sparse array with an
 * enumeration and then use a query condition to select data based on the
 * enumerations values.
 */

#include <iostream>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

// Name of array.
std::string array_name("enumerations_example_array");

void create_array() {
  Context ctx;

  // First some standard boiler plate for creating an array. Nothing here
  // is important or required for Enumerations.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  auto dim1 = Dimension::create<int>(ctx, "rows", {{1, 4}}, 4);
  auto dim2 = Dimension::create<int>(ctx, "cols", {{1, 4}}, 4);

  Domain dom(ctx);
  dom.add_dimensions(dim1, dim2);
  schema.set_domain(dom);

  // The most basic enumeration only requires a name and a vector of values
  // to use as lookups. Enumeration values can be any supported TileDB type
  // although they are most commonly strings.
  std::vector<std::string> values = {"red", "green", "blue"};
  auto enmr = Enumeration::create(ctx, "colors", values);

  // To use an enumeration with an attribute, we just set the enumeration
  // name on the attribute before adding it to the schema. Attributes that
  // use an enumeration are required to have an integral type that is wide
  // enough to index the entire enumeration. For instance, an enumeration with
  // 256 values can fit in a uint8_t type, but at 257 values, the attribute
  // would require a type of int16_t at a minimum.
  auto attr = Attribute::create<uint8_t>(ctx, "attr");
  AttributeExperimental::set_enumeration_name(ctx, attr, "colors");

  // The enumeration must be added to the schema before any attribute that
  // references the enumeration so that the requirements of the attribute
  // can be accurately checked.
  ArraySchemaExperimental::add_enumeration(ctx, schema, enmr);

  // Finally, we add the attribute as per normal.
  schema.add_attribute(attr);

  Array::create(ctx, array_name, schema);
}

void write_array() {
  Context ctx;

  std::vector<int> row_data = {1, 2, 2};
  std::vector<int> col_data = {1, 4, 3};

  // Attribute data for an enumeration is just numeric indices into the
  // list of enumeration values.
  std::vector<uint8_t> attr_data = {2, 1, 1};

  // Open the array for writing and create the query.
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", row_data)
      .set_data_buffer("cols", col_data)
      .set_data_buffer("attr", attr_data);

  // Write and close the array
  query.submit();
  array.close();
}

void read_array() {
  Context ctx;

  // This is all standard boiler plate for reading from an array. The
  // section below will demonstrate using a QueryCondition to select
  // rows based on the enumeration.
  Array array(ctx, array_name, TILEDB_READ);
  Subarray subarray(ctx, array);
  subarray.add_range(0, 1, 4).add_range(1, 1, 4);

  std::vector<int> row_data(16);
  std::vector<int> col_data(16);
  std::vector<uint8_t> attr_data(16);

  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("rows", row_data)
      .set_data_buffer("cols", col_data)
      .set_data_buffer("attr", attr_data);

  // Query conditions apply against the enumeration's values instead of the
  // integral data. Thus, we can select values here using color names instead
  // of the integer indices.
  QueryCondition qc(ctx);
  qc.init("attr", "green", 5, TILEDB_EQ);
  query.set_condition(qc);

  // Finally, submit and display the query results
  query.submit();
  array.close();

  // Print out the results.
  auto result_num = (int)query.result_buffer_elements()["attr"].second;
  for (int i = 0; i < result_num; i++) {
    int r = row_data[i];
    int c = col_data[i];
    int a = attr_data[i];
    std::cout << "Cell (" << r << ", " << c << ") has attr " << a << "\n";
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
