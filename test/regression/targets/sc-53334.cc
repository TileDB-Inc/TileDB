/**
 * @file   sc-53334.cc
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
 * When run, this program will create a simple 2D sparse array, write some data
 * to it in global order, and read the data back with aggregates.
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>
#include <vector>

#include <catch2/catch_template_test_macros.hpp>

using namespace tiledb;

typedef char min_type;

template <tiledb_datatype_t attribute_type>
struct AttributeTraits;

template <>
struct AttributeTraits<TILEDB_UINT8> {
  typedef uint8_t value_type;
};

template <>
struct AttributeTraits<TILEDB_STRING_ASCII> {
  typedef char value_type;
};

template <tiledb_datatype_t attribute_datatype>
struct MyArray {
  using AttributeValueType =
      typename AttributeTraits<attribute_datatype>::value_type;

  static void create_array(Context& ctx, const char* array_name) {
    // The array will be 4x4 with dimensions "rows" and "cols", with domain
    // [1,4].
    auto rows = Dimension::create<int32_t>(ctx, "rows", {{1, 4}}, 4);
    auto cols = Dimension::create<int32_t>(ctx, "columns", {{1, 4}}, 4);

    // Create domain
    Domain domain(ctx);
    domain.add_dimension(rows);
    domain.add_dimension(cols);

    // Create a single attribute "a" so each (i,j) cell can store a character
    Attribute a(ctx, "a", attribute_datatype);

    // Create array schema
    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_ROW_MAJOR);
    schema.add_attribute(a);

    // Create array
    Array::create(array_name, schema);
  }

  static void write_array(Context& ctx, const char* array_name) {
    // Open array for writing
    Array array(ctx, array_name, TILEDB_WRITE);

    // Data
    std::vector<int32_t> rows = {
        1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};
    std::vector<int32_t> cols = {
        1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
    std::vector<AttributeValueType> atts = {
        'a',
        'b',
        'c',
        'd',
        'e',
        'f',
        'g',
        'h',
        'i',
        'j',
        'k',
        'l',
        'm',
        'n',
        'o',
        'p'};

    Query query(ctx, array);
    query.set_data_buffer("rows", rows)
        .set_data_buffer("columns", cols)
        .set_data_buffer("a", atts);

    query.submit();
    array.close();
  }

  static AttributeValueType query_min(Context& ctx, const char* array_name) {
    // note, use C API because the CPP API doesn't seem to have Min yet
    Array array(ctx, array_name, TILEDB_READ);

    Query query(ctx, array);
    query.set_layout(TILEDB_UNORDERED);

    QueryChannel default_channel =
        QueryExperimental::get_default_channel(query);
    ChannelOperation operation =
        QueryExperimental::create_unary_aggregate<MinOperator>(query, "a");
    default_channel.apply_aggregate("Min", operation);

    std::vector<AttributeValueType> min(1);
    query.set_data_buffer("Min", min);

    query.submit();
    query.finalize();

    return min[0];
  }
};

bool array_exists(Context& ctx, const char* uri) {
  auto object = tiledb::Object::object(ctx, uri);
  return (object.type() == tiledb::Object::Type::Array);
}

TEST_CASE("SC-53334 min single value UINT8 works", "[bug][sc-53334]") {
  using MyArray = MyArray<TILEDB_UINT8>;

  Context ctx;
  std::string uri("sc-53334-uint8");

  if (!array_exists(ctx, uri.c_str())) {
    MyArray::create_array(ctx, uri.c_str());
    MyArray::write_array(ctx, uri.c_str());
  }

  const uint8_t min = MyArray::query_min(ctx, uri.c_str());
  REQUIRE(min == 'a');
}

TEST_CASE(
    "SC-53334 min single value STRING_ASCII does not work",
    "[regression][bug][sc-53334][!shouldfail]") {
  using MyArray = MyArray<TILEDB_STRING_ASCII>;

  Context ctx;
  std::string uri("sc-53334-string-ascii");

  if (!array_exists(ctx, uri.c_str())) {
    MyArray::create_array(ctx, uri.c_str());
    MyArray::write_array(ctx, uri.c_str());
  }

  /*
   * This throws an exception instead of returning the correct result "a".
   *
   * "OutputBufferValidator: Aggregate fixed size buffer should be for one
   * element" This happens because the Min/Max ops are specialized to do
   * std::string as their internal result buffer, but we are looking for a
   * single `char` result.
   *
   * When the bug is fixed, delete `CHECK_THROWS` and `if (false)`.
   */
  CHECK_NOTHROW(MyArray::query_min(ctx, uri.c_str()));
  if (false) {
    const char min = MyArray::query_min(ctx, uri.c_str());
    REQUIRE(min == 'a');
  }
}
