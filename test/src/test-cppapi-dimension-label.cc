/**
 * @file test-cppapi-dimension-label.cc
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
 * Tests the DimensionLabel C++ API
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

using namespace tiledb::test;

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Get dimension label from schema",
    "[cppapi][ArraySchema][DimensionLabel]") {
  // Set array name.
  auto array_name = fullpath("simple_array_with_label");

  // Create the C++ context.
  tiledb::Context ctx_{ctx, false};

  // Define label order for test.
  auto label_type = GENERATE(TILEDB_FLOAT64, TILEDB_STRING_ASCII);

  // Create an array with a dimension label.
  tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
  tiledb::Domain domain(ctx_);
  auto d1 = tiledb::Dimension::create<uint64_t>(ctx_, "x", {{0, 63}}, 64);
  auto d2 = tiledb::Dimension::create<uint64_t>(ctx_, "y", {{0, 63}}, 64);
  domain.add_dimension(d1);
  schema.set_domain(domain);
  auto a1 = tiledb::Attribute::create<double>(ctx_, "a");
  schema.add_attribute(a1);
  tiledb::ArraySchemaExperimental::add_dimension_label(
      ctx_, schema, 0, "l1", TILEDB_INCREASING_DATA, label_type);
  tiledb::Array::create(array_name, schema);

  // Load the array schema and get the dimension label from it.
  auto loaded_schema = tiledb::ArraySchema(ctx_, array_name);
  auto has_label = tiledb::ArraySchemaExperimental::has_dimension_label(
      ctx_, loaded_schema, "l1");
  REQUIRE(has_label);
  auto dim_label = tiledb::ArraySchemaExperimental::dimension_label(
      ctx_, loaded_schema, "l1");

  // Check the values.
  CHECK(dim_label.dimension_index() == 0);
  CHECK(dim_label.label_order() == TILEDB_INCREASING_DATA);
  if (label_type == TILEDB_FLOAT64) {
    CHECK(dim_label.label_cell_val_num() == 1);
    CHECK(dim_label.label_type() == TILEDB_FLOAT64);
  } else {
    CHECK(dim_label.label_cell_val_num() == tiledb::sm::constants::var_num);
    CHECK(dim_label.label_type() == TILEDB_STRING_ASCII);
  }
  CHECK(dim_label.label_attr_name() == "label");

  // Make sure the URI is to a valid array.
  auto dim_label_object = tiledb::Object::object(ctx_, dim_label.uri());
  CHECK(dim_label_object.type() == tiledb::Object::Type::Array);
}
