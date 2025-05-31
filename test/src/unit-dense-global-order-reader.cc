/**
 * @file   unit-dense-global-order-reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

static void create_array(Context& ctx, const std::string& array_uri);
static void write_array(
    Context& ctx, const std::string& array_uri, tiledb_layout_t layout);

TEST_CASE(
    "SC-60301: Read data with global cell order returns fill values",
    "[dense-reader][bug][global-cell-order][fixed][sc60301]") {
  test::VFSTempDir vfs_test_setup;
  const std::string array_uri =
      vfs_test_setup.array_uri("dense_global_cell_order");
  Context ctx{vfs_test_setup->ctx()};

  // Test setup
  create_array(ctx, array_uri);
  auto write_layout =
      GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR, TILEDB_GLOBAL_ORDER);
  write_array(ctx, array_uri, write_layout);

  Array array(ctx, array_uri, TILEDB_READ);
  Subarray subarray(ctx, array);
  subarray.set_subarray({1, 2, 1, 2});
  std::vector<int> a_read;
  a_read.resize(4);
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("a", a_read);

  REQUIRE(query.submit() == Query::Status::COMPLETE);

  if (write_layout == TILEDB_ROW_MAJOR) {
    REQUIRE(a_read[0] == 1);
    REQUIRE(a_read[1] == 3);
    REQUIRE(a_read[2] == 2);
    REQUIRE(a_read[3] == 4);
  } else {
    REQUIRE(a_read[0] == 1);
    REQUIRE(a_read[1] == 2);
    REQUIRE(a_read[2] == 3);
    REQUIRE(a_read[3] == 4);
  }

  array.close();
}

void create_array(Context& ctx, const std::string& array_uri) {
  auto obj = Object::object(ctx, array_uri);
  if (obj.type() != Object::Type::Invalid) {
    Object::remove(ctx, array_uri);
  }

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d1", {{1, 2}}, 2))
      .add_dimension(Dimension::create<int>(ctx, "d2", {{1, 2}}, 2));

  // Create the array schema with col-major cell order and tile order
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
      .add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(ctx, array_uri, schema);
}

void write_array(
    Context& ctx, const std::string& array_uri, tiledb_layout_t layout) {
  std::vector<int> data = {1, 2, 3, 4};
  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(layout).set_data_buffer("a", data);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  query.finalize();
  array.close();
}
