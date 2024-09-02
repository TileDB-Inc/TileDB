/**
 * @file   sc-53970.cc
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
 */
#include <climits>

#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

#include <test/support/tdb_catch.h>

using namespace tiledb;

static void create_array(const std::string& array_uri);
static void write_array(const std::string& array_uri);

TEST_CASE(
    "Subarray range expansion bug",
    "[bug][sc53970][subarray-range-expansion]") {
  std::string array_uri = "test_array_schema_dump";

  // Test setup
  create_array(array_uri);
  write_array(array_uri);

  Context ctx;
  Array array(ctx, array_uri, TILEDB_READ);

  std::vector<int64_t> dim = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
  std::vector<float> attr = {
      -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0};

  Query query(ctx, array, TILEDB_READ);

  Config cfg;
  cfg["sm.var_offsets.bitsize"] = "64";
  cfg["sm.var_offsets.mode"] = "elements";
  cfg["sm.var_offsets.extra_element"] = "true";
  query.set_config(cfg);

  int64_t range1[] = {-1374262780975110845, -1374262780975110845};
  int64_t range2[] = {-6603679540125901718, 0};

  Subarray subarray(ctx, array);
  subarray.add_range(0, range1[0], range1[1])
      .add_range(0, range2[0], range2[1]);

  query.set_layout(TILEDB_UNORDERED)
      .set_subarray(subarray)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr", attr);
  REQUIRE(query.submit() == Query::Status::COMPLETE);

  // The expected result are a single matching cell of (0, 1507468.6)
  REQUIRE(dim[0] == 0);
  REQUIRE(abs(attr[0] - 1507468.6f) < 0.00000005);

  // Check we didn't get any extra results
  REQUIRE(dim[1] == -1);
  REQUIRE(attr[1] == -1.0);
}

void create_array(const std::string& array_uri) {
  Context ctx;

  auto obj = Object::object(ctx, array_uri);
  if (obj.type() != Object::Type::Invalid) {
    Object::remove(ctx, array_uri);
  }

  auto dim =
      Dimension::create<int64_t>(ctx, "dim", {{INT64_MIN, INT64_MAX - 1}});

  Domain dom(ctx);
  dom.add_dimension(dim);

  auto attr = Attribute::create<float>(ctx, "attr");

  ArraySchema schema(ctx, TILEDB_SPARSE);

  schema.set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
      .set_domain(dom)
      .add_attribute(attr)
      .set_capacity(1713)
      .set_allows_dups(true);

  Array::create(array_uri, schema);
}

void write_array(const std::string& array_uri) {
  Context ctx;
  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  std::vector<int64_t> dim = {0, -8672700570587565350};
  std::vector<float> attr = {1507468.6f, -0.0f};

  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr", attr);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
}
