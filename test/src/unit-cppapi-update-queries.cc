/**
 * @file   unit-cppapi-updates.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests the C++ API for update value related functions.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

TEST_CASE("C++ API: Test setting an update value", "[cppapi][updates]") {
  const std::string array_name = "cpp_unit_update_values";
  Config config;
  config["sm.allow_updates_experimental"] = "true";
  Context ctx(config);
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array and query.
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);
  Array array(ctx, array_name, TILEDB_UPDATE);
  Query query(ctx, array);

  // Set update value.
  int val = 1;
  QueryExperimental::add_update_value_to_query(
      ctx, query, "a", &val, sizeof(val));

  query.ptr()->query_->update_values()[0].check(
      array.ptr()->array_->array_schema_latest());

  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}