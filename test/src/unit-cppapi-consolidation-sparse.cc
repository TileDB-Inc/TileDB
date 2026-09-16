/**
 * @file   unit-cppapi-consolidation-sparse.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2026 TileDB Inc.
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
 * Consolidation tests with the C++ API.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

namespace sparse_consolidate {
void remove_array(const std::string& array_name) {
  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

void create_array(const std::string& array_name) {
  Context ctx;
  Domain domain(ctx);
  auto d = Dimension::create<int>(ctx, "d", {{1, 4}}, 4);
  domain.add_dimensions(d);
  auto a = Attribute::create<int>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attributes(a);
  Array::create(array_name, schema);
}

void write_array(
    const std::string& array_name,
    std::vector<int> d,
    std::vector<int> values) {
  Context ctx;
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED);
  query.set_data_buffer("d", d);
  query.set_data_buffer("a", values);
  query.submit();
  array.close();
}

void read_array(
    const std::string& array_name,
    const std::vector<int>& ranges,
    const std::vector<int>& c_values) {
  Context ctx;
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_ROW_MAJOR);
  Subarray subarray(ctx, array);
  for (auto& r : ranges) {
    subarray.add_range(0, r, r);
  }
  query.set_subarray(subarray);
  std::vector<int> d(100);
  std::vector<int> values(100);
  query.set_data_buffer("d", d);
  query.set_data_buffer("a", values);
  query.submit();
  array.close();
  values.resize(query.result_buffer_elements()["a"].second);
  CHECK(values.size() == c_values.size());
  CHECK(values == c_values);
}

TEST_CASE(
    "C++ API: Test sparse consolidation with partial tiles",
    "[cppapi][consolidation][sparse]") {
  std::string array_name = "cppapi_consolidation_sparse";
  remove_array(array_name);

  create_array(array_name);
  write_array(array_name, {1, 2}, {1, 2});
  write_array(array_name, {3}, {3});
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  read_array(array_name, {1, 2, 3}, {1, 2, 3});

  Context ctx;
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, nullptr));
  CHECK(tiledb::test::num_fragments(array_name) == 3);
  REQUIRE_NOTHROW(Array::vacuum(ctx, array_name, nullptr));
  CHECK(tiledb::test::num_fragments(array_name) == 1);

  read_array(array_name, {1, 2, 3}, {1, 2, 3});

  remove_array(array_name);
}

/**
 * The important thing about this test is the multiple ranges all fall inside
 * a single tile in the consolidated fragment. This covers a case where
 * we incorrectly were marking a range as being covered by a single fragment,
 * since with overlap we were only counting a fragment+tile for the first range
 * we saw it for, thus we might miss that a following range was included in
 * more than one fragment
 */
TEST_CASE(
    "C++ API: Test sparse consolidation without vacuum",
    "[cppapi][consolidation][sparse]") {
  std::string array_name = "cppapi_consolidation_sparse";
  remove_array(array_name);

  create_array(array_name);
  write_array(array_name, {1, 2}, {1, 2});
  write_array(array_name, {3}, {3});
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  read_array(array_name, {1, 2, 3}, {1, 2, 3});

  Context ctx;
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, nullptr));
  CHECK(tiledb::test::num_fragments(array_name) == 3);

  read_array(array_name, {1, 2, 3}, {1, 2, 3});

  remove_array(array_name);
}
}  // namespace sparse_consolidate
