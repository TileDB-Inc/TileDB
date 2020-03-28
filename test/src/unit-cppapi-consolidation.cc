/**
 * @file   unit-cppapi-consolidation.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

void remove_array(const std::string& array_name) {
  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

void create_array(const std::string& array_name) {
  Context ctx;
  Domain domain(ctx);
  auto d = Dimension::create<int>(ctx, "d", {{1, 3}}, 2);
  domain.add_dimensions(d);
  auto a = Attribute::create<int>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a);
  Array::create(array_name, schema);
}

void write_array(
    const std::string& array_name,
    const std::vector<int>& subarray,
    std::vector<int> values) {
  Context ctx;
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  query.set_buffer("a", values);
  query.submit();
  array.close();
}

void read_array(
    const std::string& array_name,
    const std::vector<int>& subarray,
    const std::vector<int>& c_values) {
  Context ctx;
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  std::vector<int> values(10);
  query.set_buffer("a", values);
  query.submit();
  array.close();
  values.resize(query.result_buffer_elements()["a"].second);
  CHECK(values.size() == c_values.size());
  CHECK(values == c_values);
}

int num_fragments(const std::string& array_name) {
  Context ctx;
  VFS vfs(ctx);
  // Exclude array lock, __meta, schema
  return (int)vfs.ls(array_name).size() - 3;
}

TEST_CASE(
    "C++ API: Test consolidation with partial tiles",
    "[cppapi][consolidation]") {
  std::string array_name = "cppapi_consolidation";
  remove_array(array_name);

  create_array(array_name);
  write_array(array_name, {1, 2}, {1, 2});
  write_array(array_name, {3, 3}, {3});
  CHECK(num_fragments(array_name) == 2);

  read_array(array_name, {1, 3}, {1, 2, 3});

  Context ctx;
  Config config;
  config["sm.consolidation.buffer_size"] = "4";
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, &config));
  CHECK(num_fragments(array_name) == 1);

  read_array(array_name, {1, 3}, {1, 2, 3});

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation with domain expansion",
    "[cppapi][consolidation]") {
  std::string array_name = "cppapi_consolidation_domain_exp";

  Context ctx;
  Domain domain(ctx);
  auto d = Dimension::create<int>(ctx, "d1", {{10, 110}}, 50);
  domain.add_dimensions(d);
  auto a = Attribute::create<float>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a);
  Array::create(array_name, schema);

  // clean up
  VFS vfs(ctx);
  std::shared_ptr<int> cleanup(
      NULL, [vfs, array_name](int*) { vfs.remove_dir(array_name); });

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  std::vector<float> a1(100);
  std::fill(a1.begin(), a1.end(), 1.0);
  std::vector<float> a2({2.0});

  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray({10, 109});
  query.set_buffer("a", a1);
  query.submit();
  query.set_subarray({110, 110});
  query.set_buffer("a", a2);
  query.submit();
  array.close();

  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, NULL));
}
