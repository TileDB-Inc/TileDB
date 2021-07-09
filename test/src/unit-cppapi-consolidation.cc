/**
 * @file   unit-cppapi-consolidation.cc
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
 * Consolidation tests with the C++ API.
 */

#include "catch.hpp"
#include "helpers.h"
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
  query.set_data_buffer("a", values);
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
  query.set_data_buffer("a", values);
  query.submit();
  array.close();
  values.resize(query.result_buffer_elements()["a"].second);
  CHECK(values.size() == c_values.size());
  CHECK(values == c_values);
}

TEST_CASE(
    "C++ API: Test consolidation with partial tiles",
    "[cppapi][consolidation]") {
  std::string array_name = "cppapi_consolidation";
  remove_array(array_name);

  create_array(array_name);
  write_array(array_name, {1, 2}, {1, 2});
  write_array(array_name, {3, 3}, {3});
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  read_array(array_name, {1, 3}, {1, 2, 3});

  Context ctx;
  Config config;
  config["sm.consolidation.buffer_size"] = "4";
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, &config));
  CHECK(tiledb::test::num_fragments(array_name) == 3);
  REQUIRE_NOTHROW(Array::vacuum(ctx, array_name, &config));
  CHECK(tiledb::test::num_fragments(array_name) == 1);

  read_array(array_name, {1, 3}, {1, 2, 3});

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation with domain expansion",
    "[cppapi][consolidation][expand-domain]") {
  std::string array_name = "cppapi_consolidation_domain_exp";
  remove_array(array_name);

  // Create array
  Context ctx;
  Domain domain(ctx);
  auto d = Dimension::create<int>(ctx, "d1", {{10, 110}}, 50);
  domain.add_dimensions(d);
  auto a = Attribute::create<float>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a);
  Array::create(array_name, schema);

  // Write
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  std::vector<float> a1(100);
  std::fill(a1.begin(), a1.end(), 1.0);
  std::vector<float> a2({2.0});

  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray({10, 109});
  query.set_data_buffer("a", a1);
  query.submit();

  query = Query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray({110, 110});
  query.set_data_buffer("a", a2);
  query.submit();
  array.close();

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  query_r.set_layout(TILEDB_ROW_MAJOR);
  query_r.set_subarray({10, 110});
  std::vector<float> a_r(101);
  query_r.set_data_buffer("a", a_r);
  query_r.submit();
  array_r.close();

  std::vector<float> c_a(100, 1.0f);
  c_a.push_back(2.0f);
  CHECK(a_r == c_a);

  // Consolidate
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, nullptr));

  // Read again
  Array array_c(ctx, array_name, TILEDB_READ);
  query_r = Query(ctx, array_c, TILEDB_READ);
  query_r.set_layout(TILEDB_ROW_MAJOR);
  query_r.set_subarray({10, 110});
  query_r.set_data_buffer("a", a_r);
  query_r.submit();
  array_c.close();
  CHECK(a_r == c_a);

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation without vacuum", "[cppapi][consolidation]") {
  std::string array_name = "cppapi_consolidation";
  remove_array(array_name);

  create_array(array_name);
  write_array(array_name, {1, 2}, {1, 2});
  write_array(array_name, {3, 3}, {3});
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  read_array(array_name, {1, 3}, {1, 2, 3});

  Context ctx;
  Config config;
  config["sm.consolidation.buffer_size"] = "4";
  REQUIRE_NOTHROW(Array::consolidate(ctx, array_name, &config));
  CHECK(tiledb::test::num_fragments(array_name) == 3);

  read_array(array_name, {1, 3}, {1, 2, 3});

  remove_array(array_name);
}

TEST_CASE(
    "C++ API: Test consolidation with timestamp and max domain",
    "[cppapi][consolidation][timestamp][maxdomain]") {
  Context ctx;
  VFS vfs(ctx);
  const std::string array_name = "consolidate_timestamp_max_domain";

  int64_t domain1[] = {std::numeric_limits<int64_t>::min() + 1,
                       std::numeric_limits<int64_t>::max()};
  const uint8_t domain2[2] = {0, 1};
  Domain domain(ctx);
  domain
      .add_dimension(
          Dimension::create(ctx, "d1", TILEDB_DATETIME_MS, domain1, nullptr))
      .add_dimension(
          Dimension::create(ctx, "d2", TILEDB_INT8, domain2, nullptr));

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);

  schema.add_attribute(Attribute::create<int64_t>(ctx, "a1"));

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
  Array::create(array_name, schema);

  std::vector<int64_t> d1 = {0};
  std::vector<int8_t> d2 = {0};
  std::vector<int64_t> a1 = {0};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d1", d1)
      .set_data_buffer("d2", d2)
      .set_data_buffer("a1", a1);
  query.submit();

  d2[0] = 1;
  a1[0] = 1;
  Query query2(ctx, array, TILEDB_WRITE);
  query2.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d1", d1)
      .set_data_buffer("d2", d2)
      .set_data_buffer("a1", a1);
  query2.submit();

  array.close();

  tiledb::Array::consolidate(ctx, array_name);

  std::vector<int64_t> d1_r(2);
  std::vector<int8_t> d2_r(2);
  std::vector<int64_t> a1_r(2);
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r);
  query_r.set_data_buffer("d1", d1_r)
      .set_data_buffer("d2", d2_r)
      .set_data_buffer("a1", a1_r);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);
  array_r.close();

  REQUIRE(d1_r[0] == 0);
  REQUIRE(d1_r[1] == 0);
  REQUIRE(d2_r[0] == 0);
  REQUIRE(d2_r[1] == 1);
  REQUIRE(a1_r[0] == 0);
  REQUIRE(a1_r[1] == 1);
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}