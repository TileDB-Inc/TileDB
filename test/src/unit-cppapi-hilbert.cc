/**
 * @file   unit-cppapi-hilbert.cc
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
 * Tests the C++ API for array related functions.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

void create_int32_array(const std::string& array_name) {
  Context ctx;
  Domain domain(ctx);
  auto d1 = Dimension::create<int32_t>(ctx, "d1", {{0, 100}});
  auto d2 = Dimension::create<int32_t>(ctx, "d2", {{0, 200}});
  domain.add_dimensions(d1, d2);
  auto a = Attribute::create<int32_t>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  schema.set_cell_order(TILEDB_HILBERT);
  schema.set_capacity(2);
  CHECK_NOTHROW(schema.check());
  Array::create(array_name, schema);
}

void create_int32_array_negative_domain(const std::string& array_name) {
  Context ctx;
  Domain domain(ctx);
  auto d1 = Dimension::create<int32_t>(ctx, "d1", {{-50, 50}});
  auto d2 = Dimension::create<int32_t>(ctx, "d2", {{-100, 100}});
  domain.add_dimensions(d1, d2);
  auto a = Attribute::create<int32_t>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  schema.set_cell_order(TILEDB_HILBERT);
  schema.set_capacity(2);
  CHECK_NOTHROW(schema.check());
  Array::create(array_name, schema);
}

void create_float32_array(const std::string& array_name) {
  Context ctx;
  Domain domain(ctx);
  auto d1 = Dimension::create<float>(ctx, "d1", {{0.0, 1.0}});
  auto d2 = Dimension::create<float>(ctx, "d2", {{0.0, 2.0}});
  domain.add_dimensions(d1, d2);
  auto a = Attribute::create<int32_t>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  schema.set_cell_order(TILEDB_HILBERT);
  schema.set_capacity(2);
  CHECK_NOTHROW(schema.check());
  Array::create(array_name, schema);
}

void create_string_array(const std::string& array_name) {
  Context ctx;
  Domain domain(ctx);
  auto d1 = Dimension::create(ctx, "d1", TILEDB_STRING_ASCII, nullptr, nullptr);
  auto d2 = Dimension::create(ctx, "d2", TILEDB_STRING_ASCII, nullptr, nullptr);
  domain.add_dimensions(d1, d2);
  auto a = Attribute::create<int32_t>(ctx, "a");
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  schema.set_cell_order(TILEDB_HILBERT);
  schema.set_capacity(2);
  CHECK_NOTHROW(schema.check());
  Array::create(array_name, schema);
}

template <class T1, class T2>
void write_2d_array(
    const std::string& array_name,
    std::vector<T1>& buff_d1,
    std::vector<T2>& buff_d2,
    std::vector<int32_t>& buff_a,
    tiledb_layout_t layout) {
  Context ctx;
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_buffer("a", buff_a);
  query_w.set_buffer("d1", buff_d1);
  query_w.set_buffer("d2", buff_d2);
  query_w.set_layout(layout);
  CHECK_NOTHROW(query_w.submit());
  array_w.close();
}

void write_2d_array(
    const std::string& array_name,
    std::vector<uint64_t>& off_d1,
    std::string& buff_d1,
    std::vector<uint64_t>& off_d2,
    std::string& buff_d2,
    std::vector<int32_t>& buff_a,
    tiledb_layout_t layout) {
  Context ctx;
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_buffer("a", buff_a);
  query_w.set_buffer("d1", off_d1, buff_d1);
  query_w.set_buffer("d2", off_d2, buff_d2);
  query_w.set_layout(layout);
  CHECK_NOTHROW(query_w.submit());
  array_w.close();
}

TEST_CASE("C++ API: Test Hilbert, errors", "[cppapi][hilbert][error]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Hilbert not applicable to dense
  {
    Domain domain(ctx);
    auto d1 = Dimension::create<int32_t>(ctx, "d1", {{0, 100}});
    auto d2 = Dimension::create<int32_t>(ctx, "d2", {{0, 200}}, 10);
    domain.add_dimensions(d1, d2);
    auto a = Attribute::create<int32_t>(ctx, "a");
    ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain);
    schema.add_attribute(a);
    CHECK_THROWS(schema.set_cell_order(TILEDB_HILBERT));

    // Hilbert order only applicable to cells
    CHECK_THROWS(schema.set_tile_order(TILEDB_HILBERT));
  }

  // Check maximum dimensions
  {
    Domain domain(ctx);
    auto d1 = Dimension::create<int32_t>(ctx, "d1", {{0, 100}});
    auto d2 = Dimension::create<int32_t>(ctx, "d2", {{0, 200}});
    auto d3 = Dimension::create<int32_t>(ctx, "d3", {{0, 200}});
    auto d4 = Dimension::create<int32_t>(ctx, "d4", {{0, 200}});
    auto d5 = Dimension::create<int32_t>(ctx, "d5", {{0, 200}});
    auto d6 = Dimension::create<int32_t>(ctx, "d6", {{0, 200}});
    auto d7 = Dimension::create<int32_t>(ctx, "d7", {{0, 200}});
    auto d8 = Dimension::create<int32_t>(ctx, "d8", {{0, 200}});
    auto d9 = Dimension::create<int32_t>(ctx, "d9", {{0, 200}});
    auto d10 = Dimension::create<int32_t>(ctx, "d10", {{0, 200}});
    auto d11 = Dimension::create<int32_t>(ctx, "d11", {{0, 200}});
    auto d12 = Dimension::create<int32_t>(ctx, "d12", {{0, 200}});
    auto d13 = Dimension::create<int32_t>(ctx, "d13", {{0, 200}});
    auto d14 = Dimension::create<int32_t>(ctx, "d14", {{0, 200}});
    auto d15 = Dimension::create<int32_t>(ctx, "d15", {{0, 200}});
    auto d16 = Dimension::create<int32_t>(ctx, "d16", {{0, 200}});
    auto d17 = Dimension::create<int32_t>(ctx, "d17", {{0, 200}});
    domain.add_dimensions(
        d1,
        d2,
        d3,
        d4,
        d5,
        d6,
        d7,
        d8,
        d9,
        d10,
        d11,
        d12,
        d13,
        d14,
        d15,
        d16,
        d17);
    auto a = Attribute::create<int32_t>(ctx, "a");
    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.add_attribute(a);
    CHECK_NOTHROW(schema.set_cell_order(TILEDB_HILBERT));
    CHECK_THROWS(schema.check());
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array(array_name);

  // Hilbert order not applicable to write queries
  Array array_w(ctx, array_name, TILEDB_WRITE);
  std::vector<int32_t> buff_a = {3, 2, 1, 4};
  std::vector<int32_t> buff_d1 = {1, 1, 4, 5};
  std::vector<int32_t> buff_d2 = {1, 3, 2, 4};
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_buffer("a", buff_a);
  query_w.set_buffer("d1", buff_d1);
  query_w.set_buffer("d2", buff_d2);
  CHECK_THROWS(query_w.set_layout(TILEDB_HILBERT));
  array_w.close();

  // Hilbert order not applicable to read queries
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(4);
  std::vector<int32_t> r_buff_d1(4);
  std::vector<int32_t> r_buff_d2(4);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  CHECK_THROWS(query_r.set_layout(TILEDB_HILBERT));
  array_r.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, test 2D, int32, write unordered, read global",
    "[cppapi][hilbert][2d][int32]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array(array_name);

  // Write array
  std::vector<int32_t> buff_a = {3, 2, 1, 4};
  std::vector<int32_t> buff_d1 = {1, 1, 4, 5};
  std::vector<int32_t> buff_d2 = {1, 3, 2, 4};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Read
  SECTION("- Global order") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(4);
    std::vector<int32_t> r_buff_d1(4);
    std::vector<int32_t> r_buff_d2(4);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {2, 3, 4, 1};
    std::vector<int32_t> c_buff_d1 = {1, 1, 5, 4};
    std::vector<int32_t> c_buff_d2 = {3, 1, 4, 2};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  SECTION("- Row-major") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(4);
    std::vector<int32_t> r_buff_d1(4);
    std::vector<int32_t> r_buff_d2(4);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_ROW_MAJOR);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {3, 2, 1, 4};
    std::vector<int32_t> c_buff_d1 = {1, 1, 4, 5};
    std::vector<int32_t> c_buff_d2 = {1, 3, 2, 4};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  SECTION("- Col-major") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(4);
    std::vector<int32_t> r_buff_d1(4);
    std::vector<int32_t> r_buff_d2(4);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_COL_MAJOR);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {3, 1, 2, 4};
    std::vector<int32_t> c_buff_d1 = {1, 4, 1, 5};
    std::vector<int32_t> c_buff_d2 = {1, 2, 3, 4};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  // Read
  SECTION("- Unordered") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(4);
    std::vector<int32_t> r_buff_d1(4);
    std::vector<int32_t> r_buff_d2(4);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_UNORDERED);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {2, 3, 4, 1};
    std::vector<int32_t> c_buff_d1 = {1, 1, 5, 4};
    std::vector<int32_t> c_buff_d2 = {3, 1, 4, 2};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, int32, 2D, partitioner",
    "[cppapi][hilbert][2d][int32][partitioner]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array(array_name);

  // Write array
  std::vector<int32_t> buff_d1 = {1, 1, 4, 5};
  std::vector<int32_t> buff_d2 = {1, 3, 2, 4};
  std::vector<int32_t> buff_a = {3, 2, 1, 4};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  SECTION("- entire domain") {
    // Read array
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(2);
    std::vector<int32_t> r_buff_d1(2);
    std::vector<int32_t> r_buff_d2(2);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());

    // Check results
    CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    std::vector<int32_t> c_buff_a = {2, 3};
    std::vector<int32_t> c_buff_d1 = {1, 1};
    std::vector<int32_t> c_buff_d2 = {3, 1};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    // Read again
    CHECK_NOTHROW(query_r.submit());

    // Check results again
    CHECK(
        query_r.query_status() == (test::use_refactored_readers() ?
                                       Query::Status::COMPLETE :
                                       Query::Status::INCOMPLETE));
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    c_buff_a = {4, 1};
    c_buff_d1 = {5, 4};
    c_buff_d2 = {4, 2};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    /**
     * Old reader needs an extra round here to finish processing all the
     * partitions in the subarray. New reader is done earlier.
     */
    if (!test::use_refactored_readers()) {
      // Read until complete
      CHECK_NOTHROW(query_r.submit());
      CHECK(query_r.query_status() == Query::Status::COMPLETE);
      CHECK(query_r.result_buffer_elements()["a"].second == 0);
    }

    array_r.close();
  }

  SECTION("- subarray") {
    // Read array
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(2);
    std::vector<int32_t> r_buff_d1(2);
    std::vector<int32_t> r_buff_d2(2);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    query_r.set_subarray({1, 5, 1, 7});
    CHECK_NOTHROW(query_r.submit());

    // Check results
    CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    std::vector<int32_t> c_buff_a = {2, 3};
    std::vector<int32_t> c_buff_d1 = {1, 1};
    std::vector<int32_t> c_buff_d2 = {3, 1};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    // Read again
    CHECK_NOTHROW(query_r.submit());

    // Check results again
    CHECK(query_r.query_status() == Query::Status::COMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    c_buff_a = {4, 1};
    c_buff_d1 = {5, 4};
    c_buff_d2 = {4, 2};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    array_r.close();
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, test writing in global order",
    "[cppapi][hilbert][write][global-order]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array(array_name);

  // Write array
  std::vector<int32_t> buff_a = {3, 2, 1, 4};
  std::vector<int32_t> buff_d1 = {1, 1, 4, 5};
  std::vector<int32_t> buff_d2 = {1, 3, 2, 4};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_buffer("a", buff_a);
  query_w.set_buffer("d1", buff_d1);
  query_w.set_buffer("d2", buff_d2);
  query_w.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_THROWS(query_w.submit());

  // Write correctly
  buff_a = {2, 3, 4, 1};
  buff_d1 = {1, 1, 5, 4};
  buff_d2 = {3, 1, 4, 2};
  CHECK_NOTHROW(query_w.submit());
  query_w.finalize();
  array_w.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, slicing", "[cppapi][hilbert][read][slicing]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array(array_name);

  // Write array
  std::vector<int32_t> buff_a = {3, 2, 4, 1};
  std::vector<int32_t> buff_d1 = {1, 1, 5, 4};
  std::vector<int32_t> buff_d2 = {1, 3, 4, 2};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  SECTION("- Row-major") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(3);
    std::vector<int32_t> r_buff_d1(3);
    std::vector<int32_t> r_buff_d2(3);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_subarray({1, 4, 1, 4});
    query_r.set_layout(TILEDB_ROW_MAJOR);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {3, 2, 1};
    std::vector<int32_t> c_buff_d1 = {1, 1, 4};
    std::vector<int32_t> c_buff_d2 = {1, 3, 2};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  SECTION("- Global order") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(3);
    std::vector<int32_t> r_buff_d1(3);
    std::vector<int32_t> r_buff_d2(3);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_subarray({1, 4, 1, 4});
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {2, 3, 1};
    std::vector<int32_t> c_buff_d1 = {1, 1, 4};
    std::vector<int32_t> c_buff_d2 = {3, 1, 2};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, multiple fragments, read in global order",
    "[cppapi][hilbert][read][multiple-fragments][global-order]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {3, 2, 4, 1};
  std::vector<int32_t> buff_d1 = {1, 1, 5, 4};
  std::vector<int32_t> buff_d2 = {1, 3, 4, 2};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Write second fragment
  buff_a = {5, 6, 7, 8};
  buff_d1 = {2, 2, 3, 7};
  buff_d2 = {1, 2, 7, 7};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  std::vector<int32_t> r_buff_d1(8);
  std::vector<int32_t> r_buff_d2(8);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  array_r.close();

  // Check results. Here is the hilbert value order:
  // (1, 3) ->   673795214387276
  // (1, 1) ->   972175364522868
  // (2, 1) ->  1282377960629798
  // (2, 2) ->  2093929125029754
  // (3, 7) ->  8953131325824998
  // (5, 4) -> 14307296941447292
  // (4, 2) -> 15960414315352633
  // (7, 7) -> 34410827116042986
  std::vector<int32_t> c_buff_a = {2, 3, 5, 6, 7, 4, 1, 8};
  std::vector<int32_t> c_buff_d1 = {1, 1, 2, 2, 3, 5, 4, 7};
  std::vector<int32_t> c_buff_d2 = {3, 1, 1, 2, 7, 4, 2, 7};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_buff_d2 == c_buff_d2);

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, int32, unsplittable",
    "[cppapi][hilbert][read][2d][int32][unsplittable]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {3, 2, 4, 1};
  std::vector<int32_t> buff_d1 = {1, 1, 5, 4};
  std::vector<int32_t> buff_d2 = {1, 3, 4, 2};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  int32_t r_buff_d1[2];
  int32_t r_buff_d2[2];
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1, 0);
  query_r.set_buffer("d2", r_buff_d2, 0);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 0);
  array_r.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, consolidation",
    "[cppapi][hilbert][consolidation]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {3, 2, 4, 1};
  std::vector<int32_t> buff_d1 = {1, 1, 5, 4};
  std::vector<int32_t> buff_d2 = {1, 3, 4, 2};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Write second fragment
  buff_a = {5, 6, 7, 8};
  buff_d1 = {2, 2, 3, 7};
  buff_d2 = {1, 2, 7, 7};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Consolidate and vacuum
  Config config;
  config["sm.consolidation.mode"] = "fragments";
  config["sm.vacuum.mode"] = "fragments";
  CHECK_NOTHROW(Array::consolidate(ctx, array_name, &config));
  CHECK_NOTHROW(Array::vacuum(ctx, array_name, &config));
  auto contents = vfs.ls(array_name);
  CHECK(contents.size() == 5);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  std::vector<int32_t> r_buff_d1(8);
  std::vector<int32_t> r_buff_d2(8);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  array_r.close();

  // Check results. Here is the hilbert value order:
  // (1, 3) ->   673795214387276
  // (1, 1) ->   972175364522868
  // (2, 1) ->  1282377960629798
  // (2, 2) ->  2093929125029754
  // (3, 7) ->  8953131325824998
  // (5, 4) -> 14307296941447292
  // (4, 2) -> 15960414315352633
  // (7, 7) -> 34410827116042986
  std::vector<int32_t> c_buff_a = {2, 3, 5, 6, 7, 4, 1, 8};
  std::vector<int32_t> c_buff_d1 = {1, 1, 2, 2, 3, 5, 4, 7};
  std::vector<int32_t> c_buff_d2 = {3, 1, 1, 2, 7, 4, 2, 7};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_buff_d2 == c_buff_d2);

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2D, int32, negative, read/write in global order",
    "[cppapi][hilbert][int32][negative][write][read][global-order]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array_negative_domain(array_name);

  // Write array
  std::vector<int32_t> buff_a = {3, 2, 1, 4};
  std::vector<int32_t> buff_d1 = {-49, -49, -46, -45};
  std::vector<int32_t> buff_d2 = {-99, -97, -98, -96};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_buffer("a", buff_a);
  query_w.set_buffer("d1", buff_d1);
  query_w.set_buffer("d2", buff_d2);
  query_w.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_THROWS(query_w.submit());

  // Write correctly
  buff_a = {2, 3, 4, 1};
  buff_d1 = {-49, -49, -45, -46};
  buff_d2 = {-97, -99, -96, -98};
  CHECK_NOTHROW(query_w.submit());
  query_w.finalize();
  array_w.close();

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(4);
  std::vector<int32_t> r_buff_d1(4);
  std::vector<int32_t> r_buff_d2(4);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());

  // Check results
  CHECK(query_r.query_status() == Query::Status::COMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 4);
  std::vector<int32_t> c_buff_a = {2, 3, 4, 1};
  std::vector<int32_t> c_buff_d1 = {-49, -49, -45, -46};
  std::vector<int32_t> c_buff_d2 = {-97, -99, -96, -98};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_buff_d2 == c_buff_d2);
  array_r.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, int32, negative, 2D, partitioner",
    "[cppapi][hilbert][2d][int32][negative][partitioner]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array_negative_domain(array_name);

  // Write array
  std::vector<int32_t> buff_d1 = {-49, -49, -46, -45};
  std::vector<int32_t> buff_d2 = {-99, -97, -98, -96};
  std::vector<int32_t> buff_a = {3, 2, 1, 4};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  SECTION("- entire domain") {
    // Read array
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(2);
    std::vector<int32_t> r_buff_d1(2);
    std::vector<int32_t> r_buff_d2(2);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());

    // Check results
    CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    std::vector<int32_t> c_buff_a = {2, 3};
    std::vector<int32_t> c_buff_d1 = {-49, -49};
    std::vector<int32_t> c_buff_d2 = {-97, -99};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    // Read again
    CHECK_NOTHROW(query_r.submit());

    // Check results again
    CHECK(
        query_r.query_status() == (test::use_refactored_readers() ?
                                       Query::Status::COMPLETE :
                                       Query::Status::INCOMPLETE));
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    c_buff_a = {4, 1};
    c_buff_d1 = {-45, -46};
    c_buff_d2 = {-96, -98};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    /**
     * Old reader needs an extra round here to finish processing all the
     * partitions in the subarray. New reader is done earlier.
     */
    if (!test::use_refactored_readers()) {
      // Read until complete
      CHECK_NOTHROW(query_r.submit());
      CHECK(query_r.query_status() == Query::Status::COMPLETE);
      CHECK(query_r.result_buffer_elements()["a"].second == 0);
    }

    array_r.close();
  }

  SECTION("- subarray") {
    // Read array
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(2);
    std::vector<int32_t> r_buff_d1(2);
    std::vector<int32_t> r_buff_d2(2);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    query_r.set_subarray({-49, -45, -99, -93});
    CHECK_NOTHROW(query_r.submit());

    // Check results
    CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    std::vector<int32_t> c_buff_a = {2, 3};
    std::vector<int32_t> c_buff_d1 = {-49, -49};
    std::vector<int32_t> c_buff_d2 = {-97, -99};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    // Read again
    CHECK_NOTHROW(query_r.submit());

    // Check results again
    CHECK(query_r.query_status() == Query::Status::COMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    c_buff_a = {4, 1};
    c_buff_d1 = {-45, -46};
    c_buff_d2 = {-96, -98};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    array_r.close();
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, int32, negative, slicing",
    "[cppapi][hilbert][2d][int32][negative][read][slicing]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array_negative_domain(array_name);

  // Write array
  std::vector<int32_t> buff_a = {3, 2, 4, 1};
  std::vector<int32_t> buff_d1 = {-49, -49, -45, -46};
  std::vector<int32_t> buff_d2 = {-99, -97, -96, -98};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  SECTION("- Row-major") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(3);
    std::vector<int32_t> r_buff_d1(3);
    std::vector<int32_t> r_buff_d2(3);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_subarray({-49, -46, -99, -96});
    query_r.set_layout(TILEDB_ROW_MAJOR);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {3, 2, 1};
    std::vector<int32_t> c_buff_d1 = {-49, -49, -46};
    std::vector<int32_t> c_buff_d2 = {-99, -97, -98};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  SECTION("- Global order") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(3);
    std::vector<int32_t> r_buff_d1(3);
    std::vector<int32_t> r_buff_d2(3);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_subarray({-49, -46, -99, -96});
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {2, 3, 1};
    std::vector<int32_t> c_buff_d1 = {-49, -49, -46};
    std::vector<int32_t> c_buff_d2 = {-97, -99, -98};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, int32, negative, multiple fragments, read in "
    "global order",
    "[cppapi][hilbert][2d][int32][negative][read][multiple-fragments][global-"
    "order]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array_negative_domain(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {3, 2, 4, 1};
  std::vector<int32_t> buff_d1 = {-49, -49, -45, -46};
  std::vector<int32_t> buff_d2 = {-99, -97, -96, -98};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Write second fragment
  buff_a = {5, 6, 7, 8};
  buff_d1 = {-48, -48, -47, -43};
  buff_d2 = {-99, -98, -93, -93};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  std::vector<int32_t> r_buff_d1(8);
  std::vector<int32_t> r_buff_d2(8);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  array_r.close();

  // Check results. Here is the hilbert value order:
  std::vector<int32_t> c_buff_a = {2, 3, 5, 6, 7, 4, 1, 8};
  std::vector<int32_t> c_buff_d1 = {-49, -49, -48, -48, -47, -45, -46, -43};
  std::vector<int32_t> c_buff_d2 = {-97, -99, -99, -98, -93, -96, -98, -93};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_buff_d2 == c_buff_d2);

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, int32, negative, consolidation",
    "[cppapi][hilbert][2d][int32][negative][consolidation]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array_negative_domain(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {3, 2, 4, 1};
  std::vector<int32_t> buff_d1 = {-49, -49, -45, -46};
  std::vector<int32_t> buff_d2 = {-99, -97, -96, -98};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Write second fragment
  buff_a = {5, 6, 7, 8};
  buff_d1 = {-48, -48, -47, -43};
  buff_d2 = {-99, -98, -93, -93};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Consolidate and vacuum
  Config config;
  config["sm.consolidation.mode"] = "fragments";
  config["sm.vacuum.mode"] = "fragments";
  CHECK_NOTHROW(Array::consolidate(ctx, array_name, &config));
  CHECK_NOTHROW(Array::vacuum(ctx, array_name, &config));
  auto contents = vfs.ls(array_name);
  CHECK(contents.size() == 5);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  std::vector<int32_t> r_buff_d1(8);
  std::vector<int32_t> r_buff_d2(8);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  array_r.close();

  // Check results. Here is the hilbert value order:
  std::vector<int32_t> c_buff_a = {2, 3, 5, 6, 7, 4, 1, 8};
  std::vector<int32_t> c_buff_d1 = {-49, -49, -48, -48, -47, -45, -46, -43};
  std::vector<int32_t> c_buff_d2 = {-97, -99, -99, -98, -93, -96, -98, -93};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_buff_d2 == c_buff_d2);

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, int32, negative, unsplittable",
    "[cppapi][hilbert][read][2d][int32][negative][unsplittable]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_int32_array_negative_domain(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {3, 2, 4, 1};
  std::vector<int32_t> buff_d1 = {-49, -49, -45, -46};
  std::vector<int32_t> buff_d2 = {-99, -97, -96, -98};
  write_2d_array<int32_t, int32_t>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  int32_t r_buff_d1[2];
  int32_t r_buff_d2[2];
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1, 0);
  query_r.set_buffer("d2", r_buff_d2, 0);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 0);
  array_r.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2D, float32, read/write in global order",
    "[cppapi][hilbert][float32][write][read][global-order]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_float32_array(array_name);

  // Write array
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  std::vector<float> buff_d1 = {0.1f, 0.1f, 0.4f, 0.5f};
  std::vector<float> buff_d2 = {0.3f, 0.1f, 0.2f, 0.4f};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_buffer("a", buff_a);
  query_w.set_buffer("d1", buff_d1);
  query_w.set_buffer("d2", buff_d2);
  query_w.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_THROWS(query_w.submit());

  // Write correctly
  buff_a = {3, 2, 1, 4};
  buff_d1 = {0.1f, 0.1f, 0.4f, 0.5f};
  buff_d2 = {0.1f, 0.3f, 0.2f, 0.4f};
  CHECK_NOTHROW(query_w.submit());
  query_w.finalize();
  array_w.close();

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(4);
  std::vector<float> r_buff_d1(4);
  std::vector<float> r_buff_d2(4);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());

  // Check results
  // Hilbert values:
  // (0.1f, 0.1f) ->  31040194354799722
  // (0.1f, 0.3f) -> 141289400074368426
  // (0.4f, 0.2f) -> 429519776226080170
  // (0.5f, 0.4f) -> 474732384249878186
  CHECK(query_r.query_status() == Query::Status::COMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 4);
  std::vector<int32_t> c_buff_a = {3, 2, 1, 4};
  std::vector<float> c_buff_d1 = {0.1f, 0.1f, 0.4f, 0.5f};
  std::vector<float> c_buff_d2 = {0.1f, 0.3f, 0.2f, 0.4f};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_buff_d2 == c_buff_d2);
  array_r.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, float32, 2D, partitioner",
    "[cppapi][hilbert][2d][float32][partitioner]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_float32_array(array_name);

  // Write array
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  std::vector<float> buff_d1 = {0.1f, 0.1f, 0.41f, 0.4f};
  std::vector<float> buff_d2 = {0.3f, 0.1f, 0.41f, 0.4f};
  write_2d_array<float, float>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  SECTION("- entire domain") {
    // Read array
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(2);
    std::vector<float> r_buff_d1(2);
    std::vector<float> r_buff_d2(2);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());

    // Check results
    CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    std::vector<int32_t> c_buff_a = {3, 2};
    std::vector<float> c_buff_d1 = {0.1f, 0.1f};
    std::vector<float> c_buff_d2 = {0.1f, 0.3f};
    CHECK(r_buff_a[0] == c_buff_a[0]);
    CHECK(r_buff_d1[0] == c_buff_d1[0]);
    CHECK(r_buff_d2[0] == c_buff_d2[0]);

    // Read again
    CHECK_NOTHROW(query_r.submit());

    // Check results again
    CHECK(
        query_r.query_status() == (test::use_refactored_readers() ?
                                       Query::Status::COMPLETE :
                                       Query::Status::INCOMPLETE));
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    c_buff_a = {1, 4};
    c_buff_d1 = {0.41f, 0.4f};
    c_buff_d2 = {0.41f, 0.4f};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    /**
     * Old reader needs an extra round here to finish processing all the
     * partitions in the subarray. New reader is done earlier.
     */
    if (!test::use_refactored_readers()) {
      // Read until complete
      CHECK_NOTHROW(query_r.submit());
      CHECK(query_r.query_status() == Query::Status::COMPLETE);
      CHECK(query_r.result_buffer_elements()["a"].second == 0);
    }

    array_r.close();
  }

  SECTION("- subarray") {
    // Read array
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(2);
    std::vector<float> r_buff_d1(2);
    std::vector<float> r_buff_d2(2);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    query_r.set_subarray({0.1f, 0.6f, 0.1f, 0.7f});
    CHECK_NOTHROW(query_r.submit());

    // Check results
    CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    std::vector<int32_t> c_buff_a = {3, 2};
    std::vector<float> c_buff_d1 = {0.1f, 0.1f};
    std::vector<float> c_buff_d2 = {0.1f, 0.3f};
    CHECK(r_buff_a[0] == c_buff_a[0]);
    CHECK(r_buff_d1[0] == c_buff_d1[0]);
    CHECK(r_buff_d2[0] == c_buff_d2[0]);

    // Read again
    CHECK_NOTHROW(query_r.submit());

    // Check results again
    CHECK(
        query_r.query_status() == (test::use_refactored_readers() ?
                                       Query::Status::COMPLETE :
                                       Query::Status::INCOMPLETE));
    CHECK(query_r.result_buffer_elements()["a"].second == 2);
    c_buff_a = {1, 4};
    c_buff_d1 = {0.41f, 0.4f};
    c_buff_d2 = {0.41f, 0.4f};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);

    /**
     * Old reader needs an extra round here to finish processing all the
     * partitions in the subarray. New reader is done earlier.
     */
    if (!test::use_refactored_readers()) {
      // Read until complete
      CHECK_NOTHROW(query_r.submit());
      CHECK(query_r.query_status() == Query::Status::COMPLETE);
      CHECK(query_r.result_buffer_elements()["a"].second == 0);
    }

    array_r.close();
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, float32, slicing",
    "[cppapi][hilbert][2d][float32][read][slicing]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_float32_array(array_name);

  // Write array
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  std::vector<float> buff_d1 = {0.1f, 0.1f, 0.4f, 0.5f};
  std::vector<float> buff_d2 = {0.3f, 0.1f, 0.2f, 0.4f};
  write_2d_array<float, float>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  SECTION("- Col-major") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(3);
    std::vector<float> r_buff_d1(3);
    std::vector<float> r_buff_d2(3);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_subarray({0.1f, 0.4f, 0.1f, 0.6f});
    query_r.set_layout(TILEDB_COL_MAJOR);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {3, 1, 2};
    std::vector<float> c_buff_d1 = {0.1f, 0.4f, 0.1f};
    std::vector<float> c_buff_d2 = {0.1f, 0.2f, 0.3f};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  SECTION("- Global order") {
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(3);
    std::vector<float> r_buff_d1(3);
    std::vector<float> r_buff_d2(3);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_buff_d1);
    query_r.set_buffer("d2", r_buff_d2);
    query_r.set_subarray({0.1f, 0.4f, 0.1f, 0.6f});
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());
    array_r.close();

    // Check results
    std::vector<int32_t> c_buff_a = {3, 2, 1};
    std::vector<float> c_buff_d1 = {0.1f, 0.1f, 0.4f};
    std::vector<float> c_buff_d2 = {0.1f, 0.3f, 0.2f};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_buff_d2 == c_buff_d2);
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, floa32, multiple fragments, read in "
    "global order",
    "[cppapi][hilbert][2d][float32][read][multiple-fragments][global-"
    "order]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_float32_array(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  std::vector<float> buff_d1 = {0.1f, 0.1f, 0.4f, 0.5f};
  std::vector<float> buff_d2 = {0.3f, 0.1f, 0.2f, 0.4f};
  write_2d_array<float, float>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Write second fragment
  buff_a = {5, 6, 7, 8};
  buff_d1 = {0.2f, 0.2f, 0.3f, 0.7f};
  buff_d2 = {0.2f, 0.1f, 0.7f, 0.7f};
  write_2d_array<float, float>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  std::vector<float> r_buff_d1(8);
  std::vector<float> r_buff_d2(8);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  array_r.close();

  // Check results. Here is the hilbert value order:
  // (0.1f, 0.1f) ->   31040194354799722
  // (0.1f, 0.3f) ->  141289400074368426
  // (0.2f, 0.2f) ->  230584300921369344
  // (0.2f, 0.1f) ->  276927224145762282
  // (0.4f, 0.2f) ->  429519776226080170
  // (0.5f, 0.4f) ->  474732384249878186
  // (0.3f, 0.7f) ->  607500946658220714
  // (0.7f, 0.7f) -> 4004185071769213610
  std::vector<int32_t> c_buff_a = {3, 2, 5, 6, 1, 4, 7, 8};
  std::vector<float> c_buff_d1 = {
      0.1f, 0.1f, 0.2f, 0.2f, 0.4f, 0.5f, 0.3f, 0.7f};
  std::vector<float> c_buff_d2 = {
      0.1f, 0.3f, 0.2f, 0.1f, 0.2f, 0.4f, 0.7f, 0.7f};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_buff_d2 == c_buff_d2);

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, float32, consolidation",
    "[cppapi][hilbert][2d][float32][consolidation]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_float32_array(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  std::vector<float> buff_d1 = {0.1f, 0.1f, 0.4f, 0.5f};
  std::vector<float> buff_d2 = {0.3f, 0.1f, 0.2f, 0.4f};
  write_2d_array<float, float>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Write second fragment
  buff_a = {5, 6, 7, 8};
  buff_d1 = {0.2f, 0.2f, 0.3f, 0.7f};
  buff_d2 = {0.2f, 0.1f, 0.7f, 0.7f};
  write_2d_array<float, float>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  // Consolidate and vacuum
  Config config;
  config["sm.consolidation.mode"] = "fragments";
  config["sm.vacuum.mode"] = "fragments";
  CHECK_NOTHROW(Array::consolidate(ctx, array_name, &config));
  CHECK_NOTHROW(Array::vacuum(ctx, array_name, &config));
  auto contents = vfs.ls(array_name);
  CHECK(contents.size() == 5);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  std::vector<float> r_buff_d1(8);
  std::vector<float> r_buff_d2(8);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1);
  query_r.set_buffer("d2", r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  array_r.close();

  // Check results. Here is the hilbert value order:
  // (0.1f, 0.1f) ->   31040194354799722
  // (0.1f, 0.3f) ->  141289400074368426
  // (0.2f, 0.2f) ->  230584300921369344
  // (0.2f, 0.1f) ->  276927224145762282
  // (0.4f, 0.2f) ->  429519776226080170
  // (0.5f, 0.4f) ->  474732384249878186
  // (0.3f, 0.7f) ->  607500946658220714
  // (0.7f, 0.7f) -> 4004185071769213610
  std::vector<int32_t> c_buff_a = {3, 2, 5, 6, 1, 4, 7, 8};
  std::vector<float> c_buff_d1 = {
      0.1f, 0.1f, 0.2f, 0.2f, 0.4f, 0.5f, 0.3f, 0.7f};
  std::vector<float> c_buff_d2 = {
      0.1f, 0.3f, 0.2f, 0.1f, 0.2f, 0.4f, 0.7f, 0.7f};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_buff_d2 == c_buff_d2);

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, float32, unsplittable",
    "[cppapi][hilbert][read][2d][float32][unsplittable]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_float32_array(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  std::vector<float> buff_d1 = {0.1f, 0.1f, 0.4f, 0.5f};
  std::vector<float> buff_d2 = {0.3f, 0.1f, 0.2f, 0.4f};
  write_2d_array<float, float>(
      array_name, buff_d1, buff_d2, buff_a, TILEDB_UNORDERED);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  float r_buff_d1[2];
  float r_buff_d2[2];
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_buff_d1, 0);
  query_r.set_buffer("d2", r_buff_d2, 0);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 0);
  array_r.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2D, string, read/write in global order",
    "[cppapi][hilbert][string][write][read][global-order]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_string_array(array_name);

  // Write array
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  std::string buff_d1("1adogcamel33");
  std::vector<uint64_t> off_d1 = {0, 2, 5, 10};
  std::string buff_d2("catstopstockt1");
  std::vector<uint64_t> off_d2 = {0, 3, 7, 12};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_buffer("a", buff_a);
  query_w.set_buffer("d1", off_d1, buff_d1);
  query_w.set_buffer("d2", off_d2, buff_d2);
  query_w.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_THROWS(query_w.submit());

  // Write correctly
  buff_d1 = std::string("dogcamel331a");
  off_d1 = {0, 3, 8, 10};
  buff_d2 = std::string("stopstockt1cat");
  off_d2 = {0, 4, 9, 11};
  query_w.set_buffer("d1", off_d1, buff_d1);
  query_w.set_buffer("d2", off_d2, buff_d2);
  CHECK_NOTHROW(query_w.submit());
  query_w.finalize();
  array_w.close();

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(4);
  std::string r_buff_d1;
  r_buff_d1.resize(20);
  std::vector<uint64_t> r_off_d1(4);
  std::string r_buff_d2;
  r_buff_d2.resize(20);
  std::vector<uint64_t> r_off_d2(4);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_off_d1, r_buff_d1);
  query_r.set_buffer("d2", r_off_d2, r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());

  // Check results. Hilbert values:
  // (dog, stop)    ->     785843883856635242
  // (camel, stock) ->     785914162406170797
  // (33, t1)       ->     877430626372812800
  // (1a, cat)      ->     919167533801450154
  CHECK(query_r.query_status() == Query::Status::COMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 4);
  r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
  r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
  std::vector<int32_t> c_buff_a = {2, 3, 1, 4};
  std::string c_buff_d1("dogcamel331a");
  std::vector<uint64_t> c_off_d1 = {0, 3, 8, 10};
  std::string c_buff_d2("stopstockt1cat");
  std::vector<uint64_t> c_off_d2 = {0, 4, 9, 11};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_off_d1 == c_off_d1);
  CHECK(r_buff_d2 == c_buff_d2);
  CHECK(r_off_d2 == c_off_d2);
  array_r.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, string, multiple fragments, read in "
    "global order",
    "[cppapi][hilbert][2d][string][read][multiple-fragments][global-"
    "order]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_string_array(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  auto buff_d1 = std::string("cameldog331a");
  std::vector<uint64_t> off_d1 = {0, 5, 8, 10};
  auto buff_d2 = std::string("stockstopt1cat");
  std::vector<uint64_t> off_d2 = {0, 5, 9, 11};
  write_2d_array(
      array_name, off_d1, buff_d1, off_d2, buff_d2, buff_a, TILEDB_UNORDERED);

  // Write second fragment
  buff_a = {5, 6, 7, 8};
  buff_d1 = std::string("blueazstarurn");
  off_d1 = {0, 4, 6, 10};
  buff_d2 = std::string("aceyellowredgrey");
  off_d2 = {0, 3, 9, 12};
  write_2d_array(
      array_name, off_d1, buff_d1, off_d2, buff_d2, buff_a, TILEDB_UNORDERED);

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  std::string r_buff_d1;
  r_buff_d1.resize(100);
  std::vector<uint64_t> r_off_d1(8);
  std::string r_buff_d2;
  r_buff_d2.resize(100);
  std::vector<uint64_t> r_off_d2(8);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_off_d1, r_buff_d1);
  query_r.set_buffer("d2", r_off_d2, r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  array_r.close();

  // Check results. Hilbert values:
  // (blue, ace)    ->     721526731798250756
  // (urn, grey)    ->     741275904800572752
  // (star, red)    ->     757250025264009195
  // (dog, stop)    ->     785843883856635242
  // (camel, stock) ->     785914162406170797
  // (az, yellow)   ->     788282729955763606
  // (33, t1)       ->     877430626372812800
  // (1a, cat)      ->     919167533801450154
  CHECK(query_r.query_status() == Query::Status::COMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 8);
  r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
  r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
  std::vector<int32_t> c_buff_a = {5, 8, 7, 3, 2, 6, 1, 4};
  std::string c_buff_d1("blueurnstardogcamelaz331a");
  std::vector<uint64_t> c_off_d1 = {0, 4, 7, 11, 14, 19, 21, 23};
  std::string c_buff_d2("acegreyredstopstockyellowt1cat");
  std::vector<uint64_t> c_off_d2 = {0, 3, 7, 10, 14, 19, 25, 27};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_off_d1 == c_off_d1);
  CHECK(r_buff_d2 == c_buff_d2);
  CHECK(r_off_d2 == c_off_d2);

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, string, consolidation",
    "[cppapi][hilbert][2d][string][consolidation]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_string_array(array_name);

  // Write first fragment
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  auto buff_d1 = std::string("cameldog331a");
  std::vector<uint64_t> off_d1 = {0, 5, 8, 10};
  auto buff_d2 = std::string("stockstopt1cat");
  std::vector<uint64_t> off_d2 = {0, 5, 9, 11};
  write_2d_array(
      array_name, off_d1, buff_d1, off_d2, buff_d2, buff_a, TILEDB_UNORDERED);

  // Write second fragment
  buff_a = {5, 6, 7, 8};
  buff_d1 = std::string("blueazstarurn");
  off_d1 = {0, 4, 6, 10};
  buff_d2 = std::string("aceyellowredgrey");
  off_d2 = {0, 3, 9, 12};
  write_2d_array(
      array_name, off_d1, buff_d1, off_d2, buff_d2, buff_a, TILEDB_UNORDERED);

  // Consolidate and vacuum
  Config config;
  config["sm.consolidation.mode"] = "fragments";
  config["sm.vacuum.mode"] = "fragments";
  CHECK_NOTHROW(Array::consolidate(ctx, array_name, &config));
  CHECK_NOTHROW(Array::vacuum(ctx, array_name, &config));
  auto contents = vfs.ls(array_name);
  CHECK(contents.size() == 5);

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(8);
  std::string r_buff_d1;
  r_buff_d1.resize(100);
  std::vector<uint64_t> r_off_d1(8);
  std::string r_buff_d2;
  r_buff_d2.resize(100);
  std::vector<uint64_t> r_off_d2(8);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_off_d1, r_buff_d1);
  query_r.set_buffer("d2", r_off_d2, r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  array_r.close();

  // Check results. Hilbert values:
  // (blue, ace)    ->     721526731798250756
  // (urn, grey)    ->     741275904800572752
  // (star, red)    ->     757250025264009195
  // (dog, stop)    ->     785843883856635242
  // (camel, stock) ->     785914162406170797
  // (az, yellow)   ->     788282729955763606
  // (33, t1)       ->     877430626372812800
  // (1a, cat)      ->     919167533801450154
  CHECK(query_r.query_status() == Query::Status::COMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 8);
  r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
  r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
  std::vector<int32_t> c_buff_a = {5, 8, 7, 3, 2, 6, 1, 4};
  std::string c_buff_d1("blueurnstardogcamelaz331a");
  std::vector<uint64_t> c_off_d1 = {0, 4, 7, 11, 14, 19, 21, 23};
  std::string c_buff_d2("acegreyredstopstockyellowt1cat");
  std::vector<uint64_t> c_off_d2 = {0, 3, 7, 10, 14, 19, 25, 27};
  CHECK(r_buff_a == c_buff_a);
  CHECK(r_buff_d1 == c_buff_d1);
  CHECK(r_off_d1 == c_off_d1);
  CHECK(r_buff_d2 == c_buff_d2);
  CHECK(r_off_d2 == c_off_d2);

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, string, slicing",
    "[cppapi][hilbert][2d][string][read][slicing]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_string_array(array_name);

  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  auto buff_d1 = std::string("cameldog331a");
  std::vector<uint64_t> off_d1 = {0, 5, 8, 10};
  auto buff_d2 = std::string("stockstopt1cat");
  std::vector<uint64_t> off_d2 = {0, 5, 9, 11};
  write_2d_array(
      array_name, off_d1, buff_d1, off_d2, buff_d2, buff_a, TILEDB_UNORDERED);

  SECTION("- Row-major") {
    // Read
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(4);
    std::string r_buff_d1;
    r_buff_d1.resize(20);
    std::vector<uint64_t> r_off_d1(4);
    std::string r_buff_d2;
    r_buff_d2.resize(20);
    std::vector<uint64_t> r_off_d2(4);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_off_d1, r_buff_d1);
    query_r.set_buffer("d2", r_off_d2, r_buff_d2);
    query_r.set_layout(TILEDB_ROW_MAJOR);
    query_r.add_range(0, std::string("3"), std::string("z"));
    query_r.add_range(1, std::string("a"), std::string("vase"));
    CHECK_NOTHROW(query_r.submit());

    // Check results
    CHECK(query_r.query_status() == Query::Status::COMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 3);
    r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
    r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
    r_off_d1.resize(query_r.result_buffer_elements()["d1"].first);
    r_off_d2.resize(query_r.result_buffer_elements()["d2"].first);
    r_buff_a.resize(query_r.result_buffer_elements()["a"].second);
    std::vector<int32_t> c_buff_a = {1, 2, 3};
    std::string c_buff_d1("33cameldog");
    std::vector<uint64_t> c_off_d1 = {0, 2, 7};
    std::string c_buff_d2("t1stockstop");
    std::vector<uint64_t> c_off_d2 = {0, 2, 7};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_off_d1 == c_off_d1);
    CHECK(r_buff_d2 == c_buff_d2);
    CHECK(r_off_d2 == c_off_d2);
  }

  SECTION("- Global order") {
    // Read
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(4);
    std::string r_buff_d1;
    r_buff_d1.resize(20);
    std::vector<uint64_t> r_off_d1(4);
    std::string r_buff_d2;
    r_buff_d2.resize(20);
    std::vector<uint64_t> r_off_d2(4);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_off_d1, r_buff_d1);
    query_r.set_buffer("d2", r_off_d2, r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    query_r.add_range(0, std::string("3"), std::string("z"));
    query_r.add_range(1, std::string("a"), std::string("vase"));
    CHECK_NOTHROW(query_r.submit());

    // Check results. Hilbert values:
    // (dog, stop)    ->     785843883856635242
    // (camel, stock) ->     785914162406170797
    // (33, t1)       ->     877430626372812800
    CHECK(query_r.query_status() == Query::Status::COMPLETE);
    CHECK(query_r.result_buffer_elements()["a"].second == 3);
    r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
    r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
    r_off_d1.resize(query_r.result_buffer_elements()["d1"].first);
    r_off_d2.resize(query_r.result_buffer_elements()["d2"].first);
    r_buff_a.resize(query_r.result_buffer_elements()["a"].second);
    std::vector<int32_t> c_buff_a = {3, 2, 1};
    std::string c_buff_d1("dogcamel33");
    std::vector<uint64_t> c_off_d1 = {0, 3, 8};
    std::string c_buff_d2("stopstockt1");
    std::vector<uint64_t> c_off_d2 = {0, 4, 9};
    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_off_d1 == c_off_d1);
    CHECK(r_buff_d2 == c_buff_d2);
    CHECK(r_off_d2 == c_off_d2);
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, string, 2D, partitioner",
    "[cppapi][hilbert][2d][string][partitioner]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_string_array(array_name);

  // Write
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  auto buff_d1 = std::string("cameldog331a");
  std::vector<uint64_t> off_d1 = {0, 5, 8, 10};
  auto buff_d2 = std::string("stockstopt1cat");
  std::vector<uint64_t> off_d2 = {0, 5, 9, 11};
  write_2d_array(
      array_name, off_d1, buff_d1, off_d2, buff_d2, buff_a, TILEDB_UNORDERED);

  SECTION("- entire domain") {
    // Read array
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(4);
    std::string r_buff_d1;
    r_buff_d1.resize(13);
    std::vector<uint64_t> r_off_d1(4);
    std::string r_buff_d2;
    r_buff_d2.resize(13);
    std::vector<uint64_t> r_off_d2(4);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_off_d1, r_buff_d1);
    query_r.set_buffer("d2", r_off_d2, r_buff_d2);
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());

    // Check results
    // (dog, stop)    ->     785843883856635242
    // (camel, stock) ->     785914162406170797
    // (33, t1)       ->     877430626372812800
    // (1a, cat)      ->     919167533801450154
    CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
    r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
    r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
    r_off_d1.resize(query_r.result_buffer_elements()["d1"].first);
    r_off_d2.resize(query_r.result_buffer_elements()["d2"].first);
    r_buff_a.resize(query_r.result_buffer_elements()["a"].second);
    std::vector<int32_t> c_buff_a;
    std::string c_buff_d1;
    std::vector<uint64_t> c_off_d1;
    std::string c_buff_d2;
    std::vector<uint64_t> c_off_d2;

    /**
     * Refactored reader tries to fill as much as possible.
     * Old reader splits partition in two.
     */
    if (test::use_refactored_readers()) {
      CHECK(query_r.result_buffer_elements()["a"].second == 3);
      c_buff_a = {3, 2, 1};
      c_buff_d1 = std::string("dogcamel33");
      c_off_d1 = {0, 3, 8};
      c_buff_d2 = std::string("stopstockt1");
      c_off_d2 = {0, 4, 9};
    } else {
      CHECK(query_r.result_buffer_elements()["a"].second == 2);
      c_buff_a = {3, 2};
      c_buff_d1 = std::string("dogcamel");
      c_off_d1 = {0, 3};
      c_buff_d2 = std::string("stopstock");
      c_off_d2 = {0, 4};
    }

    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_off_d1 == c_off_d1);
    CHECK(r_buff_d2 == c_buff_d2);
    CHECK(r_off_d2 == c_off_d2);

    // Read again
    CHECK_NOTHROW(query_r.submit());

    // Check results
    // (dog, stop)    ->     785843883856635242
    // (camel, stock) ->     785914162406170797
    // (33, t1)       ->     877430626372812800
    // (1a, cat)      ->     919167533801450154
    CHECK(query_r.query_status() == Query::Status::COMPLETE);
    r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
    r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
    r_off_d1.resize(query_r.result_buffer_elements()["d1"].first);
    r_off_d2.resize(query_r.result_buffer_elements()["d2"].first);
    r_buff_a.resize(query_r.result_buffer_elements()["a"].second);

    if (test::use_refactored_readers()) {
      CHECK(query_r.result_buffer_elements()["a"].second == 1);
      c_buff_a = {4};
      c_buff_d1 = std::string("1a");
      c_off_d1 = {0};
      c_buff_d2 = std::string("cat");
      c_off_d2 = {0};
    } else {
      CHECK(query_r.result_buffer_elements()["a"].second == 2);
      c_buff_a = {1, 4};
      c_buff_d1 = std::string("331a");
      c_off_d1 = {0, 2};
      c_buff_d2 = std::string("t1cat");
      c_off_d2 = {0, 2};
    }

    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_off_d1 == c_off_d1);
    CHECK(r_buff_d2 == c_buff_d2);
    CHECK(r_off_d2 == c_off_d2);

    array_r.close();
  }

  SECTION("- subarray") {
    // Read array
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r, TILEDB_READ);
    std::vector<int32_t> r_buff_a(4);
    std::string r_buff_d1;
    r_buff_d1.resize(13);
    std::vector<uint64_t> r_off_d1(4);
    std::string r_buff_d2;
    r_buff_d2.resize(13);
    std::vector<uint64_t> r_off_d2(4);
    query_r.set_buffer("a", r_buff_a);
    query_r.set_buffer("d1", r_off_d1, r_buff_d1);
    query_r.set_buffer("d2", r_off_d2, r_buff_d2);
    query_r.add_range(0, std::string("1a", 2), std::string("w", 1));
    query_r.add_range(1, std::string("ca", 2), std::string("t1", 2));
    query_r.set_layout(TILEDB_GLOBAL_ORDER);
    CHECK_NOTHROW(query_r.submit());

    // Check results
    // (dog, stop)    ->     785843883856635242
    // (camel, stock) ->     785914162406170797
    // (33, t1)       ->     877430626372812800
    // (1a, cat)      ->     919167533801450154
    CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
    r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
    r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
    r_off_d1.resize(query_r.result_buffer_elements()["d1"].first);
    r_off_d2.resize(query_r.result_buffer_elements()["d2"].first);
    r_buff_a.resize(query_r.result_buffer_elements()["a"].second);
    std::vector<int32_t> c_buff_a;
    std::string c_buff_d1;
    std::vector<uint64_t> c_off_d1;
    std::string c_buff_d2;
    std::vector<uint64_t> c_off_d2;

    /**
     * Refactored reader tries to fill as much as possible.
     * Old reader splits partition in two.
     */
    if (test::use_refactored_readers()) {
      CHECK(query_r.result_buffer_elements()["a"].second == 3);
      c_buff_a = {3, 2, 1};
      c_buff_d1 = std::string("dogcamel33");
      c_off_d1 = {0, 3, 8};
      c_buff_d2 = std::string("stopstockt1");
      c_off_d2 = {0, 4, 9};
    } else {
      CHECK(query_r.result_buffer_elements()["a"].second == 2);
      c_buff_a = {3, 2};
      c_buff_d1 = std::string("dogcamel");
      c_off_d1 = {0, 3};
      c_buff_d2 = std::string("stopstock");
      c_off_d2 = {0, 4};
    }

    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_off_d1 == c_off_d1);
    CHECK(r_buff_d2 == c_buff_d2);
    CHECK(r_off_d2 == c_off_d2);

    // Read again
    CHECK_NOTHROW(query_r.submit());

    // Check results
    // (dog, stop)    ->     785843883856635242
    // (camel, stock) ->     785914162406170797
    // (33, t1)       ->     877430626372812800
    // (1a, cat)      ->     919167533801450154
    CHECK(query_r.query_status() == Query::Status::COMPLETE);
    r_buff_d1.resize(query_r.result_buffer_elements()["d1"].second);
    r_buff_d2.resize(query_r.result_buffer_elements()["d2"].second);
    r_off_d1.resize(query_r.result_buffer_elements()["d1"].first);
    r_off_d2.resize(query_r.result_buffer_elements()["d2"].first);
    r_buff_a.resize(query_r.result_buffer_elements()["a"].second);

    if (test::use_refactored_readers()) {
      CHECK(query_r.result_buffer_elements()["a"].second == 1);
      c_buff_a = {4};
      c_buff_d1 = std::string("1a");
      c_off_d1 = {0};
      c_buff_d2 = std::string("cat");
      c_off_d2 = {0};
    } else {
      CHECK(query_r.result_buffer_elements()["a"].second == 2);
      c_buff_a = {1, 4};
      c_buff_d1 = std::string("331a");
      c_off_d1 = {0, 2};
      c_buff_d2 = std::string("t1cat");
      c_off_d2 = {0, 2};
    }

    CHECK(r_buff_a == c_buff_a);
    CHECK(r_buff_d1 == c_buff_d1);
    CHECK(r_off_d1 == c_off_d1);
    CHECK(r_buff_d2 == c_buff_d2);
    CHECK(r_off_d2 == c_off_d2);

    array_r.close();
  }

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test Hilbert, 2d, string, unsplittable",
    "[cppapi][hilbert][read][2d][string][unsplittable]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "hilbert_array";

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  // Create array
  create_string_array(array_name);

  // Write
  std::vector<int32_t> buff_a = {2, 3, 1, 4};
  auto buff_d1 = std::string("cameldog331a");
  std::vector<uint64_t> off_d1 = {0, 5, 8, 10};
  auto buff_d2 = std::string("stockstopt1cat");
  std::vector<uint64_t> off_d2 = {0, 5, 9, 11};
  write_2d_array(
      array_name, off_d1, buff_d1, off_d2, buff_d2, buff_a, TILEDB_UNORDERED);

  // Read
  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r, TILEDB_READ);
  std::vector<int32_t> r_buff_a(1);
  std::string r_buff_d1;
  r_buff_d1.resize(1);
  std::vector<uint64_t> r_off_d1(1);
  std::string r_buff_d2;
  r_buff_d2.resize(1);
  std::vector<uint64_t> r_off_d2(1);
  query_r.set_buffer("a", r_buff_a);
  query_r.set_buffer("d1", r_off_d1, r_buff_d1);
  query_r.set_buffer("d2", r_off_d2, r_buff_d2);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  CHECK_NOTHROW(query_r.submit());
  CHECK(query_r.query_status() == Query::Status::INCOMPLETE);
  CHECK(query_r.result_buffer_elements()["a"].second == 0);
  array_r.close();

  // Remove array
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));
}
