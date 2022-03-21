/**
 * @file   unit-cppapi-subarray.cc
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
 * Tests the C++ API for subarray related functions.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;
using namespace tiledb::test;

TEST_CASE("C++ API: Test subarray", "[cppapi][sparse][subarray]") {
  const std::string array_name = "cpp_unit_array";
  tiledb::Config cfg;
  cfg.set("config.logging_level", "3");
  tiledb::Context ctx(cfg);
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  tiledb::Domain domain(ctx);
  domain.add_dimension(tiledb::Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(tiledb::Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(tiledb::Attribute::create<int>(ctx, "a"));
  tiledb::Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {1, 2, 3, 4};
  std::vector<int> coords_w = {0, 0, 1, 1, 2, 2, 3, 3};
  tiledb::Array array_w(ctx, array_name, TILEDB_WRITE);
  tiledb::Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", data_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  SECTION("- Read single cell - Subarray-query") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    int range[] = {0, 0};
    query.add_range(0, range[0], range[1]);
    query.add_range(1, range[0], range[1]);

    auto est_size = query.est_result_size("a");
    REQUIRE(est_size == 4);

    std::vector<int> data(est_size);
    query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 1);
    REQUIRE(data[0] == 1);
  }

  SECTION("- Read single cell - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
    int range[] = {0, 0};
    subarray.add_range(0, range[0], range[1]);
    subarray.add_range(1, range[0], range[1]);

    query.set_layout(TILEDB_ROW_MAJOR);
    query.set_subarray(subarray);
    auto est_size = query.est_result_size("a");
    REQUIRE(est_size == 4);

    std::vector<int> data(est_size);
    query.set_buffer("a", data);
    query.set_subarray(subarray);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 1);
    REQUIRE(data[0] == 1);
  }

  SECTION("- Read single range - Subarray-query") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    int range[] = {1, 2};
    query.add_range(0, range[0], range[1]).add_range(1, range[0], range[1]);

    auto est_size = query.est_result_size("a");
    REQUIRE(est_size == 4);
    std::array<uint64_t, 2> est_size_var;
    CHECK_THROWS(est_size_var = query.est_result_size_var("a"));

    std::vector<int> data(est_size);
    query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 2);
    REQUIRE(data[0] == 2);
    REQUIRE(data[1] == 3);
  }

  SECTION("- Read single range - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
    int range[] = {1, 2};
    subarray.add_range(0, range[0], range[1]).add_range(1, range[0], range[1]);

    query.set_subarray(subarray);
    auto est_size = query.est_result_size("a");
    REQUIRE(est_size == 4);
    std::array<uint64_t, 2> est_size_var;
    CHECK_THROWS(est_size_var = query.est_result_size_var("a"));

    std::vector<int> data(est_size);
    query.set_layout(TILEDB_ROW_MAJOR).set_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 2);
    REQUIRE(data[0] == 2);
    REQUIRE(data[1] == 3);
  }

  SECTION("- Read two cells - Subarray-query") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    int range0[] = {0, 0}, range1[] = {2, 2};
    query.add_range(0, range0[0], range0[1]);
    query.add_range(1, range0[0], range0[1]);
    query.add_range(0, range1[0], range1[1]);
    query.add_range(1, range1[0], range1[1]);

    int64_t inv_range[] = {0, 1};
    CHECK_THROWS(query.add_range(1, inv_range[0], inv_range[1]));

    // Get range
    auto range = query.range<int>(0, 0);
    CHECK(range[0] == 0);
    CHECK(range[1] == 0);
    CHECK(range[2] == 0);
    range = query.range<int>(1, 1);
    CHECK(range[0] == 2);
    CHECK(range[1] == 2);
    CHECK(range[2] == 0);

    CHECK_THROWS(range = query.range<int>(1, 3));
    std::array<int64_t, 3> range2 = {{0, 0, 0}};
    CHECK_THROWS(range2 = query.range<int64_t>(1, 1));

    auto est_size = query.est_result_size("a");
    REQUIRE(est_size == 4);

    std::vector<int> data(est_size);
    query.set_layout(TILEDB_UNORDERED).set_data_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 2);
    REQUIRE(data[0] == 1);
    REQUIRE(data[1] == 3);
  }

  SECTION("- Read two cells - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
    int range0[] = {0, 0}, range1[] = {2, 2};
    subarray.add_range(0, range0[0], range0[1]);
    subarray.add_range(1, range0[0], range0[1]);
    subarray.add_range(0, range1[0], range1[1]);
    subarray.add_range(1, range1[0], range1[1]);

    int64_t inv_range[] = {0, 1};
    CHECK_THROWS(subarray.add_range(1, inv_range[0], inv_range[1]));

    // Get range
    auto range = subarray.range<int>(0, 0);
    CHECK(range[0] == 0);
    CHECK(range[1] == 0);
    CHECK(range[2] == 0);
    range = subarray.range<int>(1, 1);
    CHECK(range[0] == 2);
    CHECK(range[1] == 2);
    CHECK(range[2] == 0);

    CHECK_THROWS(range = subarray.range<int>(1, 3));
    std::array<int64_t, 3> range2 = {{0, 0, 0}};
    CHECK_THROWS(range2 = subarray.range<int64_t>(1, 1));

    query.set_subarray(subarray);
    auto est_size = query.est_result_size("a");
    REQUIRE(est_size == 4);

    std::vector<int> data(est_size);
    query.set_layout(TILEDB_UNORDERED).set_buffer("a", data);
    query.set_subarray(subarray);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 2);
    REQUIRE(data[0] == 1);
    REQUIRE(data[1] == 3);
  }

  SECTION("- Read two regions - Subarray-query") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    int range0[] = {0, 1}, range1[] = {2, 3};
    query.add_range(0, range0[0], range0[1]);
    query.add_range(1, range0[0], range0[1]);
    query.add_range(0, range1[0], range1[1]);
    query.add_range(1, range1[0], range1[1]);

    auto est_size = query.est_result_size("a");
    std::vector<int> data(est_size);
    query.set_layout(TILEDB_UNORDERED).set_data_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 4);
    REQUIRE(data[0] == 1);
    REQUIRE(data[1] == 2);
    REQUIRE(data[2] == 3);
    REQUIRE(data[3] == 4);
  }

  SECTION("- Read two regions - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
    int range0[] = {0, 1}, range1[] = {2, 3};
    subarray.add_range(0, range0[0], range0[1]);
    subarray.add_range(1, range0[0], range0[1]);
    subarray.add_range(0, range1[0], range1[1]);
    subarray.add_range(1, range1[0], range1[1]);

    query.set_subarray(subarray);
    auto est_size = query.est_result_size("a");
    std::vector<int> data(est_size);
    query.set_layout(TILEDB_UNORDERED).set_buffer("a", data);
    query.set_subarray(subarray);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 4);
    REQUIRE(data[0] == 1);
    REQUIRE(data[1] == 2);
    REQUIRE(data[2] == 3);
    REQUIRE(data[3] == 4);
  }

  SECTION("- Read ranges oob error - Subarray-query") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    tiledb::Config config;
    config.set("sm.read_range_oob", "error");
    query.set_config(config);
    int range[] = {1, 4};
    int range2[] = {-1, 3};
    REQUIRE_THROWS(query.add_range(0, range[0], range[1]));
    REQUIRE_THROWS(query.add_range(1, range2[0], range2[1]));
  }

  SECTION("- Read ranges oob error - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Subarray subarray(ctx, array);
    tiledb::Config config;
    config.set("sm.read_range_oob", "error");
    subarray.set_config(config);
    int range[] = {1, 4};
    int range2[] = {-1, 3};
    REQUIRE_THROWS(subarray.add_range(0, range[0], range[1]));
    REQUIRE_THROWS(subarray.add_range(1, range2[0], range2[1]));
  }

  SECTION("-  set_subarray Write ranges oob error - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx, array);
    // default expected to err. (currently errs because sparse)
    REQUIRE_THROWS(subarray.set_subarray({1, 4, -1, 3}));
    tiledb::Config config;
    // The config option should be ignored but we set anyway.
    // (currently errs because sparse)
    config.set("sm.read_range_oob", "error");
    subarray.set_config(config);
    REQUIRE_THROWS(subarray.set_subarray({1, 4, -1, 3}));
  }

  SECTION("- Read ranges oob warn - Subarray-query") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    tiledb::Config config;
    config.set("sm.read_range_oob", "warn");
    query.set_config(config);
    int range[] = {1, 4};
    int range2[] = {-1, 3};
    query.add_range(0, range[0], range[1]);
    query.add_range(1, range2[0], range2[1]);

    auto est_size = query.est_result_size("a");
    REQUIRE(est_size == 12);
    std::array<uint64_t, 2> est_size_var;
    CHECK_THROWS(est_size_var = query.est_result_size_var("a"));

    std::vector<int> data(est_size);
    query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 3);
    REQUIRE(data[0] == 2);
    REQUIRE(data[1] == 3);
  }

  SECTION("- set_subarray  Write ranges oob warn - Subarray-query") {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    // throws on sparse array (set_subarray not supported)
    REQUIRE_THROWS(query.set_subarray({1, 4, -1, 3}));
    tiledb::Config config;
    config.set("sm.read_range_oob", "warn");
    query.set_config(config);
    // throws on sparse array
    REQUIRE_THROWS(query.set_subarray({1, 4, -1, 3}));
  }

  SECTION("- Read ranges oob warn - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Subarray subarray(ctx, array);
    tiledb::Config config;
    config.set("sm.read_range_oob", "warn");
    subarray.set_config(config);
    int range[] = {1, 4};
    int range2[] = {-1, 3};
    subarray.add_range(0, range[0], range[1]);
    subarray.add_range(1, range2[0], range2[1]);

    tiledb::Query query(ctx, array);
    query.set_subarray(subarray);
    auto est_size = query.est_result_size("a");
    REQUIRE(est_size == 12);
    std::array<uint64_t, 2> est_size_var;
    CHECK_THROWS(est_size_var = query.est_result_size_var("a"));

    std::vector<int> data(est_size);
    query.set_layout(TILEDB_ROW_MAJOR).set_buffer("a", data);
    query.set_subarray(subarray);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 3);
    REQUIRE(data[0] == 2);
    REQUIRE(data[1] == 3);
  }

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Test subarray (dense)", "[cppapi][dense][subarray]") {
  const std::string array_name = "cpp_unit_array";
  tiledb::Config cfg;
  cfg.set("config.logging_level", "3");
  tiledb::Context ctx(cfg);
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  tiledb::Domain domain(ctx);
  domain.add_dimension(tiledb::Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(tiledb::Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(tiledb::Attribute::create<int>(ctx, "a"));
  tiledb::Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {1, 2, 3, 4};
  tiledb::Array array_w(ctx, array_name, TILEDB_WRITE);
  tiledb::Query query_w(ctx, array_w);
  query_w.set_subarray({0, 1, 0, 1})
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", data_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  SECTION("-  set_subarray Write ranges oob error - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx, array);
    // default expected to err.
    REQUIRE_THROWS(subarray.set_subarray({1, 4, -1, 3}));
    tiledb::Config config;
    // The config option should be ignored but we set anyway.
    config.set("sm.read_range_oob", "error");
    subarray.set_config(config);
    REQUIRE_THROWS(subarray.set_subarray({1, 4, -1, 3}));
  }

  SECTION("- set_subarray Write ranges oob warn - Subarray-query") {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    // default (dense) WRITE should always err/throw on oob
    REQUIRE_THROWS(query.set_subarray({1, 4, -1, 3}));
    tiledb::Config config;
    config.set("sm.read_range_oob", "warn");
    query.set_config(config);
    // (dense) WRITE should ignore sm.read_range_oob config option
    // and err/throw on oob
    REQUIRE_THROWS(query.set_subarray({1, 4, -1, 3}));
  }

  SECTION("- set_subarray Write ranges oob warn - Subarray-cppapi") {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx, array);
    REQUIRE_THROWS(subarray.set_subarray({1, 4, -1, 3}));
    tiledb::Config config;
    config.set("sm.read_range_oob", "warn");
    subarray.set_config(config);
    REQUIRE_THROWS(subarray.set_subarray({1, 4, -1, 3}));
  }

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test subarray (incomplete) - Subarray-query",
    "[cppapi][sparse][subarray][incomplete]") {
  const std::string array_name = "cpp_unit_array";
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  tiledb::Domain domain(ctx);
  domain
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "rows", {{0, 100}}, 101))
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "cols", {{0, 100000}}, 100001));
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
      .set_capacity(10000);
  schema.add_attribute(tiledb::Attribute::create<char>(ctx, "a"));
  tiledb::Array::create(array_name, schema);

  // Write
  std::vector<char> data_w = {
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n'};
  std::vector<int> coords_w = {0, 12277, 0, 12771, 0, 13374, 0, 13395, 0, 13413,
                               0, 13451, 0, 13519, 0, 13544, 0, 13689, 0, 17479,
                               0, 17486, 1, 12277, 1, 12771, 1, 13389};
  tiledb::Array array_w(ctx, array_name, TILEDB_WRITE);
  tiledb::Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", data_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open array for reading
  tiledb::Array array(ctx, array_name, TILEDB_READ);
  tiledb::Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED);

  // Set up subarray for read
  int row_range[] = {0, 1};
  int col_range0[] = {12277, 13499};
  int col_range1[] = {13500, 17486};
  query.add_range(0, row_range[0], row_range[1]);
  query.add_range(1, col_range0[0], col_range0[1]);
  query.add_range(1, col_range1[0], col_range1[1]);

  // Test range num
  auto range_num = query.range_num(0);
  CHECK(range_num == 1);
  range_num = query.range_num(1);
  // Ranges `col_range0` and `col_range1` are coalesced.
  CHECK(range_num == 1);

  // Allocate buffers large enough to hold 2 cells at a time.
  std::vector<char> data(2, '\0');
  std::vector<int> rows(2);
  std::vector<int> cols(2);
  query.set_data_buffer("rows", rows)
      .set_data_buffer("cols", cols)
      .set_data_buffer("a", data);

  // Submit query
  auto st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  auto result_elts = query.result_buffer_elements();
  auto result_num = result_elts["rows"].second;
  REQUIRE(result_num == 2);
  REQUIRE(data[0] == 'a');
  REQUIRE(data[1] == 'l');

  // Resubmit
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  REQUIRE(result_num == 2);
  REQUIRE(data[0] == 'b');
  REQUIRE(data[1] == 'm');

  // Resubmit
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'c');
    REQUIRE(data[1] == 'n');
  } else {
    REQUIRE(result_num == 1);
    REQUIRE(data[0] == 'c');
  }

  // Resubmit
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  REQUIRE(result_num == 2);

  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(data[0] == 'd');
    REQUIRE(data[1] == 'e');
  } else {
    REQUIRE(data[0] == 'n');
    REQUIRE(data[1] == 'd');
  }

  // Resubmit
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'f');
    REQUIRE(data[1] == 'g');
  } else {
    REQUIRE(result_num == 1);
    REQUIRE(data[0] == 'e');
  }

  // Resubmit
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'h');
    REQUIRE(data[1] == 'i');
  } else {
    REQUIRE(result_num == 1);
    REQUIRE(data[0] == 'f');
  }

  // Resubmit
  st = query.submit();
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(st == tiledb::Query::Status::COMPLETE);
  } else {
    REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  }
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;

  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'j');
    REQUIRE(data[1] == 'k');
  } else {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'g');
    REQUIRE(data[1] == 'h');

    // Resubmit
    st = query.submit();
    REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
    result_elts = query.result_buffer_elements();
    result_num = result_elts["rows"].second;
    REQUIRE(result_num == 1);
    REQUIRE(data[0] == 'i');

    // Resubmit
    st = query.submit();
    REQUIRE(st == tiledb::Query::Status::COMPLETE);
    result_elts = query.result_buffer_elements();
    result_num = result_elts["rows"].second;
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'j');
    REQUIRE(data[1] == 'k');
  }

  // Close array.
  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test subarray (incomplete) - Subarray-cppapi",
    "[cppapi][sparse][subarray][incomplete]") {
  const std::string array_name = "cpp_unit_array";
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  tiledb::Domain domain(ctx);
  domain
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "rows", {{0, 100}}, 101))
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "cols", {{0, 100000}}, 100001));
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
      .set_capacity(10000);
  schema.add_attribute(tiledb::Attribute::create<char>(ctx, "a"));
  tiledb::Array::create(array_name, schema);

  // Write
  std::vector<char> data_w = {
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n'};
  std::vector<int> coords_w = {0, 12277, 0, 12771, 0, 13374, 0, 13395, 0, 13413,
                               0, 13451, 0, 13519, 0, 13544, 0, 13689, 0, 17479,
                               0, 17486, 1, 12277, 1, 12771, 1, 13389};
  tiledb::Array array_w(ctx, array_name, TILEDB_WRITE);
  tiledb::Query query_w(ctx, array_w);
  tiledb::Subarray subarray_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", data_w);
  query_w.set_subarray(subarray_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open array for reading
  tiledb::Array array(ctx, array_name, TILEDB_READ);
  tiledb::Query query(ctx, array);
  tiledb::Subarray subarray(ctx, array);
  query.set_layout(TILEDB_UNORDERED);

  // Set up subarray for read
  int row_range[] = {0, 1};
  int col_range0[] = {12277, 13499};
  int col_range1[] = {13500, 17486};
  subarray.add_range(0, row_range[0], row_range[1]);
  subarray.add_range(1, col_range0[0], col_range0[1]);
  subarray.add_range(1, col_range1[0], col_range1[1]);

  // Test range num
  auto range_num = subarray.range_num(0);
  CHECK(range_num == 1);
  range_num = subarray.range_num(1);
  // Ranges `col_range0` and `col_range1` are coalesced.
  CHECK(range_num == 1);
  std::array<int, 3> rng = subarray.range<int>(0, 0);
  CHECK(rng[0] == row_range[0]);
  CHECK(rng[1] == row_range[1]);
  rng = subarray.range<int>(1, 0);
  //...0 and ...1 were coalesced, hence ...0[0], ...1[1] checks
  CHECK(rng[0] == col_range0[0]);
  CHECK(rng[1] == col_range1[1]);

  // Allocate buffers large enough to hold 2 cells at a time.
  std::vector<char> data(2, '\0');
  std::vector<int> rows(2);
  std::vector<int> cols(2);
  query.set_data_buffer("rows", rows)
      .set_data_buffer("cols", cols)
      .set_data_buffer("a", data);

  {
    tiledb::Query query(ctx, array);
    tiledb::Subarray default_subarray_from_query(query.ctx(), query.array());
    // Check against 'default' subarray here and non-default subarray below to
    // verify that we've actually made a difference by the query.set_subarray()
    // since the other CHECKS(range_num == 1) succeed whether or not the
    //.set_subarray() was done (accidental discovery.)
    CHECK(!subarray_equiv<int>(
        *default_subarray_from_query.ptr().get()->subarray_,
        *subarray.ptr().get()->subarray_));
    query.set_subarray(subarray);
    tiledb::Subarray retrieved_query_subarray(query.ctx(), query.array());
    query.update_subarray_from_query(&retrieved_query_subarray);
    CHECK(subarray_equiv<int>(
        *retrieved_query_subarray.ptr().get()->subarray_,
        *subarray.ptr().get()->subarray_));
    // Test range num
    auto range_num = retrieved_query_subarray.range_num(0);
    CHECK(range_num == 1);
    range_num = retrieved_query_subarray.range_num(1);
    // Ranges `col_range0` and `col_range1` are coalesced.
    CHECK(range_num == 1);
  }

  // Submit query
  query.set_subarray(subarray);
  auto st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  auto result_elts = query.result_buffer_elements();
  auto result_num = result_elts["rows"].second;
  REQUIRE(result_num == 2);
  REQUIRE(data[0] == 'a');
  REQUIRE(data[1] == 'l');

  {
    tiledb::Subarray query_subarray(query.ctx(), query.array());
    auto range_num = subarray.range_num(0);
    CHECK(range_num == 1);
    range_num = subarray.range_num(1);
    // Ranges `col_range0` and `col_range1` are coalesced.
    CHECK(range_num == 1);
  }

  // Resubmit
  query.set_subarray(subarray);
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  REQUIRE(result_num == 2);
  REQUIRE(data[0] == 'b');
  REQUIRE(data[1] == 'm');

  // Resubmit
  query.set_subarray(subarray);
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'c');
    REQUIRE(data[1] == 'n');
  } else {
    REQUIRE(result_num == 1);
    REQUIRE(data[0] == 'c');
  }

  // Resubmit
  query.set_subarray(subarray);
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  REQUIRE(result_num == 2);
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(data[0] == 'd');
    REQUIRE(data[1] == 'e');
  } else {
    REQUIRE(data[0] == 'n');
    REQUIRE(data[1] == 'd');
  }

  // Resubmit
  query.set_subarray(subarray);
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'f');
    REQUIRE(data[1] == 'g');
  } else {
    REQUIRE(result_num == 1);
    REQUIRE(data[0] == 'e');
  }

  // Resubmit
  query.set_subarray(subarray);
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'h');
    REQUIRE(data[1] == 'i');
  } else {
    REQUIRE(result_num == 1);
    REQUIRE(data[0] == 'f');
  }

  // Resubmit
  query.set_subarray(subarray);
  st = query.submit();
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(st == tiledb::Query::Status::COMPLETE);
  } else {
    REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  }
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  if (test::use_refactored_sparse_global_order_reader()) {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'j');
    REQUIRE(data[1] == 'k');
  } else {
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'g');
    REQUIRE(data[1] == 'h');

    // Resubmit
    st = query.submit();
    REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
    result_elts = query.result_buffer_elements();
    result_num = result_elts["rows"].second;
    REQUIRE(result_num == 1);
    REQUIRE(data[0] == 'i');

    // Resubmit
    st = query.submit();
    REQUIRE(st == tiledb::Query::Status::COMPLETE);
    result_elts = query.result_buffer_elements();
    result_num = result_elts["rows"].second;
    REQUIRE(result_num == 2);
    REQUIRE(data[0] == 'j');
    REQUIRE(data[1] == 'k');
  }

  // Close array.
  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test subarray (incomplete overlapping) - Subarray-query",
    "[cppapi][sparse][subarray][incomplete]") {
  const std::string array_name = "cpp_unit_array";
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  tiledb::Domain domain(ctx);
  domain
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "rows", {{0, 100}}, 101))
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "cols", {{0, 100000}}, 100001));
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
      .set_capacity(10000);
  schema.add_attribute(tiledb::Attribute::create<char>(ctx, "a"));
  tiledb::Array::create(array_name, schema);

  // Write
  std::vector<char> data_w = {
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n'};
  std::vector<int> coords_w = {0, 12277, 0, 12771, 0, 13374, 0, 13395, 0, 13413,
                               0, 13451, 0, 13519, 0, 13544, 0, 13689, 0, 17479,
                               0, 17486, 1, 12277, 1, 12771, 1, 13389};
  tiledb::Array array_w(ctx, array_name, TILEDB_WRITE);
  tiledb::Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_UNORDERED)
      .set_buffer("a", data_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open array for reading
  tiledb::Array array(ctx, array_name, TILEDB_READ);
  tiledb::Query query(ctx, array);

  // Set up subarray for read
  int row_range[] = {1, 1};
  int col_range0[] = {12277, 12277};
  int col_range1[] = {12277, 13160};
  query.add_range(0, row_range[0], row_range[1]);
  query.add_range(1, col_range0[0], col_range0[1]);
  query.add_range(1, col_range1[0], col_range1[1]);
  query.set_layout(TILEDB_UNORDERED);

  // Allocate buffers large enough to hold 2 cells at a time.
  std::vector<char> data(2, '\0');
  std::vector<int> rows(2);
  std::vector<int> cols(2);
  query.set_data_buffer("rows", rows)
      .set_data_buffer("cols", cols)
      .set_data_buffer("a", data);

  // Submit query
  auto st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  auto result_elts = query.result_buffer_elements();
  auto result_num = result_elts["rows"].second;
  REQUIRE(result_num == 1);
  REQUIRE(data[0] == 'l');

  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::COMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  REQUIRE(result_num == 2);
  REQUIRE(data[0] == 'l');
  REQUIRE(data[1] == 'm');

  // Close array.
  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test subarray (incomplete overlapping) - Subarray-cppapi",
    "[cppapi][sparse][subarray][incomplete]") {
  const std::string array_name = "cpp_unit_array";
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  tiledb::Domain domain(ctx);
  domain
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "rows", {{0, 100}}, 101))
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "cols", {{0, 100000}}, 100001));
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
      .set_capacity(10000);
  schema.add_attribute(tiledb::Attribute::create<char>(ctx, "a"));
  tiledb::Array::create(array_name, schema);

  // Write
  std::vector<char> data_w = {
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n'};
  std::vector<int> coords_w = {0, 12277, 0, 12771, 0, 13374, 0, 13395, 0, 13413,
                               0, 13451, 0, 13519, 0, 13544, 0, 13689, 0, 17479,
                               0, 17486, 1, 12277, 1, 12771, 1, 13389};
  tiledb::Array array_w(ctx, array_name, TILEDB_WRITE);
  tiledb::Query query_w(ctx, array_w);
  tiledb::Subarray subarray_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_UNORDERED)
      .set_buffer("a", data_w);
  query_w.set_subarray(subarray_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open array for reading
  tiledb::Array array(ctx, array_name, TILEDB_READ);
  tiledb::Query query(ctx, array);
  tiledb::Subarray subarray(ctx, array);

  // Set up subarray for read
  int row_range[] = {1, 1};
  int col_range0[] = {12277, 12277};
  int col_range1[] = {12277, 13160};
  subarray.add_range(0, row_range[0], row_range[1]);
  subarray.add_range(1, col_range0[0], col_range0[1]);
  subarray.add_range(1, col_range1[0], col_range1[1]);
  query.set_layout(TILEDB_UNORDERED);

  // Allocate buffers large enough to hold 2 cells at a time.
  std::vector<char> data(2, '\0');
  std::vector<int> rows(2);
  std::vector<int> cols(2);
  query.set_data_buffer("rows", rows)
      .set_data_buffer("cols", cols)
      .set_data_buffer("a", data);

  // Submit query
  query.set_subarray(subarray);
  auto st = query.submit();
  REQUIRE(st == tiledb::Query::Status::INCOMPLETE);
  auto result_elts = query.result_buffer_elements();
  auto result_num = result_elts["rows"].second;
  REQUIRE(result_num == 1);
  REQUIRE(data[0] == 'l');

  query.set_subarray(subarray);
  st = query.submit();
  REQUIRE(st == tiledb::Query::Status::COMPLETE);
  result_elts = query.result_buffer_elements();
  result_num = result_elts["rows"].second;
  REQUIRE(result_num == 2);
  REQUIRE(data[0] == 'l');
  REQUIRE(data[1] == 'm');

  // Close array.
  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test subarray - error on multi-range for global layout",
    "[cppapi][subarray][error]") {
  // parameterize over cell order and read layout
  auto test_pair = GENERATE(
      std::make_pair(TILEDB_HILBERT, TILEDB_GLOBAL_ORDER),
      std::make_pair(TILEDB_ROW_MAJOR, TILEDB_GLOBAL_ORDER));

  const std::string array_name = "cpp_unit_array";
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  tiledb::Domain domain(ctx);
  domain
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "rows", {{0, 100}}, 101))
      .add_dimension(
          tiledb::Dimension::create<int>(ctx, "cols", {{0, 100}}, 101));
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_COL_MAJOR, test_pair.first}})
      .set_capacity(10000);
  schema.add_attribute(tiledb::Attribute::create<char>(ctx, "a"));
  tiledb::Array::create(array_name, schema);

  // Open array for reading
  tiledb::Array array(ctx, array_name, TILEDB_READ);
  tiledb::Query query(ctx, array);

  query.set_layout(test_pair.second);
  query.add_range(0, 0, 0);
  CHECK_THROWS(query.add_range(0, 1, 1));

  // Close array.
  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}