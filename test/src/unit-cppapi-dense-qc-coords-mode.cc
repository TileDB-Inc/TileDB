/**
 * @file   unit-cppapi-dense-qc-coords-mode.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB Inc.
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
 * Tests the C++ API for dense arrays and qc coords mode.
 */

#include <test/support/src/helpers.h>
#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/constants.h"

#include <numeric>

using namespace tiledb;

struct CPPDenseQcCoordsModeFx {
  const char* array_name = "cpp_dense_qc_coords_mode";

  CPPDenseQcCoordsModeFx()
      : vfs_(ctx_) {
    Config config;
    config["sm.query.dense.qc_coords_mode"] = "true";
    ctx_ = Context(config);
    vfs_ = VFS(ctx_);

    using namespace tiledb;

    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }

    Domain domain(ctx_);
    auto d1 = Dimension::create<int>(ctx_, "d1", {{1, 10}}, 2);
    auto d2 = Dimension::create<int>(ctx_, "d2", {{1, 10}}, 2);
    domain.add_dimensions(d1, d2);

    auto a1 = Attribute::create<int>(ctx_, "a1");

    ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain);
    schema.add_attributes(a1);

    Array::create(array_name, schema);

    std::vector<int> a1_buff(100);
    std::iota(a1_buff.begin(), a1_buff.end(), 1);
    std::vector<int> subarray = {1, 10, 1, 10};

    Array array(ctx_, array_name, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    Subarray sub(ctx_, array);
    sub.set_subarray(subarray);
    query.set_subarray(sub);
    query.set_data_buffer("a1", a1_buff);
    query.set_layout(TILEDB_ROW_MAJOR);
    REQUIRE(query.submit() == Query::Status::COMPLETE);
  }

  ~CPPDenseQcCoordsModeFx() {
    if (vfs_.is_dir(array_name)) {
      vfs_.remove_dir(array_name);
    }
  }

  Context ctx_;
  VFS vfs_;
};

TEST_CASE_METHOD(
    CPPDenseQcCoordsModeFx,
    "C++ API: Dense qc coords mode",
    "[cppapi][dense][qc-coords-mode]") {
  // Open array for read.
  Array array(ctx_, array_name, TILEDB_READ);
  Query query(ctx_, array, TILEDB_READ);

  uint64_t c_result_num = 0;
  std::vector<int> c_d1_rm;
  std::vector<int> c_d2_rm;
  std::vector<int> c_d1_go;
  std::vector<int> c_d2_go;
  QueryCondition qc(ctx_);
  tiledb_layout_t read_layout = GENERATE(TILEDB_ROW_MAJOR, TILEDB_GLOBAL_ORDER);

  // Set condition and expected results.
  SECTION("Test TILEDB_LE") {
    int val = 20;
    qc.init("a1", &val, sizeof(int), TILEDB_LE);
    c_d1_rm = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
    c_d2_rm = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    c_d1_go = {1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2};
    c_d2_go = {1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 5, 6, 7, 8, 7, 8, 9, 10, 9, 10};
    c_result_num = 20;
  }

  SECTION("Test TILEDB_AND") {
    int val = 60;
    qc.init("a1", &val, sizeof(int), TILEDB_GT);
    QueryCondition qc2(ctx_);
    val = 75;
    qc2.init("a1", &val, sizeof(int), TILEDB_LE);
    qc = qc.combine(qc2, TILEDB_AND);
    c_d1_rm = {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8};
    c_d2_rm = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5};
    c_d1_go = {7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 7, 7, 7, 7};
    c_d2_go = {1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 5, 7, 8, 9, 10};
    c_result_num = 15;
  }

  SECTION("Test TILEDB_OR") {
    int val = 90;
    qc.init("a1", &val, sizeof(int), TILEDB_GT);
    QueryCondition qc2(ctx_);
    val = 5;
    qc2.init("a1", &val, sizeof(int), TILEDB_LE);
    qc = qc.combine(qc2, TILEDB_OR);
    c_d1_rm = {1, 1, 1, 1, 1, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10};
    c_d2_rm = {1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    c_d1_go = {1, 1, 1, 1, 1, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10};
    c_d2_go = {1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    c_result_num = 15;
  }

  SECTION("Test TILEDB_NOT") {
    int val = 90;
    qc.init("a1", &val, sizeof(int), TILEDB_LT);
    QueryCondition qc2(ctx_);
    val = 10;
    qc2.init("a1", &val, sizeof(int), TILEDB_GE);
    qc = qc.combine(qc2, TILEDB_AND);
    qc = qc.negate();
    c_d1_rm = {1,  1,  1,  1,  1,  1,  1,  1,  1,  9,
               10, 10, 10, 10, 10, 10, 10, 10, 10, 10};
    c_d2_rm = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    c_d1_go = {1,  1,  1,  1,  1,  1,  1,  1, 1,  10,
               10, 10, 10, 10, 10, 10, 10, 9, 10, 10};
    c_d2_go = {1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 10, 9, 10};
    c_result_num = 20;
  }

  // Set and run the query.
  std::vector<int> subarray = {1, 10, 1, 10};
  std::vector<int> d1(100);
  std::vector<int> d2(100);
  Subarray sub(ctx_, array);
  sub.set_subarray(subarray);
  query.set_subarray(sub);
  query.set_layout(read_layout);
  query.set_data_buffer("d1", d1);
  query.set_data_buffer("d2", d2);
  query.set_condition(qc);
  query.submit();

  auto result_elts = query.result_buffer_elements();
  auto result_num = result_elts["d1"].second;

  // Check result.
  CHECK(result_num == c_result_num);
  if (read_layout == TILEDB_ROW_MAJOR) {
    CHECK(!memcmp(c_d1_rm.data(), d1.data(), c_d1_rm.size() * sizeof(int)));
    CHECK(!memcmp(c_d2_rm.data(), d2.data(), c_d2_rm.size() * sizeof(int)));
  } else {
    CHECK(!memcmp(c_d1_go.data(), d1.data(), c_d1_go.size() * sizeof(int)));
    CHECK(!memcmp(c_d2_go.data(), d2.data(), c_d2_go.size() * sizeof(int)));
  }
}
