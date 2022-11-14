/**
 * @file   cpp-integration-query-condition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests the integration of query conditions by running queries.
 */

// Template-ize the TestArray/TestQuery classes so that the `b` attribute
// is a test provided type. This will allow us to go over all of the
// checks that show we can only do primitive types and strings. I can
// probably also add dimension tempated dimensions as well to make things
// easier as well.
//
// Also add tests to see if I can break things with that missing std::min
// check I spotted in the QueryCondition code in the query ast thinger where
// conditions are implemented.


#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>

#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

int NUM_ROWS = 20;
int A_FILL = -1;
float B_FILL = -1.0;
const std::string ARRAY_NAME = "cpp_integration_query_condition_array";

typedef const std::array<int, 2> TRange;

float
rand_float() {
  return static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
}

std::string
rand_string(int min_len, int max_len) {
  const char* data =
      "0123456789abcedefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  int len = static_cast<int>(rand_float() * (max_len - min_len)) + min_len;
  std::string ret;
  for(int i = 0; i < len; i++) {
    ret += data[static_cast<size_t>(rand_float() * 62)];
  }

  return ret;
}

struct TestParams {
  TestParams(
      tiledb_array_type_t array_type,
      tiledb_layout_t layout,
      bool allows_dups,
      bool legacy)
      : array_type_(array_type)
      , layout_(layout)
      , allows_dups_(allows_dups)
      , legacy_(legacy) {
  }

  tiledb_array_type_t array_type_;
  tiledb_layout_t layout_;
  bool allows_dups_;
  bool legacy_;
};

template<typename AType, typename BType>
struct TestCell {
  using a_type = AType;
  using b_type = BType;

  TestCell() {}
  TestCell(a_type a, b_type b) : a_(a), b_(b) {}

  static TestCell<a_type, b_type> generate();

  a_type a_;
  b_type b_;
};

template<>
TestCell<int, float> TestCell<int, float>::generate() {
  return TestCell(static_cast<int>(rand_float() * 10), rand_float() * 100);
}

template<>
TestCell<int, std::string> TestCell<int, std::string>::generate() {
  return TestCell(static_cast<int>(rand_float() * 10), rand_string(1, 10));
}

template<typename CellType>
struct TestArray {
  TestArray(TestParams& params);
  ~TestArray();

  void update(int rm, int rx, int cm, int cx, CellType cell);
  CellType get_cell(int row, int col);

  Context ctx_;
  VFS vfs_;
  TestParams& params_;
  std::vector<CellType> cells_;
};

template<typename CellType>
TestArray<CellType>::TestArray(TestParams& params)
    : vfs_(ctx_)
    , params_(params)
    , cells_(NUM_ROWS * NUM_ROWS) {

  if (vfs_.is_dir(ARRAY_NAME)) {
    vfs_.remove_dir(ARRAY_NAME);
  }

  Domain dom(ctx_);
  dom.add_dimension(Dimension::create<int>(ctx_, "rows", {{1, NUM_ROWS}}, 4));
  dom.add_dimension(Dimension::create<int>(ctx_, "cols", {{1, NUM_ROWS}}, 4));

  ArraySchema schema(ctx_, params_.array_type_);
  schema.set_allows_dups(params_.allows_dups_);
  schema.set_domain(dom).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  Attribute attr_a = Attribute::create<typename CellType::a_type>(ctx_, "a");
  Attribute attr_b = Attribute::create<typename CellType::b_type>(ctx_, "b");

  if (params_.array_type_ == TILEDB_DENSE) {
    attr_a.set_fill_value(&A_FILL, sizeof(typename CellType::a_type));
    attr_b.set_fill_value(&B_FILL, sizeof(typename CellType::b_type));
  } else {
    schema.set_capacity(16);
  }
  schema.add_attribute(attr_a);
  schema.add_attribute(attr_b);

  Array::create(ARRAY_NAME, schema);

  std::vector<int> row_dims(NUM_ROWS * NUM_ROWS);
  std::vector<int> col_dims(NUM_ROWS * NUM_ROWS);
  std::vector<typename CellType::a_type> a_vals(NUM_ROWS * NUM_ROWS);
  std::vector<typename CellType::b_type> b_vals(NUM_ROWS * NUM_ROWS);

  for (int i = 0; i < NUM_ROWS * NUM_ROWS; i++) {
    row_dims[i] = (i / NUM_ROWS) + 1;
    col_dims[i] = (i % NUM_ROWS) + 1;
    cells_[i] = CellType::generate();
    a_vals[i] = cells_[i].a_;
    b_vals[i] = cells_[i].b_;
  }

  Array array_w(ctx_, ARRAY_NAME, TILEDB_WRITE);
  Query query_w(ctx_, array_w);

  if (params_.array_type_ == TILEDB_SPARSE) {
    query_w.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("rows", row_dims)
        .set_data_buffer("cols", col_dims)
        .set_data_buffer("a", a_vals)
        .set_data_buffer("b", b_vals);
  } else {
    query_w.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_vals)
        .set_data_buffer("b", b_vals);
  }

  query_w.submit();
  query_w.finalize();
  array_w.close();
}

template<typename CellType>
TestArray<CellType>::~TestArray() {
  if (vfs_.is_dir(ARRAY_NAME)) {
    vfs_.remove_dir(ARRAY_NAME);
  }
}

template<typename CellType>
void TestArray<CellType>::update(
    int rm, int rx, int cm, int cx, CellType cell) {
  REQUIRE(vfs_.is_dir(ARRAY_NAME));
  int num_cells = (rx - rm + 1) * (cx - cm + 1);
  std::vector<int> row_dims(num_cells);
  std::vector<int> col_dims(num_cells);
  std::vector<typename CellType::a_type> a_vals(num_cells);
  std::vector<typename CellType::b_type> b_vals(num_cells);

  int w_idx = 0;
  for(int r = rm; r <= rx; r++) {
    for(int c = cm; c <= cx; c++) {
      int idx = (r - 1) * NUM_ROWS + (c - 1);
      cells_[idx] = cell;
      row_dims[w_idx] = r;
      col_dims[w_idx] = c;
      a_vals[w_idx] = cell.a_;
      b_vals[w_idx] = cell.b_;
      w_idx++;
    }
  }

  Array array_w(ctx_, ARRAY_NAME, TILEDB_WRITE);
  Query query_w(ctx_, array_w);

  if (params_.array_type_ == TILEDB_SPARSE) {
    query_w.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("rows", row_dims)
        .set_data_buffer("cols", col_dims)
        .set_data_buffer("a", a_vals)
        .set_data_buffer("b", b_vals);
  } else {
    query_w.set_layout(TILEDB_ROW_MAJOR)
        .add_range("rows", rm, rx)
        .add_range("cols", cm, cx)
        .set_data_buffer("a", a_vals)
        .set_data_buffer("b", b_vals);
  }

  query_w.submit();
  query_w.finalize();
  array_w.close();
}

template<typename CellType>
CellType TestArray<CellType>::get_cell(int row, int col) {
  int idx = (row - 1) * NUM_ROWS + (col - 1);
  return cells_[idx];
}

template<typename CellType>
struct TestQuery {
  TestQuery(TestArray<CellType>& t_array, Config& cfg);
  ~TestQuery();

  template<typename F>
  void verify(QueryCondition& qc, F&& predicate);

  template<typename F>
  void verify(
      QueryCondition& qc,
      TRange& row_range,
      TRange& col_range,
      F&& predicate);

  template<typename F>
  void verify(
      Query& query,
      TRange& row_range,
      TRange& col_range,
      F&& predicate);

  template<typename T>
  QueryCondition qcb(std::string name, T val, tiledb_query_condition_op_t op);

  TestArray<CellType>& t_array_;
  Config& cfg_;
  Context ctx_;
  Array array_;

  std::vector<int> rows_;
  std::vector<int> cols_;
  std::vector<int> a_;
  std::vector<float> b_;
};

template<typename CellType>
TestQuery<CellType>::TestQuery(TestArray<CellType>& t_array, Config& cfg)
    : t_array_(t_array)
    , cfg_(cfg)
    , ctx_(cfg_)
    , array_(ctx_, ARRAY_NAME, TILEDB_READ)
    , rows_(NUM_ROWS * NUM_ROWS)
    , cols_(NUM_ROWS * NUM_ROWS)
    , a_(NUM_ROWS * NUM_ROWS)
    , b_(NUM_ROWS * NUM_ROWS) {
}

template<typename CellType>
TestQuery<CellType>::~TestQuery() {
  array_.close();
}

template<typename CellType>
template<typename F>
void TestQuery<CellType>::verify(QueryCondition& qc, F&& predicate) {
  Query query(ctx_, array_);
  query.set_layout(t_array_.params_.layout_)
    .set_data_buffer("rows", rows_)
    .set_data_buffer("cols", cols_)
    .set_data_buffer("a", a_)
    .set_data_buffer("b", b_)
    .set_condition(qc)
    .submit();
  verify(query, {1, NUM_ROWS}, {1, NUM_ROWS}, predicate);
}

template<typename CellType>
template<typename F>
void TestQuery<CellType>::verify(
    QueryCondition& qc,
    TRange& row_range,
    TRange& col_range,
    F&& predicate) {
  Subarray subarray(ctx_, array_);
  subarray.add_range("rows", row_range[0], row_range[1])
      .add_range("cols", col_range[0], col_range[1]);

  Query query(ctx_, array_);
  query.set_subarray(subarray)
      .set_layout(t_array_.params_.layout_)
      .set_data_buffer("rows", rows_)
      .set_data_buffer("cols", cols_)
      .set_data_buffer("a", a_)
      .set_data_buffer("b", b_)
      .set_condition(qc)
      .submit();
  verify(query, row_range, col_range, predicate);
}

template<typename CellType>
template<typename F>
void TestQuery<CellType>::verify(
    Query& query,
    TRange& row_range,
    TRange& col_range,
    F&& predicate) {
  std::map<std::pair<int, int>, std::pair<int, float>> results;
  auto elems = query.result_buffer_elements();

  for(uint64_t i = 0; i < elems["rows"].second; i++) {
    results[std::make_pair(rows_[i], cols_[i])] = std::make_pair(a_[i], b_[i]);
  }

  uint64_t num_elems = 0;
  for(int r = row_range[0]; r <= row_range[1]; r++) {
    for(int c = col_range[0]; c <= col_range[1]; c++) {
      // This cell should be in our query set
      auto [a, b] = t_array_.get_cell(r, c);
      fprintf(stderr, "%d %f\n", a, b);
      if (predicate(r, c, a, b)) {
        num_elems++;
        REQUIRE(results[std::pair(r, c)] == std::pair(a, b));
      } else if(t_array_.params_.array_type_ == TILEDB_DENSE) {
        num_elems++;
        REQUIRE(results[std::pair(r, c)] == std::pair(A_FILL, B_FILL));
      } else {
        REQUIRE(results.find(std::pair(r, c)) == results.end());
      }
    }
  }

  REQUIRE(elems["a"].second == num_elems);
  REQUIRE(elems["b"].second == num_elems);
}

template<typename CellType>
template<typename T>
QueryCondition TestQuery<CellType>::qcb(
    std::string name, T val, tiledb_query_condition_op_t op) {
  return QueryCondition::create(ctx_, name, val, op);
}

QueryCondition operator&(QueryCondition qc1, QueryCondition qc2) {
  return qc1.combine(qc2, TILEDB_AND);
}

QueryCondition operator|(QueryCondition qc1, QueryCondition qc2) {
  return qc1.combine(qc2, TILEDB_OR);
}

TEST_CASE(
    "Testing read query with basic query conditions",
    "[query][query-condition]") {

  TestParams params = GENERATE(
    TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
    TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, false),
    TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
    TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  TestArray<TestCell<int, float>> t_array(params);

  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }

  SECTION("simple query condition on all tiles") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.0f, TILEDB_LT);
    auto pred = [](int, int, int, float b) {
      return b < 50.0;
    };

    t_query.verify(qc, {1, NUM_ROWS}, {1, NUM_ROWS}, pred);
  }

  SECTION("simple query condtion on single tile") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.0f, TILEDB_LT);
    auto pred = [](int, int, int, float b) {
      return b < 50.0;
    };

    t_query.verify(qc, {2, 3}, {2, 3}, pred);
  }

  SECTION("simple query condition on multiple tiles") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.f, TILEDB_LT);
    auto pred = [](int, int, int, float b) {
      return b < 50.0;
    };

    t_query.verify(qc, {7, 10}, {2, 3}, pred);
    t_query.verify(qc, {2, 3}, {7, 10}, pred);
    t_query.verify(qc, {7, 14}, {7, 14}, pred);
  }

  SECTION("complex query condition on multiple tiles") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 75.0f, TILEDB_LT);
    qc = qc & t_query.qcb("b", 50.0f, TILEDB_LE);
    qc = qc & t_query.qcb("b", 25.0f, TILEDB_GE);
    qc = qc & t_query.qcb("b", 36.0f, TILEDB_NE);

    auto pred = [](int, int, int, float b) {
      return b < 75.0 && b <= 50.0 && b >= 25.0 && b != 36.0;
    };

    t_query.verify(qc, {7, 14}, {7, 14}, pred);
  }

  SECTION("multiple attribute query conditions - AND") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.0f, TILEDB_LT);
    qc = qc & t_query.qcb("a", 4, TILEDB_EQ);

    auto pred = [](int, int, int a, float b) {
      return a == 4 && b < 50.0f;
    };

    t_query.verify(qc, {1, NUM_ROWS}, {1, NUM_ROWS}, pred);
  }

  SECTION("multiple attribute query conditions - OR") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.0f, TILEDB_LT);
    qc = qc | t_query.qcb("a", 1, TILEDB_EQ);

    auto pred = [](int, int, int a, float b) {
      return a == 1 || b < 50.0f;
    };

    t_query.verify(qc, {1, NUM_ROWS}, {1, NUM_ROWS}, pred);
  }
}

TEST_CASE(
    "Testing read query with query conditions on dimensions",
    "[query][query-condition]") {

  TestParams params = GENERATE(
    TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
    TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, false),
    TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false));

  TestArray<TestCell<int, float>> t_array(params);

  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }

  auto qc_builder = [](
      TestQuery<TestCell<int, float>>& t_query, int r1, int r2, int c1, int c2) {
    auto qc = t_query.qcb("rows", r1, TILEDB_GE);
    qc = qc & t_query.qcb("rows", r2, TILEDB_LE);
    qc = qc & t_query.qcb("cols", c1, TILEDB_GE);
    qc = qc & t_query.qcb("cols", c2, TILEDB_LE);
    return qc;
  };

  SECTION("single tile with simple qc") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.0f, TILEDB_LT);
    qc = qc & qc_builder(t_query, 2, 3, 2, 3);

    auto pred = [](int r, int c, int, float b) {
      return r >= 2 && r <= 3 && c >= 2 && c <= 3 && b < 50.0f;
    };

    t_query.verify(qc, pred);
  }

  SECTION("multiple row tiles, single col tile with simple qc") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.0f, TILEDB_LT);
    qc = qc & qc_builder(t_query, 7, 10, 2, 3);

    auto pred = [](int r, int c, int, float b) {
      return r >= 7 && r <= 10 && c >= 2 && c <= 3 && b < 50.0f;
    };

    t_query.verify(qc, pred);
  }

  SECTION("single row tile, multiple col tiles with simple qc") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.0f, TILEDB_LT);
    qc = qc & qc_builder(t_query, 2, 3, 7, 10);

    auto pred = [](int r, int c, int, float b) {
      return r >= 2 && r <= 3 && c >= 7 && c <= 10 && b < 50.0f;
    };

    t_query.verify(qc, pred);
  }

  SECTION("multiples tiles on both axes with simple qc") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 50.0f, TILEDB_LT);
    qc = qc & qc_builder(t_query, 7, 14, 7, 14);

    auto pred = [](int r, int c, int, float b) {
      return r >= 7 && r <= 14 && c >= 7 && c <= 14 && b < 50.0f;
    };

    t_query.verify(qc, pred);
  }

  SECTION("multiple tiles on both axes with complex qc") {
    TestQuery t_query(t_array, config);

    auto qc = t_query.qcb("b", 75.0f, TILEDB_LT);
    qc = qc & t_query.qcb("b", 50.0f, TILEDB_LE);
    qc = qc & t_query.qcb("b", 25.0f, TILEDB_GE);
    qc = qc & t_query.qcb("b", 36.0f, TILEDB_NE);
    qc = qc & qc_builder(t_query, 7, 14, 7, 14);

    auto pred = [](int r, int c, int, float b) {
      return r >= 7 && r <= 14 && c >= 7 && c <= 14
          && b < 75.0f && b <= 50.0 && b >= 25.0 && b != 36.0;
    };

    t_query.verify(qc, pred);
  }
}

TEST_CASE(
    "Test read queries on updated arrays",
    "[query][query-condition]") {

  TestParams params = GENERATE(
    TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
    TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, false),
    TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
    TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  TestArray<TestCell<int, float>> t_array(params);
  REQUIRE(NUM_ROWS >= 20);
  t_array.update(6, 15, 6, 15, TestCell(11, 101.0f));

  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }

  SECTION("simple query condition on all tiles") {
    TestQuery t_query(t_array, config);

    // Notice that the query condition and predicate are
    // executing different logic to assert that the query
    // is only returning values from the updated square
    // centered in the middle of the array.
    auto qc = t_query.qcb("b", 100.0f, TILEDB_GT);
    auto pred = [](int r, int c, int, float) {
      return r >= 6 && r <= 15 && c >= 6 && c <= 15;
    };

    t_query.verify(qc, {1, NUM_ROWS}, {1, NUM_ROWS}, pred);
    t_query.verify(qc, {1, 10}, {1, 10}, pred);
    t_query.verify(qc, {1, 10}, {11, NUM_ROWS}, pred);
    t_query.verify(qc, {11, NUM_ROWS}, {1, 10}, pred);
    t_query.verify(qc, {11, NUM_ROWS}, {11, NUM_ROWS}, pred);
    t_query.verify(qc, {8, 12}, {8, 12}, pred);
  }
}

// TEST_CASE(
//     "Testing read query with simple QC, condition on dimensions, string dim",
//     "[query][query-condition][dimension]") {
//   Context ctx;
//   VFS vfs(ctx);
//
//   if (vfs.is_dir(array_name)) {
//     vfs.remove_dir(array_name);
//   }
//
//   Domain domain(ctx);
//   domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
//       .add_dimension(Dimension::create(
//           ctx, "cols", TILEDB_STRING_ASCII, nullptr, nullptr));
//   ArraySchema schema(ctx, TILEDB_SPARSE);
//
//   schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
//   Attribute attr_a = Attribute::create<int>(ctx, "a");
//   schema.set_capacity(16);
//   schema.add_attribute(attr_a);
//   Array::create(array_name, schema);
//
//   // Write some initial data and close the array.
//   std::vector<int> rows_dims = {1, 2};
//   std::string cols_dims = "ab";
//   std::vector<uint64_t> cols_dim_offs = {0, 1};
//   std::vector<int> a_data = {3, 4};
//
//   Array array_w(ctx, array_name, TILEDB_WRITE);
//   Query query_w(ctx, array_w);
//   query_w.set_layout(TILEDB_UNORDERED)
//       .set_data_buffer("rows", rows_dims)
//       .set_data_buffer("cols", cols_dims)
//       .set_offsets_buffer("cols", cols_dim_offs)
//       .set_data_buffer("a", a_data);
//
//   query_w.submit();
//   query_w.finalize();
//   array_w.close();
//
//   // Read the data with query condition on the string dimension.
//   Array array(ctx, array_name, TILEDB_READ);
//   Query query(ctx, array);
//
//   QueryCondition qc(ctx);
//   std::string val = "b";
//   qc.init("cols", val.data(), 1, TILEDB_EQ);
//
//   std::vector<int> rows_read(1);
//   std::string cols_read;
//   cols_read.resize(1);
//   std::vector<uint64_t> cols_offs_read(1);
//   std::vector<int> a_read(1);
//
//   query.set_layout(TILEDB_GLOBAL_ORDER)
//       .set_data_buffer("rows", rows_read)
//       .set_data_buffer("cols", cols_read)
//       .set_offsets_buffer("cols", cols_offs_read)
//       .set_data_buffer("a", a_read)
//       .set_condition(qc);
//   query.submit();
//   CHECK(query.query_status() == Query::Status::COMPLETE);
//
//   // Check the query for accuracy. The query results should contain 1 element.
//   auto table = query.result_buffer_elements();
//   CHECK(table.size() == 3);
//   CHECK(table["a"].first == 0);
//   CHECK(table["a"].second == 1);
//
//   CHECK(rows_read[0] == 2);
//   CHECK(cols_read == "b");
//   CHECK(cols_offs_read[0] == 0);
//   CHECK(a_read[0] == 4);
//
//   if (vfs.is_dir(array_name)) {
//     vfs.remove_dir(array_name);
//   }
// }

