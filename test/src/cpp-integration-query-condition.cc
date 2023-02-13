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

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#include <test/support/src/ast_helpers.h>
#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

/** Test suite constants */
static const char* ARRAY_NAME = "test_query_conditions_array";
constexpr int NUM_ROWS = 100;
constexpr int A_FILL = -1;
constexpr float B_FILL = -1.0;
constexpr const char* C_FILL = "ohai";
constexpr const char* D_FILL = "\xf0\x9f\x9a\x97";

const std::vector<std::string> OPTION_NAMES = {
    "allow_dups", "legacy", "skip_attribute_b"};

const std::vector<std::string> ASCII_CHOICES = {
    "alice",
    "bob",
    "craig",
    "dave",
    "erin",
    "frank",
    "grace",
    "heidi",
    "ivan",
    "judy"};

const std::vector<std::string> UTF8_CHOICES = {
    "\x41",                              // "A"
    "\x61",                              // "a"
    "\x61\x61",                          // "aa"
    "\x6e\xcc\x83",                      // n-plus-tilda
    "\xc3\xb1",                          // n-with-tilda
    "\xe2\x88\x9e",                      // :infinity:
    "\xe2\x98\x83\xef\xb8\x8f",          // :snowman:
    "\xf0\x9f\x80\x84",                  // :mahjong:
    "\xf0\x9f\x87\xac\xf0\x9f\x87\xb7",  // :flag-gr:
    "\xf0\x9f\x8d\xa8"                   // :icecream:
};

typedef const std::array<int, 2> TRange;

/** Test parameters */
struct TestParams {
  TestParams(
      tiledb_array_type_t array_type,
      tiledb_layout_t layout,
      std::map<std::string, bool> options = {});

  bool get(std::string option);

  tiledb_array_type_t array_type_;
  tiledb_layout_t layout_;
  std::map<std::string, bool> options_;
};

struct QCTestCell {
  QCTestCell();
  QCTestCell(int x, int y, int a, float b, std::string c, std::string d);

  bool operator==(const QCTestCell& rhs) const;
  bool in_range(TRange x_range, TRange y_range);
  std::string to_string() const;
  std::pair<int, int> key() const;

  int x_;
  int y_;
  int a_;
  float b_;
  std::string c_;
  std::string d_;
};

bool operator<(QCTestCell a, QCTestCell b);
std::ostream& operator<<(std::ostream& os, QCTestCell const& value);
typedef std::function<bool(QCTestCell&)> Chooser;

/** Tests for CPP API query conditions */
struct QueryConditionTest {
  // Constructors/Destructors
  QueryConditionTest(TestParams params);
  ~QueryConditionTest();

  void validate(QueryCondition& qc, Chooser chooser);
  void validate(
      QueryCondition& qc,
      TRange x_range,
      TRange y_range,
      Chooser chooser,
      bool range_specified = true);

  void create_array();
  void update_array(TRange x_range, TRange y_range);
  void load_array();
  void query_to_cells(
      Query& query, std::vector<QCTestCell>& cells, bool loading = false);
  void remove_array();

  // TileDB context
  Context ctx_;
  VFS vfs_;
  TestParams params_;

  std::vector<QCTestCell> cells_;
};

TEST_CASE(
    "Testing read query with basic QC", "[query][query-condition][simple]") {
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, {{"legacy", true}}),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, {{"allow_dups", true}}),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR));

  QueryConditionTest t(params);

  QueryCondition qc(t.ctx_, "b", 50.0f, TILEDB_LT);
  auto pred = [](QCTestCell& cell) { return cell.b_ < 50.0f; };

  // Simple QC with no range specified
  t.validate(qc, pred);

  // Simple QC negation with no range specified
  auto neg_qc = qc.negate();
  t.validate(neg_qc, [&pred](QCTestCell& cell) { return !pred(cell); });

  // Simple QC with range in a single tile
  t.validate(qc, {2, 3}, {2, 3}, pred);

  // Simple QC with a range spanning tiles on each dimension
  t.validate(qc, {2, 3}, {2, 15}, pred);
  t.validate(qc, {2, 15}, {2, 3}, pred);

  // Simple QC with a range spanning tiles in both dimensions
  t.validate(qc, {2, 15}, {2, 15}, pred);
}

TEST_CASE(
    "Testing read query with complex QC", "[query][query-condition][complex]") {
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, {{"legacy", true}}),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, {{"allow_dups", true}}),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR));

  QueryConditionTest t(params);

  QueryCondition qc1(t.ctx_, "a", 25, TILEDB_GT);
  QueryCondition qc2(t.ctx_, "a", 75, TILEDB_LT);
  QueryCondition qc3(t.ctx_, "b", 25.0f, TILEDB_GT);
  QueryCondition qc4(t.ctx_, "b", 75.0f, TILEDB_LT);
  QueryCondition qc = (qc1 && qc2) || (qc3 && qc4);

  auto pred = [](QCTestCell& cell) {
    return (
        (cell.a_ > 25 && cell.a_ < 75) || (cell.b_ > 25.0f && cell.b_ < 75.0f));
  };

  // QC with no range specified
  t.validate(qc, pred);

  // QC negation with no range specified
  auto neg_qc = qc.negate();
  t.validate(neg_qc, [&pred](QCTestCell& cell) { return !pred(cell); });

  // QC with range in a single tile
  t.validate(qc, {2, 3}, {2, 3}, pred);

  // QC with a range spanning tiles on each dimension
  t.validate(qc, {2, 3}, {2, 15}, pred);
  t.validate(qc, {2, 15}, {2, 3}, pred);

  // Simple QC with a range spanning tiles in both dimensions
  t.validate(qc, {2, 15}, {2, 15}, pred);
}

TEST_CASE(
    "Testing read query with complex QC against ASCII strings",
    "[query][query-condition][ascii-strings]") {
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, {{"legacy", true}}),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, {{"allow_dups", true}}),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR));

  QueryConditionTest t(params);

  QueryCondition qc1(t.ctx_, "c", "craig", TILEDB_EQ);
  QueryCondition qc2(t.ctx_, "c", "grace", TILEDB_EQ);
  QueryCondition qc3(t.ctx_, "c", "ivan", TILEDB_GE);
  QueryCondition qc = qc1 || qc2 || qc3;

  auto pred = [](QCTestCell& cell) {
    if (cell.c_ == "craig") {
      return true;
    }
    if (cell.c_ == "grace") {
      return true;
    }
    if (cell.c_ >= "ivan") {
      return true;
    }
    return false;
  };

  // QC with no range specified
  t.validate(qc, pred);

  // QC negation with no range specified
  auto neg_qc = qc.negate();
  t.validate(neg_qc, [&pred](QCTestCell& cell) { return !pred(cell); });

  // QC with range in a single tile
  t.validate(qc, {2, 3}, {2, 3}, pred);

  // QC with a range spanning tiles on each dimension
  t.validate(qc, {2, 3}, {2, 15}, pred);
  t.validate(qc, {2, 15}, {2, 3}, pred);

  // Simple QC with a range spanning tiles in both dimensions
  t.validate(qc, {2, 15}, {2, 15}, pred);
}

TEST_CASE(
    "Testing read query with complex QC against UTF-8 strings",
    "[query][query-condition][utf-8-strings]") {
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, {{"legacy", true}}),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, {{"allow_dups", true}}),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR));

  QueryConditionTest t(params);

  QueryCondition qc1(t.ctx_, "d", "aa", TILEDB_EQ);
  QueryCondition qc2(t.ctx_, "d", "\xc3\xb1", TILEDB_EQ);  // n-with-tilda
  QueryCondition qc3(t.ctx_, "d", "\xf0\x9f\x80\x84", TILEDB_GE);  // :mahjong:
  QueryCondition qc = qc1 || qc2 || qc3;

  auto pred = [](QCTestCell& cell) {
    if (cell.d_ == "aa") {
      return true;
    }
    if (cell.d_ == "\xc3\xb1") {
      return true;
    }
    if (cell.d_ >= "\xf0\x9f\x80\x84") {
      return true;
    }
    return false;
  };

  // QC with no range specified
  t.validate(qc, pred);

  // QC negation with no range specified
  auto neg_qc = qc.negate();
  t.validate(neg_qc, [&pred](QCTestCell& cell) { return !pred(cell); });

  // QC with range in a single tile
  t.validate(qc, {2, 3}, {2, 3}, pred);

  // QC with a range spanning tiles on each dimension
  t.validate(qc, {2, 3}, {2, 15}, pred);
  t.validate(qc, {2, 15}, {2, 3}, pred);

  // Simple QC with a range spanning tiles in both dimensions
  t.validate(qc, {2, 15}, {2, 15}, pred);
}

TEST_CASE(
    "Testing read query with complex QC after array update",
    "[query][query-condition][with-updates]") {
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, {{"legacy", true}}),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, {{"allow_dups", true}}),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR));

  QueryConditionTest t(params);

  QueryCondition qc1(t.ctx_, "a", 25, TILEDB_GT);
  QueryCondition qc2(t.ctx_, "a", 75, TILEDB_LT);
  QueryCondition qc3(t.ctx_, "b", 25.0f, TILEDB_GT);
  QueryCondition qc4(t.ctx_, "b", 75.0f, TILEDB_LT);
  QueryCondition qc = (qc1 && qc2) || (qc3 && qc4);

  auto pred = [](QCTestCell& cell) {
    return (
        (cell.a_ > 25 && cell.a_ < 75) || (cell.b_ > 25.0f && cell.b_ < 75.0f));
  };

  // QC with no range specified
  t.validate(qc, pred);

  // QC negation with no range specified
  auto neg_qc = qc.negate();
  t.validate(neg_qc, [&pred](QCTestCell& cell) { return !pred(cell); });

  // QC with range in a single tile
  t.validate(qc, {2, 3}, {2, 3}, pred);

  // QC with a range spanning tiles on each dimension
  t.validate(qc, {2, 3}, {2, 15}, pred);
  t.validate(qc, {2, 15}, {2, 3}, pred);

  // Simple QC with a range spanning tiles in both dimensions
  t.validate(qc, {2, 15}, {2, 15}, pred);

  // Update the center of the array
  t.update_array({5, 15}, {5, 15});
  t.load_array();

  // Re-run all tests
  t.validate(qc, pred);
  t.validate(neg_qc, [&pred](QCTestCell& cell) { return !pred(cell); });
  t.validate(qc, {2, 3}, {2, 3}, pred);
  t.validate(qc, {2, 3}, {2, 15}, pred);
  t.validate(qc, {2, 15}, {2, 3}, pred);
  t.validate(qc, {2, 15}, {2, 15}, pred);
}

TEST_CASE(
    "Testing read query with QC on dimensions",
    "[query][query-condition][dimensions]") {
  // TODO: Figure out how to match query conditions
  // against dense coordinates.

  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, {{"legacy", true}}),
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED));

  QueryConditionTest t(params);

  QueryCondition qc1(t.ctx_, "x", 5, TILEDB_GE);
  QueryCondition qc2(t.ctx_, "x", 15, TILEDB_LE);
  QueryCondition qc3(t.ctx_, "y", 5, TILEDB_GE);
  QueryCondition qc4(t.ctx_, "y", 15, TILEDB_LE);
  QueryCondition qc5(t.ctx_, "b", 50.0f, TILEDB_LT);
  QueryCondition qc = qc1 && qc2 && qc3 && qc4 && qc5;

  auto pred = [](QCTestCell& cell) {
    return cell.x_ >= 5 && cell.x_ <= 15 && cell.y_ >= 5 && cell.y_ <= 15 &&
           cell.b_ < 50.0f;
  };

  // QC with no range specified
  t.validate(qc, pred);

  // QC negation with no range specified
  auto neg_qc = qc.negate();
  t.validate(neg_qc, [&pred](QCTestCell& cell) { return !pred(cell); });

  t.validate(qc, {2, 3}, {2, 3}, pred);
  t.validate(qc, {2, 3}, {2, 17}, pred);
  t.validate(qc, {2, 17}, {2, 3}, pred);
  t.validate(qc, {2, 17}, {2, 17}, pred);
}

TEST_CASE(
    "Testing read query with simple QC with string dimension",
    "[query][query-condition][dimensions][string]") {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(ARRAY_NAME)) {
    vfs.remove_dir(ARRAY_NAME);
  }

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_capacity(16);

  auto dim_x = Dimension::create<int>(ctx, "x", {{1, 4}}, 4);
  auto dim_y =
      Dimension::create(ctx, "y", TILEDB_STRING_ASCII, nullptr, nullptr);
  auto att_a = Attribute::create<int>(ctx, "a");

  Domain dom(ctx);
  dom.add_dimensions(dim_x, dim_y);
  schema.set_domain(dom);

  schema.add_attribute(att_a);

  Array::create(ARRAY_NAME, schema);

  // Write some initial data and close the array.
  std::vector<int> x_data = {1, 2, 3, 4};
  std::string y_data = "johnpaulringogeorge";
  std::vector<uint64_t> y_off = {0, 4, 8, 13};
  std::vector<int> a_data = {42, 41, 40, 39};

  Array array_w(ctx, ARRAY_NAME, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("x", x_data)
      .set_data_buffer("y", y_data)
      .set_offsets_buffer("y", y_off)
      .set_data_buffer("a", a_data);

  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Read the data with query condition on the string dimension.
  Array array(ctx, ARRAY_NAME, TILEDB_READ);
  Query query(ctx, array);

  QueryCondition qc(ctx, "y", "ringo", TILEDB_EQ);

  std::vector<int> x_read(4);
  std::vector<char> y_read(y_data.size());
  std::vector<uint64_t> y_off_read(4);
  std::vector<int> a_read(4);

  query.set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("x", x_read)
      .set_data_buffer("y", y_read)
      .set_offsets_buffer("y", y_off_read)
      .set_data_buffer("a", a_read)
      .set_condition(qc);
  query.submit();

  auto table = query.result_buffer_elements();
  x_read.resize(table["x"].second);
  y_read.resize(table["y"].second);
  y_off_read.resize(table["y"].first);
  a_read.resize(table["a"].second);

  REQUIRE(query.query_status() == Query::Status::COMPLETE);

  REQUIRE(x_read.size() == 1);
  REQUIRE(x_read[0] == 3);

  REQUIRE(std::string(y_read.begin(), y_read.end()) == "ringo");

  REQUIRE(y_off_read.size() == 1);
  REQUIRE(y_off_read[0] == 0);

  REQUIRE(a_read.size() == 1);
  REQUIRE(a_read[0] == 40);

  if (vfs.is_dir(ARRAY_NAME)) {
    vfs.remove_dir(ARRAY_NAME);
  }
}

TEST_CASE(
    "Testing read query with QC referencing attribute not in buffers",
    "[query][query-condition][attr-not-in-buffers]") {
  TestParams params = GENERATE(
      TestParams(
          TILEDB_SPARSE,
          TILEDB_GLOBAL_ORDER,
          {{"skip_attribute_b", true}, {"legacy", true}}),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, {{"skip_attribute_b", true}}),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, {{"skip_attribute_b", true}}));

  // TODO: Figure out how to match query conditions
  // against dense coordinates.
  // TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, fale, false);

  QueryConditionTest t(params);

  QueryCondition qc(t.ctx_, "b", 50.0f, TILEDB_LT);
  auto pred = [](QCTestCell& cell) { return cell.b_ < 50.0f; };

  // QC with no range specified
  t.validate(qc, pred);

  // QC negation with no range specified
  auto neg_qc = qc.negate();
  t.validate(neg_qc, [&pred](QCTestCell& cell) { return !pred(cell); });

  t.validate(qc, {2, 3}, {2, 3}, pred);
  t.validate(qc, {2, 3}, {2, 17}, pred);
  t.validate(qc, {2, 17}, {2, 3}, pred);
  t.validate(qc, {2, 17}, {2, 17}, pred);
}

// Test Utility Functions

float rand_float() {
  return float(std::rand()) / float(RAND_MAX);
}

float rand_float(float lo, float hi) {
  return lo + rand_float() * (hi - lo);
}

int rand_int(int lo, int hi) {
  return (int)rand_float(lo, hi);
}

std::string string_at(
    size_t i,
    std::vector<char>& data,
    std::vector<uint64_t>& offsets,
    uint64_t data_size) {
  uint64_t start = offsets[i];
  uint64_t end = UINT64_MAX;
  if (i < offsets.size() - 1) {
    end = offsets[i + 1];
  } else {
    end = data_size;
  }

  end = std::min(end, data_size);

  if (end < start) {
    throw std::logic_error("Bad string range");
  }

  return std::string(data.data() + start, end - start);
}

void add_string(
    std::string choice,
    std::vector<char>& data,
    std::vector<uint64_t>& offsets) {
  offsets.push_back(data.size());
  for (size_t i = 0; i < choice.size(); i++) {
    data.push_back(choice.at(i));
  }
}

void add_string(
    std::vector<std::string> choices,
    std::vector<char>& data,
    std::vector<uint64_t>& offsets) {
  auto choice = choices[rand_int(0, choices.size() - 1)];
  add_string(choice, data, offsets);
}

TestParams::TestParams(
    tiledb_array_type_t array_type,
    tiledb_layout_t layout,
    std::map<std::string, bool> options)
    : array_type_(array_type)
    , layout_(layout)
    , options_(options) {
  for (auto name : OPTION_NAMES) {
    if (options_.find(name) == options_.end()) {
      options_[name] = false;
    }
  }
  REQUIRE(options_.size() == OPTION_NAMES.size());
}

bool TestParams::get(std::string option) {
  if (options_.find(option) == options_.end()) {
    throw std::logic_error("Invalid option name: " + option);
  }
  return options_[option];
}

QCTestCell::QCTestCell() {
}

QCTestCell::QCTestCell(
    int x, int y, int a, float b, std::string c, std::string d)
    : x_(x)
    , y_(y)
    , a_(a)
    , b_(b)
    , c_(c)
    , d_(d) {
}

bool QCTestCell::operator==(const QCTestCell& rhs) const {
  if (x_ != rhs.x_) {
    return false;
  }
  if (y_ != rhs.y_) {
    return false;
  }
  if (a_ != rhs.a_) {
    return false;
  }
  if (b_ != rhs.b_) {
    return false;
  }
  if (c_ != rhs.c_) {
    return false;
  }
  if (d_ != rhs.d_) {
    return false;
  }
  return true;
}

bool QCTestCell::in_range(TRange x_range, TRange y_range) {
  if (x_ < x_range[0] || x_ > x_range[1]) {
    return false;
  }
  if (y_ < y_range[0] || y_ > y_range[1]) {
    return false;
  }
  return true;
}

std::string QCTestCell::to_string() const {
  std::stringstream ss;
  ss << "(" << x_ << ", " << y_ << ") = ";
  ss << "(" << a_ << ", " << b_ << ", '" << c_ << "', '" << d_ << "')";
  return ss.str();
}

std::pair<int, int> QCTestCell::key() const {
  return std::make_pair(x_, y_);
}

bool operator<(QCTestCell lhs, QCTestCell rhs) {
  if (lhs.x_ != rhs.x_) {
    return lhs.x_ < rhs.x_;
  }
  if (lhs.y_ != rhs.y_) {
    return lhs.y_ < rhs.y_;
  }
  if (lhs.a_ != rhs.a_) {
    return lhs.a_ < rhs.a_;
  }
  if (lhs.b_ != rhs.b_) {
    return lhs.b_ < rhs.b_;
  }
  if (lhs.c_ != rhs.c_) {
    return lhs.c_ < rhs.c_;
  }
  if (lhs.d_ != rhs.d_) {
    return lhs.d_ < rhs.d_;
  }
  return false;
}

std::ostream& operator<<(std::ostream& os, QCTestCell const& value) {
  os << value.to_string();
  return os;
}

struct QCData {
  QCData(size_t size)
      : x_(size, -1)
      , y_(size, -1)
      , a_(size, -1)
      , b_(size, -1)
      , c_(size * 10, -1)
      , c_off_(size, UINT64_MAX)
      , d_(size * 10, -1)
      , d_off_(size, UINT64_MAX) {
  }

  QCData(TRange x_range, TRange y_range) {
    for (auto y = y_range[0]; y <= y_range[1]; y++) {
      for (auto x = x_range[0]; x <= x_range[1]; x++) {
        x_.push_back(x);
        y_.push_back(y);
        a_.push_back(rand_int(0, 100));
        b_.push_back(rand_float(0.0f, 100.0f));
        add_string(ASCII_CHOICES, c_, c_off_);
        add_string(UTF8_CHOICES, d_, d_off_);
      }
    }
  }

  std::vector<int> x_;
  std::vector<int> y_;
  std::vector<int> a_;
  std::vector<float> b_;
  std::vector<char> c_;
  std::vector<uint64_t> c_off_;
  std::vector<char> d_;
  std::vector<uint64_t> d_off_;
};

QueryConditionTest::QueryConditionTest(TestParams params)
    : vfs_(ctx_)
    , params_(params) {
  Config config;
  ctx_ = Context(config);
  vfs_ = VFS(ctx_);
  remove_array();
  create_array();
  // dump_array();
}

QueryConditionTest::~QueryConditionTest() {
  remove_array();
}

void QueryConditionTest::validate(QueryCondition& qc, Chooser chooser) {
  validate(qc, {1, NUM_ROWS}, {1, NUM_ROWS}, chooser, false);
}

void QueryConditionTest::validate(
    QueryCondition& qc,
    TRange x_range,
    TRange y_range,
    Chooser chooser,
    bool range_specified) {
  // Collect our matching cells for assertions
  size_t matches = 0;
  std::vector<QCTestCell> expect;
  for (auto cell : cells_) {
    if (!cell.in_range(x_range, y_range)) {
      continue;
    }

    if (chooser(cell)) {
      matches += 1;
      if (params_.get("skip_attribute_b")) {
        cell.b_ = B_FILL;
      }
      expect.push_back(cell);
    } else if (
        params_.array_type_ == TILEDB_DENSE &&
        cell.in_range(x_range, y_range)) {
      cell.a_ = A_FILL;
      cell.b_ = B_FILL;
      cell.c_ = std::string(C_FILL);
      cell.d_ = std::string(D_FILL);
      expect.push_back(cell);
    }
  }

  // Execute the query
  Config config;
  if (params_.get("legacy")) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }

  Context ctx = Context(config);
  Array array(ctx, ARRAY_NAME, TILEDB_READ);
  Query query(ctx, array);

  // Set a subarray for dense.
  if (params_.array_type_ == TILEDB_DENSE || range_specified) {
    Subarray subarray(ctx, array);
    subarray.add_range("x", x_range[0], x_range[1]);
    subarray.add_range("y", y_range[0], y_range[1]);
    query.set_subarray(subarray);
  }

  query.set_layout(params_.layout_).set_condition(qc);

  std::vector<QCTestCell> result;
  query_to_cells(query, result);
  array.close();

  REQUIRE(result.size() == expect.size());

  std::sort(expect.begin(), expect.end());
  std::sort(result.begin(), result.end());

  size_t nulls_seen = 0;
  for (size_t i = 0; i < result.size(); i++) {
    REQUIRE(result[i] == expect[i]);
    if (result[i].a_ == A_FILL) {
      nulls_seen += 1;
    }
  }

  REQUIRE(result.size() - nulls_seen == matches);
}

void QueryConditionTest::create_array() {
  Domain dom(ctx_);
  auto dim_x = Dimension::create<int>(ctx_, "x", {{1, NUM_ROWS}}, 4);
  auto dim_y = Dimension::create<int>(ctx_, "y", {{1, NUM_ROWS}}, 4);
  dom.add_dimensions(dim_y, dim_x);

  ArraySchema schema(ctx_, params_.array_type_);
  schema.set_domain(dom).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_allows_dups(params_.get("allow_dups"));

  auto attr_a = Attribute::create<int>(ctx_, "a");
  auto attr_b = Attribute::create<float>(ctx_, "b");
  auto attr_c = Attribute::create(ctx_, "c", TILEDB_STRING_ASCII);
  auto attr_d = Attribute::create(ctx_, "d", TILEDB_STRING_UTF8);

  attr_c.set_cell_val_num(TILEDB_VAR_NUM);
  attr_d.set_cell_val_num(TILEDB_VAR_NUM);

  if (params_.array_type_ == TILEDB_DENSE) {
    attr_a.set_fill_value(&A_FILL, sizeof(int));
    attr_b.set_fill_value(&B_FILL, sizeof(float));
    attr_c.set_fill_value(C_FILL, strlen(C_FILL));
    attr_d.set_fill_value(D_FILL, strlen(D_FILL));
  } else {
    schema.set_capacity(16);
  }

  schema.add_attributes(attr_a, attr_b, attr_c, attr_d);

  Array::create(ARRAY_NAME, schema);
  update_array({1, NUM_ROWS}, {1, NUM_ROWS});
  load_array();
}

void QueryConditionTest::update_array(TRange x_range, TRange y_range) {
  QCData data(x_range, y_range);

  Array array(ctx_, ARRAY_NAME, TILEDB_WRITE);
  Query query(ctx_, array);

  if (params_.array_type_ == TILEDB_SPARSE) {
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("x", data.x_)
        .set_data_buffer("y", data.y_);
  } else {
    Subarray subarray(ctx_, array);
    subarray.add_range("x", x_range[0], x_range[1]);
    subarray.add_range("y", y_range[0], y_range[1]);

    query.set_layout(TILEDB_ROW_MAJOR).set_subarray(subarray);
  }

  query.set_data_buffer("a", data.a_)
      .set_data_buffer("b", data.b_)
      .set_data_buffer("c", data.c_)
      .set_offsets_buffer("c", data.c_off_)
      .set_data_buffer("d", data.d_)
      .set_offsets_buffer("d", data.d_off_);

  query.submit();
  query.finalize();
  array.close();
}

void QueryConditionTest::load_array() {
  Array array(ctx_, ARRAY_NAME, TILEDB_READ);
  Query query(ctx_, array);
  query.set_layout(TILEDB_ROW_MAJOR);

  if (params_.array_type_ == TILEDB_DENSE) {
    int range[] = {1, NUM_ROWS};
    query.add_range("y", range[0], range[1]);
    query.add_range("x", range[0], range[1]);
  }

  query_to_cells(query, cells_, true);

  array.close();
}

void QueryConditionTest::query_to_cells(
    Query& query, std::vector<QCTestCell>& cells, bool loading) {
  QCData data(NUM_ROWS * NUM_ROWS);

  query.set_data_buffer("x", data.x_)
      .set_data_buffer("y", data.y_)
      .set_data_buffer("a", data.a_)
      .set_data_buffer("c", data.c_)
      .set_data_buffer("d", data.d_)
      .set_offsets_buffer("c", data.c_off_)
      .set_offsets_buffer("d", data.d_off_);

  if (!params_.get("skip_attribute_b") || loading) {
    query.set_data_buffer("b", data.b_);
  }

  query.submit();
  REQUIRE(query.query_status() == Query::Status::COMPLETE);

  auto table = query.result_buffer_elements();

  cells.clear();
  for (size_t i = 0; i < table["x"].second; i++) {
    if (!params_.get("skip_attribute_b") || loading) {
      cells.emplace_back(
          data.x_[i],
          data.y_[i],
          data.a_[i],
          data.b_[i],
          string_at(i, data.c_, data.c_off_, table["c"].second),
          string_at(i, data.d_, data.d_off_, table["d"].second));
    } else {
      cells.emplace_back(
          data.x_[i],
          data.y_[i],
          data.a_[i],
          B_FILL,
          string_at(i, data.c_, data.c_off_, table["c"].second),
          string_at(i, data.d_, data.d_off_, table["d"].second));
    }
  }
}

void QueryConditionTest::remove_array() {
  if (!vfs_.is_dir(ARRAY_NAME)) {
    return;
  }

  vfs_.remove_dir(ARRAY_NAME);
}
