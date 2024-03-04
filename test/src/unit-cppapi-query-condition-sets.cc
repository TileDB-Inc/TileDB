/**
 * @file unit-cppapi-query-condition-sets.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests the C++ API for query condition related functions.
 */

#include "test/support/src/ast_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/serialization/query.h"

#ifdef TILEDB_SERIALIZATION
#include <capnp/message.h>
#include <capnp/serialize.h>
#endif

using namespace tiledb;

enum class TestArrayType : uint8_t { DENSE, SPARSE, LEGACY };

struct QCSetsCell {
  QCSetsCell(
      int dim,
      float attr1,
      const std::string& attr2,
      const std::string& attr3,
      uint8_t attr3_validity,
      const std::string& attr4,
      const std::string& attr5,
      int attr6)
      : d(dim)
      , a1(attr1)
      , a2(attr2)
      , a3(attr3)
      , a3v(attr3_validity)
      , a4(attr4)
      , a5(attr5)
      , a6(attr6) {
  }

  int d;
  float a1;
  std::string a2;
  std::string a3;
  uint8_t a3v;
  std::string a4;
  std::string a5;
  int a6;
};

using QCSetsCellSelector = std::function<bool(QCSetsCell& cell)>;

struct CPPQueryConditionFx {
  CPPQueryConditionFx();
  ~CPPQueryConditionFx();

  void create_array(TestArrayType type, bool serialize);
  void write_delete(QueryCondition& qc);
  void rm_array();

  void check_read(QueryCondition& qc, QCSetsCellSelector func);

  void generate_data();
  void select_data(
      QCSetsCellSelector func,
      std::vector<int>& dim_expect,
      std::vector<float>& attr1_expect,
      std::vector<std::string>& attr2_expect,
      std::vector<std::string>& attr3_expect,
      std::vector<std::string>& attr4_expect,
      std::vector<std::string>& attr5_expect,
      std::vector<int>& attr6_expect);

  QueryCondition serialize_deserialize_qc(QueryCondition& qc);

  std::pair<std::string, std::vector<uint64_t>> to_buffers(
      std::vector<std::string>& values);
  std::string to_fixed_buffer(std::vector<std::string>& values);
  std::vector<std::string> to_vector(
      std::vector<char>& data, uint64_t elem_size);
  std::vector<std::string> to_vector(
      uint64_t data_size,
      std::vector<char>& data,
      std::vector<uint64_t>& offsets);
  std::vector<std::string> to_vector(
      uint64_t data_size,
      std::vector<char>& data,
      std::vector<uint64_t>& offsets,
      std::vector<uint8_t>& validity);

  template <typename T>
  T choose_value(std::vector<T>& values);
  float random();

  std::string uri_;
  Context ctx_;
  VFS vfs_;
  TestArrayType type_;
  bool serialize_;

  // Array Data
  int num_elements_;
  std::vector<int> dim_values_;
  std::vector<float> attr1_values_;
  std::vector<std::string> attr2_values_;
  std::vector<std::string> attr3_values_;
  std::vector<uint8_t> attr3_validity_;
  std::vector<std::string> attr4_values_;
  std::vector<std::string> attr5_values_;
  std::vector<int> attr6_values_;
};

#ifdef TILEDB_SERIALIZATION
#define SERIALIZE_TESTS() GENERATE(false, true)
#else
#define SERIALIZE_TESTS() false
#endif

TEST_CASE_METHOD(
    CPPQueryConditionFx, "IN - Float", "[query-condition][set][basic][float]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<float> values = {2.0f, 4.0f};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr1", values, TILEDB_IN);

  check_read(
      qc, [](const QCSetsCell& c) { return (c.a1 == 2.0f || c.a1 == 4.0f); });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN - String",
    "[query-condition][set][basic][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"barney", "wilma"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr2", values, TILEDB_IN);

  check_read(qc, [](const QCSetsCell& c) {
    return (c.a2 == "barney" || c.a2 == "wilma");
  });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN - Fixed Length String",
    "[query-condition][set][basic][fixed-length-string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"hack", "pack"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr5", values, TILEDB_IN);

  check_read(qc, [](const QCSetsCell& c) {
    return (c.a5 == "hack" || c.a5 == "pack");
  });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN - Int Dimension",
    "[query-condition][set][basic][int][dimension]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<int> values = {1, 5};
  auto qc = QueryConditionExperimental::create(ctx_, "dim", values, TILEDB_IN);

  check_read(qc, [](const QCSetsCell& c) { return (c.d == 1 || c.d == 5); });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN - Enumeration",
    "[query-condition][set][basic][enumeration]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"wilma", "betty"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr6", values, TILEDB_IN);

  check_read(qc, [](const QCSetsCell& c) { return (c.a6 == 1 || c.a6 == 3); });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN Nullable - String",
    "[query-condition][set][in-with-nullable][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"blue", "umber"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr3", values, TILEDB_IN);

  check_read(qc, [](const QCSetsCell& c) {
    return (c.a3 == "blue" || c.a3 == "umber");
  });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "NOT_IN - String",
    "[query-condition][set][not-in][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"wilma"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr2", values, TILEDB_NOT_IN);

  check_read(qc, [](const QCSetsCell& c) { return !(c.a2 == "wilma"); });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN - String With Non-Enumeration Value",
    "[query-condition][set][non-enum-value][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"wilma", "astro"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr2", values, TILEDB_NOT_IN);

  check_read(qc, [](const QCSetsCell& c) { return !(c.a2 == "wilma"); });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "NOT_IN - Enumeration",
    "[query-condition][set][basic][enumeration]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"wilma", "betty"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr6", values, TILEDB_NOT_IN);

  check_read(qc, [](const QCSetsCell& c) { return (c.a6 != 1 && c.a6 != 3); });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "NOT_IN Nullable - String",
    "[query-condition][set][not-in-nullable][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"green", "teal"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr3", values, TILEDB_NOT_IN);

  check_read(qc, [](const QCSetsCell& c) {
    return (c.a3v == 1) && !(c.a3 == "green" || c.a3 == "teal");
  });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN - Empty String",
    "[query-condition][set][in][empty-string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"", ""};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr4", values, TILEDB_IN);

  check_read(qc, [](const QCSetsCell& c) { return c.a4 == ""; });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN - Non-empty string doesn't match empty string",
    "[query-condition][set][in][non-empty-string-no-match-empty]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"not empty"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr4", values, TILEDB_IN);

  check_read(qc, [](const QCSetsCell& c) { return c.a4 == "not empty"; });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "NOT IN - Empty String",
    "[query-condition][set][not-in][empty-string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"", ""};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr4", values, TILEDB_NOT_IN);

  check_read(qc, [](const QCSetsCell& c) { return c.a4 != ""; });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "Negated IN - String",
    "[query-condition][set][negated-in][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"wilma"};
  auto qc1 =
      QueryConditionExperimental::create(ctx_, "attr2", values, TILEDB_IN);
  auto qc2 = qc1.negate();

  check_read(qc2, [](const QCSetsCell& c) { return !(c.a2 == "wilma"); });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "Negated NOT IN - String",
    "[query-condition][set][negated-not-in][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"wilma", "betty"};
  auto qc1 =
      QueryConditionExperimental::create(ctx_, "attr2", values, TILEDB_NOT_IN);
  auto qc2 = qc1.negate();

  check_read(qc2, [](const QCSetsCell& c) {
    return (c.a2 == "wilma" || c.a2 == "betty");
  });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN with AND - String",
    "[query-condition][set][in-with-and][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"wilma", "betty"};
  auto qc1 =
      QueryConditionExperimental::create(ctx_, "attr2", values, TILEDB_IN);
  auto qc2 = QueryCondition::create(ctx_, "attr1", 2.0f, TILEDB_GT);
  auto qc3 = qc1.combine(qc2, TILEDB_AND);

  check_read(qc3, [](const QCSetsCell& c) {
    return (c.a1 > 2.0f && (c.a2 == "betty" || c.a2 == "wilma"));
  });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "IN with OR - String",
    "[query-condition][set][in-with-or][string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"wilma", "betty"};
  auto qc1 =
      QueryConditionExperimental::create(ctx_, "attr2", values, TILEDB_IN);
  auto qc2 = QueryCondition::create(ctx_, "attr1", 3.0f, TILEDB_EQ);
  auto qc3 = qc1.combine(qc2, TILEDB_OR);

  check_read(qc3, [](const QCSetsCell& c) {
    return (c.a1 == 3.0f || (c.a2 == "betty" || c.a2 == "wilma"));
  });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "Delete with SET - String",
    "[query-condition][set][delete-with-set][string]") {
  auto type = GENERATE(TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> del_values = {"wilma"};
  auto del_qc =
      QueryConditionExperimental::create(ctx_, "attr2", del_values, TILEDB_IN);
  write_delete(del_qc);

  std::vector<std::string> values = {"wilma", "betty"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr2", values, TILEDB_IN);

  check_read(qc, [](const QCSetsCell& c) {
    // Every instance of "wilma" was deleted so we only expect "betty"
    return c.a2 == "betty";
  });
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "INT - Set Member Size Check",
    "[query-condition][set][size-check][int]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"", "foo"};
  auto qc = QueryConditionExperimental::create(ctx_, "dim", values, TILEDB_IN);

  REQUIRE_THROWS(check_read(qc, [](const QCSetsCell&) -> bool {
    throw std::logic_error("Shouldn't get here.");
  }));
}

TEST_CASE_METHOD(
    CPPQueryConditionFx,
    "Fixed Length STRING - Set Member Size Check",
    "[query-condition][set][size-check][fixed-length-string]") {
  auto type = GENERATE(
      TestArrayType::DENSE, TestArrayType::SPARSE, TestArrayType::LEGACY);
  auto serialize = SERIALIZE_TESTS();
  create_array(type, serialize);

  std::vector<std::string> values = {"oh", "hi"};
  auto qc =
      QueryConditionExperimental::create(ctx_, "attr5", values, TILEDB_IN);

  REQUIRE_THROWS(check_read(qc, [](const QCSetsCell&) -> bool {
    throw std::logic_error("Shouldn't get here.");
  }));
}

TEST_CASE(
    "Error - C API - nullptr field name", "[query-condition][set][error]") {
  Context ctx;
  const char* data = "foobar";
  uint64_t offsets[2] = {0, 3};
  tiledb_query_condition_t* qc;
  auto rc = tiledb_query_condition_alloc_set_membership(
      ctx.ptr().get(), nullptr, data, 6, offsets, 16, TILEDB_IN, &qc);
  REQUIRE(rc == TILEDB_ERR);
}

TEST_CASE("AST Constructor - Basic Check", "[query-condition][set][ast]") {
  const char* data = "foobar";
  uint64_t offsets[2] = {0, 3};
  REQUIRE_NOTHROW(
      sm::ASTNodeVal("foo", data, 6, offsets, 16, sm::QueryConditionOp::IN));
}

TEST_CASE(
    "AST Constructor - data nullptr", "[query-condition][set][ast][error]") {
  uint64_t offsets[2] = {0, 3};
  REQUIRE_THROWS(
      sm::ASTNodeVal("foo", nullptr, 6, offsets, 16, sm::QueryConditionOp::IN));
}

TEST_CASE(
    "AST Constructor - data_size is zero",
    "[query-condition][set][ast][error]") {
  const char* data = "foobar";
  uint64_t offsets[2] = {0, 3};
  REQUIRE_THROWS(
      sm::ASTNodeVal("foo", data, 0, offsets, 16, sm::QueryConditionOp::IN));
}

TEST_CASE(
    "AST Constructor - offsets nullptr", "[query-condition][set][ast][error]") {
  const char* data = "foobar";
  REQUIRE_THROWS(
      sm::ASTNodeVal("foo", data, 6, nullptr, 16, sm::QueryConditionOp::IN));
}

TEST_CASE(
    "AST Constructor - offsets_size is zero",
    "[query-condition][set][ast][error]") {
  const char* data = "foobar";
  uint64_t offsets[2] = {0, 3};
  REQUIRE_THROWS(
      sm::ASTNodeVal("foo", data, 6, offsets, 0, sm::QueryConditionOp::IN));
}

TEST_CASE(
    "AST Constructor - offsets_size not multiple of 8",
    "[query-condition][set][ast][error]") {
  const char* data = "foobar";
  uint64_t offsets[2] = {0, 3};
  REQUIRE_THROWS(
      sm::ASTNodeVal("foo", data, 6, offsets, 17, sm::QueryConditionOp::IN));
}

TEST_CASE(
    "AST Constructor - offsets not ordered",
    "[query-condition][set][ast][error]") {
  const char* data = "foobar";
  uint64_t offsets[2] = {3, 0};
  REQUIRE_THROWS(
      sm::ASTNodeVal("foo", data, 6, offsets, 16, sm::QueryConditionOp::IN));
}

TEST_CASE(
    "AST Constructor - offsets reference beyond data_size",
    "[query-condition][set][ast][error]") {
  const char* data = "foobar";
  uint64_t offsets[2] = {0, 200};
  REQUIRE_THROWS(
      sm::ASTNodeVal("foo", data, 6, offsets, 16, sm::QueryConditionOp::IN));
}

TEST_CASE(
    "AST Constructor - invalid non-set operator",
    "[query-condition][set][ast][error]") {
  const char* data = "foobar";
  uint64_t offsets[2] = {0, 3};
  REQUIRE_THROWS(
      sm::ASTNodeVal("foo", data, 6, offsets, 16, sm::QueryConditionOp::LT));
}

TEST_CASE(
    "AST Constructor - invalid set operator",
    "[query-condition][set][ast][error]") {
  const char* data = "foobar";
  REQUIRE_THROWS(sm::ASTNodeVal("foo", data, 6, sm::QueryConditionOp::IN));
}

TEST_CASE("AST Expression Errors", "[query-condition][set][error]") {
  auto v1 = tdb_unique_ptr<sm::ASTNode>(
      tdb_new(sm::ASTNodeVal, "foo", "bar", 3, sm::QueryConditionOp::LT));
  auto v2 = tdb_unique_ptr<sm::ASTNode>(
      tdb_new(sm::ASTNodeVal, "foo", "baz", 3, sm::QueryConditionOp::GT));
  auto expr = v1->combine(v2, sm::QueryConditionCombinationOp::OR);
  REQUIRE_THROWS(expr->get_value_ptr());
  REQUIRE_THROWS(expr->get_value_size());
  REQUIRE_THROWS(expr->get_data());
  REQUIRE_THROWS(expr->get_offsets());
}

CPPQueryConditionFx::CPPQueryConditionFx()
    : uri_("query_condition_test_array")
    , vfs_(ctx_) {
  rm_array();
}

CPPQueryConditionFx::~CPPQueryConditionFx() {
  rm_array();
}

void CPPQueryConditionFx::create_array(TestArrayType type, bool serialize) {
  type_ = type;
  serialize_ = serialize;

  generate_data();

  tiledb_array_type_t array_type = TILEDB_SPARSE;
  if (type_ == TestArrayType::DENSE) {
    array_type = TILEDB_DENSE;
  }

  ArraySchema schema(ctx_, array_type);

  auto range =
      std::array<int, 2>({dim_values_[0], dim_values_[dim_values_.size() - 1]});
  auto dim = Dimension::create<int>(ctx_, "dim", range);
  auto dom = Domain(ctx_);
  dom.add_dimension(dim);
  schema.set_domain(dom);

  auto attr1 = Attribute::create<float>(ctx_, "attr1");
  schema.add_attribute(attr1);

  auto attr2 = Attribute::create<std::string>(ctx_, "attr2");
  if (type_ == TestArrayType::DENSE) {
    attr2.set_fill_value("x", 1);
  }
  schema.add_attribute(attr2);

  auto attr3 = Attribute::create(ctx_, "attr3", TILEDB_STRING_UTF8);
  attr3.set_cell_val_num(TILEDB_VAR_NUM);
  attr3.set_nullable(true);
  schema.add_attribute(attr3);

  auto attr4 = Attribute::create<std::string>(ctx_, "attr4");
  if (type_ == TestArrayType::DENSE) {
    attr4.set_fill_value("z", 1);
  }
  schema.add_attribute(attr4);

  auto attr5 = Attribute::create<char>(ctx_, "attr5");
  attr5.set_cell_val_num(4);
  if (type_ == TestArrayType::DENSE) {
    attr5.set_fill_value("xkcd", 4);
  }
  schema.add_attribute(attr5);

  std::vector<std::string> enmr_values = {"fred", "wilma", "barney", "betty"};
  auto enmr = Enumeration::create(ctx_, "attr6_enmr", enmr_values);
  ArraySchemaExperimental::add_enumeration(ctx_, schema, enmr);

  auto attr6 = Attribute::create<int>(ctx_, "attr6");
  AttributeExperimental::set_enumeration_name(ctx_, attr6, "attr6_enmr");
  schema.add_attribute(attr6);

  if (type_ != TestArrayType::DENSE) {
    schema.set_capacity(1024);
  }

  Array::create(uri_, schema);

  Array array(ctx_, uri_, TILEDB_WRITE);
  Query query(ctx_, array);

  if (type_ == TestArrayType::DENSE) {
    Subarray subarray(ctx_, array);
    subarray.add_range(0, dim_values_[0], dim_values_[dim_values_.size() - 1]);
    query.set_subarray(subarray);
  } else {
    query.set_data_buffer("dim", dim_values_);
  }

  auto [attr2_data, attr2_offsets] = to_buffers(attr2_values_);
  auto [attr3_data, attr3_offsets] = to_buffers(attr3_values_);
  auto [attr4_data, attr4_offsets] = to_buffers(attr4_values_);
  auto attr5_data = to_fixed_buffer(attr5_values_);

  query.set_data_buffer("attr1", attr1_values_)
      .set_data_buffer("attr2", attr2_data)
      .set_offsets_buffer("attr2", attr2_offsets)
      .set_data_buffer("attr3", attr3_data)
      .set_offsets_buffer("attr3", attr3_offsets)
      .set_validity_buffer("attr3", attr3_validity_)
      .set_data_buffer("attr4", attr4_data)
      .set_offsets_buffer("attr4", attr4_offsets)
      .set_data_buffer("attr5", attr5_data)
      .set_data_buffer("attr6", attr6_values_);

  CHECK_NOTHROW(query.submit());
  query.finalize();
  array.close();
}

void CPPQueryConditionFx::write_delete(QueryCondition& qc) {
  Array array(ctx_, uri_, TILEDB_DELETE);
  Query query(ctx_, array);
  query.set_condition(qc);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
}

void CPPQueryConditionFx::check_read(
    QueryCondition& qc, QCSetsCellSelector func) {
  Array array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);

  if (type_ == TestArrayType::DENSE) {
    Subarray subarray(ctx_, array);
    subarray.add_range(0, dim_values_[0], dim_values_[dim_values_.size() - 1]);
    query.set_subarray(subarray);
  }

  if (type_ == TestArrayType::SPARSE) {
    query.set_layout(TILEDB_GLOBAL_ORDER);
  }

  std::vector<int> dim_read(num_elements_);
  std::vector<float> attr1_read(num_elements_);
  std::vector<char> attr2_read(num_elements_ * 10);
  std::vector<uint64_t> attr2_read_offsets(num_elements_);
  std::vector<char> attr3_read(num_elements_ * 10);
  std::vector<uint64_t> attr3_read_offsets(num_elements_);
  std::vector<uint8_t> attr3_read_validity(num_elements_);
  std::vector<char> attr4_read(num_elements_ * 10);
  std::vector<uint64_t> attr4_read_offsets(num_elements_);
  std::vector<char> attr5_read(num_elements_ * 4);
  std::vector<int> attr6_read(num_elements_);

  if (serialize_) {
    qc = serialize_deserialize_qc(qc);
  }

  auto core_array = array.ptr().get()->array_;
  auto core_qc = qc.ptr().get()->query_condition_;
  throw_if_not_ok(core_qc->check(core_array->array_schema_latest()));

  query.set_condition(qc)
      .set_data_buffer("dim", dim_read)
      .set_data_buffer("attr1", attr1_read)
      .set_data_buffer("attr2", attr2_read)
      .set_offsets_buffer("attr2", attr2_read_offsets)
      .set_data_buffer("attr3", attr3_read)
      .set_offsets_buffer("attr3", attr3_read_offsets)
      .set_validity_buffer("attr3", attr3_read_validity)
      .set_data_buffer("attr4", attr4_read)
      .set_offsets_buffer("attr4", attr4_read_offsets)
      .set_data_buffer("attr5", attr5_read)
      .set_data_buffer("attr6", attr6_read);

  REQUIRE(query.submit() == Query::Status::COMPLETE);

  auto table = query.result_buffer_elements();
  dim_read.resize(table["dim"].second);
  attr1_read.resize(table["attr1"].second);
  attr2_read_offsets.resize(table["attr2"].first);
  attr3_read_offsets.resize(table["attr3"].first);
  attr3_read_validity.resize(table["attr3"].first);
  attr4_read_offsets.resize(table["attr4"].first);
  attr5_read.resize(table["attr5"].second);
  attr6_read.resize(table["attr6"].second);

  auto attr2_strings =
      to_vector(table["attr2"].second, attr2_read, attr2_read_offsets);
  auto attr3_strings = to_vector(
      table["attr3"].second,
      attr3_read,
      attr3_read_offsets,
      attr3_read_validity);
  auto attr4_strings =
      to_vector(table["attr4"].second, attr4_read, attr4_read_offsets);
  auto attr5_strings = to_vector(attr5_read, 4);

  std::vector<int> dim_expected;
  std::vector<float> attr1_expected;
  std::vector<std::string> attr2_expected;
  std::vector<std::string> attr3_expected;
  std::vector<std::string> attr4_expected;
  std::vector<std::string> attr5_expected;
  std::vector<int> attr6_expected;

  select_data(
      func,
      dim_expected,
      attr1_expected,
      attr2_expected,
      attr3_expected,
      attr4_expected,
      attr5_expected,
      attr6_expected);

  REQUIRE(dim_read.size() == dim_expected.size());
  for (size_t i = 0; i < dim_expected.size(); i++) {
    REQUIRE(dim_read[i] == dim_expected[i]);
  }

  REQUIRE(attr1_read.size() == attr1_expected.size());
  for (size_t i = 0; i < attr1_expected.size(); i++) {
    // NaN != NaN, so we have to use isnan to check both
    if (std::isnan(attr1_expected[i])) {
      REQUIRE(std::isnan(attr1_read[i]));
    } else {
      REQUIRE(attr1_read[i] == attr1_expected[i]);
    }
  }

  REQUIRE(attr2_strings.size() == attr2_expected.size());
  for (size_t i = 0; i < attr2_expected.size(); i++) {
    REQUIRE(attr2_strings[i] == attr2_expected[i]);
  }

  REQUIRE(attr3_strings.size() == attr3_expected.size());
  for (size_t i = 0; i < attr3_expected.size(); i++) {
    REQUIRE(attr3_strings[i] == attr3_expected[i]);
  }

  REQUIRE(attr4_strings.size() == attr4_expected.size());
  for (size_t i = 0; i < attr4_expected.size(); i++) {
    REQUIRE(attr4_strings[i] == attr4_expected[i]);
  }

  REQUIRE(attr5_strings.size() == attr5_expected.size());
  for (size_t i = 0; i < attr5_expected.size(); i++) {
    REQUIRE(attr5_strings[i] == attr5_expected[i]);
  }

  REQUIRE(attr6_read.size() == attr6_expected.size());
  for (size_t i = 0; i < attr6_expected.size(); i++) {
    REQUIRE(attr6_read[i] == attr6_expected[i]);
  }
}

void CPPQueryConditionFx::rm_array() {
  if (vfs_.is_dir(uri_)) {
    vfs_.remove_dir(uri_);
  }
}

void CPPQueryConditionFx::generate_data() {
  num_elements_ = 1024;

  dim_values_.clear();
  attr1_values_.clear();
  attr2_values_.clear();
  attr3_values_.clear();
  attr3_validity_.clear();
  attr4_values_.clear();
  attr5_values_.clear();
  attr6_values_.clear();

  std::vector<float> floats = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  std::vector<std::string> flintstones = {"fred", "wilma", "barney", "betty"};
  std::vector<std::string> colors = {"red", "green", "blue", "teal", "umber"};
  std::vector<std::string> maybe_empty = {"", "not empty"};
  std::vector<std::string> four_chars = {"back", "hack", "pack", "sack"};
  std::vector<int> flintstone_indexes = {0, 1, 2, 3};

  for (int i = 0; i < num_elements_; i++) {
    dim_values_.push_back(i + 1);
    attr1_values_.push_back(choose_value(floats));
    attr2_values_.push_back(choose_value(flintstones));
    if (random() <= 0.1) {
      attr3_values_.push_back("");
      attr3_validity_.push_back(0);
    } else {
      attr3_values_.push_back(choose_value(colors));
      attr3_validity_.push_back(1);
    }
    attr4_values_.push_back(choose_value(maybe_empty));
    attr5_values_.push_back(choose_value(four_chars));
    attr6_values_.push_back(choose_value(flintstone_indexes));
  }
}

void CPPQueryConditionFx::select_data(
    QCSetsCellSelector func,
    std::vector<int>& dim_expect,
    std::vector<float>& attr1_expect,
    std::vector<std::string>& attr2_expect,
    std::vector<std::string>& attr3_expect,
    std::vector<std::string>& attr4_expect,
    std::vector<std::string>& attr5_expect,
    std::vector<int>& attr6_expect) {
  for (int i = 0; i < num_elements_; i++) {
    int d = dim_values_[i];
    float a1 = attr1_values_[i];
    std::string& a2 = attr2_values_[i];
    std::string& a3 = attr3_values_[i];
    uint8_t a3v = attr3_validity_[i];
    std::string& a4 = attr4_values_[i];
    std::string& a5 = attr5_values_[i];
    int a6 = attr6_values_[i];

    if (a3v == 0) {
      a3 = "<NULL>";
    }

    QCSetsCell cell(d, a1, a2, a3, a3v, a4, a5, a6);
    if (func(cell)) {
      dim_expect.push_back(d);
      attr1_expect.push_back(a1);
      attr2_expect.push_back(a2);
      attr3_expect.push_back(a3);
      attr4_expect.push_back(a4);
      attr5_expect.push_back(a5);
      attr6_expect.push_back(a6);
    } else if (type_ == TestArrayType::DENSE) {
      dim_expect.push_back(d);
      attr1_expect.push_back(NAN);
      attr2_expect.push_back("x");
      attr3_expect.push_back("<NULL>");
      attr4_expect.push_back("z");
      attr5_expect.push_back("xkcd");
      attr6_expect.push_back(std::numeric_limits<int>::min());
    }
  }
}

#ifdef TILEDB_SERIALIZATION
QueryCondition CPPQueryConditionFx::serialize_deserialize_qc(
    QueryCondition& qc) {
  using namespace tiledb::sm::serialization;
  using Condition = tiledb::sm::serialization::capnp::Condition;

  auto qc_ptr = qc.ptr().get()->query_condition_;

  QueryCondition ret(ctx_);
  auto ret_ptr = ret.ptr().get()->query_condition_;

  // Serialize the query condition.
  ::capnp::MallocMessageBuilder message;
  auto builder = message.initRoot<Condition>();
  throw_if_not_ok(condition_to_capnp(*qc_ptr, &builder));

  // Deserialize the query condition.
  *ret_ptr = condition_from_capnp(builder);
  REQUIRE(tiledb::test::ast_equal(ret_ptr->ast(), qc_ptr->ast()));

  return ret;
}
#else
QueryCondition CPPQueryConditionFx::serialize_deserialize_qc(QueryCondition&) {
  throw std::logic_error("Unable to serialize when serialization is disabled.");
}
#endif

std::pair<std::string, std::vector<uint64_t>> CPPQueryConditionFx::to_buffers(
    std::vector<std::string>& values) {
  std::stringstream data;
  std::vector<uint64_t> offsets;

  uint64_t curr_offset = 0;
  for (auto& val : values) {
    data << val;
    offsets.push_back(curr_offset);
    curr_offset += val.size();
  }

  return std::make_pair(data.str(), offsets);
}

std::string CPPQueryConditionFx::to_fixed_buffer(
    std::vector<std::string>& values) {
  std::stringstream data;

  for (auto& val : values) {
    data << val;
  }

  return data.str();
}

std::vector<std::string> CPPQueryConditionFx::to_vector(
    std::vector<char>& data, uint64_t elem_size) {
  std::vector<std::string> ret;

  auto num_elems = data.size() / elem_size;

  for (size_t i = 0; i < num_elems; i++) {
    auto start = data.data() + (i * elem_size);
    auto end = data.data() + ((i + 1) * elem_size);
    ret.emplace_back(start, end);
  }

  return ret;
}

std::vector<std::string> CPPQueryConditionFx::to_vector(
    uint64_t data_size,
    std::vector<char>& data,
    std::vector<uint64_t>& offsets) {
  std::vector<uint8_t> validity;
  return to_vector(data_size, data, offsets, validity);
}

std::vector<std::string> CPPQueryConditionFx::to_vector(
    uint64_t data_size,
    std::vector<char>& data,
    std::vector<uint64_t>& offsets,
    std::vector<uint8_t>& validity) {
  std::vector<std::string> ret;
  (void)validity;
  for (size_t i = 0; i < offsets.size(); i++) {
    if (validity.size() > 0 && validity[i] == 0) {
      ret.emplace_back("<NULL>");
      continue;
    }

    uint64_t length;
    if (i + 1 < offsets.size()) {
      length = offsets[i + 1] - offsets[i];
    } else {
      length = data_size - offsets[i];
    }

    ret.emplace_back(data.data() + offsets[i], length);
  }
  return ret;
}

template <typename T>
T CPPQueryConditionFx::choose_value(std::vector<T>& values) {
  auto rval = random();
  auto idx = static_cast<size_t>(rval * values.size());
  return values[idx];
}

float CPPQueryConditionFx::random() {
  return static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
}
