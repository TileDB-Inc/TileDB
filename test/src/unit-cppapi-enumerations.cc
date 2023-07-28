/**
 * @file unit-cppapi-enumeration.cc
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
 * Tests the C++ API for enumeration related functions.
 */

#include <fstream>

#include <test/support/tdb_catch.h>
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;

struct CPPEnumerationFx {
  CPPEnumerationFx();
  ~CPPEnumerationFx();

  template <typename T>
  void check_dump(const T& val);

  void create_array();
  void rm_array();

  std::string uri_;
  Context ctx_;
  VFS vfs_;
};

const static std::string enmr_name = "an_enumeration";
const static std::string dump_name = "enumeration_dump_test.txt";

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Create Fixed Size",
    "[enumeration][create][fixed-size]") {
  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = Enumeration::create(ctx_, enmr_name, values);
  REQUIRE(enmr.ptr() != nullptr);
  REQUIRE(enmr.name() == enmr_name);
  REQUIRE(enmr.type() == TILEDB_INT32);
  REQUIRE(enmr.cell_val_num() == 1);
  REQUIRE(enmr.ordered() == false);

  auto data = enmr.as_vector<int>();
  REQUIRE(data == values);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Create Variable Size",
    "[enumeration][create][var-size]") {
  std::vector<std::string> values = {"fee", "fi", "fo", "fum"};
  auto enmr = Enumeration::create(ctx_, enmr_name, values);
  REQUIRE(enmr.ptr() != nullptr);
  REQUIRE(enmr.name() == enmr_name);
  REQUIRE(enmr.type() == TILEDB_STRING_ASCII);
  REQUIRE(enmr.cell_val_num() == TILEDB_VAR_NUM);
  REQUIRE(enmr.ordered() == false);

  auto data = enmr.as_vector<std::string>();
  REQUIRE(data == values);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Create Ordered",
    "[enumeration][create][var-size]") {
  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = Enumeration::create(ctx_, enmr_name, values, true);
  REQUIRE(enmr.ptr() != nullptr);
  REQUIRE(enmr.name() == enmr_name);
  REQUIRE(enmr.type() == TILEDB_INT32);
  REQUIRE(enmr.cell_val_num() == 1);
  REQUIRE(enmr.ordered() == true);

  auto data = enmr.as_vector<int>();
  REQUIRE(data == values);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Dump Basic",
    "[enumeration][dump][basic]") {
  std::vector<int> values = {1, 2, 3, 4, 5};
  auto enmr = Enumeration::create(ctx_, enmr_name, values, true);
  check_dump(enmr);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Add Enumeration to Schema",
    "[enumeration][add-attribute]") {
  ArraySchema schema(ctx_, TILEDB_DENSE);

  auto dim = Dimension::create<int>(ctx_, "dim", {{-100, 100}});
  auto dom = Domain(ctx_);
  dom.add_dimension(dim);
  schema.set_domain(dom);

  std::vector<std::string> values = {"fred", "wilma", "barney", "pebbles"};
  auto enmr = Enumeration::create(ctx_, enmr_name, values);
  ArraySchemaExperimental::add_enumeration(ctx_, schema, enmr);

  auto attr = Attribute::create<int>(ctx_, "attr");
  AttributeExperimental::set_enumeration_name(ctx_, attr, enmr_name);
  schema.add_attribute(attr);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Load Schema from URI",
    "[enumeration][add-attribute]") {
  create_array();
  auto schema = ArraySchemaExperimental::load_with_enumerations(ctx_, uri_);
  auto enmr_names =
      schema.ptr().get()->array_schema_->get_loaded_enumeration_names();
  REQUIRE(enmr_names.size() > 0);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Load Schema from URI - REMOTE NOT SUPPORTED YET",
    "[enumeration][add-attribute][fixme]") {
  std::string uri = "tiledb://namespace/array_name";
  REQUIRE_THROWS(ArraySchemaExperimental::load_with_enumerations(ctx_, uri));
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Schema Dump With Enumeration",
    "[enumeration][array-schema][dump]") {
  ArraySchema schema(ctx_, TILEDB_DENSE);

  auto dim = Dimension::create<int>(ctx_, "dim", {{-100, 100}});
  auto dom = Domain(ctx_);
  dom.add_dimension(dim);
  schema.set_domain(dom);

  std::vector<std::string> values = {"fred", "wilma", "barney", "pebbles"};
  auto enmr = Enumeration::create(ctx_, enmr_name, values);
  ArraySchemaExperimental::add_enumeration(ctx_, schema, enmr);

  auto attr = Attribute::create<int>(ctx_, "attr");
  AttributeExperimental::set_enumeration_name(ctx_, attr, enmr_name);
  schema.add_attribute(attr);

  check_dump(schema);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumerations From Disk - Array::get_enumeration",
    "[enumeration][array-get-enumeration]") {
  create_array();
  auto array = Array(ctx_, uri_, TILEDB_READ);
  auto enmr = ArrayExperimental::get_enumeration(ctx_, array, "an_enumeration");
  REQUIRE(enmr.ptr() != nullptr);
  REQUIRE(enmr.name() == "an_enumeration");
  REQUIRE(enmr.type() == TILEDB_STRING_ASCII);
  REQUIRE(enmr.cell_val_num() == TILEDB_VAR_NUM);
  REQUIRE(enmr.ordered() == false);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumerations From Disk - Attribute::get_enumeration_name",
    "[enumeration][attr-get-enumeration-name]") {
  create_array();
  auto schema = Array::load_schema(ctx_, uri_);

  auto attr1 = schema.attribute("attr1");
  auto enmr_name1 = AttributeExperimental::get_enumeration_name(ctx_, attr1);
  REQUIRE(enmr_name1.has_value() == true);

  auto attr2 = schema.attribute("attr2");
  auto enmr_name2 = AttributeExperimental::get_enumeration_name(ctx_, attr2);
  REQUIRE(enmr_name2.has_value() == false);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Array::load_all_enumerations",
    "[enumeration][array-load-all-enumerations]") {
  create_array();
  auto array = Array(ctx_, uri_, TILEDB_READ);
  REQUIRE_NOTHROW(ArrayExperimental::load_all_enumerations(ctx_, array));
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "C API: Array load_all_enumerations - Check nullptr",
    "[enumeration][array-load-all-enumerations]") {
  auto rc = tiledb_array_load_all_enumerations(ctx_.ptr().get(), nullptr, 0);
  REQUIRE(rc != TILEDB_OK);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: ArraySchemaEvolution - Add Enumeration",
    "[enumeration][array-schema-evolution][add-enumeration]") {
  ArraySchemaEvolution ase(ctx_);
  std::vector<std::string> values = {"fred", "wilma", "barney", "pebbles"};
  auto enmr = Enumeration::create(ctx_, enmr_name, values);
  CHECK_NOTHROW(ase.add_enumeration(enmr));
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "C API: ArraySchemaEvolution - Add Enumeration - Check nullptr",
    "[enumeration][array-schema-evolution][error]") {
  auto rc = tiledb_array_schema_evolution_add_enumeration(
      ctx_.ptr().get(), nullptr, nullptr);
  REQUIRE(rc != TILEDB_OK);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: ArraySchemaEvolution - Drop Enumeration",
    "[enumeration][array-schema-evolution][drop-enumeration]") {
  ArraySchemaEvolution ase(ctx_);
  CHECK_NOTHROW(ase.drop_enumeration("an_enumeration_name"));
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "C API: ArraySchemaEvolution - Drop Enumeration - Check nullptr",
    "[enumeration][array-schema-evolution][drop-enumeration]") {
  auto rc = tiledb_array_schema_evolution_drop_enumeration(
      ctx_.ptr().get(), nullptr, "foo");
  REQUIRE(rc != TILEDB_OK);

  ArraySchemaEvolution ase(ctx_);
  rc = tiledb_array_schema_evolution_drop_enumeration(
      ctx_.ptr().get(), ase.ptr().get(), nullptr);
  REQUIRE(rc != TILEDB_OK);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration Query - Basic",
    "[enumeration][query][basic]") {
  // Basic smoke test. Check that a simple query condition applied against
  // an array returns sane results.
  create_array();

  // Check attr1 == "fred" when attr1 is an enumeration.
  QueryCondition qc(ctx_);
  qc.init("attr1", "fred", 4, TILEDB_EQ);

  // Execute the query condition against the array
  std::vector<int> dim(5);
  std::vector<int> attr1(5);

  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  query.add_range("dim", 1, 5)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr1", attr1)
      .set_condition(qc);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  query.finalize();

  // attr1 == "fred" in position 0 and position 4.
  std::vector<int> dim_expect = {1, 2, 3, 4, 5};
  std::vector<int> attr1_expect = {0, INT32_MIN, INT32_MIN, INT32_MIN, 0};

  REQUIRE(dim == dim_expect);
  REQUIRE(attr1 == attr1_expect);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration Query - Negation",
    "[enumeration][query][negation]") {
  // Another basic query test, the only twist here is that we're checking
  // that query condition negation works as expected.
  create_array();

  // Create a query condition attr1 == "fred" and then negate it so that
  // we can verify rewriting a negated query works.
  QueryCondition qc1(ctx_);
  qc1.init("attr1", "fred", 4, TILEDB_EQ);
  auto qc2 = qc1.negate();

  // Execute a read query against the created array
  std::vector<int> dim(5);
  std::vector<int> attr1(5);

  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  query.add_range("dim", 1, 5)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr1", attr1)
      .set_condition(qc2);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  query.finalize();

  // attr1 == "fred" in positions 0 and 4 so those values should not match
  // and return the default fill values.
  std::vector<int> dim_expect = {1, 2, 3, 4, 5};
  std::vector<int> attr1_expect = {INT32_MIN, 1, 2, 1, INT32_MIN};

  REQUIRE(dim == dim_expect);
  REQUIRE(attr1 == attr1_expect);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration Query - Combination",
    "[enumeration][query][combination]") {
  // Same test as before except using multi-condition query condtions
  create_array();

  // Create query condition: attr1 == "fred" OR attr2 = 3.0f
  QueryCondition qc1(ctx_);
  qc1.init("attr1", "fred", 4, TILEDB_EQ);

  QueryCondition qc2(ctx_);
  float val = 3.0f;
  qc2.init("attr2", &val, sizeof(float), TILEDB_EQ);

  QueryCondition qc3 = qc1.combine(qc2, TILEDB_OR);

  // Execute a query with the query condition
  std::vector<int> dim(5);
  std::vector<int> attr1(5);
  std::vector<float> attr2(5);

  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  query.add_range("dim", 1, 5)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr1", attr1)
      .set_data_buffer("attr2", attr2)
      .set_condition(qc3);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  query.finalize();

  // Check the results match the expected results. attr1 == "fred" in
  // positions 0 and 4, while attr2 == 3.0f in position 2.
  std::vector<int> dim_expect = {1, 2, 3, 4, 5};
  std::vector<int> attr1_expect = {0, INT32_MIN, 2, INT32_MIN, 0};
  std::vector<float> attr2_expect = {
      1.0f, std::nanf(""), 3.0f, std::nanf(""), 5.0f};

  REQUIRE(dim == dim_expect);
  REQUIRE(attr1 == attr1_expect);

  // NaN != NaN so we have to loop over the attr2 results to special case
  // `isnan(v1) == isnan(v2)` when NaN is encountered the expect vector.
  for (size_t i = 0; i < attr2_expect.size(); i++) {
    if (std::isnan(attr2_expect[i])) {
      REQUIRE(std::isnan(attr2[i]));
    } else {
      REQUIRE(attr2[i] == attr2_expect[i]);
    }
  }
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration Query - Set Use Enumeration",
    "[enumeration][query][set-use-enumeration]") {
  QueryCondition qc(ctx_);
  qc.init("attr1", "fred", 4, TILEDB_EQ);
  REQUIRE_NOTHROW(
      QueryConditionExperimental::set_use_enumeration(ctx_, qc, true));
  REQUIRE_NOTHROW(
      QueryConditionExperimental::set_use_enumeration(ctx_, qc, false));
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "C API: Enumeration Query - Check nullptr",
    "[enumeration][query][check-nullptr]") {
  auto rc =
      tiledb_query_condition_set_use_enumeration(ctx_.ptr().get(), nullptr, 0);
  REQUIRE(rc != TILEDB_OK);
}

CPPEnumerationFx::CPPEnumerationFx()
    : uri_("enumeration_test_array")
    , vfs_(ctx_) {
  rm_array();
}

CPPEnumerationFx::~CPPEnumerationFx() {
  rm_array();
}

template <typename T>
void CPPEnumerationFx::check_dump(const T& val) {
  FILE* handle = fopen(dump_name.c_str(), "w");
  REQUIRE(handle != nullptr);
  val.dump(handle);
  fclose(handle);

  std::stringstream ss;

  // Scoped in an anonymous block to ensure that rstream closes before
  // we attempt to delete the file for cleanup.
  {
    std::ifstream rstream(dump_name);
    if (rstream.is_open()) {
      ss << rstream.rdbuf();
    }
  }

  auto data = ss.str();
  auto iter = data.find("Enumeration");
  REQUIRE(iter != std::string::npos);

  vfs_.remove_file(dump_name);
}

void CPPEnumerationFx::create_array() {
  // Create a simple array for testing. This ends up with just five elements in
  // the array. dim is an int32_t dimension, attr1 is an enumeration with string
  // values and int32_t attribute values. attr2 is a float attribute.
  //
  // The array data is summarized as below, however, pay attention to the fact
  // that attr1 is storing integral index values instead of the raw string data.
  //
  // dim = {1, 2, 3, 4, 5}
  // attr1 = {"fred", "wilma", "barney", "wilma", "fred"}
  // attr2 = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}
  ArraySchema schema(ctx_, TILEDB_DENSE);

  auto dim = Dimension::create<int>(ctx_, "dim", {{-100, 100}});
  auto dom = Domain(ctx_);
  dom.add_dimension(dim);
  schema.set_domain(dom);

  // The list of string values in the attr1 enumeration
  std::vector<std::string> values = {"fred", "wilma", "barney", "pebbles"};
  auto enmr = Enumeration::create(ctx_, "an_enumeration", values);
  ArraySchemaExperimental::add_enumeration(ctx_, schema, enmr);

  auto attr1 = Attribute::create<int>(ctx_, "attr1");
  AttributeExperimental::set_enumeration_name(ctx_, attr1, "an_enumeration");
  schema.add_attribute(attr1);

  auto attr2 = Attribute::create<float>(ctx_, "attr2");
  schema.add_attribute(attr2);

  Array::create(uri_, schema);

  // Attribute data
  std::vector<int> attr1_values = {0, 1, 2, 1, 0};
  std::vector<float> attr2_values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};

  Array array(ctx_, uri_, TILEDB_WRITE);
  Subarray subarray(ctx_, array);
  subarray.set_subarray({1, 5});
  Query query(ctx_, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("attr1", attr1_values)
      .set_data_buffer("attr2", attr2_values);
  CHECK_NOTHROW(query.submit());
  query.finalize();
  array.close();
}

void CPPEnumerationFx::rm_array() {
  if (vfs_.is_dir(uri_)) {
    vfs_.remove_dir(uri_);
  }
}
