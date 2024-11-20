/**
 * @file unit-rest-enumerations.cc
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
 * Tests end to end enumerations.
 */

#include "test/support/src/array_schema_helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;

struct RESTEnumerationFx {
  RESTEnumerationFx();

  void create_array(const std::string& array_name);

  tiledb::test::VFSTestSetup vfs_test_setup_;
  shared_ptr<sm::MemoryTracker> memory_tracker_;
  std::string uri_;
  Context ctx_;
};

TEST_CASE_METHOD(
    RESTEnumerationFx,
    "Create array test",
    "[rest][enumeration][create-array]") {
  uri_ = vfs_test_setup_.array_uri("simple-array-create");
  create_array(uri_);
}

TEST_CASE_METHOD(
    RESTEnumerationFx,
    "Simple enumeration query",
    "[rest][enumeration][simple-query]") {
  uri_ = vfs_test_setup_.array_uri("simple-query");
  create_array(uri_);

  Array array(ctx_, uri_, TILEDB_READ);
  Subarray subarray(ctx_, array);
  subarray.set_subarray({1, 5});

  QueryCondition qc(ctx_);
  qc.init("attr1", "wilma", 5, TILEDB_EQ);

  std::vector<int> attr1_read(5);
  std::vector<float> attr2_read(5);

  Query query(ctx_, array);
  query.set_subarray(subarray)
      .set_condition(qc)
      .set_data_buffer("attr1", attr1_read)
      .set_data_buffer("attr2", attr2_read);

  REQUIRE(query.submit() == Query::Status::COMPLETE);
  REQUIRE(attr1_read[1] == 1);
  REQUIRE(attr1_read[3] == 1);
  array.close();
}

TEST_CASE_METHOD(
    RESTEnumerationFx,
    "Get enumeration",
    "[rest][enumeration][get-enumeration]") {
  uri_ = vfs_test_setup_.array_uri("get-enumeration");
  create_array(uri_);

  Array array(ctx_, uri_, TILEDB_READ);
  auto enmr = ArrayExperimental::get_enumeration(ctx_, array, "my_enum");

  std::vector<std::string> expected = {"fred", "wilma", "barney", "pebbles"};
  REQUIRE(enmr.as_vector<std::string>() == expected);
  array.close();
}

TEST_CASE_METHOD(
    RESTEnumerationFx,
    "Get previously loaded enumeration",
    "[rest][enumeration][get-enumeration]") {
  uri_ = vfs_test_setup_.array_uri("get-enumeration");
  create_array(uri_);

  Array array(ctx_, uri_, TILEDB_READ);
  auto enmr1 = ArrayExperimental::get_enumeration(ctx_, array, "my_enum");
  auto enmr2 = ArrayExperimental::get_enumeration(ctx_, array, "my_enum");

  REQUIRE(enmr1.ptr()->copy() == enmr2.ptr()->copy());

  std::vector<std::string> expected = {"fred", "wilma", "barney", "pebbles"};
  REQUIRE(enmr2.as_vector<std::string>() == expected);
  array.close();
}

TEST_CASE_METHOD(
    RESTEnumerationFx,
    "Enumeration Extension",
    "[rest][enumeration][extension]") {
  uri_ = vfs_test_setup_.array_uri("extension");
  create_array(uri_);

  Array old_array(ctx_, uri_, TILEDB_READ);
  auto old_enmr = ArrayExperimental::get_enumeration(ctx_, old_array, "fruit");
  old_array.close();

  std::vector<std::string> fruit = {
      "apple", "blueberry", "cherry", "durian", "elderberry"};
  auto new_enmr = old_enmr.extend(fruit);

  ArraySchemaEvolution ase(ctx_);
  ase.extend_enumeration(new_enmr);
  ase.array_evolve(uri_);

  Array new_array(ctx_, uri_, TILEDB_READ);
  auto enmr = ArrayExperimental::get_enumeration(ctx_, new_array, "fruit");
  REQUIRE(enmr.as_vector<std::string>() == fruit);
  new_array.close();
}

TEST_CASE_METHOD(
    RESTEnumerationFx,
    "Load Enumerations - All Schemas",
    "[enumeration][array][load-all-enumerations][all-schemas][rest]") {
  uri_ = vfs_test_setup_.array_uri("load_enmrs_all_schemas");
  auto config = vfs_test_setup_.ctx().config();
  bool load_enmrs = GENERATE(true, false);
  config["rest.load_enumerations_on_array_open"] =
      load_enmrs ? "true" : "false";
  vfs_test_setup_.update_config(config.ptr().get());
  ctx_ = vfs_test_setup_.ctx();

  create_array(uri_);
  Array opened_array(ctx_, uri_, TILEDB_READ);
  if (!vfs_test_setup_.is_rest() && load_enmrs) {
    if (opened_array.ptr()->array()->use_refactored_array_open()) {
      CHECK_NOTHROW(
          ArrayExperimental::load_enumerations_all_schemas(ctx_, opened_array));
    } else {
      CHECK_NOTHROW(
          ArrayExperimental::load_all_enumerations(ctx_, opened_array));
    }
  }
  auto array = opened_array.ptr()->array();
  auto schema = array->array_schema_latest_ptr();
  REQUIRE(schema->is_enumeration_loaded("my_enum") == load_enmrs);
  REQUIRE(schema->is_enumeration_loaded("fruit") == load_enmrs);

  // If not using array open v3 just test that the correct exception is thrown
  if (!array->use_refactored_array_open()) {
    CHECK_THROWS_WITH(
        array->load_all_enumerations(true),
        Catch::Matchers::ContainsSubstring(
            "The array must be opened using "
            "`rest.use_refactored_array_open=true`"));
    return;
  }
  // If enumerations were loaded on array open this will not submit an
  // additional request.
  auto actual_enmrs = array->get_enumerations_all_schemas();
  if (!load_enmrs) {
    REQUIRE(schema->is_enumeration_loaded("my_enum") == true);
    REQUIRE(schema->is_enumeration_loaded("fruit") == true);
  }

  // Fetch enumerations created in the initial array schema for validation.
  auto enmr1 = array->get_enumeration("my_enum");
  auto enmr2 = array->get_enumeration("fruit");
  decltype(actual_enmrs) expected_enmrs{{schema->name(), {enmr1, enmr2}}};
  auto validate_enmrs = [&]() {
    for (const auto& [schema_name, enmrs] : expected_enmrs) {
      REQUIRE(actual_enmrs.contains(schema_name));
      REQUIRE(enmrs.size() == actual_enmrs[schema_name].size());
      CHECK_THAT(
          enmrs,
          Catch::Matchers::UnorderedRangeEquals(
              actual_enmrs[schema_name], [](const auto& a, const auto& b) {
                return tiledb::test::is_equivalent_enumeration(*a, *b);
              }));
    }
  };
  validate_enmrs();

  // Evolve once to add an enumeration.
  sm::URI uri(uri_);
  auto ase = make_shared<sm::ArraySchemaEvolution>(HERE(), memory_tracker_);
  std::vector<std::string> var_values{"one", "two", "three"};
  auto var_enmr = Enumeration::create(ctx_, "ase_var_enmr", var_values);
  ase->add_enumeration(var_enmr.ptr()->enumeration());
  auto attr4 =
      make_shared<sm::Attribute>(HERE(), "attr4", sm::Datatype::UINT16);
  attr4->set_enumeration_name("ase_var_enmr");
  CHECK_NOTHROW(ase->evolve_schema(schema));
  // Apply evolution to the array and reopen.
  CHECK_NOTHROW(sm::Array::evolve_array_schema(
      ctx_.ptr()->resources(), uri, ase.get(), array->get_encryption_key()));
  CHECK(array->reopen().ok());
  if (load_enmrs && !vfs_test_setup_.is_rest()) {
    array->load_all_enumerations(array->use_refactored_array_open());
  }
  schema = array->array_schema_latest_ptr();
  std::string schema_name_2 = schema->name();
  REQUIRE(schema->is_enumeration_loaded("my_enum") == load_enmrs);
  REQUIRE(schema->is_enumeration_loaded("fruit") == load_enmrs);
  REQUIRE(schema->is_enumeration_loaded("ase_var_enmr") == load_enmrs);

  expected_enmrs[schema_name_2] = {enmr1, enmr2, var_enmr.ptr()->enumeration()};
  actual_enmrs = array->get_enumerations_all_schemas();
  if (!load_enmrs) {
    if (!vfs_test_setup_.is_rest()) {
      array->load_all_enumerations(array->use_refactored_array_open());
    }
    REQUIRE(schema->is_enumeration_loaded("my_enum") == true);
    REQUIRE(schema->is_enumeration_loaded("fruit") == true);
    REQUIRE(schema->is_enumeration_loaded("ase_var_enmr") == true);
  }
  validate_enmrs();

  // Evolve a second time to drop an enumeration.
  ase = make_shared<sm::ArraySchemaEvolution>(HERE(), memory_tracker_);
  ase->drop_enumeration("my_enum");
  ase->drop_attribute("attr1");
  CHECK_NOTHROW(ase->evolve_schema(schema));
  // Apply evolution to the array and reopen.
  CHECK_NOTHROW(sm::Array::evolve_array_schema(
      ctx_.ptr()->resources(), uri, ase.get(), array->get_encryption_key()));
  CHECK(array->reopen().ok());
  if (load_enmrs && !vfs_test_setup_.is_rest()) {
    array->load_all_enumerations(array->use_refactored_array_open());
  }
  schema = array->array_schema_latest_ptr();
  std::string schema_name_3 = schema->name();
  REQUIRE_THROWS_WITH(
      schema->is_enumeration_loaded("my_enum"),
      Catch::Matchers::ContainsSubstring("unknown enumeration"));
  REQUIRE(schema->is_enumeration_loaded("fruit") == load_enmrs);
  REQUIRE(schema->is_enumeration_loaded("ase_var_enmr") == load_enmrs);

  expected_enmrs[schema_name_3] = {enmr2, var_enmr.ptr()->enumeration()};
  actual_enmrs = array->get_enumerations_all_schemas();
  if (!load_enmrs) {
    if (!vfs_test_setup_.is_rest()) {
      array->load_all_enumerations(array->use_refactored_array_open());
    }
    REQUIRE_THROWS_WITH(
        schema->is_enumeration_loaded("my_enum"),
        Catch::Matchers::ContainsSubstring("unknown enumeration"));
    REQUIRE(schema->is_enumeration_loaded("fruit") == true);
    REQUIRE(schema->is_enumeration_loaded("ase_var_enmr") == true);
  }

  validate_enmrs();
}

TEST_CASE_METHOD(
    RESTEnumerationFx,
    "Load Enumerations - All Schemas partial load",
    "[enumeration][array][load-all-enumerations][all-schemas][rest]") {
  uri_ = vfs_test_setup_.array_uri("load_enmrs_all_schemas");

  create_array(uri_);
  Array opened_array(ctx_, uri_, TILEDB_READ);
  auto array = opened_array.ptr()->array();
  auto schema = array->array_schema_latest_ptr();
  REQUIRE(schema->is_enumeration_loaded("my_enum") == false);
  REQUIRE(schema->is_enumeration_loaded("fruit") == false);
  // Fetch one enumeration, intentionally leaving the other unloaded.
  auto enmr1 = array->get_enumeration("my_enum");
  REQUIRE(schema->is_enumeration_loaded("my_enum") == true);

  // If not using array open v3 just test that the correct exception is thrown
  if (!array->use_refactored_array_open()) {
    CHECK_THROWS_WITH(
        array->load_all_enumerations(true),
        Catch::Matchers::ContainsSubstring(
            "The array must be opened using "
            "`rest.use_refactored_array_open=true`"));
    return;
  }

  // Load all enumerations.
  auto actual_enmrs = array->get_enumerations_all_schemas();
  REQUIRE(schema->is_enumeration_loaded("fruit") == true);
  auto enmr2 = array->get_enumeration("fruit");

  decltype(actual_enmrs) expected_enmrs{{schema->name(), {enmr1, enmr2}}};
  auto validate_enmrs = [&]() {
    for (const auto& [schema_name, enmrs] : expected_enmrs) {
      REQUIRE(actual_enmrs.contains(schema_name));
      REQUIRE(enmrs.size() == actual_enmrs[schema_name].size());
      CHECK_THAT(
          enmrs,
          Catch::Matchers::UnorderedRangeEquals(
              actual_enmrs[schema_name], [](const auto& a, const auto& b) {
                return tiledb::test::is_equivalent_enumeration(*a, *b);
              }));
    }
  };
  validate_enmrs();

  // Evolve once to add an enumeration.
  sm::URI uri(uri_);
  auto ase = make_shared<sm::ArraySchemaEvolution>(HERE(), memory_tracker_);
  std::vector<std::string> var_values{"one", "two", "three"};
  auto var_enmr = Enumeration::create(ctx_, "ase_var_enmr", var_values);
  ase->add_enumeration(var_enmr.ptr()->enumeration());
  auto attr4 =
      make_shared<sm::Attribute>(HERE(), "attr4", sm::Datatype::UINT16);
  attr4->set_enumeration_name("ase_var_enmr");
  CHECK_NOTHROW(ase->evolve_schema(schema));
  // Apply evolution to the array and reopen.
  CHECK_NOTHROW(sm::Array::evolve_array_schema(
      ctx_.ptr()->resources(), uri, ase.get(), array->get_encryption_key()));
  CHECK(array->reopen().ok());
  schema = array->array_schema_latest_ptr();
  std::string schema_name_2 = schema->name();
  REQUIRE(schema->is_enumeration_loaded("my_enum") == false);
  REQUIRE(schema->is_enumeration_loaded("fruit") == false);
  REQUIRE(schema->is_enumeration_loaded("ase_var_enmr") == false);

  SECTION("Partial load a single evolved enumeration") {
    // Load all enumerations except the enumeration we added with evolution
    // above.
    array->get_enumeration("my_enum");
    REQUIRE(schema->is_enumeration_loaded("my_enum") == true);
    array->get_enumeration("fruit");
    REQUIRE(schema->is_enumeration_loaded("fruit") == true);
    // Load the remaining `ase_var_enmr` enumeration.
    actual_enmrs = array->get_enumerations_all_schemas();
    expected_enmrs[schema_name_2] = {
        enmr1, enmr2, var_enmr.ptr()->enumeration()};
    validate_enmrs();
  }

  SECTION("Partial load multiple enumerations") {
    // Load all enumerations except the enumeration we added with evolution
    // above.
    array->get_enumeration("fruit");
    REQUIRE(schema->is_enumeration_loaded("fruit") == true);
    // Load the remaining `my_enum` and `ase_var_enmr` enumerations.
  }

  actual_enmrs = array->get_enumerations_all_schemas();
  expected_enmrs[schema_name_2] = {enmr1, enmr2, var_enmr.ptr()->enumeration()};
  validate_enmrs();

  SECTION("Drop all enumerations and validate earlier schemas") {
    ase = make_shared<sm::ArraySchemaEvolution>(HERE(), memory_tracker_);
    ase->drop_enumeration("my_enum");
    ase->drop_attribute("attr1");
    ase->drop_enumeration("fruit");
    ase->drop_attribute("attr3");
    ase->drop_enumeration("ase_var_enmr");
    CHECK_NOTHROW(ase->evolve_schema(schema));
    CHECK_NOTHROW(sm::Array::evolve_array_schema(
        ctx_.ptr()->resources(), uri, ase.get(), array->get_encryption_key()));
    CHECK(array->reopen().ok());
    schema = array->array_schema_latest_ptr();
    std::string schema_name_3 = schema->name();
    actual_enmrs = array->get_enumerations_all_schemas();
    expected_enmrs[schema_name_3] = {};
    validate_enmrs();
  }
}

RESTEnumerationFx::RESTEnumerationFx()
    : memory_tracker_(tiledb::test::create_test_memory_tracker())
    , ctx_(vfs_test_setup_.ctx()){};

void RESTEnumerationFx::create_array(const std::string& array_name) {
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
  auto enmr = Enumeration::create(ctx_, "my_enum", values);
  ArraySchemaExperimental::add_enumeration(ctx_, schema, enmr);

  auto attr1 = Attribute::create<int>(ctx_, "attr1");
  AttributeExperimental::set_enumeration_name(ctx_, attr1, "my_enum");
  schema.add_attribute(attr1);

  auto attr2 = Attribute::create<float>(ctx_, "attr2");
  schema.add_attribute(attr2);

  auto fruit = Enumeration::create_empty(
      ctx_, "fruit", TILEDB_STRING_ASCII, TILEDB_VAR_NUM);
  ArraySchemaExperimental::add_enumeration(ctx_, schema, fruit);

  auto attr3 = Attribute::create<int>(ctx_, "attr3");
  AttributeExperimental::set_enumeration_name(ctx_, attr3, "fruit");
  schema.add_attribute(attr3);

  Array::create(array_name, schema);

  // Attribute data
  std::vector<int> attr1_values = {0, 1, 2, 1, 0};
  std::vector<float> attr2_values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  std::vector<int> attr3_values = {0, 1, 2, 3, 4};

  Array array(ctx_, array_name, TILEDB_WRITE);
  Subarray subarray(ctx_, array);
  subarray.set_subarray({1, 5});
  Query query(ctx_, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("attr1", attr1_values)
      .set_data_buffer("attr2", attr2_values)
      .set_data_buffer("attr3", attr3_values);
  CHECK_NOTHROW(query.submit());
  query.finalize();
  array.close();
}
