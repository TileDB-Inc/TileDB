/**
 * @file unit-cppapi-enumeration.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB Inc.
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
#include "test/support/src/array_schema_helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_internal.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;

struct CPPEnumerationFx {
  CPPEnumerationFx();
  ~CPPEnumerationFx() = default;

  template <typename T>
  void check_dump(const T& val);

  void create_array(bool with_empty_enumeration = false);

  tiledb::test::VFSTestSetup vfs_test_setup_;
  std::string uri_;
  Context ctx_;
  VFS vfs_;
};

const static std::string enmr_name = "an_enumeration";
const static std::string dump_name = "enumeration_dump_test.txt";

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Create Boolean",
    "[enumeration][create][bool]") {
  std::vector<bool> values = {true, false};
  auto enmr = Enumeration::create(ctx_, enmr_name, values);
  REQUIRE(enmr.ptr() != nullptr);
  REQUIRE(enmr.name() == enmr_name);
  REQUIRE(enmr.type() == TILEDB_BOOL);
  REQUIRE(enmr.cell_val_num() == 1);
  REQUIRE(enmr.ordered() == false);

  auto data = enmr.as_vector<bool>();
  REQUIRE(data == values);
}

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
    "CPP: Enumeration API - Extend Fixed Size",
    "[enumeration][extend][fixed-size]") {
  std::vector<int> init_values = {1, 2, 3, 4, 5};
  std::vector<int> add_values = {6, 7, 8, 9, 10};
  std::vector<int> final_values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  auto enmr1 = Enumeration::create(ctx_, enmr_name, init_values, true);
  auto enmr2 = enmr1.extend(add_values);

  REQUIRE(enmr2.ptr()->is_extension_of(enmr1.ptr().get()));

  auto data = enmr2.as_vector<int>();
  REQUIRE(data == final_values);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Extend Var Size",
    "[enumeration][extend][var-size]") {
  std::vector<std::string> init_values = {"fred", "wilma"};
  std::vector<std::string> add_values = {"barney", "betty"};
  std::vector<std::string> final_values = {"fred", "wilma", "barney", "betty"};
  auto enmr1 = Enumeration::create(ctx_, enmr_name, init_values, true);
  auto enmr2 = enmr1.extend(add_values);

  REQUIRE(enmr2.ptr()->is_extension_of(enmr1.ptr().get()));

  auto data = enmr2.as_vector<std::string>();
  REQUIRE(data == final_values);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Fixed Size Empty Vector extension",
    "[enumeration][extend][error]") {
  std::vector<int> init_values = {1, 2, 3, 4, 5};
  std::vector<int> add_values = {};
  auto enmr = Enumeration::create(ctx_, enmr_name, init_values, true);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "Unable to extend an enumeration with an empty vector.");
  REQUIRE_THROWS_WITH(enmr.extend(add_values), matcher);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Var Size Empty Vector extension",
    "[enumeration][extend][error]") {
  std::vector<std::string> init_values = {"fred", "wilma"};
  std::vector<std::string> add_values = {};
  auto enmr = Enumeration::create(ctx_, enmr_name, init_values, true);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "Unable to extend an enumeration with an empty vector.");
  REQUIRE_THROWS_WITH(enmr.extend(add_values), matcher);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Invalid cell val num extension",
    "[enumeration][extend][error]") {
  std::vector<int> init_values = {1, 2, 3, 4};
  std::vector<int> add_values = {5};
  auto enmr = Enumeration::create(
      ctx_,
      enmr_name,
      TILEDB_INT32,
      2,
      true,
      init_values.data(),
      init_values.size() * sizeof(int),
      nullptr,
      0);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "Invalid data size is not a multiple of the cell size.");
  REQUIRE_THROWS_WITH(enmr.extend(add_values), matcher);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Fixed Size Wrong Type extension",
    "[enumeration][extend][error]") {
  std::vector<int> init_values = {1, 2, 3, 4, 5};
  std::vector<std::string> add_values = {"barney", "betty"};
  auto enmr = Enumeration::create(ctx_, enmr_name, init_values, true);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "Error extending fixed sized enumeration with var size data.");
  REQUIRE_THROWS_WITH(enmr.extend(add_values), matcher);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration API - Var Size Wrong Type extension",
    "[enumeration][extend][error]") {
  std::vector<std::string> init_values = {"fred", "wilma"};
  std::vector<int> add_values = {6, 7, 8, 9, 10};
  auto enmr = Enumeration::create(ctx_, enmr_name, init_values, true);

  auto matcher = Catch::Matchers::ContainsSubstring(
      "Error extending var sized enumeration with fixed size data.");
  REQUIRE_THROWS_WITH(enmr.extend(add_values), matcher);
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
    "[enumeration][array-get-enumeration][rest]") {
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
    "[enumeration][attr-get-enumeration-name][rest]") {
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
    "CPP: Enumerations From Disk - ArraySchema::get_enumeration_from_name",
    "[enumeration][array-schema-get-enumeration-from-name][rest]") {
  create_array();

  std::optional<Enumeration> expect_enumeration;
  {
    auto array = tiledb::Array(ctx_, uri_, TILEDB_READ);
    expect_enumeration =
        ArrayExperimental::get_enumeration(ctx_, array, enmr_name);
  }

  SECTION("default schema load retrieves enumeration on request only") {
    auto schema = Array::load_schema(ctx_, uri_);

    CHECK(!schema.ptr()->array_schema()->is_enumeration_loaded(enmr_name));

    auto actual_enumeration =
        ArraySchemaExperimental::get_enumeration_from_name(
            ctx_, schema, enmr_name);
    CHECK(schema.ptr()->array_schema()->is_enumeration_loaded(enmr_name));
    CHECK(test::is_equivalent_enumeration(
        *expect_enumeration, actual_enumeration));
  }

  SECTION("schema load with rest config retrieves enumeration eagerly") {
    Config config;
    config["rest.load_enumerations_on_array_open"] = "true";

    auto schema = Array::load_schema_with_config(ctx_, config, uri_);
    CHECK(schema.ptr()->array_schema()->is_enumeration_loaded(enmr_name));

    // requesting it should do no I/O (we did it already),
    // unclear how to check that
    auto actual_enumeration =
        ArraySchemaExperimental::get_enumeration_from_name(
            ctx_, schema, enmr_name);
    CHECK(schema.ptr()->array_schema()->is_enumeration_loaded(enmr_name));
    CHECK(test::is_equivalent_enumeration(
        *expect_enumeration, actual_enumeration));
  }
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumerations From Disk - "
    "ArraySchema::get_enumeration_from_attribute_name",
    "[enumeration][array-schema-get-enumeration-from-attribute-name][rest]") {
  create_array();

  const std::string attr_name = "attr1";

  std::optional<Enumeration> expect_enumeration;
  {
    auto array = tiledb::Array(ctx_, uri_, TILEDB_READ);
    expect_enumeration =
        ArrayExperimental::get_enumeration(ctx_, array, enmr_name);
  }

  SECTION("default schema load retrieves enumeration on request only") {
    auto schema = Array::load_schema(ctx_, uri_);

    CHECK(!schema.ptr()->array_schema()->is_enumeration_loaded(enmr_name));

    auto actual_enumeration =
        ArraySchemaExperimental::get_enumeration_from_attribute_name(
            ctx_, schema, attr_name);
    CHECK(schema.ptr()->array_schema()->is_enumeration_loaded(enmr_name));
    CHECK(test::is_equivalent_enumeration(
        *expect_enumeration, actual_enumeration));
  }

  SECTION("schema load with rest config retrieves enumeration eagerly") {
    Config config;
    config["rest.load_enumerations_on_array_open"] = "true";

    auto schema = Array::load_schema_with_config(ctx_, config, uri_);
    CHECK(schema.ptr()->array_schema()->is_enumeration_loaded(enmr_name));

    // requesting it should do no I/O (we did it already),
    // unclear how to check that
    auto actual_enumeration =
        ArraySchemaExperimental::get_enumeration_from_attribute_name(
            ctx_, schema, attr_name);
    CHECK(schema.ptr()->array_schema()->is_enumeration_loaded(enmr_name));
    CHECK(test::is_equivalent_enumeration(
        *expect_enumeration, actual_enumeration));
  }
}
TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Array::load_all_enumerations",
    "[enumeration][array-load-all-enumerations][rest]") {
  create_array();
  auto array = Array(ctx_, uri_, TILEDB_READ);
  REQUIRE_NOTHROW(ArrayExperimental::load_all_enumerations(ctx_, array));
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "C API: Array load_all_enumerations - Check nullptr",
    "[enumeration][array-load-all-enumerations][rest]") {
  auto rc = tiledb_array_load_all_enumerations(ctx_.ptr().get(), nullptr);
  REQUIRE(rc != TILEDB_OK);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP API: Load All Enumerations - All Schemas",
    "[enumeration][array][load-all-enumerations][all-schemas][rest]") {
  create_array();
  // Test with `rest.load_enumerations_on_array_open` enabled and disabled.
  bool load_enmrs = GENERATE(true, false);
  auto config = ctx_.config();
  config["rest.load_enumerations_on_array_open"] =
      load_enmrs ? "true" : "false";
  vfs_test_setup_.update_config(config.ptr().get());
  ctx_ = vfs_test_setup_.ctx();

  auto array = tiledb::Array(ctx_, uri_, TILEDB_READ);
  // Loading the array with array open v1 will only initialize the latest schema
  // Adjust future validations to check latest schema only if this is the case.
  bool array_open_v2 = array.ptr()->array()->use_refactored_array_open();

  // Helper function to reopen and conditionally call load_all_enumerations.
  auto reopen_and_load_enmrs = [&]() {
    // We always reopen because we are evolving the schema which requires
    // reopening the array after applying the schema evolution.
    CHECK_NOTHROW(array.reopen());

    if (array_open_v2) {
      // If we are not loading enmrs on array open we must load them explicitly
      // with a separate request.
      if (!load_enmrs) {
        // Load enumerations for all schemas if using array open v2.
        CHECK_NOTHROW(
            ArrayExperimental::load_enumerations_all_schemas(ctx_, array));
      }
    } else {
      // If using array open v1 test that the correct exception is thrown
      // when attempting to load enmrs for all schemas.
      CHECK_THROWS_WITH(
          ArrayExperimental::load_enumerations_all_schemas(ctx_, array),
          Catch::Matchers::ContainsSubstring(
              "The array must be opened using "
              "`rest.use_refactored_array_open=true`"));
      // Load enumerations for the latest schema only if using array open v1.
      CHECK_NOTHROW(ArrayExperimental::load_all_enumerations(ctx_, array));
    }
  };
  reopen_and_load_enmrs();

  REQUIRE(
      array.schema().ptr()->array_schema()->has_enumeration("an_enumeration") ==
      true);
  REQUIRE(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "an_enumeration") == true);
  std::string schema_name_1 = array.schema().ptr()->array_schema()->name();

  // Evolve once to add an enumeration.
  ArraySchemaEvolution ase(ctx_);
  std::vector<std::string> var_values{"one", "two", "three"};
  auto var_enmr = Enumeration::create(ctx_, "ase_var_enmr", var_values);
  ase.add_enumeration(var_enmr);
  auto attr4 = Attribute::create<uint16_t>(ctx_, "attr4");
  AttributeExperimental::set_enumeration_name(ctx_, attr4, "ase_var_enmr");
  ase.add_attribute(attr4);
  // Apply evolution to the array and reopen.
  ase.array_evolve(uri_);
  reopen_and_load_enmrs();

  std::string schema_name_2 = array.schema().ptr()->array_schema()->name();
  if (array_open_v2) {
    // Check all schemas if we are using array open v2.
    auto all_schemas = array.ptr()->array()->array_schemas_all();
    CHECK(
        all_schemas[schema_name_1]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_1]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(
        all_schemas[schema_name_2]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_2]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(all_schemas[schema_name_2]->has_enumeration("ase_var_enmr") == true);
    CHECK(
        all_schemas[schema_name_2]->is_enumeration_loaded("ase_var_enmr") ==
        true);
  }
  // We can always validate the latest schema.
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("an_enumeration") ==
      true);
  CHECK(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "an_enumeration") == true);
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("ase_var_enmr") ==
      true);
  CHECK(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "ase_var_enmr") == true);

  // Evolve a second time to drop an enumeration.
  ArraySchemaEvolution ase2(ctx_);
  ase2.drop_enumeration("an_enumeration");
  ase2.drop_attribute("attr1");
  // Apply evolution to the array and reopen.
  CHECK_NOTHROW(ase2.array_evolve(uri_));
  reopen_and_load_enmrs();

  std::string schema_name_3 = array.schema().ptr()->array_schema()->name();
  if (array_open_v2) {
    // Check all schemas if we are using array open v2.
    auto all_schemas = array.ptr()->array()->array_schemas_all();
    CHECK(
        all_schemas[schema_name_1]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_1]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(
        all_schemas[schema_name_2]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_2]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(all_schemas[schema_name_2]->has_enumeration("ase_var_enmr") == true);
    CHECK(
        all_schemas[schema_name_2]->is_enumeration_loaded("ase_var_enmr") ==
        true);
    CHECK(
        all_schemas[schema_name_3]->has_enumeration("an_enumeration") == false);
    CHECK(all_schemas[schema_name_3]->has_enumeration("ase_var_enmr") == true);
    CHECK(
        all_schemas[schema_name_3]->is_enumeration_loaded("ase_var_enmr") ==
        true);
  }
  // Always validate the latest schema.
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("an_enumeration") ==
      false);
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("ase_var_enmr") ==
      true);
  CHECK(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "ase_var_enmr") == true);

  // Evolve a third time to add an enumeration with a name equal to a previously
  // dropped enumeration.
  ArraySchemaEvolution ase3(ctx_);
  auto old_enmr = Enumeration::create(ctx_, "an_enumeration", var_values);
  ase3.add_enumeration(old_enmr);
  auto attr5 = Attribute::create<uint16_t>(ctx_, "attr5");
  AttributeExperimental::set_enumeration_name(ctx_, attr5, "an_enumeration");
  ase.add_attribute(attr5);
  // Apply evolution to the array and reopen.
  CHECK_NOTHROW(ase3.array_evolve(uri_));
  reopen_and_load_enmrs();

  // Check all schemas.
  if (array_open_v2) {
    auto all_schemas = array.ptr()->array()->array_schemas_all();
    std::string schema_name_4 = array.schema().ptr()->array_schema()->name();
    CHECK(
        all_schemas[schema_name_1]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_1]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(
        all_schemas[schema_name_2]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_2]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(all_schemas[schema_name_2]->has_enumeration("ase_var_enmr") == true);
    CHECK(
        all_schemas[schema_name_2]->is_enumeration_loaded("ase_var_enmr") ==
        true);
    CHECK(
        all_schemas[schema_name_3]->has_enumeration("an_enumeration") == false);
    CHECK(all_schemas[schema_name_3]->has_enumeration("ase_var_enmr") == true);
    CHECK(
        all_schemas[schema_name_3]->is_enumeration_loaded("ase_var_enmr") ==
        true);
    CHECK(
        all_schemas[schema_name_4]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_4]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(all_schemas[schema_name_4]->has_enumeration("ase_var_enmr") == true);
    CHECK(
        all_schemas[schema_name_4]->is_enumeration_loaded("ase_var_enmr") ==
        true);
  }
  // Always validate the latest schema.
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("an_enumeration") ==
      true);
  CHECK(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "an_enumeration") == true);
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("ase_var_enmr") ==
      true);
  CHECK(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "ase_var_enmr") == true);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP API: Load All Enumerations - All schemas post evolution",
    "[enumeration][array][schema-evolution][load-all-enumerations][all-schemas]"
    "[rest]") {
  create_array();
  // Test with `rest.load_enumerations_on_array_open` enabled and disabled.
  bool load_enmrs = GENERATE(true, false);
  auto config = ctx_.config();
  config["rest.load_enumerations_on_array_open"] =
      load_enmrs ? "true" : "false";
  vfs_test_setup_.update_config(config.ptr().get());
  ctx_ = vfs_test_setup_.ctx();

  // Open the array with no explicit timestamps set.
  auto array = tiledb::Array(ctx_, uri_, TILEDB_READ);

  // REST CI will test with both array open v1 and v2.
  bool array_open_v2 = array.ptr()->array()->use_refactored_array_open();
  REQUIRE(
      array.schema().ptr()->array_schema()->has_enumeration("an_enumeration") ==
      true);
  if (load_enmrs) {
    // Array open v1 does not support loading enumerations on array open so we
    // must load them explicitly in this case.
    if (!array_open_v2) {
      ArrayExperimental::load_all_enumerations(ctx_, array);
    }
    REQUIRE(
        array.schema().ptr()->array_schema()->is_enumeration_loaded(
            "an_enumeration") == true);
  }
  // If `rest.load_enumerations_on_array_open=false` do not load enmrs
  // explicitly. Leave enumerations unloaded initially, evolve the array without
  // reopening, and then attempt to load all enumerations.

  // Evolve once to add an enumeration.
  ArraySchemaEvolution ase(ctx_);
  std::vector<std::string> var_values{"one", "two", "three"};
  auto var_enmr = Enumeration::create(ctx_, "ase_var_enmr", var_values);
  ase.add_enumeration(var_enmr);
  auto attr4 = Attribute::create<uint16_t>(ctx_, "attr4");
  AttributeExperimental::set_enumeration_name(ctx_, attr4, "ase_var_enmr");
  ase.add_attribute(attr4);
  // Apply evolution to the array and intentionally skip reopening after
  // evolution to test exceptions.
  ase.array_evolve(uri_);

  // Store the original array schema name so we can validate all_schemas later.
  std::string schema_name_1 = array.schema().ptr()->array_schema()->name();
  if (array_open_v2) {
    // If we have loaded all initial enumerations on array open we will not hit
    // the exception.
    if (load_enmrs) {
      // If we load enmrs before reopen, we will not load the evolved schema
      // that contains the enumeration added during evolution.
      CHECK_NOTHROW(
          ArrayExperimental::load_enumerations_all_schemas(ctx_, array));
      auto all_schemas = array.ptr()->array_schemas_all();
      CHECK(
          all_schemas[schema_name_1]->has_enumeration("an_enumeration") ==
          true);
      CHECK(
          all_schemas[schema_name_1]->is_enumeration_loaded("an_enumeration") ==
          true);
      // We did not reopen so we should not have loaded the evolved schema.
      CHECK(all_schemas.size() == 1);
    } else {
      // If we have not loaded all enumerations at this point we will hit an
      // exception. REST has reopened the array server-side and as a result
      // loaded the evolved schema. Since we did not reopen locally after
      // evolving, this schema doesn't exist client-side.
      auto start = std::to_string(array.open_timestamp_start());
      auto end = std::to_string(array.open_timestamp_end());
      CHECK_THROWS_WITH(
          ArrayExperimental::load_enumerations_all_schemas(ctx_, array),
          Catch::Matchers::ContainsSubstring(
              "Array opened using timestamp range (" + start + ", " + end +
              ") has no loaded schema named"));
    }

    // Reopen and load the evolved enumerations.
    array.reopen();
    std::string schema_name_2 = array.schema().ptr()->array_schema()->name();
    CHECK_NOTHROW(
        ArrayExperimental::load_enumerations_all_schemas(ctx_, array));
    // Validate all array schemas now contain the expected enumerations.
    auto all_schemas = array.ptr()->array_schemas_all();
    CHECK(
        all_schemas[schema_name_1]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_1]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(
        all_schemas[schema_name_2]->has_enumeration("an_enumeration") == true);
    CHECK(
        all_schemas[schema_name_2]->is_enumeration_loaded("an_enumeration") ==
        true);
    CHECK(all_schemas[schema_name_2]->has_enumeration("ase_var_enmr") == true);
    CHECK(
        all_schemas[schema_name_2]->is_enumeration_loaded("ase_var_enmr") ==
        true);
  } else {
    // If we load enmrs before reopen, we will not load the evolved schema that
    // contains the enumeration added during evolution.
    CHECK_NOTHROW(ArrayExperimental::load_all_enumerations(ctx_, array));
    CHECK(
        array.schema().ptr()->array_schema()->has_enumeration(
            "an_enumeration") == true);
    CHECK(
        array.schema().ptr()->array_schema()->is_enumeration_loaded(
            "an_enumeration") == true);
    CHECK(
        array.schema().ptr()->array_schema()->has_enumeration("ase_var_enmr") ==
        false);
    CHECK_THROWS_WITH(
        array.schema().ptr()->array_schema()->is_enumeration_loaded(
            "ase_var_enmr"),
        Catch::Matchers::ContainsSubstring("unknown enumeration"));

    // Reopen to apply the schema evolution and reload enumerations.
    array.reopen();
    CHECK_NOTHROW(ArrayExperimental::load_all_enumerations(ctx_, array));
  }

  // Check all original and evolved enumerations are in the latest schema.
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("an_enumeration") ==
      true);
  CHECK(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "an_enumeration") == true);
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("ase_var_enmr") ==
      true);
  CHECK(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "ase_var_enmr") == true);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP API: Load All Enumerations - All schemas reloading does not throw",
    "[enumeration][array][load-all-enumerations][all-schemas][rest]") {
  create_array();
  auto config = ctx_.config();
  // If we open the array with this option set we should not get an exception on
  // subsequent calls to load_enumerations_all_schemas.
  config["rest.load_enumerations_on_array_open"] = "true";
  vfs_test_setup_.update_config(config.ptr().get());
  ctx_ = vfs_test_setup_.ctx();

  auto array = tiledb::Array(ctx_, uri_, TILEDB_READ);
  // load_enumerations_all_schemas requires array open v2, so this test is not
  // valid if we are using array open v1.
  if (!array.ptr()->array()->use_refactored_array_open()) {
    return;
  }

  // Check that we do not get an exception for loading enumerations that were
  // previously loaded during array open.
  CHECK_NOTHROW(ArrayExperimental::load_enumerations_all_schemas(ctx_, array));

  // Validate all array schemas contain the expected enumerations.
  std::string schema_name = array.schema().ptr()->array_schema()->name();
  auto all_schemas = array.ptr()->array_schemas_all();
  CHECK(all_schemas[schema_name]->has_enumeration("an_enumeration") == true);
  CHECK(
      all_schemas[schema_name]->is_enumeration_loaded("an_enumeration") ==
      true);

  // Check enumerations are in the latest schema.
  CHECK(
      array.schema().ptr()->array_schema()->has_enumeration("an_enumeration") ==
      true);
  CHECK(
      array.schema().ptr()->array_schema()->is_enumeration_loaded(
          "an_enumeration") == true);
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
    "[enumeration][array-schema-evolution][error][rest]") {
  auto rc = tiledb_array_schema_evolution_add_enumeration(
      ctx_.ptr().get(), nullptr, nullptr);
  REQUIRE(rc != TILEDB_OK);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: ArraySchemaEvolution - Extend Enumeration",
    "[enumeration][array-schema-evolution][extend-enumeration]") {
  ArraySchemaEvolution ase(ctx_);
  std::vector<std::string> values = {"fred", "wilma", "barney", "pebbles"};
  auto enmr = Enumeration::create(ctx_, enmr_name, values);
  CHECK_NOTHROW(ase.extend_enumeration(enmr));
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "C API: ArraySchemaEvolution - Extend Enumeration - Check nullptr",
    "[enumeration][array-schema-evolution][drop-enumeration][rest]") {
  std::vector<std::string> values = {"fred", "wilma", "barney", "pebbles"};
  auto enmr = Enumeration::create(ctx_, enmr_name, values);

  auto rc = tiledb_array_schema_evolution_extend_enumeration(
      ctx_.ptr().get(), nullptr, enmr.ptr().get());
  REQUIRE(rc != TILEDB_OK);

  ArraySchemaEvolution ase(ctx_);
  rc = tiledb_array_schema_evolution_extend_enumeration(
      ctx_.ptr().get(), ase.ptr().get(), nullptr);
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
    "[enumeration][array-schema-evolution][drop-enumeration][rest]") {
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
    "[enumeration][query][basic][rest]") {
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
  Subarray subarray(ctx_, array);
  subarray.add_range("dim", 1, 5);
  query.set_subarray(subarray)
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
    "[enumeration][query][negation][rest]") {
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
  Subarray subarray(ctx_, array);
  subarray.add_range("dim", 1, 5);
  query.set_subarray(subarray)
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
    "[enumeration][query][combination][rest]") {
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
  Subarray subarray(ctx_, array);
  subarray.add_range("dim", 1, 5);
  query.set_subarray(subarray)
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
    "CPP: Enumeration Query - Invalid Enumeration Value is Always False",
    "[enumeration][query][basic][rest]") {
  create_array();

  // Attempt to query with an enumeration value that isn't in the Enumeration
  QueryCondition qc(ctx_);
  qc.init("attr1", "alf", 3, TILEDB_EQ);

  // Execute the query condition against the array
  std::vector<int> dim(5);
  std::vector<int> attr1(5);

  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  Subarray subarray(ctx_, array);
  subarray.add_range("dim", 1, 5);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr1", attr1)
      .set_condition(qc);

  REQUIRE_NOTHROW(query.submit());

  std::vector<int> dim_expect = {1, 2, 3, 4, 5};
  std::vector<int> attr1_expect = {
      INT32_MIN, INT32_MIN, INT32_MIN, INT32_MIN, INT32_MIN};

  REQUIRE(dim == dim_expect);
  REQUIRE(attr1 == attr1_expect);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration Query - Invalid Enumeration Value Accepted by EQ",
    "[enumeration][query][basic][rest]") {
  create_array();

  // Attempt to query with an enumeration value that isn't in the Enumeration
  QueryCondition qc(ctx_);
  qc.init("attr1", "alf", 3, TILEDB_EQ);

  // Execute the query condition against the array
  std::vector<int> dim(5);
  std::vector<int> attr1(5);

  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  Subarray subarray(ctx_, array);
  subarray.add_range("dim", 1, 5);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr1", attr1)
      .set_condition(qc);

  CHECK_NOTHROW(query.submit());
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration Query - Invalid Enumeration Value Accepted by IN",
    "[enumeration][query][basic][rest]") {
  create_array();

  // Attempt to query with an enumeration value that isn't in the Enumeration
  std::vector<std::string> vals = {"alf", "fred"};
  auto qc = QueryConditionExperimental::create(ctx_, "attr1", vals, TILEDB_IN);

  // Execute the query condition against the array
  std::vector<int> dim(5);
  std::vector<int> attr1(5);

  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  Subarray subarray(ctx_, array);
  subarray.add_range("dim", 1, 5);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr1", attr1)
      .set_condition(qc);

  CHECK_NOTHROW(query.submit());
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration Query - Set Use Enumeration",
    "[enumeration][query][set-use-enumeration][rest]") {
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
    "[enumeration][query][check-nullptr][rest]") {
  auto rc =
      tiledb_query_condition_set_use_enumeration(ctx_.ptr().get(), nullptr, 0);
  REQUIRE(rc != TILEDB_OK);
}

TEST_CASE_METHOD(
    CPPEnumerationFx,
    "CPP: Enumeration Query - Attempt to query on empty enumeration",
    "[enumeration][query][empty-results][rest]") {
  create_array(true);

  // Attempt to query with an enumeration value that isn't in the Enumeration
  QueryCondition qc(ctx_);
  qc.init("attr3", "alf", 3, TILEDB_EQ);

  // Execute the query condition against the array
  std::vector<int> dim(5);
  std::vector<int> attr3(5);

  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  Subarray subarray(ctx_, array);
  subarray.add_range("dim", 1, 5);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("dim", dim)
      .set_data_buffer("attr3", attr3)
      .set_condition(qc);

  REQUIRE_NOTHROW(query.submit());
}

CPPEnumerationFx::CPPEnumerationFx()
    : uri_(vfs_test_setup_.array_uri("enumeration_test_array"))
    , ctx_(vfs_test_setup_.ctx())
    , vfs_(vfs_test_setup_.ctx()) {
}

template <typename T>
void CPPEnumerationFx::check_dump(const T& val) {
  std::stringstream ss;
  ss << val;

  REQUIRE(ss.str().find("Enumeration") != std::string::npos);
}

void CPPEnumerationFx::create_array(bool with_empty_enumeration) {
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

  if (with_empty_enumeration) {
    auto empty_enmr = Enumeration::create_empty(
        ctx_, "empty_enmr", TILEDB_STRING_ASCII, TILEDB_VAR_NUM);
    ArraySchemaExperimental::add_enumeration(ctx_, schema, empty_enmr);

    auto attr3 = Attribute::create<int>(ctx_, "attr3");
    AttributeExperimental::set_enumeration_name(ctx_, attr3, "empty_enmr");
    schema.add_attribute(attr3);
  }

  Array::create(uri_, schema);

  // Attribute data
  std::vector<int> attr1_values = {0, 1, 2, 1, 0};
  std::vector<float> attr2_values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  std::vector<int> attr3_values = {0, 0, 0, 0, 0};

  Array array(ctx_, uri_, TILEDB_WRITE);
  Subarray subarray(ctx_, array);
  subarray.set_subarray({1, 5});
  Query query(ctx_, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("attr1", attr1_values)
      .set_data_buffer("attr2", attr2_values);

  if (with_empty_enumeration) {
    query.set_data_buffer("attr3", attr3_values);
  }

  CHECK_NOTHROW(query.submit());
  query.finalize();
  array.close();
}
