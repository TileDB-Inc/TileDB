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

#include "test/support/src/vfs_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;

struct RESTEnumerationFx {
  RESTEnumerationFx();

  void create_array(const std::string& array_name);

  tiledb::test::VFSTestSetup vfs_test_setup_;
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

RESTEnumerationFx::RESTEnumerationFx()
    : ctx_(vfs_test_setup_.ctx()){};

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
