/**
 * @file unit-rest-array-schema-load.cc
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
 * Tests tiledb_array_schema_load* functions via a REST server.
 */

#include "test/support/src/vfs_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;

struct RESTArraySchemaLoadFx {
  RESTArraySchemaLoadFx();
  ~RESTArraySchemaLoadFx();

  void create_array(const std::string& array_name);
  void rm_array();

  std::string bucket_;
  std::string uri_;
  Config cfg_;
  Context ctx_;
  VFS vfs_;
};

TEST_CASE_METHOD(
    RESTArraySchemaLoadFx,
    "Simple schema load test",
    "[rest][array-schema][simple-load]") {
  create_array("simple-load");

  ArraySchema schema = Array::load_schema(ctx_, uri_);
  auto matcher = Catch::Matchers::ContainsSubstring(
      "Enumeration 'my_enum' is not loaded.");
  REQUIRE_THROWS_WITH(
      ArraySchemaExperimental::get_enumeration(ctx_, schema, "my_enum"),
      matcher);
}

TEST_CASE_METHOD(
    RESTArraySchemaLoadFx,
    "Simple schema load with enumerations test",
    "[rest][array-schema][simple-load-with-enumerations]") {
  create_array("simple-load-with-enumerations");

  ArraySchema schema =
      ArrayExperimental::load_schema_with_enumerations(ctx_, uri_);
  REQUIRE_NOTHROW(
      ArraySchemaExperimental::get_enumeration(ctx_, schema, "my_enum"));
}

Config& setup_config(Config& cfg) {
  cfg["vfs.s3.endpoint_override"] = "localhost:9999";
  cfg["vfs.s3.scheme"] = "https";
  cfg["vfs.s3.use_virtual_addressing"] = "false";
  cfg["ssl.verify"] = "false";
  return cfg;
}

RESTArraySchemaLoadFx::RESTArraySchemaLoadFx()
    : bucket_("s3://array-schema-load-tests")
    , ctx_(setup_config(cfg_))
    , vfs_(ctx_) {
  if (!vfs_.is_bucket(bucket_)) {
    vfs_.create_bucket(bucket_);
  }
}

RESTArraySchemaLoadFx::~RESTArraySchemaLoadFx() {
  rm_array();
  if (vfs_.is_bucket(bucket_)) {
    vfs_.remove_bucket(bucket_);
  }
}

void RESTArraySchemaLoadFx::create_array(const std::string& array_name) {
  uri_ = "tiledb://unit/" + bucket_ + "/" + array_name;

  // Ensure that no array exists at uri_
  rm_array();

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

  Array::create(uri_, schema);
}

void RESTArraySchemaLoadFx::rm_array() {
  auto obj = Object::object(ctx_, uri_);
  if (obj.type() == Object::Type::Array) {
    Array::delete_array(ctx_, uri_);
  }
}
