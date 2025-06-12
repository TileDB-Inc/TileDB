/**
 * @file tiledb/sm/array_schema/test/unit_array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2023 TileDB, Inc.
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
 * This file contains unit tests for the array schema
 */

#include <tdb_catch.h>

#include "array_schema_test_support.h"
#include "tiledb/sm/array_schema/dimension_label.h"

using namespace tiledb::common;
using namespace tiledb::sm;

/*
 * Every attribute must appear in both attribute containers. Indices and
 * pointers must be coherent. For example, if attribute(i).name() == s, then
 * attribute(i) == attribute(s).
 */
TEST_CASE(
    "ArraySchema - accessors by name and by index are coherent",
    "[array_schema]") {
  TestArraySchema test_schema{
      {TestDimension("d1", Datatype::UINT32),
       TestDimension("d2", Datatype::UINT32)},
      {
          TestAttribute("a3", Datatype::UINT32),
          TestAttribute("a4", Datatype::UINT32),
          TestAttribute("a5", Datatype::UINT32),
          TestAttribute("a6", Datatype::UINT32),
      }};
  const auto& schema{test_schema.schema()};
  REQUIRE(schema.attribute_num() == 4);
  std::array names{"a3", "a4", "a5", "a6"};
  for (unsigned int j = 0; j < 4; ++j) {
    std::string name{names[j]};
    // name must match the index
    auto attr_i{schema.attribute(j)};
    CHECK(attr_i->name() == name);
    // shared attribute pointer must match
    auto attr_si{schema.shared_attribute(j)};
    CHECK(attr_i == attr_si.get());
    // attribute by name must match
    auto attr_n{schema.attribute(name)};
    CHECK(attr_i == attr_n);
    // shared attribute by name must match
    auto attr_sn{schema.shared_attribute(name)};
    CHECK(attr_i == attr_sn.get());
  }
}

TEST_CASE("Repeated names in schema columns", "[array_schema]") {
  using tp = std::pair<std::string, TestArraySchema>;
  auto test_schema = GENERATE(
      tp{"XA",
         TestArraySchema{
             {TestDimension("x", Datatype::UINT64)},
             {TestAttribute("a", Datatype::UINT64)}}},
      tp{"XYA",
         TestArraySchema{
             {TestDimension("x", Datatype::UINT64),
              TestDimension("y", Datatype::UINT64)},
             {TestAttribute("a", Datatype::UINT64)}}});
  auto name{test_schema.first};
  auto schema{test_schema.second.schema()};

  DYNAMIC_SECTION("Base schema is valid - " + name) {
    REQUIRE_NOTHROW(schema.check_without_config());
  }

  DYNAMIC_SECTION("Dimension names must be unique - " + name) {
    /*
     * `ArraySchema::domain` is cast to `const`, so it's not an issue to cast it
     * away.
     */
    auto& domain{const_cast<Domain&>(schema.domain())};
    TestDimension td("x", Datatype::UINT64);
    auto d{td.dimension()};
    domain.add_dimension(d);
    /*
     * `add_dimension()` does not incrementally validate that names are unique.
     * If this changes the following check will fail.
     */
    REQUIRE_THROWS(schema.check_without_config());
  }

  DYNAMIC_SECTION("Attribute names must be unique - " + name) {
    TestAttribute ta{"a", Datatype::UINT64};
    auto a{ta.attribute()};
    schema.add_attribute(a);
    /*
     * `add_attribute()` does not incrementally validate that names are unique.
     * If this changes the following check will fail.
     */
    REQUIRE_THROWS(schema.check_without_config());
  }

  DYNAMIC_SECTION("Label names must be unique - " + name) {
    schema.add_dimension_label(
        0, "z", DataOrder::INCREASING_DATA, Datatype::UINT64, true);
    REQUIRE_THROWS(schema.add_dimension_label(
        0, "z", DataOrder::INCREASING_DATA, Datatype::UINT64, true));
  }

  DYNAMIC_SECTION("Attribute name may not be a dimension name - " + name) {
    TestAttribute ta{"x", Datatype::UINT64};
    auto a{ta.attribute()};
    schema.add_attribute(a);
    /*
     * `add_attribute()` does not incrementally validate that names are unique.
     * If this changes the following check will fail.
     */
    REQUIRE_THROWS(schema.check_without_config());
  }

  DYNAMIC_SECTION("Label name may not be a dimension name - " + name) {
    REQUIRE_THROWS(schema.add_dimension_label(
        0, "x", DataOrder::INCREASING_DATA, Datatype::FLOAT64, true));
  }

  DYNAMIC_SECTION("Label name may not be a dimension name 2 - " + name) {
    schema.add_dimension_label(
        0, "x", DataOrder::INCREASING_DATA, Datatype::FLOAT64, false);
    REQUIRE_THROWS(schema.check_without_config());
  }

  DYNAMIC_SECTION("Label name may really not be a dimension name - " + name) {
    schema.add_dimension_label(
        0, "x", DataOrder::INCREASING_DATA, Datatype::FLOAT64, false);
    schema.add_dimension_label(
        0, "x", DataOrder::INCREASING_DATA, Datatype::FLOAT64, false);
    REQUIRE_THROWS(schema.check_without_config());
  }

  DYNAMIC_SECTION("Label name may not be an attribute name - " + name) {
    // Final 'true' argument verifies that names are unique
    REQUIRE_THROWS(schema.add_dimension_label(
        0, "a", DataOrder::INCREASING_DATA, Datatype::FLOAT64, true));
  }

  DYNAMIC_SECTION("Label name may not be an attribute name 2 - " + name) {
    // final 'false' argument suppresses verification of unique names
    schema.add_dimension_label(
        0, "a", DataOrder::INCREASING_DATA, Datatype::FLOAT64, false);
    REQUIRE_THROWS(schema.check_without_config());
  }
}

TEST_CASE(
    "URIs for dimension labels are properly formed",
    "[array_schema][dimension_label]") {
  TestArraySchema test_schema{
      {TestDimension("x", Datatype::UINT64)},
      {TestAttribute("a", Datatype::UINT64)}};
  auto schema{test_schema.schema()};
  schema.add_dimension_label(
      0, "x1", DataOrder::INCREASING_DATA, Datatype::FLOAT64, true);
  schema.add_dimension_label(
      0, "y", DataOrder::INCREASING_DATA, Datatype::FLOAT64, true);
  schema.add_dimension_label(
      0, "z", DataOrder::INCREASING_DATA, Datatype::FLOAT64, true);
  // Check dimension label schemas
  const auto& xref = schema.dimension_label("x1");
  REQUIRE(
      xref.uri().to_string() ==
      constants::array_dimension_labels_dir_name + "/l0");
  const auto& yref = schema.dimension_label("y");
  REQUIRE(
      yref.uri().to_string() ==
      constants::array_dimension_labels_dir_name + "/l1");
  const auto& zref = schema.dimension_label("z");
  REQUIRE(
      zref.uri().to_string() ==
      constants::array_dimension_labels_dir_name + "/l2");
}

TEST_CASE("ArraySchema::has_ordered_attributes, false", "[array_schema]") {
  TestArraySchema test_schema{
      {TestDimension("x", Datatype::UINT64)},
      {TestAttribute("a", Datatype::UINT64),
       TestAttribute("b", Datatype::FLOAT64)}};
  auto& schema{test_schema.schema()};
  REQUIRE_FALSE(schema.has_ordered_attributes());
}

TEST_CASE("ArraySchema::has_ordered_attributes, true", "[array_schema]") {
  TestArraySchema test_schema{
      {TestDimension("x", Datatype::UINT64)},
      {TestAttribute("a", Datatype::UINT64)}};
  auto schema{test_schema.schema()};
  // TestAttribute does not yet support DataOrder
  schema.add_attribute(make_shared<Attribute>(
      HERE(), "b", Datatype::FLOAT64, 1, DataOrder::INCREASING_DATA));
  CHECK(schema.has_ordered_attributes());
}
