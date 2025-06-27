/**
 * @file unit-capi-query-add-predicate.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * Tests for the C API tiledb_query_add_predicate.
 */

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>

#include "test/support/src/error_helpers.h"
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;
using namespace tiledb::test;

// no rapidcheck
using Asserter = AsserterCatch;

struct QueryAddPredicateFx {
  VFSTestSetup vfs_test_setup_;

  Context context() const {
    return vfs_test_setup_.ctx();
  }

  /**
   * Creates and writes a two-dimension array with attributes:
   * - 'a INT32'
   * - 'v VARCHAR NOT NULL'
   * - 'e UINT8:VARCHAR'
   */
  void create_array(const std::string& path, tiledb_array_type_t atype);

  /**
   * Writes cells to saturate the ranges [[1, 4], [1, 4]] for an array
   * of the schema given above
   */
  void write_array(const std::string& path, tiledb_array_type_t atype);
};

void QueryAddPredicateFx::create_array(
    const std::string& path, tiledb_array_type_t atype) {
  auto ctx = context();

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<uint64_t>(ctx, "row", {{1, 4}}, 4));
  domain.add_dimension(Dimension::create<uint64_t>(ctx, "col", {{1, 4}}, 4));

  ArraySchema schema(ctx, atype);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_domain(domain);

  schema.add_attribute(Attribute::create<int32_t>(ctx, "a").set_nullable(true));
  schema.add_attribute(Attribute::create<std::string>(ctx, "v"));

  // enumerated attribute
  std::vector<std::string> us_states = {
      "alabama",
      "alaska",
      "arizona",
      "arkansas",
      "california",
      "colorado",
      "connecticut",
      "etc"};
  ArraySchemaExperimental::add_enumeration(
      ctx,
      schema,
      Enumeration::create(ctx, std::string("us_states"), us_states));
  {
    auto e = Attribute::create<int32_t>(ctx, "e").set_nullable(true);
    AttributeExperimental::set_enumeration_name(ctx, e, "us_states");
    schema.add_attribute(e);
  }

  Array::create(path, schema);
}

void QueryAddPredicateFx::write_array(
    const std::string& path, tiledb_array_type_t atype) {
  auto ctx = context();
  Array array(ctx, path, TILEDB_WRITE);
  Query query(ctx, array);

  std::vector<uint64_t> rows = {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};
  std::vector<uint64_t> cols = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};

  if (atype == TILEDB_SPARSE) {
    query.set_data_buffer("row", rows).set_data_buffer("col", cols);
  } else {
    Subarray s(ctx, array);
    s.add_range(0, 1, 4);
    s.add_range(1, 1, 4);
    query.set_layout(TILEDB_ROW_MAJOR).set_subarray(s);
  }

  std::vector<int32_t> a_values = {
      15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
  std::vector<uint8_t> a_validity(a_values.size(), 1);
  a_validity[1] = a_validity[2] = a_validity[3] = a_validity[5] =
      a_validity[8] = a_validity[13] = 0;

  std::vector<std::string> v_strings = {
      "one",
      "two",
      "three",
      "four",
      "five",
      "six",
      "seven",
      "eight",
      "nine",
      "ten",
      "eleven",
      "twelve",
      "thirteen",
      "fourteen",
      "fifteen",
      "sixteen"};
  std::vector<char> v_values;
  std::vector<uint64_t> v_offsets;
  for (const auto& s : v_strings) {
    v_offsets.push_back(v_values.size());
    v_values.insert(v_values.end(), s.begin(), s.end());
  }

  std::vector<int32_t> e_keys = {
      4, 4, 7, 4, 7, 7, 7, 0, 1, 2, 3, 4, 5, 6, 7, 6};
  std::vector<uint8_t> e_validity(e_keys.size(), 1);
  e_validity[3] = e_validity[6] = e_validity[9] = e_validity[12] =
      e_validity[15] = 0;

  query.set_data_buffer("a", a_values)
      .set_validity_buffer("a", a_validity)
      .set_data_buffer("v", v_values)
      .set_offsets_buffer("v", v_offsets)
      .set_data_buffer("e", e_keys)
      .set_validity_buffer("e", e_validity);

  query.submit();
}

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "C API: Test query add predicate errors",
    "[capi][query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_qeury_add_predicate_errors");

  create_array(array_name, TILEDB_SPARSE);
  write_array(array_name, TILEDB_SPARSE);

  auto ctx = context();

  SECTION("Non-read query errors") {
    Array array(ctx, array_name, TILEDB_WRITE);
    Query query(ctx, array);

    REQUIRE_THROWS_WITH(
        QueryExperimental::add_predicate(ctx, query, "row BETWEEN 4 AND 7"),
        Catch::Matchers::ContainsSubstring(
            "Cannot add query predicate; Operation only applicable to read "
            "queries"));
  }

  SECTION("Read query errors") {
    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array);

    SECTION("Null") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, nullptr),
          Catch::Matchers::ContainsSubstring(
              "Argument \"predicate\" may not be NULL"));
    }

    SECTION("Syntax error") {
      // FIXME: this smells like a bug in datafusion.
      // If you dbg! the returned expr it prints `Expr::Column(Column { name:
      // "row" })`
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, "row col"),
          Catch::Matchers::ContainsSubstring(
              "Error: Expression does not return a boolean value"));
    }

    SECTION("Non-expression") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(
              ctx, query, "CREATE TABLE foo (id INT)"),
          Catch::Matchers::ContainsSubstring(
              "Error adding predicate: Parse error: SQL error: "
              "ParserError(\"Unsupported command in expression\")"));
    }

    SECTION("Not a predicate") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, "row"),
          Catch::Matchers::ContainsSubstring(
              "Expression does not return a boolean value"));
    }

    SECTION("Schema error") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, "depth = 3"),
          Catch::Matchers::ContainsSubstring(
              "Error adding predicate: Parse error: Schema error: No field "
              "named depth. Valid fields are row, col, a, v, e."));
    }

    SECTION("Type coercion failure") {
      // FIXME: from the tables CLI this gives a very different error which is
      // more user-friendly, there must be some optimization pass which we are
      // not doing
      const std::string dferror =
          "Error adding predicate: Type coercion error: Internal error: Expect "
          "TypeSignatureClass::Native(LogicalType(Native(String), String)) but "
          "received NativeType::UInt64, DataType: UInt64.\nThis was likely "
          "caused by a bug in DataFusion's code and we would welcome that you "
          "file an bug report in our issue tracker";
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, "starts_with(row, '1')"),
          Catch::Matchers::ContainsSubstring(dferror));
    }

    SECTION("Aggregate") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, "sum(row) >= 10"),
          Catch::Matchers::ContainsSubstring(
              "Aggregate functions in predicate is not supported"));
    }
  }
}
