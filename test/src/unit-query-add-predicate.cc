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

#include "test/support/src/array_templates.h"
#include "test/support/src/error_helpers.h"
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;
using namespace tiledb::test;

// no rapidcheck
using Asserter = AsserterCatch;

// query result type for the array schema used in these tests
using Cells = templates::Fragment2D<
    uint64_t,
    uint64_t,
    std::optional<int32_t>,
    std::vector<char>,
    std::optional<int32_t>>;

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

  Cells query_array(
      const std::string& path,
      tiledb_layout_t layout,
      std::vector<const char*> predicates);

  Cells query_array(
      const std::string& path, tiledb_layout_t layout, const char* predicate) {
    return query_array(path, layout, std::vector<const char*>{predicate});
  }

  static const Cells INPUT;
};

const Cells QueryAddPredicateFx::INPUT = Cells{
    .d1_ = templates::query_buffers<uint64_t>(
        {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4}),
    .d2_ = templates::query_buffers<uint64_t>(
        {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4}),
    .atts_ = std::make_tuple(
        templates::query_buffers<std::optional<int32_t>>(
            std::vector<std::optional<int32_t>>{
                15,
                std::nullopt,
                std::nullopt,
                12,
                std::nullopt,
                10,
                9,
                std::nullopt,
                7,
                6,
                5,
                4,
                std::nullopt,
                2,
                1,
                0}),
        templates::query_buffers<std::string>(std::vector<std::string>{
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
            "sixteen"}),
        templates::query_buffers<std::optional<int32_t>>(
            std::vector<std::optional<int32_t>>{
                4,
                4,
                7,
                std::nullopt,
                7,
                7,
                std::nullopt,
                0,
                1,
                std::nullopt,
                3,
                4,
                std::nullopt,
                6,
                7,
                std::nullopt}))};

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

  if (atype == TILEDB_DENSE) {
    Subarray s(ctx, array);
    s.add_range<uint64_t>(0, 1, 4);
    s.add_range<uint64_t>(1, 1, 4);
    query.set_layout(TILEDB_ROW_MAJOR).set_subarray(s);

    templates::Fragment<
        std::optional<int32_t>,
        std::vector<char>,
        std::optional<int32_t>>
        cells = {.atts_ = INPUT.atts_};

    auto field_sizes = templates::query::make_field_sizes<Asserter>(cells);
    templates::query::set_fields<Asserter>(
        ctx.ptr().get(),
        query.ptr().get(),
        field_sizes,
        cells,
        array.ptr().get()->array_schema_latest());

    query.submit();
  } else {
    auto field_sizes =
        templates::query::make_field_sizes<Asserter>(const_cast<Cells&>(INPUT));
    templates::query::set_fields<Asserter>(
        ctx.ptr().get(),
        query.ptr().get(),
        field_sizes,
        const_cast<Cells&>(INPUT),
        array.ptr().get()->array_schema_latest());
    query.submit();
  }
}

Cells QueryAddPredicateFx::query_array(
    const std::string& path,
    tiledb_layout_t layout,
    std::vector<const char*> predicates) {
  auto ctx = context();

  Array array(ctx, path, TILEDB_READ);
  Query query(ctx, array);

  query.set_layout(layout);

  Cells out;
  out.resize(32);

  auto field_sizes =
      templates::query::make_field_sizes<Asserter>(out, out.size());

  templates::query::set_fields<Asserter>(
      ctx.ptr().get(),
      query.ptr().get(),
      field_sizes,
      out,
      array.ptr().get()->array_schema_latest());

  for (const char* pred : predicates) {
    QueryExperimental::add_predicate(ctx, query, pred);
  }

  if (array.schema().array_type() == TILEDB_DENSE) {
    Subarray s(ctx, array);
    s.add_range<uint64_t>(0, 1, 4);
    s.add_range<uint64_t>(1, 1, 4);
    query.set_subarray(s);
  }

  const auto st = query.submit();
  REQUIRE(st == Query::Status::COMPLETE);

  templates::query::resize_fields<Asserter>(out, field_sizes);

  return out;
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

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "C API: Test query add predicate dense",
    "[capi][query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_qeury_add_predicate_dense");

  create_array(array_name, TILEDB_DENSE);
  write_array(array_name, TILEDB_DENSE);

  // FIXME: error messages
  REQUIRE_THROWS(query_array(array_name, TILEDB_UNORDERED, "row >= 3"));
  REQUIRE_THROWS(query_array(array_name, TILEDB_ROW_MAJOR, "row >= 3"));
  REQUIRE_THROWS(query_array(array_name, TILEDB_COL_MAJOR, "row >= 3"));
  REQUIRE_THROWS(query_array(array_name, TILEDB_GLOBAL_ORDER, "row >= 3"));
  REQUIRE_THROWS(query_array(array_name, TILEDB_HILBERT, "row >= 3"));
}

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "C API: Test query add predicate legacy",
    "[capi][query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_qeury_add_predicate_legacy");
  // TODO
}

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "C API: Test query add predicate sparse unsupported query order",
    "[capi][query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_qeury_add_predicate_sparse_unsupported");

  create_array(array_name, TILEDB_SPARSE);
  write_array(array_name, TILEDB_SPARSE);
  // TODO
}

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "C API: Test query add predicate sparse global order",
    "[capi][query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_qeury_add_predicate_sparse_global_order");

  create_array(array_name, TILEDB_SPARSE);
  write_array(array_name, TILEDB_SPARSE);

  SECTION("WHERE TRUE") {
    const auto result = query_array(array_name, TILEDB_GLOBAL_ORDER, "TRUE");
    CHECK(result == INPUT);
  }

  SECTION("WHERE a IS NULL") {
    // TODO
  }

  SECTION("WHERE b < 'fourteen'") {
    // TODO
  }

  SECTION("WHERE row + col <= 4") {
    // TODO
  }

  SECTION("WHERE coalesce(a, row) > a") {
    // TODO
  }
}
