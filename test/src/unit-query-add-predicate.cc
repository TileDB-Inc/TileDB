/**
 * @file unit-query-add-predicate.cc
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
 * Tests for the `tiledb_query_add_predicate` API.
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

  static const Cells INPUT;

  Context context() const {
    return vfs_test_setup_.ctx();
  }

  /**
   * Creates and writes a two-dimension array with attributes:
   * - 'a INT32'
   * - 'v VARCHAR NOT NULL'
   * - 'e UINT8:VARCHAR'
   */
  void create_array(
      const std::string& path,
      tiledb_array_type_t atype,
      bool allow_dups = false);

  /**
   * Writes cells to a sparse array using the data in `input`
   */
  template <templates::FragmentType F = Cells>
  void write_array(const std::string& path, const F& input = INPUT);

  /**
   * Writes `INPUT` to saturate the ranges [[1, 4], [1, 4]] for an array
   * of the schema given above
   */
  void write_array_dense(const std::string& path);

  template <templates::FragmentType F = Cells>
  F query_array(
      const std::string& path,
      tiledb_layout_t layout,
      const std::vector<std::string>& predicates,
      const Config& query_config = Config());
};

template <templates::FragmentType F, typename... CellType>
static F make_cells_generic(
    std::vector<uint64_t> d1,
    std::vector<uint64_t> d2,
    std::vector<CellType>... atts) {
  return F{
      .d1_ = templates::query_buffers<uint64_t>(d1),
      .d2_ = templates::query_buffers<uint64_t>(d2),
      .atts_ = std::apply(
          []<typename... T>(std::vector<T>... att) {
            return std::make_tuple<templates::query_buffers<T>...>(
                templates::query_buffers<T>(att)...);
          },
          std::make_tuple(atts...))};
}

static Cells make_cells(
    std::vector<uint64_t> d1,
    std::vector<uint64_t> d2,
    std::vector<std::optional<int32_t>> a,
    std::vector<std::string> v,
    std::vector<std::optional<int32_t>> e) {
  return make_cells_generic<Cells>(d1, d2, a, v, e);
}

const Cells QueryAddPredicateFx::INPUT = make_cells(
    {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4},
    {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4},
    {15,
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
     0},
    {"one",
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
     "sixteen"},
    {4,
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
     std::nullopt});

void QueryAddPredicateFx::create_array(
    const std::string& path, tiledb_array_type_t atype, bool allow_dups) {
  auto ctx = context();

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<uint64_t>(ctx, "row", {{1, 4}}, 4));
  domain.add_dimension(Dimension::create<uint64_t>(ctx, "col", {{1, 4}}, 4));

  ArraySchema schema(ctx, atype);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_domain(domain);
  schema.set_allows_dups(allow_dups);

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

template <templates::FragmentType F>
void QueryAddPredicateFx::write_array(const std::string& path, const F& input) {
  auto ctx = context();
  Array array(ctx, path, TILEDB_WRITE);
  Query query(ctx, array);

  auto field_sizes =
      templates::query::make_field_sizes<Asserter>(const_cast<F&>(input));
  templates::query::set_fields<Asserter>(
      ctx.ptr().get(),
      query.ptr().get(),
      field_sizes,
      const_cast<F&>(input),
      array.ptr().get()->array_schema_latest());
  query.submit();
}

void QueryAddPredicateFx::write_array_dense(const std::string& path) {
  auto ctx = context();
  Array array(ctx, path, TILEDB_WRITE);
  Query query(ctx, array);

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
}

template <templates::FragmentType F>
F QueryAddPredicateFx::query_array(
    const std::string& path,
    tiledb_layout_t layout,
    const std::vector<std::string>& predicates,
    const Config& config) {
  auto ctx = context();

  Array array(ctx, path, TILEDB_READ);
  Query query(ctx, array);

  query.set_config(config).set_layout(layout);

  F out;
  out.resize(32);

  auto field_sizes =
      templates::query::make_field_sizes<Asserter>(out, out.size());

  templates::query::set_fields<Asserter>(
      ctx.ptr().get(),
      query.ptr().get(),
      field_sizes,
      out,
      array.ptr().get()->array_schema_latest());

  for (const std::string& pred : predicates) {
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
    "Query add predicate errors",
    "[capi][query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_query_add_predicate_errors");

  create_array(array_name, TILEDB_SPARSE);
  write_array(array_name);

  auto ctx = context();

  SECTION("Non-read query errors") {
    Array array(ctx, array_name, TILEDB_WRITE);
    Query query(ctx, array);

    REQUIRE_THROWS_WITH(
        QueryExperimental::add_predicate(ctx, query, {"row BETWEEN 4 AND 7"}),
        Catch::Matchers::ContainsSubstring(
            "Cannot add query predicate; Operation only applicable to read "
            "queries"));
  }

  SECTION("Read query errors") {
    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array);

    SECTION("Null") {
      const auto maybe_err = error_if_any(
          ctx.ptr().get(),
          tiledb_query_add_predicate(
              ctx.ptr().get(), query.ptr().get(), nullptr));
      REQUIRE(maybe_err.has_value());
      REQUIRE_THAT(
          maybe_err.value(),
          Catch::Matchers::ContainsSubstring(
              "Argument \"predicate\" may not be NULL"));
    }

    SECTION("Syntax error") {
      // FIXME: this smells like a bug in datafusion.
      // If you dbg! the returned expr it prints `Expr::Column(Column { name:
      // "row" })`
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, {"row col"}),
          Catch::Matchers::ContainsSubstring(
              "Error: Expression does not return a boolean value"));
    }

    SECTION("Non-expression") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(
              ctx, query, {"CREATE TABLE foo (id INT)"}),
          Catch::Matchers::ContainsSubstring(
              "Error adding predicate: Parse error: SQL error: "
              "ParserError(\"Unsupported command in expression\")"));
    }

    SECTION("Not a predicate") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, {"row"}),
          Catch::Matchers::ContainsSubstring(
              "Expression does not return a boolean value"));
    }

    SECTION("Schema error") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, {"depth = 3"}),
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
          QueryExperimental::add_predicate(
              ctx, query, {"starts_with(row, '1')"}),
          Catch::Matchers::ContainsSubstring(dferror));
    }

    SECTION("Aggregate") {
      REQUIRE_THROWS_WITH(
          QueryExperimental::add_predicate(ctx, query, {"sum(row) >= 10"}),
          Catch::Matchers::ContainsSubstring(
              "Aggregate functions in predicate is not supported"));
    }
  }
}

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "Query add predicate dense array",
    "[query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_query_add_predicate_dense");

  create_array(array_name, TILEDB_DENSE);
  write_array_dense(array_name);

  // FIXME: error messages
  REQUIRE_THROWS(query_array(array_name, TILEDB_UNORDERED, {"row >= 3"}));
  REQUIRE_THROWS(query_array(array_name, TILEDB_ROW_MAJOR, {"row >= 3"}));
  REQUIRE_THROWS(query_array(array_name, TILEDB_COL_MAJOR, {"row >= 3"}));
  REQUIRE_THROWS(query_array(array_name, TILEDB_GLOBAL_ORDER, {"row >= 3"}));
  REQUIRE_THROWS(query_array(array_name, TILEDB_HILBERT, {"row >= 3"}));
}

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "Query add predicate sparse unsupported query order",
    "[query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_query_add_predicate_sparse_unsupported");

  create_array(array_name, TILEDB_SPARSE);
  write_array(array_name);

  const auto match = Catch::Matchers::ContainsSubstring(
      "This query does not support predicates added with "
      "tiledb_query_add_predicate");

  SECTION("Row major") {
    REQUIRE_THROWS_WITH(
        query_array(array_name, TILEDB_ROW_MAJOR, {"a IS NULL", "row > col"}),
        match);
  }

  SECTION("Col major") {
    REQUIRE_THROWS_WITH(
        query_array(array_name, TILEDB_COL_MAJOR, {"a IS NULL", "row > col"}),
        match);
  }

  SECTION("Legacy global order") {
    Config qconf;
    qconf["sm.query.sparse_global_order.reader"] = "legacy";
    REQUIRE_THROWS_WITH(
        query_array(
            array_name, TILEDB_GLOBAL_ORDER, {"a IS NULL", "row > col"}, qconf),
        match);
  }
}

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "Query add predicate sparse global order",
    "[query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_query_add_predicate_sparse_global_order");

  const auto query_order = GENERATE(TILEDB_GLOBAL_ORDER, TILEDB_UNORDERED);

  create_array(array_name, TILEDB_SPARSE);
  write_array(array_name);

  SECTION("WHERE TRUE") {
    const auto result = query_array(array_name, query_order, {"TRUE"});
    CHECK(result == INPUT);
  }

  SECTION("WHERE a IS NOT NULL") {
    const Cells expect = make_cells(
        {1, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4},
        {1, 4, 2, 3, 1, 2, 3, 4, 2, 3, 4},
        {15, 12, 10, 9, 7, 6, 5, 4, 2, 1, 0},
        {"one",
         "four",
         "six",
         "seven",
         "nine",
         "ten",
         "eleven",
         "twelve",
         "fourteen",
         "fifteen",
         "sixteen"},
        {4,
         std::nullopt,
         7,
         std::nullopt,
         1,
         std::nullopt,
         3,
         4,
         6,
         7,
         std::nullopt});

    const auto result = query_array(array_name, query_order, {"a IS NOT NULL"});
    CHECK(result == expect);
  }

  SECTION("WHERE v < 'fourteen'") {
    const Cells expect = make_cells(
        {1, 2, 2, 3, 4},
        {4, 1, 4, 3, 3},
        {12, std::nullopt, std::nullopt, 5, 1},
        {"four", "five", "eight", "eleven", "fifteen"},
        {std::nullopt, 7, 0, 3, 7});

    const auto result =
        query_array(array_name, query_order, {"v < 'fourteen'"});
    CHECK(result == expect);
  }

  SECTION("WHERE row + col <= 4") {
    const Cells expect = make_cells(
        {1, 1, 1, 2, 2, 3},
        {1, 2, 3, 1, 2, 1},
        {15, std::nullopt, std::nullopt, std::nullopt, 10, 7},
        {"one", "two", "three", "five", "six", "nine"},
        {4, 4, 7, 7, 7, 1});

    const auto result =
        query_array(array_name, query_order, {"row + col <= 4"});
    CHECK(result == expect);
  }

  SECTION("WHERE a IS NULL AND row > col") {
    const Cells expect = make_cells(
        {2, 4},
        {1, 1},
        {std::nullopt, std::nullopt},
        {"five", "thirteen"},
        {7, std::nullopt});

    const auto result =
        query_array(array_name, query_order, {"a IS NULL", "row > col"});
    CHECK(result == expect);
  }

  SECTION("WHERE coalesce(a, row) > col") {
    const Cells expect = make_cells(
        {1, 1, 2, 2, 2, 3, 3, 3, 4},
        {1, 4, 1, 2, 3, 1, 2, 3, 1},
        {15, 12, std::nullopt, 10, 9, 7, 6, 5, std::nullopt},
        {"one",
         "four",
         "five",
         "six",
         "seven",
         "nine",
         "ten",
         "eleven",
         "thirteen"},
        {4,
         std::nullopt,
         7,
         7,
         std::nullopt,
         1,
         std::nullopt,
         3,
         std::nullopt});

    const auto result =
        query_array(array_name, query_order, {"coalesce(a, row) > col"});
    CHECK(result == expect);
  }

  SECTION("WHERE e < 'california'") {
    // enumeration not supported yet
    REQUIRE_THROWS_WITH(
        query_array(array_name, query_order, {"e < 'california'"}),
        Catch::Matchers::ContainsSubstring(
            "QueryCondition: Error evaluating expression: Cannot process field "
            "'e': Attributes with enumerations are not supported in text "
            "predicates"));
  }
}

TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "Query add predicate sparse unordered with dups",
    "[query][add_predicate]") {
  const std::string array_name = vfs_test_setup_.array_uri(
      "test_query_add_predicate_sparse_unordered_with_dups");

  create_array(array_name, TILEDB_SPARSE, true);

  const auto query_order = TILEDB_UNORDERED;

  const Cells f2 = make_cells(
      {1, 1, 2, 2, 3, 3, 4, 4},
      {1, 4, 2, 3, 1, 4, 2, 3},
      {-1, std::nullopt, std::nullopt, -4, std::nullopt, -6, -7, std::nullopt},
      {"ένα", "δύο", "τρία", "τέσσερα", "πέντε", "έξι", "επτά", "οκτώ"},
      {0, 7, 1, std::nullopt, 2, 6, std::nullopt, 3});
  const Cells f3 = make_cells(
      {1, 1, 2, 2, 3, 3, 4, 4},
      {1, 2, 3, 4, 1, 2, 3, 4},
      {-9, -10, -11, -12, std::nullopt, -14, -15, -16},
      {"uno", "dos", "tres", "quatro", "cinco", "seis", "siete", "ocho"},
      {7, 0, 6, std::nullopt, 1, 5, std::nullopt, 2});

  // fragment 1: base input
  write_array(array_name);
  write_array(array_name, f2);
  write_array(array_name, f3);

  SECTION("WHERE TRUE") {
    const Cells expect = templates::query::concat({INPUT, f2, f3});
    const auto result = query_array(array_name, query_order, {"TRUE"});
    CHECK(result == expect);
  }

  SECTION("WHERE v < 'fourteen'") {
    const Cells expect = make_cells(
        {1, 2, 2, 3, 4, 1, 3},
        {4, 1, 4, 3, 3, 2, 1},
        {12, std::nullopt, std::nullopt, 5, 1, -10, std::nullopt},
        {"four", "five", "eight", "eleven", "fifteen", "dos", "cinco"},
        {std::nullopt, 7, 0, 3, 7, 0, 1});

    const auto result =
        query_array(array_name, query_order, {"v < 'fourteen'"});
    CHECK(result == expect);
  }

  SECTION("WHERE row + col <= 4") {
    const Cells expect = make_cells(
        {1, 1, 1, 2, 2, 3, 1, 2, 3, 1, 1, 3},
        {1, 2, 3, 1, 2, 1, 1, 2, 1, 1, 2, 1},
        {15,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         10,
         7,
         -1,
         std::nullopt,
         std::nullopt,
         -9,
         -10,
         std::nullopt},
        {"one",
         "two",
         "three",
         "five",
         "six",
         "nine",
         "ένα",
         "τρία",
         "πέντε",
         "uno",
         "dos",
         "cinco"},
        {4, 4, 7, 7, 7, 1, 0, 1, 2, 7, 0, 1});

    const auto result =
        query_array(array_name, query_order, {"row + col <= 4"});
    CHECK(result == expect);
  }

  SECTION("WHERE a IS NULL AND row > col") {
    const Cells expect = make_cells(
        {2, 4, 3, 4, 3},
        {1, 1, 1, 3, 1},
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
        {"five", "thirteen", "πέντε", "οκτώ", "cinco"},
        {7, std::nullopt, 2, 3, 1});

    const auto result =
        query_array(array_name, query_order, {"a IS NULL", "row > col"});
    CHECK(result == expect);
  }

  SECTION("WHERE octet_length(v) > char_length(v)") {
    const Cells expect = f2;

    const auto result = query_array(
        array_name, query_order, {"octet_length(v) > char_length(v)"});
    CHECK(result == expect);
  }

  SECTION("WHERE e < 'california'") {
    // enumeration not supported yet
    REQUIRE_THROWS_WITH(
        query_array(array_name, query_order, {"e < 'california'"}),
        Catch::Matchers::ContainsSubstring(
            "QueryCondition: Error evaluating expression: Cannot process field "
            "'e': Attributes with enumerations are not supported in text "
            "predicates"));
  }
}

/**
 * Test that we do something reasonable when evaluating a predicate
 * on an array whose schema evolved to have a different type for the
 * same attribute
 */
TEST_CASE_METHOD(
    QueryAddPredicateFx,
    "Query add predicate evolved schema",
    "[query][add_predicate]") {
  const std::string array_name =
      vfs_test_setup_.array_uri("test_query_add_predicate_evolution");

  create_array(array_name, TILEDB_SPARSE);
  write_array(array_name, INPUT);

  {
    auto ctx = context();
    ArraySchemaEvolution(ctx).drop_attribute("a").array_evolve(array_name);

    ArraySchemaEvolution(ctx)
        .add_attribute(Attribute::create<std::string>(ctx, "a"))
        .array_evolve(array_name);
  }

  using CellsEvolved = templates::Fragment2D<
      uint64_t,
      uint64_t,
      std::string,
      std::optional<int32_t>,
      std::string>;

  const CellsEvolved f2 = make_cells_generic<
      CellsEvolved,
      std::string,
      std::optional<int32_t>,
      std::string>(
      {1, 2, 3, 4},
      {1, 2, 3, 4},
      {"seventeen", "eighteen", "nineteen", "twenty"},
      {0, 1, 2, 3},
      {"00", "01", "10", "11"});
  write_array(array_name, f2);

  SECTION("WHERE a LIKE '%1'") {
    CellsEvolved expect = make_cells_generic<
        CellsEvolved,
        std::string,
        std::optional<int32_t>,
        std::string>(
        {2, 4}, {2, 4}, {"eighteen", "twenty"}, {1, 3}, {"01", "11"});

    const auto result = query_array<CellsEvolved>(
        array_name, TILEDB_GLOBAL_ORDER, {"a LIKE '%1'"});
    CHECK(result == expect);
  }

  SECTION("WHERE a & 1 = 0") {
    REQUIRE_THROWS_WITH(
        query_array<CellsEvolved>(
            array_name, TILEDB_GLOBAL_ORDER, {"a & 1 = 0"}),
        Catch::Matchers::ContainsSubstring(
            "Error: Error adding predicate: Type coercion error: Error during "
            "planning: Cannot infer common type for bitwise operation "
            "LargeUtf8 & Int64"));
  }
}
