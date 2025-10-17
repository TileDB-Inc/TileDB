/**
 * @file unit_query_condition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2024 TileDB, Inc.
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
 * Tests the `QueryCondition` class.
 */

#include "test/support/src/ast_helpers.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/storage_format/uri/generate_uri.h"

#ifdef HAVE_RUST
#include "test/support/assert_helpers.h"
#include "tiledb/oxidize/arrow.h"
#include "tiledb/oxidize/unit_query_condition.h"
#endif

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb::sm;

void check_ast_str(QueryCondition qc, std::string expect) {
  std::string ast_str = tiledb::test::ast_node_to_str(qc.ast());
  CHECK(ast_str == expect);
}

TEST_CASE(
    "QueryCondition: Test default constructor",
    "[QueryCondition][default_constructor]") {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  QueryCondition query_condition;
  REQUIRE(query_condition.empty());
  REQUIRE(query_condition.field_names().empty());

  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  std::vector<ResultCellSlab> result_cell_slabs;
  std::vector<shared_ptr<FragmentMetadata>> frag_md;
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(query_condition.apply(params, frag_md, result_cell_slabs, 1).ok());
}

TEST_CASE("QueryCondition: Test init", "[QueryCondition][value_constructor]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  REQUIRE(!query_condition.empty());
  REQUIRE(!query_condition.field_names().empty());
  REQUIRE(query_condition.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test copy constructor",
    "[QueryCondition][copy_constructor]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  QueryCondition query_condition2(query_condition1);
  REQUIRE(!query_condition2.empty());
  REQUIRE(!query_condition2.field_names().empty());
  REQUIRE(query_condition2.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test move constructor",
    "[QueryCondition][move_constructor]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  QueryCondition query_condition2(std::move(query_condition1));
  REQUIRE(!query_condition2.empty());
  REQUIRE(!query_condition2.field_names().empty());
  REQUIRE(query_condition2.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test assignment operator",
    "[QueryCondition][assignment_operator]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  QueryCondition query_condition2;
  query_condition2 = query_condition1;
  REQUIRE(!query_condition2.empty());
  REQUIRE(!query_condition2.field_names().empty());
  REQUIRE(query_condition2.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test move-assignment operator",
    "[QueryCondition][move_assignment_operator]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  QueryCondition query_condition2;
  query_condition2 = std::move(query_condition1);
  REQUIRE(!query_condition2.empty());
  REQUIRE(!query_condition2.field_names().empty());
  REQUIRE(query_condition2.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test char", "[QueryCondition][char_value][ast][api]") {
  std::string field_name = "foo";
  char value[] = "bar";

  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(
                  std::string(field_name),
                  &value,
                  strlen(value),
                  QueryConditionOp::LT)
              .ok());
  REQUIRE(!query_condition.empty());
  REQUIRE(!query_condition.field_names().empty());
  REQUIRE(query_condition.field_names().count(field_name) == 1);
  check_ast_str(query_condition, "foo LT 62 61 72");
  check_ast_str(query_condition.negated_condition(), "foo GE 62 61 72");
}

TEST_CASE("QueryCondition: Test blob type", "[QueryCondition][blob]") {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  std::string field_name = "blob_attr";
  std::byte value{5};

  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());

  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  shared_ptr<Attribute> attr =
      make_shared<Attribute>(HERE(), "blob_attr", Datatype::BLOB);
  array_schema->add_attribute(attr);

  std::vector<ResultCellSlab> result_cell_slabs;
  std::vector<shared_ptr<FragmentMetadata>> frag_md;

  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE_THROWS_WITH(
      query_condition.apply(params, frag_md, result_cell_slabs, 1),
      Catch::Matchers::ContainsSubstring(
          "Cannot perform query comparison; Unsupported datatype " +
          datatype_str(Datatype::BLOB)));
}

TEST_CASE(
    "QueryCondition: Test AST construction, basic",
    "[QueryCondition][ast][api]") {
  std::string field_name = "x";
  int val = 0x12345678;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  check_ast_str(query_condition, "x LT 78 56 34 12");
  check_ast_str(query_condition.negated_condition(), "x GE 78 56 34 12");
}

TEST_CASE(
    "Query Condition: Test AST construction, basic AND combine",
    "[QueryCondition][ast][api]") {
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  check_ast_str(query_condition, "x LT 12 ef cd ab");

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  check_ast_str(query_condition1, "y GT 33 33 33 33");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  check_ast_str(combined_and, "(x LT 12 ef cd ab AND y GT 33 33 33 33)");
  check_ast_str(
      combined_and.negated_condition(),
      "(x GE 12 ef cd ab OR y LE 33 33 33 33)");
}

TEST_CASE(
    "Query Condition: Test AST construction, basic OR combine",
    "[QueryCondition][ast][api]") {
  // OR combine.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  check_ast_str(query_condition, "x LT 12 ef cd ab");

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  check_ast_str(query_condition1, "y GT 33 33 33 33");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  check_ast_str(combined_or, "(x LT 12 ef cd ab OR y GT 33 33 33 33)");
  check_ast_str(
      combined_or.negated_condition(),
      "(x GE 12 ef cd ab AND y LE 33 33 33 33)");
}

TEST_CASE(
    "Query Condition: Test AST construction, basic AND combine, string",
    "[QueryCondition][ast][api]") {
  char e[] = "eve";
  QueryCondition query_condition;
  REQUIRE(query_condition.init("x", &e, strlen(e), QueryConditionOp::LT).ok());
  check_ast_str(query_condition, "x LT 65 76 65");

  char b[] = "bob";
  QueryCondition query_condition1;
  REQUIRE(query_condition1.init("x", &b, strlen(b), QueryConditionOp::GT).ok());
  check_ast_str(query_condition1, "x GT 62 6f 62");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  check_ast_str(combined_and, "(x LT 65 76 65 AND x GT 62 6f 62)");
  check_ast_str(
      combined_and.negated_condition(), "(x GE 65 76 65 OR x LE 62 6f 62)");
}

TEST_CASE(
    "Query Condition: Test AST construction, basic OR combine, string",
    "[QueryCondition][ast][api]") {
  char e[] = "eve";
  QueryCondition query_condition;
  REQUIRE(query_condition.init("x", &e, strlen(e), QueryConditionOp::LT).ok());
  check_ast_str(query_condition, "x LT 65 76 65");

  char b[] = "bob";
  QueryCondition query_condition1;
  REQUIRE(query_condition1.init("x", &b, strlen(b), QueryConditionOp::GT).ok());
  check_ast_str(query_condition1, "x GT 62 6f 62");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  check_ast_str(combined_or, "(x LT 65 76 65 OR x GT 62 6f 62)");
  check_ast_str(
      combined_or.negated_condition(), "(x GE 65 76 65 AND x LE 62 6f 62)");
}

TEST_CASE(
    "Query Condition: Test AST construction, tree structure, AND of 2 OR ASTs",
    "[QueryCondition][ast][api]") {
  // First OR compound AST.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  check_ast_str(query_condition, "x LT 12 ef cd ab");

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  check_ast_str(query_condition1, "y GT 33 33 33 33");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  check_ast_str(combined_or, "(x LT 12 ef cd ab OR y GT 33 33 33 33)");

  // Second OR compound AST.
  std::string field_name2 = "a";
  int val2 = 0x12121212;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  check_ast_str(query_condition2, "a EQ 12 12 12 12");

  std::string field_name3 = "b";
  int val3 = 0x34343434;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition3, "b NE 34 34 34 34");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition2
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  check_ast_str(combined_or1, "(a EQ 12 12 12 12 OR b NE 34 34 34 34)");

  QueryCondition combined_and;
  REQUIRE(combined_or
              .combine(
                  combined_or1, QueryConditionCombinationOp::AND, &combined_and)
              .ok());
  check_ast_str(
      combined_and,
      "((x LT 12 ef cd ab OR y GT 33 33 33 33) "
      "AND (a EQ 12 12 12 12 OR b NE 34 34 34 34))");
  check_ast_str(
      combined_and.negated_condition(),
      "((x GE 12 ef cd ab AND y LE 33 33 33 33) "
      "OR (a NE 12 12 12 12 AND b EQ 34 34 34 34))");
}

TEST_CASE(
    "Query Condition: Test AST construction, tree structure, OR of 2 AND ASTs",
    "[QueryCondition][ast][api]") {
  // First AND compound AST.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  check_ast_str(query_condition, "x LT 12 ef cd ab");

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  check_ast_str(query_condition1, "y GT 33 33 33 33");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  check_ast_str(combined_and, "(x LT 12 ef cd ab AND y GT 33 33 33 33)");

  // Second AND compound AST.
  std::string field_name2 = "a";
  int val2 = 0x12121212;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  check_ast_str(query_condition2, "a EQ 12 12 12 12");

  std::string field_name3 = "b";
  int val3 = 0x34343434;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition3, "b NE 34 34 34 34");

  QueryCondition combined_and1;
  REQUIRE(query_condition2
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  check_ast_str(combined_and1, "(a EQ 12 12 12 12 AND b NE 34 34 34 34)");

  QueryCondition combined_or;
  REQUIRE(
      combined_and
          .combine(combined_and1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  check_ast_str(
      combined_or,
      "((x LT 12 ef cd ab AND y GT 33 33 33 33) "
      "OR (a EQ 12 12 12 12 AND b NE 34 34 34 34))");
  check_ast_str(
      combined_or.negated_condition(),
      "((x GE 12 ef cd ab OR y LE 33 33 33 33) "
      "AND (a NE 12 12 12 12 OR b EQ 34 34 34 34))");
}

TEST_CASE(
    "Query Condition: Test AST construction, tree structure with same "
    "combining operator, OR of 2 OR ASTs",
    "[QueryCondition][ast][api]") {
  // First OR compound AST.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  check_ast_str(query_condition, "x LT 12 ef cd ab");

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  check_ast_str(query_condition1, "y GT 33 33 33 33");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  check_ast_str(combined_or, "(x LT 12 ef cd ab OR y GT 33 33 33 33)");

  // Second OR compound AST.
  std::string field_name2 = "a";
  int val2 = 0x12121212;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  check_ast_str(query_condition2, "a EQ 12 12 12 12");

  std::string field_name3 = "b";
  int val3 = 0x34343434;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition3, "b NE 34 34 34 34");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition2
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  check_ast_str(combined_or1, "(a EQ 12 12 12 12 OR b NE 34 34 34 34)");

  QueryCondition combined_or2;
  REQUIRE(
      combined_or
          .combine(combined_or1, QueryConditionCombinationOp::OR, &combined_or2)
          .ok());
  check_ast_str(
      combined_or2,
      "(x LT 12 ef cd ab OR y GT 33 33 33 33 "
      "OR a EQ 12 12 12 12 OR b NE 34 34 34 34)");
  check_ast_str(
      combined_or2.negated_condition(),
      "(x GE 12 ef cd ab AND y LE 33 33 33 33 "
      "AND a NE 12 12 12 12 AND b EQ 34 34 34 34)");
}

TEST_CASE(
    "Query Condition: Test AST construction, tree structure with same "
    "combining operator, AND of 2 AND ASTs",
    "[QueryCondition][ast][api]") {
  // First AND compound AST.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  check_ast_str(query_condition, "x LT 12 ef cd ab");

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  check_ast_str(query_condition1, "y GT 33 33 33 33");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  check_ast_str(combined_and, "(x LT 12 ef cd ab AND y GT 33 33 33 33)");

  // Second AND compound AST.
  std::string field_name2 = "a";
  int val2 = 0x12121212;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  check_ast_str(query_condition2, "a EQ 12 12 12 12");

  std::string field_name3 = "b";
  int val3 = 0x34343434;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition3, "b NE 34 34 34 34");

  QueryCondition combined_and1;
  REQUIRE(query_condition2
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  check_ast_str(combined_and1, "(a EQ 12 12 12 12 AND b NE 34 34 34 34)");

  QueryCondition combined_and2;
  REQUIRE(
      combined_and
          .combine(
              combined_and1, QueryConditionCombinationOp::AND, &combined_and2)
          .ok());
  check_ast_str(
      combined_and2,
      "(x LT 12 ef cd ab AND y GT 33 33 33 33 "
      "AND a EQ 12 12 12 12 AND b NE 34 34 34 34)");
  check_ast_str(
      combined_and2.negated_condition(),
      "(x GE 12 ef cd ab OR y LE 33 33 33 33 "
      "OR a NE 12 12 12 12 OR b EQ 34 34 34 34)");
}

TEST_CASE(
    "Query Condition: Test AST construction, adding simple clauses to AND tree",
    "[QueryCondition][ast][api]") {
  // foo != 0xaaaaaaaa && foo != 0xbbbbbbbb && foo != 0xcccccccc && foo !=
  // 0xdddddddd && foo != 0xeeeeeeee
  std::string field_name1 = "foo";
  int val1 = 0xaaaaaaaa;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition1, "foo NE aa aa aa aa");

  std::string field_name2 = "foo";
  int val2 = 0xbbbbbbbb;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition2, "foo NE bb bb bb bb");

  std::string field_name3 = "foo";
  int val3 = 0xcccccccc;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition3, "foo NE cc cc cc cc");

  std::string field_name4 = "foo";
  int val4 = 0xdddddddd;
  QueryCondition query_condition4;
  REQUIRE(query_condition4
              .init(
                  std::string(field_name4),
                  &val4,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition4, "foo NE dd dd dd dd");

  std::string field_name5 = "foo";
  int val5 = 0xeeeeeeee;
  QueryCondition query_condition5;
  REQUIRE(query_condition5
              .init(
                  std::string(field_name5),
                  &val5,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition5, "foo NE ee ee ee ee");

  QueryCondition combined_and1;
  REQUIRE(query_condition1
              .combine(
                  query_condition2,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  check_ast_str(combined_and1, "(foo NE aa aa aa aa AND foo NE bb bb bb bb)");

  QueryCondition combined_and2;
  REQUIRE(combined_and1
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and2)
              .ok());
  check_ast_str(
      combined_and2,
      "(foo NE aa aa aa aa AND foo NE bb bb bb bb "
      "AND foo NE cc cc cc cc)");

  QueryCondition combined_and3;
  REQUIRE(combined_and2
              .combine(
                  query_condition4,
                  QueryConditionCombinationOp::AND,
                  &combined_and3)
              .ok());
  check_ast_str(
      combined_and3,
      "(foo NE aa aa aa aa AND foo NE bb bb bb bb "
      "AND foo NE cc cc cc cc AND foo NE dd dd dd dd)");

  QueryCondition combined_and4;
  REQUIRE(combined_and3
              .combine(
                  query_condition5,
                  QueryConditionCombinationOp::AND,
                  &combined_and4)
              .ok());
  check_ast_str(
      combined_and4,
      "(foo NE aa aa aa aa AND foo NE bb bb bb bb "
      "AND foo NE cc cc cc cc AND foo NE dd dd dd dd AND foo NE ee ee ee ee)");
  check_ast_str(
      combined_and4.negated_condition(),
      "(foo EQ aa aa aa aa "
      "OR foo EQ bb bb bb bb OR foo EQ cc cc cc cc OR foo EQ dd dd dd dd "
      "OR foo EQ ee ee ee ee)");
}

TEST_CASE(
    "Query Condition: Test AST construction, adding simple clauses to OR tree",
    "[QueryCondition][ast][api]") {
  // foo != 0xaaaaaaaa OR foo != 0xbbbbbbbb OR foo != 0xcccccccc OR foo !=
  // 0xdddddddd OR foo != 0xeeeeeeee
  std::string field_name1 = "foo";
  int val1 = 0xaaaaaaaa;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition1, "foo NE aa aa aa aa");

  std::string field_name2 = "foo";
  int val2 = 0xbbbbbbbb;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition2, "foo NE bb bb bb bb");

  std::string field_name3 = "foo";
  int val3 = 0xcccccccc;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition3, "foo NE cc cc cc cc");

  std::string field_name4 = "foo";
  int val4 = 0xdddddddd;
  QueryCondition query_condition4;
  REQUIRE(query_condition4
              .init(
                  std::string(field_name4),
                  &val4,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition4, "foo NE dd dd dd dd");

  std::string field_name5 = "foo";
  int val5 = 0xeeeeeeee;
  QueryCondition query_condition5;
  REQUIRE(query_condition5
              .init(
                  std::string(field_name5),
                  &val5,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  check_ast_str(query_condition5, "foo NE ee ee ee ee");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition1
          .combine(
              query_condition2, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  check_ast_str(combined_or1, "(foo NE aa aa aa aa OR foo NE bb bb bb bb)");

  QueryCondition combined_or2;
  REQUIRE(
      combined_or1
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or2)
          .ok());
  check_ast_str(
      combined_or2,
      "(foo NE aa aa aa aa OR foo NE bb bb bb bb "
      "OR foo NE cc cc cc cc)");

  QueryCondition combined_or3;
  REQUIRE(
      combined_or2
          .combine(
              query_condition4, QueryConditionCombinationOp::OR, &combined_or3)
          .ok());
  check_ast_str(
      combined_or3,
      "(foo NE aa aa aa aa OR foo NE bb bb bb bb "
      "OR foo NE cc cc cc cc OR foo NE dd dd dd dd)");

  QueryCondition combined_or4;
  REQUIRE(
      combined_or3
          .combine(
              query_condition5, QueryConditionCombinationOp::OR, &combined_or4)
          .ok());
  check_ast_str(
      combined_or4,
      "(foo NE aa aa aa aa OR foo NE bb bb bb bb "
      "OR foo NE cc cc cc cc OR foo NE dd dd dd dd OR foo NE ee ee ee ee)");
  check_ast_str(
      combined_or4.negated_condition(),
      "(foo EQ aa aa aa aa "
      "AND foo EQ bb bb bb bb AND foo EQ cc cc cc cc "
      "AND foo EQ dd dd dd dd AND foo EQ ee ee ee ee)");
}

TEST_CASE(
    "Query Condition: Test AST construction, complex tree with depth > 2",
    "[QueryCondition][ast][api]") {
  std::vector<int> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<QueryCondition> qc_value_vector;
  for (int i = 0; i < 7; ++i) {
    QueryCondition qc;
    REQUIRE(qc.init("x", &vals[i], sizeof(vals[i]), QueryConditionOp::EQ).ok());
    check_ast_str(qc, "x EQ 0" + std::to_string(vals[i]) + " 00 00 00");
    qc_value_vector.push_back(qc);
  }

  for (int i = 7; i < 9; ++i) {
    QueryCondition qc;
    REQUIRE(qc.init("x", &vals[i], sizeof(vals[i]), QueryConditionOp::NE).ok());
    check_ast_str(qc, "x NE 0" + std::to_string(vals[i]) + " 00 00 00");
    qc_value_vector.push_back(qc);
  }

  int x = 6;
  QueryCondition x_neq_six;
  REQUIRE(x_neq_six.init("x", &x, sizeof(x), QueryConditionOp::NE).ok());
  check_ast_str(x_neq_six, "x NE 06 00 00 00");

  QueryCondition one_or_two;
  REQUIRE(
      qc_value_vector[0]
          .combine(
              qc_value_vector[1], QueryConditionCombinationOp::OR, &one_or_two)
          .ok());
  check_ast_str(one_or_two, "(x EQ 01 00 00 00 OR x EQ 02 00 00 00)");

  QueryCondition three_or_four;
  REQUIRE(qc_value_vector[2]
              .combine(
                  qc_value_vector[3],
                  QueryConditionCombinationOp::OR,
                  &three_or_four)
              .ok());
  check_ast_str(three_or_four, "(x EQ 03 00 00 00 OR x EQ 04 00 00 00)");

  QueryCondition six_or_seven;
  REQUIRE(qc_value_vector[5]
              .combine(
                  qc_value_vector[6],
                  QueryConditionCombinationOp::OR,
                  &six_or_seven)
              .ok());
  check_ast_str(six_or_seven, "(x EQ 06 00 00 00 OR x EQ 07 00 00 00)");

  QueryCondition eight_and_nine;
  REQUIRE(qc_value_vector[7]
              .combine(
                  qc_value_vector[8],
                  QueryConditionCombinationOp::AND,
                  &eight_and_nine)
              .ok());
  check_ast_str(eight_and_nine, "(x NE 08 00 00 00 AND x NE 09 00 00 00)");

  QueryCondition subtree_a;
  REQUIRE(
      one_or_two
          .combine(three_or_four, QueryConditionCombinationOp::AND, &subtree_a)
          .ok());
  check_ast_str(
      subtree_a,
      "((x EQ 01 00 00 00 OR x EQ 02 00 00 00) "
      "AND (x EQ 03 00 00 00 OR x EQ 04 00 00 00))");

  QueryCondition subtree_d;
  REQUIRE(
      eight_and_nine
          .combine(six_or_seven, QueryConditionCombinationOp::AND, &subtree_d)
          .ok());
  check_ast_str(
      subtree_d,
      "(x NE 08 00 00 00 AND x NE 09 00 00 00 "
      "AND (x EQ 06 00 00 00 OR x EQ 07 00 00 00))");

  QueryCondition subtree_c;
  REQUIRE(
      subtree_d
          .combine(
              qc_value_vector[4], QueryConditionCombinationOp::OR, &subtree_c)
          .ok());
  check_ast_str(
      subtree_c,
      "((x NE 08 00 00 00 AND x NE 09 00 00 00 "
      "AND (x EQ 06 00 00 00 OR x EQ 07 00 00 00)) OR x EQ 05 00 00 00)");

  QueryCondition subtree_b;
  REQUIRE(
      subtree_c.combine(x_neq_six, QueryConditionCombinationOp::AND, &subtree_b)
          .ok());
  check_ast_str(
      subtree_b,
      "(((x NE 08 00 00 00 AND x NE 09 00 00 00 "
      "AND (x EQ 06 00 00 00 OR x EQ 07 00 00 00)) OR x EQ 05 00 00 00) "
      "AND x NE 06 00 00 00)");

  QueryCondition qc;
  REQUIRE(
      subtree_a.combine(subtree_b, QueryConditionCombinationOp::OR, &qc).ok());
  check_ast_str(
      qc,
      "(((x EQ 01 00 00 00 OR x EQ 02 00 00 00) "
      "AND (x EQ 03 00 00 00 OR x EQ 04 00 00 00)) "
      "OR (((x NE 08 00 00 00 AND x NE 09 00 00 00 "
      "AND (x EQ 06 00 00 00 OR x EQ 07 00 00 00)) "
      "OR x EQ 05 00 00 00) AND x NE 06 00 00 00))");
  check_ast_str(
      qc.negated_condition(),
      "(((x NE 01 00 00 00 AND x NE 02 00 00 00) "
      "OR (x NE 03 00 00 00 AND x NE 04 00 00 00)) "
      "AND (((x EQ 08 00 00 00 OR x EQ 09 00 00 00 "
      "OR (x NE 06 00 00 00 AND x NE 07 00 00 00)) "
      "AND x NE 05 00 00 00) OR x EQ 06 00 00 00))");
}

/**
 * Tests a comparison operator on all cells in a tile.
 *
 * @param op The relational query condition operator.
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_cells(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values);

/**
 * C-string template-specialization for `test_apply_cells`.
 */
template <>
void test_apply_cells<char*>(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const char* const cmp_value = "ae";
  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(std::string(field_name), cmp_value, 2 * sizeof(char), op)
              .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  bool nullable = array_schema->attribute(field_name)->nullable();

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    if (nullable && (i % 2 == 0))
      continue;

    switch (op) {
      case QueryConditionOp::LT:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) <
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) <=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) >
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) >=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) ==
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) !=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  ResultCellSlab result_cell_slab(result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));
  std::vector<shared_ptr<FragmentMetadata>> frag_md(1);
  frag_md[0] = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(query_condition.apply(params, frag_md, result_cell_slabs, 1).ok());

  // Verify the result cell slabs contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (const auto& result_cell_slab : result_cell_slabs) {
    for (uint64_t cell_idx = result_cell_slab.start_;
         cell_idx < (result_cell_slab.start_ + result_cell_slab.length_);
         ++cell_idx) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  if (nullable) {
    if (op == QueryConditionOp::EQ || op == QueryConditionOp::NE) {
      const uint64_t eq = op == QueryConditionOp::EQ ? 0 : 1;
      QueryCondition query_condition_eq_null;
      REQUIRE(
          query_condition_eq_null.init(std::string(field_name), nullptr, 0, op)
              .ok());
      // Run Check for query_condition_eq_null
      REQUIRE(query_condition_eq_null.check(*array_schema).ok());

      ResultCellSlab result_cell_slab_eq_null(result_tile, 0, cells);
      std::vector<ResultCellSlab> result_cell_slabs_eq_null;
      result_cell_slabs_eq_null.emplace_back(
          std::move(result_cell_slab_eq_null));
      std::vector<shared_ptr<FragmentMetadata>> frag_md(1);
      frag_md[0] = make_shared<FragmentMetadata>(
          HERE(),
          nullptr,
          array_schema,
          URI(tiledb::storage_format::generate_timestamped_name(
              tiledb::sm::utils::time::timestamp_now_ms(),
              constants::format_version)),
          std::make_pair<uint64_t, uint64_t>(0, 0),
          memory_tracker,
          true);
      QueryCondition::Params params(memory_tracker, *array_schema);
      REQUIRE(query_condition_eq_null
                  .apply(params, frag_md, result_cell_slabs_eq_null, 1)
                  .ok());

      REQUIRE(result_cell_slabs_eq_null.size() == (cells / 2));
      for (const auto& result_cell_slab : result_cell_slabs_eq_null) {
        REQUIRE((result_cell_slab.start_ % 2) == eq);
        REQUIRE(result_cell_slab.length_ == 1);
      }
    }

    return;
  }

  // Fetch the fill value.
  const void* fill_value{nullptr};
  uint64_t fill_value_size{0};
  array_schema->attribute(field_name)
      ->get_fill_value(&fill_value, &fill_value_size);
  REQUIRE(fill_value_size == 2 * sizeof(char));

  // Build expected indexes of cells that meet the query condition
  // criteria with the fill value;
  std::vector<uint64_t> fill_expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::LT:
        if (std::string(static_cast<const char*>(fill_value), 2) <
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (std::string(static_cast<const char*>(fill_value), 2) <=
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (std::string(static_cast<const char*>(fill_value), 2) >
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (std::string(static_cast<const char*>(fill_value), 2) >=
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (std::string(static_cast<const char*>(fill_value), 2) ==
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (std::string(static_cast<const char*>(fill_value), 2) !=
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition with an empty result tile, which will
  // use the fill value.
  ResultCellSlab fill_result_cell_slab(nullptr, 0, cells);
  std::vector<ResultCellSlab> fill_result_cell_slabs;
  fill_result_cell_slabs.emplace_back(std::move(fill_result_cell_slab));
  REQUIRE(
      query_condition.apply(params, frag_md, fill_result_cell_slabs, 1).ok());

  // Verify the fill result cell slabs contain the expected cells.
  auto fill_expected_iter = fill_expected_cell_idx_vec.begin();
  for (const auto& fill_result_cell_slab : fill_result_cell_slabs) {
    for (uint64_t cell_idx = fill_result_cell_slab.start_;
         cell_idx <
         (fill_result_cell_slab.start_ + fill_result_cell_slab.length_);
         ++cell_idx) {
      REQUIRE(*fill_expected_iter == cell_idx);
      ++fill_expected_iter;
    }
  }
}

/**
 * Non-specialized template type for `test_apply_cells`.
 */
template <typename T>
void test_apply_cells(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const T cmp_value = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition.init(std::string(field_name), &cmp_value, sizeof(T), op)
          .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::LT:
        if (static_cast<T*>(values)[i] < cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (static_cast<T*>(values)[i] <= cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (static_cast<T*>(values)[i] > cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (static_cast<T*>(values)[i] >= cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (static_cast<T*>(values)[i] == cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (static_cast<T*>(values)[i] != cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  ResultCellSlab result_cell_slab(result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));
  std::vector<shared_ptr<FragmentMetadata>> frag_md(1);
  frag_md[0] = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(query_condition.apply(params, frag_md, result_cell_slabs, 1).ok());

  // Verify the result cell slabs contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (const auto& rcs : result_cell_slabs) {
    for (uint64_t cell_idx = rcs.start_; cell_idx < (rcs.start_ + rcs.length_);
         ++cell_idx) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  // Fetch the fill value.
  const void* fill_value{nullptr};
  uint64_t fill_value_size{0};
  array_schema->attribute(field_name)
      ->get_fill_value(&fill_value, &fill_value_size);
  REQUIRE(fill_value_size == sizeof(T));

  // Build expected indexes of cells that meet the query condition
  // criteria with the fill value;
  std::vector<uint64_t> fill_expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::LT:
        if (*static_cast<const T*>(fill_value) < cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (*static_cast<const T*>(fill_value) <= cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (*static_cast<const T*>(fill_value) > cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (*static_cast<const T*>(fill_value) >= cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (*static_cast<const T*>(fill_value) == cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (*static_cast<const T*>(fill_value) != cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition with an empty result tile, which will
  // use the fill value.
  ResultCellSlab fill_result_cell_slab(nullptr, 0, cells);
  std::vector<ResultCellSlab> fill_result_cell_slabs;
  fill_result_cell_slabs.emplace_back(std::move(fill_result_cell_slab));
  REQUIRE(
      query_condition.apply(params, frag_md, fill_result_cell_slabs, 1).ok());

  // Verify the fill result cell slabs contain the expected cells.
  auto fill_expected_iter = fill_expected_cell_idx_vec.begin();
  for (const auto& fill_result_cell_slab : fill_result_cell_slabs) {
    for (uint64_t cell_idx = fill_result_cell_slab.start_;
         cell_idx <
         (fill_result_cell_slab.start_ + fill_result_cell_slab.length_);
         ++cell_idx) {
      REQUIRE(*fill_expected_iter == cell_idx);
      ++fill_expected_iter;
    }
  }
}

/**
 * Tests each comparison operator on all cells in a tile.
 *
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_operators(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  test_apply_cells<T>(
      QueryConditionOp::LT,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::LE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::GT,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::GE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::EQ,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::NE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
}

/**
 * Populates a tile and tests query condition comparisons against
 * each cell.
 *
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 */
template <typename T>
void test_apply_tile(
    const std::string& field_name,
    uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* result_tile);

/**
 * C-string template-specialization for `test_apply_tile`.
 */
template <>
void test_apply_tile<char*>(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);

  bool var_size = array_schema->attribute(field_name)->var_size();
  bool nullable = array_schema->attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &tile_tuple->var_tile() : &tile_tuple->fixed_tile();

  std::vector<char> values(2 * cells);
  for (uint64_t i = 0; i < cells; ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }
  REQUIRE_NOTHROW(tile->write(values.data(), 0, 2 * cells * sizeof(char)));

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells + 1);
    uint64_t offset = 0;
    for (uint64_t i = 0; i <= cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE_NOTHROW(
        tile_offsets->write(offsets.data(), 0, (cells + 1) * sizeof(uint64_t)));
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE_NOTHROW(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)));
  }

  test_apply_operators<char*>(
      field_name,
      cells,
      array_schema,
      result_tile,
      static_cast<void*>(values.data()));
}

/**
 * Non-specialized template type for `test_apply_tile`.
 */
template <typename T>
void test_apply_tile(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);
  Tile* const tile = &tile_tuple->fixed_tile();
  std::vector<T> values(cells);
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = static_cast<T>(i);
  }
  REQUIRE_NOTHROW(tile->write(values.data(), 0, cells * sizeof(T)));

  test_apply_operators<T>(
      field_name,
      cells,
      array_schema,
      result_tile,
      static_cast<void*>(values.data()));
}

/**
 * Constructs a tile and tests query condition comparisons against
 * each cell.
 *
 * @param type The TILEDB data type of the attribute.
 * @param var_size Run the test with variable size attribute.
 * @param nullable Run the test with nullable attribute.
 */
template <typename T>
void test_apply(
    const Datatype type, bool var_size = false, bool nullable = false);

/**
 * C-string template-specialization for `test_apply`.
 */
template <>
void test_apply<char*>(const Datatype type, bool var_size, bool nullable) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  REQUIRE((type == Datatype::STRING_ASCII || type == Datatype::STRING_UTF8));

  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(nullable);
  attr.set_cell_val_num(var_size ? constants::var_num : 2);

  if (!nullable) {
    attr.set_fill_value(fill_value, 2 * sizeof(char));
  }

  array_schema->add_attribute(make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{
      make_shared<Dimension>(HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileSizes tile_sizes(
      var_size ? (cells + 1) * constants::cell_var_offset_size :
                 2 * cells * sizeof(char),
      0,
      var_size ? std::optional(2 * cells * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  test_apply_tile<char*>(field_name, cells, array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply`.
 */
template <typename T>
void test_apply(const Datatype type, bool var_size, bool nullable) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_cell_val_num(1);
  attr.set_fill_value(&fill_value, sizeof(T));
  array_schema->add_attribute(make_shared<Attribute>(HERE(), attr));

  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{
      make_shared<Dimension>(HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(T),
      0,
      var_size ? std::optional(0) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  test_apply_tile<T>(field_name, cells, array_schema, &result_tile);
}

TEST_CASE("QueryCondition: Test apply", "[QueryCondition][apply]") {
  test_apply<int8_t>(Datatype::INT8);
  test_apply<uint8_t>(Datatype::UINT8);
  test_apply<int16_t>(Datatype::INT16);
  test_apply<uint16_t>(Datatype::UINT16);
  test_apply<int32_t>(Datatype::INT32);
  test_apply<uint32_t>(Datatype::UINT32);
  test_apply<int64_t>(Datatype::INT64);
  test_apply<uint64_t>(Datatype::UINT64);
  test_apply<float>(Datatype::FLOAT32);
  test_apply<double>(Datatype::FLOAT64);
  test_apply<char>(Datatype::CHAR);
  test_apply<int64_t>(Datatype::DATETIME_YEAR);
  test_apply<int64_t>(Datatype::DATETIME_MONTH);
  test_apply<int64_t>(Datatype::DATETIME_WEEK);
  test_apply<int64_t>(Datatype::DATETIME_DAY);
  test_apply<int64_t>(Datatype::DATETIME_HR);
  test_apply<int64_t>(Datatype::DATETIME_MIN);
  test_apply<int64_t>(Datatype::DATETIME_SEC);
  test_apply<int64_t>(Datatype::DATETIME_MS);
  test_apply<int64_t>(Datatype::DATETIME_US);
  test_apply<int64_t>(Datatype::DATETIME_NS);
  test_apply<int64_t>(Datatype::DATETIME_PS);
  test_apply<int64_t>(Datatype::DATETIME_FS);
  test_apply<int64_t>(Datatype::DATETIME_AS);
  test_apply<int64_t>(Datatype::TIME_HR);
  test_apply<int64_t>(Datatype::TIME_MIN);
  test_apply<int64_t>(Datatype::TIME_SEC);
  test_apply<int64_t>(Datatype::TIME_MS);
  test_apply<int64_t>(Datatype::TIME_US);
  test_apply<int64_t>(Datatype::TIME_NS);
  test_apply<int64_t>(Datatype::TIME_PS);
  test_apply<int64_t>(Datatype::TIME_FS);
  test_apply<int64_t>(Datatype::TIME_AS);
  test_apply<char*>(Datatype::STRING_ASCII);
  test_apply<char*>(Datatype::STRING_ASCII, true);
  test_apply<char*>(Datatype::STRING_ASCII, false, true);
  test_apply<char*>(Datatype::STRING_UTF8);
  test_apply<char*>(Datatype::STRING_UTF8, true);
  test_apply<char*>(Datatype::STRING_UTF8, false, true);
}

TEST_CASE(
    "QueryCondition: Test empty/null strings",
    "[QueryCondition][empty_string][null_string]") {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";
  const Datatype type = GENERATE(Datatype::STRING_ASCII, Datatype::STRING_UTF8);
  bool var_size = true;
  bool nullable = GENERATE(true, false);
  bool null_cmp = GENERATE(true, false);
  QueryConditionOp op = GENERATE(QueryConditionOp::NE, QueryConditionOp::EQ);

  if (!nullable && null_cmp)
    return;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(nullable);
  attr.set_cell_val_num(var_size ? constants::var_num : 2);

  if (!nullable) {
    attr.set_fill_value(fill_value, 2 * sizeof(char));
  }

  array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  std::vector<shared_ptr<FragmentMetadata>> frag_md(1);
  frag_md[0] = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? (cells + 1) * constants::cell_var_offset_size :
                 2 * (cells - 2) * sizeof(char),
      0,
      var_size ? std::optional(2 * (cells - 2) * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *frag_md[0], memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);

  var_size = array_schema->attribute(field_name)->var_size();
  nullable = array_schema->attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &tile_tuple->var_tile() : &tile_tuple->fixed_tile();
  std::vector<char> values(2 * (cells - 2));

  // Empty strings are at idx 8 and 9
  for (uint64_t i = 0; i < (cells - 2); ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }

  REQUIRE_NOTHROW(
      tile->write(values.data(), 0, 2 * (cells - 2) * sizeof(char)));

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells + 1);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    offsets[cells] = offset;
    REQUIRE_NOTHROW(
        tile_offsets->write(offsets.data(), 0, (cells + 1) * sizeof(uint64_t)));
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE_NOTHROW(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)));
  }

  // Empty string or null string as condition value
  const char* cmp_value = null_cmp ? nullptr : "";

  QueryCondition query_condition;
  REQUIRE(query_condition.init(std::string(field_name), cmp_value, 0, op).ok());

  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::EQ:
        if (null_cmp) {
          if (i % 2 == 0)
            expected_cell_idx_vec.emplace_back(i);
        } else if (nullable) {
          if ((i % 2 != 0) && (i >= 8))
            expected_cell_idx_vec.emplace_back(i);
        } else if (i >= 8) {
          expected_cell_idx_vec.emplace_back(i);
        }
        break;
      case QueryConditionOp::NE:
        if (null_cmp) {
          if (i % 2 != 0)
            expected_cell_idx_vec.emplace_back(i);
        } else if (nullable) {
          if ((i % 2 != 0) && (i < 8))
            expected_cell_idx_vec.emplace_back(i);
        } else if (i < 8) {
          expected_cell_idx_vec.emplace_back(i);
        }
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  ResultCellSlab result_cell_slab(&result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(query_condition.apply(params, frag_md, result_cell_slabs, 1).ok());

  // Verify the result cell slabs contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (const auto& result_cell_slab : result_cell_slabs) {
    for (uint64_t cell_idx = result_cell_slab.start_;
         cell_idx < (result_cell_slab.start_ + result_cell_slab.length_);
         ++cell_idx) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }
}

/**
 * Tests a comparison operator on all cells in a tile.
 *
 * @param op The relational query condition operator.
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_cells_dense(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values);

/**
 * C-string template-specialization for `test_apply_cells_dense`.
 */
template <>
void test_apply_cells_dense<char*>(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const char* const cmp_value = "ae";
  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(std::string(field_name), cmp_value, 2 * sizeof(char), op)
              .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  bool nullable = array_schema->attribute(field_name)->nullable();

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    if (nullable && (i % 2 == 0))
      continue;

    switch (op) {
      case QueryConditionOp::LT:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) <
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) <=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) >
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) >=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) ==
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) !=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  std::vector<uint8_t> result_bitmap(cells, 1);
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(
      query_condition
          .apply_dense(
              params, result_tile, 0, 10, 0, 1, nullptr, result_bitmap.data())
          .ok());

  // Verify the result bitmap contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
    if (result_bitmap[cell_idx]) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  if (nullable) {
    if (op == QueryConditionOp::EQ || op == QueryConditionOp::NE) {
      const uint64_t eq = op == QueryConditionOp::EQ ? 0 : 1;
      QueryCondition query_condition_eq_null;
      REQUIRE(
          query_condition_eq_null.init(std::string(field_name), nullptr, 0, op)
              .ok());
      // Run Check for query_condition_eq_null
      REQUIRE(query_condition_eq_null.check(*array_schema).ok());

      // Apply the query condition.
      std::vector<uint8_t> result_bitmap_eq_null(cells, 1);
      QueryCondition::Params params(memory_tracker, *array_schema);
      REQUIRE(query_condition_eq_null
                  .apply_dense(
                      params,
                      result_tile,
                      0,
                      10,
                      0,
                      1,
                      nullptr,
                      result_bitmap_eq_null.data())
                  .ok());

      // Verify the result bitmap contain the expected cells.
      for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
        REQUIRE(result_bitmap_eq_null[cell_idx] == (cell_idx + eq + 1) % 2);
      }
    }

    return;
  }
}

/**
 * Non-specialized template type for `test_apply_cells_dense`.
 */
template <typename T>
void test_apply_cells_dense(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const T cmp_value = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition.init(std::string(field_name), &cmp_value, sizeof(T), op)
          .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::LT:
        if (static_cast<T*>(values)[i] < cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (static_cast<T*>(values)[i] <= cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (static_cast<T*>(values)[i] > cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (static_cast<T*>(values)[i] >= cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (static_cast<T*>(values)[i] == cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (static_cast<T*>(values)[i] != cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  std::vector<uint8_t> result_bitmap(cells, 1);
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(
      query_condition
          .apply_dense(
              params, result_tile, 0, 10, 0, 1, nullptr, result_bitmap.data())
          .ok());

  // Verify the result bitmap contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
    if (result_bitmap[cell_idx]) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }
}

/**
 * Tests each comparison operator on all cells in a tile.
 *
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_operators_dense(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  test_apply_cells_dense<T>(
      QueryConditionOp::LT,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_dense<T>(
      QueryConditionOp::LE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_dense<T>(
      QueryConditionOp::GT,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_dense<T>(
      QueryConditionOp::GE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_dense<T>(
      QueryConditionOp::EQ,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_dense<T>(
      QueryConditionOp::NE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
}

/**
 * Populates a tile and tests query condition comparisons against
 * each cell.
 *
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 */
template <typename T>
void test_apply_tile_dense(
    const std::string& field_name,
    uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* result_tile);

/**
 * C-string template-specialization for `test_apply_tile_dense`.
 */
template <>
void test_apply_tile_dense<char*>(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);

  bool var_size = array_schema->attribute(field_name)->var_size();
  bool nullable = array_schema->attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &tile_tuple->var_tile() : &tile_tuple->fixed_tile();
  std::vector<char> values(2 * cells);
  for (uint64_t i = 0; i < cells; ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }
  REQUIRE_NOTHROW(tile->write(values.data(), 0, 2 * cells * sizeof(char)));

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells + 1);
    uint64_t offset = 0;
    for (uint64_t i = 0; i <= cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE_NOTHROW(
        tile_offsets->write(offsets.data(), 0, (cells + 1) * sizeof(uint64_t)));
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE_NOTHROW(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)));
  }

  test_apply_operators_dense<char*>(
      field_name,
      cells,
      array_schema,
      result_tile,
      static_cast<void*>(values.data()));
}

/**
 * Non-specialized template type for `test_apply_tile_dense`.
 */
template <typename T>
void test_apply_tile_dense(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);
  Tile* const tile = &tile_tuple->fixed_tile();
  std::vector<T> values(cells);
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = static_cast<T>(i);
  }
  REQUIRE_NOTHROW(tile->write(values.data(), 0, cells * sizeof(T)));

  test_apply_operators_dense<T>(
      field_name,
      cells,
      array_schema,
      result_tile,
      static_cast<void*>(values.data()));
}

/**
 * Constructs a tile and tests query condition comparisons against
 * each cell.
 *
 * @param type The TILEDB data type of the attribute.
 * @param var_size Run the test with variable size attribute.
 * @param nullable Run the test with nullable attribute.
 */
template <typename T>
void test_apply_dense(
    const Datatype type, bool var_size = false, bool nullable = false);

/**
 * C-string template-specialization for `test_apply_dense`.
 */
template <>
void test_apply_dense<char*>(
    const Datatype type, bool var_size, bool nullable) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  REQUIRE((type == Datatype::STRING_ASCII || type == Datatype::STRING_UTF8));

  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(nullable);
  attr.set_cell_val_num(var_size ? constants::var_num : 2);

  if (!nullable) {
    attr.set_fill_value(fill_value, 2 * sizeof(char));
  }

  array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? (cells + 1) * constants::cell_var_offset_size :
                 2 * cells * sizeof(char),
      0,
      var_size ? std::optional(2 * cells * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  test_apply_tile_dense<char*>(field_name, cells, array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply_dense`.
 */
template <typename T>
void test_apply_dense(const Datatype type, bool var_size, bool nullable) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_cell_val_num(1);
  attr.set_fill_value(&fill_value, sizeof(T));
  array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);
  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(T),
      0,
      var_size ? std::optional(0) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  test_apply_tile_dense<T>(field_name, cells, array_schema, &result_tile);
}

TEST_CASE(
    "QueryCondition: Test apply dense", "[QueryCondition][apply][dense]") {
  test_apply_dense<int8_t>(Datatype::INT8);
  test_apply_dense<uint8_t>(Datatype::UINT8);
  test_apply_dense<int16_t>(Datatype::INT16);
  test_apply_dense<uint16_t>(Datatype::UINT16);
  test_apply_dense<int32_t>(Datatype::INT32);
  test_apply_dense<uint32_t>(Datatype::UINT32);
  test_apply_dense<int64_t>(Datatype::INT64);
  test_apply_dense<uint64_t>(Datatype::UINT64);
  test_apply_dense<float>(Datatype::FLOAT32);
  test_apply_dense<double>(Datatype::FLOAT64);
  test_apply_dense<char>(Datatype::CHAR);
  test_apply_dense<int64_t>(Datatype::DATETIME_YEAR);
  test_apply_dense<int64_t>(Datatype::DATETIME_MONTH);
  test_apply_dense<int64_t>(Datatype::DATETIME_WEEK);
  test_apply_dense<int64_t>(Datatype::DATETIME_DAY);
  test_apply_dense<int64_t>(Datatype::DATETIME_HR);
  test_apply_dense<int64_t>(Datatype::DATETIME_MIN);
  test_apply_dense<int64_t>(Datatype::DATETIME_SEC);
  test_apply_dense<int64_t>(Datatype::DATETIME_MS);
  test_apply_dense<int64_t>(Datatype::DATETIME_US);
  test_apply_dense<int64_t>(Datatype::DATETIME_NS);
  test_apply_dense<int64_t>(Datatype::DATETIME_PS);
  test_apply_dense<int64_t>(Datatype::DATETIME_FS);
  test_apply_dense<int64_t>(Datatype::DATETIME_AS);
  test_apply_dense<int64_t>(Datatype::TIME_HR);
  test_apply_dense<int64_t>(Datatype::TIME_MIN);
  test_apply_dense<int64_t>(Datatype::TIME_SEC);
  test_apply_dense<int64_t>(Datatype::TIME_MS);
  test_apply_dense<int64_t>(Datatype::TIME_US);
  test_apply_dense<int64_t>(Datatype::TIME_NS);
  test_apply_dense<int64_t>(Datatype::TIME_PS);
  test_apply_dense<int64_t>(Datatype::TIME_FS);
  test_apply_dense<int64_t>(Datatype::TIME_AS);
  test_apply_dense<char*>(Datatype::STRING_ASCII);
  test_apply_dense<char*>(Datatype::STRING_ASCII, true);
  test_apply_dense<char*>(Datatype::STRING_ASCII, false, true);
  test_apply_dense<char*>(Datatype::STRING_UTF8);
  test_apply_dense<char*>(Datatype::STRING_UTF8, true);
  test_apply_dense<char*>(Datatype::STRING_UTF8, false, true);
}

TEST_CASE(
    "QueryCondition: Test empty/null strings dense",
    "[QueryCondition][empty_string][null_string][dense]") {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";
  const Datatype type = GENERATE(Datatype::STRING_ASCII, Datatype::STRING_UTF8);
  bool var_size = true;
  bool nullable = GENERATE(true, false);
  bool null_cmp = GENERATE(true, false);
  QueryConditionOp op = GENERATE(QueryConditionOp::NE, QueryConditionOp::EQ);

  if (!nullable && null_cmp)
    return;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(nullable);
  attr.set_cell_val_num(var_size ? constants::var_num : 2);

  if (!nullable) {
    attr.set_fill_value(fill_value, 2 * sizeof(char));
  }

  array_schema->add_attribute(make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? (cells + 1) * constants::cell_var_offset_size :
                 2 * (cells - 2) * sizeof(char),
      0,
      var_size ? std::optional(2 * (cells - 2) * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);

  var_size = array_schema->attribute(field_name)->var_size();
  nullable = array_schema->attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &tile_tuple->var_tile() : &tile_tuple->fixed_tile();
  std::vector<char> values(2 * (cells - 2));
  // Empty strings are at idx 8 and 9
  for (uint64_t i = 0; i < (cells - 2); ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }

  REQUIRE_NOTHROW(
      tile->write(values.data(), 0, 2 * (cells - 2) * sizeof(char)));

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells + 1);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    offsets[cells] = offset;
    REQUIRE_NOTHROW(
        tile_offsets->write(offsets.data(), 0, (cells + 1) * sizeof(uint64_t)));
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE_NOTHROW(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)));
  }

  // Empty string or null string as condition value
  const char* cmp_value = null_cmp ? nullptr : "";

  QueryCondition query_condition;
  REQUIRE(query_condition.init(std::string(field_name), cmp_value, 0, op).ok());

  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::EQ:
        if (null_cmp) {
          if (i % 2 == 0)
            expected_cell_idx_vec.emplace_back(i);
        } else if (nullable) {
          if ((i % 2 != 0) && (i >= 8))
            expected_cell_idx_vec.emplace_back(i);
        } else if (i >= 8) {
          expected_cell_idx_vec.emplace_back(i);
        }
        break;
      case QueryConditionOp::NE:
        if (null_cmp) {
          if (i % 2 != 0)
            expected_cell_idx_vec.emplace_back(i);
        } else if (nullable) {
          if ((i % 2 != 0) && (i < 8))
            expected_cell_idx_vec.emplace_back(i);
        } else if (i < 8) {
          expected_cell_idx_vec.emplace_back(i);
        }
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  std::vector<uint8_t> result_bitmap(cells, 1);
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(
      query_condition
          .apply_dense(
              params, &result_tile, 0, 10, 0, 1, nullptr, result_bitmap.data())
          .ok());

  // Verify the result bitmap contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
    if (result_bitmap[cell_idx]) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }
}

/**
 * Tests a comparison operator on all cells in a tile.
 *
 * @param op The relational query condition operator.
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_cells_sparse(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values);

/**
 * C-string template-specialization for `test_apply_cells_sparse`.
 */
template <>
void test_apply_cells_sparse<char*>(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const char* const cmp_value = "ae";
  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(std::string(field_name), cmp_value, 2 * sizeof(char), op)
              .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  bool nullable = array_schema->attribute(field_name)->nullable();

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    if (nullable && (i % 2 == 0))
      continue;

    switch (op) {
      case QueryConditionOp::LT:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) <
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) <=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) >
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) >=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) ==
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) !=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  QueryCondition::Params params(memory_tracker, *array_schema);
  auto resource = memory_tracker->get_resource(MemoryType::RESULT_TILE_BITMAP);
  tdb::pmr::vector<uint8_t> result_bitmap(cells, 1, resource);
  REQUIRE(
      query_condition.apply_sparse<uint8_t>(params, *result_tile, result_bitmap)
          .ok());

  // Verify the result bitmap contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
    if (result_bitmap[cell_idx]) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  if (nullable) {
    if (op == QueryConditionOp::EQ || op == QueryConditionOp::NE) {
      const uint64_t eq = op == QueryConditionOp::EQ ? 0 : 1;
      QueryCondition query_condition_eq_null;
      REQUIRE(
          query_condition_eq_null.init(std::string(field_name), nullptr, 0, op)
              .ok());
      // Run Check for query_condition_eq_null
      REQUIRE(query_condition_eq_null.check(*array_schema).ok());

      // Apply the query condition.
      QueryCondition::Params params(memory_tracker, *array_schema);
      auto resource =
          memory_tracker->get_resource(MemoryType::RESULT_TILE_BITMAP);
      tdb::pmr::vector<uint8_t> result_bitmap_eq_null(cells, 1, resource);
      REQUIRE(query_condition_eq_null
                  .apply_sparse<uint8_t>(
                      params, *result_tile, result_bitmap_eq_null)
                  .ok());

      // Verify the result bitmap contain the expected cells.
      for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
        REQUIRE(result_bitmap_eq_null[cell_idx] == (cell_idx + eq + 1) % 2);
      }
    }

    return;
  }
}

/**
 * Non-specialized template type for `test_apply_cells_sparse`.
 */
template <typename T>
void test_apply_cells_sparse(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const T cmp_value = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition.init(std::string(field_name), &cmp_value, sizeof(T), op)
          .ok());
  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::LT:
        if (static_cast<T*>(values)[i] < cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (static_cast<T*>(values)[i] <= cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (static_cast<T*>(values)[i] > cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (static_cast<T*>(values)[i] >= cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (static_cast<T*>(values)[i] == cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (static_cast<T*>(values)[i] != cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  QueryCondition::Params params(memory_tracker, *array_schema);
  auto resource = memory_tracker->get_resource(MemoryType::RESULT_TILE_BITMAP);
  tdb::pmr::vector<uint8_t> result_bitmap(cells, 1, resource);
  REQUIRE(
      query_condition.apply_sparse<uint8_t>(params, *result_tile, result_bitmap)
          .ok());

  // Verify the result bitmap contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
    if (result_bitmap[cell_idx]) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }
}

/**
 * Tests each comparison operator on all cells in a tile.
 *
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_operators_sparse(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile,
    void* values) {
  test_apply_cells_sparse<T>(
      QueryConditionOp::LT,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_sparse<T>(
      QueryConditionOp::LE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_sparse<T>(
      QueryConditionOp::GT,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_sparse<T>(
      QueryConditionOp::GE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_sparse<T>(
      QueryConditionOp::EQ,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells_sparse<T>(
      QueryConditionOp::NE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
}

/**
 * Populates a tile and tests query condition comparisons against
 * each cell.
 *
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 */
template <typename T>
void test_apply_tile_sparse(
    const std::string& field_name,
    uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* result_tile);

/**
 * C-string template-specialization for `test_apply_tile_sparse`.
 */
template <>
void test_apply_tile_sparse<char*>(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);

  bool var_size = array_schema->attribute(field_name)->var_size();
  bool nullable = array_schema->attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &tile_tuple->var_tile() : &tile_tuple->fixed_tile();
  std::vector<char> values(cells * 2);
  for (uint64_t i = 0; i < cells; ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }
  REQUIRE_NOTHROW(tile->write(values.data(), 0, 2 * cells * sizeof(char)));

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells + 1);
    uint64_t offset = 0;
    for (uint64_t i = 0; i <= cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE_NOTHROW(
        tile_offsets->write(offsets.data(), 0, (cells + 1) * sizeof(uint64_t)));
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE_NOTHROW(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)));
  }

  test_apply_operators_sparse<char*>(
      field_name,
      cells,
      array_schema,
      result_tile,
      static_cast<void*>(values.data()));
}

/**
 * Non-specialized template type for `test_apply_tile_sparse`.
 */
template <typename T>
void test_apply_tile_sparse(
    const std::string& field_name,
    const uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);
  Tile* const tile = &tile_tuple->fixed_tile();
  std::vector<T> values(cells);
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = static_cast<T>(i);
  }
  REQUIRE_NOTHROW(tile->write(values.data(), 0, cells * sizeof(T)));

  test_apply_operators_sparse<T>(
      field_name,
      cells,
      array_schema,
      result_tile,
      static_cast<void*>(values.data()));
}

/**
 * Constructs a tile and tests query condition comparisons against
 * each cell.
 *
 * @param type The TILEDB data type of the attribute.
 * @param var_size Run the test with variable size attribute.
 * @param nullable Run the test with nullable attribute.
 */
template <typename T>
void test_apply_sparse(
    const Datatype type, bool var_size = false, bool nullable = false);

/**
 * C-string template-specialization for `test_apply_sparse`.
 */
template <>
void test_apply_sparse<char*>(
    const Datatype type, bool var_size, bool nullable) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  REQUIRE((type == Datatype::STRING_ASCII || type == Datatype::STRING_UTF8));

  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(nullable);
  attr.set_cell_val_num(var_size ? constants::var_num : 2);

  if (!nullable) {
    attr.set_fill_value(fill_value, 2 * sizeof(char));
  }

  array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? (cells + 1) * constants::cell_var_offset_size :
                 2 * cells * sizeof(char),
      0,
      var_size ? std::optional(2 * cells * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  test_apply_tile_sparse<char*>(field_name, cells, array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply_sparse`.
 */
template <typename T>
void test_apply_sparse(const Datatype type, bool var_size, bool nullable) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_cell_val_num(1);
  attr.set_fill_value(&fill_value, sizeof(T));
  array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(T),
      0,
      var_size ? std::optional(0) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  test_apply_tile_sparse<T>(field_name, cells, array_schema, &result_tile);
}

TEST_CASE(
    "QueryCondition: Test apply sparse", "[QueryCondition][apply][sparse]") {
  test_apply_sparse<int8_t>(Datatype::INT8);
  test_apply_sparse<uint8_t>(Datatype::UINT8);
  test_apply_sparse<int16_t>(Datatype::INT16);
  test_apply_sparse<uint16_t>(Datatype::UINT16);
  test_apply_sparse<int32_t>(Datatype::INT32);
  test_apply_sparse<uint32_t>(Datatype::UINT32);
  test_apply_sparse<int64_t>(Datatype::INT64);
  test_apply_sparse<uint64_t>(Datatype::UINT64);
  test_apply_sparse<float>(Datatype::FLOAT32);
  test_apply_sparse<double>(Datatype::FLOAT64);
  test_apply_sparse<char>(Datatype::CHAR);
  test_apply_sparse<int64_t>(Datatype::DATETIME_YEAR);
  test_apply_sparse<int64_t>(Datatype::DATETIME_MONTH);
  test_apply_sparse<int64_t>(Datatype::DATETIME_WEEK);
  test_apply_sparse<int64_t>(Datatype::DATETIME_DAY);
  test_apply_sparse<int64_t>(Datatype::DATETIME_HR);
  test_apply_sparse<int64_t>(Datatype::DATETIME_MIN);
  test_apply_sparse<int64_t>(Datatype::DATETIME_SEC);
  test_apply_sparse<int64_t>(Datatype::DATETIME_MS);
  test_apply_sparse<int64_t>(Datatype::DATETIME_US);
  test_apply_sparse<int64_t>(Datatype::DATETIME_NS);
  test_apply_sparse<int64_t>(Datatype::DATETIME_PS);
  test_apply_sparse<int64_t>(Datatype::DATETIME_FS);
  test_apply_sparse<int64_t>(Datatype::DATETIME_AS);
  test_apply_sparse<int64_t>(Datatype::TIME_HR);
  test_apply_sparse<int64_t>(Datatype::TIME_MIN);
  test_apply_sparse<int64_t>(Datatype::TIME_SEC);
  test_apply_sparse<int64_t>(Datatype::TIME_MS);
  test_apply_sparse<int64_t>(Datatype::TIME_US);
  test_apply_sparse<int64_t>(Datatype::TIME_NS);
  test_apply_sparse<int64_t>(Datatype::TIME_PS);
  test_apply_sparse<int64_t>(Datatype::TIME_FS);
  test_apply_sparse<int64_t>(Datatype::TIME_AS);
  test_apply_sparse<char*>(Datatype::STRING_ASCII);
  test_apply_sparse<char*>(Datatype::STRING_ASCII, true);
  test_apply_sparse<char*>(Datatype::STRING_ASCII, false, true);
  test_apply_sparse<char*>(Datatype::STRING_UTF8);
  test_apply_sparse<char*>(Datatype::STRING_UTF8, true);
  test_apply_sparse<char*>(Datatype::STRING_UTF8, false, true);
}

/**
 * @brief Test parameters structure that contains the query condition
 * object, and the expected results of running the query condition on
 * an size 10 array containing {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}.
 */
struct TestParams {
  QueryCondition qc_;
  std::vector<uint8_t> expected_bitmap_;
  std::vector<uint8_t> neg_expected_bitmap_;
  std::vector<ResultCellSlab> expected_slabs_;

  TestParams(
      ResultTile* result_tile,
      QueryCondition qc,
      std::vector<uint8_t> expected_bitmap,
      std::vector<uint8_t> neg_expected_bitmap = {})
      : qc_(qc)
      , expected_bitmap_(std::move(expected_bitmap))
      , neg_expected_bitmap_(std::move(neg_expected_bitmap)) {
    if (neg_expected_bitmap_.size() == 0) {
      neg_expected_bitmap_.resize(expected_bitmap_.size());
      for (size_t i = 0; i < expected_bitmap_.size(); i++) {
        neg_expected_bitmap_[i] = expected_bitmap_[i] ? 0 : 1;
      }
    }

    // Calculate expected_slabs_
    using signed_size_t = std::make_signed_t<size_t>;
    signed_size_t start{-1};
    signed_size_t length{-1};
    for (signed_size_t i = 0;
         i < static_cast<signed_size_t>(expected_bitmap_.size());
         i++) {
      if (expected_bitmap_[i] && start < 0) {
        start = i;
        length = 1;
      } else if (expected_bitmap_[i] && start >= 0) {
        length += 1;
      } else if (!expected_bitmap_[i] && start >= 0) {
        expected_slabs_.emplace_back(result_tile, start, length);
        start = -1;
        length = -1;
      }
    }
    if (start > 0) {
      expected_slabs_.emplace_back(result_tile, start, length);
    }
  }

  TestParams negate(ResultTile* result_tile) {
    TestParams tp(
        result_tile,
        qc_.negated_condition(),
        neg_expected_bitmap_,
        expected_bitmap_);
    return tp;
  }
};

/**
 * @brief Validate QueryCondition::apply by calling it and verifying
 * the results against the expected results.
 *
 * @param tp TestParams object that contains the query condition object
 * and the expected results.
 * @param cells The number of cells in the array we're running the query on.
 * @param array_schema The array schema of the array we're running the query on.
 * @param result_tile The result tile.
 */
void validate_qc_apply(
    TestParams& tp,
    uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile& result_tile,
    bool negated = false) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  ResultCellSlab result_cell_slab(&result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));
  std::vector<shared_ptr<FragmentMetadata>> frag_md(1);
  frag_md[0] = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(tp.qc_.apply(params, frag_md, result_cell_slabs, 1).ok());
  REQUIRE(result_cell_slabs.size() == tp.expected_slabs_.size());
  uint64_t result_cell_slabs_size = result_cell_slabs.size();
  for (uint64_t i = 0; i < result_cell_slabs_size; ++i) {
    CHECK(result_cell_slabs[i].start_ == tp.expected_slabs_[i].start_);
    CHECK(result_cell_slabs[i].length_ == tp.expected_slabs_[i].length_);
  }

  if (!negated) {
    auto neg_tp = tp.negate(&result_tile);
    validate_qc_apply(neg_tp, cells, array_schema, result_tile, true);
  }
}

/**
 * @brief Validate QueryCondition::apply_sparse by calling it and verifying
 * the results against the expected results.
 *
 * @param tp TestParams object that contains the query condition object
 * and the expected results.
 * @param cells The number of cells in the array we're running the query on.
 * @param array_schema The array schema of the array we're running the query on.
 * @param result_tile The result tile that contains the array data.
 */
void validate_qc_apply_sparse(
    TestParams& tp,
    uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile& result_tile,
    bool negated = false) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  QueryCondition::Params params(memory_tracker, *array_schema);
  auto resource = memory_tracker->get_resource(MemoryType::RESULT_TILE_BITMAP);
  tdb::pmr::vector<uint8_t> sparse_result_bitmap(cells, 1, resource);
  REQUIRE(
      tp.qc_.apply_sparse<uint8_t>(params, result_tile, sparse_result_bitmap)
          .ok());
  for (uint64_t i = 0; i < cells; ++i) {
    CHECK(sparse_result_bitmap[i] == tp.expected_bitmap_[i]);
  }

  tdb::pmr::vector<uint64_t> sparse_result_bitmap1(cells, 2, resource);
  REQUIRE(
      tp.qc_.apply_sparse<uint64_t>(params, result_tile, sparse_result_bitmap1)
          .ok());
  for (uint64_t i = 0; i < cells; ++i) {
    CHECK(sparse_result_bitmap1[i] == tp.expected_bitmap_[i] * 2);
  }

  if (!negated) {
    auto neg_tp = tp.negate(&result_tile);
    validate_qc_apply(neg_tp, cells, array_schema, result_tile, true);
  }
}

/**
 * @brief Validate QueryCondition::apply_dense by calling it and verifying
 * the results against the expected results.
 *
 * @param tp TestParams object that contains the query condition object
 * and the expected results.
 * @param cells The number of cells in the array we're running the query on.
 * @param array_schema The array schema of the array we're running the query on.
 * @param result_tile The result tile that contains the array data.
 */
void validate_qc_apply_dense(
    TestParams& tp,
    uint64_t cells,
    shared_ptr<const ArraySchema> array_schema,
    ResultTile& result_tile,
    bool negated = false) {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  std::vector<uint8_t> dense_result_bitmap(cells, 1);
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(tp.qc_
              .apply_dense(
                  params,
                  &result_tile,
                  0,
                  10,
                  0,
                  1,
                  nullptr,
                  dense_result_bitmap.data())
              .ok());
  for (uint64_t i = 0; i < cells; ++i) {
    CHECK(dense_result_bitmap[i] == tp.expected_bitmap_[i]);
  }

  if (!negated) {
    auto neg_tp = tp.negate(&result_tile);
    validate_qc_apply(neg_tp, cells, array_schema, result_tile, true);
  }
}

/**
 * @brief Function that takes a selection of QueryConditions, with their
 * expected results, and combines them together.
 *
 * @param field_name The field name of the query condition.
 * @param result_tile The result tile of the array we're running the query on.
 * @param tp_vec The vector that stores the test parameter structs.
 */
void populate_test_params_vector(
    const std::string& field_name,
    ResultTile* result_tile,
    std::vector<TestParams>& tp_vec) {
  // Construct basic AND query condition `foo > 3 AND foo <= 6`.
  {
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());
    uint64_t cmp_value_2 = 6;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());
    QueryCondition query_condition_3;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::AND,
                    &query_condition_3)
                .ok());

    TestParams tp(
        result_tile,
        std::move(query_condition_3),
        {0, 0, 0, 0, 1, 1, 1, 0, 0, 0});
    tp_vec.push_back(tp);
  }

  // Construct basic OR query condition `foo > 6 OR foo <= 3`.
  {
    uint64_t cmp_value_1 = 6;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());

    uint64_t cmp_value_2 = 3;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());

    QueryCondition query_condition_3;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::OR,
                    &query_condition_3)
                .ok());

    TestParams tp(
        result_tile,
        std::move(query_condition_3),
        {1, 1, 1, 1, 0, 0, 0, 1, 1, 1});
    tp_vec.push_back(tp);
  }
  // Construct query condition `(foo >= 3 AND foo <= 6) OR (foo > 5 AND foo <
  // 9)`. (OR of 2 AND ASTs)
  {
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::GE)
                .ok());

    uint64_t cmp_value_2 = 6;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::LE)
                .ok());

    QueryCondition combined_and;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::AND,
                    &combined_and)
                .ok());

    uint64_t cmp_value_3 = 5;
    QueryCondition query_condition_3;
    REQUIRE(query_condition_3
                .init(
                    std::string(field_name),
                    &cmp_value_3,
                    sizeof(uint64_t),
                    QueryConditionOp::GT)
                .ok());

    uint64_t cmp_value_4 = 9;
    QueryCondition query_condition_4;
    REQUIRE(query_condition_4
                .init(
                    std::string(field_name),
                    &cmp_value_4,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());

    QueryCondition combined_and1;
    REQUIRE(query_condition_3
                .combine(
                    query_condition_4,
                    QueryConditionCombinationOp::AND,
                    &combined_and1)
                .ok());

    QueryCondition combined_or;
    REQUIRE(
        combined_and
            .combine(
                combined_and1, QueryConditionCombinationOp::OR, &combined_or)
            .ok());

    TestParams tp(
        result_tile, std::move(combined_or), {0, 0, 0, 1, 1, 1, 1, 1, 1, 0});
    tp_vec.push_back(tp);
  }

  // Construct query condition `(foo < 3 OR foo >= 8) AND (foo <= 4 OR foo =
  // 9)`. (AND of 2 OR ASTs)
  {
    uint64_t cmp_value_1 = 3;
    QueryCondition query_condition_1;
    REQUIRE(query_condition_1
                .init(
                    std::string(field_name),
                    &cmp_value_1,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());

    uint64_t cmp_value_2 = 8;
    QueryCondition query_condition_2;
    REQUIRE(query_condition_2
                .init(
                    std::string(field_name),
                    &cmp_value_2,
                    sizeof(uint64_t),
                    QueryConditionOp::GE)
                .ok());

    QueryCondition combined_or;
    REQUIRE(query_condition_1
                .combine(
                    query_condition_2,
                    QueryConditionCombinationOp::OR,
                    &combined_or)
                .ok());

    uint64_t cmp_value_3 = 4;
    QueryCondition query_condition_3;
    REQUIRE(query_condition_3
                .init(
                    std::string(field_name),
                    &cmp_value_3,
                    sizeof(uint64_t),
                    QueryConditionOp::LT)
                .ok());

    uint64_t cmp_value_4 = 9;
    QueryCondition query_condition_4;
    REQUIRE(query_condition_4
                .init(
                    std::string(field_name),
                    &cmp_value_4,
                    sizeof(uint64_t),
                    QueryConditionOp::EQ)
                .ok());

    QueryCondition combined_or1;
    REQUIRE(query_condition_3
                .combine(
                    query_condition_4,
                    QueryConditionCombinationOp::OR,
                    &combined_or1)
                .ok());

    QueryCondition combined_and;
    REQUIRE(
        combined_or
            .combine(
                combined_or1, QueryConditionCombinationOp::AND, &combined_and)
            .ok());

    TestParams tp(
        result_tile, std::move(combined_and), {1, 1, 1, 0, 0, 0, 0, 0, 0, 1});
    tp_vec.push_back(tp);
  }

  // Construct query condition `(((foo = 1 || foo = 2) && (foo = 3 || foo = 4))
  // || (((foo != 8
  // && foo != 9 && (foo = 6 || foo = 7)) || foo = 5) && foo != 6))`. (Complex
  // tree with depth > 2)
  {
    std::vector<uint64_t> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<QueryCondition> qc_value_vector;
    for (int i = 0; i < 7; ++i) {
      QueryCondition qc;
      REQUIRE(qc.init(
                    std::string(field_name),
                    &vals[i],
                    sizeof(vals[i]),
                    QueryConditionOp::EQ)
                  .ok());
      qc_value_vector.push_back(qc);
    }

    for (int i = 7; i < 9; ++i) {
      QueryCondition qc;
      REQUIRE(qc.init(
                    std::string(field_name),
                    &vals[i],
                    sizeof(vals[i]),
                    QueryConditionOp::NE)
                  .ok());
      qc_value_vector.push_back(qc);
    }

    uint64_t x = 6;
    QueryCondition x_neq_six;
    REQUIRE(
        x_neq_six
            .init(std::string(field_name), &x, sizeof(x), QueryConditionOp::NE)
            .ok());
    QueryCondition one_or_two;
    REQUIRE(qc_value_vector[0]
                .combine(
                    qc_value_vector[1],
                    QueryConditionCombinationOp::OR,
                    &one_or_two)
                .ok());
    QueryCondition three_or_four;
    REQUIRE(qc_value_vector[2]
                .combine(
                    qc_value_vector[3],
                    QueryConditionCombinationOp::OR,
                    &three_or_four)
                .ok());
    QueryCondition six_or_seven;
    REQUIRE(qc_value_vector[5]
                .combine(
                    qc_value_vector[6],
                    QueryConditionCombinationOp::OR,
                    &six_or_seven)
                .ok());
    QueryCondition eight_and_nine;
    REQUIRE(qc_value_vector[7]
                .combine(
                    qc_value_vector[8],
                    QueryConditionCombinationOp::AND,
                    &eight_and_nine)
                .ok());
    QueryCondition subtree_a;
    REQUIRE(one_or_two
                .combine(
                    three_or_four, QueryConditionCombinationOp::AND, &subtree_a)
                .ok());
    QueryCondition subtree_d;
    REQUIRE(
        eight_and_nine
            .combine(six_or_seven, QueryConditionCombinationOp::AND, &subtree_d)
            .ok());
    QueryCondition subtree_c;
    REQUIRE(
        subtree_d
            .combine(
                qc_value_vector[4], QueryConditionCombinationOp::OR, &subtree_c)
            .ok());
    QueryCondition subtree_b;
    REQUIRE(
        subtree_c
            .combine(x_neq_six, QueryConditionCombinationOp::AND, &subtree_b)
            .ok());
    QueryCondition qc;
    REQUIRE(subtree_a.combine(subtree_b, QueryConditionCombinationOp::OR, &qc)
                .ok());

    TestParams tp(result_tile, std::move(qc), {0, 0, 0, 0, 0, 1, 0, 1, 0, 0});
    tp_vec.push_back(tp);
  }

  // Construct query condition `foo != 1 && foo != 3 && foo != 5 && foo != 7 &&
  // foo != 9`. (Adding simple clauses to AND tree)
  {
    uint64_t val1 = 1;
    QueryCondition query_condition1;
    REQUIRE(query_condition1
                .init(
                    std::string(field_name),
                    &val1,
                    sizeof(val1),
                    QueryConditionOp::NE)
                .ok());

    uint64_t val2 = 3;
    QueryCondition query_condition2;
    REQUIRE(query_condition2
                .init(
                    std::string(field_name),
                    &val2,
                    sizeof(val2),
                    QueryConditionOp::NE)
                .ok());

    uint64_t val3 = 5;
    QueryCondition query_condition3;
    REQUIRE(query_condition3
                .init(
                    std::string(field_name),
                    &val3,
                    sizeof(val3),
                    QueryConditionOp::NE)
                .ok());

    uint64_t val4 = 7;
    QueryCondition query_condition4;
    REQUIRE(query_condition4
                .init(
                    std::string(field_name),
                    &val4,
                    sizeof(val4),
                    QueryConditionOp::NE)
                .ok());

    uint64_t val5 = 9;
    QueryCondition query_condition5;
    REQUIRE(query_condition5
                .init(
                    std::string(field_name),
                    &val5,
                    sizeof(val5),
                    QueryConditionOp::NE)
                .ok());

    QueryCondition combined_and1;
    REQUIRE(query_condition1
                .combine(
                    query_condition2,
                    QueryConditionCombinationOp::AND,
                    &combined_and1)
                .ok());

    QueryCondition combined_and2;
    REQUIRE(combined_and1
                .combine(
                    query_condition3,
                    QueryConditionCombinationOp::AND,
                    &combined_and2)
                .ok());

    QueryCondition combined_and3;
    REQUIRE(combined_and2
                .combine(
                    query_condition4,
                    QueryConditionCombinationOp::AND,
                    &combined_and3)
                .ok());

    QueryCondition combined_and4;
    REQUIRE(combined_and3
                .combine(
                    query_condition5,
                    QueryConditionCombinationOp::AND,
                    &combined_and4)
                .ok());

    TestParams tp(
        result_tile, std::move(combined_and4), {1, 0, 1, 0, 1, 0, 1, 0, 1, 0});
    tp_vec.push_back(tp);
  }

  // Construct query condition `foo = 0 || foo = 2 || foo = 4 || foo = 6 || foo
  // = 8`. (Adding simple clauses to OR tree)
  {
    uint64_t val1 = 0;
    QueryCondition query_condition1;
    REQUIRE(query_condition1
                .init(
                    std::string(field_name),
                    &val1,
                    sizeof(val1),
                    QueryConditionOp::EQ)
                .ok());

    uint64_t val2 = 2;
    QueryCondition query_condition2;
    REQUIRE(query_condition2
                .init(
                    std::string(field_name),
                    &val2,
                    sizeof(val2),
                    QueryConditionOp::EQ)
                .ok());

    uint64_t val3 = 4;
    QueryCondition query_condition3;
    REQUIRE(query_condition3
                .init(
                    std::string(field_name),
                    &val3,
                    sizeof(val3),
                    QueryConditionOp::EQ)
                .ok());

    uint64_t val4 = 6;
    QueryCondition query_condition4;
    REQUIRE(query_condition4
                .init(
                    std::string(field_name),
                    &val4,
                    sizeof(val4),
                    QueryConditionOp::EQ)
                .ok());

    uint64_t val5 = 8;
    QueryCondition query_condition5;
    REQUIRE(query_condition5
                .init(
                    std::string(field_name),
                    &val5,
                    sizeof(val5),
                    QueryConditionOp::EQ)
                .ok());

    QueryCondition combined_or1;
    REQUIRE(query_condition1
                .combine(
                    query_condition2,
                    QueryConditionCombinationOp::OR,
                    &combined_or1)
                .ok());
    QueryCondition combined_or2;
    REQUIRE(combined_or1
                .combine(
                    query_condition3,
                    QueryConditionCombinationOp::OR,
                    &combined_or2)
                .ok());
    QueryCondition combined_or3;
    REQUIRE(combined_or2
                .combine(
                    query_condition4,
                    QueryConditionCombinationOp::OR,
                    &combined_or3)
                .ok());
    QueryCondition combined_or4;
    REQUIRE(combined_or3
                .combine(
                    query_condition5,
                    QueryConditionCombinationOp::OR,
                    &combined_or4)
                .ok());

    TestParams tp(
        result_tile, std::move(combined_or4), {1, 0, 1, 0, 1, 0, 1, 0, 1, 0});
    tp_vec.push_back(tp);
  }
}

TEST_CASE(
    "QueryCondition: Test combinations", "[QueryCondition][combinations]") {
  // Setup.
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::UINT64;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), attr));

  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(uint64_t),
      0,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);
  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &tile_tuple->fixed_tile();

  // Populate the data tile.
  std::vector<uint64_t> values(cells);
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = i;
  }
  REQUIRE_NOTHROW(tile->write(values.data(), 0, cells * sizeof(uint64_t)));

  std::vector<TestParams> tp_vec;
  populate_test_params_vector(field_name, &result_tile, tp_vec);

  SECTION("Validate apply.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply(elem, cells, array_schema, result_tile);
    }
  }

  SECTION("Validate apply_sparse.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply_sparse(elem, cells, array_schema, result_tile);
    }
  }

  SECTION("Validate apply_dense.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply_dense(elem, cells, array_schema, result_tile);
    }
  }
}

/**
 * @brief Function that takes a selection of QueryConditions, with their
 * expected results, and combines them together. This function is
 * exclusively for string query conditions.
 *
 * @param field_name The field name of the query condition.
 * @param result_tile The result tile of the array we're running the query on.
 * @param tp_vec The vector that stores the test parameter structs.
 */
void populate_string_test_params_vector(
    const std::string& field_name,
    ResultTile* result_tile,
    std::vector<TestParams>& tp_vec) {
  // Construct basic query condition `foo < "eve"`.
  {
    char e[] = "eve";
    QueryCondition qc;
    REQUIRE(
        qc.init(std::string(field_name), &e, strlen(e), QueryConditionOp::LT)
            .ok());

    TestParams tp(result_tile, std::move(qc), {1, 1, 1, 1, 1, 0, 0, 0, 0, 0});
    tp_vec.push_back(tp);
  }
  // Construct basic AND query condition `foo >= "bob" AND foo <= "eve"`.
  {
    char b[] = "bob";
    char e[] = "eve";
    QueryCondition qc1;
    REQUIRE(
        qc1.init(std::string(field_name), &e, strlen(e), QueryConditionOp::LE)
            .ok());

    QueryCondition qc2;
    REQUIRE(
        qc2.init(std::string(field_name), &b, strlen(b), QueryConditionOp::GE)
            .ok());

    QueryCondition qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::AND, &qc).ok());

    TestParams tp(result_tile, std::move(qc), {0, 1, 1, 1, 1, 0, 0, 0, 0, 0});
    tp_vec.push_back(tp);
  }

  // Construct basic OR query condition `foo >= "eve" OR foo <= "bob"`.
  {
    char b[] = "bob";
    char e[] = "eve";
    QueryCondition qc1;
    REQUIRE(
        qc1.init(std::string(field_name), &e, strlen(e), QueryConditionOp::GE)
            .ok());

    QueryCondition qc2;
    REQUIRE(
        qc2.init(std::string(field_name), &b, strlen(b), QueryConditionOp::LE)
            .ok());

    QueryCondition qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::OR, &qc).ok());

    TestParams tp(result_tile, std::move(qc), {1, 1, 0, 0, 0, 1, 1, 1, 1, 1});
    tp_vec.push_back(tp);
  }

  // Construct query condition `(foo > "ask" AND foo <= "hi") OR (foo > "bye"
  // AND foo < "just")`. (OR of 2 AND ASTs)
  {
    char a[] = "ask";
    char b[] = "bye";
    char h[] = "hi";
    char j[] = "just";
    QueryCondition qc1;
    REQUIRE(
        qc1.init(std::string(field_name), &a, strlen(a), QueryConditionOp::GT)
            .ok());

    QueryCondition qc2;
    REQUIRE(
        qc2.init(std::string(field_name), &h, strlen(h), QueryConditionOp::LE)
            .ok());

    QueryCondition qc3;
    REQUIRE(
        qc3.init(std::string(field_name), &b, strlen(b), QueryConditionOp::GT)
            .ok());

    QueryCondition qc4;
    REQUIRE(
        qc4.init(std::string(field_name), &j, strlen(j), QueryConditionOp::LT)
            .ok());

    QueryCondition qc5, qc6, qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::AND, &qc5).ok());
    REQUIRE(qc3.combine(qc4, QueryConditionCombinationOp::AND, &qc6).ok());
    REQUIRE(qc5.combine(qc6, QueryConditionCombinationOp::OR, &qc).ok());

    TestParams tp(result_tile, std::move(qc), {0, 1, 1, 1, 1, 1, 1, 1, 1, 1});
    tp_vec.push_back(tp);
  }

  // Construct query condition `(foo = "craig" OR foo == "heidi") AND (foo >
  // "eve" OR foo < "bye")`. (AND of 2 OR ASTs)
  {
    char b[] = "bye";
    char c[] = "craig";
    char e[] = "eve";
    char h[] = "heidi";
    QueryCondition qc1;
    REQUIRE(
        qc1.init(std::string(field_name), &c, strlen(c), QueryConditionOp::EQ)
            .ok());

    QueryCondition qc2;
    REQUIRE(
        qc2.init(std::string(field_name), &h, strlen(h), QueryConditionOp::EQ)
            .ok());

    QueryCondition qc3;
    REQUIRE(
        qc3.init(std::string(field_name), &e, strlen(e), QueryConditionOp::GT)
            .ok());

    QueryCondition qc4;
    REQUIRE(
        qc4.init(std::string(field_name), &b, strlen(b), QueryConditionOp::LT)
            .ok());

    QueryCondition qc5, qc6, qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::OR, &qc5).ok());
    REQUIRE(qc3.combine(qc4, QueryConditionCombinationOp::OR, &qc6).ok());
    REQUIRE(qc5.combine(qc6, QueryConditionCombinationOp::AND, &qc).ok());

    TestParams tp(result_tile, std::move(qc), {0, 0, 0, 0, 0, 0, 0, 1, 0, 0});
    tp_vec.push_back(tp);
  }

  {
    std::vector<std::string> vec = {"alice", "craig", "erin", "grace", "ivan"};
    // Construct query condition `foo != "alice" && foo != "craig" && foo !=
    // "erin" && foo != "grace" && foo != "ivan"`. (Adding simple clauses to AND
    // tree)
    {
      std::vector<QueryCondition> val_nodes;
      for (uint64_t i = 0; i < 5; ++i) {
        QueryCondition temp;
        REQUIRE(temp.init(
                        std::string(field_name),
                        vec[i].c_str(),
                        vec[i].size(),
                        QueryConditionOp::NE)
                    .ok());
        val_nodes.push_back(temp);
      }

      QueryCondition qc1, qc2, qc3, qc;
      REQUIRE(val_nodes[0]
                  .combine(val_nodes[1], QueryConditionCombinationOp::AND, &qc1)
                  .ok());
      REQUIRE(qc1.combine(val_nodes[2], QueryConditionCombinationOp::AND, &qc2)
                  .ok());
      REQUIRE(qc2.combine(val_nodes[3], QueryConditionCombinationOp::AND, &qc3)
                  .ok());
      REQUIRE(qc3.combine(val_nodes[4], QueryConditionCombinationOp::AND, &qc)
                  .ok());

      TestParams tp(result_tile, std::move(qc), {0, 1, 0, 1, 0, 1, 0, 1, 0, 1});
      tp_vec.push_back(tp);
    }

    // Construct query condition `foo = "alice" || foo = "craig" || foo = "erin"
    // || foo = "grace" || foo = "ivan"`. (Adding simple clauses to OR tree)
    {
      std::vector<QueryCondition> val_nodes;
      for (uint64_t i = 0; i < 5; ++i) {
        QueryCondition temp;
        REQUIRE(temp.init(
                        std::string(field_name),
                        vec[i].c_str(),
                        vec[i].size(),
                        QueryConditionOp::EQ)
                    .ok());
        val_nodes.push_back(temp);
      }

      QueryCondition qc1, qc2, qc3, qc;
      REQUIRE(val_nodes[0]
                  .combine(val_nodes[1], QueryConditionCombinationOp::OR, &qc1)
                  .ok());
      REQUIRE(qc1.combine(val_nodes[2], QueryConditionCombinationOp::OR, &qc2)
                  .ok());
      REQUIRE(qc2.combine(val_nodes[3], QueryConditionCombinationOp::OR, &qc3)
                  .ok());
      REQUIRE(
          qc3.combine(val_nodes[4], QueryConditionCombinationOp::OR, &qc).ok());

      TestParams tp(result_tile, std::move(qc), {1, 0, 1, 0, 1, 0, 1, 0, 1, 0});
      tp_vec.push_back(tp);
    }
  }
}

TEST_CASE(
    "QueryCondition: Test combinations, string",
    "[QueryCondition][combinations][string]") {
  // Setup.
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = GENERATE(Datatype::STRING_ASCII, Datatype::STRING_UTF8);

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(false);
  attr.set_cell_val_num(constants::var_num);
  attr.set_fill_value("ac", 2 * sizeof(char));

  array_schema->add_attribute(make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{
      make_shared<Dimension>(HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  std::string data = "alicebobcraigdaveerinfrankgraceheidiivanjudy";
  ResultTile::TileSizes tile_sizes(
      (cells + 1) * constants::cell_var_offset_size,
      0,
      data.size(),
      0,
      std::nullopt,
      std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &tile_tuple->var_tile();

  std::vector<uint64_t> offsets = {0, 5, 8, 13, 17, 21, 26, 31, 36, 40, 44};
  REQUIRE_NOTHROW(tile->write(data.c_str(), 0, data.size()));

  // Write the tile offsets.
  Tile* const tile_offsets = &tile_tuple->fixed_tile();
  REQUIRE_NOTHROW(
      tile_offsets->write(offsets.data(), 0, (cells + 1) * sizeof(uint64_t)));

  std::vector<TestParams> tp_vec;
  populate_string_test_params_vector(field_name, &result_tile, tp_vec);

  SECTION("Validate apply.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply(elem, cells, array_schema, result_tile);
    }
  }

  SECTION("Validate apply_sparse.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply_sparse(elem, cells, array_schema, result_tile);
    }
  }

  SECTION("Validate apply_dense.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply_dense(elem, cells, array_schema, result_tile);
    }
  }
}

/**
 * @brief Function that takes a selection of QueryConditions, with their
 * expected results, and combines them together. This function is
 * exclusively for UTF-8 string query conditions.
 *
 * @param field_name The field name of the query condition.
 * @param result_tile The result tile of the array we're running the query on.
 * @param tp_vec The vector that stores the test parameter structs.
 */
void populate_utf8_string_test_params_vector(
    const std::string& field_name,
    ResultTile* result_tile,
    std::vector<TestParams>& tp_vec) {
  // UTF-8 String data sorted according to raw byte comparisons.
  std::string upper_a = "\x41";
  std::string upper_aa = "\x41\x41";
  std::string lower_a = "\x61";
  std::string lower_a0 = "\x61\x30";
  std::string lower_aa = "\x61\x61";
  std::string n_plus_tilda = "\x6e\xcc\x83";
  std::string n_with_tilda = "\xc3\xb1";
  std::string u_with_umlaut = "\xc3\xbc";
  std::string infinity = "\xe2\x88\x9e";
  std::string snowman = "\xe2\x98\x83\xef\xb8\x8f";
  std::string mahjong = "\xf0\x9f\x80\x84";
  std::string yarn = "\xf0\x9f\xa7\xb6";
  std::string flag_gr = "\xf0\x9f\x87\xac\xf0\x9f\x87\xb7";
  std::string icecream = "\xf0\x9f\x8d\xa8";
  std::string doughnut = "\xf0\x9f\x8d\xa9";

  // N.B. This is a cop-pasta of populate_string_test_params_vector
  // updated to use different string data. The equivalency table is:
  //
  // alice -> upper_a
  // ask -> uper_aa
  // bob -> lower_a
  // bye -> lower_a0
  // craig -> lower_aa
  // dave -> n_plus_tilda
  // erin -> n_with_tilda
  // eve -> u_with_umlaut
  // frank -> infinity
  // grace -> snowman
  // heidi -> mahjong
  // hi -> yarn
  // ivan -> flag_gr
  // judy -> icecream
  // just -> doughnut

  // $val < u_with_umlaut
  {
    QueryCondition qc;
    REQUIRE(qc.init(
                  std::string(field_name),
                  u_with_umlaut.data(),
                  u_with_umlaut.size(),
                  QueryConditionOp::LT)
                .ok());

    TestParams tp(result_tile, std::move(qc), {1, 1, 1, 1, 1, 0, 0, 0, 0, 0});
    tp_vec.push_back(tp);
  }

  // $val >= lower_a && $val <= u_with_umlaut
  {
    QueryCondition qc1;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   u_with_umlaut.data(),
                   u_with_umlaut.size(),
                   QueryConditionOp::LE)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(
                   std::string(field_name),
                   lower_a.data(),
                   lower_a.size(),
                   QueryConditionOp::GE)
                .ok());

    QueryCondition qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::AND, &qc).ok());
    TestParams tp(result_tile, std::move(qc), {0, 1, 1, 1, 1, 0, 0, 0, 0, 0});
    tp_vec.push_back(tp);
    tp_vec.push_back(tp);
  }

  // $val >= u_with_umlaut || $val <= lower_a
  {
    QueryCondition qc1;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   u_with_umlaut.data(),
                   u_with_umlaut.size(),
                   QueryConditionOp::GE)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(
                   std::string(field_name),
                   lower_a.data(),
                   lower_a.size(),
                   QueryConditionOp::LE)
                .ok());

    QueryCondition qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::OR, &qc).ok());

    TestParams tp(result_tile, std::move(qc), {1, 1, 0, 0, 0, 1, 1, 1, 1, 1});
  }

  // ($val > upper_aa && $val <= yarn) || ($val > lower_a0 && $val < doughnut)
  // I.e., OR of 2 AND ASTs
  {
    QueryCondition qc1;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   upper_aa.data(),
                   upper_aa.size(),
                   QueryConditionOp::GT)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(
                   std::string(field_name),
                   yarn.data(),
                   yarn.size(),
                   QueryConditionOp::LE)
                .ok());

    QueryCondition qc3;
    REQUIRE(qc3.init(
                   std::string(field_name),
                   lower_a0.data(),
                   lower_a0.size(),
                   QueryConditionOp::GT)
                .ok());

    QueryCondition qc4;
    REQUIRE(qc4.init(
                   std::string(field_name),
                   doughnut.data(),
                   doughnut.size(),
                   QueryConditionOp::LT)
                .ok());

    QueryCondition qc5, qc6, qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::AND, &qc5).ok());
    REQUIRE(qc3.combine(qc4, QueryConditionCombinationOp::AND, &qc6).ok());
    REQUIRE(qc5.combine(qc6, QueryConditionCombinationOp::OR, &qc).ok());

    TestParams tp(result_tile, std::move(qc), {0, 1, 1, 1, 1, 1, 1, 1, 1, 1});
    tp_vec.push_back(tp);
  }

  // ($val == lower_aa || $val == mahjong) &&
  //    ($val > u_with_umlaut || $val < lower_a0)
  // I.e., AND of 2 OR ASTs
  {
    QueryCondition qc1;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   lower_aa.data(),
                   lower_aa.size(),
                   QueryConditionOp::EQ)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(
                   std::string(field_name),
                   mahjong.data(),
                   mahjong.size(),
                   QueryConditionOp::EQ)
                .ok());

    QueryCondition qc3;
    REQUIRE(qc3.init(
                   std::string(field_name),
                   u_with_umlaut.data(),
                   u_with_umlaut.size(),
                   QueryConditionOp::GT)
                .ok());

    QueryCondition qc4;
    REQUIRE(qc4.init(
                   std::string(field_name),
                   lower_a0.data(),
                   lower_a0.size(),
                   QueryConditionOp::LT)
                .ok());

    QueryCondition qc5, qc6, qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::OR, &qc5).ok());
    REQUIRE(qc3.combine(qc4, QueryConditionCombinationOp::OR, &qc6).ok());
    REQUIRE(qc5.combine(qc6, QueryConditionCombinationOp::AND, &qc).ok());

    TestParams tp(result_tile, std::move(qc), {0, 0, 0, 0, 0, 0, 0, 1, 0, 0});
    tp_vec.push_back(tp);
  }

  {
    std::vector<std::string> vec = {
        upper_a, lower_aa, n_with_tilda, snowman, flag_gr};
    // $val != upper_a && $val != lower_a && $val != n_with_tilda &&
    //    $val != snowman && $val != flag_gr
    // I.e., combine multiple simple AND clauses
    {
      std::vector<QueryCondition> val_nodes;
      for (uint64_t i = 0; i < 5; ++i) {
        QueryCondition temp;
        REQUIRE(temp.init(
                        std::string(field_name),
                        vec[i].c_str(),
                        vec[i].size(),
                        QueryConditionOp::NE)
                    .ok());
        val_nodes.push_back(temp);
      }

      QueryCondition qc1, qc2, qc3, qc;
      REQUIRE(val_nodes[0]
                  .combine(val_nodes[1], QueryConditionCombinationOp::AND, &qc1)
                  .ok());
      REQUIRE(qc1.combine(val_nodes[2], QueryConditionCombinationOp::AND, &qc2)
                  .ok());
      REQUIRE(qc2.combine(val_nodes[3], QueryConditionCombinationOp::AND, &qc3)
                  .ok());
      REQUIRE(qc3.combine(val_nodes[4], QueryConditionCombinationOp::AND, &qc)
                  .ok());

      TestParams tp(result_tile, std::move(qc), {0, 1, 0, 1, 0, 1, 0, 1, 0, 1});
      tp_vec.push_back(tp);
    }

    // $val == $upper_a || $val == lower_a || $val == n_with_tilda ||
    //    $ val == snowman || $val == flag_gr
    // I.e., combine multiple simple OR clauses
    {
      std::vector<QueryCondition> val_nodes;
      for (uint64_t i = 0; i < 5; ++i) {
        QueryCondition temp;
        REQUIRE(temp.init(
                        std::string(field_name),
                        vec[i].c_str(),
                        vec[i].size(),
                        QueryConditionOp::EQ)
                    .ok());
        val_nodes.push_back(temp);
      }

      QueryCondition qc1, qc2, qc3, qc;
      REQUIRE(val_nodes[0]
                  .combine(val_nodes[1], QueryConditionCombinationOp::OR, &qc1)
                  .ok());
      REQUIRE(qc1.combine(val_nodes[2], QueryConditionCombinationOp::OR, &qc2)
                  .ok());
      REQUIRE(qc2.combine(val_nodes[3], QueryConditionCombinationOp::OR, &qc3)
                  .ok());
      REQUIRE(
          qc3.combine(val_nodes[4], QueryConditionCombinationOp::OR, &qc).ok());

      TestParams tp(result_tile, std::move(qc), {1, 0, 1, 0, 1, 0, 1, 0, 1, 0});
      tp_vec.push_back(tp);
    }
  }
}

TEST_CASE(
    "QueryCondition: Test combinations, string with UTF-8 data",
    "[QueryCondition][combinations][string][utf-8]") {
  // Setup.
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::STRING_UTF8;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(false);
  attr.set_cell_val_num(constants::var_num);
  attr.set_fill_value("ac", 2 * sizeof(char));

  array_schema->add_attribute(make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{
      make_shared<Dimension>(HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // For pasting into a Python shell:
  //
  // To install pyicu on MacOS:
  // $ brew install icu4c
  // $ python3 -m venv venv
  // $ source venv/bin/activate
  // $ PATH="$(brew --prefix icu4c)/bin:$PATH" pip install pyicu
  //
  // If you run the snippet below you'll see that the sort order
  // changes drastically using the ICU collator. Specifically, you
  // should see the following orders for without and with ICU collation:
  //
  //   A a aa n-yay n-yay :infinity: :snowman: :mahjong: :flag-gr: :icecream:
  //   :infinity: :snowman: :flag-gr: :mahjong: :icecream: a A aa n-yay n-yay
  //
  // data = [
  //   b"\x41".decode("utf-8"),
  //   b"\x61".decode("utf-8"),
  //   b"\x61\x61".decode("utf-8"),
  //   b"\x6e\xcc\x83".decode("utf-8"),
  //   b"\xc3\xb1".decode("utf-8"),
  //   b"\xe2\x88\x9e".decode("utf-8"),
  //   b"\xe2\x98\x83\xef\xb8\x8f".decode("utf-8"),
  //   b"\xf0\x9f\x80\x84".decode("utf-8"),
  //   b"\xf0\x9f\x87\xac\xf0\x9f\x87\xb7".decode("utf-8"),
  //   b"\xf0\x9f\x8d\xa8".decode("utf-8")
  // ]
  // sorted(data)
  // import icu
  // coll = icu.Collator.createInstance()
  // sorted(data, key=coll.getSortKey)

  // UTF-8 String Data
  //
  // These strings are sorted according to raw byte comparisons
  // without the use of ICU collation. The Python snippet above shows
  // the difference between the two sort methods.
  std::vector<std::string> utf8_data = {
      "\x41",                              // "A"
      "\x61",                              // "a"
      "\x61\x61",                          // "aa"
      "\x6e\xcc\x83",                      // n-plus-tilda
      "\xc3\xb1",                          // n-with-tilda
      "\xe2\x88\x9e",                      // :infinity:
      "\xe2\x98\x83\xef\xb8\x8f",          // :snowman:
      "\xf0\x9f\x80\x84",                  // :mahjong:
      "\xf0\x9f\x87\xac\xf0\x9f\x87\xb7",  // :flag-gr:
      "\xf0\x9f\x8d\xa8"                   // :icecream:
  };

  std::string data;
  std::vector<uint64_t> offsets;
  uint64_t curr_offset = 0;
  for (auto& elem : utf8_data) {
    data += elem;
    offsets.push_back(curr_offset);
    curr_offset += elem.size();
  }
  offsets.push_back(curr_offset);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      (cells + 1) * constants::cell_var_offset_size,
      0,
      data.size(),
      0,
      std::nullopt,
      std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &tile_tuple->var_tile();

  REQUIRE_NOTHROW(tile->write(data.c_str(), 0, data.size()));

  // Write the tile offsets.
  Tile* const tile_offsets = &tile_tuple->fixed_tile();
  REQUIRE_NOTHROW(
      tile_offsets->write(offsets.data(), 0, (cells + 1) * sizeof(uint64_t)));

  std::vector<TestParams> tp_vec;
  populate_utf8_string_test_params_vector(field_name, &result_tile, tp_vec);

  SECTION("Validate apply.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply(elem, cells, array_schema, result_tile);
    }
  }

  SECTION("Validate apply_sparse.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply_sparse(elem, cells, array_schema, result_tile);
    }
  }

  SECTION("Validate apply_dense.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply_dense(elem, cells, array_schema, result_tile);
    }
  }
}

/**
 * @brief Function that takes a selection of QueryConditions, with their
 * expected results, and combines them together. This function is
 * exclusively for nullable query conditions.
 *
 * @param field_name The field name of the query condition.
 * @param result_tile The result tile of the array we're running the query on.
 * @param tp_vec The vector that stores the test parameter structs.
 */
void populate_nullable_test_params_vector(
    const std::string& field_name,
    ResultTile* result_tile,
    std::vector<TestParams>& tp_vec) {
  // Construct basic query condition `foo = null`.
  {
    QueryCondition qc;
    REQUIRE(qc.init(std::string(field_name), nullptr, 0, QueryConditionOp::EQ)
                .ok());
    TestParams tp(result_tile, std::move(qc), {1, 0, 1, 0, 1, 0, 1, 0, 1, 0});
    tp_vec.push_back(tp);
  }

  // Construct basic query condition `foo != null`.
  {
    QueryCondition qc;
    REQUIRE(qc.init(std::string(field_name), nullptr, 0, QueryConditionOp::NE)
                .ok());
    TestParams tp(result_tile, std::move(qc), {0, 1, 0, 1, 0, 1, 0, 1, 0, 1});
    tp_vec.push_back(tp);
  }

  // Construct basic query condition `foo > 2`.
  {
    QueryCondition qc;
    float val = 2.0f;
    REQUIRE(qc.init(
                  std::string(field_name),
                  &val,
                  sizeof(float),
                  QueryConditionOp::GT)
                .ok());
    TestParams tp(
        result_tile,
        std::move(qc),
        {0, 0, 0, 1, 0, 1, 0, 1, 0, 1},
        {0, 1, 0, 0, 0, 0, 0, 0, 0, 0});
    tp_vec.push_back(tp);
  }

  // Construct query condition `foo < 2 || foo > 4`.
  {
    QueryCondition qc1;
    float val = 2.0f;
    float val1 = 4.0f;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   &val,
                   sizeof(float),
                   QueryConditionOp::LT)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(
                   std::string(field_name),
                   &val1,
                   sizeof(float),
                   QueryConditionOp::GT)
                .ok());
    QueryCondition qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::OR, &qc).ok());

    TestParams tp(
        result_tile,
        std::move(qc),
        {0, 1, 0, 1, 0, 0, 0, 0, 0, 1},
        {0, 0, 0, 0, 0, 1, 0, 1, 0, 0});
    tp_vec.push_back(tp);
  }

  // Construct query condition `foo > 4 || foo = null`.
  {
    QueryCondition qc1;
    float val = 4.0f;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   &val,
                   sizeof(float),
                   QueryConditionOp::GT)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(std::string(field_name), nullptr, 0, QueryConditionOp::EQ)
                .ok());
    QueryCondition qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::OR, &qc).ok());
    TestParams tp(
        result_tile,
        std::move(qc),
        {1, 0, 1, 1, 1, 0, 1, 0, 1, 1},
        {0, 1, 0, 0, 0, 1, 0, 1, 0, 0});
    tp_vec.push_back(tp);
  }

  // Construct query condition `foo = null || foo > 4`.
  {
    QueryCondition qc1;
    float val = 4.0f;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   &val,
                   sizeof(float),
                   QueryConditionOp::GT)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(std::string(field_name), nullptr, 0, QueryConditionOp::EQ)
                .ok());
    QueryCondition qc;
    REQUIRE(qc2.combine(qc1, QueryConditionCombinationOp::OR, &qc).ok());

    TestParams tp(
        result_tile,
        std::move(qc),
        {1, 0, 1, 1, 1, 0, 1, 0, 1, 1},
        {0, 1, 0, 0, 0, 1, 0, 1, 0, 0});
    tp_vec.push_back(tp);
  }

  // Construct basic query condition `foo != null || foo > 4`.
  {
    QueryCondition qc1;
    float val = 4.0f;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   &val,
                   sizeof(float),
                   QueryConditionOp::GT)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(std::string(field_name), nullptr, 0, QueryConditionOp::NE)
                .ok());
    QueryCondition qc;
    REQUIRE(qc2.combine(qc1, QueryConditionCombinationOp::OR, &qc).ok());

    TestParams tp(
        result_tile,
        std::move(qc),
        {0, 1, 0, 1, 0, 1, 0, 1, 0, 1},
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    tp_vec.push_back(tp);
  }

  // Construct basic query condition `foo > 4 || foo != null`.
  {
    QueryCondition qc1;
    float val = 4.0f;
    REQUIRE(qc1.init(
                   std::string(field_name),
                   &val,
                   sizeof(float),
                   QueryConditionOp::GT)
                .ok());

    QueryCondition qc2;
    REQUIRE(qc2.init(std::string(field_name), nullptr, 0, QueryConditionOp::NE)
                .ok());
    QueryCondition qc;
    REQUIRE(qc1.combine(qc2, QueryConditionCombinationOp::OR, &qc).ok());

    TestParams tp(
        result_tile,
        std::move(qc),
        {0, 1, 0, 1, 0, 1, 0, 1, 0, 1},
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    tp_vec.push_back(tp);
  }
}

TEST_CASE(
    "QueryCondition: Test combinations, nullable attributes",
    "[QueryCondition][combinations][nullable]") {
  // Setup.
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::FLOAT32;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(true);
  array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(float),
      0,
      std::nullopt,
      std::nullopt,
      cells * constants::cell_validity_size,
      0);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);
  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &tile_tuple->fixed_tile();

  // Populate the data tile.
  std::vector<float> values = {
      3.4f, 1.3f, 2.2f, 4.5f, 2.8f, 2.1f, 1.7f, 3.3f, 1.9f, 4.2f};
  REQUIRE_NOTHROW(tile->write(values.data(), 0, cells * sizeof(float)));

  Tile* const tile_validity = &tile_tuple->validity_tile();
  std::vector<uint8_t> validity(cells);
  for (uint64_t i = 0; i < cells; ++i) {
    validity[i] = i % 2;
  }
  REQUIRE_NOTHROW(
      tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)));

  std::vector<TestParams> tp_vec;
  populate_nullable_test_params_vector(field_name, &result_tile, tp_vec);

  SECTION("Validate apply.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply(elem, cells, array_schema, result_tile);
    }
  }

  SECTION("Validate apply_sparse.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply_sparse(elem, cells, array_schema, result_tile);
    }
  }

  SECTION("Validate apply_dense.") {
    for (auto& elem : tp_vec) {
      validate_qc_apply_dense(elem, cells, array_schema, result_tile);
    }
  }
}

TEST_CASE(
    "QueryCondition: Test empty/null strings sparse",
    "[QueryCondition][empty_string][null_string][sparse]") {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";
  const Datatype type = GENERATE(Datatype::STRING_ASCII, Datatype::STRING_UTF8);
  bool var_size = true;
  bool nullable = GENERATE(true, false);
  bool null_cmp = GENERATE(true, false);
  QueryConditionOp op = GENERATE(QueryConditionOp::NE, QueryConditionOp::EQ);

  if (!nullable && null_cmp)
    return;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE, memory_tracker);
  Attribute attr(field_name, type);
  attr.set_nullable(nullable);
  attr.set_cell_val_num(var_size ? constants::var_num : 2);

  if (!nullable) {
    attr.set_fill_value(fill_value, 2 * sizeof(char));
  }

  array_schema->add_attribute(make_shared<Attribute>(HERE(), attr));
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  auto dim{
      make_shared<Dimension>(HERE(), "dim1", Datatype::UINT32, memory_tracker)};
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  dim->set_domain(range);

  uint32_t tile_extent = 10;
  dim->set_tile_extent(&tile_extent);
  domain->add_dimension(dim);
  array_schema->set_domain(domain);

  FragmentMetadata frag_md(
      nullptr,
      array_schema,
      URI(tiledb::storage_format::generate_timestamped_name(
          tiledb::sm::utils::time::timestamp_now_ms(),
          constants::format_version)),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      memory_tracker,
      true);

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? (cells + 1) * constants::cell_var_offset_size :
                 2 * (cells - 2) * sizeof(char),
      0,
      var_size ? std::optional(2 * (cells - 2) * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, frag_md, memory_tracker);
  ResultTile::TileData tile_data{nullptr, nullptr, nullptr};
  result_tile.init_attr_tile(
      constants::format_version,
      *array_schema,
      field_name,
      tile_sizes,
      tile_data);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);

  var_size = array_schema->attribute(field_name)->var_size();
  nullable = array_schema->attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &tile_tuple->var_tile() : &tile_tuple->fixed_tile();
  std::vector<char> values(2 * (cells - 2));
  // Empty strings are at idx 8 and 9
  for (uint64_t i = 0; i < (cells - 2); ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }

  REQUIRE_NOTHROW(
      tile->write(values.data(), 0, 2 * (cells - 2) * sizeof(char)));

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells + 1);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    offsets[cells] = offset;
    REQUIRE_NOTHROW(
        tile_offsets->write(offsets.data(), 0, (cells + 1) * sizeof(uint64_t)));
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE_NOTHROW(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)));
  }

  // Empty string or null string as condition value
  const char* cmp_value = null_cmp ? nullptr : "";

  QueryCondition query_condition;
  REQUIRE(query_condition.init(std::string(field_name), cmp_value, 0, op).ok());

  // Run Check for query_condition
  REQUIRE(query_condition.check(*array_schema).ok());

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::EQ:
        if (null_cmp) {
          if (i % 2 == 0)
            expected_cell_idx_vec.emplace_back(i);
        } else if (nullable) {
          if ((i % 2 != 0) && (i >= 8))
            expected_cell_idx_vec.emplace_back(i);
        } else if (i >= 8) {
          expected_cell_idx_vec.emplace_back(i);
        }
        break;
      case QueryConditionOp::NE:
        if (null_cmp) {
          if (i % 2 != 0)
            expected_cell_idx_vec.emplace_back(i);
        } else if (nullable) {
          if ((i % 2 != 0) && (i < 8))
            expected_cell_idx_vec.emplace_back(i);
        } else if (i < 8) {
          expected_cell_idx_vec.emplace_back(i);
        }
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  auto resource = memory_tracker->get_resource(MemoryType::RESULT_TILE_BITMAP);
  tdb::pmr::vector<uint8_t> result_bitmap(cells, 1, resource);
  QueryCondition::Params params(memory_tracker, *array_schema);
  REQUIRE(
      query_condition.apply_sparse<uint8_t>(params, result_tile, result_bitmap)
          .ok());

  // Verify the result bitmap contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (uint64_t cell_idx = 0; cell_idx < cells; ++cell_idx) {
    if (result_bitmap[cell_idx]) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }
}

#ifdef HAVE_RUST

namespace tiledb::test::query_condition_datafusion {

/**
 * Returns a schema for running query condition example tests.
 *
 * Dimension "d" INT64
 * Attribute "a" INT64 NOT NULL
 * Attribute "v" STRING_ASCII[]
 * Attribute "f" UINT16[4]
 * Attribute "ea" INT32:INT64
 * Attribute "ev" INT16:STRING_ASCII[]
 */
std::shared_ptr<ArraySchema> example_schema() {
  uint64_t d_domain[2] = {0, 10};
  std::shared_ptr<Dimension> dimension = std::make_shared<Dimension>(
      "d", Datatype::UINT64, tiledb::test::get_test_memory_tracker());
  dimension->set_domain(&d_domain[0]);

  std::shared_ptr<ArraySchema> schema = std::make_shared<ArraySchema>(
      ArrayType::SPARSE, tiledb::test::get_test_memory_tracker());
  std::shared_ptr<Domain> domain =
      std::make_shared<Domain>(tiledb::test::get_test_memory_tracker());
  domain->add_dimension(dimension);
  schema->set_domain(domain);
  schema->add_attribute(std::make_shared<Attribute>("a", Datatype::UINT64));

  std::shared_ptr<Attribute> v =
      std::make_shared<Attribute>("v", Datatype::STRING_ASCII, true);
  v->set_cell_val_num(constants::var_num);
  schema->add_attribute(v);

  std::shared_ptr<Attribute> f =
      std::make_shared<Attribute>("f", Datatype::UINT16, true);
  f->set_cell_val_num(4);
  schema->add_attribute(f);

  int64_t interesting_i64s_values[] = {
      std::numeric_limits<int64_t>::min(),
      -2,
      -1,
      0,
      1,
      2,
      std::numeric_limits<int64_t>::max()};
  std::shared_ptr<const Enumeration> interesting_i64s = Enumeration::create(
      "interesting_i64s",
      Datatype::INT64,
      1,
      false,
      &interesting_i64s_values[0],
      sizeof(interesting_i64s_values),
      nullptr,
      0,
      tiledb::test::get_test_memory_tracker());
  schema->add_enumeration(interesting_i64s);

  char interesting_strs_var[] = {"foobarbazquuxgraultgarplygub"};
  uint64_t interesting_strs_offsets[] = {0, 3, 6, 9, 13, 19, 25};
  std::shared_ptr<const Enumeration> interesting_strs = Enumeration::create(
      "interesting_strs",
      Datatype::STRING_ASCII,
      constants::var_num,
      false,
      &interesting_strs_var[0],
      sizeof(interesting_strs_var),
      &interesting_strs_offsets[0],
      sizeof(interesting_strs_offsets),
      tiledb::test::get_test_memory_tracker());
  schema->add_enumeration(interesting_strs);

  std::shared_ptr<Attribute> ea =
      std::make_shared<Attribute>("ea", Datatype::INT32, true);
  ea->set_enumeration_name("interesting_i64s");
  schema->add_attribute(ea);

  std::shared_ptr<Attribute> ev =
      std::make_shared<Attribute>("ev", Datatype::INT16, true);
  ev->set_enumeration_name("interesting_strs");
  schema->add_attribute(ev);

  return schema;
}

/**
 * Evaluates a query condition `ast` on a `tile`
 * using both the "ast" and "datafusion" evaluators
 * and compares their results.
 *
 * @return the bitmap produce by the "ast" evaluator
 */
std::vector<uint8_t> instance(
    const tiledb::sm::ArraySchema& array_schema,
    const ResultTile& tile,
    const tiledb::sm::ASTNode& ast) {
  using Asserter = tiledb::test::AsserterRapidcheck;

  // set up traditional TileDB evaluation
  QueryCondition qc_ast(ast.clone());
  qc_ast.rewrite_for_schema(array_schema);

  // set up datafusion evaluation
  QueryCondition qc_datafusion(ast.clone());
  qc_datafusion.rewrite_for_schema(array_schema);
  const bool datafusion_ok = qc_datafusion.rewrite_to_datafusion(
      array_schema, tiledb::oxidize::arrow::schema::WhichSchema::Storage);
  ASSERTER(datafusion_ok);

  // prepare to evaluate
  QueryCondition::Params params(
      tiledb::test::get_test_memory_tracker(), array_schema);

  std::vector<uint8_t> bitmap_ast(tile.cell_num(), 1);
  std::vector<uint8_t> bitmap_datafusion(tile.cell_num(), 1);

  // evaluate traditional ast
  const auto status_ast =
      qc_ast.apply_sparse<uint8_t>(params, tile, bitmap_ast);
  ASSERTER(status_ast.ok());

  // evaluate datafusion
  const auto status_datafusion =
      qc_datafusion.apply_sparse<uint8_t>(params, tile, bitmap_datafusion);
  ASSERTER(status_datafusion.ok());

  // compare
  ASSERTER(bitmap_ast == bitmap_datafusion);

  return bitmap_ast;
}

/**
 * See `instance`. Callable from Rust FFI.
 */
std::unique_ptr<std::vector<uint8_t>> instance_ffi(
    const tiledb::sm::ArraySchema& array_schema,
    const ResultTile& tile,
    const tiledb::sm::ASTNode& ast) {
  return std::make_unique<std::vector<uint8_t>>(
      instance(array_schema, tile, ast));
}

}  // namespace tiledb::test::query_condition_datafusion

TEST_CASE("QueryCondition: Apache DataFusion evaluation", "[QueryCondition]") {
  using tiledb::test::query_condition_datafusion::instance;

  SECTION("Example") {
    auto schema = tiledb::test::query_condition_datafusion::example_schema();

    std::vector<uint64_t> values_d = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<uint64_t> values_a = {
        1,
        22,
        333,
        4444,
        55555,
        666666,
        7777777,
        88888888,
        999999999,
        1010101010};

    // NB: offsets for tiles are arrow-like, i.e. N elements have N+1 offsets
    std::string values_v = {
        "oneonetwoonetwothreeonetwothreefouronetwothreefourfiveonetwothreefourf"
        "ivesix"};
    std::vector<uint64_t> offsets_v = {0, 3, 3, 9, 9, 20, 20, 35, 35, 54, 76};
    std::vector<uint8_t> validity_v = {1, 0, 1, 0, 1, 0, 1, 0, 1, 1};

    std::vector<uint16_t> values_f = {
        1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5,  5,  5,  5,
        6, 6, 6, 6, 7, 7, 7, 7, 8, 8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 10};
    std::vector<uint8_t> validity_f = {1, 1, 0, 1, 1, 0, 1, 1, 0, 1};

    std::vector<int32_t> keys_ea = {0, 2, 4, 4, 3, 1, 2, 1, 4, 1};
    std::vector<uint8_t> validity_ea = {1, 1, 1, 0, 0, 1, 1, 1, 0, 0};

    std::vector<int16_t> keys_ev = {4, 4, 3, 4, 6, 2, 0, 1, 0, 4};
    std::vector<uint8_t> validity_ev = {1, 0, 1, 1, 1, 1, 0, 0, 1, 1};

    ResultTileSizes d_sizes(
        values_d.size() * sizeof(uint64_t),
        0,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt);
    ResultTileData d_data(values_d.data(), nullptr, nullptr);

    ResultTileSizes a_sizes(
        values_a.size() * sizeof(uint64_t),
        0,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt);
    ResultTileData a_data(values_a.data(), nullptr, nullptr);

    ResultTileSizes v_sizes(
        offsets_v.size() * sizeof(uint64_t),
        0,
        values_v.size(),
        0,
        validity_v.size() * sizeof(uint8_t),
        0);
    ResultTileData v_data(offsets_v.data(), values_v.data(), validity_v.data());

    ResultTileSizes f_sizes(
        values_f.size() * sizeof(uint16_t),
        0,
        std::nullopt,
        std::nullopt,
        validity_f.size() * sizeof(uint8_t),
        0);
    ResultTileData f_data(values_f.data(), nullptr, validity_f.data());

    ResultTileSizes ea_sizes(
        keys_ea.size() * sizeof(int32_t),
        0,
        std::nullopt,
        std::nullopt,
        validity_ea.size() * sizeof(uint8_t),
        0);
    ResultTileData ea_data(keys_ea.data(), nullptr, validity_ea.data());

    ResultTileSizes ev_sizes(
        keys_ev.size() * sizeof(int16_t),
        0,
        std::nullopt,
        std::nullopt,
        validity_ev.size() * sizeof(uint8_t),
        0);
    ResultTileData ev_data(keys_ev.data(), nullptr, validity_ev.data());

    ResultTile tile(
        *schema, values_d.size(), tiledb::test::get_test_memory_tracker());
    tile.init_coord_tile(
        constants::format_version, *schema, "d", d_sizes, d_data, 0);
    tile.init_attr_tile(
        constants::format_version, *schema, "a", a_sizes, a_data);
    tile.init_attr_tile(
        constants::format_version, *schema, "v", v_sizes, v_data);
    tile.init_attr_tile(
        constants::format_version, *schema, "f", f_sizes, f_data);
    tile.init_attr_tile(
        constants::format_version, *schema, "ea", ea_sizes, ea_data);
    tile.init_attr_tile(
        constants::format_version, *schema, "ev", ev_sizes, ev_data);

    tile.tile_tuple("d")->fixed_tile().write(
        values_d.data(), 0, values_d.size() * sizeof(uint64_t));
    tile.tile_tuple("a")->fixed_tile().write(
        values_a.data(), 0, values_a.size() * sizeof(uint64_t));

    tile.tile_tuple("v")->fixed_tile().write(
        offsets_v.data(), 0, offsets_v.size() * sizeof(uint64_t));
    tile.tile_tuple("v")->var_tile().write(&values_v[0], 0, values_v.size());
    tile.tile_tuple("v")->validity_tile().write(
        validity_v.data(), 0, validity_v.size() * sizeof(uint8_t));

    tile.tile_tuple("f")->fixed_tile().write(
        values_f.data(), 0, values_f.size() * sizeof(uint16_t));
    tile.tile_tuple("f")->validity_tile().write(
        validity_f.data(), 0, validity_f.size() * sizeof(uint8_t));

    tile.tile_tuple("ea")->fixed_tile().write(
        keys_ea.data(), 0, keys_ea.size() * sizeof(int32_t));
    tile.tile_tuple("ea")->validity_tile().write(
        validity_ea.data(), 0, validity_ea.size() * sizeof(uint8_t));

    tile.tile_tuple("ev")->fixed_tile().write(
        keys_ev.data(), 0, keys_ev.size() * sizeof(int16_t));
    tile.tile_tuple("ev")->validity_tile().write(
        validity_ev.data(), 0, validity_ev.size() * sizeof(uint8_t));

    SECTION("a < 100000") {
      uint64_t value = 100000;
      ASTNodeVal ast(
          "a", &value, sizeof(uint64_t), tiledb::sm::QueryConditionOp::LT);
      const auto results = instance(*schema, tile, ast);
      CHECK(results == std::vector<uint8_t>{1, 1, 1, 1, 1, 0, 0, 0, 0, 0});
    }

    SECTION("d != 6") {
      uint64_t value = 6;
      ASTNodeVal ast(
          "d", &value, sizeof(uint64_t), tiledb::sm::QueryConditionOp::NE);
      const auto results = instance(*schema, tile, ast);
      CHECK(results == std::vector<uint8_t>{1, 1, 1, 1, 1, 0, 1, 1, 1, 1});
    }

    SECTION("4 <= d <= 8") {
      uint64_t lb_value = 4;
      tdb_unique_ptr<ASTNode> lb(new ASTNodeVal(
          "d", &lb_value, sizeof(uint64_t), tiledb::sm::QueryConditionOp::GE));
      const auto results_lb = instance(*schema, tile, *lb);
      CHECK(results_lb == std::vector<uint8_t>{0, 0, 0, 1, 1, 1, 1, 1, 1, 1});

      uint64_t ub_value = 8;
      tdb_unique_ptr<ASTNode> ub(new ASTNodeVal(
          "d", &ub_value, sizeof(uint64_t), tiledb::sm::QueryConditionOp::LE));
      const auto results_ub = instance(*schema, tile, *ub);
      CHECK(results_ub == std::vector<uint8_t>{1, 1, 1, 1, 1, 1, 1, 1, 0, 0});

      std::vector<tdb_unique_ptr<ASTNode>> args;
      args.push_back(std::move(lb));
      args.push_back(std::move(ub));
      ASTNodeExpr ast(std::move(args), QueryConditionCombinationOp::AND);

      const auto results = instance(*schema, tile, ast);
      CHECK(results == std::vector<uint8_t>{0, 0, 0, 1, 1, 1, 1, 1, 0, 0});
    }

    SECTION("v = 'onetwothree'") {
      std::string value = "onetwothree";
      ASTNodeVal ast(
          "v", value.data(), value.size(), tiledb::sm::QueryConditionOp::EQ);
      const auto results = instance(*schema, tile, ast);
      CHECK(results == std::vector<uint8_t>{0, 0, 0, 0, 1, 0, 0, 0, 0, 0});
    }

    SECTION("f != {5, 5, 5, 5}") {
      uint16_t value[] = {5, 5, 5, 5};
      ASTNodeVal ast(
          "f", &value[0], sizeof(value), tiledb::sm::QueryConditionOp::NE);
      const auto results = instance(*schema, tile, ast);
      CHECK(results == std::vector<uint8_t>{1, 1, 0, 1, 0, 0, 1, 1, 0, 1});
    }

    SECTION("v IS NOT NULL") {
      ASTNodeVal ast("v", nullptr, 0, tiledb::sm::QueryConditionOp::NE);
      const auto results = instance(*schema, tile, ast);
      CHECK(results == validity_v);
    }

    SECTION("v IS NULL") {
      ASTNodeVal ast("v", nullptr, 0, tiledb::sm::QueryConditionOp::EQ);
      const auto results = instance(*schema, tile, ast);
      CHECK(results == std::vector<uint8_t>{0, 1, 0, 1, 0, 1, 0, 1, 0, 0});
    }

    SECTION("v IS NOT NULL") {
      ASTNodeVal ast("f", nullptr, 0, tiledb::sm::QueryConditionOp::NE);
      const auto results = instance(*schema, tile, ast);
      CHECK(results == validity_f);
    }

    SECTION("v IS NULL") {
      ASTNodeVal ast("f", nullptr, 0, tiledb::sm::QueryConditionOp::EQ);
      const auto results = instance(*schema, tile, ast);
      CHECK(results == std::vector<uint8_t>{0, 0, 1, 0, 0, 1, 0, 0, 1, 0});
    }

    SECTION("d < 4 OR a > 1000000") {
      uint64_t d_value = 4;
      tdb_unique_ptr<ASTNode> left(new ASTNodeVal(
          "d", &d_value, sizeof(uint64_t), tiledb::sm::QueryConditionOp::LT));
      const auto results_d = instance(*schema, tile, *left);
      CHECK(results_d == std::vector<uint8_t>{1, 1, 1, 0, 0, 0, 0, 0, 0, 0});

      uint64_t a_value = 1000000;
      tdb_unique_ptr<ASTNode> right(new ASTNodeVal(
          "a", &a_value, sizeof(uint64_t), tiledb::sm::QueryConditionOp::GT));
      const auto results_a = instance(*schema, tile, *right);
      CHECK(results_a == std::vector<uint8_t>{0, 0, 0, 0, 0, 0, 1, 1, 1, 1});

      std::vector<tdb_unique_ptr<ASTNode>> args;
      args.push_back(std::move(left));
      args.push_back(std::move(right));
      ASTNodeExpr ast(std::move(args), QueryConditionCombinationOp::OR);

      const auto results = instance(*schema, tile, ast);
      CHECK(results == std::vector<uint8_t>{1, 1, 1, 0, 0, 0, 1, 1, 1, 1});
    }

    SECTION("d IN 1, 3, 5, 7, 9") {
      uint64_t d_accept[] = {1, 3, 5, 7, 9};
      uint64_t d_offsets[] = {0, 8, 16, 24, 32};
      ASTNodeVal ast(
          "d",
          &d_accept[0],
          sizeof(d_accept),
          &d_offsets[0],
          sizeof(d_offsets),
          QueryConditionOp::IN);

      instance(*schema, tile, ast);
    }

    SECTION("ea = -2") {
      int64_t e_value = -2;
      tdb_unique_ptr<ASTNode> ast(new ASTNodeVal(
          "ea", &e_value, sizeof(uint64_t), tiledb::sm::QueryConditionOp::EQ));
      const auto results = instance(*schema, tile, *ast);
      CHECK(results == std::vector<uint8_t>{0, 0, 0, 0, 0, 1, 0, 1, 0, 0});
    }

    SECTION("ev != 'grault'") {
      const std::string ev_value = "grault";
      tdb_unique_ptr<ASTNode> ast(new ASTNodeVal(
          "ev",
          ev_value.data(),
          ev_value.size(),
          tiledb::sm::QueryConditionOp::NE));
      const auto results = instance(*schema, tile, *ast);
      CHECK(results == std::vector<uint8_t>{0, 0, 1, 0, 1, 1, 0, 0, 1, 0});
    }
  }

  SECTION("Example Oxidized") {
    REQUIRE(tiledb::test::examples_query_condition_datafusion());
  }

  SECTION("Proptest") {
    REQUIRE(tiledb::test::proptest_query_condition_datafusion());
  }
}

#endif
