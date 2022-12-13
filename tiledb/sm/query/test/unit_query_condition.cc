/**
 * @file unit_query_condition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"

#include <test/support/tdb_catch.h>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "QueryCondition: Test default constructor",
    "[QueryCondition][default_constructor]") {
  QueryCondition query_condition;
  REQUIRE(query_condition.empty());
  REQUIRE(query_condition.field_names().empty());

  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  std::vector<ResultCellSlab> result_cell_slabs;
  std::vector<shared_ptr<FragmentMetadata>> frag_md;
  REQUIRE(
      query_condition.apply(*array_schema, frag_md, result_cell_slabs, 1).ok());
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
  REQUIRE(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "foo LT 62 61 72");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 78 56 34 12");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 12 ef cd ab");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 33 33 33 33");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "(x LT 12 ef cd ab AND y GT 33 33 33 33)");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 12 ef cd ab");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 33 33 33 33");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "(x LT 12 ef cd ab OR y GT 33 33 33 33)");
}

TEST_CASE(
    "Query Condition: Test AST construction, basic AND combine, string",
    "[QueryCondition][ast][api]") {
  char e[] = "eve";
  QueryCondition query_condition;
  REQUIRE(query_condition.init("x", &e, strlen(e), QueryConditionOp::LT).ok());
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) == "x LT 65 76 65");

  char b[] = "bob";
  QueryCondition query_condition1;
  REQUIRE(query_condition1.init("x", &b, strlen(b), QueryConditionOp::GT).ok());
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) == "x GT 62 6f 62");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "(x LT 65 76 65 AND x GT 62 6f 62)");
}

TEST_CASE(
    "Query Condition: Test AST construction, basic OR combine, string",
    "[QueryCondition][ast][api]") {
  char e[] = "eve";
  QueryCondition query_condition;
  REQUIRE(query_condition.init("x", &e, strlen(e), QueryConditionOp::LT).ok());
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) == "x LT 65 76 65");

  char b[] = "bob";
  QueryCondition query_condition1;
  REQUIRE(query_condition1.init("x", &b, strlen(b), QueryConditionOp::GT).ok());
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) == "x GT 62 6f 62");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "(x LT 65 76 65 OR x GT 62 6f 62)");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 12 ef cd ab");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 33 33 33 33");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "(x LT 12 ef cd ab OR y GT 33 33 33 33)");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "a EQ 12 12 12 12");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "b NE 34 34 34 34");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition2
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or1.ast()) ==
      "(a EQ 12 12 12 12 OR b NE 34 34 34 34)");

  QueryCondition combined_and;
  REQUIRE(combined_or
              .combine(
                  combined_or1, QueryConditionCombinationOp::AND, &combined_and)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "((x LT 12 ef cd ab OR y GT 33 33 33 33) AND (a EQ 12 12 12 12 OR b NE "
      "34 34 34 34))");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 12 ef cd ab");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 33 33 33 33");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "(x LT 12 ef cd ab AND y GT 33 33 33 33)");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "a EQ 12 12 12 12");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "b NE 34 34 34 34");

  QueryCondition combined_and1;
  REQUIRE(query_condition2
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and1.ast()) ==
      "(a EQ 12 12 12 12 AND b NE 34 34 34 34)");

  QueryCondition combined_or;
  REQUIRE(
      combined_and
          .combine(combined_and1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "((x LT 12 ef cd ab AND y GT 33 33 33 33) OR (a EQ 12 12 12 12 AND b NE "
      "34 34 34 34))");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 12 ef cd ab");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 33 33 33 33");

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or.ast()) ==
      "(x LT 12 ef cd ab OR y GT 33 33 33 33)");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "a EQ 12 12 12 12");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "b NE 34 34 34 34");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition2
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or1.ast()) ==
      "(a EQ 12 12 12 12 OR b NE 34 34 34 34)");

  QueryCondition combined_or2;
  REQUIRE(
      combined_or
          .combine(combined_or1, QueryConditionCombinationOp::OR, &combined_or2)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or2.ast()) ==
      "(x LT 12 ef cd ab OR y GT 33 33 33 33 OR a EQ 12 12 12 12 OR b NE 34 34 "
      "34 34)");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition.ast()) ==
      "x LT 12 ef cd ab");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "y GT 33 33 33 33");

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and.ast()) ==
      "(x LT 12 ef cd ab AND y GT 33 33 33 33)");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "a EQ 12 12 12 12");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "b NE 34 34 34 34");

  QueryCondition combined_and1;
  REQUIRE(query_condition2
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and1.ast()) ==
      "(a EQ 12 12 12 12 AND b NE 34 34 34 34)");

  QueryCondition combined_and2;
  REQUIRE(
      combined_and
          .combine(
              combined_and1, QueryConditionCombinationOp::AND, &combined_and2)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and2.ast()) ==
      "(x LT 12 ef cd ab AND y GT 33 33 33 33 AND a EQ 12 12 12 12 AND b NE 34 "
      "34 34 34)");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "foo NE aa aa aa aa");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "foo NE bb bb bb bb");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "foo NE cc cc cc cc");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition4.ast()) ==
      "foo NE dd dd dd dd");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition5.ast()) ==
      "foo NE ee ee ee ee");

  QueryCondition combined_and1;
  REQUIRE(query_condition1
              .combine(
                  query_condition2,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and1.ast()) ==
      "(foo NE aa aa aa aa AND foo NE bb bb bb bb)");
  QueryCondition combined_and2;
  REQUIRE(combined_and1
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and2)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and2.ast()) ==
      "(foo NE aa aa aa aa AND foo NE bb bb bb bb AND foo NE cc cc cc cc)");
  QueryCondition combined_and3;
  REQUIRE(combined_and2
              .combine(
                  query_condition4,
                  QueryConditionCombinationOp::AND,
                  &combined_and3)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and3.ast()) ==
      "(foo NE aa aa aa aa AND foo NE bb bb bb bb AND foo NE cc cc cc cc AND "
      "foo NE dd dd dd dd)");
  QueryCondition combined_and4;
  REQUIRE(combined_and3
              .combine(
                  query_condition5,
                  QueryConditionCombinationOp::AND,
                  &combined_and4)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_and4.ast()) ==
      "(foo NE aa aa aa aa AND foo NE bb bb bb bb AND foo NE cc cc cc cc AND "
      "foo NE dd dd dd dd AND foo NE ee ee ee ee)");
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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition1.ast()) ==
      "foo NE aa aa aa aa");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition2.ast()) ==
      "foo NE bb bb bb bb");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition3.ast()) ==
      "foo NE cc cc cc cc");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition4.ast()) ==
      "foo NE dd dd dd dd");

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
  CHECK(
      tiledb::test::ast_node_to_str(query_condition5.ast()) ==
      "foo NE ee ee ee ee");

  QueryCondition combined_or1;
  REQUIRE(
      query_condition1
          .combine(
              query_condition2, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or1.ast()) ==
      "(foo NE aa aa aa aa OR foo NE bb bb bb bb)");
  QueryCondition combined_or2;
  REQUIRE(
      combined_or1
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or2)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or2.ast()) ==
      "(foo NE aa aa aa aa OR foo NE bb bb bb bb OR foo NE cc cc cc cc)");
  QueryCondition combined_or3;
  REQUIRE(
      combined_or2
          .combine(
              query_condition4, QueryConditionCombinationOp::OR, &combined_or3)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or3.ast()) ==
      "(foo NE aa aa aa aa OR foo NE bb bb bb bb OR foo NE cc cc cc cc OR "
      "foo NE dd dd dd dd)");
  QueryCondition combined_or4;
  REQUIRE(
      combined_or3
          .combine(
              query_condition5, QueryConditionCombinationOp::OR, &combined_or4)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(combined_or4.ast()) ==
      "(foo NE aa aa aa aa OR foo NE bb bb bb bb OR foo NE cc cc cc cc OR "
      "foo NE dd dd dd dd OR foo NE ee ee ee ee)");
}

TEST_CASE(
    "Query Condition: Test AST construction, complex tree with depth > 2",
    "[QueryCondition][ast][api]") {
  std::vector<int> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<QueryCondition> qc_value_vector;
  for (int i = 0; i < 7; ++i) {
    QueryCondition qc;
    REQUIRE(qc.init("x", &vals[i], sizeof(vals[i]), QueryConditionOp::EQ).ok());
    CHECK(
        tiledb::test::ast_node_to_str(qc.ast()) ==
        "x EQ 0" + std::to_string(vals[i]) + " 00 00 00");
    qc_value_vector.push_back(qc);
  }

  for (int i = 7; i < 9; ++i) {
    QueryCondition qc;
    REQUIRE(qc.init("x", &vals[i], sizeof(vals[i]), QueryConditionOp::NE).ok());
    CHECK(
        tiledb::test::ast_node_to_str(qc.ast()) ==
        "x NE 0" + std::to_string(vals[i]) + " 00 00 00");
    qc_value_vector.push_back(qc);
  }

  int x = 6;
  QueryCondition x_neq_six;
  REQUIRE(x_neq_six.init("x", &x, sizeof(x), QueryConditionOp::NE).ok());
  CHECK(tiledb::test::ast_node_to_str(x_neq_six.ast()) == "x NE 06 00 00 00");

  QueryCondition one_or_two;
  REQUIRE(
      qc_value_vector[0]
          .combine(
              qc_value_vector[1], QueryConditionCombinationOp::OR, &one_or_two)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(one_or_two.ast()) ==
      "(x EQ 01 00 00 00 OR x EQ 02 00 00 00)");

  QueryCondition three_or_four;
  REQUIRE(qc_value_vector[2]
              .combine(
                  qc_value_vector[3],
                  QueryConditionCombinationOp::OR,
                  &three_or_four)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(three_or_four.ast()) ==
      "(x EQ 03 00 00 00 OR x EQ 04 00 00 00)");

  QueryCondition six_or_seven;
  REQUIRE(qc_value_vector[5]
              .combine(
                  qc_value_vector[6],
                  QueryConditionCombinationOp::OR,
                  &six_or_seven)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(six_or_seven.ast()) ==
      "(x EQ 06 00 00 00 OR x EQ 07 00 00 00)");

  QueryCondition eight_and_nine;
  REQUIRE(qc_value_vector[7]
              .combine(
                  qc_value_vector[8],
                  QueryConditionCombinationOp::AND,
                  &eight_and_nine)
              .ok());
  CHECK(
      tiledb::test::ast_node_to_str(eight_and_nine.ast()) ==
      "(x NE 08 00 00 00 AND x NE 09 00 00 00)");

  QueryCondition subtree_a;
  REQUIRE(
      one_or_two
          .combine(three_or_four, QueryConditionCombinationOp::AND, &subtree_a)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(subtree_a.ast()) ==
      "((x EQ 01 00 00 00 OR x EQ 02 00 00 00) AND (x EQ 03 00 00 00 OR x EQ "
      "04 00 00 00))");

  QueryCondition subtree_d;
  REQUIRE(
      eight_and_nine
          .combine(six_or_seven, QueryConditionCombinationOp::AND, &subtree_d)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(subtree_d.ast()) ==
      "(x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ 07 "
      "00 00 00))");

  QueryCondition subtree_c;
  REQUIRE(
      subtree_d
          .combine(
              qc_value_vector[4], QueryConditionCombinationOp::OR, &subtree_c)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(subtree_c.ast()) ==
      "((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ "
      "07 00 00 00)) OR x EQ 05 00 00 00)");

  QueryCondition subtree_b;
  REQUIRE(
      subtree_c.combine(x_neq_six, QueryConditionCombinationOp::AND, &subtree_b)
          .ok());
  CHECK(
      tiledb::test::ast_node_to_str(subtree_b.ast()) ==
      "(((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 00 00 00 OR x EQ "
      "07 00 00 00)) OR x EQ 05 00 00 00) AND x NE 06 00 00 00)");

  QueryCondition qc;
  REQUIRE(
      subtree_a.combine(subtree_b, QueryConditionCombinationOp::OR, &qc).ok());
  CHECK(
      tiledb::test::ast_node_to_str(qc.ast()) ==
      "(((x EQ 01 00 00 00 OR x EQ 02 00 00 00) AND (x EQ 03 00 00 00 OR x EQ "
      "04 00 00 00)) OR (((x NE 08 00 00 00 AND x NE 09 00 00 00 AND (x EQ 06 "
      "00 00 00 OR x EQ 07 00 00 00)) OR x EQ 05 00 00 00) AND x NE 06 00 00 "
      "00))");
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
      nullptr,
      array_schema,
      URI(),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      true);
  REQUIRE(
      query_condition.apply(*array_schema, frag_md, result_cell_slabs, 1).ok());

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
          nullptr,
          array_schema,
          URI(),
          std::make_pair<uint64_t, uint64_t>(0, 0),
          true);
      REQUIRE(query_condition_eq_null
                  .apply(*array_schema, frag_md, result_cell_slabs_eq_null, 1)
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
  REQUIRE(array_schema->attribute(field_name)
              ->get_fill_value(&fill_value, &fill_value_size)
              .ok());
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
      query_condition.apply(*array_schema, frag_md, fill_result_cell_slabs, 1)
          .ok());

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
      nullptr,
      array_schema,
      URI(),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      true);
  REQUIRE(
      query_condition.apply(*array_schema, frag_md, result_cell_slabs, 1).ok());

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
  REQUIRE(array_schema->attribute(field_name)
              ->get_fill_value(&fill_value, &fill_value_size)
              .ok());
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
      query_condition.apply(*array_schema, frag_md, fill_result_cell_slabs, 1)
          .ok());

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
  REQUIRE(tile->write(values.data(), 0, 2 * cells * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE(
        tile_offsets->write(offsets.data(), 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)).ok());
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
  REQUIRE(tile->write(values.data(), 0, cells * sizeof(T)).ok());

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
  REQUIRE(type == Datatype::STRING_ASCII);

  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema->add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema->set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, *array_schema);
  ResultTile::TileSizes tile_sizes(
      var_size ? cells * constants::cell_var_offset_size :
                 2 * cells * sizeof(char),
      0,
      var_size ? std::optional(2 * cells * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

  test_apply_tile<char*>(field_name, cells, array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply`.
 */
template <typename T>
void test_apply(const Datatype type, bool var_size, bool nullable) {
  (void)var_size;
  (void)nullable;
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_cell_val_num(1).ok());
  REQUIRE(attr.set_fill_value(&fill_value, sizeof(T)).ok());
  REQUIRE(
      array_schema->add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema->set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(T),
      0,
      var_size ? std::optional(0) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

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
  test_apply<char*>(Datatype::STRING_ASCII);
  test_apply<char*>(Datatype::STRING_ASCII, true);
  test_apply<char*>(Datatype::STRING_ASCII, false, true);
}

TEST_CASE(
    "QueryCondition: Test empty/null strings",
    "[QueryCondition][empty_string][null_string]") {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";
  const Datatype type = Datatype::STRING_ASCII;
  bool var_size = true;
  bool nullable = GENERATE(true, false);
  bool null_cmp = GENERATE(true, false);
  QueryConditionOp op = GENERATE(QueryConditionOp::NE, QueryConditionOp::EQ);

  if (!nullable && null_cmp)
    return;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
          .ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(
      domain
          .add_dimension(tdb::make_shared<tiledb::sm::Dimension>(HERE(), &dim))
          .ok());
  REQUIRE(
      array_schema->set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? cells * constants::cell_var_offset_size :
                 2 * (cells - 2) * sizeof(char),
      0,
      var_size ? std::optional(2 * (cells - 2) * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

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

  REQUIRE(tile->write(values.data(), 0, 2 * (cells - 2) * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    REQUIRE(
        tile_offsets->write(offsets.data(), 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)).ok());
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
  std::vector<shared_ptr<FragmentMetadata>> frag_md(1);
  frag_md[0] = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      nullptr,
      array_schema,
      URI(),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      true);
  REQUIRE(
      query_condition.apply(*array_schema, frag_md, result_cell_slabs, 1).ok());

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

  // Check null comparisons with apply_sparse
  std::vector<uint8_t> bitmap(cells, 1);
  REQUIRE(query_condition.apply_sparse(*array_schema, result_tile, bitmap).ok());

  expected_iter = expected_cell_idx_vec.begin();
  for(uint64_t cell_idx = 0; cell_idx < bitmap.size(); cell_idx++) {
    if (expected_iter != expected_cell_idx_vec.end()
        && cell_idx == *expected_iter) {
      REQUIRE(bitmap[cell_idx] > 0);
      ++expected_iter;
    } else {
      REQUIRE(bitmap[cell_idx] == 0);
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
  REQUIRE(query_condition
              .apply_dense(
                  *array_schema, result_tile, 0, 10, 0, 1, result_bitmap.data())
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
      REQUIRE(query_condition_eq_null
                  .apply_dense(
                      *array_schema,
                      result_tile,
                      0,
                      10,
                      0,
                      1,
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
  REQUIRE(query_condition
              .apply_dense(
                  *array_schema, result_tile, 0, 10, 0, 1, result_bitmap.data())
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
  REQUIRE(tile->write(values.data(), 0, 2 * cells * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE(
        tile_offsets->write(offsets.data(), 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)).ok());
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
  REQUIRE(tile->write(values.data(), 0, cells * sizeof(T)).ok());

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
  REQUIRE(type == Datatype::STRING_ASCII);

  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
          .ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(
      domain
          .add_dimension(tdb::make_shared<tiledb::sm::Dimension>(HERE(), &dim))
          .ok());
  REQUIRE(
      array_schema->set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? cells * constants::cell_var_offset_size :
                 2 * cells * sizeof(char),
      0,
      var_size ? std::optional(2 * cells * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

  test_apply_tile_dense<char*>(field_name, cells, array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply_dense`.
 */
template <typename T>
void test_apply_dense(const Datatype type, bool var_size, bool nullable) {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_cell_val_num(1).ok());
  REQUIRE(attr.set_fill_value(&fill_value, sizeof(T)).ok());
  REQUIRE(
      array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
          .ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(
      domain
          .add_dimension(tdb::make_shared<tiledb::sm::Dimension>(HERE(), &dim))
          .ok());
  REQUIRE(
      array_schema->set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(T),
      0,
      var_size ? std::optional(0) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

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
  test_apply_dense<char*>(Datatype::STRING_ASCII);
  test_apply_dense<char*>(Datatype::STRING_ASCII, true);
  test_apply_dense<char*>(Datatype::STRING_ASCII, false, true);
}

TEST_CASE(
    "QueryCondition: Test empty/null strings dense",
    "[QueryCondition][empty_string][null_string][dense]") {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";
  const Datatype type = Datatype::STRING_ASCII;
  bool var_size = true;
  bool nullable = GENERATE(true, false);
  bool null_cmp = GENERATE(true, false);
  QueryConditionOp op = GENERATE(QueryConditionOp::NE, QueryConditionOp::EQ);

  if (!nullable && null_cmp)
    return;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema->add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(
      domain
          .add_dimension(tdb::make_shared<tiledb::sm::Dimension>(HERE(), &dim))
          .ok());
  REQUIRE(
      array_schema->set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? cells * constants::cell_var_offset_size :
                 2 * (cells - 2) * sizeof(char),
      0,
      var_size ? std::optional(2 * (cells - 2) * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

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

  REQUIRE(tile->write(values.data(), 0, 2 * (cells - 2) * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    REQUIRE(
        tile_offsets->write(offsets.data(), 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)).ok());
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
  REQUIRE(
      query_condition
          .apply_dense(
              *array_schema, &result_tile, 0, 10, 0, 1, result_bitmap.data())
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
  REQUIRE(query_condition
              .apply_sparse<uint8_t>(*array_schema, *result_tile, result_bitmap)
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
      REQUIRE(query_condition_eq_null
                  .apply_sparse<uint8_t>(
                      *array_schema, *result_tile, result_bitmap_eq_null)
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
  REQUIRE(query_condition
              .apply_sparse<uint8_t>(*array_schema, *result_tile, result_bitmap)
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
  REQUIRE(tile->write(values.data(), 0, 2 * cells * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE(
        tile_offsets->write(offsets.data(), 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)).ok());
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
  REQUIRE(tile->write(values.data(), 0, cells * sizeof(T)).ok());

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
  REQUIRE(type == Datatype::STRING_ASCII);

  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
          .ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(
      domain
          .add_dimension(tdb::make_shared<tiledb::sm::Dimension>(HERE(), &dim))
          .ok());
  REQUIRE(
      array_schema->set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? cells * constants::cell_var_offset_size :
                 2 * cells * sizeof(char),
      0,
      var_size ? std::optional(2 * cells * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

  test_apply_tile_sparse<char*>(field_name, cells, array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply_sparse`.
 */
template <typename T>
void test_apply_sparse(const Datatype type, bool var_size, bool nullable) {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_cell_val_num(1).ok());
  REQUIRE(attr.set_fill_value(&fill_value, sizeof(T)).ok());
  REQUIRE(
      array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
          .ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(
      domain
          .add_dimension(tdb::make_shared<tiledb::sm::Dimension>(HERE(), &dim))
          .ok());
  REQUIRE(
      array_schema->set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(T),
      0,
      var_size ? std::optional(0) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

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
  test_apply_sparse<char*>(Datatype::STRING_ASCII);
  test_apply_sparse<char*>(Datatype::STRING_ASCII, true);
  test_apply_sparse<char*>(Datatype::STRING_ASCII, false, true);
}

/**
 * @brief Test parameters structure that contains the query condition
 * object, and the expected results of running the query condition on
 * an size 10 array containing {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}.
 *
 */
struct TestParams {
  QueryCondition qc_;
  std::vector<uint8_t> expected_bitmap_;
  std::vector<ResultCellSlab> expected_slabs_;

  TestParams(
      QueryCondition&& qc,
      std::vector<uint8_t>&& expected_bitmap,
      std::vector<ResultCellSlab>&& expected_slabs)
      : qc_(std::move(qc))
      , expected_bitmap_(std::move(expected_bitmap))
      , expected_slabs_(expected_slabs) {
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
    ResultTile& result_tile) {
  ResultCellSlab result_cell_slab(&result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));
  std::vector<shared_ptr<FragmentMetadata>> frag_md(1);
  frag_md[0] = make_shared<FragmentMetadata>(
      HERE(),
      nullptr,
      nullptr,
      array_schema,
      URI(),
      std::make_pair<uint64_t, uint64_t>(0, 0),
      true);
  REQUIRE(tp.qc_.apply(*array_schema, frag_md, result_cell_slabs, 1).ok());
  REQUIRE(result_cell_slabs.size() == tp.expected_slabs_.size());
  uint64_t result_cell_slabs_size = result_cell_slabs.size();
  for (uint64_t i = 0; i < result_cell_slabs_size; ++i) {
    CHECK(result_cell_slabs[i].start_ == tp.expected_slabs_[i].start_);
    CHECK(result_cell_slabs[i].length_ == tp.expected_slabs_[i].length_);
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
    ResultTile& result_tile) {
  std::vector<uint8_t> sparse_result_bitmap(cells, 1);
  REQUIRE(tp.qc_
              .apply_sparse<uint8_t>(
                  *array_schema, result_tile, sparse_result_bitmap)
              .ok());
  for (uint64_t i = 0; i < cells; ++i) {
    CHECK(sparse_result_bitmap[i] == tp.expected_bitmap_[i]);
  }

  std::vector<uint64_t> sparse_result_bitmap1(cells, 2);
  REQUIRE(tp.qc_
              .apply_sparse<uint64_t>(
                  *array_schema, result_tile, sparse_result_bitmap1)
              .ok());
  for (uint64_t i = 0; i < cells; ++i) {
    CHECK(sparse_result_bitmap1[i] == tp.expected_bitmap_[i] * 2);
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
    ResultTile& result_tile) {
  std::vector<uint8_t> dense_result_bitmap(cells, 1);
  REQUIRE(tp.qc_
              .apply_dense(
                  *array_schema,
                  &result_tile,
                  0,
                  10,
                  0,
                  1,
                  dense_result_bitmap.data())
              .ok());
  for (uint64_t i = 0; i < cells; ++i) {
    CHECK(dense_result_bitmap[i] == tp.expected_bitmap_[i]);
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

    std::vector<uint8_t> expected_bitmap = {0, 0, 0, 0, 1, 1, 1, 0, 0, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 4, 3}};
    TestParams tp(
        std::move(query_condition_3),
        std::move(expected_bitmap),
        std::move(expected_slabs));
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

    std::vector<uint8_t> expected_bitmap = {1, 1, 1, 1, 0, 0, 0, 1, 1, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 4},
                                                  {result_tile, 7, 3}};
    TestParams tp(
        std::move(query_condition_3),
        std::move(expected_bitmap),
        std::move(expected_slabs));
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

    std::vector<uint8_t> expected_bitmap = {0, 0, 0, 1, 1, 1, 1, 1, 1, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 3, 6}};
    TestParams tp(
        std::move(combined_or),
        std::move(expected_bitmap),
        std::move(expected_slabs));
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

    std::vector<uint8_t> expected_bitmap = {1, 1, 1, 0, 0, 0, 0, 0, 0, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 3},
                                                  {result_tile, 9, 1}};
    TestParams tp(
        std::move(combined_and),
        std::move(expected_bitmap),
        std::move(expected_slabs));
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

    std::vector<uint8_t> expected_bitmap = {0, 0, 0, 0, 0, 1, 0, 1, 0, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 5, 1},
                                                  {result_tile, 7, 1}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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

    std::vector<uint8_t> expected_bitmap = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 1},
                                                  {result_tile, 2, 1},
                                                  {result_tile, 4, 1},
                                                  {result_tile, 6, 1},
                                                  {result_tile, 8, 1}};
    TestParams tp(
        std::move(combined_and4),
        std::move(expected_bitmap),
        std::move(expected_slabs));
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
    std::vector<uint8_t> expected_bitmap = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 1},
                                                  {result_tile, 2, 1},
                                                  {result_tile, 4, 1},
                                                  {result_tile, 6, 1},
                                                  {result_tile, 8, 1}};
    TestParams tp(
        std::move(combined_or4),
        std::move(expected_bitmap),
        std::move(expected_slabs));
    tp_vec.push_back(tp);
  }
}

TEST_CASE(
    "QueryCondition: Test combinations", "[QueryCondition][combinations]") {
  // Setup.
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::UINT64;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(
      array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
          .ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(
      domain
          .add_dimension(tdb::make_shared<tiledb::sm::Dimension>(HERE(), &dim))
          .ok());
  REQUIRE(
      array_schema->set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(uint64_t),
      0,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);
  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &tile_tuple->fixed_tile();

  // Populate the data tile.
  std::vector<uint64_t> values(cells);
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = i;
  }
  REQUIRE(tile->write(values.data(), 0, cells * sizeof(uint64_t)).ok());

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
    std::vector<uint8_t> expected_bitmap = {1, 1, 1, 1, 1, 0, 0, 0, 0, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 5}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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
    std::vector<uint8_t> expected_bitmap = {0, 1, 1, 1, 1, 0, 0, 0, 0, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 1, 4}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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

    std::vector<uint8_t> expected_bitmap = {1, 1, 0, 0, 0, 1, 1, 1, 1, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 2},
                                                  {result_tile, 5, 5}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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

    std::vector<uint8_t> expected_bitmap = {0, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 1, 9}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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

    std::vector<uint8_t> expected_bitmap = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 7, 1}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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

      std::vector<uint8_t> expected_bitmap = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
      std::vector<ResultCellSlab> expected_slabs = {{result_tile, 1, 1},
                                                    {result_tile, 3, 1},
                                                    {result_tile, 5, 1},
                                                    {result_tile, 7, 1},
                                                    {result_tile, 9, 1}};
      TestParams tp(
          std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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

      std::vector<uint8_t> expected_bitmap = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
      std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 1},
                                                    {result_tile, 2, 1},
                                                    {result_tile, 4, 1},
                                                    {result_tile, 6, 1},
                                                    {result_tile, 8, 1}};
      TestParams tp(
          std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
      tp_vec.push_back(tp);
    }
  }
}

TEST_CASE(
    "QueryCondition: Test combinations, string",
    "[QueryCondition][combinations][string]") {
  // Setup.
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::STRING_ASCII;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(false).ok());
  REQUIRE(attr.set_cell_val_num(constants::var_num).ok());
  REQUIRE(attr.set_fill_value("ac", 2 * sizeof(char)).ok());

  REQUIRE(
      array_schema->add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema->set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  std::string data = "alicebobcraigdaveerinfrankgraceheidiivanjudy";
  ResultTile::TileSizes tile_sizes(
      cells * constants::cell_var_offset_size,
      0,
      data.size(),
      0,
      std::nullopt,
      std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &tile_tuple->var_tile();

  std::vector<uint64_t> offsets = {0, 5, 8, 13, 17, 21, 26, 31, 36, 40};
  REQUIRE(tile->write(data.c_str(), 0, data.size()).ok());

  // Write the tile offsets.
  Tile* const tile_offsets = &tile_tuple->fixed_tile();
  REQUIRE(
      tile_offsets->write(offsets.data(), 0, cells * sizeof(uint64_t)).ok());

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
    std::vector<uint8_t> expected_bitmap = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 1},
                                                  {result_tile, 2, 1},
                                                  {result_tile, 4, 1},
                                                  {result_tile, 6, 1},
                                                  {result_tile, 8, 1}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
    tp_vec.push_back(tp);
  }

  // Construct basic query condition `foo != null`.
  {
    QueryCondition qc;
    REQUIRE(qc.init(std::string(field_name), nullptr, 0, QueryConditionOp::NE)
                .ok());
    std::vector<uint8_t> expected_bitmap = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 1, 1},
                                                  {result_tile, 3, 1},
                                                  {result_tile, 5, 1},
                                                  {result_tile, 7, 1},
                                                  {result_tile, 9, 1}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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
    std::vector<uint8_t> expected_bitmap = {0, 0, 0, 1, 0, 1, 0, 1, 0, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 3, 1},
                                                  {result_tile, 5, 1},
                                                  {result_tile, 7, 1},
                                                  {result_tile, 9, 1}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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
    std::vector<uint8_t> expected_bitmap = {0, 1, 0, 1, 0, 0, 0, 0, 0, 1};
    std::vector<ResultCellSlab> expected_slabs = {
        {result_tile, 1, 1}, {result_tile, 3, 1}, {result_tile, 9, 1}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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
    std::vector<uint8_t> expected_bitmap = {1, 0, 1, 1, 1, 0, 1, 0, 1, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 1},
                                                  {result_tile, 2, 3},
                                                  {result_tile, 6, 1},
                                                  {result_tile, 8, 2}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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
    std::vector<uint8_t> expected_bitmap = {1, 0, 1, 1, 1, 0, 1, 0, 1, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 0, 1},
                                                  {result_tile, 2, 3},
                                                  {result_tile, 6, 1},
                                                  {result_tile, 8, 2}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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
    std::vector<uint8_t> expected_bitmap = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 1, 1},
                                                  {result_tile, 3, 1},
                                                  {result_tile, 5, 1},
                                                  {result_tile, 7, 1},
                                                  {result_tile, 9, 1}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
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
    std::vector<uint8_t> expected_bitmap = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    std::vector<ResultCellSlab> expected_slabs = {{result_tile, 1, 1},
                                                  {result_tile, 3, 1},
                                                  {result_tile, 5, 1},
                                                  {result_tile, 7, 1},
                                                  {result_tile, 9, 1}};
    TestParams tp(
        std::move(qc), std::move(expected_bitmap), std::move(expected_slabs));
    tp_vec.push_back(tp);
  }
}

TEST_CASE(
    "QueryCondition: Test combinations, nullable attributes",
    "[QueryCondition][combinations][nullable]") {
  // Setup.
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::FLOAT32;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(true).ok());
  REQUIRE(
      array_schema->add_attribute(tdb::make_shared<Attribute>(HERE(), &attr))
          .ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(
      domain
          .add_dimension(tdb::make_shared<tiledb::sm::Dimension>(HERE(), &dim))
          .ok());
  REQUIRE(
      array_schema->set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain))
          .ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      cells * sizeof(float),
      0,
      std::nullopt,
      std::nullopt,
      cells * constants::cell_validity_size,
      0);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);
  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &tile_tuple->fixed_tile();

  // Populate the data tile.
  std::vector<float> values = {
      3.4f, 1.3f, 2.2f, 4.5f, 2.8f, 2.1f, 1.7f, 3.3f, 1.9f, 4.2f};
  REQUIRE(tile->write(values.data(), 0, cells * sizeof(float)).ok());

  Tile* const tile_validity = &tile_tuple->validity_tile();
  std::vector<uint8_t> validity(cells);
  for (uint64_t i = 0; i < cells; ++i) {
    validity[i] = i % 2;
  }
  REQUIRE(
      tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)).ok());

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
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";
  const Datatype type = Datatype::STRING_ASCII;
  bool var_size = true;
  bool nullable = GENERATE(true, false);
  bool null_cmp = GENERATE(true, false);
  QueryConditionOp op = GENERATE(QueryConditionOp::NE, QueryConditionOp::EQ);

  if (!nullable && null_cmp)
    return;

  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(HERE());
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(
      array_schema->add_attribute(make_shared<Attribute>(HERE(), &attr)).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(make_shared<Dimension>(HERE(), &dim)).ok());
  REQUIRE(array_schema->set_domain(make_shared<Domain>(HERE(), &domain)).ok());

  // Initialize the result tile.
  ResultTile::TileSizes tile_sizes(
      var_size ? cells * constants::cell_var_offset_size :
                 2 * (cells - 2) * sizeof(char),
      0,
      var_size ? std::optional(2 * (cells - 2) * sizeof(char)) : std::nullopt,
      var_size ? std::optional(0) : std::nullopt,
      nullable ? std::optional(cells * constants::cell_validity_size) :
                 std::nullopt,
      nullable ? std::optional(0) : std::nullopt);
  ResultTile result_tile(0, 0, *array_schema);
  result_tile.init_attr_tile(
      constants::format_version, *array_schema, field_name, tile_sizes);

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

  REQUIRE(tile->write(values.data(), 0, 2 * (cells - 2) * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &tile_tuple->fixed_tile();
    std::vector<uint64_t> offsets(cells);
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    REQUIRE(
        tile_offsets->write(offsets.data(), 0, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &tile_tuple->validity_tile();
    std::vector<uint8_t> validity(cells);
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(
        tile_validity->write(validity.data(), 0, cells * sizeof(uint8_t)).ok());
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
  REQUIRE(query_condition
              .apply_sparse<uint8_t>(*array_schema, result_tile, result_bitmap)
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
